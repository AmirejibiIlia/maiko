import pandas as pd
import sqlite3
import anthropic
import streamlit as st

def get_dataframe_schema(df):
    """Extract schema information from a dataframe with improved column classification"""
    schema_info = []
    
    # Identify potential client and metric columns
    potential_client_columns = []
    potential_metric_columns = []
    
    # Simple heuristics to identify column types
    for column in df.columns:
        col_lower = column.lower()
        
        # Client identification
        if any(term in col_lower for term in ['client', 'customer', 'user', 'account', 'person', 'id']):
            potential_client_columns.append(column)
        
        # Metric identification - numerical columns with aggregation potential
        if df[column].dtype in ['int64', 'float64', 'int32', 'float32']:
            if not any(term in col_lower for term in ['id', 'code', 'year', 'month', 'day']):
                potential_metric_columns.append(column)
    
    for column in df.columns:
        dtype = str(df[column].dtype)
        non_null_count = df[column].count()
        null_count = df[column].isna().sum()
        unique_count = df[column].nunique()
        
        # Get sample values (handling different data types)
        try:
            sample_vals = df[column].dropna().head(3).tolist()
        except:
            sample_vals = ["[complex data]"]
        
        # Determine column category
        col_category = []
        if column in potential_client_columns:
            col_category.append("client")
        if column in potential_metric_columns:
            col_category.append("metric")
        if 'datetime' in dtype or 'date' in dtype:
            col_category.append("date")
            
        schema_info.append({
            "column": column,
            "dtype": dtype,
            "non_null_count": non_null_count,
            "null_count": null_count,
            "unique_count": unique_count,
            "sample_values": sample_vals,
            "category": col_category
        })
    
    return schema_info, potential_client_columns, potential_metric_columns

def format_schema_for_prompt(schema_info, potential_client_columns, potential_metric_columns):
    """Format schema information for the LLM prompt with column classifications"""
    schema_text = "DataFrame Schema:\n"
    
    # Add column classification overview
    schema_text += "Potential client-related columns: " + ", ".join(potential_client_columns) + "\n"
    schema_text += "Potential metric-related columns: " + ", ".join(potential_metric_columns) + "\n\n"
    
    for col_info in schema_info:
        schema_text += f"- Column: {col_info['column']}\n"
        schema_text += f"  Type: {col_info['dtype']}\n"
        schema_text += f"  Non-null values: {col_info['non_null_count']}, Null values: {col_info['null_count']}\n"
        schema_text += f"  Unique values: {col_info['unique_count']}\n"
        schema_text += f"  Sample values: {col_info['sample_values']}\n"
        if col_info['category']:
            schema_text += f"  Category: {', '.join(col_info['category'])}\n"
        schema_text += "\n"
    
    return schema_text

def generate_sql_query(question, schema_text):
    """Generate SQL query using Anthropic's Claude with improved handling for distinct values"""
    client = anthropic.Client(api_key=st.secrets["ANTHROPIC_API_KEY"])
    
    system_prompt = """You are an expert SQL query generator. You convert natural language questions into SQL queries.
    Please only return the SQL query with no additional explanation, comments, or markdown formatting.
    
    Important guidelines:
    1. For SQLite date handling:
       - Do NOT use EXTRACT() function
       - Instead, use strftime('%m', date_column) to extract month
       - Use strftime('%Y', date_column) to extract year
       - Use strftime('%d', date_column) to extract day
       - For counting distinct months, use: COUNT(DISTINCT strftime('%m', date_column))
    
    2. For client-related questions:
       - When asked about clients, use client-related columns (like client_id, client_name, etc.)
       - For distinct client counts, use COUNT(DISTINCT client_column)
       - Group by client columns when aggregating client-specific metrics
    
    3. For metric-related questions:
       - When asked about metrics, focus on numerical/metric columns
       - Use appropriate aggregation functions (SUM, AVG, MIN, MAX) based on the question
       - For distinct metric values, use COUNT(DISTINCT metric_column)
       - Use HAVING clauses when filtering on aggregated values
       
    4. For questions in Georgian language:
       - Interpret the question and generate the appropriate SQL query
       - Handle transliterations and fuzzy matches for column names and values
    """
    
    user_prompt = f"""I have a pandas DataFrame with the following schema:

{schema_text}

The dataframe will be converted to a SQL table named 'data'.

Question: {question}

Generate only the SQL query to answer this question. Return only the query itself without any explanation or comments.
Pay special attention to distinct client values and distinct metric values when mentioned in the question.
"""

    try:
        response = client.messages.create(
            model="claude-3-sonnet-20240229",
            system=system_prompt,
            max_tokens=1000,
            temperature=0,  # Use 0 for deterministic responses
            messages=[
                {"role": "user", "content": user_prompt}
            ]
        )
        
        sql_query = response.content[0].text.strip()
        return sql_query
    
    except Exception as e:
        return f"Error generating SQL query: {str(e)}"

def execute_sql_on_dataframe(df, sql_query):
    """Execute SQL query on the dataframe using SQLite"""
    conn = sqlite3.connect(':memory:')
    
    # Convert 'date' column to datetime if not already
    if 'date' in df.columns and not pd.api.types.is_datetime64_any_dtype(df['date']):
        df['date'] = pd.to_datetime(df['date'], format='%d.%m.%y')
    
    # Write the dataframe to SQLite
    df.to_sql('data', conn, index=False, if_exists='replace')
    
    try:
        # Execute the query
        result = pd.read_sql_query(sql_query, conn)
        return result, None
    except Exception as e:
        return None, str(e)
    finally:
        conn.close()

def answer_question_with_sql(df, question):
    """Main function to answer questions using SQL on a dataframe"""
    # Get schema info
    schema_info, client_cols, metric_cols = get_dataframe_schema(df)
    schema_text = format_schema_for_prompt(schema_info, client_cols, metric_cols)
    
    # Generate SQL
    sql_query = generate_sql_query(question, schema_text)
    
    # Execute SQL
    result, error = execute_sql_on_dataframe(df, sql_query)
    
    return {
        "question": question,
        "sql_query": sql_query,
        "result": result if result is not None else pd.DataFrame(),
        "error": error
    }