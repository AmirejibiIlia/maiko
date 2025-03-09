import pandas as pd
import streamlit as st
import anthropic
import json
import re

def query_data(data: pd.DataFrame, where: dict = None, group_by: list = None, 
               aggregations: dict = None, order_by: list = None) -> pd.DataFrame:
    """Main function that ties together all the SQL-like operations."""
    # Convert 'year' column to datetime if not already
    if 'year' in data.columns and not pd.api.types.is_datetime64_any_dtype(data['year']):
        data['year'] = pd.to_datetime(data['year'], format='%d.%m.%y')
    
    # Apply operations step by step
    data = apply_where(data, where)
    data = group_and_aggregate(data, group_by, aggregations)
    data = apply_order_by(data, order_by)
    
    return data

def apply_where(data: pd.DataFrame, where: dict) -> pd.DataFrame:
    """Applies WHERE conditions to the DataFrame."""
    if where:
        for column, condition in where.items():
            for operator, value in condition.items():
                # Convert value to datetime if the column is 'year' (or any datetime column)
                if column == "year" and isinstance(value, str):  
                    value = pd.to_datetime(value, format='%Y-%m-%d', errors='coerce')  
                    
                if operator == '>=':
                    data = data[data[column] >= value]
                elif operator == '<':
                    data = data[data[column] < value]
                elif operator == '=':
                    data = data[data[column] == value]
                elif operator == '!=':
                    data = data[data[column] != value]
                elif operator == '<=':
                    data = data[data[column] <= value]
                elif operator == '>':
                    data = data[data[column] > value]
    return data

def group_and_aggregate(data: pd.DataFrame, group_by: list, aggregations: dict) -> pd.DataFrame:
    """Groups the DataFrame by specified columns and applies aggregations, renaming the resulting columns."""
    if group_by:
        # If grouping by certain columns, perform the aggregation
        data = data.groupby(group_by, as_index=False).agg(aggregations)
    else:
        # If no grouping is needed, simply apply the aggregation over the whole dataset
        data = pd.DataFrame(data.agg(aggregations)).transpose()

    # Rename columns to match SQL-style SELECT aliases
    new_columns = []
    for col in data.columns:
        if isinstance(col, tuple):  # Multi-index columns from aggregation
            new_columns.append(f"{col[0]}_{col[1]}")  # Example: 'Sales_sum'
        else:
            new_columns.append(col)  # Keep original column name

    data.columns = new_columns  # Assign new column names
    return data

def apply_order_by(data: pd.DataFrame, order_by: list) -> pd.DataFrame:
    """Applies ORDER BY sorting to the DataFrame."""
    if order_by:
        for col, ascending in order_by:
            if isinstance(data, pd.DataFrame):  # Check if it's a DataFrame
                data = data.sort_values(by=col, ascending=ascending)
    return data

def execute_query(query_json: dict) -> pd.DataFrame:
    """
    Executes the query by receiving the general JSON-like object with all query parameters.
    The query_json structure should include 'data', 'where', 'group_by', 'aggregations', and 'order_by'.
    """
    # Extract parameters from the provided JSON object
    data = query_json.get("data")
    where = query_json.get("where")
    group_by = query_json.get("group_by")
    aggregations = query_json.get("aggregations")
    order_by = query_json.get("order_by")
    
    # Call query_data with the parameters extracted from the JSON-like object
    result = query_data(data, 
                        where=where, 
                        group_by=group_by, 
                        aggregations=aggregations, 
                        order_by=order_by)
    
    return result

def extract_json_from_text(text):
    """
    Extract JSON object from text that might contain markdown and other content.
    """
    # First try to extract JSON from markdown code blocks
    json_match = re.search(r'```(?:json)?\s*([\s\S]*?)\s*```', text)
    
    if json_match:
        return json_match.group(1).strip()
    
    # If no code blocks, try to find JSON-like structure
    # Look for the opening/closing braces
    brace_match = re.search(r'(\{[\s\S]*\})', text)
    if brace_match:
        return brace_match.group(1).strip()
    
    # If all else fails, return the original text
    return text

# def simple_finance_chat():
#     st.title("სალამი, მე ვარ MAIA")
#     st.write("ატვირთე ფაილი და იგრიალე!")
    
#     uploaded_file = st.file_uploader("Upload your financial data Excel file", type=["xlsx"])
    
#     if uploaded_file is not None:
#         df = pd.read_excel(uploaded_file)
#         required_columns = {"year", "metrics", "value"}
        
#         if not required_columns.issubset(df.columns):
#             st.error(f"Your file must contain the following columns: {', '.join(required_columns)}")
#             return
        
#         df["value"] = pd.to_numeric(df["value"], errors="coerce")
#         df.dropna(subset=["value"], inplace=True)
        
#         question = st.text_input("Ask your financial question:")
        
#         if question:
#             client = anthropic.Client(api_key=st.secrets["ANTHROPIC_API_KEY"])
            
#             # Format prompt properly with question inside
#             prompt = f'''
#             Question: {question}
            
#             Convert the following financial question into a structured JSON query object, that will contain arguments for SQL like operational query_data().
#             So, JSON query object should consist of "where","group_by","aggregations","order_by" parts. 
            
#             query_data( data, 
#                         where=where, 
#                         group_by=group_by, 
#                         aggregations=aggregations, 
#                         order_by=order_by)

#             ## Available Data Structure
#             The dataframe contains financial data with these key columns:
#             - `metrics`: Various types of income categories denominated in Georgian Language
#             - `year`: Date of income (daily granularity)
#             - `value`: Numerical amount of income

#             ## Required JSON Structure
#             Your response must follow this exact format (structure only, not these example values):
#             ```json
#             {{
#                 "data": "df",
#                 "where": {{
#                             "column_name" : {{ }} 
#                             }}, // Empty unless filters are explicitly mentioned
#                 "group_by": ["column_name"],
#                 "aggregations": {{"column_name": ["aggregation_function"]}},
#                 "order_by": [["column_name_with_suffix", boolean]]
#             }}
#             ```

#             ## Technical Specifications

#             1. **"data"**: Always set to `df` (the dataframe variable name) - Mandatory

#             2. **"where"**: A filtering dictionary specifying conditions - Optional
#             - Keys represent column names to filter on
#             - Values are nested dictionaries with operator-value pairs
#             - Operators include: "=", ">", "<", ">=", "<=", "!="
#             - Example: `{{"metrics": {{"=": "income from production"}}}}` filters for rows where metrics equals "income from production"
#             - Multiple conditions can be specified as separate key-value pairs

#             3. **"group_by"**: List of columns to group by - Optional
#             - Only group in case question asks grouping, based on data structure.
#             - Example of groupings: `["year"]`, `["metrics"]`, or `["year", "metrics"]`
            

#             4. **"aggregations"**: Dictionary defining aggregation operations - Optional
#             - Key: Column to aggregate (typically `"value"`)
#             - Value: List of aggregation functions (typically `["sum"]`)
#             - Example: `{{"value": ["sum"]}}` calculates sum of values in each group

#             5. **"order_by"**: List of arrays for sorting results - Optional
#             - Only order by in case question asks ordering, based on data column.
#             - Each tuple contains: (column_name, sort_direction)
#             - Column names often include aggregation suffix (e.g., `"value_sum"`)
#             - Sort direction: has two possible boolean values,`false` for descending, `true` for ascending
#             - Example: `[["value_sum", false]]` sorts by total value in descending order

#             ## Implementation Rules
#             - Include any of above Optional components if and only if asked.
#             - Always include `"where"` when question mentions specific metrics or time periods
#             - Use appropriate `"group_by"` based on the question's focus (by year, by metric type, etc.)
#             - For aggregations, use `"value"` as the key and include appropriate functions (typically `["sum"]`)
#             - Include `"order_by"` !only! when question mentions sorting or ranking (e.g., "highest", "lowest")
#             - Dates should be formatted as "YYYY-MM-DD"

#             VERY IMPORTANT: Return only a valid JSON object without any markdown formatting, comments, or explanations.
#             '''

def simple_finance_chat():
    st.title("სალამი, მე ვარ MAIA")
    st.write("ატვირთე ფაილი და იგრიალე!")
    
    uploaded_file = st.file_uploader("Upload your financial data Excel file", type=["xlsx"])
    
    if uploaded_file is not None:
        df = pd.read_excel(uploaded_file)
        required_columns = {"year", "metrics", "value"}
        
        if not required_columns.issubset(df.columns):
            st.error(f"Your file must contain the following columns: {', '.join(required_columns)}")
            return
        
        df["value"] = pd.to_numeric(df["value"], errors="coerce")
        df.dropna(subset=["value"], inplace=True)
        
        # Generate data overview for context
        with st.expander("Data Overview"):
            st.write("### Dataset Summary")
            st.dataframe(df.head())
            
            # Display unique metrics
            unique_metrics = df['metrics'].unique()
            st.write(f"### Available Metrics ({len(unique_metrics)})")
            st.write(", ".join(unique_metrics))
            
            # Display time range
            min_date = df['year'].min()
            max_date = df['year'].max()
            st.write(f"### Time Range: {min_date} to {max_date}")
            
            # Display basic statistics
            st.write("### Value Statistics")
            st.dataframe(df.groupby('metrics')['value'].agg(['sum', 'mean', 'count']))
        
        # Create data context for Claude
        data_context = {
            "metrics_list": unique_metrics.tolist(),
            "date_range": {
                "min": min_date.strftime('%Y-%m-%d') if isinstance(min_date, pd.Timestamp) else str(min_date),
                "max": max_date.strftime('%Y-%m-%d') if isinstance(max_date, pd.Timestamp) else str(max_date)
            },
            "total_records": len(df),
            "sample_data": df.head(5).to_dict(orient='records')
        }
        
        question = st.text_input("Ask your financial question:")
        
        if question:
            client = anthropic.Client(api_key=st.secrets["ANTHROPIC_API_KEY"])
            
            # Update prompt with data context
            prompt = f'''
            Question: {question}
            
            Convert the following financial question into a structured JSON query object, that will contain arguments for SQL like operational query_data().
            
            ## Data Overview
            {json.dumps(data_context, indent=2)}
            
            ## Available Data Structure
            The dataframe contains financial data with these key columns:
            - `metrics`: Various types of income categories denominated in Georgian Language
            - `year`: Date of income (daily granularity)
            - `value`: Numerical amount of income

            ## Required JSON Structure
            Your response must follow this exact format (structure only, not these example values):
            ```json
            {{
                "data": "df",
                "where": {{
                            "column_name" : {{ }} 
                            }}, // Empty unless filters are explicitly mentioned
                "group_by": ["column_name"],
                "aggregations": {{"column_name": ["aggregation_function"]}},
                "order_by": [["column_name_with_suffix", boolean]]
            }}
            ```

            ## Technical Specifications

            1. **"data"**: Always set to `df` (the dataframe variable name) - Mandatory

            2. **"where"**: A filtering dictionary specifying conditions - Optional
            - Keys represent column names to filter on
            - Values are nested dictionaries with operator-value pairs
            - Operators include: "=", ">", "<", ">=", "<=", "!="
            - Example: `{{"metrics": {{"=": "income from production"}}}}` filters for rows where metrics equals "income from production"
            - Multiple conditions can be specified as separate key-value pairs

            3. **"group_by"**: List of columns to group by - Optional
            - Only group in case question asks grouping, based on data structure.
            - Example of groupings: `["year"]`, `["metrics"]`, or `["year", "metrics"]`
            

            4. **"aggregations"**: Dictionary defining aggregation operations - Optional
            - Key: Column to aggregate (typically `"value"`)
            - Value: List of aggregation functions (typically `["sum"]`)
            - Example: `{{"value": ["sum"]}}` calculates sum of values in each group

            5. **"order_by"**: List of arrays for sorting results - Optional
            - Only order by in case question asks ordering, based on data column.
            - Each tuple contains: (column_name, sort_direction)
            - Column names often include aggregation suffix (e.g., `"value_sum"`)
            - Sort direction: has two possible boolean values,`false` for descending, `true` for ascending
            - Example: `[["value_sum", false]]` sorts by total value in descending order

            ## Implementation Rules
            - Include any of above Optional components if and only if asked.
            - Always include `"where"` when question mentions specific metrics or time periods
            - Use appropriate `"group_by"` based on the question's focus (by year, by metric type, etc.)
            - For aggregations, use `"value"` as the key and include appropriate functions (typically `["sum"]`)
            - Include `"order_by"` !only! when question mentions sorting or ranking (e.g., "highest", "lowest")
            - Dates should be formatted as "YYYY-MM-DD"
            - When the question is vague or doesn't specify filters, use the context from Data Overview to provide sensible defaults
            - Match metrics names exactly as they appear in the metrics_list from the data context

            VERY IMPORTANT: Return only a valid JSON object without any markdown formatting, comments, or explanations.
            '''
            
            try:
                response = client.messages.create(
                    model="claude-3-sonnet-20240229",
                    max_tokens=1000,
                    temperature=0,
                    messages=[{"role": "user", "content": prompt}]
                )
                
                response_text = response.content[0].text.strip()
                
                # Display raw response for debugging (can be removed in production)
                st.write("### Raw Response from Claude:")
                st.write(response_text)
                
                # Directly parse the JSON
                try:
                    query_json = json.loads(response_text)
                except json.JSONDecodeError:
                    # If direct parsing fails, try a basic cleanup
                    # This removes any markdown code block indicators
                    cleaned_text = response_text.replace("```json", "").replace("```", "").strip()
                    query_json = json.loads(cleaned_text)
                
                query_json["data"] = df
                
                # Fix order_by format if needed - ensure it's a list of lists, not tuples
                if "order_by" in query_json and query_json["order_by"]:
                    query_json["order_by"] = [[item[0], item[1]] if isinstance(item, tuple) else item for item in query_json["order_by"]]
                
                st.write("### Your Question:")
                st.write(question)
                
                st.write("### Generated JSON Query:")
                st.json(query_json)
                
                result_df = execute_query(query_json)
                
                st.write("### Query Result:")
                st.dataframe(result_df)
                
            except Exception as e:
                st.error(f"Error: {str(e)}")
                st.error(f"Response content: {response.content[0].text if 'response' in locals() else 'No response'}")
                
                # Show debugging info for JSON parsing errors
                if isinstance(e, json.JSONDecodeError):
                    st.error("JSON parsing error. Check the response structure.")
                    if 'response_text' in locals():
                        st.write("Problematic character position:", e.pos)
                        st.write("Character causing the error:", response_text[e.pos:e.pos+10] if e.pos < len(response_text) else "End of string")
                        st.code(response_text, language="json")

if __name__ == "__main__":
    simple_finance_chat()