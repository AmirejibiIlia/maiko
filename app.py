import pandas as pd
import streamlit as st
import anthropic
import json
import re
import boto3
import io
import csv
import datetime 
import base64
import uuid
from streamlit.components.v1 import html


def query_data(data: pd.DataFrame, where: dict = None, group_by: list = None, 
               aggregations: dict = None, order_by: list = None) -> pd.DataFrame:
    """Main function that ties together all the SQL-like operations."""
    # Convert 'date' column to datetime if not already
    if 'date' in data.columns and not pd.api.types.is_datetime64_any_dtype(data['date']):
        data['date'] = pd.to_datetime(data['date'], format='%d.%m.%y')
    
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
                # Convert value to datetime if the column is 'date' (or any datetime column)
                if column == "date" and isinstance(value, str):  
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
    """Groups the DataFrame by specified columns and applies aggregations, handling time periods."""
    if not group_by:
        # If no grouping is needed, simply apply the aggregation over the whole dataset
        data = pd.DataFrame(data.agg(aggregations)).transpose()
    else:
        # Process special time period groupings
        processed_group_by = []
        data_copy = data.copy()
        
        for group_col in group_by:
            # Handle special time period groupings
            if group_col == 'quarter':
                data_copy['quarter'] = data_copy['date'].dt.to_period('Q').astype(str)
                processed_group_by.append('quarter')
            elif group_col == 'month':
                data_copy['month'] = data_copy['date'].dt.to_period('M').astype(str)
                processed_group_by.append('month')
            elif group_col == 'year_only':
                data_copy['year_only'] = data_copy['date'].dt.year
                processed_group_by.append('year_only')
            elif group_col == 'week':
                data_copy['week'] = data_copy['date'].dt.isocalendar().week
                processed_group_by.append('week')
            else:
                # Regular column (metrics, client, or others)
                processed_group_by.append(group_col)
        
        # Perform grouping with processed columns
        data = data_copy.groupby(processed_group_by, as_index=False).agg(aggregations)

    # Rename columns to match SQL-style SELECT aliases
    new_columns = []
    
    for col in data.columns:
        if isinstance(col, tuple):  # Multi-index columns from aggregation
            if isinstance(col[1], list):
                # Handle case where aggregation is a list like ["sum"]
                new_columns.append(f"{col[0]}_{col[1][0]}")
            else:
                # Handle case where aggregation is a string like "sum"
                new_columns.append(f"{col[0]}_{col[1]}")
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

def interpret_results(df, question):
    """
    Send the query results back to Claude for interpretation.
    """
    client = anthropic.Client(api_key=st.secrets["ANTHROPIC_API_KEY"])
    
    # Convert the dataframe to a JSON string for Claude
    df_json = df.to_json(orient='records', date_format='iso')
    
    interpretation_prompt = f"""
    Original question: {question}
    
    Query results:
    {df_json}
        
    Please answer in a clear, concise way to the original question. Answer in Georgian.
    Keep your answer brief and to the point, focusing only on what was asked in the original question!

    If the question asks for trends or comparisons, express the percent changes when relevant.

    Rules for answering:
        1. Perform all calculations with precision – use exact arithmetic operations instead of estimations.  
        2. If the question involves addition, subtraction, multiplication, or division, compute the exact result.   
        3. Do not provide additional information, for example trends, comparisons, or additional analysis unless specifically requested.
        4. If asked about a specific metric in a specific year, provide just that number
        5. If asked about trends, compare numbers and calculate percentage changes
        6. If asked about highest/lowest values, specify both the date and the value
        7. Format numbers with thousand separators for better readability
        8. Give structured output!
    """
    
    response = client.messages.create(
        model="claude-3-sonnet-20240229",
        max_tokens=1000,
        temperature=0,
        messages=[{"role": "user", "content": interpretation_prompt}]
    )
    
    return response.content[0].text.strip()

def load_excel_from_s3():
    """
    Load the FullData.xlsx file from S3 bucket
    """
    try:
        # Create S3 client
        s3_client = boto3.client(
            's3',
            aws_access_key_id=st.secrets["aws"]["access_key_id"],
            aws_secret_access_key=st.secrets["aws"]["secret_access_key"],
            region_name=st.secrets["aws"]["region"]
        )
        
        bucket_name = st.secrets["aws"]["bucket_name"]
        file_key = "FullData.xlsx"
        
        # Get the file from S3
        response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        excel_content = response['Body'].read()
        
        # Load into pandas DataFrame
        df = pd.read_excel(io.BytesIO(excel_content))
        
        return df
    except Exception as e:
        st.error(f"Error loading Excel file from S3: {str(e)}")
        return None

def log_question_and_rating_to_s3(question=None, rating=None, uploaded_file_name=None, question_id=None):
    """
    Efficiently logs questions and ratings to S3 with proper encoding for Georgian characters
    """
    try:
        # Create S3 client and set up constants
        timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        bucket_name = st.secrets["aws"]["bucket_name"]
        file_key = "question_logs.csv"
        s3_client = boto3.client(
            's3',
            aws_access_key_id=st.secrets["aws"]["access_key_id"],
            aws_secret_access_key=st.secrets["aws"]["secret_access_key"],
            region_name=st.secrets["aws"]["region"]
        )
        
        # Load existing log data or create new DataFrame
        try:
            response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
            df = pd.read_csv(io.StringIO(response['Body'].read().decode('utf-8')))
        except Exception:
            # Create new DataFrame if file doesn't exist or other error occurs
            df = pd.DataFrame(columns=["timestamp", "file_name", "question", "rating", "question_id"])
        
        # Handle new question or rating update
        if question_id is None and question is not None:
            # New question entry
            question_id = str(uuid.uuid4())
            new_row = {
                "timestamp": timestamp,
                "file_name": uploaded_file_name or 'None',
                "question": question,
                "rating": rating if rating is not None else "",
                "question_id": question_id
            }
            df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
            
        elif question_id is not None and rating is not None:
            # Update existing entry with rating
            if "question_id" in df.columns and df["question_id"].astype(str).eq(str(question_id)).any():
                idx = df.index[df["question_id"].astype(str) == str(question_id)].tolist()[0]
                df.at[idx, "rating"] = rating
            else:
                # Create a new entry if question_id not found
                new_row = {
                    "timestamp": timestamp,
                    "file_name": uploaded_file_name or 'None',
                    "question": "Rating update for missing ID",
                    "rating": rating,
                    "question_id": question_id
                }
                df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
        
        # Upload to S3 with explicit UTF-8 encoding
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        s3_client.put_object(
            Bucket=bucket_name,
            Key=file_key,
            Body=csv_buffer.getvalue().encode('utf-8'),
            ContentType='text/csv; charset=utf-8'
        )
        
        return question_id
    
    except Exception as e:
        # Simplified error handling
        print(f"Error in logging function: {type(e).__name__}: {e}")
        import traceback
        print(f"Traceback: {traceback.format_exc()}")
        return question_id

    
                    
def set_background_from_s3():
    """
    Function to set a background image from S3 for the Streamlit app
    """
    try:
        # Create S3 client
        s3_client = boto3.client(
            's3',
            aws_access_key_id=st.secrets["aws"]["access_key_id"],
            aws_secret_access_key=st.secrets["aws"]["secret_access_key"],
            region_name=st.secrets["aws"]["region"]
        )
        
        bucket_name = st.secrets["aws"]["bucket_name"]
        image_key = "Image.png"  # The name of your image in S3
        
        response = s3_client.get_object(Bucket=bucket_name, Key=image_key)
        image_bytes = response['Body'].read()
        encoded_string = base64.b64encode(image_bytes).decode()
        
        st.markdown(
            f"""
            <style>
            .stApp {{
                background-image: url(data:image/png;base64,{encoded_string});
                background-size: cover;
                background-repeat: no-repeat;
                background-attachment: fixed;
            }}
            
            /* Add semi-transparent background behind text elements */
            .st-emotion-cache-1kyxreq h1,
            .st-emotion-cache-1kyxreq h2,
            .st-emotion-cache-1kyxreq h3,
            .st-emotion-cache-1kyxreq p,
            .st-emotion-cache-1kyxreq div,
            .st-emotion-cache-1kyxreq li,
            div.st-emotion-cache-ue6h4q,
            div.st-emotion-cache-4z1n9p p,
            .st-emotion-cache-r421ms,
            .element-container {{
                background-color: rgba(255, 255, 255, 1);
                padding: 10px;
                border-radius: 5px;
                margin-bottom: 10px;
            }}
            
            /* Style for dataframes */
            .dataframe {{
                background-color: rgba(255, 255, 255, 1) !important;
            }}
            
            /* Style for expander headers */
            .st-emotion-cache-10oheav {{
                background-color: rgba(255, 255, 255, 1);
                padding: 5px;
                border-radius: 5px;
            }}
            
            /* Style for file uploader and input fields */
            .st-emotion-cache-1gulkj5,
            .st-emotion-cache-16toyut {{
                background-color: rgba(255, 255, 255, 1);
                padding: 10px;
                border-radius: 5px;
            }}
            
            /* Style for buttons */
            .stButton > button {{
                background-color: rgba(255, 255, 255, 1) !important;
            }}
            </style>
            """,
            unsafe_allow_html=True
        )
    except Exception as e:
        print(f"Error loading background image: {str(e)}")
        
def simple_finance_chat():
    # # Set the background image at the beginning
    # set_background_from_s3()
    
    st.title("სალამი, მე ვარ MAIA - Demo ვერსია")
    st.write("დამისვი მრავალფეროვანი კითხვები, რომ ბევრი ვისწავლო!")
    
    # Initialize session state for rating
    if 'has_rated' not in st.session_state:
        st.session_state.has_rated = False
    if 'current_question' not in st.session_state:
        st.session_state.current_question = ""
    
    # Load data directly from S3 instead of file uploader
    try:
        # Create S3 client
        s3_client = boto3.client(
            's3',
            aws_access_key_id=st.secrets["aws"]["access_key_id"],
            aws_secret_access_key=st.secrets["aws"]["secret_access_key"],
            region_name=st.secrets["aws"]["region"]
        )
        
        bucket_name = st.secrets["aws"]["bucket_name"]
        file_key = "FullData.xlsx"  # Excel file name in S3
        
        # Download file from S3 to memory
        response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        excel_data = response['Body'].read()
        
        # Read Excel file from memory
        df = pd.read_excel(io.BytesIO(excel_data))
        
        required_columns = {"date", "metrics", "value", "client"}
        
        if not required_columns.issubset(df.columns):
            st.error(f"The data file must contain the following columns: {', '.join(required_columns)}")
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
            
            # Display unique clients
            unique_clients = df['client'].unique()
            st.write(f"### Available Clients ({len(unique_clients)})")
            st.write(", ".join(unique_clients))
            
            # Display time range
            min_date = df['date'].min()
            max_date = df['date'].max()
            st.write(f"### Time Range: {min_date} to {max_date}")
            
            # Display basic statistics
            st.write("### Value Statistics")
            st.dataframe(df.groupby('metrics')['value'].agg(['sum', 'mean', 'count']))
            
            # Display client statistics
            st.write("### Client Statistics")
            st.dataframe(df.groupby('client')['value'].agg(['sum', 'mean', 'count']))
                                                     
        # Convert non-serializable types to strings
        min_date_str = min_date.strftime('%Y-%m-%d') if hasattr(min_date, 'strftime') else str(min_date)
        max_date_str = max_date.strftime('%Y-%m-%d') if hasattr(max_date, 'strftime') else str(max_date)
        
        # Create sample data that's JSON serializable
        sample_records = []
        for _, row in df.head(5).iterrows():
            record = {}
            for column, value in row.items():
                # Convert any timestamps or other problematic types to string
                if pd.api.types.is_datetime64_any_dtype(pd.Series([value])):
                    record[column] = value.strftime('%Y-%m-%d')
                else:
                    record[column] = str(value) if not isinstance(value, (int, float)) else value
            sample_records.append(record)
        
        # Create data context for Claude
        data_context = {
            "metrics_list": unique_metrics.tolist(),
            "client_list": unique_clients.tolist(),  # Added client list to context
            "date_range": {
                "min": min_date_str,
                "max": max_date_str
            },
            "total_records": len(df),
            "sample_data": sample_records
        }
        
        question = st.text_input("Ask your financial question:")
        
        if question and question != st.session_state.current_question:
            # Reset rating state when a new question is asked
            st.session_state.has_rated = False
            st.session_state.current_question = question
            
            # Log the question silently to Amazon S3 (without rating initially)
            question_id = log_question_and_rating_to_s3(question=question, uploaded_file_name="TestDoc")
            st.session_state.current_question_id = question_id
            print(f"Set current_question_id in session state: {question_id}")
            
            
            client = anthropic.Client(api_key=st.secrets["ANTHROPIC_API_KEY"])
            
            # Update prompt with data context
            prompt = f'''
            You are a helpful assistant knowing both finance and sql well:
            
            Question: {question}
            
            Convert the following financial question into a structured JSON query object, that will contain arguments for SQL like operational query_data().
            
            ## Question Simplification Process
            First, if the question is a temporal question (includes words like "როდის", "რომელ", etc.), mentally reformat it to a form that will require same grouping and aggregation procedures.
    
            For example:
            - "მითხარი რომელ კვარტალში იყო ჯამური შემოსავალი საქონლის მიწოდებიდან ყველაზე მეტი?" 
            - Can be processed as: "მითხარი ჯამური შემოსავალი საქონლის მიწოდებიდან კვარტლურად?"
            
            
            ## Data Overview
            {json.dumps(data_context, indent=2)}
            
            ## Available Data Structure
            The dataframe contains financial data with these key columns:
            - `client`: Client or company name (added new column)
            - `metrics`: Various types of income categories denominated in Georgian Language
            - `date`: Date of income (daily granularity)
            - `value`: Numerical amount of income
            
            ## Georgian Language Handling Instructions     
            
            ### Critical Requirements
            - The user query is in Georgian and all metrics and client names in data_context are in Georgian
            - NEVER translate metric names or client names from Georgian to English during processing

                ### Metric and Client Detection Guidelines
                - Use exact string matching between Georgian terms in queries and metrics_list/client_list entries
                - Match metrics/clients by searching for complete or partial string matches in the query
                - Always prioritize exact metric/client names from their respective lists in their original form
                - Note: The dataset concerns many types of "შემოსავლები" (revenue) - so a question mentioning "შემოსავლები" without specifying the type of "შემოსავლები" from metrics_list, is not enough enough to filter. 
                - The Question should be referring (complete or partial) to specific value from metrics_list to filter.
                - Similarly, if a client name is mentioned, it should be matched against the client_list.

                ### Query Processing Requirements
                - When a specific metric or client is mentioned in the query, ALWAYS include a "where" clause filtering for that specific metric/client
                - Implement fuzzy matching as a fallback method to identify the closest metric/client match in metrics_list/client_list when exact matching fails but the intent to query a specific metric/client is clear
                - Time-based questions (containing words like "როდის") still require metric/client filtering when specific metrics/clients are mentioned
                - Questions asking for superlatives (like "ყველაზე მეტი" or "ყველაზე დაბალი") should:
                    1. Filter for the specified metric/client
                    2. Include appropriate "order_by" clauses (descending for "მეტი"/highest, ascending for "დაბალი"/lowest)
                    3. Limit results if appropriate

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
            - Example: `{{"client": {{"=": "შპს მაიჯიპიეს 205216176"}}}}` filters for rows where client equals "შპს მაიჯიპიეს 205216176"
            - Multiple conditions can be specified as separate key-value pairs
            - The "where" should NEVER be empty when the question clearly specifies filtering criteria.
            - Especially, ALWAYS include a "where" if question refers to filtering metrics or clients, match to those provided in "metrics_list" or "client_list" - If multiple matches, include as many as relevants.
            - IMPORTANT: Do not translate metrics or client names between Georgian and English - use the exact strings from metrics_list or client_list
            - CRITICAL: When filtering client look for most exact match from unique_clients, top 1.
            
            3. **"group_by"**: List of columns to group by - Optional
            - Only group in case question asks grouping, based on data structure.
            - Example of standard groupings: `["date"]`, `["metrics"]`, `["client"]`, or combinations like `["date", "metrics"]`, `["client", "metrics"]`
            - Example of time-based groupings: `["quarter"]`, `["month"]`, `["year_only"]`, `["week"]`
            - For time period groupings:
                - When user asks for quarterly data, use EXACTLY `"quarter"` as a string in group_by, NOT SQL functions
                    - Example: `"group_by": ["quarter"]`
                - When user asks for monthly data, use EXACTLY `"month"` as a string in group_by
                    - Example: `"group_by": ["month"]`
                - When user asks for yearly data, use EXACTLY `"year_only"` as a string in group_by
                    - Example: `"group_by": ["year_only"]`
                - When user asks for weekly data, use EXACTLY `"week"` as a string in group_by
                    - Example: `"group_by": ["week"]`
            - For combining time periods with other columns (e.g., "monthly income by metrics" or "client income by month"):
                - Include both the time period and the column name in the group_by list
                - Example: `"group_by": ["month", "metrics"]` for monthly data by metrics
                - Example: `"group_by": ["month", "client"]` for monthly data by client
                - Always put time period first, then other grouping columns
            - DO NOT use SQL functions like date_trunc() or EXTRACT()
            
            4. **"aggregations"**: Dictionary defining aggregation operations - Optional
            - Key: Column to aggregate (typically `"value"`)
            - Value: List of aggregation functions (e.g., `["sum"]`, `["mean"]`, `["count"]`, or multiple like `["sum", "mean"]`)
            - Examples: 
            - `{{"value": ["sum"]}}` calculates sum of values in each group
            - `{{"value": ["mean"]}}` calculates average value in each group
            - `{{"value": ["sum", "mean"]}}` calculates both sum and average in each group
            - When a question asks for "average" or "mean", use `"mean"` as the aggregation function
            - When a question asks for "total" or "sum", use `"sum"` as the aggregation function

            5. **"order_by"**: List of arrays for sorting results - Optional
            - Only order by in case question asks ordering, based on data column.
            - Each tuple contains: (column_name, sort_direction)
            - Column names often include aggregation suffix (e.g., `"value_sum"`)
            - Sort direction: has two possible boolean values,`false` for descending, `true` for ascending
            - Example: `[["value_sum", false]]` sorts by total value in descending order
            - Example: `[["value_mean", false]]` sorts by average value in descending order

            ## Implementation Rules
            - Include any of above Optional components if and only if asked.
            - Always include `"where"` when question mentions or refers to the specific metrics or client names based on data overview or time periods
            - Use appropriate `"group_by"` based on the question's focus (by date, by metric type, by client, etc.)
            - For time period groupings:
              - When user asks for quarterly data, use `"quarter"` in group_by
              - When user asks for monthly data, use `"month"` in group_by
              - When user asks for yearly data, use `"year_only"` in group_by
              - When user asks for weekly data, use `"week"` in group_by
            - For aggregations, use `"value"` as the key and include appropriate functions (typically `["sum"]`)
            - Include `"order_by"` !only! when question mentions sorting or ranking (e.g., "highest", "lowest")
            - Dates should be formatted as "YYYY-MM-DD"
            - When the question is vague or doesn't specify filters, use the context from Data Overview to provide sensible defaults
            - Match metrics and client names exactly as they appear in the metrics_list and client_list from the data context
            - CRITICAL: When filtering client look for most exact match from unique_clients, top 1.


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
                
                st.write("### Generated JSON Query:")
                st.json(query_json)
                
                result_df = execute_query(query_json)
                
                st.write("### Query Result:")
                st.dataframe(result_df)
                
                # New section: Interpret results using Claude
                interpretation_section = st.container()
                with interpretation_section:
                    result_df = execute_query(query_json)
                    interpretation = interpret_results(result_df, question)
                        
                    st.write("### Interpretation:")                
                    st.markdown(f"<div style='background-color: transparent; padding: 20px; border-radius: 5px; font-size: 16px;'>{interpretation}</div>", unsafe_allow_html=True)

                    # In your interpretation section after displaying the result
                    st.write("### How would you rate this answer?")

                    # Create columns for the rating system
                    col1, col2, col3, col4, col5, col6 = st.columns(6)

                    # # Define rating submission function with explicit form submission
                    # def submit_rating(rating_value):
                    #     rating_str = str(rating_value)  # Ensure rating is a string
                    #     st.session_state.rating = rating_str
                    #     st.session_state.has_rated = True
                        
                    #     # Get the current question ID from session state
                    #     question_id = st.session_state.get("current_question_id")
                        
                    #     # Log the rating to S3
                    #     if question_id:
                    #         log_question_and_rating_to_s3(question_id=question_id, rating=rating_str)
                    #     else:
                    #         # Fallback to old method
                    #         log_question_and_rating_to_s3(question=st.session_state.current_question, 
                    #                                     rating=rating_str, 
                    #                                     uploaded_file_name="TestDoc")
                        
                    #     # No need for success message here as page will reload
                    
                    def submit_rating(rating_value):
                        rating_str = str(rating_value)
                        st.session_state.rating = rating_str
                        st.session_state.has_rated = True
                        
                        question_id = st.session_state.get("current_question_id")
                        
                        if question_id:
                            log_question_and_rating_to_s3(question_id=question_id, rating=rating_str)
                        else:
                            log_question_and_rating_to_s3(question=st.session_state.current_question, 
                                                        rating=rating_str, 
                                                        uploaded_file_name="TestDoc")
                        
                        # Show success message without reload
                        st.success(f"You rated this answer: {rating_str}/5. Thank you for your feedback!", icon="✅")
                    

                    # # Rating buttons - now in a form for explicit submission
                    # with st.form(key="rating_form"):
                    #     st.write("Select your rating:")
                        
                    #     # Create a horizontal layout for rating buttons
                    #     cols = st.columns(5)
                    #     with cols[0]:
                    #         rate1 = st.form_submit_button("1", disabled=st.session_state.has_rated)
                    #     with cols[1]:
                    #         rate2 = st.form_submit_button("2", disabled=st.session_state.has_rated)
                    #     with cols[2]:
                    #         rate3 = st.form_submit_button("3", disabled=st.session_state.has_rated)
                    #     with cols[3]:
                    #         rate4 = st.form_submit_button("4", disabled=st.session_state.has_rated)
                    #     with cols[4]:
                    #         rate5 = st.form_submit_button("5", disabled=st.session_state.has_rated)
                        
                    #     # Custom submission message
                    #     st.caption("Click a rating to submit your feedback")

                    # # Process button clicks
                    # if rate1:
                    #     submit_rating(1)
                    # elif rate2:
                    #     submit_rating(2)
                    # elif rate3:
                    #     submit_rating(3)
                    # elif rate4:
                    #     submit_rating(4)
                    # elif rate5:
                    #     submit_rating(5)

                    # # Show current rating if it exists
                    # if st.session_state.get('has_rated', False):
                    #     rating_value = st.session_state.get('rating', '0')
                    #     st.success(f"You rated this answer: {rating_value}/5. Thank you for your feedback!")
                    
                    # Rating buttons with callbacks
                    cols = st.columns(5)
                    with cols[0]:
                        if st.button("1", disabled=st.session_state.has_rated, key="rate1"):
                            submit_rating(1)
                    with cols[1]:
                        if st.button("2", disabled=st.session_state.has_rated, key="rate2"):
                            submit_rating(2)
                    with cols[2]:
                        if st.button("3", disabled=st.session_state.has_rated, key="rate3"):
                            submit_rating(3)
                    with cols[3]:
                        if st.button("4", disabled=st.session_state.has_rated, key="rate4"):
                            submit_rating(4)
                    with cols[4]:
                        if st.button("5", disabled=st.session_state.has_rated, key="rate5"):
                            submit_rating(5)
        
                    
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
    except Exception as e:
        st.error(f"Error loading data from S3: {str(e)}")    


if __name__ == "__main__":
    simple_finance_chat()