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
import sqlite3


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

def store_question_in_session(question, raw_response=None, uploaded_file_name="TestDoc"):
    """
    Store question details in session state to be logged later
    """
    # Generate a unique ID for this question
    question_id = str(uuid.uuid4())
    timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    # Store all information in session state
    st.session_state.current_question = question
    st.session_state.current_question_id = question_id
    st.session_state.question_timestamp = timestamp
    st.session_state.question_file_name = uploaded_file_name
    st.session_state.has_rated = False
    
    # Store the raw response if provided
    if raw_response is not None:
        st.session_state.raw_response = str(raw_response)
    
    return question_id

def log_to_s3():
    """
    Log the question and any available rating to S3
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
        file_key = "question_logs.csv"
        
        # Load existing log data or create new DataFrame
        try:
            response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
            df = pd.read_csv(io.StringIO(response['Body'].read().decode('utf-8')))
        except Exception:
            # Create new DataFrame if file doesn't exist or other error occurs
            df = pd.DataFrame(columns=["timestamp", "file_name", "question", "rating", "question_id", "raw_response"])
        
        # Get current data from session state
        question_id = st.session_state.get("current_question_id", str(uuid.uuid4()))
        timestamp = st.session_state.get("question_timestamp", datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        question = st.session_state.get("current_question", "")
        file_name = st.session_state.get("question_file_name", "None")
        rating = st.session_state.get("rating", "")
        raw_response = st.session_state.get("raw_response", "")
        
        # Add some debug output to streamlit
        st.write(f"Debug - Current rating: {rating}", key="debug_rating")
        
        # Check if this question ID already exists in the dataframe
        if "question_id" in df.columns and question_id and df["question_id"].astype(str).eq(str(question_id)).any():
            # Update existing entry
            idx = df.index[df["question_id"].astype(str) == str(question_id)].tolist()[0]
            df.at[idx, "rating"] = rating
            # Update raw response if not already set
            if (df.at[idx, "raw_response"] == "" or pd.isna(df.at[idx, "raw_response"])) and raw_response:
                df.at[idx, "raw_response"] = raw_response
        else:
            # Create a new entry
            new_row = {
                "timestamp": timestamp,
                "file_name": file_name,
                "question": question,
                "rating": rating,
                "question_id": question_id,
                "raw_response": raw_response
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
        
        return True
    except Exception as e:
        st.error(f"Error in logging function: {str(e)}")
        return False
    
            
def submit_rating(rating_value):
    """
    Store rating in session state and trigger logging
    """
    rating_str = str(rating_value)  # Ensure rating is a string
    st.session_state.rating = rating_str
    
    # Log the question and rating to S3
    log_success = log_to_s3()
    
    if log_success:
        st.session_state.has_rated = True
        return True
    else:
        return False

    
                    
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
        

def initialize_session_state():
    """Initialize all session state variables"""
    if 'has_rated' not in st.session_state:
        st.session_state.has_rated = False
    if 'rating_submitted' not in st.session_state:
        st.session_state.rating_submitted = False
    if 'current_rating' not in st.session_state:
        st.session_state.current_rating = None
    if 'current_question' not in st.session_state:
        st.session_state.current_question = ""

def setup_rating_callbacks():
    """Define callback functions for rating buttons"""
    def set_rating(value):
        st.session_state.current_rating = value
        st.session_state.rating_submitted = True
        
    return {
        "1": lambda: set_rating("1"),
        "2": lambda: set_rating("2"),
        "3": lambda: set_rating("3"),
        "4": lambda: set_rating("4"),
        "5": lambda: set_rating("5")
    }

def process_rating():
    """Process rating submission"""
    if st.session_state.rating_submitted and not st.session_state.has_rated:
        # Set the rating in session state
        st.session_state.rating = st.session_state.current_rating
        
        # Log the rating
        log_success = log_to_s3()
        
        if log_success:
            st.session_state.has_rated = True
        
        # Reset the submission flag
        st.session_state.rating_submitted = False

def load_data_from_s3():
    """Load data from S3 and return DataFrame with required processing"""
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
            return None
        
        df["value"] = pd.to_numeric(df["value"], errors="coerce")
        df.dropna(subset=["value"], inplace=True)
        
        return df
    except Exception as e:
        st.error(f"Error loading data from S3: {str(e)}")
        return None

def display_data_overview(df):
    """Display data overview in an expander"""
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
    
    return {
        "unique_metrics": unique_metrics,
        "unique_clients": unique_clients,
        "min_date": min_date,
        "max_date": max_date
    }

def prepare_data_context(df, overview_data):
    """Prepare data context for Claude"""
    # Convert non-serializable dates to strings
    min_date_str = overview_data["min_date"].strftime('%Y-%m-%d') if hasattr(overview_data["min_date"], 'strftime') else str(overview_data["min_date"])
    max_date_str = overview_data["max_date"].strftime('%Y-%m-%d') if hasattr(overview_data["max_date"], 'strftime') else str(overview_data["max_date"])
    
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
        "metrics_list": overview_data["unique_metrics"].tolist(),
        "client_list": overview_data["unique_clients"].tolist(),
        "date_range": {
            "min": min_date_str,
            "max": max_date_str
        },
        "total_records": len(df),
        "sample_data": sample_records
    }
    
    return data_context



def display_rating_buttons(callbacks):
    """Display rating buttons or the current rating"""
    st.write("### How would you rate this answer?")
    
    if not st.session_state.get('has_rated', False):
        # Create a horizontal layout for rating buttons
        cols = st.columns(5)
        for i, col in enumerate(cols, 1):
            with col:
                st.button(str(i), key=f"rate{i}", on_click=callbacks[str(i)])
    else:
        # If already rated, show the current rating
        rating_value = st.session_state.get('rating', '0')
        st.success(f"You rated this answer: {rating_value}/5. Thank you for your feedback!")

# def get_dataframe_schema(df):
#     """Extract schema information from a dataframe with improved column classification"""
#     schema_info = []
    
#     # Identify potential client and metric columns
#     potential_client_columns = []
#     potential_metric_columns = []
    
#     # Simple heuristics to identify column types
#     for column in df.columns:
#         col_lower = column.lower()
        
#         # Client identification
#         if any(term in col_lower for term in ['client', 'customer', 'user', 'account', 'person', 'id']):
#             potential_client_columns.append(column)
        
#         # Metric identification - numerical columns with aggregation potential
#         if df[column].dtype in ['int64', 'float64', 'int32', 'float32']:
#             if not any(term in col_lower for term in ['id', 'code', 'year', 'month', 'day']):
#                 potential_metric_columns.append(column)
    
#     for column in df.columns:
#         dtype = str(df[column].dtype)
#         non_null_count = df[column].count()
#         null_count = df[column].isna().sum()
#         unique_count = df[column].nunique()
        
#         # Get sample values (handling different data types)
#         try:
#             sample_vals = df[column].dropna().head(3).tolist()
#         except:
#             sample_vals = ["[complex data]"]
        
#         # Determine column category
#         col_category = []
#         if column in potential_client_columns:
#             col_category.append("client")
#         if column in potential_metric_columns:
#             col_category.append("metric")
#         if 'datetime' in dtype or 'date' in dtype:
#             col_category.append("date")
            
#         schema_info.append({
#             "column": column,
#             "dtype": dtype,
#             "non_null_count": non_null_count,
#             "null_count": null_count,
#             "unique_count": unique_count,
#             "sample_values": sample_vals,
#             "category": col_category
#         })
    
#     return schema_info, potential_client_columns, potential_metric_columns

# def format_schema_for_prompt(schema_info, potential_client_columns, potential_metric_columns):
#     """Format schema information for the LLM prompt with column classifications"""
#     schema_text = "DataFrame Schema:\n"
    
#     # Add column classification overview
#     schema_text += "Potential client-related columns: " + ", ".join(potential_client_columns) + "\n"
#     schema_text += "Potential metric-related columns: " + ", ".join(potential_metric_columns) + "\n\n"
    
#     for col_info in schema_info:
#         schema_text += f"- Column: {col_info['column']}\n"
#         schema_text += f"  Type: {col_info['dtype']}\n"
#         schema_text += f"  Non-null values: {col_info['non_null_count']}, Null values: {col_info['null_count']}\n"
#         schema_text += f"  Unique values: {col_info['unique_count']}\n"
#         schema_text += f"  Sample values: {col_info['sample_values']}\n"
#         if col_info['category']:
#             schema_text += f"  Category: {', '.join(col_info['category'])}\n"
#         schema_text += "\n"
    
#     return schema_text

# def generate_sql_query(question, schema_text):
#     """Generate SQL query using Anthropic's Claude with improved handling for distinct values"""
#     client = anthropic.Client(api_key=st.secrets["ANTHROPIC_API_KEY"])
    
#     system_prompt = """You are an expert SQL query generator. You convert natural language questions into SQL queries.
#     Please only return the SQL query with no additional explanation, comments, or markdown formatting.
    
#     Important guidelines:
#     1. For SQLite date handling:
#        - Do NOT use EXTRACT() function
#        - Instead, use strftime('%m', date_column) to extract month
#        - Use strftime('%Y', date_column) to extract year
#        - Use strftime('%d', date_column) to extract day
#        - For counting distinct months, use: COUNT(DISTINCT strftime('%m', date_column))
    
#     2. For client-related questions:
#        - When asked about clients, use client-related columns (like client_id, client_name, etc.)
#        - For distinct client counts, use COUNT(DISTINCT client_column)
#        - Group by client columns when aggregating client-specific metrics
    
#     3. For metric-related questions:
#        - When asked about metrics, focus on numerical/metric columns
#        - Use appropriate aggregation functions (SUM, AVG, MIN, MAX) based on the question
#        - For distinct metric values, use COUNT(DISTINCT metric_column)
#        - Use HAVING clauses when filtering on aggregated values
       
#     4. For questions in Georgian language:
#        - Interpret the question and generate the appropriate SQL query
#        - Handle transliterations and fuzzy matches for column names and values
#     """
    
#     user_prompt = f"""I have a pandas DataFrame with the following schema:

# {schema_text}

# The dataframe will be converted to a SQL table named 'data'.

# Question: {question}

# Generate only the SQL query to answer this question. Return only the query itself without any explanation or comments.
# Pay special attention to distinct client values and distinct metric values when mentioned in the question.
# """

#     try:
#         response = client.messages.create(
#             model="claude-3-sonnet-20240229",
#             system=system_prompt,
#             max_tokens=1000,
#             temperature=0,  # Use 0 for deterministic responses
#             messages=[
#                 {"role": "user", "content": user_prompt}
#             ]
#         )
        
#         sql_query = response.content[0].text.strip()
#         return sql_query
    
#     except Exception as e:
#         return f"Error generating SQL query: {str(e)}"

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

# def answer_question_with_sql(df, question):
#     """Main function to answer questions using SQL on a dataframe"""
#     # Get schema info
#     schema_info, client_cols, metric_cols = get_dataframe_schema(df)
#     schema_text = format_schema_for_prompt(schema_info, client_cols, metric_cols)
    
#     # Generate SQL
#     sql_query = generate_sql_query(question, schema_text)
    
#     # Execute SQL
#     result, error = execute_sql_on_dataframe(df, sql_query)
    
#     return {
#         "question": question,
#         "sql_query": sql_query,
#         "result": result if result is not None else pd.DataFrame(),
#         "error": error
#     }


#new functions

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
    
    # Get all unique client values if client column is identified
    unique_clients = []
    if potential_client_columns and 'client' in df.columns:
        unique_clients = df['client'].dropna().unique().tolist()
    
    return schema_info, potential_client_columns, potential_metric_columns, unique_clients

def format_schema_for_prompt(schema_info, potential_client_columns, potential_metric_columns, unique_clients):
    """Format schema information for the LLM prompt with column classifications and unique clients"""
    schema_text = "DataFrame Schema:\n"
    
    # Add column classification overview
    schema_text += "Potential client-related columns: " + ", ".join(potential_client_columns) + "\n"
    schema_text += "Potential metric-related columns: " + ", ".join(potential_metric_columns) + "\n\n"
    
    # Add all unique client values
    if unique_clients:
        schema_text += "AVAILABLE CLIENTS (EXACT STRINGS FOR FILTERING):\n"
        for idx, client in enumerate(unique_clients, 1):
            schema_text += f"{idx}. '{client}'\n"
        schema_text += "\n"
    
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

def generate_sql_query(question, schema_text, unique_clients=None):
    """Generate SQL query using Anthropic's Claude with improved handling for distinct values and client matching"""
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
       - When asked about clients, use the EXACT client string provided in the AVAILABLE CLIENTS list
       - For Georgian text, match client names by looking for fuzzy matches, partial matches, or substring matches
       - When multiple potential matches exist, choose the most closely matching client
       - Use the exact client string provided in the list for filtering - do not make up client names!
       - For distinct client counts, use COUNT(DISTINCT client_column)
       - Group by client columns when aggregating client-specific metrics
    
    3. For metric-related questions:
       - When asked about metrics, focus on numerical/metric columns
       - Use appropriate aggregation functions (SUM, AVG, MIN, MAX) based on the question
       - For distinct metric values, use COUNT(DISTINCT metric_column)
       - Use HAVING clauses when filtering on aggregated values
       
    4. For questions in Georgian language:
       - Follow the exact client names in the AVAILABLE CLIENTS list
       - Do proper matching between Georgian terms in the question and available client names
       - Be careful with company names - use fuzzy matching for Georgian terms when needed
    """
    
    user_prompt = f"""I have a pandas DataFrame with the following schema:

{schema_text}

The dataframe will be converted to a SQL table named 'data'.

Question (in Georgian): {question}

Generate only the SQL query to answer this question. Return only the query itself without any explanation or comments.
Pay special attention to matching client names correctly - look for the most similar company name from the provided AVAILABLE CLIENTS list.
Your job is to find the closest matching client from the list when a company is mentioned.

For example:
- If the question asks about "პსპ ფარმა", you should match it to the most similar client in the AVAILABLE CLIENTS list
- Use LIKE or exact matches based on the available client strings
- If the client isn't in the list, consider if it's a partial/abbreviated match
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

def answer_question_with_sql(df, question):
    """Main function to answer questions using SQL on a dataframe with improved client matching"""
    # Get schema info with unique clients
    schema_info, client_cols, metric_cols, unique_clients = get_dataframe_schema(df)
    schema_text = format_schema_for_prompt(schema_info, client_cols, metric_cols, unique_clients)
    
    # Generate SQL with improved client matching
    sql_query = generate_sql_query(question, schema_text, unique_clients)
    
    # Execute SQL
    result, error = execute_sql_on_dataframe(df, sql_query)
    
    return {
        "question": question,
        "sql_query": sql_query,
        "result": result if result is not None else pd.DataFrame(),
        "error": error
    }