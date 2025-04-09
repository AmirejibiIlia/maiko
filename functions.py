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
from prompt import create_claude_prompt

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


def query_claude(prompt):
    """Query Claude API and process the response"""
    try:
        client = anthropic.Client(api_key=st.secrets["ANTHROPIC_API_KEY"])
        
        response = client.messages.create(
            model="claude-3-sonnet-20240229",
            max_tokens=1000,
            temperature=0,
            messages=[{"role": "user", "content": prompt}]
        )
                        
        response_text = response.content[0].text.strip()
        
        # Directly parse the JSON
        try:
            query_json = json.loads(response_text)
        except json.JSONDecodeError:
            # If direct parsing fails, try a basic cleanup
            # This removes any markdown code block indicators
            cleaned_text = response_text.replace("```json", "").replace("```", "").strip()
            query_json = json.loads(cleaned_text)
            
        return response_text, query_json
    except Exception as e:
        st.error(f"Error querying Claude: {str(e)}")
        return None, None

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

def process_and_display_results(df, query_json, question):
    """Process the query and display results"""
    # Fix order_by format if needed
    if "order_by" in query_json and query_json["order_by"]:
        query_json["order_by"] = [[item[0], item[1]] if isinstance(item, tuple) else item for item in query_json["order_by"]]
    
    st.write("### Generated JSON Query:")
    st.json(query_json)
    
    # Execute query
    result_df = execute_query(query_json)
    
    st.write("### Query Result:")
    st.dataframe(result_df)
    
    # Interpret results
    interpretation_section = st.container()
    with interpretation_section:
        interpretation = interpret_results(result_df, question)
        
        st.write("### Interpretation:")                
        st.markdown(f"<div style='background-color: transparent; padding: 20px; border-radius: 5px; font-size: 16px;'>{interpretation}</div>", unsafe_allow_html=True)

        # Log the question and empty rating
        log_to_s3()