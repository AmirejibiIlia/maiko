import pandas as pd
import streamlit as st
import boto3
import io
import datetime
import uuid

def initialize_session_log():
    """Initialize the session logging DataFrame if it doesn't exist."""
    if 'log_df' not in st.session_state:
        st.session_state.log_df = pd.DataFrame(columns=[
            "session_id",
            "timestamp",
            "file_name",
            "question",
            "question_id",
            "rating",
            "session_start"
        ])
        # Generate a session ID once per session
        st.session_state.session_id = str(uuid.uuid4())
        # Record session start time
        st.session_state.session_start = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

def log_question(question, file_name="DefaultData"):
    """Log a new question to the session DataFrame."""
    # Make sure the session log is initialized
    initialize_session_log()
    
    # Generate a unique ID for this question
    question_id = str(uuid.uuid4())
    
    # Create a new row for the question
    new_row = {
        "session_id": st.session_state.session_id,
        "timestamp": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        "file_name": file_name,
        "question": question,
        "question_id": question_id,
        "rating": None,  # Rating will be added later
        "session_start": st.session_state.session_start
    }
    
    # Add to the session DataFrame
    st.session_state.log_df = pd.concat([
        st.session_state.log_df, 
        pd.DataFrame([new_row])
    ], ignore_index=True)
    
    # Store the current question ID for later rating
    st.session_state.current_question_id = question_id
    
    return question_id

def log_rating(question_id, rating):
    """Add a rating to a previously logged question."""
    if 'log_df' not in st.session_state or question_id is None:
        return False
    
    # Find the row with the matching question_id
    mask = st.session_state.log_df['question_id'] == question_id
    if not any(mask):
        return False
    
    # Update the rating
    st.session_state.log_df.loc[mask, 'rating'] = rating
    return True

def save_logs_to_s3():
    """Write the complete session log to S3."""
    if 'log_df' not in st.session_state or len(st.session_state.log_df) == 0:
        return False
    
    try:
        # Create S3 client
        s3_client = boto3.client(
            's3',
            aws_access_key_id=st.secrets["aws"]["access_key_id"],
            aws_secret_access_key=st.secrets["aws"]["secret_access_key"],
            region_name=st.secrets["aws"]["region"]
        )
        
        bucket_name = st.secrets["aws"]["bucket_name"]
        file_key = "session_logs.csv"
        
        # Try to load existing log file
        try:
            response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
            existing_df = pd.read_csv(io.StringIO(response['Body'].read().decode('utf-8')))
            
            # Combine existing logs with new session logs
            combined_df = pd.concat([existing_df, st.session_state.log_df], ignore_index=True)
        except:
            # If file doesn't exist, use only the session logs
            combined_df = st.session_state.log_df
        
        # Upload to S3
        csv_buffer = io.StringIO()
        combined_df.to_csv(csv_buffer, index=False)
        
        s3_client.put_object(
            Bucket=bucket_name,
            Key=file_key,
            Body=csv_buffer.getvalue().encode('utf-8'),
            ContentType='text/csv; charset=utf-8'
        )
        
        return True
    
    except Exception as e:
        print(f"Error saving logs to S3: {str(e)}")
        return False

# Function to check for session end and save logs
def check_session_end():
    """
    This function should be called at strategic points to check 
    if the session is ending and logs should be saved.
    """
    # In Streamlit, we can use this before an app restart
    # or when a user indicates they're done with the session
    if st.session_state.get('end_session', False):
        save_logs_to_s3()
        # Reset the flag
        st.session_state.end_session = False
        return True
    return False