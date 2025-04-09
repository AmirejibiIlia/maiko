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
import sqlite3
from streamlit.components.v1 import html
from functions import interpret_results, store_question_in_session, log_to_s3, set_background_from_s3, initialize_session_state, setup_rating_callbacks, process_rating, load_data_from_s3, display_data_overview, display_rating_buttons, get_dataframe_schema, format_schema_for_prompt, generate_sql_query, execute_sql_on_dataframe, answer_question_with_sql

def simple_finance_chat():
    """Main function for the finance chat application"""
    # Initialize session state
    initialize_session_state()
    
    # Set up rating callbacks
    rating_callbacks = setup_rating_callbacks()
    
    # Process any pending rating
    process_rating()
    
    # App title and introduction
    st.title("სალამი, მე ვარ MAIA")
    st.write("დამისვი ბევრი კითხვები, რომ ბევრი ვისწავლო!")
    
    # Load data
    df = load_data_from_s3()
    if df is None:
        return
    
    # Display data overview and get metadata
    overview_data = display_data_overview(df)
    
    # User input
    question = st.text_input("Ask your financial question:")
    
    if question and question != st.session_state.get('current_question', ""):
        # Reset rating state when a new question is asked
        st.session_state.has_rated = False
        
        # Store the question in session state
        question_id = store_question_in_session(question=question, uploaded_file_name="TestDoc")
        
        # Process the question using the SQL approach
        with st.spinner("Processing your question..."):
            query_result = answer_question_with_sql(df, question)
            
            # Store the raw SQL in session state
            store_question_in_session(question=question, raw_response=query_result["sql_query"], uploaded_file_name="TestDoc")
            
            # Display SQL query (for debugging)
            with st.expander("Generated SQL Query"):
                st.code(query_result["sql_query"], language="sql")
            
            if query_result["error"]:
                st.error(f"Error executing query: {query_result['error']}")
            else:
                # Display query results
                st.write("### Query Result:")
                st.dataframe(query_result["result"])
                
                # Interpret results
                interpretation_section = st.container()
                with interpretation_section:
                    interpretation = interpret_results(query_result["result"], question)
                    
                    st.write("### Interpretation:")                
                    st.markdown(f"<div style='background-color: transparent; padding: 20px; border-radius: 5px; font-size: 16px;'>{interpretation}</div>", unsafe_allow_html=True)
                
                # Display rating buttons
                display_rating_buttons(rating_callbacks)
                
                # Log to S3
                log_to_s3()

if __name__ == "__main__":
    simple_finance_chat()