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
from functions import query_data, apply_where, group_and_aggregate, apply_order_by, execute_query, extract_json_from_text, interpret_results, load_excel_from_s3, store_question_in_session, log_to_s3, submit_rating, set_background_from_s3,initialize_session_state,setup_rating_callbacks,process_rating,load_data_from_s3,display_data_overview,prepare_data_context,create_claude_prompt,query_claude,display_rating_buttons,process_and_display_results


def simple_finance_chat():
    """Main function for the finance chat application"""
    #     # # Set the background image at the beginning
    #     # set_background_from_s3()

    # Initialize session state
    initialize_session_state()
    
    # Set up rating callbacks
    rating_callbacks = setup_rating_callbacks()
    
    # Process any pending rating
    process_rating()
    
    # App title and introduction
    st.title("სალამი, მე ვარ MAIA - Demo")
    st.write("დამისვი ბევრი კითხვები, რომ ბევრი ვისწავლო!")
    
    # Load data
    df = load_data_from_s3()
    if df is None:
        return
    
    # Display data overview and get metadata
    overview_data = display_data_overview(df)
    
    # Prepare data context for Claude
    data_context = prepare_data_context(df, overview_data)
    
    # User input
    question = st.text_input("Ask your financial question:")
    
    if question and question != st.session_state.get('current_question', ""):
        # Reset rating state when a new question is asked
        st.session_state.has_rated = False
        
        # Store the question in session state
        question_id = store_question_in_session(question=question, uploaded_file_name="TestDoc")
        
        # Create prompt for Claude
        prompt = create_claude_prompt(question, data_context)
        
        # Query Claude
        response_text, query_json = query_claude(prompt)
        
        if response_text and query_json:
            # Store the raw response in session state
            store_question_in_session(question=question, raw_response=response_text, uploaded_file_name="TestDoc")
            
            # Display raw response (for debugging)
            st.write("### Raw Response from Claude:")
            st.write(response_text)
            
            # Add DataFrame to query JSON
            query_json["data"] = df
            
            # Process and display results
            process_and_display_results(df, query_json, question)
            
            # Display rating buttons
            display_rating_buttons(rating_callbacks)

if __name__ == "__main__":
    simple_finance_chat()