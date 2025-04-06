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
from functions import query_data,apply_where,group_and_aggregate,apply_order_by,execute_query,extract_json_from_text,interpret_results,load_excel_from_s3,store_question_in_session,log_to_s3,submit_rating,set_background_from_s3
          

def simple_finance_chat():
    # # Set the background image at the beginning
    # set_background_from_s3()
    
    st.title("სალამი, მე ვარ MAIA - Demo")
    st.write("დამისვი ბევრი კითხვები, რომ ბევრი ვისწავლო!")
    
    # Initialize session state variables for tracking ratings
    if 'has_rated' not in st.session_state:
        st.session_state.has_rated = False
    if 'rating_submitted' not in st.session_state:
        st.session_state.rating_submitted = False
    if 'current_rating' not in st.session_state:
        st.session_state.current_rating = None
    # Add this line to initialize current_question
    if 'current_question' not in st.session_state:
        st.session_state.current_question = ""
        
    # Define callback functions for each rating button
    def set_rating_1():
        st.session_state.current_rating = "1"
        st.session_state.rating_submitted = True
        
    def set_rating_2():
        st.session_state.current_rating = "2"
        st.session_state.rating_submitted = True
        
    def set_rating_3():
        st.session_state.current_rating = "3"
        st.session_state.rating_submitted = True
        
    def set_rating_4():
        st.session_state.current_rating = "4"
        st.session_state.rating_submitted = True
        
    def set_rating_5():
        st.session_state.current_rating = "5"
        st.session_state.rating_submitted = True
    
    # Process rating submission if needed
    if st.session_state.rating_submitted and not st.session_state.has_rated:
        # Set the rating in session state
        st.session_state.rating = st.session_state.current_rating
        
        # Log the rating
        log_success = log_to_s3()
        
        if log_success:
            st.session_state.has_rated = True
        
        # Reset the submission flag
        st.session_state.rating_submitted = False

    
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
        
        # if question and question != st.session_state.current_question:
        if question and question != st.session_state.get('current_question', ""):
            # Reset rating state when a new question is asked
            st.session_state.has_rated = False
            # Store the question in session state first, don't log it yet
            question_id = store_question_in_session(question=question, uploaded_file_name="TestDoc")
            
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
                
                # Store the raw response text in session state
                store_question_in_session(question=question, raw_response=response_text, uploaded_file_name="TestDoc") 
                
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
                    
                # # Now that we have the JSON, update the session state with it
                # # Create a copy of the query_json without the DataFrame to store in logs
                # log_json = {k: v for k, v in query_json.items() if k != "data"}
                # store_question_in_session(question=question, query_json=log_json, uploaded_file_name="TestDoc")
                    
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

                    # Log the question and empty rating now that we have a complete response
                    # This ensures we log the question even if user doesn't rate it
                    log_to_s3()

                    # Modify your rating form implementation to:

                    # Show rating buttons or the current rating
                    st.write("### How would you rate this answer?")
                    
                    if not st.session_state.get('has_rated', False):
                        # Create a horizontal layout for rating buttons
                        cols = st.columns(5)
                        with cols[0]:
                            st.button("1", key="rate1", on_click=set_rating_1)
                        with cols[1]:
                            st.button("2", key="rate2", on_click=set_rating_2)
                        with cols[2]:
                            st.button("3", key="rate3", on_click=set_rating_3)
                        with cols[3]:
                            st.button("4", key="rate4", on_click=set_rating_4)
                        with cols[4]:
                            st.button("5", key="rate5", on_click=set_rating_5)
                    else:
                        # If already rated, show the current rating
                        rating_value = st.session_state.get('rating', '0')
                        st.success(f"You rated this answer: {rating_value}/5. Thank you for your feedback!")
        
                    
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