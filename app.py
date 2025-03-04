# import pandas as pd
# import anthropic
# import streamlit as st

# def simple_finance_chat():
#     st.title("სალამი, მე ვარ MAIA")
#     st.write("ატვირთე ფაილი და იგრიალე!")
    
#     # File uploader for the Excel file
#     uploaded_file = st.file_uploader("Upload your financial data Excel file", type=["xlsx"])
    
#     if uploaded_file is not None:
#         # Load the Excel file into a DataFrame
#         df = pd.read_excel(uploaded_file)
        
#         # Check if the necessary columns are present
#         required_columns = {"year", "metrics", "value"}
#         if not required_columns.issubset(df.columns):
#             st.error(f"Your file must contain the following columns: {', '.join(required_columns)}")
#             return
        
#         # Convert value column to numeric and clean NaN values
#         df["value"] = pd.to_numeric(df["value"], errors="coerce")
#         df.dropna(subset=["value"], inplace=True)
        
#         # Aggregate data by year and metric
#         summary_df = df.groupby(["year", "metrics"])['value'].sum().reset_index()
        
#         # Format data summary in a structured way
#         data_summary = "Financial Data Summary:\n"
#         for year in summary_df["year"].unique():
#             data_summary += f"\nYear {year}:\n"
#             metrics_data = summary_df[summary_df["year"] == year]
#             for _, row in metrics_data.iterrows():
#                 formatted_value = f"{row['value']:,.2f}"  # Format with thousand separators
#                 data_summary += f"{row['metrics']}: {formatted_value}\n"
        
#         # Display data summary
#         st.write("### Data Summary")
#         st.text(data_summary)
        
#         # Debugging: Show data preview and summation check
#         st.write("### Debugging Information")
#         st.write("Raw Data Preview:")
#         st.write(df.head())
        
#         # Input field for user question
#         question = st.text_input("Ask your financial question:")
        
#         if question:
#             # Generate prompt for the AI
#             prompt = f"""
#             Here is the financial data:
#             {data_summary}
#             Question: {question}
#             Please analyze this financial data and answer the question. If the question asks for trends or comparisons,
#             express the percent changes when relevant. Answer in Georgian language.
#             Rules for answering:
#             1. **Perform all calculations with precision** – use exact arithmetic operations instead of estimations.  
#             2. **If the question involves addition, subtraction, multiplication, or division, compute the exact result.**   
#             3. Do not provide additional information, for example trends, comparisons, or additional analysis unless specifically requested.
#             4. If asked about a specific metric in a specific year, provide just that number
#             5. If asked about trends, compare numbers and calculate percentage changes
#             6. If asked about highest/lowest values, specify both the year and the value
#             7. Format numbers with thousand separators for better readability
#             """
            
#             # Initialize Claude client
#             client = anthropic.Client(api_key=st.secrets["ANTHROPIC_API_KEY"])
            
#             # Get the response from the AI
#             try:
#                 response = client.messages.create(
#                     model="claude-3-sonnet-20240229",
#                     max_tokens=1000,
#                     temperature=0,
#                     messages=[
#                         {"role": "user", "content": prompt}
#                     ]
#                 )
#                 # Display the AI's response
#                 st.write("### Response:")
#                 st.write(response.content[0].text)  # Updated to access the response correctly
#             except Exception as e:
#                 st.error(f"Error: {str(e)}")

# if __name__ == "__main__":
#     simple_finance_chat()

# #### BOTH CODE ABOVE AND BELOW WORKS FINE

# # import pandas as pd
# # import anthropic
# # import streamlit as st
# # import json
# # from datetime import datetime

# # def simple_finance_chat():
# #     st.title("სალამი, მე ვარ MAIA")
# #     st.write("ატვირთე ფაილი და იგრიალე!")
    
# #     # File uploader for the Excel file
# #     uploaded_file = st.file_uploader("Upload your financial data Excel file", type=["xlsx"])
    
# #     if uploaded_file is not None:
# #         # Load the Excel file into a DataFrame
# #         try:
# #             df = pd.read_excel(uploaded_file)
            
# #             # Check if the necessary columns are present
# #             required_columns = {"year", "metrics", "value"}
# #             if not required_columns.issubset(df.columns):
# #                 st.error(f"Your file must contain the following columns: {', '.join(required_columns)}")
# #                 return
            
# #             # Convert date strings (DD.MM.YY) to datetime objects
# #             try:
# #                 df['date'] = pd.to_datetime(df['year'], format='%d.%m.%y')
# #                 # Create month and year columns for easier filtering
# #                 df['month'] = df['date'].dt.month
# #                 df['year_num'] = df['date'].dt.year
# #             except Exception as e:
# #                 st.warning(f"Could not parse dates: {str(e)}. Using original values.")
            
# #             # Display data summary
# #             st.write("### Data Summary")
            
# #             # Show the data
# #             st.dataframe(df[['metrics', 'year', 'value']])
            
# #             # Input field for user question
# #             question = st.text_input("Ask your financial question:")
            
# #             if question:
# #                 # Simple processing
# #                 client = anthropic.Client(api_key=st.secrets["ANTHROPIC_API_KEY"])
                
# #                 # Create a data summary string
# #                 metrics_list = df['metrics'].unique().tolist()
                
# #                 # Generate a simple summary of the data
# #                 data_summary = ""
# #                 for metric in metrics_list:
# #                     metric_df = df[df['metrics'] == metric]
# #                     total = metric_df['value'].sum()
# #                     dates = [str(date) for date in metric_df['year'].tolist()]  # Convert to string
# #                     data_summary += f"{metric}: Total {total}, Dates: {', '.join(dates)}\n"
                
# #                 # Generate prompt for the AI
# #                 prompt = f"""
# #                 Here is the financial data summary:
# #                 {data_summary}
                
# #                 The data contains values for different metrics over time (dates in DD.MM.YY format).
                
# #                 Question: {question}
                
# #                 Please analyze this financial data and answer the question. 
# #                 If the question asks for sums or totals, provide those calculations.
# #                 If the question asks for trends or comparisons, express the percent changes when relevant.
# #                 Answer in Georgian language.
# #                 """
                
# #                 # Get the response from the AI
# #                 try:
# #                     response = client.messages.create(
# #                         model="claude-3-sonnet-20240229",
# #                         max_tokens=1000,
# #                         temperature=0,
# #                         messages=[
# #                             {"role": "user", "content": prompt}
# #                         ]
# #                     )
# #                     # Display the AI's response
# #                     st.write("### პასუხი:")
# #                     st.write(response.content[0].text)
# #                 except Exception as e:
# #                     st.error(f"Error: {str(e)}")
                    
# #         except Exception as e:
# #             st.error(f"Error processing file: {str(e)}")

# # if __name__ == "__main__":
# #     simple_finance_chat()


import pandas as pd
import json
import anthropic
import streamlit as st

# Importing your original data analysis functions (unchanged)
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
                # Convert value to datetime only if it's a valid date string
                if isinstance(value, str) and ('.' in value or '-' in value):  # Check if it's a date-like string
                    value = pd.to_datetime(value, format='%d.%m.%y', errors='coerce')  # Use 'coerce' to handle invalid dates
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
        data = data.groupby(group_by, as_index=False).agg(aggregations)
    
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

def generate_query_json(client, natural_language_query: str, df: pd.DataFrame) -> dict:
    """
    Use LLM to convert natural language query into a structured JSON query object.
    
    Args:
        client (anthropic.Client): Anthropic API client
        natural_language_query (str): User's natural language query
        df (pd.DataFrame): DataFrame to provide context for query generation
    
    Returns:
        dict: Structured JSON query object
    """
    # Prepare a prompt that helps the LLM understand the data structure and generate an appropriate query
    prompt = f"""
    You are an expert at converting natural language queries into structured JSON queries for financial data analysis.

    Data Columns: {', '.join(df.columns)}
    Available Metrics: {', '.join(df['metrics'].unique())}
    Available Years: {', '.join(map(str, df['year'].unique()))}

    User Query: {natural_language_query}

    Generate a JSON query object with the following structure:
    {{
        "where": {{column: {{operator: value}}}},
        "group_by": [columns to group by],
        "aggregations": {{column: [aggregation_method]}},
        "order_by": [(column, is_ascending)]
    }}

    Important rules:
    - Use '=' for exact matches
    - Use '>=' and '<=' for range comparisons
    - Only include sections relevant to the query
    - Be precise in selecting columns and operations
    """

    try:
        response = client.messages.create(
            model="claude-3-sonnet-20240229",
            max_tokens=1000,
            messages=[{"role": "user", "content": prompt}]
        )
        
        # Parse the JSON response
        query_json = json.loads(response.content[0].text)
        query_json['data'] = df  # Add the DataFrame to the query object
        
        return query_json
    
    except Exception as e:
        st.error(f"Error generating query JSON: {e}")
        return {}

def interpret_result(client, query_json: dict, result: pd.DataFrame) -> str:
    """
    Use LLM to interpret the query result in natural language.
    
    Args:
        client (anthropic.Client): Anthropic API client
        query_json (dict): Original query object
        result (pd.DataFrame): Query result
    
    Returns:
        str: Natural language interpretation of the result
    """
    prompt = f"""
    Interpret the following query results:

    Query Context: {json.dumps(query_json, indent=2)}
    Results:
    {result.to_string()}

    Provide a concise, human-readable explanation of the results.
    Highlight the key insights from the data analysis.
    """

    try:
        response = client.messages.create(
            model="claude-3-sonnet-20240229",
            max_tokens=1000,
            messages=[{"role": "user", "content": prompt}]
        )
        
        return response.content[0].text
    
    except Exception as e:
        st.error(f"Error interpreting results: {e}")
        return "Unable to interpret results."

def main():
    st.title("Financial Data Query Assistant")
    
    # Initialize Anthropic client
    client = anthropic.Client(st.secrets["ANTHROPIC_API_KEY"])

    # File uploader
    uploaded_file = st.file_uploader("Upload Financial Data", type=["xlsx"])
    
    if uploaded_file is not None:
        # Read the Excel file
        df = pd.read_excel(uploaded_file)
        
        # Ensure numeric conversion and clean data
        df["value"] = pd.to_numeric(df["value"], errors="coerce")
        df.dropna(subset=["value"], inplace=True)

        # Natural language query input
        query = st.text_input("Ask a question about your financial data:")
        
        if query:
            # Generate structured query
            query_json = generate_query_json(client, query, df)
            
            # Execute query
            result = execute_query(query_json)
            
            # Display raw results
            st.write("### Query Results")
            st.dataframe(result)
            
            # Interpret results
            interpretation = interpret_result(client, query_json, result)
            st.write("### Interpretation")
            st.write(interpretation)

if __name__ == "__main__":
    main()