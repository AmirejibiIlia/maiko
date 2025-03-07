# # import pandas as pd
# # import anthropic
# # import streamlit as st

# # def simple_finance_chat():
# #     st.title("სალამი, მე ვარ MAIA")
# #     st.write("ატვირთე ფაილი და იგრიალე!")
    
# #     # File uploader for the Excel file
# #     uploaded_file = st.file_uploader("Upload your financial data Excel file", type=["xlsx"])
    
# #     if uploaded_file is not None:
# #         # Load the Excel file into a DataFrame
# #         df = pd.read_excel(uploaded_file)
        
# #         # Check if the necessary columns are present
# #         required_columns = {"year", "metrics", "value"}
# #         if not required_columns.issubset(df.columns):
# #             st.error(f"Your file must contain the following columns: {', '.join(required_columns)}")
# #             return
        
# #         # Convert value column to numeric and clean NaN values
# #         df["value"] = pd.to_numeric(df["value"], errors="coerce")
# #         df.dropna(subset=["value"], inplace=True)
        
# #         # Aggregate data by year and metric
# #         summary_df = df.groupby(["year", "metrics"])['value'].sum().reset_index()
        
# #         # Format data summary in a structured way
# #         data_summary = "Financial Data Summary:\n"
# #         for year in summary_df["year"].unique():
# #             data_summary += f"\nYear {year}:\n"
# #             metrics_data = summary_df[summary_df["year"] == year]
# #             for _, row in metrics_data.iterrows():
# #                 formatted_value = f"{row['value']:,.2f}"  # Format with thousand separators
# #                 data_summary += f"{row['metrics']}: {formatted_value}\n"
        
# #         # Display data summary
# #         st.write("### Data Summary")
# #         st.text(data_summary)
        
# #         # Debugging: Show data preview and summation check
# #         st.write("### Debugging Information")
# #         st.write("Raw Data Preview:")
# #         st.write(df.head())
        
# #         # Input field for user question
# #         question = st.text_input("Ask your financial question:")
        
# #         if question:
# #             # Generate prompt for the AI
# #             prompt = f"""
# #             Here is the financial data:
# #             {data_summary}
# #             Question: {question}
# #             Please analyze this financial data and answer the question. If the question asks for trends or comparisons,
# #             express the percent changes when relevant. Answer in Georgian language.
# #             Rules for answering:
# #             1. **Perform all calculations with precision** – use exact arithmetic operations instead of estimations.  
# #             2. **If the question involves addition, subtraction, multiplication, or division, compute the exact result.**   
# #             3. Do not provide additional information, for example trends, comparisons, or additional analysis unless specifically requested.
# #             4. If asked about a specific metric in a specific year, provide just that number
# #             5. If asked about trends, compare numbers and calculate percentage changes
# #             6. If asked about highest/lowest values, specify both the year and the value
# #             7. Format numbers with thousand separators for better readability
# #             """
            
# #             # Initialize Claude client
# #             client = anthropic.Client(api_key=st.secrets["ANTHROPIC_API_KEY"])
            
# #             # Get the response from the AI
# #             try:
# #                 response = client.messages.create(
# #                     model="claude-3-sonnet-20240229",
# #                     max_tokens=1000,
# #                     temperature=0,
# #                     messages=[
# #                         {"role": "user", "content": prompt}
# #                     ]
# #                 )
# #                 # Display the AI's response
# #                 st.write("### Response:")
# #                 st.write(response.content[0].text)  # Updated to access the response correctly
# #             except Exception as e:
# #                 st.error(f"Error: {str(e)}")

# # if __name__ == "__main__":
# #     simple_finance_chat()

# # # #### BOTH CODE ABOVE AND BELOW WORKS FINE

# import pandas as pd
# import streamlit as st
# import anthropic

# def query_data(data: pd.DataFrame, where: dict = None, group_by: list = None, 
#                aggregations: dict = None, order_by: list = None) -> pd.DataFrame:
#     """Main function that ties together all the SQL-like operations."""
#     # Convert 'year' column to datetime if not already
#     if 'year' in data.columns and not pd.api.types.is_datetime64_any_dtype(data['year']):
#         data['year'] = pd.to_datetime(data['year'], format='%d.%m.%y')
    
#     # Apply operations step by step
#     data = apply_where(data, where)
#     data = group_and_aggregate(data, group_by, aggregations)
#     data = apply_order_by(data, order_by)
    
#     return data

# def apply_where(data: pd.DataFrame, where: dict) -> pd.DataFrame:
#     """Applies WHERE conditions to the DataFrame."""
#     if where:
#         for column, condition in where.items():
#             for operator, value in condition.items():
#                 # Convert value to datetime only if it's a valid date string
#                 if isinstance(value, str) and ('.' in value or '-' in value):  # Check if it's a date-like string
#                     value = pd.to_datetime(value, format='%d.%m.%y', errors='coerce')  # Use 'coerce' to handle invalid dates
#                 if operator == '>=':
#                     data = data[data[column] >= value]
#                 elif operator == '<':
#                     data = data[data[column] < value]
#                 elif operator == '=':
#                     data = data[data[column] == value]
#                 elif operator == '!=':
#                     data = data[data[column] != value]
#                 elif operator == '<=':
#                     data = data[data[column] <= value]
#                 elif operator == '>':
#                     data = data[data[column] > value]
#     return data

# def group_and_aggregate(data: pd.DataFrame, group_by: list, aggregations: dict) -> pd.DataFrame:
#     """Groups the DataFrame by specified columns and applies aggregations, renaming the resulting columns."""
#     if group_by:
#         data = data.groupby(group_by, as_index=False).agg(aggregations)
    
#     # Rename columns to match SQL-style SELECT aliases
#     new_columns = []
#     for col in data.columns:
#         if isinstance(col, tuple):  # Multi-index columns from aggregation
#             new_columns.append(f"{col[0]}_{col[1]}")  # Example: 'Sales_sum'
#         else:
#             new_columns.append(col)  # Keep original column name

#     data.columns = new_columns  # Assign new column names
#     return data

# def apply_order_by(data: pd.DataFrame, order_by: list) -> pd.DataFrame:
#     """Applies ORDER BY sorting to the DataFrame."""
#     if order_by:
#         for col, ascending in order_by:
#             if isinstance(data, pd.DataFrame):  # Check if it's a DataFrame
#                 data = data.sort_values(by=col, ascending=ascending)
#     return data

# def execute_query(query_json: dict) -> pd.DataFrame:
#     """
#     Executes the query by receiving the general JSON-like object with all query parameters.
#     The query_json structure should include 'data', 'where', 'group_by', 'aggregations', and 'order_by'.
#     """
#     # Extract parameters from the provided JSON object
#     data = query_json.get("data")
#     where = query_json.get("where")
#     group_by = query_json.get("group_by")
#     aggregations = query_json.get("aggregations")
#     order_by = query_json.get("order_by")
    
#     # Call query_data with the parameters extracted from the JSON-like object
#     result = query_data(data, 
#                         where=where, 
#                         group_by=group_by, 
#                         aggregations=aggregations, 
#                         order_by=order_by)
    
#     return result

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
#             #### Old prompt
#             # prompt = f"""
#             # Convert the following financial question into a structured JSON query:
#             # Question: {question}. 
            
#             # Example response format:
#             # {{
#             #     "data": df,
#             #     "where": {{ "metrics": {{ "=": "income from service" }} }},
#             #     "group_by": ["metrics"],
#             #     "aggregations": {{ "value": ["sum"] }},
#             #     "order_by": [("value_sum", False)]
#             # }}
#             # """
#             #### New prompt
#             prompt = f"""
#             Convert the following financial question into a structured JSON query.
#             Here is the structure of df you should build the JSON query for:
#             1) metrics - numerous types of income
#             2) year - date of income, daily
#             3) value - amount of income

#             Ensure the JSON follows **exactly** this format:

#             Example:
        
#             {
#                 "data": df,
#                 "where": { "metrics": { "=": "income from production" } },
#                 "group_by": ["metrics"],
#                 "aggregations": { "value": ["sum"] },
#                 "order_by": [("value_sum", False)]
#             }

#             Important Rules:

#             Take into account that the data consists of daily incomes of various metrics.
#             1. Always include `"where"` if the question contains a filter.
#             2. Use `"group_by"` if needed. `"group_by"` should match the relevant metric, like `["metrics"]`.
#             3. `"aggregations"` must be a dictionary where the key is always `"value"`, and the corresponding value must be an array containing `"sum"`, like `"aggregations": { "value": ["sum"] }`.
#             4. `"order_by"` should contain tuples like `[("value_sum", False)]` if sorting is needed.

#             Now, generate a JSON query for the following question:
#             Question: {question}

#             Return **only** the JSON output, without explanations.
#             """
        
            
#             try:
#                 response = client.messages.create(
#                     model="claude-3-sonnet-20240229",
#                     max_tokens=1000,
#                     temperature=0,
#                     messages=[{"role": "user", "content": prompt}]
#                 )
                
#                 query_json = eval(response.content[0].text)
#                 # query_json["data"] = df.to_dict(orient="records")  # Inject financial data
#                 query_json["data"] = df
                
#                 st.write("### Your Question:")
#                 st.write(question)
                
#                 st.write("### Generated JSON Query:")
#                 st.json(query_json)
                
#                 result_df = execute_query(query_json)
                
#                 st.write("### Query Result:")
#                 st.dataframe(result_df)
                
#             except Exception as e:
#                 st.error(f"Error: {str(e)}")

# if __name__ == "__main__":
#     simple_finance_chat()


import pandas as pd
import streamlit as st
import anthropic
import json

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
        
        question = st.text_input("Ask your financial question:")
        
        if question:
            client = anthropic.Client(api_key=st.secrets["ANTHROPIC_API_KEY"])
            
            # prompt = f"""
            # Convert the following financial question into a structured JSON query.
            # Here is the structure of df you should build the JSON query for:
            # 1) metrics - numerous types of income
            # 2) year - date of income, daily
            # 3) value - amount of income

            # Ensure the JSON follows **exactly** this format:

            # {{
            #     "data": "df",
            #     "where": {{ "metrics": {{ "=": "income from production" }} }},
            #     "group_by": ["metrics"],
            #     "aggregations": {{ "value": ["sum"] }},
            #     "order_by": [["value_sum", false]]
            # }}

            # Important Rules:

            # Take into account that the data consists of daily incomes of various metrics.
            # 1. Always include `"where"` if the question contains a filter.
            # 2. Use `"group_by"` if needed. `"group_by"` should match the relevant metric, like `["metrics"]`.
            # 3. `"aggregations"` must be a dictionary where the key is always `"value"`, and the corresponding value must be an array containing `"sum"`, like `"aggregations": {{ "value": ["sum"] }}`.
            # 4. `"order_by"` should be a list of lists like `[["value_sum", false]]` if sorting is needed.

            # Now, generate a JSON query for the following question:
            # Question: {question}

            # Return **only** the JSON output, without explanations.
            # """
            prompt = '''
            Convert the following financial question into a structured JSON query object.

            ## Available Data Structure
            The dataframe contains financial data with these key columns:
            - `metrics`: Various types of income categories
            - `year`: Date of income (daily granularity)
            - `value`: Numerical amount of income

            ## Required JSON Structure
            Your response must follow this exact format:
            ```json
            {
                "data": df,
                "where": {
                    "metrics": {"=": "income from production"}
                },
                "group_by": ["year"],
                "aggregations": {"value": ["sum"]},
                "order_by": [("value_sum", false)]
            }
            ```

            ## Technical Specifications

            1. **"data"**: Always set to `df` (the dataframe variable name)

            2. **"where"**: A filtering dictionary specifying conditions
            - Keys represent column names to filter on
            - Values are nested dictionaries with operator-value pairs
            - Operators include: "=", ">", "<", ">=", "<=", "!="
            - Example: `{"metrics": {"=": "income from production"}}` filters for rows where metrics equals "income from production"
            - Multiple conditions can be specified as separate key-value pairs

            3. **"group_by"**: List of columns to group by
            - To group question should ask grouping
            - Example of groupings: `["year"]`, `["metrics"]`, or `["year", "metrics"]`
            

            4. **"aggregations"**: Dictionary defining aggregation operations
            - Key: Column to aggregate (typically `"value"`)
            - Value: List of aggregation functions (typically `["sum"]`)
            - Example: `{"value": ["sum"]}` calculates sum of values in each group

            5. **"order_by"**: List of tuples for sorting results
            - Each tuple contains: (column_name, sort_direction)
            - Column names often include aggregation suffix (e.g., `"value_sum"`)
            - Sort direction: has two possible boolean values, with first letter upper case - `False` for descending, `True` for ascending
            - Example: `[("value_sum", False)]` sorts by total value in descending order

            ## Implementation Rules
            - Always include `"where"` when question mentions specific metrics or time periods
            - Use appropriate `"group_by"` based on the question's focus (by year, by metric type, etc.)
            - For aggregations, use `"value"` as the key and include appropriate functions (typically `["sum"]`)
            - Include `"order_by"` when question mentions sorting or ranking (e.g., "highest", "lowest")
            - Dates should be formatted as "YYYY-MM-DD"

            Question: {question}

            Return only the JSON object, without explanations.
            
            '''
            try:
                response = client.messages.create(
                    model="claude-3-sonnet-20240229",
                    max_tokens=1000,
                    temperature=0,
                    messages=[{"role": "user", "content": prompt}]
                )
                
                # Use json.loads instead of eval to safely parse the JSON
                query_json_text = response.content[0].text.strip()
                # Remove any non-JSON text (in case Claude adds explanations)
                if query_json_text.startswith("```json"):
                    query_json_text = query_json_text.split("```json")[1]
                if query_json_text.endswith("```"):
                    query_json_text = query_json_text.split("```")[0]
                
                query_json = json.loads(query_json_text)
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

if __name__ == "__main__":
    simple_finance_chat()