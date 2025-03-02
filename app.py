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
        
#         # Convert data to a more readable format for prompting
#         data_summary = ""
#         for year in df['year'].unique():
#             data_summary += f"\nYear {year}:\n"
#             year_data = df[df['year'] == year]
#             for _, row in year_data.iterrows():
#                 data_summary += f"{row['metrics']}: {row['value']}\n"
        
#         # Display data summary
#         st.write("### Data Summary")
#         st.text(data_summary)
        
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


import pandas as pd
import anthropic
import streamlit as st
import json
import numpy as np
from datetime import datetime
import re

def clean_date_column(df):
    """Convert date strings in DD.MM.YY format to datetime objects"""
    # Copy the dataframe to avoid modifying the original
    df_cleaned = df.copy()
    
    # Check if the year column contains date-like strings
    if df_cleaned['year'].dtype == 'object':
        # Try to convert DD.MM.YY to datetime
        try:
            df_cleaned['date'] = pd.to_datetime(df_cleaned['year'], format='%d.%m.%y')
            # Extract year and month for easier filtering
            df_cleaned['year'] = df_cleaned['date'].dt.year
            df_cleaned['month'] = df_cleaned['date'].dt.month
        except Exception as e:
            st.warning(f"Could not parse dates: {str(e)}. Using original values.")
    
    return df_cleaned

def interpret_financial_query(query, available_metrics):
    """Parse financial query into structured parameters using Claude"""
    client = anthropic.Client(api_key=st.secrets["ANTHROPIC_API_KEY"])
    
    prompt = f"""
    Parse this financial data query into structured parameters.
    Available metrics: {', '.join(available_metrics)}
    
    The data contains monthly values from different dates.
    
    Query: "{query}"
    
    Return a JSON with:
    - operation: [sum, average, growth_rate, comparison, correlation, projection, etc.]
    - metrics: list of metrics mentioned
    - time_period: specified time range (specific months or date ranges)
    - filters: any conditions to apply
    - grouping: any grouping instructions (e.g. by month, by quarter)
    - advanced_parameters: any operation-specific parameters
    
    JSON:
    """
    
    try:
        response = client.messages.create(
            model="claude-3-sonnet-20240229",
            max_tokens=500,
            temperature=0,
            messages=[{"role": "user", "content": prompt}]
        )
        
        # Extract JSON from response
        content = response.content[0].text
        # Find JSON in the response (in case Claude adds explanation)
        json_start = content.find('{')
        json_end = content.rfind('}') + 1
        if json_start >= 0 and json_end > json_start:
            json_str = content[json_start:json_end]
            try:
                params = json.loads(json_str)
                return params
            except json.JSONDecodeError:
                pass
        
        # If we couldn't parse JSON, use fallback
        return fallback_parse(query, available_metrics)
    except Exception as e:
        st.error(f"Error interpreting query: {str(e)}")
        return fallback_parse(query, available_metrics)

def fallback_parse(query, available_metrics):
    """Simple fallback parser if LLM JSON parsing fails"""
    params = {
        "operation": "summary",
        "metrics": [m for m in available_metrics if m.lower() in query.lower()],
        "time_period": None,
        "filters": {},
        "grouping": None,
        "advanced_parameters": {}
    }
    
    # Try to detect months or dates in the query
    months_pattern = r'\b(January|February|March|April|May|June|July|August|September|October|November|December|Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\b'
    date_pattern = r'\b\d{1,2}[./-]\d{1,2}[./-]\d{2,4}\b'
    
    months = re.findall(months_pattern, query, re.IGNORECASE)
    dates = re.findall(date_pattern, query)
    
    if months:
        params["time_period"] = {"months": months}
    if dates:
        params["time_period"] = {"dates": dates}
        
    # Detect basic operations
    if "growth" in query.lower() or "increase" in query.lower() or "change" in query.lower():
        params["operation"] = "growth_rate"
    elif "compare" in query.lower() or "difference" in query.lower():
        params["operation"] = "comparison"
    elif "average" in query.lower() or "mean" in query.lower():
        params["operation"] = "average"
    elif "correlation" in query.lower() or "relationship" in query.lower():
        params["operation"] = "correlation"
    elif "total" in query.lower() or "sum" in query.lower():
        params["operation"] = "sum"
        
    return params

def execute_calculation(params, df):
    """Execute financial calculations based on parsed parameters"""
    # Create a copy of the dataframe to avoid modifying the original
    result_df = df.copy()
    
    # Filter by time period if specified
    if params.get('time_period'):
        time_period = params['time_period']
        if isinstance(time_period, dict):
            # Handle months
            if 'months' in time_period and isinstance(time_period['months'], list):
                month_mapping = {
                    'january': 1, 'jan': 1, 'february': 2, 'feb': 2, 'march': 3, 'mar': 3,
                    'april': 4, 'apr': 4, 'may': 5, 'june': 6, 'jun': 6, 'july': 7, 'jul': 7,
                    'august': 8, 'aug': 8, 'september': 9, 'sep': 9, 'october': 10, 'oct': 10,
                    'november': 11, 'nov': 11, 'december': 12, 'dec': 12
                }
                
                month_nums = [month_mapping.get(m.lower(), 0) for m in time_period['months']]
                result_df = result_df[result_df['month'].isin(month_nums)]
            
            # Handle dates
            if 'dates' in time_period and isinstance(time_period['dates'], list):
                # This would require more advanced date parsing logic
                pass
    
    # Initialize result dictionary
    result = {
        "operation": params.get('operation', 'summary'),
        "metrics": params.get('metrics', []),
        "time_period": params.get('time_period'),
        "calculations": {},
        "error": None
    }
    
    # Handle empty metrics
    if not params.get('metrics'):
        result["error"] = "No metrics specified in the query"
        return result
    
    # Execute the appropriate operation
    operation = params.get('operation', 'summary')
    
    try:
        if operation == 'summary':
            for metric in params['metrics']:
                metric_data = result_df[result_df['metrics'] == metric]
                if not metric_data.empty:
                    result["calculations"][metric] = {
                        "latest": metric_data.sort_values('date', ascending=False)['value'].iloc[0],
                        "average": metric_data['value'].mean(),
                        "min": metric_data['value'].min(),
                        "max": metric_data['value'].max(),
                        "months": sorted(metric_data['date'].dt.strftime('%b %Y').unique().tolist())
                    }
                else:
                    result["calculations"][metric] = {"error": f"No data found for metric: {metric}"}
        
        elif operation == 'growth_rate':
            for metric in params['metrics']:
                result["calculations"][metric] = calculate_growth_rate(
                    result_df, 
                    metric,
                    params.get('advanced_parameters', {}).get('method', 'simple')
                )
        
        elif operation == 'comparison':
            if len(params['metrics']) >= 2:
                result["calculations"]["comparison"] = compare_metrics(
                    result_df, 
                    params['metrics'],
                    params.get('advanced_parameters', {}).get('method', 'absolute')
                )
            else:
                result["calculations"]["error"] = "Need at least two metrics for comparison"
        
        elif operation == 'correlation':
            if len(params['metrics']) >= 2:
                result["calculations"]["correlation"] = calculate_correlation(
                    result_df, 
                    params['metrics'][0], 
                    params['metrics'][1]
                )
            else:
                result["calculations"]["error"] = "Need exactly two metrics for correlation"
                
        elif operation == 'average':
            for metric in params['metrics']:
                metric_data = result_df[result_df['metrics'] == metric]
                if not metric_data.empty:
                    result["calculations"][metric] = {
                        "average": metric_data['value'].mean(),
                        "months": sorted(metric_data['date'].dt.strftime('%b %Y').unique().tolist())
                    }
                else:
                    result["calculations"][metric] = {"error": f"No data found for metric: {metric}"}
                    
        elif operation == 'sum':
            for metric in params['metrics']:
                metric_data = result_df[result_df['metrics'] == metric]
                if not metric_data.empty:
                    result["calculations"][metric] = {
                        "sum": metric_data['value'].sum(),
                        "months": sorted(metric_data['date'].dt.strftime('%b %Y').unique().tolist())
                    }
                else:
                    result["calculations"][metric] = {"error": f"No data found for metric: {metric}"}
                    
    except Exception as e:
        result["error"] = f"Error in calculation: {str(e)}"
    
    return result

def calculate_growth_rate(df, metric, method='simple'):
    """Calculate growth rate for a specific metric"""
    metric_data = df[df['metrics'] == metric].sort_values('date')
    
    if len(metric_data) < 2:
        return {"error": "Insufficient data points for growth calculation"}
    
    start_value = metric_data['value'].iloc[0]
    end_value = metric_data['value'].iloc[-1]
    start_date = metric_data['date'].iloc[0]
    end_date = metric_data['date'].iloc[-1]
    
    # Validate to prevent division by zero
    if start_value == 0:
        return {"error": "Cannot calculate growth rate from zero base value"}
    
    # Calculate growth rate
    if method == 'simple':
        growth = ((end_value - start_value) / start_value)
    elif method == 'cagr':
        # Calculate CAGR (Compound Annual Growth Rate)
        days = (end_date - start_date).days
        if days == 0:
            return {"error": "Time period must be greater than zero for CAGR calculation"}
        growth = (end_value / start_value) ** (365 / days) - 1
    else:
        growth = ((end_value - start_value) / start_value)
    
    return {
        "rate": growth * 100,  # Convert to percentage
        "start_value": float(start_value),
        "end_value": float(end_value),
        "start_date": start_date.strftime('%b %Y'),
        "end_date": end_date.strftime('%b %Y'),
        "method": method
    }

def compare_metrics(df, metrics, method='absolute'):
    """Compare two or more metrics"""
    result = {}
    
    for metric in metrics:
        metric_data = df[df['metrics'] == metric].sort_values('date')
        if not metric_data.empty:
            result[metric] = {
                "latest": float(metric_data.sort_values('date', ascending=False)['value'].iloc[0]),
                "earliest": float(metric_data.sort_values('date')['value'].iloc[0]),
                "average": float(metric_data['value'].mean()),
                "months": sorted(metric_data['date'].dt.strftime('%b %Y').unique().tolist())
            }
    
    # Add comparative analysis
    if len(result) >= 2 and method == 'absolute':
        for i, metric1 in enumerate(metrics):
            if metric1 not in result:
                continue
            for metric2 in metrics[i+1:]:
                if metric2 not in result:
                    continue
                    
                # Compare latest values
                if result[metric1]["latest"] != 0:
                    result[f"{metric1}_vs_{metric2}"] = {
                        "difference": result[metric1]["latest"] - result[metric2]["latest"],
                        "percentage": (result[metric1]["latest"] - result[metric2]["latest"]) / result[metric1]["latest"] * 100
                    }
    
    return result

def calculate_correlation(df, metric1, metric2):
    """Calculate correlation between two metrics"""
    # Get data for both metrics
    df1 = df[df['metrics'] == metric1][['date', 'value']].sort_values('date')
    df2 = df[df['metrics'] == metric2][['date', 'value']].sort_values('date')
    
    # Merge data to ensure we're comparing the same timepoints
    # First, create a date index for all dates in both dataframes
    all_dates = sorted(set(df1['date'].tolist() + df2['date'].tolist()))
    
    # Create dataframes with full date ranges
    df1_full = pd.DataFrame({'date': all_dates})
    df1_full = df1_full.merge(df1, on='date', how='left').fillna(method='ffill')
    df1_full.rename(columns={'value': metric1}, inplace=True)
    
    df2_full = pd.DataFrame({'date': all_dates})
    df2_full = df2_full.merge(df2, on='date', how='left').fillna(method='ffill')
    df2_full.rename(columns={'value': metric2}, inplace=True)
    
    # Merge on date
    merged = pd.merge(df1_full, df2_full, on='date')
    
    if len(merged) < 2:
        return {"error": "Insufficient data points for correlation calculation"}
    
    try:
        # Calculate correlation
        correlation = np.corrcoef(merged[metric1], merged[metric2])[0, 1]
        return {
            "coefficient": float(correlation),
            "strength": interpret_correlation(correlation),
            "data_points": len(merged),
            "date_range": f"{merged['date'].min().strftime('%b %Y')} - {merged['date'].max().strftime('%b %Y')}"
        }
    except Exception as e:
        return {"error": f"Error calculating correlation: {str(e)}"}

def interpret_correlation(coef):
    """Interpret correlation coefficient strength"""
    abs_coef = abs(coef)
    if abs_coef < 0.3:
        return "Weak"
    elif abs_coef < 0.7:
        return "Moderate"
    else:
        return "Strong"

def format_response_in_georgian(result, query):
    """Format calculation results in Georgian language"""
    client = anthropic.Client(api_key=st.secrets["ANTHROPIC_API_KEY"])
    
    # Create a detailed explanation of the results
    explanation = json.dumps(result, indent=2, ensure_ascii=False)
    
    prompt = f"""
    მომხმარებელმა დასვა ფინანსური კითხვა: "{query}"
    
    მე გავაანალიზე მონაცემები და მივიღე შემდეგი შედეგები:
    ```
    {explanation}
    ```
    
    გთხოვთ, ამ შედეგების საფუძველზე ჩამოაყალიბოთ მოკლე და გასაგები პასუხი ქართულ ენაზე.
    პასუხში:
    1. ახსენით ძირითადი შედეგები მარტივი ენით
    2. თუ არის პროცენტული ცვლილებები, ხაზი გაუსვით მათ
    3. თუ არის შეცდომები გამოთვლებში, ახსენით რატომ
    4. არ ჩამოთვალოთ ყველა ტექნიკური დეტალი, ფოკუსირდით მთავარ ინფორმაციაზე
    
    პასუხი:
    """
    
    try:
        response = client.messages.create(
            model="claude-3-sonnet-20240229",
            max_tokens=1000,
            temperature=0,
            messages=[{"role": "user", "content": prompt}]
        )
        return response.content[0].text
    except Exception as e:
        # Fallback in case of API error
        return f"შეცდომა პასუხის ფორმირებისას: {str(e)}"

def simple_finance_chat():
    """Main Streamlit application function"""
    st.title("სალამი, მე ვარ MAIA")
    st.write("ატვირთე ფაილი და იგრიალე!")
    
    # File uploader for the Excel file
    uploaded_file = st.file_uploader("Upload your financial data Excel file", type=["xlsx"])
    
    if uploaded_file is not None:
        # Load the Excel file into a DataFrame
        try:
            df = pd.read_excel(uploaded_file)
            
            # Check if the necessary columns are present
            required_columns = {"year", "metrics", "value"}
            if not required_columns.issubset(df.columns):
                st.error(f"Your file must contain the following columns: {', '.join(required_columns)}")
                return
            
            # Clean and convert date column
            df = clean_date_column(df)
            
            # Display data summary
            st.write("### Data Summary")
            
            # Create a more structured display with the new date format
            pivot_df = df.pivot_table(index='metrics', columns='date', values='value', aggfunc='first')
            st.dataframe(pivot_df)
            
            # Input field for user question
            question = st.text_input("Ask your financial question:")
            
            if question:
                with st.spinner("Analyzing your query..."):
                    # Parse the query
                    unique_metrics = sorted(df['metrics'].unique())
                    params = interpret_financial_query(question, list(unique_metrics))
                    
                    # Show parsed parameters (optional - can be commented out)
                    with st.expander("Query understanding"):
                        st.json(params)
                    
                    # Execute calculations
                    result = execute_calculation(params, df)
                    
                    # Format response in Georgian
                    response = format_response_in_georgian(result, question)
                    
                    # Display the AI's response
                    st.write("### პასუხი:")
                    st.write(response)
                    
                    # Show raw calculation results (optional - can be commented out)
                    with st.expander("Raw calculation results"):
                        st.json(result)
                    
        except Exception as e:
            st.error(f"Error processing file: {str(e)}")

if __name__ == "__main__":
    simple_finance_chat()