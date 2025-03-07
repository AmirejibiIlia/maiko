import pandas as pd
import anthropic
import streamlit as st

def simple_finance_chat():
    st.title("სალამი, მე ვარ MAIA")
    st.write("ატვირთე ფაილი და იგრიალე!")
    
    # File uploader for the Excel file
    uploaded_file = st.file_uploader("Upload your financial data Excel file", type=["xlsx"])
    
    if uploaded_file is not None:
        # Load the Excel file into a DataFrame
        df = pd.read_excel(uploaded_file)
        
        # Check if the necessary columns are present
        required_columns = {"year", "metrics", "value"}
        if not required_columns.issubset(df.columns):
            st.error(f"Your file must contain the following columns: {', '.join(required_columns)}")
            return
        
        # Convert value column to numeric and clean NaN values
        df["value"] = pd.to_numeric(df["value"], errors="coerce")
        df.dropna(subset=["value"], inplace=True)
        
        # Aggregate data by year and metric
        summary_df = df.groupby(["year", "metrics"])['value'].sum().reset_index()
        
        # Format data summary in a structured way
        data_summary = "Financial Data Summary:\n"
        for year in summary_df["year"].unique():
            data_summary += f"\nYear {year}:\n"
            metrics_data = summary_df[summary_df["year"] == year]
            for _, row in metrics_data.iterrows():
                formatted_value = f"{row['value']:,.2f}"  # Format with thousand separators
                data_summary += f"{row['metrics']}: {formatted_value}\n"
        
        # Display data summary
        st.write("### Data Summary")
        st.text(data_summary)
        
        # Debugging: Show data preview and summation check
        st.write("### Debugging Information")
        st.write("Raw Data Preview:")
        st.write(df.head())
        
        # Input field for user question
        question = st.text_input("Ask your financial question:")
        
        if question:
            # Generate prompt for the AI
            prompt = f"""
            Here is the financial data:
            {data_summary}
            Question: {question}
            Please analyze this financial data and answer the question. If the question asks for trends or comparisons,
            express the percent changes when relevant. Answer in Georgian language.
            Rules for answering:
            1. **Perform all calculations with precision** – use exact arithmetic operations instead of estimations.  
            2. **If the question involves addition, subtraction, multiplication, or division, compute the exact result.**   
            3. Do not provide additional information, for example trends, comparisons, or additional analysis unless specifically requested.
            4. If asked about a specific metric in a specific year, provide just that number
            5. If asked about trends, compare numbers and calculate percentage changes
            6. If asked about highest/lowest values, specify both the year and the value
            7. Format numbers with thousand separators for better readability
            """
            
            # Initialize Claude client
            client = anthropic.Client(api_key=st.secrets["ANTHROPIC_API_KEY"])
            
            # Get the response from the AI
            try:
                response = client.messages.create(
                    model="claude-3-sonnet-20240229",
                    max_tokens=1000,
                    temperature=0,
                    messages=[
                        {"role": "user", "content": prompt}
                    ]
                )
                # Display the AI's response
                st.write("### Response:")
                st.write(response.content[0].text)  # Updated to access the response correctly
            except Exception as e:
                st.error(f"Error: {str(e)}")

if __name__ == "__main__":
    simple_finance_chat()
