import pandas as pd
import anthropic
import streamlit as st

def simple_finance_chat():
    st.title("Simple Finance Chatbot")
    st.write("Upload a financial data file (Excel format) and ask questions about it!")

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

        # Convert data to a more readable format for prompting
        data_summary = ""
        for year in df['year'].unique():
            data_summary += f"\nYear {year}:\n"
            year_data = df[df['year'] == year]
            for _, row in year_data.iterrows():
                data_summary += f"{row['metrics']}: {row['value']}\n"

        # Display data summary
        st.write("### Data Summary")
        st.text(data_summary)

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
            1. Do not provide additional information, for example trends, comparisons, or additional analysis unless specifically requested.
            2. If asked about a specific metric in a specific year, provide just that number
            3. If asked about trends, compare numbers and calculate percentage changes
            4. If asked about highest/lowest values, specify both the year and the value
            5. Format numbers with thousand separators for better readability
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
                st.write(response["completion"])
            except Exception as e:
                st.error(f"Error: {str(e)}")

if __name__ == "__main__":
    simple_finance_chat()
