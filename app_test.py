import pandas as pd
import anthropic
import streamlit as st

def simple_finance_chat():
    st.title("Finance Chat with Claude")

    # File uploader
    uploaded_file = st.file_uploader("Upload your Excel file", type=["xlsx"])
    if uploaded_file:
        # Load data
        df = pd.read_excel(uploaded_file)

        # Display dataframe preview
        st.subheader("Preview of Uploaded Data")
        st.dataframe(df)

        # Convert data to a more readable format for prompting
        data_summary = ""
        for year in df['year'].unique():
            data_summary += f"\nYear {year}:\n"
            year_data = df[df['year'] == year]
            for _, row in year_data.iterrows():
                data_summary += f"{row['metrics']}: {row['value']}\n"

        # Question input
        question = st.text_input("Ask your question:")
        if question:
            # Claude API key (ensure to replace `MY_API_KEY` with a secret key management system)
            api_key = st.secrets["anthropic_api_key"]
            client = anthropic.Client(api_key=api_key)

            prompt = f"""
            Here is the financial data:
            {data_summary}
            Question: {question}
            Please analyze this financial data and answer the question. If the question asks for trends or comparisons,
            express the percent changes when relevant. Answer in Georgian language.
            Rules for answering:
            1. Do not provide additional information, for example trends, comparisons, or additional analysis unless specifically requested.
            2. If asked about a specific metric in a specific year, provide just that number.
            3. If asked about trends, compare numbers and calculate percentage changes.
            4. If asked about highest/lowest values, specify both the year and the value.
            5. Format numbers with thousand separators for better readability.
            """

            try:
                response = client.messages.create(
                    model="claude-3-sonnet-20240229",
                    max_tokens=1000,
                    temperature=0,
                    messages=[{"role": "user", "content": prompt}]
                )
                
                # Debugging: Print the entire response to inspect its structure
                st.write(response)

                # Check response structure (assuming the response structure is a dictionary with a 'content' key)
                if 'content' in response:
                    st.subheader("Response from Claude")
                    st.write(response['content'])
                else:
                    st.error("Response does not contain expected 'content' field.")

            except Exception as e:
                st.error(f"Error: {str(e)}")

if __name__ == "__main__":
    simple_finance_chat()
