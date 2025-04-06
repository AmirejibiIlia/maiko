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


# def create_claude_prompt(question, data_context):
#     """Create the prompt for Claude"""
#     prompt = f'''
#     You are a helpful assistant knowing both finance and sql well:
    
#     Question: {question}
    
#     Convert the following financial question into a structured JSON query object, that will contain arguments for SQL like operational query_data().
    
#     ## Question Simplification Process
#     First, if the question is a temporal question (includes words like "როდის", "რომელ", etc.), mentally reformat it to a form that will require same grouping and aggregation procedures.

#     For example:
#     - "მითხარი რომელ კვარტალში იყო ჯამური შემოსავალი საქონლის მიწოდებიდან ყველაზე მეტი?" 
#     - Can be processed as: "მითხარი ჯამური შემოსავალი საქონლის მიწოდებიდან კვარტლურად?"
    
    
#     ## Data Overview
#     {json.dumps(data_context, indent=2)}
    
#     ## Available Data Structure
#     The dataframe contains financial data with these key columns:
#     - `client`: Client or company name (added new column)
#     - `metrics`: Various types of income categories denominated in Georgian Language
#     - `date`: Date of income (daily granularity)
#     - `value`: Numerical amount of income
    
#     ## Georgian Language Handling Instructions     
    
#     ### Critical Requirements
#     - The user query is in Georgian and all metrics and client names in data_context are in Georgian
#     - NEVER translate metric names or client names from Georgian to English during processing

#         ### Metric and Client Detection Guidelines
#         - Use exact string matching between Georgian terms in queries and metrics_list/client_list entries
#         - Match metrics/clients by searching for complete or partial string matches in the query
#         - Always prioritize exact metric/client names from their respective lists in their original form
#         - Note: The dataset concerns many types of "შემოსავლები" (revenue) - so a question mentioning "შემოსავლები" without specifying the type of "შემოსავლები" from metrics_list, is not enough enough to filter. 
#         - The Question should be referring (complete or partial) to specific value from metrics_list to filter.
#         - Similarly, if a client name is mentioned, it should be matched against the client_list.

#         ### Query Processing Requirements
#         - When a specific metric or client is mentioned in the query, ALWAYS include a "where" clause filtering for that specific metric/client
#         - Implement fuzzy matching as a fallback method to identify the closest metric/client match in metrics_list/client_list when exact matching fails but the intent to query a specific metric/client is clear
#         - Time-based questions (containing words like "როდის") still require metric/client filtering when specific metrics/clients are mentioned
#         - Questions asking for superlatives (like "ყველაზე მეტი" or "ყველაზე დაბალი") should:
#             1. Filter for the specified metric/client
#             2. Include appropriate "order_by" clauses (descending for "მეტი"/highest, ascending for "დაბალი"/lowest)
#             3. Limit results if appropriate

#     ## Required JSON Structure
#     Your response must follow this exact format (structure only, not these example values):
#     ```json
#     {{
#         "data": "df",
#         "where": {{
#                     "column_name" : {{ }} 
#                     }}, // Empty unless filters are explicitly mentioned
#         "group_by": ["column_name"],
#         "aggregations": {{"column_name": ["aggregation_function"]}},
#         "order_by": [["column_name_with_suffix", boolean]]
#     }}
#     ```

#     ## Technical Specifications

#     1. **"data"**: Always set to `df` (the dataframe variable name) - Mandatory

#     2. **"where"**: A filtering dictionary specifying conditions - Optional
#     - Keys represent column names to filter on
#     - Values are nested dictionaries with operator-value pairs
#     - Operators include: "=", ">", "<", ">=", "<=", "!="
#     - Example: `{{"metrics": {{"=": "income from production"}}}}` filters for rows where metrics equals "income from production"
#     - Example: `{{"client": {{"=": "შპს მაიჯიპიეს 205216176"}}}}` filters for rows where client equals "შპს მაიჯიპიეს 205216176"
#     - Multiple conditions can be specified as separate key-value pairs
#     - The "where" should NEVER be empty when the question clearly specifies filtering criteria.
#     - Especially, ALWAYS include a "where" if question refers to filtering metrics or clients, match to those provided in "metrics_list" or "client_list" - If multiple matches, include as many as relevants.
#     - IMPORTANT: Do not translate metrics or client names between Georgian and English - use the exact strings from metrics_list or client_list
#     - CRITICAL: When filtering client look for most exact match from unique_clients, top 1.
    
#     3. **"group_by"**: List of columns to group by - Optional
#     - Only group in case question asks grouping, based on data structure.
#     - Example of standard groupings: `["date"]`, `["metrics"]`, `["client"]`, or combinations like `["date", "metrics"]`, `["client", "metrics"]`
#     - Example of time-based groupings: `["quarter"]`, `["month"]`, `["year_only"]`, `["week"]`
#     - For time period groupings:
#         - When user asks for quarterly data, use EXACTLY `"quarter"` as a string in group_by, NOT SQL functions
#             - Example: `"group_by": ["quarter"]`
#         - When user asks for monthly data, use EXACTLY `"month"` as a string in group_by
#             - Example: `"group_by": ["month"]`
#         - When user asks for yearly data, use EXACTLY `"year_only"` as a string in group_by
#             - Example: `"group_by": ["year_only"]`
#         - When user asks for weekly data, use EXACTLY `"week"` as a string in group_by
#             - Example: `"group_by": ["week"]`
#     - For combining time periods with other columns (e.g., "monthly income by metrics" or "client income by month"):
#         - Include both the time period and the column name in the group_by list
#         - Example: `"group_by": ["month", "metrics"]` for monthly data by metrics
#         - Example: `"group_by": ["month", "client"]` for monthly data by client
#         - Always put time period first, then other grouping columns
#     - DO NOT use SQL functions like date_trunc() or EXTRACT()
    
#     4. **"aggregations"**: Dictionary defining aggregation operations - Optional
#     - Key: Column to aggregate (typically `"value"`)
#     - Value: List of aggregation functions (e.g., `["sum"]`, `["mean"]`, `["count"]`, or multiple like `["sum", "mean"]`)
#     - Examples: 
#     - `{{"value": ["sum"]}}` calculates sum of values in each group
#     - `{{"value": ["mean"]}}` calculates average value in each group
#     - `{{"value": ["sum", "mean"]}}` calculates both sum and average in each group
#     - When a question asks for "average" or "mean", use `"mean"` as the aggregation function
#     - When a question asks for "total" or "sum", use `"sum"` as the aggregation function

#     5. **"order_by"**: List of arrays for sorting results - Optional
#     - Only order by in case question asks ordering, based on data column.
#     - Each tuple contains: (column_name, sort_direction)
#     - Column names often include aggregation suffix (e.g., `"value_sum"`)
#     - Sort direction: has two possible boolean values,`false` for descending, `true` for ascending
#     - Example: `[["value_sum", false]]` sorts by total value in descending order
#     - Example: `[["value_mean", false]]` sorts by average value in descending order

#     ## Implementation Rules
#     - Include any of above Optional components if and only if asked.
#     - Always include `"where"` when question mentions or refers to the specific metrics or client names based on data overview or time periods
#     - Use appropriate `"group_by"` based on the question's focus (by date, by metric type, by client, etc.)
#     - For time period groupings:
#       - When user asks for quarterly data, use `"quarter"` in group_by
#       - When user asks for monthly data, use `"month"` in group_by
#       - When user asks for yearly data, use `"year_only"` in group_by
#       - When user asks for weekly data, use `"week"` in group_by
#     - For aggregations, use `"value"` as the key and include appropriate functions (typically `["sum"]`)
#     - Include `"order_by"` !only! when question mentions sorting or ranking (e.g., "highest", "lowest")
#     - Dates should be formatted as "YYYY-MM-DD"
#     - When the question is vague or doesn't specify filters, use the context from Data Overview to provide sensible defaults
#     - Match metrics and client names exactly as they appear in the metrics_list and client_list from the data context
#     - CRITICAL: When filtering client look for most exact match from unique_clients, top 1.


#     VERY IMPORTANT: Return only a valid JSON object without any markdown formatting, comments, or explanations.
#     '''
    
#     return prompt


def create_claude_prompt(question, data_context):
    """
    Create a well-structured prompt for Claude to convert financial questions in Georgian to JSON query objects.
    
    Args:
        question (str): The financial question in Georgian
        data_context (dict): Dictionary containing data structure and context information
        
    Returns:
        str: Formatted prompt for Claude
    """
    # Base template for the prompt with sections
    prompt_sections = {
        "introduction": "You are a helpful assistant knowing both finance and sql well:",
        
        "task_definition": f"Question: {question}\n\nConvert the following financial question into a structured JSON query object, that will contain arguments for SQL like operational query_data().",
        
        "question_simplification": """
## Question Simplification Process
First, if the question is a temporal question (includes words like "როდის", "რომელ", etc.), mentally reformat it to a form that will require same grouping and aggregation procedures.

For example:
- "მითხარი რომელ კვარტალში იყო ჯამური შემოსავალი საქონლის მიწოდებიდან ყველაზე მეტი?" 
- Can be processed as: "მითხარი ჯამური შემოსავალი საქონლის მიწოდებიდან კვარტლურად?"
""",
        
        "data_overview": f"## Data Overview\n{json.dumps(data_context, indent=2)}",
        
        "data_structure": """
## Available Data Structure
The dataframe contains financial data with these key columns:
- `client`: Client or company name (added new column)
- `metrics`: Various types of income categories denominated in Georgian Language
- `date`: Date of income (daily granularity)
- `value`: Numerical amount of income
""",
        
        "georgian_language_handling": """
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
""",
        
        "json_structure": """
## Required JSON Structure
Your response must follow this exact format (structure only, not these example values):
```json
{
    "data": "df",
    "where": {
                "column_name" : { } 
                }, // Empty unless filters are explicitly mentioned
    "group_by": ["column_name"],
    "aggregations": {"column_name": ["aggregation_function"]},
    "order_by": [["column_name_with_suffix", boolean]]
}
```
""",
        
        "technical_specifications": """
## Technical Specifications

1. **"data"**: Always set to `df` (the dataframe variable name) - Mandatory

2. **"where"**: A filtering dictionary specifying conditions - Optional
- Keys represent column names to filter on
- Values are nested dictionaries with operator-value pairs
- Operators include: "=", ">", "<", ">=", "<=", "!="
- Example: `{"metrics": {"=": "income from production"}}` filters for rows where metrics equals "income from production"
- Example: `{"client": {"=": "შპს მაიჯიპიეს 205216176"}}` filters for rows where client equals "შპს მაიჯიპიეს 205216176"
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
- `{"value": ["sum"]}` calculates sum of values in each group
- `{"value": ["mean"]}` calculates average value in each group
- `{"value": ["sum", "mean"]}` calculates both sum and average in each group
- When a question asks for "average" or "mean", use `"mean"` as the aggregation function
- When a question asks for "total" or "sum", use `"sum"` as the aggregation function

5. **"order_by"**: List of arrays for sorting results - Optional
- Only order by in case question asks ordering, based on data column.
- Each tuple contains: (column_name, sort_direction)
- Column names often include aggregation suffix (e.g., `"value_sum"`)
- Sort direction: has two possible boolean values,`false` for descending, `true` for ascending
- Example: `[["value_sum", false]]` sorts by total value in descending order
- Example: `[["value_mean", false]]` sorts by average value in descending order
""",
        
        "implementation_rules": """
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
""",
        
        "final_instruction": "VERY IMPORTANT: Return only a valid JSON object without any markdown formatting, comments, or explanations."
    }
    
    # Combine all sections into a complete prompt
    prompt = "\n\n".join([
        prompt_sections["introduction"],
        prompt_sections["task_definition"],
        prompt_sections["question_simplification"],
        prompt_sections["data_overview"],
        prompt_sections["data_structure"],
        prompt_sections["georgian_language_handling"],
        prompt_sections["json_structure"],
        prompt_sections["technical_specifications"],
        prompt_sections["implementation_rules"],
        prompt_sections["final_instruction"]
    ])
    
    return prompt