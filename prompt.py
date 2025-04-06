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
from typing import Dict, List, Optional, Union, Literal, Any
from pydantic import BaseModel, Field

class WhereCondition(BaseModel):
    """Represents a single condition in a WHERE clause"""
    operator: str = Field(..., description="Operator like '=', '>', '<', '>=', '<=', '!='")
    value: str = Field(..., description="Value to compare against")

class OrderByItem(BaseModel):
    """Represents a single order by specification"""
    column: str = Field(..., description="Column name to order by, often with suffix like 'value_sum'")
    ascending: bool = Field(..., description="True for ascending order, False for descending")

class QueryModel(BaseModel):
    """Model representing the expected JSON structure for financial queries"""
    data: Literal["df"] = Field("df", description="Always set to 'df' (the dataframe variable name)")
    where: Dict[str, Dict[str, str]] = Field(
        default_factory=dict,
        description="Filtering dictionary specifying conditions"
    )
    group_by: Optional[List[str]] = Field(
        None, 
        description="List of columns to group by"
    )
    aggregations: Optional[Dict[str, List[str]]] = Field(
        None,
        description="Dictionary defining aggregation operations"
    )
    order_by: Optional[List[List[Union[str, bool]]]] = Field(
        None,
        description="List of arrays for sorting results"
    )


def create_claude_prompt(question, data_context):
    """
    Create a well-structured prompt for Claude to convert financial questions in Georgian to JSON query objects.
    Includes enhanced fuzzy matching for metrics and clients.
    
    Args:
        question (str): The financial question in Georgian
        data_context (dict): Dictionary containing data structure and context information
        
    Returns:
        str: Formatted prompt for Claude
    """
    # Generate Pydantic schema as JSON schema for reference
    query_model_schema = QueryModel.model_json_schema()
    schema_str = json.dumps(query_model_schema, indent=2)
    
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
        
        "improved_matching_guidelines": """
## Enhanced Matching Guidelines for Metrics and Clients

### Critical Requirements for Metrics and Clients
- ALWAYS ANALYZE THE QUESTION FOR BOTH METRICS AND CLIENT REFERENCES INDEPENDENTLY
- When the question mentions both a specific revenue type AND a specific client, BOTH filters must be applied

### Examples:
- Question about "შემოსავალი თი ჯი ლისინგისგან" should be analyzed carefully:
  - If "შემოსავალი" with a specific type appears, filter by that metric
  - Since "თი ჯი ლისინგი" is a client reference, also filter by client "შპს თი ჯი ლიზინგი 402086924"
  - YOU MUST CHECK FOR BOTH POSSIBILITIES AND APPLY BOTH FILTERS WHEN APPROPRIATE

### Critical Requirements for Metrics
- The dataset is about "შემოსავლები" (revenues) with DIFFERENT SPECIFIC TYPES of revenue metrics
- ONLY filter for specific metrics when there's HIGH CONFIDENCE (98%+) that the question refers to a SPECIFIC metric
- CRITICAL: If the question ONLY mentions general "შემოსავლები" WITHOUT specifying a particular type, DO NOT apply ANY metrics filter
- If the user simply asks about "შემოსავლები" or "revenues" without any specific metric type, leave the metrics filter COMPLETELY OUT of the query
- Look for partial string matches that uniquely identify a specific metric from metrics_list

#### Examples of when NOT to filter by metrics:
- "მაჩვენე შემოსავლები კომპანიების ჭრილში" - NO metrics filter should be applied
- "რა არის ჯამური შემოსავლები?" - NO metrics filter should be applied
- "შემოსავლები თვეების მიხედვით" - NO metrics filter should be applied

#### Examples of when TO filter by metrics:
- "მაჩვენე შემოსავალი მომსახურების მიწოდებიდან" - Apply metrics filter
- "შემოსავალი საქონლის მიწოდებიდან რამდენია?" - Apply metrics filter

### Critical Requirements for Clients
- ONLY filter for specific clients when there's HIGH CONFIDENCE (95%+) that the question refers to a SPECIFIC client
- CRITICAL: If the question mentions "კომპანიების ჭრილში" or "companies" generally WITHOUT specifying a particular company, DO NOT apply ANY client filter
- Apply client filter ONLY when a specific company name or clear reference is made
- Client names in the data are very formal (e.g., "შპს თი ჯი ლიზინგი 402086924") but users may use informal versions
- Apply fuzzy matching for client names with these steps:
  1. Look for key identifying parts of company names (e.g., "თი ჯი ლიზინგი" for "შპს თი ჯი ლიზინგი 402086924")
  2. Ignore prefixes like "შპს", "სს", etc. and ID numbers when matching
  3. Handle slight spelling variations (e.g., "ლისინგი" vs "ლიზინგი")
  4. If multiple matches are possible, choose the client with highest string similarity

### Examples of when NOT to filter by client:
- "მაჩვენე შემოსავლები კომპანიების ჭრილში" - NO client filter should be applied
- "ჯამური შემოსავლები კომპანიების მიხედვით" - NO client filter should be applied
- "შემოსავლები ყველა კომპანიისთვის" - NO client filter should be applied

### Examples of when TO filter by client:
- "მაჩვენე შემოსავლები თი ჯი ლიზინგისთვის" - Apply client filter
- "რა არის ჯამური შემოსავალი თი ჯი ლიზინგის შემთხვევაში?" - Apply client filter
""",
        
        "json_structure": f"""
## Required JSON Structure
Your response must follow the Pydantic schema below:

{schema_str}

Example valid structure (not these example values):
```json
{{
    "data": "df",
    "where": {{
                "metrics": {{"=": "specific_metric_name"}},
                "client" : {{"=": "specific_client_name"}},
                "date": {{">=": "2023-01-01", "<=": "2023-12-31"}}
              }},
    "group_by": ["column_name"],
    "aggregations": {{"column_name": ["aggregation_function"]}},
    "order_by": [["column_name_with_suffix", false]]
}}
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
- Example: `{"client": {"=": "შპს თი ჯი ლიზინგი 402086924"}}` filters for rows where client equals "შპს თი ჯი ლიზინგი 402086924"
- CRITICAL: Multiple filters are represented as separate key-value pairs in the "where" object:
  ```json
  "where": {
    "date": {">=": "2024-01-01", "<=": "2024-12-31"},
    "metrics": {"=": "შემოსავალი საქონლის მიწოდებიდან 18%"},
    "client": {"=": "შპს თი ჯი ლიზინგი 402086924"}
  }
  ```
- The "where" clause should include all applicable filters mentioned in the question
- IMPORTANT: Do not translate metrics or client names between Georgian and English - use the exact strings from metrics_list or client_list
- CRITICAL: When filtering by client, look for the best match from unique_clients using fuzzy matching techniques

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
- CRITICAL: Always check for BOTH client and metrics references in the question INDEPENDENTLY
- For "where" clause filters:
  - When both a revenue type AND a client are mentioned, BOTH filters must be included
  - Only filter for metrics when highly confident (95%+) that a specific metric is being requested
  - For client filtering, use intelligent fuzzy matching to identify the most likely client
  - Be cautious about ambiguous terms (like "თი ჯი ლისინგისგან") that could refer to either metrics or clients
- Use appropriate `"group_by"` based on the question's focus (by date, by metric type, by client, etc.)
- For time period groupings:
  - When user asks for quarterly data, use `"quarter"` in group_by
  - When user asks for monthly data, use `"month"` in group_by
  - When user asks for yearly data, use `"year_only"` in group_by
  - When user asks for weekly data, use `"week"` in group_by
- For aggregations, use `"value"` as the key and include appropriate functions (typically `["sum"]`)
- Include `"order_by"` !only! when question mentions sorting or ranking (e.g., "highest", "lowest")
- Dates should be formatted as "YYYY-MM-DD"
- When the question is vague or doesn't specify filters, do not apply filters without high confidence
- CRITICAL: When filtering client look for most similar match from unique_clients accounting for informal naming
""",
        
        "problem_solving_approach": """
## Problem Solving Approach

For each question, follow these steps to ensure accurate interpretation:

1. **Context Analysis**: Determine ALL applicable filters from the question:
   - General revenue information (avoid specific metrics filters)
   - A specific metric (apply metrics filter with high confidence)
   - A specific client (apply client filter using fuzzy matching)
   - A time period (apply time-based grouping)
   - APPLY ALL APPLICABLE FILTERS SIMULTANEOUSLY


2. **Metrics AND Client Identification**:
   - ALWAYS check for both metrics AND clients independently in each question
   - When the question contains "შემოსავლები" with a specific type (e.g., "ტექნიკური მომსახურებიდან") AND a client reference:
     - Apply BOTH the metrics filter AND the client filter
   - Example: "მაჩვენე შემოსავლები ტექნიკური მომსახურებიდან ჯამურად თი სი ლიზინგისგან"
     - Filter by metrics: "შემოსავლები ტექნიკური მომსახურებიდან"
     - Filter by client: "შპს თი ჯი ლიზინგი 402086924" (matching "თი სი ლიზინგი")

3. **Client Name Identification**:
   - Extract key identifying parts of potential client names
   - Match against unique_clients while ignoring prefixes and IDs
   - Use similarity scoring to find the best match
   - Return exact full client name from data for filtering

4. **Confidence Check**:
   - Only apply specific filters when confidence is high (95%+)
   - For ambiguous queries, prefer broader results (no filter) over potentially incorrect filters
   - ALWAYS apply multiple filters when multiple filtering criteria are identified
   
5. **Metrics Filter Rule - CRITICAL**:
   - When a question only mentions generic "შემოსავლები" WITHOUT a specific type of 99 percent similar to one from metrics_list, DO NOT include any metrics filter
   - Only include a metrics filter when a SPECIFIC type of revenue is clearly specified in terms of metrics_list
   - Example: "მაჩვენე შემოსავლები კომპანიების ჭრილში" should NOT have any metrics filter
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
        prompt_sections["improved_matching_guidelines"],
        prompt_sections["json_structure"],
        prompt_sections["technical_specifications"],
        prompt_sections["implementation_rules"],
        prompt_sections["problem_solving_approach"],
        prompt_sections["final_instruction"]
    ])
    
    return prompt