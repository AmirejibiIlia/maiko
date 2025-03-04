import pandas as pd
import numpy as np
from typing import List, Dict, Callable, Union, Any

def sql_query(
    table: pd.DataFrame,
    select: List[Union[str, Dict[str, Callable]]],
    where: Dict[str, Any] = None,
    group_by: List[str] = None,
    order_by: List[Dict[str, bool]] = None,
    limit: int = None
) -> pd.DataFrame:
    """
    A SQL-like function for querying pandas DataFrames.
    
    Parameters:
    -----------
    table : pd.DataFrame
        The input DataFrame to query
    select : List[Union[str, Dict[str, Callable]]]
        Columns to select. Can include aggregation functions as dicts like {"max": "column_name"}
        or simple column names as strings
    where : Dict[str, Any], optional
        Filtering conditions with column names as keys and values to filter by
    group_by : List[str], optional
        Columns to group by
    order_by : List[Dict[str, bool]], optional
        Columns to sort by with direction. Dict key is column name, value is True for ascending, False for descending
    limit : int, optional
        Number of rows to return
    
    Returns:
    --------
    pd.DataFrame
        The resulting DataFrame after applying the query
    
    Examples:
    ---------
    >>> df = pd.DataFrame({
    ...     "name": ["Alice", "Bob", "Charlie", "Alice"],
    ...     "age": [25, 30, 35, 25],
    ...     "salary": [50000, 60000, 70000, 55000]
    ... })
    >>> sql_query(
    ...     table=df,
    ...     select=["name", {"avg": "salary"}, {"max": "age"}],
    ...     where={"age": 25},
    ...     group_by=["name"]
    ... )
    """
    # Make a copy of the original table
    result = table.copy()
    
    # Apply WHERE conditions
    if where:
        for column, value in where.items():
            if isinstance(value, list):
                result = result[result[column].isin(value)]
            elif callable(value):
                result = result[value(result[column])]
            else:
                result = result[result[column] == value]
    
    # Process SELECT with GROUP BY
    if group_by:
        agg_dict = {}
        simple_cols = []
        
        for item in select:
            if isinstance(item, str):
                simple_cols.append(item)
            elif isinstance(item, dict):
                for agg_func, col in item.items():
                    if col not in agg_dict:
                        agg_dict[col] = []
                    agg_dict[col].append(agg_func)
        
        # Ensure all group_by columns are included
        for col in group_by:
            if col not in simple_cols:
                simple_cols.append(col)
        
        # Perform groupby operation
        if agg_dict:
            result = result.groupby(group_by).agg(agg_dict)
            # Flatten multi-level column names
            if isinstance(result.columns, pd.MultiIndex):
                result.columns = [f"{col}_{agg}" if agg != "" else col 
                                for col, agg in result.columns]
            # Reset index to convert group_by columns back to regular columns
            result = result.reset_index()
        else:
            # Just group by and take the first record of each group if no aggregation
            result = result[simple_cols].groupby(group_by).first().reset_index()
    else:
        # No GROUP BY, just select the requested columns
        selected_cols = []
        for item in select:
            if isinstance(item, str):
                selected_cols.append(item)
            elif isinstance(item, dict):
                for agg_func, col in item.items():
                    if agg_func == "min":
                        result[f"min_{col}"] = result[col].min()
                    elif agg_func == "max":
                        result[f"max_{col}"] = result[col].max()
                    elif agg_func == "avg" or agg_func == "mean":
                        result[f"avg_{col}"] = result[col].mean()
                    elif agg_func == "sum":
                        result[f"sum_{col}"] = result[col].sum()
                    elif agg_func == "count":
                        result[f"count_{col}"] = result[col].count()
                    selected_cols.append(f"{agg_func}_{col}")
        
        if selected_cols:
            result = result[selected_cols]
    
    # Apply ORDER BY
    if order_by:
        sort_cols = []
        ascending = []
        for order_item in order_by:
            for col, asc in order_item.items():
                sort_cols.append(col)
                ascending.append(asc)
        
        result = result.sort_values(by=sort_cols, ascending=ascending)
    
    # Apply LIMIT
    if limit:
        result = result.head(limit)
    
    return result

# Advanced WHERE condition examples - can be passed to where parameter
def greater_than(value):
    return lambda x: x > value

def less_than(value):
    return lambda x: x < value

def between(min_val, max_val):
    return lambda x: (x >= min_val) & (x <= max_val)

def like(pattern):
    return lambda x: x.str.contains(pattern, regex=True)


######## Test Below ########

# Create a sample DataFrame
data = pd.DataFrame({
    "name": ["Alice", "Bob", "Charlie", "Alice", "Dave"],
    "department": ["Sales", "IT", "Marketing", "Sales", "IT"],
    "age": [25, 30, 35, 25, 40],
    "salary": [50000, 60000, 70000, 55000, 65000]
})

# Simple SELECT with WHERE
result = sql_query(
    table=data,
    select=["name", "age"],
    where={"department": "Sales"}
)

# # SELECT with aggregation functions and GROUP BY
# result = sql_query(
#     table=data,
#     select=["department", {"avg": "salary"}, {"max": "age"}, {"count": "name"}],
#     group_by=["department"],
#     order_by=[{"avg_salary": False}]  # Sort by average salary descending
# )

# # More complex WHERE conditions
# result = sql_query(
#     table=data,
#     select=["name", "age", "salary"],
#     where={"age": greater_than(30), "department": ["IT", "Marketing"]},
#     limit=2
# )

print(result)