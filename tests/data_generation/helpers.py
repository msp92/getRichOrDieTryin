from typing import List, TypeVar, Union
import pandas as pd

T = TypeVar("T")  # Generic type for rows


# Function to generate a DataFrame from a list of rows or a single row object.
def generate_df_from_rows(rows: Union[List[T], T]) -> pd.DataFrame:
    # Ensure rows is a list, even if a single instance is provided
    if isinstance(rows, list):
        data = [row.__dict__ for row in rows]
    else:
        data = [rows.__dict__]  # Wrap the single instance in a list

    return pd.DataFrame(data)
