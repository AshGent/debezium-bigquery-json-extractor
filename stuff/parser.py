# Standard Imports
from typing import Tuple, List

# Third-Party Imports
import pandas as pd


def parse_studio3t_csv(filename: str) -> pd.DataFrame:
    """Parse Studio 3T CSV and return a dataframe containing column names and datatypes"""
    file_contents = pd.read_csv(filename)
    return file_contents[["name", "field_type"]].copy()


def format_json_path(column_name: str) -> Tuple[str, str]:
    """Create and return the JSON Path for a BigQuery SQL statement

    Args:
        column_name: Name of the column

    Returns:
        A tuple containing a BigQuery valid JSON Path and column name without nested prefixes
    """

    # Nested columns are represented with a `.` as a seperator
    if "." not in column_name:
        return column_name, column_name

    nested_columns = column_name.split(".")
    top_level = nested_columns[0]
    mid_levels = [f"['{column}']" for column in nested_columns[1:-1]]
    bottom_level = nested_columns[-1]

    json_path = f"{top_level}{''.join(mid_levels)}['{bottom_level}']"

    return json_path, bottom_level


def create_sql_line(column, datatype) -> str:
    """Generate a JSON_EXTRACT SQL line from column name and datatype"""

    json_path, base_name = format_json_path(column)

    # Specific datatypes are nested an additional level with the column names
    # containing invalid characters for BigQuery columns

    # Handle Mongodb ObjectId datatypes
    if datatype == "ObjectId":
        # `after` is the field containing the Mongodb record
        sql_line = f"JSON_EXTRACT(after, \"$.{json_path}['$oid']\" AS ObjectId,"
    elif datatype == "Array":
        sql_line = f"JSON_EXTRACT_ARRAY(after, \"$.{json_path}\" AS {base_name},"
    elif datatype == "Date":
        sql_line = f"JSON_EXTRACT(after, \"$.{json_path}['$date']\" AS {base_name},"
    elif datatype == "Object":
        sql_line = ""
    else:
        sql_line = f"JSON_EXTRACT(after, \"$.{json_path}\" AS {base_name},"

    return sql_line


def combine_sql_lines(sql_lines: List[str]) -> List[str]:
    """Remove empty lines caused by Object datatypes"""
    return [line for line in sql_lines if line != ""]


def generate_sql(filename: str) -> str:

    file_contents = parse_studio3t_csv(filename)
    lines: List[str] = []
    for index, column in file_contents.iterrows():
        lines.append(create_sql_line(column=column["name"], datatype=column["field_type"]))

    cleansed = combine_sql_lines(lines)
    return '\n'.join(cleansed)
