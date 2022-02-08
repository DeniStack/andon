import pandas as pd

from datetime import datetime, timedelta

def tuple_to_df(tuple_list: tuple, columns: list=None) -> pd.DataFrame:
    df = pd.DataFrame(data=tuple_list, columns=columns)
    return df

def set_query_date(query_date: datetime) -> datetime:
    query_date = query_date - timedelta(hours=1)
    query_date = query_date.replace(minute=0, second=0, microsecond=0)
    return query_date

def convert_result_code(value: int) -> int:
    if value == 2:
        value = 0
    return value

def convert_result_code(value: int) -> int:
    if value == 2:
        value = 0
    return value