"""Creation of the target variable"""
from typing import Dict

from pyspark.sql import DataFrame


def create_target_variable(raw_data: DataFrame, params: Dict[str, str]) -> DataFrame:
    """This simple function retrieves the target variable which is right now the full
    time result. This can be changed though in case it is desired to predict e.g.
    the number of goals.

    Args:
        raw_data (DataFrame): Raw concatenated dataframe
        params (Dict[str, str]): Dictionary containing information about which 
            columns to keep for the target

    Returns:
        DataFrame: Dataframe containing the key variables plus the full_time_result
    """

    key_columns = params["keys"]
    target_column_name = params["target_column_name"]

    if isinstance(target_column_name, str):
        target_column_name = [target_column_name]

    return raw_data.select(key_columns + target_column_name).na.drop(
        subset=target_column_name
    )
