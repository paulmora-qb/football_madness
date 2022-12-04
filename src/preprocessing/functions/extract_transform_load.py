"""Function for loading the raw data"""

import re
from typing import Any, Dict

import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType
from tqdm import tqdm

from utilities.helper import _get_spark_session


def concatenate_raw_data(
    partition_data_dict: Dict[str, Any],
    json_schema: Dict[str, Any],
    renaming_dict: Dict[str, str],
) -> DataFrame:
    """This function concatenates all dataframes vertically onto each other.

    Args:
        partition_data_dict (Dict[str, Any]): This dictionary is part of the partitioned
            dataset from kedro and represents a dictionary in which the values are a
            function which triggers the data to load
        json_schema (Dict[str, Any]): The json schema for transforming the dataframe
            from a pandas to a spark dataframe
        renaming_dict (Dict[str, str]): A dictionary which renames the columns into
            more sensible names

    Returns:
        DataFrame: Spark dataframe of the concatenated and renamed league results
    """

    master_df = pd.DataFrame()

    for df_key, df_value in tqdm(partition_data_dict.items()):
        temp_expanded_df = pd.concat(df_value().values())
        temp_expanded_df.loc[:, "season"] = re.search("[0-9]+-[0-9]+", df_key).group()
        master_df = pd.concat([master_df, temp_expanded_df])

    # Filter and rename columns
    renamed_columns_data = _select_and_rename_columns(master_df, renaming_dict)

    # Save dataframe as spark dataframe
    schema = StructType.fromJson(json_schema)
    spark = _get_spark_session()
    spark_df = spark.createDataFrame(renamed_columns_data, schema=schema)
    return spark_df


def _select_and_rename_columns(
    data: pd.DataFrame, renaming_dict: Dict[str, str]
) -> pd.DataFrame:
    """This function ingests data and a dictionary with which we are renaming
    columns. We are basically doing two things. For once we are keeping certain
    columns and the other step is that we are renaming the columns which have an
    unfortunate/ complicated name.

    Args:
        data (pd.DataFrame): Raw concatenated data with all columns
        renaming_dict (Dict[str, str]): Dictionary with key-value pair for renaming
            the columns

    Raises:
        KeyError: If we find that there are columns we would like to keep but we could
            not find them in the dataset

    Returns:
        pd.DataFrame: Dataframes with renamed column names
    """

    # Select relevant columns
    columns_to_keep = list(renaming_dict.keys())
    selected_columns_data = data.loc[:, columns_to_keep]

    columns_difference = set(columns_to_keep).difference(
        set(selected_columns_data.columns)
    )
    if columns_difference:
        raise KeyError(
            f"The following columns are not selected in filtering: {columns_difference}"
        )

    # Rename the columns
    renamed_columns_data = selected_columns_data.rename(columns=renaming_dict)

    return renamed_columns_data
