"""Function for loading the raw data"""

from typing import Any, Dict

import pandas as pd
from pyspark.sql import DataFrame
from tqdm import tqdm

from general.pkgs.utilities.helper import _get_spark_session, _validate_keys


def _select_rename_data(data: pd.DataFrame) -> pd.DataFrame:

    data = data.loc[:, ["Div", "Date", "HomeTeam", "AwayTeam", "Attendance", "Referee"]]


def concatenate_raw_data(partition_data_dict) -> pd.DataFrame:

    master_df = pd.DataFrame()

    for dataframe_value in tqdm(partition_data_dict.values()):
        temp_expanded_df = pd.concat(dataframe_value().values())
        master_df = pd.concat([master_df, temp_expanded_df])

    return master_df


def _select_and_rename_columns(data, renaming_dict) -> pd.DataFrame:

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


def filter_rename_raw_data(
    data: pd.DataFrame,
    schema,
    filter_params: Dict[str, Any],
    renaming_dict: Dict[str, str],
) -> DataFrame:

    _validate_keys(filter_params.keys(), mandatory_keys=["leagues", "dates"])

    # Filter and rename columns
    renamed_columns_data = _select_and_rename_columns(data, renaming_dict)

    # Filter data range
    start_year = filter_params["dates"]["season_year_start"]
    end_year = filter_params["dates"]["season_year_end"]
    renamed_columns_data.loc[:, "date"] = pd.to_datetime(
        renamed_columns_data.loc[:, "date"], format="%Y-%m-%d"
    )
    time_filtered_bool = (
        renamed_columns_data.loc[:, "date"].dt.year.between(start_year, end_year).values
    )
    time_filtered_data = renamed_columns_data.loc[time_filtered_bool, :]

    # Filter leagues
    relevant_leagues_list = filter_params["leagues"]
    league_filter_bool = (
        time_filtered_data.loc[:, "league"].isin(relevant_leagues_list).values
    )
    league_filtered_data = time_filtered_data.loc[league_filter_bool, :]

    # Turn pandas dataframe into spark dataframe
    spark = _get_spark_session()
    spark.createDataFrame(league_filtered_data)

    return league_filtered_data
