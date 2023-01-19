"""Filter dataframes"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as f


def filter_dataframe(
    data: DataFrame, reference_season: str, reference_team: str
) -> DataFrame:
    """This function filters the master table down to what is desired to be predicted
    in the end. Herein we namely filter the two column

    Args:
        data (DataFrame): Data should be filtered down. Should have 
        reference_season (str): Season for which we would like to filter
        reference_team (str): Team for which we would like to get the predictions

    Returns:
        DataFrame: Filtered dataframe
    """

    dataframe_columns = data.columns

    column_difference = set(["season", "home_team", "away_team"]) - set(
        dataframe_columns
    )
    assert len(column_difference) == 0, f"The column(s) {column_difference} are missing"

    filtered_dataframe = data.filter(
        (f.col("home_team") == reference_team) | (f.col("away_team") == reference_team)
    ).filter(f.col("season") == reference_season)

    assert filter_dataframe.count() > 0, f"The filtered dataframe is empty"
    return filtered_dataframe
