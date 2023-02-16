"""Filter dataframes"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as f


def filter_dataframe(
    data: DataFrame,
    reference_season: str = None,
    reference_team: str = None,
    reference_league: str = None,
) -> DataFrame:
    """This function filters the master table down to what is desired to be predicted
    in the end. Herein we namely filter the two column

    Args:
        data (DataFrame): Data should be filtered down. Should have the columns
            `home_team`, `away_team` and `season` in order to filter
        reference_season (str): Season for which we would like to filter
        reference_team (str): Team for which we would like to get the predictions
        reference_league(str): League which we would like to filter

    Returns:
        DataFrame: Filtered dataframe
    """

    if (not reference_season) and (not reference_team) and (not reference_league):
        raise ValueError("Either reference season, team or season has to be specified")

    dataframe_columns = data.columns

    column_difference = set(["season", "home_team", "away_team"]) - set(
        dataframe_columns
    )
    assert len(column_difference) == 0, f"The column(s) {column_difference} are missing"

    filtered_dataframe = data

    # Filtering the team
    if reference_team:
        filtered_dataframe = filtered_dataframe.filter(
            (f.col("home_team") == reference_team)
            | (f.col("away_team") == reference_team)
        )

    # Filtering the season
    if reference_season:
        filtered_dataframe = filtered_dataframe.filter(
            f.col("season") == reference_season
        )

    # Filtering the league
    if reference_league:
        filtered_dataframe = filter_dataframe.filter(
            f.col("league") == reference_league
        )

    assert filtered_dataframe.count() > 0, f"The filtered dataframe is empty"
    return filtered_dataframe
