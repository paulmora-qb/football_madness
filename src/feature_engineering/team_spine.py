"""Create team spine"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as f

from src.general.pkgs.utilities.helper import _find_list_elements_using_keyword


def _create_generic_team_data(wide_data_set: DataFrame, team_type: str) -> DataFrame:
    """This function extracts the team information from the match dataset, and makes
        it generic. The result of this is getting the same information structure for the
        away as we get for the home team. The benefit of this is an unified feature
        creation.

    Args:
        wide_data_set (DataFrame): Match dataset, which contains the information for
            each team split by 'home' and 'away'
        team_type (str): Indication whether team plays 'home' or 'away'

    Returns:
        DataFrame: _description_
    """

    if team_type == "home":
        opponent_type = "away"
    else:
        opponent_type = "home"

    original_columns = wide_data_set.columns

    team_data_columns = _find_list_elements_using_keyword(
        original_columns, excluding_keyword=opponent_type
    )

    team_data = wide_data_set.select(team_data_columns)
    general_column_names = [x.replace(f"{team_type}_", "") for x in team_data.columns]

    for old_col, new_col in zip(team_data_columns, general_column_names):
        team_data = team_data.withColumnRenamed(old_col, new_col)

    team_data = team_data.withColumn("home_away_indication", f.lit(team_type))

    return team_data


def create_team_spine(match_data: DataFrame) -> DataFrame:
    """This function concatenates the home and away data after they have been stated
        in a generic format.

    Args:
        match_data (DataFrame): Match dataset which contains the information of the home
            and away team and their respective statistics

    Returns:
        DataFrame: Concatenated generically stated dataset. This dataset does not have
            statistics such as 'shots' divided by home and away, but rather equally
            stated.
    """

    # Reshape data into one row per team
    home_team_data = _create_generic_team_data(match_data, team_type="home")
    away_team_data = _create_generic_team_data(match_data, team_type="away")
    team_spine = home_team_data.union(away_team_data)

    return team_spine
