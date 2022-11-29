"""Create team spine"""

from pyspark.sql import DataFrame

from src.general.pkgs.utilities.helper import _find_list_elements_using_keyword


def _create_generic_team_data(
    wide_data_set: DataFrame, team_type: str, opponent_type
) -> DataFrame:

    original_columns = wide_data_set.columns

    team_data_columns = _find_list_elements_using_keyword(
        original_columns, excluding_keyword=opponent_type
    )

    location_data = wide_data_set.select(team_data_columns)
    general_column_names = [
        x.replace(f"{team_type}_", "") for x in location_data.columns
    ]

    for old_col, new_col in zip(team_data_columns, general_column_names):
        location_data = location_data.withColumnRenamed(old_col, new_col)

    return location_data


def create_team_spine(match_data: DataFrame) -> DataFrame:

    # Reshape data into one row per team
    home_team_data = _create_generic_team_data(
        match_data, team_type="home", opponent_type="away"
    )
    away_team_data = _create_generic_team_data(
        match_data, team_type="away", opponent_type="home"
    )
    team_spine = home_team_data.union(away_team_data)

    return team_spine
