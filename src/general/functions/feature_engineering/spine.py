"""Create team spine"""

from typing import Dict

from pyspark.sql import DataFrame
from pyspark.sql import functions as f


def _create_generic_team_data(match_data: DataFrame, team_type: str) -> DataFrame:
    """This function creates an overview of the team performance from the perspective
    of the home/away team. That facilitates the subsequent feature creation

    Args:
        match_data (DataFrame): The concatenated dataframe of all football results
        team_type (str): Indication whether the team was playing home/ away

    Returns:
        DataFrame: A dataframe from the perspective of the home/away team
    """

    # Clarifying home/ away team
    if team_type == "home":
        opponent_type = "away"
        win_lose_dict = {"H": "win", "A": "loss", "D": "draw"}
    else:
        opponent_type = "home"
        win_lose_dict = {"A": "win", "H": "loss", "D": "draw"}

    # Column renaming
    original_column_names = match_data.columns
    adj_column_names = []
    for col in original_column_names:
        if team_type in col:
            adj_column_names.append(col.replace(f"{team_type}_", ""))
        elif opponent_type in col:
            adj_column_names.append(col.replace(f"{opponent_type}_", "opponent_"))
        else:
            adj_column_names.append(col)

    # Match data renaming
    for old_col, new_col in zip(original_column_names, adj_column_names):
        match_data = match_data.withColumnRenamed(old_col, new_col)
    match_data = match_data.withColumn("home_away_indication", f.lit(team_type))

    # Dropping opponent name
    match_data = match_data.drop(f"{opponent_type}_team")

    # Creation of difference in days column
    match_data = match_data.withColumn(
        "datediff", f.datediff(f.col("date"), f.current_date())
    )

    # Clarifying which team won from the perspective of the home/away team
    match_data = match_data.replace(win_lose_dict, subset="full_time_result")

    return match_data


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
    team_spine = home_team_data.unionByName(away_team_data)

    return team_spine


def create_match_spine(match_data: DataFrame, params: Dict[str, str]) -> DataFrame:
    """This function creates the backbone of the matches, to which we are then merging
    the features created in the feature engineering pipeline

    Args:
        match_data (DataFrame): Dataframe with all available columns

    Returns:
        DataFrame: Dataframe containing only the relevant merging variables
            for each game, which are home-team, away-team and the date
    """
    return match_data.select(params["keys"])
