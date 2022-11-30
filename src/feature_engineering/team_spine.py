"""Create team spine"""

import pyspark.pandas as ps
from pyspark.sql import functions as f

from src.general.pkgs.utilities.helper import _find_list_elements_using_keyword


def _create_generic_team_data(
    wide_data_set: ps.DataFrame, team_type: str
) -> ps.DataFrame:
    """This function extracts the team information from the match dataset, and makes
        it generic. The result of this is getting the same information structure for the
        away as we get for the home team. The benefit of this is an unified feature
        creation.

    Args:
        wide_data_set (ps.DataFrame): Match dataset, which contains the information for
            each team split by 'home' and 'away'
        team_type (str): Indication whether team plays 'home' or 'away'

    Returns:
        ps.DataFrame: _description_
    """

    # Indication of away and home team
    if team_type == "home":
        opponent_type = "away"
    else:
        opponent_type = "home"
    original_columns = wide_data_set.columns

    # Getting team data in place
    team_data_columns = _find_list_elements_using_keyword(
        original_columns, excluding_keyword=opponent_type
    )
    team_data = wide_data_set.loc[:, team_data_columns]
    team_renaming_dict = {
        col: col.replace(f"{team_type}", "team") for col in team_data_columns
    }
    team_renaming_dict[f"{team_type}_team"] = "team"
    team_data = team_data.rename(columns=team_renaming_dict)
    team_data.loc[:, "home_away_indication"] = team_type

    # Getting opponent data in place
    opponent_data_columns = [
        x
        for x in _find_list_elements_using_keyword(
            original_columns, including_keyword=opponent_type
        )
        if not x.endswith("_odds") and x != f"{opponent_type}_team"
    ]
    opponent_data = wide_data_set.loc[:, opponent_data_columns]
    opponent_renaming_dict = {
        col: col.replace(opponent_type, "opponent") for col in opponent_data_columns
    }
    opponent_data = opponent_data.rename(columns=opponent_renaming_dict)

    total_data = ps.concat([team_data, opponent_data], axis=1)

    from datetime import datetime

    total_data.loc["date_diff"] = (
        ps.to_datetime(total_data.loc[:, "date"]) - datetime.today()
    )

    return total_data


def create_team_spine(match_data: ps.DataFrame) -> ps.DataFrame:
    """This function concatenates the home and away data after they have been stated
        in a generic format.

    Args:
        match_data (ps.DataFrame): Match dataset which contains the information of the
        home and away team and their respective statistics

    Returns:
        ps.DataFrame: Concatenated generically stated dataset. This dataset does not
            have statistics such as 'shots' divided by home and away, but rather equally
            stated.
    """

    # Reshape data into one row per team
    home_team_data = _create_generic_team_data(match_data, team_type="home")
    away_team_data = _create_generic_team_data(match_data, team_type="away")
    total_team_data = ps.concat([home_team_data, away_team_data], axis=0)

    return total_team_data
