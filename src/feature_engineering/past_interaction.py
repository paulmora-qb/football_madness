"""Creating past interaction features"""

from typing import Dict

from pyspark.sql import DataFrame


def create_past_interaction_features(
    match_spine: DataFrame, params: Dict[str, str]
) -> DataFrame:
    """_summary_

    Args:
        match_spine (DataFrame): _description_
        params (Dict[str, str]): _description_

    Returns:
        DataFrame: _description_
    """
    core_columns = ["team", "date", "league", "home_away_indication"]

    return match_spine
