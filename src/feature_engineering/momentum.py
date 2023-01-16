"""Creation of momentum features"""

import re
from typing import Any, Dict

from pyspark.sql import DataFrame

from general.nodes.feature_engineering.create_columns import create_column_from_config


def create_momentum_features(
    team_spine: DataFrame, params: Dict[str, Any]
) -> DataFrame:
    """This function creates rolling averages out of several already existing and some
    build features. This is done entirely through the configuration files

    Args:
        team_spine (DataFrame): Information of every team and game-day
        params (Dict[str, Any]): Parameter containing information about which operation
            should be done to which column. The ordering of the `transform` commands
            specifies when which column is created

    Returns:
        DataFrame: Dataframe containing the core column to merge the dataframe and the
            built features
    """

    core_columns = ["team", "date", "league", "home_away_indication"]

    df = (
        team_spine.transform(
            lambda x: create_column_from_config(x, params["translation"])
        )
        .transform(
            lambda x: create_column_from_config(x, params["horizontal_averages"])
        )
        .transform(lambda x: create_column_from_config(x, params["window_operations"]))
    )

    regex = re.compile("ftr_window_.*")
    feature_columns = core_columns + list(filter(regex.match, df.columns))
    return df.select(feature_columns).na.drop()

