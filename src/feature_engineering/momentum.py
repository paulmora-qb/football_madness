"""Creation of momentum features"""

import re
from typing import Any, Dict

from pyspark.sql import DataFrame

from general.functions.feature_engineering.preprocess_params import preprocess_params
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
    params = preprocess_params(params)

    df = (
        team_spine
        # creating an idea how many points winning/drawing/losing is worth
        .transform(lambda x: create_column_from_config(x, params["translation"]))
        # creating an indication whether game was played from home/ away/ won/ lost
        .transform(lambda x: create_column_from_config(x, params["flagging"]))
        # creating averages of all betting scores
        .transform(
            lambda x: create_column_from_config(x, params["horizontal_averages"])
        )
        # creating the rolling window features
        .transform(lambda x: create_column_from_config(x, params["window_operations"]))
        # creating post window features
        .transform(
            lambda x: create_column_from_config(x, params["post_window_operations"])
        )
    )

    regex = re.compile("ftr_.*")
    feature_columns = core_columns + list(filter(regex.match, df.columns))
    return df.select(feature_columns).na.drop()

