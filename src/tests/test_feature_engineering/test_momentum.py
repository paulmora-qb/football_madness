"""Tests for momentum features"""

from typing import Dict

import numpy as np
import pandas as pd
from pyspark.sql import DataFrame

from feature_engineering.momentum import create_momentum_features


def test_create_momentum_features(
    team_spine: DataFrame, momentum_feature_params: Dict[str, str], spark, helper
) -> None:
    df_actual = create_momentum_features(team_spine, momentum_feature_params)
    df_expected = spark.createDataFrame(
        pd.DataFrame(
            {
                "team": {
                    0: "FC Everton",
                    1: "FC Everton",
                    2: "FC Everton",
                    3: "Granula Bar",
                    4: "Granula Bar",
                    5: "Granula Bar",
                },
                "season": {
                    0: "2000-2001",
                    1: "2000-2001",
                    2: "2001-2002",
                    3: "2000-2001",
                    4: "2000-2001",
                    5: "2001-2002",
                },
                "datediff": {0: -90, 1: -80, 2: -6, 3: -50, 4: -40, 5: -3,},
                "window_full_time_goals_row3_row1": {
                    0: 0,
                    1: 1,
                    2: 3,
                    3: 3,
                    4: 8,
                    5: 1,
                },
                "window_full_time_opponent_goals_row3_row1": {
                    0: 5,
                    1: 9,
                    2: 2,
                    3: 3,
                    4: 4,
                    5: 5,
                },
            }
        )
    )
    assert (
        df_actual.columns == df_expected.columns
    ), "Column names differ from actual and expected"
    assert len(df_actual.columns) == 5, "Number of column names are not 5 anymore"
    helper.assert_df_equality(df_actual, df_expected, ["team", "season", "datediff"])
