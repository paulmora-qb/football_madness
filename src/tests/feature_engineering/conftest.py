"""Conftests for feature engineering"""

import datetime
from typing import Dict

import pandas as pd
import pytest
import yaml
from pyspark.sql import DataFrame, Row
from pyspark.sql.types import (
    DateType,
    FloatType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


@pytest.fixture(scope="function")
def momentum_params() -> Dict[str, str]:
    return yaml.safe_load(
        """
    translation:
      - object: general.functions.feature_engineering.utils.transforming.dict_replace
        input_col: full_time_result
        dictionary:
          win: 3
          draw: 1
          loss: 0
        column_name: points_per_game
    flagging:
      - object: general.functions.feature_engineering.flags.expr_flag
        expr: case when full_time_result = 'win' then 1 else 0 end
        column_name: winning_the_game
      - object: general.functions.feature_engineering.flags.expr_flag
        expr: "1"
        column_name: game_indication
    horizontal_averages:
      - object: general.functions.feature_engineering.aggregation.create_horizontal_averages
        aggregation_columns:
          - shots_left
          - shots_right
        column_name: total_shots
    window_operations:
      - object: general.functions.feature_engineering.aggregation.create_window_aggregates
        partition_by: ["team"]
        order_by: ["date"]
        aggregation_columns:
          - winning_the_game
          - total_shots
          - game_indication
        aggregation_type: pyspark.sql.functions.sum
        rows_between: [[-1, 0]]
        prefix: ftr
        suffix: last
    post_window_operations:
      - object: general.functions.feature_engineering.aggregation.dynamic_post_window_aggregation
        left_column: ftr_winning_the_game_(?P<window>.*)_last
        right_column: ftr_game_indication_(?P<window>.*)_last
        math_operation: /
        output_column: ftr_ratio_winning_past_game_{window}
    """
    )


@pytest.fixture(scope="function")
def momentum_expected(spark) -> DataFrame:
    return spark.createDataFrame(
        pd.DataFrame(
            {
                "team": {0: "rick", 1: "rick"},
                "date": {0: datetime.date(2020, 10, 5), 1: datetime.date(2020, 10, 10)},
                "league": {0: "funny_league", 1: "funny_league"},
                "home_away_indication": {0: "away", 1: "home"},
                "ftr_winning_the_game_row_1_row_0_last": {0: 0, 1: 1},
                "ftr_total_shots_row_1_row_0_last": {0: 20.0, 1: 40.0},
                "ftr_game_indication_row_1_row_0_last": {0: 1, 1: 2},
                "ftr_ratio_winning_past_game_row_1_row_0": {0: 0.0, 1: 0.5},
            }
        ),
        schema=StructType(
            [
                StructField("team", StringType(), True),
                StructField("date", DateType(), True),
                StructField("league", StringType(), True),
                StructField("home_away_indication", StringType(), True),
                StructField(
                    "ftr_winning_the_game_row_1_row_0_last", IntegerType(), True
                ),
                StructField("ftr_total_shots_row_1_row_0_last", FloatType(), True),
                StructField(
                    "ftr_game_indication_row_1_row_0_last", IntegerType(), True
                ),
                StructField(
                    "ftr_ratio_winning_past_game_row_1_row_0", FloatType(), True
                ),
            ]
        ),
    )
