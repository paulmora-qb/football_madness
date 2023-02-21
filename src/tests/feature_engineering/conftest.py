"""Conftests for feature engineering"""

import datetime
from typing import Dict

import pytest
import yaml
from pyspark.sql import DataFrame, Row


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
        aggregation_type: pyspark.sql.functions.avg
        rows_between: [[-1, 0]]
        prefix: ftr
        suffix: last
    post_window_operations:
      - object: general.functions.feature_engineering.aggregation.dynamic_post_window_aggregation
        left_column: tmp_general_winning_the_game_(?P<window>.*)_last_games
        right_column: tmp_general_game_indication_(?P<window>.*)_last_games
        math_operation: /
        output_column: ftr_ratio_winning_past_game_{window}
    """
    )


@pytest.fixture(scope="function")
def momentum_expected(spark) -> DataFrame:
    return spark.createDataFrame(
        [
            Row(
                team="rick",
                date=datetime.date(2020, 10, 5),
                league="funny_league",
                home_away_indication="away",
                ftr_winning_the_game_row_1_row_0_last=0.0,
                ftr_total_shots_row_1_row_0_last=25.0,
                ftr_game_indication_row_1_row_0_last=1.0,
            ),
            Row(
                team="rick",
                date=datetime.date(2020, 10, 10),
                league="funny_league",
                home_away_indication="home",
                ftr_winning_the_game_row_1_row_0_last=0.5,
                ftr_total_shots_row_1_row_0_last=20.0,
                ftr_game_indication_row_1_row_0_last=1.0,
            ),
        ]
    )
