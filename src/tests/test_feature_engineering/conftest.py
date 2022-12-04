"""Creation of fixtures for pytests"""

from typing import Dict, List

import pandas as pd
import pytest
from pyspark.sql import DataFrame, SparkSession

from general.pkgs.utilities.helper import _get_spark_session


class Helper:
    @staticmethod
    def assert_df_equality(df1: DataFrame, df2: DataFrame, key_columns: List[str]):
        sorted_df1 = df1.sort(key_columns)
        sorted_df2 = df2.sort(key_columns)

        assert (
            sorted_df1.collect() == sorted_df2.collect()
        ), "Dataframes have different values"
        assert (
            sorted_df1.schema == sorted_df2.schema
        ), "Dataframes have different schemas"


@pytest.fixture(scope="session")
def helper():
    return Helper


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    return SparkSession.builder.getOrCreate()


@pytest.fixture(scope="session")
def team_spine() -> DataFrame:
    spark = _get_spark_session()
    return spark.createDataFrame(
        pd.DataFrame(
            {
                "team": {
                    0: "FC Everton",
                    1: "FC Everton",
                    2: "FC Everton",
                    3: "FC Everton",
                    4: "FC Everton",
                    5: "Granula Bar",
                    6: "Granula Bar",
                    7: "Granula Bar",
                    8: "Granula Bar",
                    9: "Granula Bar",
                },
                "season": {
                    0: "2000-2001",
                    1: "2000-2001",
                    2: "2000-2001",
                    3: "2001-2002",
                    4: "2001-2002",
                    5: "2000-2001",
                    6: "2000-2001",
                    7: "2000-2001",
                    8: "2001-2002",
                    9: "2001-2002",
                },
                "datediff": {
                    0: -100,
                    1: -90,
                    2: -80,
                    3: -7,
                    4: -6,
                    5: -50,
                    6: -40,
                    7: -55,
                    8: -3,
                    9: -10,
                },
                "full_time_goals": {
                    0: 0,
                    1: 1,
                    2: 2,
                    3: 3,
                    4: 4,
                    5: 5,
                    6: 4,
                    7: 3,
                    8: 2,
                    9: 1,
                },
                "full_time_opponent_goals": {
                    0: 5,
                    1: 4,
                    2: 3,
                    3: 2,
                    4: 1,
                    5: 1,
                    6: 2,
                    7: 3,
                    8: 4,
                    9: 5,
                },
                "full_time_result": {
                    0: "loss",
                    1: "loss",
                    2: "loss",
                    3: "win",
                    4: "win",
                    5: "win",
                    6: "win",
                    7: "draw",
                    8: "loss",
                    9: "loss",
                },
            }
        )
    )


@pytest.fixture(scope="session")
def momentum_feature_params() -> Dict:
    return [
        {
            "function": "feature_engineering.core.aggregation.create_window_aggregates",
            "partition_by": ["team", "season"],
            "order_by": ["datediff"],
            "aggregation_columns": ["full_time_goals", "full_time_opponent_goals",],
            "aggregation_type": "pyspark.sql.functions.sum",
            "rows_between": [-3, -1],
            "prefix": "window",
            "suffix": "last_games",
        }
    ]
