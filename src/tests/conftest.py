"""Main conftest file"""

import datetime
from functools import partial

import pandas as pd
import pytest
from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql.types import DateType, FloatType, StringType, StructField, StructType


class Helpers:
    @staticmethod
    def assert_equality(actual: DataFrame, expected: DataFrame) -> None:
        """This function helps to assess the equality of two spark dataframes

        Args:
            actual (DataFrame): Actual dataframe
            expected (DataFrame): Truth to compare the actual dataframe to
        """
        actual_row_list = actual.collect()
        expected_row_list = expected.collect()

        assert actual.columns == expected.columns

        sorted_by_keys = partial(
            sorted, key=lambda row: tuple(row[k] for k in actual.columns)
        )
        for act_row, exp_row in zip(
            sorted_by_keys(actual_row_list), sorted_by_keys(expected_row_list)
        ):
            for k in act_row.asDict().keys():
                if isinstance(act_row[k], float) and isinstance(exp_row[k], float):
                    assert (
                        abs(act_row[k] - exp_row[k]) < 0.001
                    ), f"Key: {k}; Got {act_row[k]}; Expect {exp_row[k]}"
                else:
                    assert (
                        act_row[k] == exp_row[k]
                    ), f"Key: {k}; Got {act_row[k]}; Expect {exp_row[k]}"


@pytest.fixture(scope="session")
def helpers():
    return Helpers


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    return SparkSession.builder.getOrCreate()


@pytest.fixture(scope="session")
def team_spine(spark) -> DataFrame:
    return spark.createDataFrame(
        pd.DataFrame(
            {
                "team": {0: "rick", 1: "rick"},
                "opponent_team": {0: "morty", 1: "morty"},
                "league": {0: "funny_league", 1: "funny_league"},
                "full_time_result": {0: "win", 1: "loss"},
                "shots_left": {0: None, 1: 20},
                "shots_right": {0: 20.0, 1: None},
                "home_away_indication": {0: "home", 1: "away"},
                "date": {0: datetime.date(2020, 10, 10), 1: datetime.date(2020, 10, 5)},
            }
        ),
        schema=StructType(
            [
                StructField("team", StringType(), True),
                StructField("opponent_team", StringType(), True),
                StructField("league", StringType(), True),
                StructField("full_time_result", StringType(), True),
                StructField("shots_left", FloatType(), True),
                StructField("shots_right", FloatType(), True),
                StructField("home_away_indication", StringType(), True),
                StructField("date", DateType(), True),
            ]
        ),
    )

