"""Main conftest file"""

import datetime
from functools import partial

import pytest
from pyspark.sql import DataFrame, Row, SparkSession


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
        [
            Row(
                team="rick",
                opponent_team="morty",
                league="funny_league",
                full_time_result="win",
                shots_left=10,
                shots_right=20,
                home_away_indication="home",
                date=datetime.date(2020, 10, 10),
            ),
            Row(
                team="rick",
                opponent_team="morty",
                league="funny_league",
                full_time_result="loss",
                shots_left=20,
                shots_right=30,
                home_away_indication="away",
                date=datetime.date(2020, 10, 5),
            ),
        ]
    )

