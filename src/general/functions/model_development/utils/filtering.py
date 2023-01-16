"""Filtering functions for pyspark dataframes"""

from pyspark.sql import DataFrame


def filter_dataframe(
    data: DataFrame, reference_team: str, reference_season: str
) -> DataFrame:
    a = 1
