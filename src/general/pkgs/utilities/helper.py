"""General helper functions"""

from typing import Iterable, List

from pyspark.sql import SparkSession


def _get_spark_session() -> SparkSession:
    return SparkSession.builder.getOrCreate()


def _validate_keys(provided_keys: Iterable[str], mandatory_keys: List[str]):
    missing_keys = set(mandatory_keys).difference(provided_keys)
    if missing_keys:
        raise KeyError(f"The following key(s) are missing: {missing_keys}")
