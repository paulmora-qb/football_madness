"""Model evaluation"""

from typing import Callable, List

from pyspark.sql import DataFrame


def prediction_evaluation(
    data: DataFrame, evaluation_function: Callable, evaluation_metrics: List[str]
) -> dict:
    a = 1
