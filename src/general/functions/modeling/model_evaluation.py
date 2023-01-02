"""Model evaluation"""

from typing import Callable, List

from pyspark.sql import DataFrame
from pyspark.sql import functions as f


def prediction_evaluation(
    data: DataFrame, evaluation_function: Callable, evaluation_metrics: List[str]
) -> dict:
    """This function calculates the specified evaluation metrics and stores them
    in a dictionary.

    Args:
        data (DataFrame): Dataframe containing the true and predicted NUMERIC target
        evaluation_function (Callable): Pypspark function to evaluate the data
        evaluation_metrics (List[str]): Metric name which to use

    Returns:
        dict: Dictionary in which the split and metric is the key and the result the
            value
    """

    main_evaluation_dict = {}

    split_categories = [x[0] for x in data.select("split").distinct().collect()]
    for category in split_categories:
        sub_sample = data.filter(f.col("split") == category)

        for metric_name in evaluation_metrics:
            evaluation_function.setMetricName(metric_name)
            main_evaluation_dict[
                f"{category}_{metric_name}"
            ] = evaluation_function.evaluate(sub_sample)

    return main_evaluation_dict
