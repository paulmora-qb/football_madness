"""Prepare the performance metrics"""

from typing import Dict

import pandas as pd

from utilities.helper import sentence_case


def prepare_performance_metrics(
    performance_metric_dict: Dict[str, float]
) -> pd.DataFrame:
    """The performance metric is currently provided in dictionary format with
    sub-optimal string format. Before we plot the performances, it would be therefore
    beneficial to bring the dictionary format into a more usable pandas dataframe.

    Args:
        performance_metric_dict (Dict[str, float]): Dictionary with the split and metric
            information provided as key and the performance as the value

    Returns:
        pd.DataFrame: Dataframe with information about the split, metric and
            corresponding performance
    """

    performance_metric_df = pd.DataFrame(index=range(len(performance_metric_dict)))

    for index, (key, value) in enumerate(performance_metric_dict.items()):
        split, metric = key.split("_")
        performance_metric_df.loc[index, "split"] = split.title()
        performance_metric_df.loc[index, "metric"] = sentence_case(metric)
        performance_metric_df.loc[index, "performance"] = value

    return performance_metric_df
