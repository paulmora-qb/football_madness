"""Splitter functions for the training"""

from typing import Dict

import numpy as np
from pyspark.sql import DataFrame
from pyspark.sql import functions as f

from src.utilities.helper import _get_spark_session, load_obj


def split_train_test_dataframe(data: DataFrame, params: Dict[str, str]):

    splitter_function = load_obj(params["function"])(**params["function_params"])

    target_column_values = np.array(
        data.select(params["target_column_name"]).collect()
    ).flatten()

    n_samples = np.zeros(len(target_column_values))
    list(splitter_function.split(n_samples, target_column_values))

