"""Functions to prepare the confusion matrix plotting"""

from typing import Dict, List

import numpy as np
import pandas as pd
from pyspark.sql import DataFrame
from sklearn.metrics import confusion_matrix


def prepare_confusion_matrix(
    prediction_data: DataFrame,
    confusion_matrix_params: Dict[str, str],
    labels: List[str],
) -> pd.DataFrame:
    """This function calculates the array needed for plotting a confusion matrix.
    In the end we are also building a pandas dataframe which has the labels as the index
    and column names in order to have an easier time plotting the dataframe.

    Args:
        prediction_data (DataFrame): Pyspark dataframe including the prediction and the
            true target
        confusion_matrix_params (Dict[str, str]): Parameters which includes the column
            names of the true and prediction of the target
        labels (List[str]): List of label names

    Returns:
        pd.DataFrame: Pandas dataframe having the labels as the column names and the
            index
    """

    prediction_column_name = confusion_matrix_params["prediction_column_name"]
    target_column_name = confusion_matrix_params["target_column_name"]
    scaled = confusion_matrix_params.get("scaled", False)

    for column_name in [prediction_column_name, target_column_name]:
        assert (
            column_name in prediction_data.schema.fieldNames()
        ), f"The column {column_name} is not present in the prediction dataframe"

    array = np.array(
        prediction_data.select([prediction_column_name, target_column_name]).collect()
    )

    confusion_matrix_array = confusion_matrix(array[:, 1], array[:, 0], labels=labels)

    if scaled:
        confusion_matrix_array = confusion_matrix_array.astype(float)
        row_sums = confusion_matrix_array.sum(axis=1, keepdims=True)
        confusion_matrix_array /= row_sums

    return pd.DataFrame(index=labels, columns=labels, data=confusion_matrix_array)

