"""Model prediction functions"""

from typing import Callable, List

from pyspark.sql import DataFrame


def model_prediction(
    data: DataFrame,
    trained_model: Callable,
    prediction_suffix: str,
    prediction_proba_suffix: str,
    inverter: Callable = None,
    labels: List[str] = None,
    index_sub_suffix: str = None,
) -> DataFrame:
    """This function creates the prediction and prediction probability column. This is
    done by first adding the input to the model and afterwards transform the datasset.

    Args:
        data (DataFrame): Dataset for which we are creating the predictions
        trained_model (Callable): Fitted model to which we have to add the prediction
            columns
        prediction_suffix (str): Suffix for the prediction column
        prediction_proba_suffix (str): Suffix for the prediction probability column

    Returns:
        DataFrame: Dataframe with prediction and prediction probability column
    """

    label_column_name = trained_model.getLabelCol()
    if inverter:
        label_column_name += index_sub_suffix

    prediction_col_name = label_column_name + prediction_suffix
    prediction_proba_col_name = label_column_name + prediction_proba_suffix

    trained_model.setPredictionCol(prediction_col_name)
    prediction_data = trained_model.transform(data)

    if inverter:
        inverter.setLabels(labels)

        for column_name in [label_column_name, prediction_col_name]:
            inverter.setInputCol(column_name)
            inverter.setOutputCol(column_name.replace(index_sub_suffix, ""))
            prediction_data = inverter.transform(prediction_data)

    return prediction_data

