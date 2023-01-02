"""Model prediction functions"""

from typing import Callable, List

from pyspark.sql import DataFrame


def target_column_inverter(
    data: DataFrame,
    inverter: Callable,
    target_column_name: str,
    prediction_suffix: str,
    index_suffix: str,
) -> DataFrame:
    """This function reverts the encoded target variable. That is necessary because in
    Pyspark it is currently not possible to use a categorical target for a
    classification model. Therefore we encode that variable right at the beginning of
    the pipeline. At this point we then have to transfer that back, inverting the
    encoding.

    Args:
        data (DataFrame): Dataframe containing the encoded target column
        inverter (Callable): Inverter instance
        target_column_name (str): Target column name
        prediction_suffix (str): Suffix added for the prediction column
        index_suffix (str): Index suffix which was used to distinguish the encoded
            target from the real one

    Returns:
        DataFrame: Dataframe with the inverted target
    """

    target_pred_column_name = target_column_name + prediction_suffix
    for col_name in [target_column_name, target_pred_column_name]:
        adjusted_col_name = col_name.replace(index_suffix, "")
        inverter.setInputCol(col_name)
        inverter.setOutputCol(adjusted_col_name)

        data = inverter.transform(data)

    return data


def model_prediction(
    data: DataFrame,
    trained_model: Callable,
    prediction_suffix: str,
    prediction_proba_suffix: str = None,
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
    prediction_col_name = label_column_name + prediction_suffix

    trained_model.setPredictionCol(prediction_col_name)
    return trained_model.transform(data)

