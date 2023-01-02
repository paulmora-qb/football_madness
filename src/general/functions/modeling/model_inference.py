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
    """This function inputs a dataframe in which the target variable is currently
    encoded in a numerical format and we would like to get the inverted shape back

    Args:
        data (DataFrame): Dataframe containing the target column which has a numerical
            target variable which we would like to transform
        inverter (Callable): The inverter function we use to bring the category value
            back
        label_col_name (str): Name of the target column which is still in numerical
            format 

    Returns:
        DataFrame: Dataframe which now has the inverted target variable back. We also
            delete the numerical column since we do not need two
    """
    a = 1
    # assert label_column_name.endswith(
    #     index_sub_suffix
    # ), "The encoded target variable has the wrong suffix."

    # adjusted_label_column_name = label_column_name.replace(index_sub_suffix, "")
    # inverter.setInputCol(label_column_name)
    # inverter.setOutputCol(adjusted_label_column_name)
    return inverter.transform(data)


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

