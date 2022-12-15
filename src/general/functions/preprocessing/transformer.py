"""Function for fitting and transforming"""

from typing import Callable

from pyspark.sql import DataFrame
from pyspark.sql import functions as f


def fit(data: DataFrame, transformer: Callable, split_data: bool = True) -> Callable:
    """This function fits the transformer on the provided dataframe. We also distinguish
    between only using the train dataset for fitting, or the entire dataset. Using the
    entire dataset is sometimes required, when applying a stringindexer for example.

    Args:
        data (DataFrame): Input dataframe
        transformer (Callable): Callable transformer
        split_data (bool, optional): Indication whether we would like the transformer
            to fit only on the training data. Defaults to True.

    Returns:
        Callable: _description_
    """

    if not split_data:
        model = transformer.fit(data)
    else:
        train_data = data.filter(f.col("split") == "TRAIN")
        model = transformer.fit(train_data)

    return model


def transform(data: DataFrame, fitted_transformer: Callable) -> DataFrame:
    """This function applies the fitted transformer and applies it on the dataset

    Args:
        data (DataFrame): Dataset on which we apply the transformer
        fitted_transformer (Callable): Fitted instance of the transformer

    Returns:
        DataFrame: Transformed dataset
    """

    return fitted_transformer.transform(data)


def fit_transform(data: DataFrame, transformer: Callable) -> DataFrame:
    return fit(data, transformer, target_col_name).transform(data)
