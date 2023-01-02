"""Function to create the report of the feature importance"""

from typing import Callable, List

import pandas as pd


def calculate_feature_importance(
    model: Callable, feature_name_list: List[str]
) -> pd.DataFrame:
    """This function extracts the feature importance from the model and puts
    it together with the corresponding feature name into a dataframe.

    Args:
        model (Callable): Model from which we are extracting the feature importance
        feature_name_list (List[str]): List of column names

    Raises:
        ValueError: Raised when the model does not have the feature importance
            attribute

    Returns:
        pd.DataFrame: Dataframe containing the feature importance and corresponding
            column name
    """

    if not hasattr(model, "featureImportances"):
        raise ValueError(
            "One must use a model which has a feature importance attribute"
        )

    return pd.DataFrame(
        {
            "feature_importance": model.featureImportances.toArray(),
            "feature_column_name": feature_name_list,
        }
    ).sort_values(by="feature_importance", ascending=False)

