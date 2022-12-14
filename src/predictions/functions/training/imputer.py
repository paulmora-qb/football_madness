"""Imputation functions for the modeling"""

from typing import Dict

from pyspark.sql import DataFrame

from utilities.objects import adj_params
from utilities.helper import load_obj


def impute_dataframe(data: DataFrame, params: Dict[str, str]) -> DataFrame:

    function_params = params["function_params"]
    function_params["inputCols"] = params["feature_names"]
    function_params["outputCols"] = params["feature_names"]

    imputer = load_obj(params["function"])(**function_params)
    model = imputer.fit(data)
    return model.transform(data)

