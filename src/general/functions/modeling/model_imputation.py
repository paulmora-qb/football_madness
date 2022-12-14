"""Model imputation functions"""

from typing import Dict

from pyspark.sql import DataFrame


def imputer(data: DataFrame, params: Dict[str, str]) -> DataFrame:

    # TODO: Split the fit and the transform
    model = params["transformer"].fit(data)
    return model.transform(data)

