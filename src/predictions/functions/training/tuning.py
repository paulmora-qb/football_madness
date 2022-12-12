"""Fine-tune hyper-parameters for modeling"""

from typing import Dict

from pyspark.ml.tuning import ParamGridBuilder
from pyspark.sql import DataFrame

from utilities.helper import load_obj


def hp_tuning(
    data: DataFrame, model_params: Dict[str, str], cv_params: Dict[str, str]
) -> Dict[str, str]:

    a = 1
    # func = load_obj(params["function"])

    # CrossValidator()
