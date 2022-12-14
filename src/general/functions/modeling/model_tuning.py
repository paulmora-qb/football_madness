"""Fine-tune hyper-parameters for modeling"""

from random import Random
from typing import Any, Callable, Dict, List

from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.sql import DataFrame


def _get_param_grid_ready(
    estimator: Any, param_dict: Dict[str, List[Any]]
) -> ParamGridBuilder:
    """This function builds the parameter grid needed for the cross validation

    Args:
        estimator (Any): The estimator function needed for the forecasting
        param_dict (Dict[str, List[Any]]): A dictionary including the name of parameter
            that should be cross validated and the according values

    Returns:
        ParamGridBuilder: A build parameter grid builder
    """

    param_grid_instance = ParamGridBuilder()
    for parameter_string_name, parameter_values in param_dict.items():
        param = estimator.getParam(parameter_string_name)
        param_grid_instance = param_grid_instance.addGrid(param, parameter_values)
    return param_grid_instance.build()


def tuner(
    data: DataFrame, tuner: Callable, param_grid: Dict[str, str], target_col_name
) -> Dict[str, str]:

    param_grid_instance = _get_param_grid_ready(tuner.getEstimator(), param_grid)
    tuner.setParams(estimatorParamMaps=param_grid_instance)

    from pyspark.ml.classification import RandomForestClassifier

    RandomForestClassifier().fit(data)

    tuner.fit(data)

