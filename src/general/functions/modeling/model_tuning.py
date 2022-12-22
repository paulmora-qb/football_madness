"""Fine-tune hyper-parameters for modeling"""

from re import L
from typing import Any, Callable, Dict, List, Tuple

import numpy as np
from pyspark.ml.tuning import ParamGridBuilder
from pyspark.sql import DataFrame
from pyspark.sql import functions as f


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


def tuner(data: DataFrame, tuner: Callable, param_grid: Dict[str, str]):
    """The tuner function cross-validates the provided model and parameter grid.
    Afterwards we return the best fitted model and the parameters of the best fitted
    model

    Args:
        data (DataFrame): DataFrame to be cross-validated
        tuner (Callable): Cross-Validation function
        param_grid (Dict[str, str]): Parameter grid with the string name of the model
            and a list of values to try 

    Returns:
        Tuple[Callable, Dict[str, str]]: Fitted model and best parameter set
    """

    param_grid_instance = _get_param_grid_ready(tuner.getEstimator(), param_grid)
    tuner.setParams(estimatorParamMaps=param_grid_instance)

    fitted_tuner = tuner.fit(data.filter(f.col("split") == "TRAIN"))

    best_params = fitted_tuner.getEstimatorParamMaps()[
        np.argmax(fitted_tuner.avgMetrics)
    ]
    best_params = {key.name: value for key, value in best_params.items()}

    return (fitted_tuner.bestModel, best_params)

