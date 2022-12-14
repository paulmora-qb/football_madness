"""Fine-tune hyper-parameters for modeling"""

from typing import Any, Dict, List

from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.sql import DataFrame
from pyspark.sql import functions as f

from utilities.helper import load_obj


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


def hp_tuning(
    data: DataFrame, model_params: Dict[str, Any], cv_params: Dict[str, Any],
) -> Dict[str, str]:

    function_name = model_params.pop("function")
    function = load_obj(function_name)(**model_params)

    param_grid = cv_params["param_grid"]

    # TODO: think about how to use that gridbuilder and getparams in an automated way

    train_data = data.filter(f.col("split") == "TRAIN")

    cv_instance_name = cv_params.pop("function")
    param_grid = cv_params.pop("param_grid")
    cv_params["estimatorParamMaps"] = _get_param_grid_ready(function, param_grid)

    cross_validator = load_obj(cv_instance_name)(**cv_params)

    # func = load_obj(params["function"])

    # CrossValidator()
