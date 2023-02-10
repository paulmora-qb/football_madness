"""Preprocessing the parameter file"""

from importlib import import_module
from typing import Any, Dict, Set

from general.functions.feature_engineering.aggregation import create_window_column_name


def _get_column_names(params: Dict[str, Any], function_name: str) -> Set[str]:
    """_summary_

    Args:
        params (Dict[str, Any]): _description_

    Returns:
        Set[str]: _description_
    """
    columns_created = set()

    if function_name == "create_window_aggregates":
        if params["rows_between"]:
            ranges = params["rows_between"]
            range_str = "row"
        else:
            ranges = params["range_between"]
            range_str = "range"

        for col in params["aggregation_columns"]:
            for range in ranges:
                output_column_name = create_window_column_name(
                    prefix=params["prefix"],
                    col=col,
                    range_str=range_str,
                    range=range,
                    suffix=params["suffix"],
                )
                columns_created.add(output_column_name)

    else:
        columns_created.add(params["column_name"])

    return columns_created


def preprocess_params(params: Dict[str, str]) -> Dict[str, str]:
    """This function adds if needed some more information to the feature engineering
    functions

    Args:
        params (Dict[str, str]): The original parameters

    Returns:
        Dict[str, str]: A potentially updated version of the parameters
    """

    updated_parameters = {}
    generated_column_names = set()

    for feature_group, feature_group_list in params.items():

        new_feature_group_list = []
        for feature_dict in feature_group_list:
            module_name, function_name = feature_dict["object"].rsplit(".", 1)
            if function_name.startswith("dynamic"):
                module_object = import_module(module_name)
                dynamic_function = getattr(module_object, function_name)
                updated_feature_dict, column_names = dynamic_function(
                    existing_columns=generated_column_names,
                    left_column=feature_dict["left_column"],
                    right_column=feature_dict["right_column"],
                    math_operation=feature_dict["math_operation"],
                    output_column_name=feature_dict["output_column"],
                )
            else:
                column_names = _get_column_names(feature_dict, function_name)
                generated_column_names = generated_column_names.union(column_names)
                new_feature_group_list.append(feature_dict)

        updated_parameters[feature_group] = new_feature_group_list
    return updated_parameters
