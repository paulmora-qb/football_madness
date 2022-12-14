"""General helper functions"""

import importlib
import re
from typing import Any, Callable, Dict, Iterable, List

from pyspark.sql import SparkSession


def sentence_case(string: str) -> str:
    """Function to convert the camel case into a sentence case

    Args:
        string (str): String in camel case format

    Returns:
        str: String in sentence case format
    """
    if string != "":
        result = re.sub("([A-Z])", r" \1", string)
        return result[:1].upper() + result[1:].lower()
    return


def update_dictionary(
    original_dict: Dict[str, str], key: str, value: Any, level_name: str = None
) -> Dict[str, str]:

    if level_name:
        for iter_key in level_name.split("."):
            if iter_key not in original_dict.keys():
                raise ValueError("Pathing of the dictionary is not correctly stated")
            else:
                original_dict = original_dict[iter_key]

        original_dict[key] = value
    else:
        original_dict[key] = value
    return original_dict


def _find_list_elements_using_keyword(
    lst: List, including_keyword: str = None, excluding_keyword: str = None
) -> List:

    if including_keyword:
        lst = [x for x in lst if including_keyword in x]

    if excluding_keyword:
        lst = [x for x in lst if excluding_keyword not in x]

    return lst


def _get_spark_session() -> SparkSession:
    return SparkSession.builder.getOrCreate()


def _validate_keys(provided_keys: Iterable[str], mandatory_keys: List[str]):
    missing_keys = set(mandatory_keys).difference(provided_keys)
    if missing_keys:
        raise KeyError(f"The following key(s) are missing: {missing_keys}")


def load_obj(object_path: str) -> Callable:

    object_path, object_name = object_path.rsplit(".", 1)
    module_object = importlib.import_module(object_path)

    if not hasattr(module_object, object_name):
        raise AttributeError(
            f"The object {object_path} does not have function {object_name}"
        )
    return getattr(module_object, object_name)
