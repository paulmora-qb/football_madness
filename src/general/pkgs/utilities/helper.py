"""General helper functions"""

from typing import Callable, Iterable, List

from pyspark.sql import SparkSession


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
    a = 1
    pass
