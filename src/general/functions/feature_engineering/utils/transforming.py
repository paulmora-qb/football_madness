"""Function from transforming"""

from itertools import chain
from typing import Any, Dict, List

from pyspark.sql import functions as f


def dict_replace(input_col: str, dictionary: Dict[str, Any], column_name: str) -> f.col:
    """Function to replace pyspark column values

    Args:
        input_col str: Column of the dataframe to alter
        dictionary (Dict[str, Any]): Dictionary with which to replace column
        column_name (str): Column name which are used as an alias

    Returns:
        f.col: Column with the replaced values
    """

    mapping_expr = f.create_map([f.lit(x) for x in chain(*dictionary.items())])

    return mapping_expr[f.col(input_col)].alias(column_name)

