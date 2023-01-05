"""Basic functions to interact across columns"""

from typing import List

from pyspark.sql import Column


def averaging(list_of_columns: List[str]) -> List[Column]:
    # TODO: Ignoring nans
    return sum(list_of_columns) / len(list_of_columns)
