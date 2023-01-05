"""Function with create window aggregations"""

from typing import Callable, List

from pyspark.sql import functions as f
from pyspark.sql.window import Window

from utilities.helper import load_obj


def column_aggregation(
    aggregation_columns: List[str],
    functions: List[Callable],
    prefix: str = None,
    suffix: str = None,
) -> List[f.col]:
    """This function aggregates columns. This is done by looping over the functions
    provided in the configuration file.

    Args:
        aggregation_columns (List[str]): List of columns that should be aggregated
        functions (List[Callable]): Functions that should be applied on the aggregate
        prefix (str, optional): Prefix of the column name. Defaults to None.
        suffix (str, optional): Suffix of the column name. Defaults to None.

    Returns:
        List[f.col]: List of columns to be added
    """

    aggregation_column_list = []

    for function in functions:
        column_name = f"{prefix}_{function.__name__}_{suffix}"
        pyspark_columns = [f.col(x) for x in aggregation_columns]
        aggregation_column_list.append(function(pyspark_columns).alias(column_name))

    return aggregation_column_list


def create_last_season_indicator():
    pass


def create_window_aggregates(
    partition_by: List[str],
    order_by: List[str],
    aggregation_columns: List[str],
    aggregation_type: str,
    rows_between: List[int],
    range_between: List[int] = None,
    prefix: str = None,
    suffix: str = None,
) -> List[f.col]:

    ranges = rows_between or range_between
    output_column_list = []

    for col in aggregation_columns:
        window = Window.partitionBy(partition_by).orderBy(order_by)

        if rows_between:
            window = window.rowsBetween(ranges[0], ranges[1])
            range_str = "row"
        else:
            window = window.rangeBetween(ranges[0], ranges[1])
            range_str = "range"

        output_column_name = (
            f"{prefix}_{col}_{range_str}{abs(ranges[0])}_{range_str}{abs(ranges[1])}"
        )

        output_column_list.append(
            load_obj(aggregation_type)(f.col(col))
            .over(window)
            .alias(output_column_name)
        )
    return output_column_list

