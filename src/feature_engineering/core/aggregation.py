"""Function with create window aggregations"""

from typing import List

from pyspark.sql import DataFrame
from pyspark.sql import functions as f
from pyspark.sql.window import Window

from general.pkgs.utilities.helper import load_obj


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

