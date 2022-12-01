"""Function with create window aggregations"""

from typing import List

from pyspark.sql import DataFrame
from pyspark.sql import functions as f
from pyspark.sql.window import Window

from general.pkgs.utilities.helper import load_obj


def create_window_aggregates(
    partition_by: List[str],
    order_by: List[str],
    aggregation_columns: List[str],
    aggregation_type: str,
    aggregation_range: List[int],
    prefix: str,
    suffix: str,
) -> List[f.col]:

    output_column_list = []

    for col in aggregation_columns:
        start_range = aggregation_range[0]
        end_range = aggregation_range[1]
        w = (
            Window.partitionBy(partition_by)
            .orderBy(order_by)
            .rangeBetween(start_range, end_range)
        )

        output_column_name = f"{prefix}_{col}_{suffix}"
        output_column_list.append(
            load_obj(aggregation_type)(f.col(col)).over(w).alias(output_column_name)
        )
    return output_column_list

