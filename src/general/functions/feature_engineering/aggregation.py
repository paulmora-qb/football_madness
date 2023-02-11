"""Function with create window aggregations"""

import re
from itertools import product
from typing import Dict, List, Tuple

from pyspark.sql import functions as f
from pyspark.sql.window import Window

from utilities.helper import load_obj


def create_horizontal_averages(
    aggregation_columns: List[str], column_name: str,
) -> f.col:
    """This function takes a horizontal average out of the column specified in the
    aggregation column. This is done relatively manually, by summing the columns
    and then dividing by their number.

    Given the occurrence of potential nans, we are summing the number of non-nan columns
    and sum before we are dealing with the ratio

    Args:
        aggregation_columns (List[str]): List of columns that should be aggregated
        column_name (str): Column name of the resulting average

    Returns:
        f.col: Renamed average column 
    """

    number_of_non_nans = sum(
        (~f.isnan(f.col(x))).cast("int") for x in aggregation_columns
    )

    total_sum_of_all_bets = sum(
        f.nanvl(f.col(x), f.lit(0)) for x in aggregation_columns
    )

    return (total_sum_of_all_bets / number_of_non_nans).alias(column_name)


def dynamic_post_window_aggregation(
    existing_columns: List[str],
    left_column: str,
    right_column: str,
    math_operation: str,
    output_column_name: str,
) -> List[Dict]:
    """Function which creates ratios of window functions.

    Args:
        existing_columns (List[str]): List of column names that already exists
        left_column (str): Column on the left side of the equation
        right_column (str): Column name of the right side of the equation
        math_operation (str): Math operation of the columns
        output_column_name (str): Name of the resulting column
    """

    def _extract_regex_match_and_column(
        column: str, existing_columns: List[str]
    ) -> List[Tuple]:
        matched_cols = [re.match(column, col) for col in existing_columns]
        return [
            (c, re_match)
            for c, re_match in zip(existing_columns, matched_cols)
            if re_match
        ]

    def _extract_matching_groupdict(
        dictionary: Dict[str, str], key_master_list: List[str]
    ) -> Dict[str, str]:
        return {
            key: value
            for key, value in dictionary.groupdict().items()
            if key in key_master_list
        }

    newly_created_columns = []
    matched_left_columns = _extract_regex_match_and_column(
        left_column, existing_columns
    )
    matched_right_columns = _extract_regex_match_and_column(
        right_column, existing_columns
    )

    common_capture_group_names = set(
        re.compile(left_column).groupindex.keys()
    ).intersection(set(re.compile(right_column).groupindex.keys()))

    new_columns_list = []
    for (left_col, left_match), (right_col, right_match) in product(
        matched_left_columns, matched_right_columns
    ):

        common_left_regex_dictionary = _extract_matching_groupdict(
            left_match, common_capture_group_names
        )
        common_right_regex_dictionary = _extract_matching_groupdict(
            right_match, common_capture_group_names
        )

        if common_left_regex_dictionary == common_right_regex_dictionary:
            filled_output_column_name = output_column_name.format(
                **common_left_regex_dictionary
            )

            newly_created_columns.append(
                {
                    "object": "general.functions.feature_engineering.flags.expr_flag",
                    "expr": f"{left_col} {math_operation} {right_col}",
                    "column_name": filled_output_column_name,
                }
            )
    return newly_created_columns


def create_window_column_name(
    prefix: str, col: str, range_str: str, range: List[int], suffix: str
) -> str:
    """Creates a column name for a window aggregation

    Args:
        prefix (str): Prefix of the window aggregation name
        col (str): Column that is aggregated
        range_str (str): Indication whether we are talking about rows or ranges
        range (List[int]): List of start/ end range indication
        suffix (str): Suffix of the window aggregation name

    Returns:
        str: Column name for window column
    """
    return (
        f"{prefix}_{col}_{range_str}_{abs(range[0])}"
        + f"_{range_str}_{abs(range[1])}_{suffix}"
    )


def create_window_aggregates(
    partition_by: List[str],
    order_by: List[str],
    aggregation_columns: List[str],
    aggregation_type: str,
    rows_between: List[List[int]] = None,
    range_between: List[int] = None,
    prefix: str = None,
    suffix: str = None,
) -> List[f.col]:
    """This function creates rolling window aggregates for specified partitions,
    orders and aggregation types. Further, this is done for a specified range or
    ranges.

    Args:
        partition_by (List[str]): Column name by which is partitioned
        order_by (List[str]): Column name that is ordered by
        aggregation_columns (List[str]): Column name that is aggregated
        aggregation_type (str): Type of aggregation (e.g. pyspark.sql.functions.sum)
        rows_between (List[List[int]]): Number of rows within the window. Defaults to
            None
        range_between (List[int], optional): Number of date units within the window.
            Defaults to None.
        prefix (str, optional): Prefix for the column name. Defaults to None.
        suffix (str, optional): Suffix for the column name. Defaults to None.

    Returns:
        List[f.col]: List of pyspark columns to be created
    """

    ranges = rows_between or range_between
    if not ranges:
        raise ValueError("One has to specify either rows_between or range_between")

    if isinstance(ranges[0], int):
        ranges = [ranges]

    output_column_list = []

    for col in aggregation_columns:
        for range in ranges:
            window = Window.partitionBy(partition_by).orderBy(order_by)

            if rows_between:
                window = window.rowsBetween(range[0], range[1])
                range_str = "row"
            else:
                window = window.rangeBetween(range[0], range[1])
                range_str = "range"

            output_column_name = create_window_column_name(
                prefix=prefix, col=col, range_str=range_str, range=range, suffix=suffix
            )

            output_column_list.append(
                load_obj(aggregation_type)(f.col(col))
                .over(window)
                .alias(output_column_name)
            )
    return output_column_list

