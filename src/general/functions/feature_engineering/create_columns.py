"""Creating new columns using leveraging .yml files"""

from typing import Callable, List

from pyspark.sql import Column, DataFrame


def create_column_from_config(
    data: DataFrame, column_params: List[Callable]
) -> DataFrame:
    """This function collects all the newly created functions

    Args:
        data (DataFrame): Dataframe to which we would like to add the newly created
            columns
        column_params (List[Callable]): Either a list of new columns, or a list of lists
            which contain the column functions

    Returns:
        DataFrame: Dataframe with all old columns plus the additionally newly created
            ones
    """

    newly_created_columns = []

    existing_columns = data.columns
    for columns in column_params:
        # If it is only one function, then we have a list of columns and not a list of lists
        if isinstance(columns, Column):
            columns = [columns]

        newly_created_columns += columns

    return data.select(existing_columns + newly_created_columns)
