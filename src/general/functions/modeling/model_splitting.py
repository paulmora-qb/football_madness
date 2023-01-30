"""Splitter functions for the training"""

from typing import Dict

from pyspark.sql import DataFrame
from pyspark.sql import functions as f
from pyspark.sql.types import IntegerType


def split_train_test_dataframe(
    data: DataFrame, splitting_params: Dict[str, str]
) -> DataFrame:
    """This function splits the dataframe into two, assigns a new column with which we
    are indicating whether the observation belongs to the training or testing dataset.
    Note that we are using the .sampleBy command which does not gurantee an exact amount
    but rather an approximation.

    Args:
        data (DataFrame): Dataframe with features and target
        splitting_params (Dict[str, str]): Dictionary containing the information for
            the split
    Returns:
        Dataframe: Inputted dataframe with an additional column indicating whether
            observation belongs to train or test 
    """

    split_date_col_name = splitting_params["split_date_col_name"]
    number_of_test_time = splitting_params["test_period_years"]
    number_of_train_time = splitting_params["train_period_years"]

    max_test_year = data.select(f.max(f.col(split_date_col_name))).collect()[0][0]
    min_test_year = max_test_year - number_of_test_time
    min_train_year = min_test_year - number_of_train_time

    # Remove all data before the min train year
    data = data.filter(f.col(split_date_col_name) > min_train_year)

    # Create the indication whether data point belongs to train or test
    data.withColumn(
        "split",
        f.when(f.col("season_end_year") <= min_test_year, "TRAIN").otherwise("TEST"),
    )

    return train_data.union(test_data)

