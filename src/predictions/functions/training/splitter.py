"""Splitter functions for the training"""

from typing import Dict

from pyspark.sql import DataFrame
from pyspark.sql import functions as f


def split_train_test_dataframe(data: DataFrame, params: Dict[str, str]) -> DataFrame:
    """This function splits the dataframe into two, assigns a new column with which we
    are indicating whether the observation belongs to the training or testing dataset.
    Note that we are using the .sampleBy command which does not gurantee an exact amount
    but rather an approximation.

    Args:
        data (DataFrame): Dataframe with features and target
        params (Dict[str, str]): Dictionary containing the information for the split

    Returns:
        Dataframe: Inputted dataframe with an additional column indicating whether
            observation belongs to train or test 
    """

    unique_target_category_values = params["unique_target_category_values"]
    train_size = params["train_size"]
    fractions = {x: train_size for x in unique_target_category_values}

    target_column_name = params["target_column_name"]
    seed = params["seed"]
    train_data = data.sampleBy(target_column_name, fractions=fractions, seed=seed)
    test_data = data.subtract(train_data)

    train_data = train_data.withColumn("split", f.lit("TRAIN"))
    test_data = test_data.withColumn("split", f.lit("TEST"))

    # TODO: Think about validation split
    return train_data.union(test_data)

