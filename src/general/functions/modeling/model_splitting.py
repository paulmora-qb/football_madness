"""Splitter functions for the training"""

from typing import Dict

from pyspark.sql import DataFrame
from pyspark.sql import functions as f


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

    target_column_name = splitting_params["target_column_name"]
    # Note that the category can be in type float, given that we had to apply stringindexer
    unqiue_category_values = [
        data[0] for data in data.select(target_column_name).distinct().collect()
    ]

    train_size = splitting_params["train_size"]
    fractions = {x: train_size for x in unqiue_category_values}

    seed = splitting_params["seed"]
    train_data = data.sampleBy(target_column_name, fractions=fractions, seed=seed)
    test_data = data.subtract(train_data)

    train_data = train_data.withColumn("split", f.lit("TRAIN"))
    test_data = test_data.withColumn("split", f.lit("TEST"))

    # TODO: Think about validation split
    return train_data.union(test_data)

