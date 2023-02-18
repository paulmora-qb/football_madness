"""Model helper functions"""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as f


def creating_weight_col(
    data: DataFrame, target_col_name: str, weight_col_name: str
) -> DataFrame:
    """This function creates a weighting column which can be used to fight a potential
    imbalance within a classification problem. This is done by calculating the frequency
    of the target column for every category and inverse this column. The resulting
    column is then labelled the 'weight_column' which is then passed further to the
    modeling function. This is of course not needed for all models.

    Args:
        data (DataFrame): DataFrame containing the target column
        target_col_name (str): Name of the target column
        weight_col_name (str): Name the weight column should be referred to

    Returns:
        DataFrame: Dataframe containing the weight column
    """
    return data.withColumn(
        weight_col_name,
        data.count()
        / f.count(target_col_name).over(Window.partitionBy(target_col_name)),
    )

