"""Functions/ Classes for the creation of data dictionary"""

from typing import Dict, List

from pyspark.sql import DataFrame


class DataDictionary:
    def __init__(self, column_names: List[str]):
        self.column_names = column_names

    def get_feature_columns(self) -> List[str]:
        """This functions returns the list of column names that we treat as features.
        They have the characteristics that they start with ftr.

        Returns:
            List[str]: List of column names that start with ftr_ and are therefore to be
                treated as features
        """
        return [x for x in self.column_names if x.startswith("ftr_")]


def create_data_dictionary(master_table: DataFrame) -> DataDictionary:
    """This function initializes the data dictionary with the parameters and
    outputs it afterwards

    Args:
        master_table (DataFrame): pyspark dataframe which contains all the features and
            key columns, etc.
        params (Dict[str, str]): Dictionary with the relevant settings for the data
            dictionary

    Returns:
        DataDictionary: Data Dictionary class instance
    """

    return DataDictionary(master_table.columns)

