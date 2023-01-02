"""Functions/ Classes for the creation of data dictionary"""

from multiprocessing.sharedctypes import Value
from typing import List

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

    def get_target_column(self) -> str:
        """This function returns the target column name. For now we allow only for one
        target variable column.

        Returns:
            str: The name of the target column
        """
        tgt_column_name_list = [x for x in self.column_names if x.startswith("tgt_")]
        if len(tgt_column_name_list) > 1:
            raise ValueError("There should not be more than one target variable")
        else:
            return tgt_column_name_list[0]


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

