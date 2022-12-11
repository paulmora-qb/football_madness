"""Imputation functions for the modeling"""

from typing import Dict

from pyspark.sql import DataFrame

from utilities.helper import _find_list_elements_using_keyword, load_obj


def impute_dataframe(data: DataFrame, params: Dict[str, str]) -> DataFrame:

    imputer = load_obj(params["function"])(**params["function_params"])

    data_column_names = data.columns
    feature_column_names = _find_list_elements_using_keyword(data_column_names, "ftr_")

    imputer.setInputCols(["away_window_full_time_goals_row3_row1"])
    imputer.setOutputCols(["away_window_full_time_goals_row3_row1"])

    model = imputer.fit(data)

    model.transform(data).select(*["away_window_full_time_goals_row3_row1",]).show()

    data.isnan()

    a = 1
    pass
