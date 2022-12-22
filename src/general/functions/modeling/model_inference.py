"""Model prediction functions"""

from typing import Callable

from pyspark.ml import functions as mlf
from pyspark.sql import DataFrame
from pyspark.sql import functions as f


def model_prediction(
    data: DataFrame,
    trained_model: Callable,
    prediction_suffix: str,
    prediction_proba_suffix: str,
) -> DataFrame:

    prediction_col_name = trained_model.getLabelCol() + prediction_suffix
    trained_model.setPredictionCol(prediction_col_name)
    prediction_proba_col_name = trained_model.getLabelCol() + prediction_proba_suffix
    trained_model.setProbabilityCol(prediction_proba_col_name)

    prediction_data = trained_model.transform(data)

    testdf = (
        prediction_data.withColumn(
            "test", mlf.vector_to_array(prediction_proba_col_name)
        )
        .select("test")
        .show()
    )

    testdf.withColumnRenamed("test[0]", "something_else")

    return prediction_data

