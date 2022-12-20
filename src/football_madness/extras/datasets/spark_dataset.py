"""Dataset for Spark ML objects"""
from typing import Callable

from kedro.extras.datasets.spark.spark_dataset import SparkDataSet, _strip_dbfs_prefix
from pyspark.ml.pipeline import PipelineModel


class SparkMLDataSet(SparkDataSet):
    def _save(self, transformer: Callable) -> None:
        """This function helps with fitted instances of pyspark objects. The issue
        with the original sparkdataset is that the save option contains the `write`
        function which does not comply with pyspark objects. We therefore simply removed
        it, while importing the rest from the original SparkDataSet parent class

        Args:
            transformer (Callable): Fitted transformer instance
        """
        save_path = _strip_dbfs_prefix(self._fs_prefix + str(self._get_save_path()))
        transformer.write().overwrite().save(save_path)

    def _load(self) -> Callable:
        """The function to load the fitted instance of a ML model.

        Returns:
            Callable: Fitted ml instance
        """
        load_path = _strip_dbfs_prefix(self._fs_prefix + str(self._get_load_path()))
        # model = PipelineModel.load(load_path)
        # from pyspark.ml.util import MLReader, DefaultParamsReadable

        # return model

