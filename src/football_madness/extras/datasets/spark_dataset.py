"""Dataset for Spark ML objects"""
from typing import Callable

from kedro.extras.datasets.spark.spark_dataset import SparkDataSet


class SparkMLDataSet(SparkDataSet):
    def _save(self, transformer: Callable) -> None:
        save_path = SparkDataSet._strip_dbfs_prefix(
            self._fs_prefix + str(self._get_save_path())
        )
        transformer.save(save_path, **self._save_args)

