"""Spark ML object dataset"""

from typing import Any, Callable, Dict

from kedro.extras.datasets.pickle import PickleDataSet
from kedro.io.core import DataSetError, get_filepath_str

from utilities.objects import _load_obj


class SparkMLDataSet(PickleDataSet):
    def __init__(
        self,
        filepath: str,
        load_instance: Callable,
        load_args: Dict[str, Any] = None,
        save_args: Dict[str, Any] = None,
    ) -> None:
        super().__init__(filepath)

        self.load_instance = _load_obj(load_instance)

    def _load(self) -> Any:
        load_path = get_filepath_str(self._get_load_path(), self._protocol)
        return self.load_instance.load(load_path)

    def _save(self, data: Any) -> None:
        save_path = get_filepath_str(self._get_save_path(), self._protocol)

        try:
            data.write().overwrite().save(save_path)
        except Exception as exc:
            raise DataSetError(
                f"{data.__class__} was not serialized due to: {exc}"
            ) from exc

        self._invalidate_cache()
