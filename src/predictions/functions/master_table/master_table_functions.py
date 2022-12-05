"""Master table functions"""

from typing import Dict

from kedro.framework.session import get_current_session
from kedro.io.core import DataSetNotFoundError
from pyspark.sql import DataFrame


def _load_dataset_using_string(catalog_name: str) -> DataFrame:
    current_session = get_current_session()
    catalog = current_session.load_context().catalog

    try:
        data = catalog.load(catalog_name)
        return data
    except DataSetNotFoundError:
        print(f"Dataset called {catalog_name} cannot be found in the catalog")


def adding_features_to_master_table(data, params: Dict[str, str]) -> DataFrame:

    for ftr_catalog_name in params["feature_families"]:
        a = _load_dataset_using_string(ftr_catalog_name)
