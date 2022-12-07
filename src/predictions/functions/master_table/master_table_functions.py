"""Master table functions"""

from typing import Dict, List

from kedro.framework.session import get_current_session
from kedro.io.core import DataSetNotFoundError
from pyspark.sql import DataFrame
from pyspark.sql import functions as f


def _load_dataset_using_string(catalog_name: str) -> DataFrame:
    current_session = get_current_session()
    catalog = current_session.load_context().catalog

    try:
        data = catalog.load(catalog_name)
        return data
    except DataSetNotFoundError:
        print(f"Dataset called {catalog_name} cannot be found in the catalog")


def _retrieve_team_features(
    ftr_dataframe: DataFrame, team_indication: str, non_feature_columns: List[str]
):
    ftr_team_segment = ftr_dataframe.filter(
        f.col("home_away_indication") == team_indication
    )
    feature_columns = [
        col for col in ftr_team_segment.columns if col not in non_feature_columns
    ]
    for col in feature_columns:
        ftr_team_segment = ftr_team_segment.withColumnRenamed(
            col, f"{team_indication}_{col}"
        )
    return ftr_team_segment


def adding_features_to_master_table(match_data, params: Dict[str, str]) -> DataFrame:

    non_feature_columns = ["season", "date", "league", "home_away_indication"]
    for ftr_catalog_name in params["feature_families"]:
        ftr_dataframe_both_segments = _load_dataset_using_string(ftr_catalog_name)

        for team_indication in ["away", "home"]:
            ftr_dataframe_single_segment = _retrieve_team_features(
                ftr_dataframe_both_segments, team_indication, non_feature_columns
            )
            match_data = match_data.join(
                ftr_dataframe_single_segment,
                on=[f"{team_indication}_team"] + non_feature_columns,
                how="left",
            )
        return match_data

