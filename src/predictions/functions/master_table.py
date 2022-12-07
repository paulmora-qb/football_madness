"""Master table functions"""

from typing import Dict, List

from kedro.framework.session import get_current_session
from kedro.io.core import DataSetNotFoundError
from pyspark.sql import DataFrame
from pyspark.sql import functions as f


def _load_datasets_using_string(catalog_names: List[str]) -> List[DataFrame]:
    """This function loads the dataframes which are indicated in the inputted list.

    Args:
        catalog_names (List[str]): List of all the dataframe names which have to be
            indicated in the catalog.

    Returns:
        List[DataFrame]: List of all pyspark dataframes loaded through the configuration
    """
    current_session = get_current_session()
    catalog = current_session.load_context().catalog

    dfs = []
    for cg_name in catalog_names:
        try:
            data = catalog.load(cg_name)
            dfs.append(data)
        except DataSetNotFoundError:
            print(f"Dataset called {cg_name} cannot be found in the catalog")
    return dfs


def _retrieve_team_features(
    ftr_dataframe: DataFrame, team_indication: str
) -> DataFrame:
    """This function get the feature family inputted, filters for the home/ away team,
    renames several columns and returns the dataframe.

    Args:
        ftr_dataframe (DataFrame): Dataframe containing a feature family
        team_indication (str): Indication of whether the team is 'home' or 'away'

    Returns:
        DataFrame: Dataframe containing either the 'home' OR 'away' feature part with
            renamed columns
    """

    non_feature_columns = ["date", "home_away_indication", "league"]
    ftr_team_segment = ftr_dataframe.filter(
        f.col("home_away_indication") == team_indication
    ).drop(*["home_away_indication"])
    feature_columns = [
        col for col in ftr_team_segment.columns if col not in non_feature_columns
    ]
    for col in feature_columns:
        ftr_team_segment = ftr_team_segment.withColumnRenamed(
            col, f"{team_indication}_{col}"
        )
    return ftr_team_segment


def create_match_data_master_table(
    match_data: DataFrame, params: Dict[str, str]
) -> DataFrame:
    """This function is only placed in order to write a proper pytest for the actual
    function. This is necessary since we are dealing with loading datasets through
    parameter files, which is hardly testable in pytest files

    Args:
        match_data (DataFrame): Dataframe containing all information of who played
            against whom and on which date
        params (Dict[str, str]): Dictionary containing information about which feature
            families are going to be used

    Returns:
        DataFrame: Master table dataframe
    """

    feature_dfs = _load_datasets_using_string(params["feature_families"])
    return adding_features_to_master_table(match_data, *feature_dfs)


def adding_features_to_master_table(
    match_data: DataFrame, *dfs: List[DataFrame]
) -> DataFrame:
    """This function creates the master table which is the basis for the modelling
    pipeline. In here we are taking the match_data table, which contains the information
    about every game that was played and the date of when it was played. We are then
    merging all features to this 'spine'. We are doing that separately for the home
    and away team of course.

    Args:
        match_data (DataFrame): Dataframe containing the match data for all games and
            on which date the game was played
        params (Dict[str, str]): Parameters including the feature families that we are
            merging towards the master spine

    Returns:
        DataFrame: The master table is returned, which is the match_data 'spine' with
            all the feature families attached to it
    """

    for df in dfs:
        for team_indication in ["away", "home"]:
            ftr_dataframe_single_segment = _retrieve_team_features(df, team_indication)
            match_data = match_data.join(
                ftr_dataframe_single_segment,
                on=[f"{team_indication}_team", "date", "league"],
                how="left",
            )
        return match_data

