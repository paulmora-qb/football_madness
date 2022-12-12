"""Master table functions"""

from typing import Any, Dict, List

from kedro.framework.session import get_current_session
from kedro.io.core import DataSetNotFoundError
from pyspark.sql import DataFrame
from pyspark.sql import functions as f


def _load_dataframe_using_string(catalog, catalog_name: str) -> DataFrame:
    """Loading the dataframe using a string which points to an entry in the catalog

    Args:
        catalog: Catalog of the current session
        catalog_name (str): Name of the dataframe that should be loaded

    Returns:
        DataFrame: Loaded dataframe
    """

    try:
        data = catalog.load(catalog_name)
        return data
    except DataSetNotFoundError:
        print(f"Dataset called {catalog_name} cannot be found in the catalog")


def _load_datasets(catalog_names: Any) -> Any:
    """This function loads datasets either through a list or a string which is tested

    Args:
        catalog_names (Any): Either a string with a dataframe name, or an entire list
            of them

    Returns:
        Any: Loaded dataframe(s)
    """
    current_session = get_current_session()
    catalog = current_session.load_context().catalog

    if isinstance(catalog_names, list):
        dfs = []
        for cg_name in catalog_names:
            dfs.append(_load_dataframe_using_string(catalog, cg_name))
        return dfs
    else:
        return _load_dataframe_using_string(catalog, catalog_names)


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

    non_feature_columns = ["team", "date", "home_away_indication", "league"]
    ftr_team_segment = ftr_dataframe.filter(
        f.col("home_away_indication") == team_indication
    ).drop(*["home_away_indication"])
    feature_columns = [
        col for col in ftr_team_segment.columns if col not in non_feature_columns
    ]

    # Adding an indication to all features whether they belong to the home/ away team
    for col in feature_columns:
        ftr_team_segment = ftr_team_segment.withColumnRenamed(
            col, f"{col}_{team_indication}"
        )

    # Adding an information whether the team is home/ away
    ftr_team_segment = ftr_team_segment.withColumnRenamed(
        "team", f"{team_indication}_team"
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

    feature_dfs = _load_datasets(params["feature_families"])
    target_df = _load_datasets(params["target_df"])
    return adding_features_to_master_table(match_data, target_df, *feature_dfs)


def adding_features_to_master_table(
    match_data: DataFrame, target_df: DataFrame, *feature_dfs: List[DataFrame],
) -> DataFrame:
    """This function creates the master table which is the basis for the modelling
    pipeline. In here we are taking the match_data table, which contains the information
    about every game that was played and the date of when it was played. We are then
    merging all features to this 'spine'. We are doing that separately for the home
    and away team of course.

    Args:
        match_data (DataFrame): Dataframe containing the match data for all games and
            on which date the game was played
        target_df (DataFrame): Dataframe containing the target variable
        feature_dfs* (List[DataFrame]): List of pyspark dataframes to be added to the
            master table

    Returns:
        DataFrame: The master table is returned, which is the match_data 'spine' with
            all the feature families attached to it
    """

    for df in feature_dfs:
        for team_indication in ["away", "home"]:
            ftr_dataframe_single_segment = _retrieve_team_features(df, team_indication)
            match_data = match_data.join(
                ftr_dataframe_single_segment,
                on=[f"{team_indication}_team", "date", "league"],
                how="left",
            )
    match_data = match_data.join(
        target_df, on=["home_team", "away_team", "date", "league"], how="inner"
    )

    return match_data

