"""Function for loading the raw data"""

from typing import Any, Dict, List

import pandas as pd
from datapackage import Package
from tqdm import tqdm


def load_raw_data(params: Dict[str, Any]) -> pd.DataFrame:

    league_list = params["leagues_list"]
    master_data = pd.DataFrame()
    for league in tqdm(league_list):
        api_call = _create_api_link(params, league)
        retrieved_data = _extract_data(api_call, league)
        master_data = master_data.append(retrieved_data)

    return master_data


def _create_api_link(params: Dict[str, Any], league: str) -> str:
    """Creating a list of api call links. This is useful if we would like to scrape
    the informations from multiple football leagues.

    Args:
        params (Dict[str, Any]): Information about the api link and which league we
            are interested int

    Returns:
        List[str]: List of api call links
    """

    return "/".join([params["api_start"], league, params["api_end"]])


def _extract_data(api_link: str, league: str) -> pd.DataFrame:

    temp_master_data = pd.DataFrame()
    package = Package(api_link)
    csv_file_names = [x for x in package.resource_names if x.endswith("_csv")]

    for file_name in tqdm(csv_file_names):
        list_results = package.get_resource(file_name)
        data = pd.DataFrame(list_results.read(keyed=True))
        assert (
            len(data) > 0
        ), f"The api call for {file_name} for the league {league} has zero entries"

        data.loc[:, "league"] = league
        temp_master_data = temp_master_data.append(data)

    return temp_master_data
