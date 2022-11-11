"""Function for loading the raw data"""

from typing import Any, Dict, List

import pandas as pd
from datapackage import Package
from tqdm import tqdm

from src.general.pkgs.utilities.helper import _concatenate_data


def load_raw_data(partition_data_dict, params: Dict[str, Any]) -> pd.DataFrame:

    concat_data = _concatenate_data(partition_data_dict)
    a = 1
