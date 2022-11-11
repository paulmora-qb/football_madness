"""General helper functions"""

from typing import Dict

import pandas as pd
from tqdm import tqdm


def _concatenate_data(partitioned_data_dict: Dict) -> pd.DataFrame:

    result = pd.DataFrame()

    list(partitioned_data_dict.values())[0]()
    for partition_key, partition_load_func in tqdm(
        sorted(partitioned_data_dict.items())
    ):
        partition_data = partition_load_func()  # load the actual partition data
        # concat with existing result
        result = pd.concat([result, partition_data], ignore_index=True, sort=True)

    return result
