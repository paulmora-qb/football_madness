"""This file contains nodes for the data creation"""

from kedro.pipeline import node

from .get_raw_data import load_raw_data

prm_raw_data = [
    node(
        func=load_raw_data,
        inputs=["all_euro_data_2001_2023", "params:raw_data_creation"],
        outputs="test",
        name="create_raw_data",
        tags=["data_creation"],
    )
]
