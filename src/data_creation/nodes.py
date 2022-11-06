"""This file contains nodes for the data creation"""

from kedro.pipeline import node

from .get_raw_data import load_raw_data

prm_raw_data = [
    node(
        func=load_raw_data,
        inputs=["params:raw_data_creation"],
        outputs="raw_data",
        name="create_raw_data",
        tags=["data_creation"],
    )
]
