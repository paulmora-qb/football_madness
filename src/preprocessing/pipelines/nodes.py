"""This file contains nodes for the data creation"""

from kedro.pipeline import node

from ..functions.extract_transform_load import concatenate_raw_data

concatenate_raw_data_node = node(
    func=concatenate_raw_data,
    inputs=[
        "all_euro_data_2000_2023",
        "filtered_schema",
        "params:data_preparation.renaming_columns",
    ],
    outputs="concatenated_raw_data",
    name="create_raw_data",
    tags=["data_creation"],
)
