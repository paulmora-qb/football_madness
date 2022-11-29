"""This file contains nodes for the data creation"""

from kedro.pipeline import node

from .extract_transform_load import concatenate_raw_data, filter_rename_raw_data

concatenate_raw_data_node = node(
    func=concatenate_raw_data,
    inputs=["all_euro_data_2000_2023"],
    outputs="concatenated_raw_data",
    name="create_raw_data",
    tags=["data_creation"],
)

filter_rename_raw_data_node = node(
    func=filter_rename_raw_data,
    inputs=[
        "concatenated_raw_data",
        "filtered_dataset_schema",
        "params:data_preparation.filter_params",
        "params:data_preparation.renaming_columns",
    ],
    outputs="filtered_league_data",
    name="filter_rename_raw_data",
    tags=["data_creation"],
)
