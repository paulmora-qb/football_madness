"""Nodes for the creation of the master table"""

from kedro.pipeline import node

from ...functions.master_table.master_table import create_match_data_master_table
from ...functions.master_table.target_variable import create_target_variable

target_creation_node = node(
    func=create_target_variable,
    inputs=["concatenated_raw_data", "params:target_variable"],
    outputs="target_variable",
    name="create_target_variable",
    tags=["master_table"],
)


master_table_node = node(
    func=create_match_data_master_table,
    inputs=["match_spine", "params:master_table"],
    outputs="master_table",
    name="create_master_table",
    tags=["master_table"],
)

