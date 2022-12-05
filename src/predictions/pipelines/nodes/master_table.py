"""Nodes for the creation of the master table"""

from kedro.pipeline import node

from ...functions.master_table.master_table_functions import (
    adding_features_to_master_table,
)

master_table_node = node(
    func=adding_features_to_master_table,
    inputs=["match_spine", "params:master_table"],
    outputs="master_table",
    name="creation_of_master_table",
    tags=["master_table"],
)

