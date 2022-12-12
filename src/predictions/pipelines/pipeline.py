"""Creating the pipeline for prediticting the results"""

from kedro.pipeline import Pipeline, node

from .nodes.master_table import master_table_node, target_creation_node
from .nodes.training import (
    data_dictionary_node,
    hyperparameter_tuning_node,
    imputing_nodes,
    splitting_node,
)


def create_master_pipeline() -> Pipeline:
    nodes = [target_creation_node, master_table_node]
    return Pipeline(nodes)


def create_training_pipeline() -> Pipeline:
    nodes = [
        data_dictionary_node,
        splitting_node,
        imputing_nodes,
        hyperparameter_tuning_node,
    ]
    return Pipeline(nodes)


def create_modeling_pipeline() -> Pipeline:
    return create_master_pipeline() + create_training_pipeline()
