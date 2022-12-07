"""Creating the pipeline for prediticting the results"""

from kedro.pipeline import Pipeline, node

from .nodes.master_table import master_table_node
from .nodes.training import splitting_node


def create_master_pipeline() -> Pipeline:
    nodes = [master_table_node]
    return Pipeline(nodes)


def create_training_pipeline() -> Pipeline:
    nodes = [splitting_node]
    return Pipeline(nodes)


def create_modeling_pipeline() -> Pipeline:
    return create_master_pipeline() + create_training_pipeline()
