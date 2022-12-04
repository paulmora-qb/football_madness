"""Creating the pipeline for prediticting the results"""

from kedro.pipeline import Pipeline, node

from .nodes.master_table.master_table_functions import adding_features_to_master_table


def create_master_pipeline() -> Pipeline:

    nodes = [
        # Add features to league master
        node(
            func=adding_features_to_master_table,
            inputs=["league_spine", "params:master_table"],
            outputs="master_table",
            name="adding_features_to_master_table",
            tags=["master_table"],
        ),
    ]
    return Pipeline(nodes)


def create_modeling_pipeline() -> Pipeline:
    return create_master_pipeline()

