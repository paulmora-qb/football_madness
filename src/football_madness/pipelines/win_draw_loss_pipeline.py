"""Creating the pipeline for prediticting the results"""

from functools import partial

from kedro.pipeline import Pipeline, node

from general.functions.model_development.master_table.master import (
    create_match_data_master_table,
)
from general.functions.model_development.master_table.target_variable import (
    create_target_variable,
)
from general.nodes.preprocessing.transformer import fit, transform
from general.pipelines import modeling_pipeline, post_train_eda_pipeline
from utilities.helper import update_dictionary


def create_master_pipeline() -> Pipeline:

    target_creation_node = Pipeline(
        nodes=[
            node(
                func=create_target_variable,
                inputs=["concatenated_raw_data", "params:target_variable"],
                outputs="intermediate_target_variable",
                name="create_target_variable",
                tags=["master_table"],
            ),
            node(
                func=partial(fit, split_data=False),
                inputs={
                    "data": "intermediate_target_variable",
                    "transformer": "params:target_variable.encoder",
                },
                outputs="fitted_target_variable_encoder",
                name="fitting_target_variable_encoder",
                tags=["master_table"],
            ),
            node(
                func=transform,
                inputs={
                    "data": "intermediate_target_variable",
                    "transformer": "fitted_target_variable_encoder",
                },
                outputs="target_variable",
                name="transform_target_variable_encoder",
                tags=["master_table"],
            ),
            node(
                func=lambda x: x.labels,
                inputs="fitted_target_variable_encoder",
                outputs="target_encoder_index_labels",
                name="extracting_labels_from_target_encoder",
                tags=["master_table"],
            ),
        ]
    )

    master_table_node = node(
        func=create_match_data_master_table,
        inputs=["match_spine", "params:master_table"],
        outputs="master_table",
        name="create_master_table",
        tags=["master_table"],
    )

    nodes = [target_creation_node, master_table_node]

    return Pipeline(nodes)


def create_pipeline(categorical_target: bool) -> Pipeline:
    return (
        create_master_pipeline()
        + modeling_pipeline.create_modeling_pipeline(
            model_type="classification", categorical_target=categorical_target
        )
        + post_train_eda_pipeline.create_pipeline(
            model_type="classification",
            tree_model=True,
            categorical_target=categorical_target,
        )
    )
