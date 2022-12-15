"""Creating the pipeline for prediticting the results"""

from kedro.pipeline import Pipeline, node

from general.functions.model_development.master_table.master import (
    create_match_data_master_table,
)
from general.functions.model_development.master_table.target_variable import (
    create_target_variable,
)
from general.pipelines.modeling_pipeline import create_modeling_pipeline


def create_master_pipeline() -> Pipeline:

    target_creation_node = Pipeline(
        nodes=[
            node(
                func=create_target_variable,
                inputs=["concatenated_raw_data", "params:target_variable"],
                outputs="target_variable",
                name="create_target_variable",
                tags=["master_table"],
            ),
            node(
                func=fit_transform,
                inputs={
                    "data": "target_variable",
                    "transformer": "params:target_variable.encoder",
                    "target_col_name": "params:target_variable.target_column_name",
                },
                outputs="encoded_target_variable",
                name="encoding_target_variable",
                tags=["master_table"],
            ),
            node(
                func=fit,
                inputs={
                    "data": "target_variable",
                    "transformer": "params:target_variable.inverted_encoder",
                    "target_col_name": "params:target_variable.target_column_name",
                },
                outputs="fitted_target_inverter",
                name="fitting_target_variable_inverter",
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


def create_pipeline() -> Pipeline:
    return create_master_pipeline() + create_modeling_pipeline()
