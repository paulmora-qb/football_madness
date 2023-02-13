"""Creates pipeline for inference"""

from kedro.pipeline import Pipeline, node

from general.functions.preprocessing.filtering import filter_dataframe
from general.functions.reporting.utils.prediction_sorting import create_standing_table
from general.nodes.modeling.model_inference import (
    model_prediction,
    target_column_inverter,
)
from general.nodes.preprocessing.transformer import fit, transform

filter_dataframe_node = Pipeline(
    nodes=[
        node(
            func=filter_dataframe,
            inputs={
                "data": "master_table",
                "reference_team": "params:reference_team",
                "reference_season": "params:reference_season",
            },
            outputs="master_table_inference",
            name="filter_master_table_inference",
            tags=["filter", "inference"],
        )
    ]
)


apply_imputer = Pipeline(
    nodes=[
        node(
            func=transform,
            inputs={"data": "master_table_inference", "transformer": "fitted_imputer",},
            outputs="imputed_dataset_inference",
            name="imputing_dataset_inference",
            tags=["imputation", "inference"],
        )
    ]
)

make_model_predictions = Pipeline(
    nodes=[
        node(
            func=transform,
            inputs={
                "data": "imputed_dataset_inference",
                "transformer": "adjusted_assembler",
            },
            outputs="assembled_imputed_dataset_inference",
            name="assembling_features_inference",
        ),
        node(
            func=model_prediction,
            inputs={
                "data": "assembled_imputed_dataset_inference",
                "trained_model": "prediction_model",
                "prediction_suffix": "params:model_params.prediction_suffix",
                "prediction_proba_suffix": "params:model_params.prediction_proba_suffix",
            },
            outputs="model_predictions_inference",
            name="model_predictions_inference",
        ),
    ],
    tags=["predictions", "inference"],
)

invert_categorical_target = Pipeline(
    nodes=[
        node(
            func=target_column_inverter,
            inputs={
                "data": "model_predictions_inference",
                "inverter": "adj_index_to_string_encoder",
                "target_column_name": "params:target_variable.encoder.outputCol",
                "prediction_suffix": "params:model_params.prediction_suffix",
                "index_suffix": "params:inverter_params.index_sub_suffix",
            },
            outputs="inverted_model_predictions_inference",
            name="inverting_model_predictions_inference",
        ),
    ]
)

create_final_table = Pipeline(
    nodes=[
        node(
            func=create_standing_table,
            inputs={"data": "inverted_model_predictions_inference"},
            outputs="standing_table_inference",
            name="standing_table_inference",
        )
    ]
)


def create_pipeline(categorical_target: bool, create_standing_table: bool) -> Pipeline:

    nodes = [filter_dataframe_node, apply_imputer, make_model_predictions]

    if categorical_target:
        nodes += [invert_categorical_target]

    if create_standing_table:
        nodes += [create_final_table]

    return Pipeline(nodes, tags=["inference"])
