"""Creates pipeline for inference"""

from kedro.pipeline import Pipeline, node

from general.functions.preprocessing.filtering import filter_dataframe
from general.nodes.modeling.model_inference import model_prediction
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


assembling = Pipeline(
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
    ],
    tags=["assembling", "inference"],
)


make_model_predictions = Pipeline(
    nodes=[
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
            tags=["predictions", "inference"],
        )
    ]
)


def create_pipeline():

    nodes = [filter_dataframe_node, apply_imputer, assembling, make_model_predictions]

    return Pipeline(nodes, tags=["inference"])
