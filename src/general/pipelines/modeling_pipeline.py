"""Modeling pipeline for pyspark"""

from functools import partial

from kedro.pipeline import Pipeline, node

from general.functions.model_development.data_dictionary.data_dictionary import (
    create_data_dictionary,
)
from general.functions.modeling.model_splitting import split_train_test_dataframe
from general.nodes.modeling.model_inference import model_prediction
from general.nodes.modeling.model_tuning import tuner
from general.nodes.preprocessing.transformer import fit, transform
from utilities.helper import update_dictionary


def create_modeling_pipeline() -> Pipeline:
    """Create the modeling pipeline

    Returns:
        Pipeline: Modeling pipeline
    """

    data_dictionary = Pipeline(
        nodes=[
            node(
                func=create_data_dictionary,
                inputs="master_table",
                outputs="data_dictionary",
                name="create_data_dictionary",
                tags=["data_dictionary", "modeling"],
            ),
            node(
                func=lambda x: x.get_feature_columns(),
                inputs="data_dictionary",
                outputs="feature_name_list",
                name="feature_name_list",
                tags=["data_dictionary", "modeling"],
            ),
        ]
    )

    splitting_train_test = node(
        func=split_train_test_dataframe,
        inputs=["master_table", "params:splitting_params"],
        outputs="train_test_splitted_data",
        name="create_train_test_split",
        tags=["modeling"],
    )

    imputing = Pipeline(
        nodes=[
            node(
                func=partial(update_dictionary, key="inputCols"),
                inputs={
                    "original_dict": "params:imputing_transformer",
                    "value": "feature_name_list",
                },
                outputs="adjusted_imputing_transformer_input",
                name="adjusting_imputed_params_input_cols",
                tags=["imputation", "modeling"],
            ),
            node(
                func=partial(update_dictionary, key="outputCols"),
                inputs={
                    "original_dict": "adjusted_imputing_transformer_input",
                    "value": "feature_name_list",
                },
                outputs="adjusted_imputing_transformer_output",
                name="adjusting_imputed_params_output_cols",
                tags=["imputation", "modeling"],
            ),
            node(
                func=fit,
                inputs={
                    "data": "train_test_splitted_data",
                    "transformer": "adjusted_imputing_transformer_output",
                },
                outputs="fitted_imputer",
                name="fitting_imputerimputation",
                tags=["imputation", "modeling"],
            ),
            node(
                func=transform,
                inputs={
                    "data": "train_test_splitted_data",
                    "transformer": "fitted_imputer",
                },
                outputs="imputed_dataset",
                name="imputing_dataset",
                tags=["imputation", "modeling"],
            ),
        ]
    )

    assembling = Pipeline(
        nodes=[
            node(
                func=partial(update_dictionary, key="inputCols"),
                inputs={
                    "original_dict": "params:assembler",
                    "value": "feature_name_list",
                },
                outputs="adjusted_assembler",
                name="adjusintg_assembler_configuration",
                tags=["imputation", "modeling"],
            ),
            node(
                func=transform,
                inputs={
                    "data": "imputed_dataset",
                    "transformer": "adjusted_assembler",
                },
                outputs="assembled_imputed_dataset",
                name="assembling_features",
                tags=["assembling", "modeling"],
            ),
        ]
    )

    hyperparameter_tuning = Pipeline(
        nodes=[
            node(
                func=tuner,
                inputs={
                    "data": "assembled_imputed_dataset",
                    "tuner": "params:tuning_params.tuning_func",
                    "param_grid": "params:tuning_params.param_grid",
                },
                outputs=["best_fitted_model", "optimal_tuning_params"],
                name="hyperparameter_tune",
                tags=["tuning", "modeling"],
            ),
        ]
    )

    make_model_predictions = Pipeline(
        nodes=[
            node(
                func=model_prediction,
                inputs={
                    "data": "assembled_imputed_dataset",
                    "trained_model": "best_fitted_model",
                    "prediction_suffix": "params:model_params.prediction_suffix",
                    "prediction_proba_suffix": "params:model_params.prediction_proba_suffix",
                },
                outputs="model_predictions",
                name="make_model_predictions",
                tags=["predictions", "modeling"],
            )
        ]
    )

    performance_evaluation = Pipeline(
        nodes=[
            node(
                func=prediction_evaluation,
                inputs={
                    "data": "model_predictions",
                    "evaluation_function": "params:performance_evaluation_params.evaluator",
                    "evaluation_metrics": "params:performance_evaluation_params.metrics",
                },
            )
        ]
    )

    modeling_nodes = [
        data_dictionary,
        splitting_train_test,
        imputing,
        assembling,
        hyperparameter_tuning,
        make_model_predictions,
        performance_evaluation,
    ]

    return Pipeline(modeling_nodes)
