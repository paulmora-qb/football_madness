"""Modeling pipeline for pyspark"""

from functools import partial

from kedro.pipeline import Pipeline, node

from general.functions.model_development.data_dictionary.data_dictionary import (
    create_data_dictionary,
)
from general.functions.modeling.model_helper import creating_weight_col
from general.functions.modeling.model_splitting import split_train_test_dataframe
from general.nodes.modeling.model_evaluation import prediction_evaluation
from general.nodes.modeling.model_inference import (
    model_prediction,
    target_column_inverter,
)
from general.nodes.modeling.model_tuning import tuner
from general.nodes.preprocessing.transformer import fit, transform
from utilities.helper import update_dictionary


def create_pipeline(categorical_target: bool) -> Pipeline:
    """Supervised pipeline

    Args:
        categorical_target (bool): Indication whether the data science problem deals
            with a classification case. If false a regression case is assumed.

    Returns:
        Pipeline: Supervised learning pipeline
    """

    data_dictionary = Pipeline(
        nodes=[
            node(
                func=create_data_dictionary,
                inputs="master_table",
                outputs="data_dictionary",
                name="create_data_dictionary",
            ),
            node(
                func=lambda x: x.get_feature_columns(),
                inputs="data_dictionary",
                outputs="feature_name_list",
                name="feature_name_list",
            ),
        ],
        tags=["data_dictionary", "modeling"],
    )

    splitting_train_test = node(
        func=split_train_test_dataframe,
        inputs={"data": "master_table", "splitting_params": "params:splitting_params",},
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
            ),
            node(
                func=partial(update_dictionary, key="outputCols"),
                inputs={
                    "original_dict": "adjusted_imputing_transformer_input",
                    "value": "feature_name_list",
                },
                outputs="adjusted_imputing_transformer_output",
                name="adjusting_imputed_params_output_cols",
            ),
            node(
                func=fit,
                inputs={
                    "data": "train_test_splitted_data",
                    "transformer": "adjusted_imputing_transformer_output",
                },
                outputs="fitted_imputer",
                name="fitting_imputer",
            ),
            node(
                func=transform,
                inputs={
                    "data": "train_test_splitted_data",
                    "transformer": "fitted_imputer",
                },
                outputs="imputed_dataset",
                name="imputing_dataset",
            ),
        ],
        tags=["imputation", "modeling"],
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
                name="adjusting_assembler_configuration",
            ),
            node(
                func=transform,
                inputs={
                    "data": "imputed_dataset",
                    "transformer": "adjusted_assembler",
                },
                outputs="assembled_imputed_dataset",
                name="assembling_features",
            ),
        ],
        tags=["assembling", "modeling"],
    )

    create_weight_column = Pipeline(
        nodes=[
            node(
                func=creating_weight_col,
                inputs={
                    "data": "assembled_imputed_dataset",
                    "target_col_name": "params:weight_column.target_column",
                    "weight_col_name": "params:weight_column.weight_column",
                },
                outputs="assembled_imputed_dataset_w_weight_col",
                name="create_weight_column",
            )
        ]
    )

    hyperparameter_tuning = Pipeline(
        nodes=[
            node(
                func=tuner,
                inputs={
                    "data": "assembled_imputed_dataset_w_weight_col"
                    if categorical_target
                    else "assembled_imputed_dataset",
                    "tuner": "params:tuning_params.tuning_func",
                    "param_grid": "params:tuning_params.param_grid",
                },
                outputs=["prediction_model", "optimal_tuning_params"],
                name="hyperparameter_tune",
            ),
        ],
        tags=["tuning", "modeling"],
    )

    make_model_predictions = Pipeline(
        nodes=[
            node(
                func=model_prediction,
                inputs={
                    "data": "assembled_imputed_dataset",
                    "trained_model": "prediction_model",
                    "prediction_suffix": "params:model_params.prediction_suffix",
                    "prediction_proba_suffix": "params:model_params.prediction_proba_suffix",
                },
                outputs="model_predictions",
                name="model_predictions",
            )
        ],
        tags=["predictions", "modeling"],
    )

    invert_numerical_target_to_category = Pipeline(
        nodes=[
            node(
                func=partial(update_dictionary, key="labels"),
                inputs={
                    "original_dict": "params:inverter_params.index_to_string_encoder",
                    "value": "target_encoder_index_labels",
                },
                outputs="adj_index_to_string_encoder",
                name="adj_index_to_string_encoding",
            ),
            node(
                func=target_column_inverter,
                inputs={
                    "data": "model_predictions",
                    "inverter": "adj_index_to_string_encoder",
                    "target_column_name": "params:target_variable.encoder.outputCol",
                    "prediction_suffix": "params:model_params.prediction_suffix",
                    "index_suffix": "params:inverter_params.index_sub_suffix",
                },
                outputs="inverted_model_predictions",
                name="inverting_model_predictions",
            ),
        ],
        tags=["inverting", "modeling"],
    )

    performance_evaluation = Pipeline(
        nodes=[
            node(
                func=prediction_evaluation,
                inputs={
                    "data": (
                        "inverted_model_predictions"
                        if categorical_target
                        else "model_predictions"
                    ),
                    "evaluation_function": "params:performance_evaluation_params.evaluator",
                    "evaluation_metrics": "params:performance_evaluation_params.metrics",
                },
                outputs="prediction_evaluation_dict",
                name="prediction_evaluation",
            )
        ],
        tags=["evaluation", "modeling"],
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

    if categorical_target:
        modeling_nodes += [create_weight_column, invert_numerical_target_to_category]

    return Pipeline(modeling_nodes)


def create_modeling_pipeline(categorical_target: str = False):
    """For adjusting whether we are dealing with a classification or regression case"""
    return create_pipeline(categorical_target=categorical_target)
