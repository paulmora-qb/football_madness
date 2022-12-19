"""Modeling pipeline for pyspark"""

from functools import partial

from kedro.pipeline import Pipeline, node

from general.functions.model_development.data_dictionary.data_dictionary import (
    create_data_dictionary,
)
from general.functions.modeling.model_splitting import split_train_test_dataframe
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
                inputs=[
                    "train_test_splitted_data",
                    "adjusted_imputing_transformer_output",
                ],
                outputs="fitted_imputer",
                name="fitting_imputerimputation",
                tags=["imputation", "modeling"],
            ),
            node(
                func=transform,
                inputs=["train_test_splitted_data", "fitted_imputer"],
                outputs="imputed_data",
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
                inputs=["imputed_dataset", "adjusted_assembler"],
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
                    "data": "assembled_imputed_data",
                    "tuner": "params:tuning_params.tuning_func",
                    "param_grid": "params:tuning_params.param_grid",
                    "target_col_name": "params:tuning_params.target_col_name",
                },
                outputs="optimal_tuning_params",
                name="hyperparameter_tune",
                tags=["tuning", "modeling"],
            ),
        ]
    )

    modeling_nodes = [
        data_dictionary,
        splitting_train_test,
        imputing,
        assembling,
        hyperparameter_tuning,
    ]

    return Pipeline(modeling_nodes)
