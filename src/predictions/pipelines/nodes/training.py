"""Nodes for the training pipeline"""

from functools import partial

from kedro.pipeline import Pipeline, node

from general.nodes.modeling.model_imputation import imputer
from general.nodes.modeling.model_tuning import tuner
from utilities.helper import update_dictionary, vector_assemble

from ...functions.data_dictionary.data_dictionary import create_data_dictionary
from ...functions.training.splitter import split_train_test_dataframe

data_dictionary_node = node(
    func=create_data_dictionary,
    inputs="master_table",
    outputs="data_dictionary",
    name="create_data_dictionary",
    tags=["modeling"],
)

splitting_node = node(
    func=split_train_test_dataframe,
    inputs=["master_table", "params:splitting_params"],
    outputs="train_test_splitted_data",
    name="create_train_test_split",
    tags=["modeling"],
)

imputing_nodes = Pipeline(
    nodes=[
        node(
            func=lambda x: x.get_feature_columns(),
            inputs="data_dictionary",
            outputs="feature_name_list",
            name="feature_name_list",
            tags=["imputation", "modeling"],
        ),
        node(
            func=partial(update_dictionary, level_name="transformer", key="inputCols"),
            inputs={
                "original_dict": "params:imputing_params",
                "value": "feature_name_list",
            },
            outputs="adjusted_imputed_params_input",
            name="adjusting_imputed_params_input_cols",
            tags=["imputation", "modeling"],
        ),
        node(
            func=partial(update_dictionary, level_name="transformer", key="outputCols"),
            inputs={
                "original_dict": "adjusted_imputed_params_input",
                "value": "feature_name_list",
            },
            outputs="adjusted_imputed_params_output",
            name="adjusting_imputed_params_output_cols",
            tags=["imputation", "modeling"],
        ),
        node(
            func=imputer,
            inputs=["train_test_splitted_data", "adjusted_imputed_params_output"],
            outputs="imputed_data",
            name="imputation",
            tags=["imputation", "modeling"],
        ),
        node(
            func=vector_assemble,
            inputs=["imputed_data", "feature_name_list"],
            outputs="assembled_imputed_data",
            name="create_vector_assemble",
            tags=["imputation", "modeling"],
        ),
    ]
)

hyperparameter_tuning_node = Pipeline(
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
