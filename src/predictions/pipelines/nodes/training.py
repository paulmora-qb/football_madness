"""Nodes for the training pipeline"""

from functools import partial

from kedro.pipeline import Pipeline, node

from utilities.helper import update_dictionary

from ...functions.data_dictionary.data_dictionary import create_data_dictionary
from ...functions.training.imputer import impute_dataframe
from ...functions.training.splitter import split_train_test_dataframe
from ...functions.training.tuning import hp_tuning

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

# 1: Include additional parameters
# 2: Update the parameters with the additional parameters
# 3: Fit and transform the nodes

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
            func=partial(update_dictionary, key="feature_names"),
            inputs={
                "original_dict": "params:imputing_params",
                "value": "feature_name_list",
            },
            outputs="adjusted_imputed_params",
            name="adjusting_imputed_params",
            tags=["imputation", "modeling"],
        ),
        node(
            func=impute_dataframe,
            inputs=["train_test_splitted_data", "adjusted_imputed_params"],
            outputs="imputed_data",
            name="imputation",
            tags=["imputation", "modeling"],
        ),
    ]
)

hyperparameter_tuning_node = Pipeline(
    nodes=[
        node(
            func=partial(update_dictionary, key="featuresCol"),
            inputs={
                "original_dict": "params:tuning_params.model",
                "value": "feature_name_list",
            },
            outputs="adjusted_tuning_model_params",
            name="adjusting_tuning_params",
            tags=["tuning", "modeling"],
        ),
        node(
            func=hp_tuning,
            inputs={
                "data": "imputed_data",
                "model_params": "adjusted_tuning_model_params",
                "cv_params": "params:tuning_params.cv",
            },
            outputs="optimal_tuning_params",
            name="hyperparameter_tune",
            tags=["tuning", "modeling"],
        ),
    ]
)
