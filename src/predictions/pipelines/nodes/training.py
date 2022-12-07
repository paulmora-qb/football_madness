"""Nodes for the training pipeline"""

from kedro.pipeline import node

from ...functions.training.splitter import split_train_test_dataframe

splitting_node = node(
    func=split_train_test_dataframe,
    inputs=["master_table", "params:splitting_params"],
    outputs="train_test_splitted_data",
    name="create_train_test_split",
    tags=["modeling"],
)
