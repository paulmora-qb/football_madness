"""Data Creation Pipeline"""


from kedro.pipeline import Pipeline

from .nodes import concatenate_raw_data_node, filter_rename_raw_data_node


def create_data_creation_pipeline() -> Pipeline:
    """Create a pipeline to create data from different leagues

    Returns:
        Pipeline
    """

    nodes = [
        concatenate_raw_data_node,
        filter_rename_raw_data_node,
    ]
    return Pipeline(nodes)
