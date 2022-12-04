"""Data Creation Pipeline"""


from kedro.pipeline import Pipeline

from .nodes import concatenate_raw_data_node


def create_preprocessing_pipeline() -> Pipeline:
    """Create a pipeline to create data from different leagues

    Returns:
        Pipeline
    """
    return Pipeline([concatenate_raw_data_node])
