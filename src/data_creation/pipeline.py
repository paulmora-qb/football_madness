"""Data Creation Pipeline"""


from kedro.pipeline import Pipeline, node

from .nodes import prm_raw_data


def create_data_creation_pipeline() -> Pipeline:
    """Create a pipeline to create data from different leagues

    Returns:
        Pipeline
    """
    return Pipeline(prm_raw_data)
