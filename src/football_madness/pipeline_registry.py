"""Project pipelines."""
from typing import Dict

from kedro.pipeline import Pipeline

from data_creation import create_data_creation_pipeline


def register_pipelines() -> Dict[str, Pipeline]:
    """Register the project's pipelines.

    Returns:
        A mapping from a pipeline name to a ``Pipeline`` object.
    """
    return {"data_creation": create_data_creation_pipeline()}
