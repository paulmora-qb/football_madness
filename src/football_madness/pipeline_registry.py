"""Project pipelines."""
from typing import Dict

from kedro.pipeline import Pipeline

from feature_engineering import create_feature_engineering_pipeline
from predictions import create_modeling_pipeline
from preprocessing import create_preprocessing_pipeline


def register_pipelines() -> Dict[str, Pipeline]:
    """Register the project's pipelines.

    Returns:
        A mapping from a pipeline name to a ``Pipeline`` object.
    """
    return {
        "preprocessing": create_preprocessing_pipeline(),
        "feature_engineering": create_feature_engineering_pipeline(),
        "modeling": create_modeling_pipeline(),
    }
