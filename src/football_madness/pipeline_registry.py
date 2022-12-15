"""Project pipelines."""
from typing import Dict

from kedro.pipeline import Pipeline

from feature_engineering import create_feature_engineering_pipeline
from football_madness.pipelines.win_draw_loss_pipeline import (
    create_pipeline as create_win_draw_loss_pipeline,
)
from preprocessing import create_preprocessing_pipeline


def register_pipelines() -> Dict[str, Pipeline]:
    """Register the project's pipelines.

    Returns:
        A mapping from a pipeline name to a ``Pipeline`` object.
    """
    return {
        "preprocessing": create_preprocessing_pipeline(),
        "feature_engineering": create_feature_engineering_pipeline(),
        "win_draw_loss_prediction": create_win_draw_loss_pipeline(),
    }
