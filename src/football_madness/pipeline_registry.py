"""Project pipelines."""
from typing import Dict

from kedro.pipeline import Pipeline

from data_loading import create_data_loading_pipeline
from feature_engineering.pipeline import (
    create_pipeline as create_feature_engineering_pipeline,
)
from football_madness.pipelines.win_draw_loss_pipeline import (
    create_pipeline as create_win_draw_loss_pipeline,
)


def register_pipelines() -> Dict[str, Pipeline]:
    """Register the project's pipelines.

    Returns:
        A mapping from a pipeline name to a ``Pipeline`` object.
    """
    return {
        "data_loading": create_data_loading_pipeline(),
        "feature_engineering": create_feature_engineering_pipeline(),
        "win_draw_loss_prediction": create_win_draw_loss_pipeline(),
    }
