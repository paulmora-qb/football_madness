"""Data Creation Pipeline"""


from kedro.pipeline import Pipeline

from .nodes import team_spine_node


def create_feature_engineering_pipeline() -> Pipeline:
    """Create feature engineering pipeline

    Returns:
        Pipeline
    """
    nodes = [team_spine_node]
    return Pipeline(nodes)
