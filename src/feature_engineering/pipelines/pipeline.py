"""Data Creation Pipeline"""


from kedro.pipeline import Pipeline

from .nodes import match_spine_node, momentum_features_node, team_spine_node


def create_feature_engineering_pipeline() -> Pipeline:
    """Create feature engineering pipeline

    Returns:
        Pipeline
    """
    nodes = [match_spine_node, team_spine_node, momentum_features_node]
    return Pipeline(nodes)