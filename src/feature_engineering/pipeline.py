"""Feature Engineering Pipeline"""


from kedro.pipeline import Pipeline, node

from feature_engineering.momentum import create_momentum_features
from feature_engineering.past_interaction import create_past_interaction_features
from general.functions.feature_engineering.spine import (
    create_match_spine,
    create_team_spine,
)


def create_pipeline() -> Pipeline:
    """Create feature engineering pipeline

    Returns:
        Pipeline
    """

    match_spine_node = node(
        func=create_match_spine,
        inputs=["concatenated_raw_data", "params:ftr_match_spine"],
        outputs="match_spine",
        name="create_match_spine",
        tags=["feature_engineering"],
    )

    team_spine_node = node(
        func=create_team_spine,
        inputs=["concatenated_raw_data",],
        outputs="team_spine",
        name="create_team_spine",
        tags=["feature_engineering"],
    )

    momentum_features_node = node(
        func=create_momentum_features,
        inputs=["team_spine", "params:feature.momentum"],
        outputs="ftr_momentum",
        name="create_momentum_features",
        tags=["feature_engineering"],
    )

    past_interaction_node = node(
        func=create_past_interaction_features,
        inputs=["match_spine", "params:feature.past_interaction"],
        outputs="ftr_past_interaction",
        name="create_past_interaction_features",
        tags=["feature_engineering"],
    )

    feature_engineering_nodes = [
        match_spine_node,
        team_spine_node,
        momentum_features_node,
        past_interaction_node,
    ]

    return Pipeline(feature_engineering_nodes)
