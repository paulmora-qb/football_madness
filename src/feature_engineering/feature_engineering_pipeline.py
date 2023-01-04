"""Feature Engineering Pipeline"""


from kedro.pipeline import Pipeline, node

from general.functions.feature_engineering.momentum import create_momentum_features
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

    momentum_features_node = Pipeline(
        nodes=[
            node(
                func=create_momentum_features,
                inputs=["team_spine", "params:feature.momentum"],
                outputs="ftr_momentum",
                name="create_momentum_features",
                tags=["feature_engineering"],
            )
        ]
    )

    feature_engineering_nodes = [
        match_spine_node,
        team_spine_node,
        momentum_features_node,
    ]

    return Pipeline(feature_engineering_nodes)
