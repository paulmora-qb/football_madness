from kedro.pipeline import node

from .momentum import create_momentum_features
from .team_spine import create_team_spine

team_spine_node = node(
    func=create_team_spine,
    inputs=["concatenated_raw_data",],
    outputs="team_spine",
    name="create_team_spine",
    tags=["feature_engineering"],
)

momentum_features_node = node(
    func=create_momentum_features,
    inputs=["team_spine", "params:feature_engineering.momentum_features"],
    outputs="momentum_features",
    name="create_momentum_features",
    tags=["feature_engineering"],
)

# historic_interaction_node = node(
#     func=create_historic_interaction_feature,
#     inputs=["concatenated_raw_data"],
#     outputs="historic_interaction_feature",
#     name="create_historic_interaction_feature",
#     tags=["feature_engineering"]
# )

# season_statistics_node = node(
#     func=create_season_statistics,
#     inputs=["team_spine"],
#     outputs="season_statistics_feature",
#     name="create_season_statistics",
#     tags=["feature_engineering"]
# )
