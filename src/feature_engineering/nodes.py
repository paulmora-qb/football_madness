"""This file contains nodes for the data creation"""

from kedro.pipeline import node

from .team_spine import create_team_spine

team_spine_node = node(
    func=create_team_spine,
    inputs=["concatenated_raw_data",],
    outputs="team_spine",
    name="create_team_spine",
    tags=["feature_engineering"],
)
