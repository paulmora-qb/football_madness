"""Creates pipeline for inference"""

from kedro.pipeline import Pipeline, node

from general.functions.preprocessing.filtering import filter_dataframe

filter_dataframe_node = Pipeline(
    nodes=[
        node(
            func=filter_dataframe,
            inputs={
                "data": "master_table",
                "reference_team": "params:reference_team",
                "reference_season": "params:reference_season",
            },
            outputs="inference_master_table",
            name="filter_master_table_inference",
            tags=["inference"],
        )
    ]
)


def create_pipeline():

    nodes = [filter_dataframe_node]

    return Pipeline(nodes, tags=["inference"])
