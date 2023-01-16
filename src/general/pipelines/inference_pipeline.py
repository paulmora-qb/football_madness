"""Creates pipeline for inference"""

from kedro.pipeline import Pipeline, node


def create_inference_pipeline():

    filter_dataframe = Pipeline(
        nodes=[
            node(
                function=filter_dataframe,
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

