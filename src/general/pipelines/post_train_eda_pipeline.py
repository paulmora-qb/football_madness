"""Post train EDA pipeline"""

from kedro.pipeline import Pipeline, node

from general.functions.reporting.utils.feature_importance import (
    calculate_feature_importance,
)
from general.functions.reporting.visualizations.feature_importance import (
    plot_feature_importance,
)


def create_pipeline(
    model_type: str = "classification", tree_model: bool = False
) -> Pipeline:

    feature_importance = Pipeline(
        nodes=[
            node(
                func=calculate_feature_importance,
                inputs={
                    "model": "best_fitted_model",
                    "feature_name_list": "feature_name_list",
                },
                outputs="feature_importance_report",
                name="feature_importance_report_creation",
                tags=["post_eda", "feature_importance"],
            ),
            node(
                func=plot_feature_importance,
                inputs="feature_importance_report",
                outputs="feature_importance_plot",
                name="plot_feature_importance",
                tags=["post_eda", "feature_importance"],
            ),
        ]
    )

    # confusion_matrix = Pipeline()

    post_eda_nodes = [feature_importance]

    return Pipeline(post_eda_nodes)
