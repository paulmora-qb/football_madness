"""Post train EDA pipeline"""

from kedro.pipeline import Pipeline, node

from general.functions.reporting.utils.confusion_matrix import prepare_confusion_matrix
from general.functions.reporting.utils.feature_importance import (
    calculate_feature_importance,
)
from general.functions.reporting.utils.performance_metric import (
    prepare_performance_metrics,
)
from general.functions.reporting.visualizations.barplot import plot_barplot
from general.functions.reporting.visualizations.heatmap import plot_heatmap


def create_pipeline(
    model_type: str = "classification",
    tree_model: bool = False,
    categorical_target: bool = False,
) -> Pipeline:

    performance_metrics = Pipeline(
        nodes=[
            node(
                func=prepare_performance_metrics,
                inputs="prediction_evaluation_dict",
                outputs="prediction_evaluation_df",
                name="prepare_performance_metrics",
                tags=["post_eda", "performance_metrics"],
            ),
            node(
                func=plot_barplot,
                inputs={
                    "data": "prediction_evaluation_df",
                    "params": "params:performance_metrics_plot",
                },
                outputs="performance_metrics_plot",
                name="performance_metric_plot",
                tags=["post_eda", "performance_metrics"],
            ),
        ]
    )

    feature_importance = Pipeline(
        nodes=[
            node(
                func=calculate_feature_importance,
                inputs={
                    "model": "prediction_model",
                    "feature_name_list": "feature_name_list",
                },
                outputs="feature_importance_report",
                name="feature_importance_report_creation",
                tags=["post_eda", "feature_importance"],
            ),
            node(
                func=plot_barplot,
                inputs={
                    "data": "feature_importance_report",
                    "params": "params:feature_importance_plot",
                },
                outputs="feature_importance_plot",
                name="plot_feature_importance",
                tags=["post_eda", "feature_importance"],
            ),
        ]
    )

    confusion_matrix = Pipeline(
        nodes=[
            node(
                func=prepare_confusion_matrix,
                inputs={
                    "prediction_data": (
                        "inverted_model_predictions"
                        if categorical_target
                        else "model_predictions"
                    ),
                    "confusion_matrix_params": "params:confusion_matrix_preparation",
                    "labels": "target_encoder_index_labels",
                },
                outputs="confusion_matrix_df",
                name="create_confusion_matrix_array",
                tags=["post_eda", "confusion_matrix"],
            ),
            node(
                func=plot_heatmap,
                inputs={
                    "data": "confusion_matrix_df",
                    "params": "params:confusion_matrix_plot",
                },
                outputs="confusion_matrix_heatmap_plot",
                name="plot_confusion_matrix_heatmap",
                tags=["post_eda", "confusion_matrix"],
            ),
        ]
    )

    post_eda_nodes = [performance_metrics]

    if tree_model:
        post_eda_nodes += [feature_importance]

    if model_type == "classification":
        post_eda_nodes += [confusion_matrix]

    return Pipeline(post_eda_nodes)
