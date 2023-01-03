"""Post train EDA pipeline"""

from kedro.pipeline import Pipeline, node

from general.functions.reporting.utils.feature_importance import (
    calculate_feature_importance,
)
from general.functions.reporting.utils.performance_metric import (
    prepare_performance_metrics,
)
from general.functions.reporting.visualizations.barplot import plot_barplot


def create_pipeline(
    model_type: str = "classification", tree_model: bool = False
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
                    "x": "params:performance_metrics_plot.x",
                    "y": "params:performance_metrics_plot.y",
                    "hue": "params:performance_metrics_plot.hue",
                    "xlabel": "params:performance_metrics_plot.xlabel",
                    "ylabel": "params:performance_metrics_plot.ylabel",
                    "legend_title": "params:performance_metrics_plot.legend_title",
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
                    "model": "best_fitted_model",
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
                    "x": "params:feature_importance_plot.x",
                    "y": "params:feature_importance_plot.y",
                    "xlabel": "params:feature_importance_plot.xlabel",
                    "ylabel": "params:feature_importance_plot.ylabel",
                },
                outputs="feature_importance_plot",
                name="plot_feature_importance",
                tags=["post_eda", "feature_importance"],
            ),
        ]
    )

    # confusion_matrix = Pipeline()

    post_eda_nodes = [performance_metrics]

    if tree_model:
        post_eda_nodes += [feature_importance]

    # if model_type == "classification":
    #     post_eda_nodes += [confusion_matrix]

    return Pipeline(post_eda_nodes)
