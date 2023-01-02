"""Post train EDA pipeline"""

from kedro import Pipeline


def create_pipeline(model_type="classification") -> Pipeline:

    feature_importance = Pipeline()

    confusion_matrix = Pipeline()

    post_eda_nodes = [feature_importance, confusion_matrix]

    return Pipeline(post_eda_nodes)
