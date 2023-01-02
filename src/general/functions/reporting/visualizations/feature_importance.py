"""Plotting function for feature importance"""

from typing import Tuple

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns


def plot_feature_importance(
    feature_importance_report: pd.DataFrame,
    figsize: Tuple[int] = (16, 9),
    palette: str = "pastel",
) -> plt.subplot:

    fig, axs = plt.subplots(figsize=figsize)

    # actual plot
    sns.barplot(
        data=feature_importance_report,
        x="feature_column_name",
        y="feature_importance",
        palette=palette,
        ax=axs,
    )

    # rotate labels

    # Axis names adjustment

    return fig
