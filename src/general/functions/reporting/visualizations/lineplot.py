"""Function for lineplot"""

from typing import Dict

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns


def plot_lineplot(data: pd.DataFrame, params: Dict[str, str]) -> plt.figure:

    figsize = params.get("figsize", (16, 9))
    xlabel = params.get("xlabel", "X label")
    ylabel = params.get("ylabel", "Y label")
    hue = params.get("hue", None)
    x = params.get("x", None)
    y = params.get("y", None)
    label_fontsize = params.get("label_fontsize", 20)
    legend_title = params.get("legend_title", None)
    legend_title_fontsize = params.get("legend_title_fontsize", 17)
    legend_fontsize = params.get("legend_fontsize", 15)
    tick_fontsize = params.get("tick_fontsize", 18)

    fig, axs = plt.subplots(figsize=figsize)
    sns.lineplot(data=data, x=x, y=y, hue=hue, ax=axs)

    # Legend
    if legend_title:
        axs.legend(
            title=legend_title,
            prop={"size": legend_fontsize},
            title_fontsize=legend_title_fontsize,
        )
    axs.tick_params(axis="both", labelsize=tick_fontsize)

    # Axis names adjustment
    axs.set_xlabel(xlabel, fontsize=label_fontsize)
    axs.set_ylabel(ylabel, fontsize=label_fontsize)

    return fig
