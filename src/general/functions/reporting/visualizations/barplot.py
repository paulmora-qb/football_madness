"""Function for a barplot"""

from typing import Any, Dict, Tuple

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from matplotlib.figure import Figure


def plot_barplot(data: pd.DataFrame, params: Dict[str, Any]) -> Figure:
    """Function to plot generic barplots using seaborn"""

    x = params.get("x")
    y = params.get("y")
    hue = params.get("hue", None)
    limit_bars = params.get("limit_bars", None)
    figsize = params.get("figsize", (16, 9))
    palette = params.get("palette", "Blues_r")
    rotation = params.get("rotation", 45)
    xlabel = params.get("xlabel", "X Axis")
    ylabel = params.get("ylabel", "Y Axis")
    legend_title = params.get("legend_title", None)
    legend_title_fontsize = params.get("legend_title_fontsize", 17)
    legend_fontsize = params.get("legend_fontsize", 15)
    label_fontsize = params.get("label_fontsize", 20)
    tick_fontsize = params.get("tick_fontsize", 18)

    fig, axs = plt.subplots(figsize=figsize)
    if limit_bars:
        data = data.iloc[:limit_bars, :]

    barplot_settings = {
        "data": data,
        "x": x,
        "y": y,
        "palette": palette,
        "ax": axs,
    }

    if hue:
        barplot_settings["hue"] = hue

    sns.barplot(**barplot_settings)

    # Legend
    if legend_title:
        axs.legend(
            title=legend_title,
            prop={"size": legend_fontsize},
            title_fontsize=legend_title_fontsize,
        )

    # Ticks
    if rotation:
        axs.tick_params(rotation=rotation, axis="x")
    axs.tick_params(axis="both", labelsize=tick_fontsize)

    # Axis names adjustment
    axs.set_xlabel(xlabel, fontsize=label_fontsize)
    axs.set_ylabel(ylabel, fontsize=label_fontsize)

    return fig
