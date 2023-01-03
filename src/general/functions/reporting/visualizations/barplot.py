"""Function for a barplot"""

from typing import Tuple

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from matplotlib.figure import Figure


def plot_barplot(
    data: pd.DataFrame,
    x: str,
    y: str,
    hue: str = None,
    limit_bars: int = None,
    figsize: Tuple[int] = (16, 9),
    palette: str = "pastel",
    rotation: int = 45,
    xlabel: str = "X Axis",
    ylabel: str = "Y Axis",
    legend_title: str = None,
    legend_title_fontsize: int = 17,
    legend_fontsize: int = 15,
    label_fontsize: int = 20,
    tick_fontsize: int = 18,
) -> Figure:
    """Function to plot generic barplots using seaborn"""

    fig, axs = plt.subplots(figsize=figsize)
    barplot_settings = {
        "data": data,
        "x": x,
        "y": y,
        "palette": palette,
        "ax": axs,
    }

    if limit_bars:
        data = data.iloc[:limit_bars, :]

    if hue:
        barplot_settings["hue"] = hue

    sns.barplot(**barplot_settings)

    # Legend
    if legend_title:
        axs.legend(
            title=legend_title,
            prop={"size": legend_fontsize},
            title_fontisze=legend_title_fontsize,
        )

    # Ticks
    if rotation:
        axs.tick_params(rotation=rotation, axis="x")
    axs.tick_params(axis="both", labelsize=tick_fontsize)

    # Axis names adjustment
    axs.set_xlabel(xlabel, fontsize=label_fontsize)
    axs.set_ylabel(ylabel, fontsize=label_fontsize)

    fig.savefig("./data.png")

    return fig
