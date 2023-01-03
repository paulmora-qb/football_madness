"""Function to plot heatmap"""

from typing import Dict, Tuple

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from matplotlib.figure import Figure


def plot_heatmap(
    data: pd.DataFrame,
    figsize: Tuple[int] = (10, 10),
    vmin: float = None,
    vmax: float = None,
    cmap: str = "Blues",
    annot: bool = True,
    fmt: str = ".2g",
    linewidths: int = 1,
    xlabel: str = "X Axis",
    ylabel: str = "Y Axis",
    label_fontsize: int = 20,
    tick_fontsize: int = 16,
    colormap_fontsize: int = 16,
    annot_kws: Dict[str, int] = {"fontsize": 14},
) -> Figure:
    """Function to plot generic heatmap using seaborn"""

    fig, axs = plt.subplots(figsize=figsize)

    heatmap = sns.heatmap(
        data=data,
        vmin=vmin,
        vmax=vmax,
        cmap=cmap,
        annot=annot,
        fmt=fmt,
        linewidths=linewidths,
        annot_kws=annot_kws,
    )

    # Ticks
    axs.tick_params(axis="both", labelsize=tick_fontsize)

    # Colorbar
    cbar = heatmap.collections[0].colorbar
    cbar.ax.tick_params(labelsize=colormap_fontsize)

    # Axis names adjustment
    axs.set_xlabel(xlabel, fontsize=label_fontsize)
    axs.set_ylabel(ylabel, fontsize=label_fontsize)

    return fig
