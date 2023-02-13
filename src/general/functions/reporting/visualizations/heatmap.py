"""Function to plot heatmap"""

from typing import Any, Dict, Tuple

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from matplotlib.figure import Figure


def plot_heatmap(data: pd.DataFrame, params: Dict[str, Any]) -> Figure:
    """Function to plot generic heatmap using seaborn"""

    figsize = params.get("figsize", (10, 10))
    vmin = params.get("vmin", None)
    vmax = params.get("vmax", None)
    cmap = params.get("cmap", "Blues")
    annot = params.get("annot", True)
    fmt = params.get("fmt", "g")
    linewidths = params.get("linewidths", 1)
    xlabel = params.get("xlabel", "X Axis")
    ylabel = params.get("ylabel", "Y Axis")
    label_fontsize = params.get("label_fontsize", 20)
    tick_fontsize = params.get("tick_fontsize", 16)
    colormap_fontsize = params.get("colormap_fontsize", 16)
    annot_kws = params.get("annot_kws", {"fontsize": 14})

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
