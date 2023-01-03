"""Function for a barplot"""

from typing import Tuple

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns


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
):
    """Function to plot generic barplots using seaborn

    Args:
        data (pd.DataFrame): _description_
        x (str): _description_
        y (str): _description_
        hue (str, optional): _description_. Defaults to None.
        limit_bars (int, optional): _description_. Defaults to None.
        figsize (Tuple[int], optional): _description_. Defaults to (16, 9).
        palette (str, optional): _description_. Defaults to "pastel".
        rotation (int, optional): _description_. Defaults to 45.
        xlabel (str, optional): _description_. Defaults to "X Axis".
        ylabel (str, optional): _description_. Defaults to "Y Axis".
        legend_title (str, optional): _description_. Defaults to None.
        legend_title_fontsize (int, optional): _description_. Defaults to 17.
        legend_fontsize (int, optional): _description_. Defaults to 15.
        label_fontsize (int, optional): _description_. Defaults to 20.
        tick_fontsize (int, optional): _description_. Defaults to 18.

    Returns:
        _type_: _description_
    """

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

    return fig
