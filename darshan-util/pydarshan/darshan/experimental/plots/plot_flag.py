from typing import Any

import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.collections import PatchCollection


def _plot_warning_sign(ax: Any):
    """
    Adds a triangular warning sign to an axis.

    Parameters
    ----------
    ax: a ``matplotlib`` axis object.

    """
    # add an exclamation point
    ax.text(
        x=0.43,
        y=0.42,
        s="!",
        size=36,
        ha="center",
        va="center",
    )

    tri_patch = mpatches.RegularPolygon(
        xy=(0.43, 0.42),
        numVertices=3,
        radius=0.45,
        facecolor="white",
        edgecolor="red",
        lw=4,
    )
    ax.add_collection(PatchCollection([tri_patch], match_original=True))


def plot_flag(warn_msg: str, font_size: int = 16) -> Any:
    """
    Generates a figure containing a triangular sign with a warning message.

    Parameters
    ----------

    warn_msg: warning message to add to canvas. Message text
    will be wrapped if it is too long for a single line.

    font_size: warning message font size.

    Returns
    -------

    fig: a ``matplotlib.pyplot.figure`` object of a warning message and flag.

    """
    fig, (ax_tri_sign, ax_warn_msg) = plt.subplots(
        nrows=1,
        ncols=2,
        figsize=(4, 1),
        # set the subfigure widths so the text is 75% of total width
        gridspec_kw={"width_ratios": [1, 3],},
    )

    # make plotting canvas span entire figure
    plt.subplots_adjust(left=0, bottom=0, right=1, top=1)

    ax_warn_msg.text(
        x=0.4,
        y=0.5,
        s=warn_msg,
        size=font_size,
        color="red",
        ha="center",
        va="center",
        wrap=True,
    )

    _plot_warning_sign(ax=ax_tri_sign)

    for ax in (ax_tri_sign, ax_warn_msg):
        ax.axis("off")

    plt.close()
    return fig
