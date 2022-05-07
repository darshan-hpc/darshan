"""
Module for creating the ranks vs. time IO intensity
heatmap figure for the Darshan job summary.
"""

import functools
from typing import (Any, List, Sequence, Union,
                    TYPE_CHECKING, Tuple, Optional)

import numpy as np

if TYPE_CHECKING:
    import numpy.typing as npt

# we can't use PEP563 delayed type
# evaluations while we have to support
# Python 3.6 because the __future__ import is not
# available yet;
# TODO: delete the mocking and restore the
# from __future__ import annotations
from unittest.mock import MagicMock
npt = MagicMock()

import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from matplotlib.colors import LogNorm

import darshan
from darshan.experimental.plots import heatmap_handling


@functools.lru_cache(maxsize=10)
def determine_hmap_runtime(report: darshan.DarshanReport) -> Tuple[float, float]:
    """
    Determine the effective heatmap runtime to be used
    for plotting in cases where only DXT, only HEATMAP,
    or both module types are available, to achieve a common
    max displayed runtime.

    In some cases, this may mean that the time value is
    rounded up from the actual runtime.

    Paramaters
    ----------

    report: a ``darshan.DarshanReport``

    Returns
    -------

    A tuple containing `tmax`, (rounded) `runtime` floats.

    """
    # calculate the elapsed runtime
    runtime = report.metadata["job"]["end_time"] - report.metadata["job"]["start_time"]
    # ensure a minimum runtime value of 1 second
    runtime = max(runtime, 1)
    # leverage higher resolution DXT timing
    # data if available
    if ("DXT_POSIX" in report.modules or
        "DXT_MPIIO" in report.modules):
        tmax = 0.0
        for mod in report.modules:
            if "DXT" in mod:
                agg_df = heatmap_handling.get_aggregate_data(report=report,
                                                             mod=mod,
                                                             ops=["read", "write"])
                tmax_dxt = float(agg_df["end_time"].max())
                if tmax_dxt > tmax:
                    tmax = tmax_dxt
        # if the data max time exceeds the runtime, buffer by 1 second
        # until our timer precision improves to prevent truncation
        if tmax > runtime:
            runtime += 1
    else:
        tmax = runtime
    return tmax, runtime


def get_x_axis_ticks(bin_max: float, n_xlabels: int = 4) -> npt.NDArray[np.float64]:
    """
    Creates the x-axis tick mark locations.

    Parameters
    ----------

    bin_max: the maximum number of bins.

    n_xlabels: the number of x-axis tick marks to create. Default is 4.

    Returns
    -------

    Array of x-axis tick mark locations of length ``n_xlabels``.

    """
    return np.linspace(0, bin_max, n_xlabels)


def get_x_axis_tick_labels(
    max_time: float, n_xlabels: int = 4
) -> Union[npt.NDArray[np.float64], npt.NDArray[np.intc]]:
    """
    Creates the x-axis tick mark labels.

    Parameters
    ----------

    max_time: the maximum time to plot.

    n_xlabels: the number of x-axis tick marks to create. Default is 4.

    Returns
    -------

    x_ticklabels: array of x-axis tick mark labels of length ``n_xlabels``.

    """
    # for the x tick labels, start at 0 and end with
    # the max time (converted to an integer)
    if max_time <= 1:
        # use 2 decimal places for run times less than 1 second
        x_ticklabels = np.around(np.linspace(0.0, max_time, n_xlabels), decimals=2)
    elif (max_time > 1) & (max_time <= 10):
        # use 1 decimal place for run times between 1 and 10 seconds
        x_ticklabels = np.around(np.linspace(0.0, max_time, n_xlabels), decimals=1)
    else:
        # for run times greater than 10 seconds, round the max
        # time up and round labels to the nearest integer
        x_ticklabels = np.linspace(0.0, np.ceil(max_time), n_xlabels, dtype=int)
    return x_ticklabels


def get_y_axis_ticks(ax: Any, n_ylabels: int = 6) -> npt.NDArray[np.float64]:
    """
    Creates the y-axis tick mark locations.

    Parameters
    ----------

    ax: a ``matplotlib`` axis object.

    n_ylabels: The number of y-axis tick mark labels to create. Default is 6.

    Returns
    -------

    yticks: array of y-axis tick mark locations of length ``n_ylabels``.

    """
    # get the original y-axis tick locations
    initial_yticks = ax.get_yticks()
    if len(initial_yticks) < n_ylabels:
        # if there are less tick marks available than requested,
        # use the original tick mark locations
        yticks = initial_yticks
    else:
        # use the original tick marks to make a new array that
        # contains a subset of the original tick marks
        tick_idx = np.round(np.linspace(0, initial_yticks.size - 1, n_ylabels)).astype(
            int
        )
        yticks = initial_yticks[tick_idx]
    return yticks


def get_yticklabels(ax: Any) -> List[str]:
    """
    Utility function for ``get_y_axis_tick_labels`` that retrieves the
    y-axis tick mark labels from the input axis.

    Parameters
    ----------

    ax: a ``matplotlib`` axis object.

    Returns
    -------

    y_ticklabels: list of y-axis tick mark labels of length ``n_ylabels``.

    """
    # retrieve the original y-axis tick label strings from the axis object
    y_ticklabels = [tl.get_text() for tl in ax.get_yticklabels()]
    return y_ticklabels


def get_y_axis_tick_labels(ax: Any, n_ylabels: int = 6) -> npt.NDArray[np.intc]:
    """
    Sets the y-axis tick mark labels.

    Parameters
    ----------

    ax: a ``matplotlib`` axis object.

    n_ylabels: The number of y-axis tick mark labels to create. Default is 6.

    Returns
    -------

    y_ticklabels: array of y-axis tick mark labels of length ``n_ylabels``.

    """
    # get the original y-axis tick mark labels and convert them to an array
    initial_yticklabels = np.asarray(get_yticklabels(ax=ax))
    if initial_yticklabels.size < n_ylabels:
        # if there are less tick marks available than requested,
        # use the original tick mark labels
        y_ticklabels = initial_yticklabels

    else:
        # use the original tick marks to make a new array that
        # contains a subset of the original tick mark labels
        tick_idx = np.round(
            np.linspace(0, initial_yticklabels.size - 1, n_ylabels)
        ).astype(int)
        y_ticklabels = initial_yticklabels[tick_idx]
    return y_ticklabels


def set_x_axis_ticks_and_labels(
    jointgrid: Any, tmax: float, bin_max: float, n_xlabels: int = 4
):
    """
    Sets the x-axis tick mark locations and labels.

    Parameters
    ----------

    jointgrid: a ``sns.axisgrid.JointGrid`` object.

    tmax: the maximum time to plot.

    bin_max: the maximum number of bins.

    n_xlabels: the number of x-axis tick marks to create. Default is 4.

    """
    # retrieve the x-axis tick mark locations and labels
    xticks = get_x_axis_ticks(bin_max=bin_max, n_xlabels=n_xlabels)
    xticklabels = get_x_axis_tick_labels(max_time=tmax, n_xlabels=n_xlabels)
    # set the x-axis ticks and labels
    jointgrid.ax_joint.set_xticks(xticks)
    jointgrid.ax_joint.set_xticklabels(xticklabels, minor=False)


def set_y_axis_ticks_and_labels(jointgrid: Any, n_ylabels: int = 6):
    """
    Sets the y-axis tick mark locations and labels.

    Parameters
    ----------
    jointgrid: a ``sns.axisgrid.JointGrid`` object.

    n_ylabels: The number of y-axis tick mark labels to create. Default is 6.

    """
    # retrieve the y-axis tick mark locations and labels
    yticks = get_y_axis_ticks(ax=jointgrid.ax_joint, n_ylabels=n_ylabels)
    yticklabels = get_y_axis_tick_labels(ax=jointgrid.ax_joint, n_ylabels=n_ylabels)
    # set the new y-axis tick locations and labels
    jointgrid.ax_joint.set_yticks(yticks)
    jointgrid.ax_joint.set_yticklabels(yticklabels, minor=False)


def remove_marginal_graph_ticks_and_labels(marg_x: Any, marg_y: Any):
    """
    Removes the frame, tick marks, and tick mark
    labels for the marginal bar graphs.

    Parameters
    ----------
    marg_x : a x-axis marginal bar graph object.

    marg_y : a y-axis marginal bar graph object.
    """
    for marg in [marg_x, marg_y]:
        # turn the frame off for both bar graphs
        marg.axis("off")
        # remove all tick mark labels for both bar graphs
        marg.tick_params(
            axis="x", bottom=False, labelbottom=False, top=False, labeltop=False
        )
        marg.tick_params(
            axis="y", left=False, labelleft=False, right=False, labelright=False
        )


def adjust_for_colorbar(jointgrid: Any, fig_right: float, cbar_x0: float):
    """
    Makes various subplot location adjustments such that
    a colorbar can fit in the overal figure panel.

    Parameters
    ----------

    jointgrid: a ``sns.axisgrid.JointGrid`` object.

    fig_right: the location to set for the right side of the heatmap figure.

    cbar_x0: the x-axis location of the colorbar.

    """
    # adjust the subplot so the x/y tick labels are legible
    jointgrid.fig.subplots_adjust(
        left=0.1, bottom=0.15, top=0.9, hspace=0.03, wspace=0.04
    )
    # set the location of the right side of the figure
    jointgrid.fig.subplots_adjust(right=fig_right)
    # get the positions of the joint and marginal x axes
    pos_joint_ax = jointgrid.ax_joint.get_position()
    pos_marg_x_ax = jointgrid.ax_marg_x.get_position()
    # set the position and dimensions of the joint plot such that it fills
    # the space as if there was no colorbar
    jointgrid.ax_joint.set_position(
        [pos_joint_ax.x0, pos_joint_ax.y0, pos_marg_x_ax.width, pos_joint_ax.height]
    )
    # set the position of the colorbar such that it is on the
    # right side of the horizontal bar graph, and set its dimensions
    jointgrid.fig.axes[-1].set_position(
        [cbar_x0, pos_joint_ax.y0, 0.9, pos_joint_ax.height]
    )


def plot_heatmap(
    report: darshan.DarshanReport,
    mod: str = "DXT_POSIX",
    ops: Sequence[str] = ["read", "write"],
    xbins: int = 200,
    submodule: Optional[str] = None,
) -> Any:
    """
    Creates a heatmap with marginal bar graphs and colorbar.

    Parameters
    ----------

    report: a ``darshan.DarshanReport``.

    mod: the DXT module to do analysis for (i.e. "DXT_POSIX"
    or "DXT_MPIIO"). Default is ``"DXT_POSIX"``.

    ops: a sequence of keys designating which Darshan operations to use for
    data aggregation. Default is ``["read", "write"]``.

    xbins: the number of x-axis bins to create; it has
           no effect when `mod` is `HEATMAP`

    submodule: when `mod` is `HEATMAP` this specifies the
               source of the runtime heatmap data, otherwise
               it has no effect

    Returns
    -------

    jgrid: a ``sns.axisgrid.JointGrid`` object containing a heat
    map of IO data, marginal bar graphs, and a colorbar.

    Raises
    ------

    NotImplementedError: if a DXT module is not input (i.e. "DXT_POSIX").

    ValueError: if the input module is not in the ``DarshanReport``.

    """
    if "DXT" not in mod and "HEATMAP" not in mod:
        raise NotImplementedError("Only DXT and HEATMAP modules are supported.")

    if mod not in report.modules:
        raise ValueError(f"Module {mod} not found in DarshanReport.")

    nprocs = report.metadata["job"]["nprocs"]

    if "DXT" in mod:
        # aggregate the data according to the selected modules and operations
        agg_df = heatmap_handling.get_aggregate_data(report=report, mod=mod, ops=ops)
        # get the heatmap data array
        hmap_df = heatmap_handling.get_heatmap_df(agg_df=agg_df, xbins=xbins, nprocs=nprocs)
    elif mod == "HEATMAP":
        hmap_df = report.heatmaps[submodule].to_df(ops=ops)
        # mirror the DXT approach to heatmaps by
        # adding all-zero rows for inactive ranks
        hmap_df = hmap_df.reindex(index=range(nprocs), fill_value=0.0)
        xbins = hmap_df.shape[1]

    # build the joint plot with marginal histograms
    jgrid = sns.jointplot(kind="hist", bins=[xbins, nprocs], space=0.05)
    # clear the x and y axis marginal graphs
    jgrid.ax_marg_x.cla()
    jgrid.ax_marg_y.cla()

    # create the label for the colorbar
    colorbar_label = f"Data (B): {', '.join(ops)}"
    colorbar_kws = {"label": colorbar_label}
    # create the heatmap object using the heatmap data,
    # and assign it to the jointplot main figure
    hmap = sns.heatmap(
        hmap_df,
        ax=jgrid.ax_joint,
        # choose a color map that is not white at any value
        cmap="YlOrRd",
        norm=LogNorm(),
        cbar_kws=colorbar_kws,
    )

    # add text for x-axis bin count
    xbin_label = f"Time bins: {xbins}"
    plt.text(
        x=1.03,
        y=-0.04,
        s=xbin_label,
        fontsize=9,
        verticalalignment="top",
        horizontalalignment="left",
        transform=jgrid.ax_joint.transAxes,
    )

    # make the heatmap border visible
    for _, spine in hmap.spines.items():
        spine.set_visible(True)

    # if there is more than 1 process,
    # create the horizontal bar graph
    if nprocs > 1:
        jgrid.ax_marg_y.barh(
            y=np.arange(nprocs),
            width=hmap_df.sum(axis=1),
            align="edge",
            facecolor="black",
            lw=0.5,
        )
    else:
        # if there is only 1 rank turn the axis off
        jgrid.ax_marg_y.axis("off")

    # create the vertical bar graph
    jgrid.ax_marg_x.bar(
        x=np.arange(xbins),
        height=hmap_df.sum(axis=0),
        facecolor="black",
        align="edge",
    )

    tmax, runtime = determine_hmap_runtime(report=report)
    # scale the x-axis to span the calculated run time
    xbin_max = xbins * (runtime / tmax)
    jgrid.ax_joint.set_xlim(0.0, xbin_max)
    # set the x and y tick locations and labels using the runtime
    set_x_axis_ticks_and_labels(jointgrid=jgrid, tmax=runtime, bin_max=xbin_max, n_xlabels=4)
    set_y_axis_ticks_and_labels(jointgrid=jgrid, n_ylabels=6)

    # cleanup the marginal bar graph ticks and tick labels
    remove_marginal_graph_ticks_and_labels(
        marg_x=jgrid.ax_marg_x, marg_y=jgrid.ax_marg_y
    )

    # set the dimensions of the figure to 6.5" wide x 4.5" tall
    jgrid.fig.set_size_inches(6.5, 4.5)

    if nprocs > 1:
        # if there are multiple ranks we want to move the colorbar on the far
        # right side of the horizontal bar graph
        adjust_for_colorbar(jointgrid=jgrid, fig_right=0.84, cbar_x0=0.85)

    else:
        # if there is only 1 unique rank there is no horizontal bar graph,
        # so set the subplot dimensions to fill the space
        adjust_for_colorbar(jointgrid=jgrid, fig_right=0.92, cbar_x0=0.82)

    # invert the y-axis so rank values are increasing
    jgrid.ax_joint.invert_yaxis()

    # set the axis labels
    jgrid.ax_joint.set_xlabel("Time (s)")
    jgrid.ax_joint.set_ylabel("Rank")

    plt.close()

    return jgrid
