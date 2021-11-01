"""
Module for creating the the I/O cost
bar graph for the Darshan job summary.
"""
from typing import Any

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick

import darshan


def get_io_cost_df(report: darshan.DarshanReport, mod_key: str) -> Any:
    """
    Generates the I/O cost dataframe which contains the
    raw data to plot the I/O cost stacked bar graph.

    Parameters
    ----------
    report: a ``darshan.DarshanReport``.

    mod_key: module to generate the I/O cost stacked
    bar graph for (i.e. "POSIX", "MPI-IO", "STDIO").

    Returns
    -------
    io_cost_df: a ``pd.DataFrame`` containing the
    average read, write, and meta times.

    """
    # collect the records in dataframe form
    recs = report.records[mod_key].to_df(attach=["rank"])
    # correct the MPI module key
    if mod_key == "MPI-IO":
        mod_key = "MPIIO"
    # filter out all except the following columns
    cols = [
        "rank",
        f"{mod_key}_F_READ_TIME",
        f"{mod_key}_F_WRITE_TIME",
        f"{mod_key}_F_META_TIME",
    ]
    rd_wr_meta_df = recs["fcounters"].filter(cols, axis=1)
    # locate any rows where "rank" == -1 and divide them by nprocs
    nprocs = report.metadata["job"]["nprocs"]
    rd_wr_meta_df.loc[rd_wr_meta_df["rank"] == -1] /= nprocs
    # drop the "rank" column since it's no longer needed
    rd_wr_meta_df.drop("rank", axis=1, inplace=True)
    # rename the columns so the labels are automatically generated when plotting
    name_dict = {cols[1]: "Read", cols[2]: "Write", cols[3]: "Meta"}
    rd_wr_meta_df.rename(columns=name_dict, inplace=True)
    # calculate the means and put the output `pd.Series` into a new dataframe
    io_cost_df = pd.DataFrame({"Avg. Operation Time": rd_wr_meta_df.mean(axis=0)}).T
    return io_cost_df


def plot_io_cost(report: darshan.DarshanReport, mod_key: str) -> Any:
    """
    Creates a stacked bar graph illustrating the percentage of
    runtime spent in read, write, and metadata operations.

    Parameters
    ----------
    report: a ``darshan.DarshanReport``.

    mod_key: module to generate the I/O cost stacked
    bar graph for (i.e. "POSIX", "MPI-IO", "STDIO").

    Returns
    -------
    io_cost_fig: a ``matplotlib.pyplot.figure`` object containing a
    stacked bar graph of the average read, write, and metadata times.

    Raises
    ------
    NotImplementedError: raised if the input module key is not "POSIX",
    "MPI-IO", or "STDIO".

    """
    if mod_key not in ["POSIX", "MPI-IO", "STDIO"]:
        # TODO: expand the scope of this function
        # to include HDF5 module
        raise NotImplementedError(f"{mod_key} module is not supported.")
    # calculate the run time from the report metadata
    runtime = report.metadata["job"]["end_time"] - report.metadata["job"]["start_time"]
    if runtime == 0:
        # for cases where runtime is < 1, just set it
        # to 1 like the original perl code
        runtime = 1
    # get the I/O cost dataframe
    io_cost_df = get_io_cost_df(report=report, mod_key=mod_key)
    # generate a figure with 2 y axes
    io_cost_fig = plt.figure(figsize=(4.5, 4))
    ax_raw = io_cost_fig.add_subplot(111)
    ax_norm = ax_raw.twinx()
    # use the dataframe to plot the stacked bar graph
    io_cost_df.plot.bar(stacked=True, rot=0, ax=ax_raw, legend=False, zorder=3)
    ax_raw.grid(axis="y", zorder=0, alpha=0.6)
    # set the y limits for both axes
    ax_raw.set_ylim(0, runtime)
    ax_norm.set_ylim(0, 100)
    # convert the normalized axis y labels to percentages
    ax_norm.yaxis.set_major_formatter(mtick.PercentFormatter())
    # align both axes tick labels so the grid matches
    # values on both sides of the bar graph
    norm_yticks = ax_raw.get_yticks()
    n_ticks = len(ax_norm.get_yticks())
    yticks = np.linspace(norm_yticks[0], norm_yticks[-1], n_ticks)
    ax_raw.set_yticks(yticks)
    # add the legend and appropriate labels
    ax_raw.set_ylabel("Runtime (s)")
    handles, labels = ax_raw.get_legend_handles_labels()
    ax_norm.legend(handles[::-1], labels[::-1], loc="upper left", bbox_to_anchor=(1.3, 1.02), title="Operation")
    # adjust the figure to reduce white space
    io_cost_fig.subplots_adjust(right=0.59)
    io_cost_fig.tight_layout()
    return io_cost_fig
