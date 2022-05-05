"""
Module for creating the I/O cost
bar graph for the Darshan job summary.
"""
from typing import Any

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick

import darshan


def get_by_avg_series(df: Any, mod_key: str, nprocs: int) -> Any:
    """
    Create the "by-average" series for the stacked
    bar graph in the I/O cost figure.

    Parameters
    ----------
    df: the dataframe containing the relevant data, typically the
    "fcounter" data from a Darshan report.

    mod_key: module to generate the I/O cost stacked
    bar graph for (i.e. "POSIX", "MPI-IO", "STDIO").

    nprocs: the number of MPI ranks used for the log of interest.

    Returns
    -------
    by_avg_series: a ``pd.Series`` containing the
    average read, write, and meta times.

    """
    # filter out all except the following columns
    cols = [
        f"{mod_key}_F_READ_TIME",
        f"{mod_key}_F_WRITE_TIME",
        f"{mod_key}_F_META_TIME",
    ]
    by_avg_series = df.filter(cols, axis=1).sum(axis=0) / nprocs
    # reindex to ensure 3 rows are always created
    by_avg_series = by_avg_series.reindex(cols, fill_value=0.0)
    # rename the columns so the labels are automatically generated when plotting
    name_dict = {cols[0]: "Read", cols[1]: "Write", cols[2]: "Meta"}
    by_avg_series.rename(index=name_dict, inplace=True)
    return by_avg_series


def combine_hdf5_modules(df: Any) -> Any:
    """
    Combines the "H5F" and "H5D" rows in the input dataframe into
    a single entry under the "HDF5" title.

    Parameters
    ----------
    df: a ``pd.DataFrame`` containing the average read, write, and meta
    times for various pydarshan modules (i.e. "POSIX", "MPI-IO", "STDIO").

    Returns
    -------
    Modified version of the input dataframe, where
    if either or both "H5F" and "H5D" modules are
    present, they have been renamed and/or summed
    under a new index "HDF5", if available.

    Notes
    -----
    If a single HDF5-related module is present it will
    be renamed as "HDF5". If no HDF5-related modules
    are present the dataframe will be unchanged.

    """
    # replace the H5D/H5F indexes with HDF5 and sum them
    df = df.reset_index().replace(to_replace=r"H5[FD]", value="HDF5", regex=True)
    df = df.groupby('index', sort=False).sum()
    # clean up the index name
    df.index.name = None
    return df


def get_io_cost_df(report: darshan.DarshanReport) -> Any:
    """
    Generates the I/O cost dataframe which contains the
    raw data to plot the I/O cost stacked bar graph.

    Parameters
    ----------
    report: a ``darshan.DarshanReport``.

    Returns
    -------
    io_cost_df: a ``pd.DataFrame`` containing the
    average read, write, and meta times.

    """
    io_cost_dict = {}
    supported_modules = ["POSIX", "MPI-IO", "STDIO", "H5F", "H5D"]
    for mod_key in report.modules:
        if mod_key in supported_modules:
            # collect the records in dataframe form
            recs = report.records[mod_key].to_df(attach=None)
            # correct the MPI module key
            if mod_key == "MPI-IO":
                mod_key = "MPIIO"
            nprocs = report.metadata["job"]["nprocs"]
            # collect the data needed for the I/O cost dataframe
            io_cost_dict[mod_key] = get_by_avg_series(
                df=recs["fcounters"],
                mod_key=mod_key,
                nprocs=nprocs,
                )

    # construct the I/O cost dataframe with
    # appropriate labels for each series
    # TODO: add the "by-slowest" category
    io_cost_df = pd.DataFrame(io_cost_dict).T

    # combine `H5F` and `H5D` modules
    io_cost_df = combine_hdf5_modules(df=io_cost_df)

    return io_cost_df


def plot_io_cost(report: darshan.DarshanReport) -> Any:
    """
    Creates a stacked bar graph illustrating the percentage of
    runtime spent in read, write, and metadata operations.

    Parameters
    ----------
    report: a ``darshan.DarshanReport``.

    Returns
    -------
    io_cost_fig: a ``matplotlib.pyplot.figure`` object containing a
    stacked bar graph of the average read, write, and metadata times.

    """
    # calculate the run time from the report metadata
    runtime = report.metadata["job"]["end_time"] - report.metadata["job"]["start_time"]
    if runtime == 0:
        # for cases where runtime is < 1, just set it
        # to 1 like the original perl code
        runtime = 1
    # get the I/O cost dataframe
    io_cost_df = get_io_cost_df(report=report)
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
    n_ticks = len(ax_norm.get_yticks())
    yticks = np.linspace(0, runtime, n_ticks)
    ax_raw.set_yticks(yticks)
    # add the legend and appropriate labels
    ax_raw.set_ylabel("Runtime (s)")
    handles, labels = ax_raw.get_legend_handles_labels()
    ax_norm.legend(handles[::-1], labels[::-1], loc="upper left", bbox_to_anchor=(1.22, 1.02))
    # adjust the figure to reduce white space
    io_cost_fig.subplots_adjust(right=0.59)
    io_cost_fig.tight_layout()
    plt.close()
    return io_cost_fig
