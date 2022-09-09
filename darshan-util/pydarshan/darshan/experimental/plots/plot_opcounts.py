# -*- coding: utf-8 -*-
from collections import defaultdict

import matplotlib.pyplot as plt
import numpy as np


def autolabel(ax, rects):
    """Attach a text label above each bar in *rects*, displaying its height."""
    for rect in rects:
        height = rect.get_height()
        ax.annotate(
            '{}'.format(height),
            xy=(rect.get_x() + rect.get_width() / 2, height),
            xytext=(0, 3),  # 3 points vertical offset
            textcoords="offset points",
            ha='center',
            va='bottom',
            rotation=45,
        )

def gather_count_data(report, mod):
    """
    Collect the module counts and labels
    for the I/O Operation Count plot.
    """
    # TODO: change to report.summary
    if 'agg_ioops' in dir(report):
        report.agg_ioops()
    else:
        print(
            "Cannot create summary, agg_ioops aggregator is not "
            "registered with the report class. Be sure to call "
            "darshan.experimental() once before invoking this plot."
        )

    mod_data = report.summary['agg_ioops'][mod]

    # Gather POSIX
    if mod == 'POSIX':
        labels = ['Read', 'Write', 'Open', 'Stat', 'Seek', 'Mmap', 'Fsync']
        counts = [
            mod_data['POSIX_READS'],
            mod_data['POSIX_WRITES'],
            mod_data['POSIX_OPENS'],
            mod_data['POSIX_STATS'],
            mod_data['POSIX_SEEKS'],
            0, # faulty? mod_data['POSIX_MMAPS'],
            mod_data['POSIX_FSYNCS'] + mod_data['POSIX_FDSYNCS']
        ]

    # Gather MPIIO
    elif mod == 'MPI-IO':
        labels = [
            'Ind. Read', 'Ind. Write', 'Ind. Open',
            'Col. Read', 'Col. Write', 'Col. Open', 'Sync']
        counts = [
            mod_data['MPIIO_INDEP_READS'],
            mod_data['MPIIO_INDEP_WRITES'],
            mod_data['MPIIO_INDEP_OPENS'],
            mod_data['MPIIO_COLL_READS'],
            mod_data['MPIIO_COLL_WRITES'],
            mod_data['MPIIO_COLL_OPENS'],
            mod_data['MPIIO_SYNCS'],
        ]

    # Gather Stdio
    elif mod == 'STDIO':
        labels = ['Read', 'Write', 'Open', 'Seek', 'Flush']
        counts = [
            mod_data['STDIO_READS'],
            mod_data['STDIO_WRITES'],
            mod_data['STDIO_OPENS'],
            mod_data['STDIO_SEEKS'],
            mod_data['STDIO_FLUSHES']
        ]

    elif mod == 'H5F':
        labels = [
            'H5D Read', 'H5D Write', 'H5D Open',
            'H5D Flush', 'H5F Open', 'H5F Flush',
        ]
        counts = [
            # set H5D counters to zero
            0, 0, 0, 0,
            mod_data['H5F_OPENS'],
            mod_data['H5F_FLUSHES'],
        ]

    elif mod == 'H5D':
        labels = [
            'H5D Read', 'H5D Write', 'H5D Open',
            'H5D Flush', 'H5F Open', 'H5F Flush',
        ]
        # H5F is not necessarily available following
        # gh-703
        if not "H5F" in report.summary["agg_ioops"]:
            report.summary['agg_ioops']['H5F'] = defaultdict(lambda: 0)

        counts = [
            report.summary['agg_ioops']['H5D']['H5D_READS'],
            report.summary['agg_ioops']['H5D']['H5D_WRITES'],
            report.summary['agg_ioops']['H5D']['H5D_OPENS'],
            report.summary['agg_ioops']['H5D']['H5D_FLUSHES'],
            report.summary['agg_ioops']['H5F']['H5F_OPENS'],
            report.summary['agg_ioops']['H5F']['H5F_FLUSHES'],
        ]

    elif mod == 'PNETCDF_FILE':
        labels = [
            'Var Ind Read', 'Var Ind Write', 'Var Open',
            'Var Coll Read', 'Var Coll Write',
            'Var NB Read', 'Var NB Write',
            'File Open',
            'File Sync',
            'File Ind Waits',
            'File Coll Waits',
        ]
        counts = [
                # most of the counters will all get set in PNETCDF_VAR
                0, 0, 0, 0, 0, 0, 0,
                mod_data["PNETCDF_FILE_OPENS"] + mod_data["PNETCDF_FILE_CREATES"],
                mod_data["PNETCDF_FILE_SYNCS"],
                mod_data['PNETCDF_FILE_INDEP_WAITS'],
                mod_data['PNETCDF_FILE_COLL_WAITS'],
        ]

    elif mod == 'PNETCDF_VAR':
        labels = [
            'Var Ind Read', 'Var Ind Write', 'Var Open',
            'Var Coll Read', 'Var Coll Write',
            'Var NB Read', 'Var NB Write',
            'File Open',
            'File Sync',
            'File Ind Waits',
            'File Coll Waits',
        ]
        counts = [
            report.summary['agg_ioops']['PNETCDF_VAR']['PNETCDF_VAR_INDEP_READS'],
            report.summary['agg_ioops']['PNETCDF_VAR']['PNETCDF_VAR_INDEP_WRITES'],
            report.summary['agg_ioops']['PNETCDF_VAR']['PNETCDF_VAR_OPENS'],
            report.summary['agg_ioops']['PNETCDF_VAR']['PNETCDF_VAR_COLL_READS'],
            report.summary['agg_ioops']['PNETCDF_VAR']['PNETCDF_VAR_COLL_WRITES'],
            report.summary['agg_ioops']['PNETCDF_VAR']['PNETCDF_VAR_NB_READS'],
            report.summary['agg_ioops']['PNETCDF_VAR']['PNETCDF_VAR_NB_WRITES'],
            # NOTE: should handle cases where only 1/2 PNETCDF mods
            # are present?
            (report.summary['agg_ioops']['PNETCDF_FILE']['PNETCDF_FILE_OPENS'] +
             report.summary['agg_ioops']['PNETCDF_FILE']['PNETCDF_FILE_CREATES']
            ),
            report.summary['agg_ioops']['PNETCDF_FILE']['PNETCDF_FILE_SYNCS'],
            report.summary['agg_ioops']['PNETCDF_FILE']['PNETCDF_FILE_INDEP_WAITS'],
            report.summary['agg_ioops']['PNETCDF_FILE']['PNETCDF_FILE_COLL_WAITS'],
        ]

    return labels, counts

def plot_opcounts(report, mod, ax=None):
    """
    Generates a bar chart summary for operation counts.

    Parameters
    ----------

    report (DarshanReport): darshan report object to plot

    mod: the module to plot operation counts for (i.e. "POSIX",
    "MPI-IO", "STDIO", "H5F", "H5D"). If "H5D" is input the returned
    figure will contain both "H5F" and "H5D" module data.

    """

    if ax is None:
        fig, ax = plt.subplots()
    else:
        fig = None

    labels, counts = gather_count_data(report=report, mod=mod)

    x = np.arange(len(labels))  # the label locations
    rects = ax.bar(x, counts)

    # Add some text for labels, title and custom x-axis tick labels, etc.
    ax.set_ylabel('Count')
    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=90)

    autolabel(ax=ax, rects=rects)

    plt.tight_layout()

    if fig is not None:
        plt.close()
        return fig
