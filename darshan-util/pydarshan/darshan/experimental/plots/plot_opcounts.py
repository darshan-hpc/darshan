# -*- coding: utf-8 -*-

import matplotlib.pyplot as plt
import numpy as np


def plot_opcounts(report, ax=None):
    """
    Generates a bar chart summary for operation counts.

	Args:
    	report (DarshanReport): darshan report object to plot
    """

    if ax is None:
        fig, ax = plt.subplots()
    else:
        fig = None

    # defaults
    labels = ['Read', 'Write', 'Open', 'Stat', 'Seek', 'Mmap', 'Fsync']
    posix_vals = [0, 0, 0, 0, 0, 0, 0]
    mpiind_vals = [0, 0, 0, 0, 0, 0, 0]
    mpicol_vals = [0, 0, 0, 0, 0, 0, 0]
    stdio_vals = [0, 0, 0, 0, 0, 0, 0]


    # TODO: change to report.summary
    if 'agg_ioops' in dir(report):
        report.agg_ioops()
    else:
        print(
            "Cannot create summary, agg_ioops aggregator is not "
            "registered with the report class. Be sure to call "
            "darshan.experimental() once before invoking this plot."
        )

    mods = report.summary['agg_ioops']

    # Gather POSIX
    if 'POSIX' in mods:

        posix = mods['POSIX']

        posix_vals = [
            posix['POSIX_READS'],
            posix['POSIX_WRITES'],
            posix['POSIX_OPENS'],
            posix['POSIX_STATS'],
            posix['POSIX_SEEKS'],
            0, # faulty? posix['POSIX_MMAPS'],
            posix['POSIX_FSYNCS'] + posix['POSIX_FDSYNCS']
        ]

    # Gather MPIIO
    if 'MPI-IO' in mods:

        mpiio = mods['MPI-IO']

        mpiind_vals = [
            mpiio['MPIIO_INDEP_READS'],
            mpiio['MPIIO_INDEP_WRITES'],
            mpiio['MPIIO_INDEP_OPENS'],
            0, # stat
            0, # seek
            0, # mmap
            0, # sync
        ]

        mpicol_vals = [
            mpiio['MPIIO_COLL_READS'],
            mpiio['MPIIO_COLL_WRITES'],
            mpiio['MPIIO_COLL_OPENS'],
            0, # stat
            0, # seek
            0, # mmap
            mpiio['MPIIO_SYNCS']
        ]

    # Gather Stdio
    if 'STDIO' in mods:

        stdio = mods['STDIO']

        stdio_vals = [
            stdio['STDIO_READS'],
            stdio['STDIO_WRITES'],
            stdio['STDIO_OPENS'],
            0, # stat
            stdio['STDIO_SEEKS'],
            0, # mmap
            stdio['STDIO_FLUSHES']
        ]

    x = np.arange(len(labels))  # the label locations
    width = 0.15  # the width of the bars

    rects1 = ax.bar(x - width/2 - width, posix_vals, width, label='POSIX')
    rects2 = ax.bar(x - width/2, mpiind_vals, width, label='MPI-IO Indep.')
    rects3 = ax.bar(x + width/2, mpicol_vals, width, label='MPI-IO Coll.')
    rects4 = ax.bar(x + width/2 + width, stdio_vals, width, label='STDIO')

    # Add some text for labels, title and custom x-axis tick labels, etc.
    ax.set_ylabel('Count')
    ax.set_xticks(x)
    ax.set_xticklabels(labels)
    ax.legend()


    def autolabel(rects):
        """Attach a text label above each bar in *rects*, displaying its height."""
        for rect in rects:
            height = rect.get_height()
            ax.annotate(
                '{}'.format(height),
                xy=(rect.get_x() + rect.get_width() / 4 + rect.get_width(), height),
                xytext=(0, 3),  # 3 points vertical offset
                textcoords="offset points",
                ha='center', va='bottom', rotation=45
                )

    autolabel(rects1)
    autolabel(rects2)
    autolabel(rects3)
    autolabel(rects4)

    plt.tight_layout()

    if fig is not None:
        plt.close()
        return fig
