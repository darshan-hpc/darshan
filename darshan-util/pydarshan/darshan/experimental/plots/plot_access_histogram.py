# -*- coding: utf-8 -*-

import matplotlib.pyplot as plt
import numpy as np

def autolabel(ax, rects):
    """Attach a text label above each bar in *rects*, displaying its value."""
    for rect in rects:
        height = rect.get_height()
        if height > 0:
            ax.annotate(
                '{}'.format(height),
                xy=(rect.get_x() + rect.get_width() / 2, height),
                xytext=(0, 3),  # 3 points vertical offset
                textcoords="offset points",
                ha='center',
                va='bottom',
                rotation=45,
            )

def plot_access_histogram(record, mod, ax=None):
    """
    Plots a histogram of access sizes for specified module.

	Args:
		record: record to generate plot from
		mod (str): mod-string for which to generate access_histogram

    """

    if ax is None:
        fig, ax = plt.subplots()
    else:
        fig = None


    # defaults
    labels = ['0-100', '101-1K', '1K-10K', '10K-100K', '100K-1M', '1M-4M', '4M-10M', '10M-100M', '100M-1G', '1G+']

    agg=record['counters']
    if mod == 'POSIX':
        read_vals = [
            agg['POSIX_SIZE_READ_0_100'][0],
            agg['POSIX_SIZE_READ_100_1K'][0],
            agg['POSIX_SIZE_READ_1K_10K'][0],
            agg['POSIX_SIZE_READ_10K_100K'][0],
            agg['POSIX_SIZE_READ_100K_1M'][0],
            agg['POSIX_SIZE_READ_1M_4M'][0],
            agg['POSIX_SIZE_READ_4M_10M'][0],
            agg['POSIX_SIZE_READ_10M_100M'][0],
            agg['POSIX_SIZE_READ_100M_1G'][0],
            agg['POSIX_SIZE_READ_1G_PLUS'][0]
        ]

        write_vals = [
            agg['POSIX_SIZE_WRITE_0_100'][0],
            agg['POSIX_SIZE_WRITE_100_1K'][0],
            agg['POSIX_SIZE_WRITE_1K_10K'][0],
            agg['POSIX_SIZE_WRITE_10K_100K'][0],
            agg['POSIX_SIZE_WRITE_100K_1M'][0],
            agg['POSIX_SIZE_WRITE_1M_4M'][0],
            agg['POSIX_SIZE_WRITE_4M_10M'][0],
            agg['POSIX_SIZE_WRITE_10M_100M'][0],
            agg['POSIX_SIZE_WRITE_100M_1G'][0],
            agg['POSIX_SIZE_WRITE_1G_PLUS'][0]
        ]
    elif mod == 'MPI-IO':
        read_vals = [
            agg['MPIIO_SIZE_READ_AGG_0_100'][0],
            agg['MPIIO_SIZE_READ_AGG_100_1K'][0],
            agg['MPIIO_SIZE_READ_AGG_1K_10K'][0],
            agg['MPIIO_SIZE_READ_AGG_10K_100K'][0],
            agg['MPIIO_SIZE_READ_AGG_100K_1M'][0],
            agg['MPIIO_SIZE_READ_AGG_1M_4M'][0],
            agg['MPIIO_SIZE_READ_AGG_4M_10M'][0],
            agg['MPIIO_SIZE_READ_AGG_10M_100M'][0],
            agg['MPIIO_SIZE_READ_AGG_100M_1G'][0],
            agg['MPIIO_SIZE_READ_AGG_1G_PLUS'][0]
        ]

        write_vals = [
            agg['MPIIO_SIZE_WRITE_AGG_0_100'][0],
            agg['MPIIO_SIZE_WRITE_AGG_100_1K'][0],
            agg['MPIIO_SIZE_WRITE_AGG_1K_10K'][0],
            agg['MPIIO_SIZE_WRITE_AGG_10K_100K'][0],
            agg['MPIIO_SIZE_WRITE_AGG_100K_1M'][0],
            agg['MPIIO_SIZE_WRITE_AGG_1M_4M'][0],
            agg['MPIIO_SIZE_WRITE_AGG_4M_10M'][0],
            agg['MPIIO_SIZE_WRITE_AGG_10M_100M'][0],
            agg['MPIIO_SIZE_WRITE_AGG_100M_1G'][0],
            agg['MPIIO_SIZE_WRITE_AGG_1G_PLUS'][0]
        ]
    #TODO: add support for HDF5/PnetCDF modules
    x = np.arange(len(labels))  # the label locations
    width = 0.35  # the width of the bars

    rects1 = ax.bar(x - width/2, read_vals, width, label='Read')
    rects2 = ax.bar(x + width/2, write_vals, width, label='Write')

    # Add some text for labels, title and custom x-axis tick labels, etc.
    ax.set_ylabel('Count')
    ax.set_xlabel('Access Sizes')
    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=45, ha='right')
    ax.set_ylim(ymin = 0)
    ax.legend()

    ax.spines[['right', 'top']].set_visible(False)

    autolabel(ax=ax, rects=rects1)
    autolabel(ax=ax, rects=rects2)

    plt.tight_layout()

    if fig is not None:
        plt.close()
        return fig
