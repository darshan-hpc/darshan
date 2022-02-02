# -*- coding: utf-8 -*-

import matplotlib.pyplot as plt
import numpy as np

def autolabel(ax, rects):
    """Attach a text label above each bar in *rects*, displaying its value."""
    for rect in rects:
        height = rect.get_height()
        ax.annotate(
            '{}'.format(height),
            xy=(rect.get_x() + rect.get_width() / 2, height),
            xytext=(0, 3),  # 3 points vertical offset
            textcoords="offset points",
            ha='center',
            va='bottom',
            rotation=0,
        )

def plot_access_histogram(report, mod, ax=None):
    """
    Plots a histogram of access sizes for specified module.

	Args:
		report (darshan.DarshanReport): report to generate plot from
		mod (str): mod-string for which to generate access_histogram

    """

    if ax is None:
        fig, ax = plt.subplots()
    else:
        fig = None

    # TODO: change to report.summary
    if 'mod_agg_iohist' in dir(report):
        report.mod_agg_iohist(mod)
    else:
        print("Cannot create summary, mod_agg_iohist aggregator is not registered with the report class.")

    # defaults
    labels = ['0-100', '101-1K', '1K-10K', '10K-100K', '100K-1M', '1M-4M', '4M-10M', '10M-100M', '100M-1G', '1G+']

    agg = report.summary['agg_iohist'][mod]
    # TODO: can simplify the read/write vals below after
    # support for python 3.6 is dropped
    read_vals = [
        agg['READ_0_100'],
        agg['READ_100_1K'],
        agg['READ_1K_10K'],
        agg['READ_10K_100K'],
        agg['READ_100K_1M'],
        agg['READ_1M_4M'],
        agg['READ_4M_10M'],
        agg['READ_10M_100M'],
        agg['READ_100M_1G'],
        agg['READ_1G_PLUS']
    ]

    write_vals = [
        agg['WRITE_0_100'],
        agg['WRITE_100_1K'],
        agg['WRITE_1K_10K'],
        agg['WRITE_10K_100K'],
        agg['WRITE_100K_1M'],
        agg['WRITE_1M_4M'],
        agg['WRITE_4M_10M'],
        agg['WRITE_10M_100M'],
        agg['WRITE_100M_1G'],
        agg['WRITE_1G_PLUS']
    ]

    x = np.arange(len(labels))  # the label locations
    width = 0.35  # the width of the bars

    rects1 = ax.bar(x - width/2, read_vals, width, label='Read')
    rects2 = ax.bar(x + width/2, write_vals, width, label='Write')

    # Add some text for labels, title and custom x-axis tick labels, etc.
    ax.set_ylabel('Count')
    ax.set_xlabel('Access Sizes')
    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=45, ha='right')
    ax.legend()

    autolabel(ax=ax, rects=rects1)
    autolabel(ax=ax, rects=rects2)

    plt.tight_layout()

    if fig is not None:
        plt.close()
        return fig
