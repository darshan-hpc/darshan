# -*- coding: utf-8 -*-

from darshan.report import *

import matplotlib
import matplotlib.pyplot as plt
import numpy as np


def plot_access_histogram(report, mod):
    """
    Plots a histogram of access sizes for specified module.

	Args:
		report (darshan.DarshanReport): report to generate plot from
		mod (str): mod-string for which to generate access_histogram

    """

    # TODO: change to report.summary
    if 'mod_agg_iohist' in dir(report):
        report.mod_agg_iohist(mod)
    else:
        print("Can not create summary, mod_agg_iohist aggregator is not registered with the report class.")

    # defaults
    labels = ['0-100', '101-1K', '1K-10K', '10K-100K', '100K-1M', '1M-4M', '4M-10M', '10M-100M', '100M-1G', '1G+']
    read_vals = [0, 0, 0, 0, 0,  0, 0, 0, 0, 0]
    write_vals = [0, 0, 0, 0, 0,  0, 0, 0, 0, 0]

    posix = report.summary['agg_iohist'][mod]

    read_vals = [
        posix['READ_0_100'],
        posix['READ_100_1K'],
        posix['READ_1K_10K'],
        posix['READ_10K_100K'],
        posix['READ_100K_1M'],
        posix['READ_1M_4M'],
        posix['READ_4M_10M'],
        posix['READ_10M_100M'],
        posix['READ_100M_1G'],
        posix['READ_1G_PLUS']
    ]

    write_vals = [
        posix['WRITE_0_100'],
        posix['WRITE_100_1K'],
        posix['WRITE_1K_10K'],
        posix['WRITE_10K_100K'],
        posix['WRITE_100K_1M'],
        posix['WRITE_1M_4M'],
        posix['WRITE_4M_10M'],
        posix['WRITE_10M_100M'],
        posix['WRITE_100M_1G'],
        posix['WRITE_1G_PLUS']
    ]


    x = np.arange(len(labels))  # the label locations
    width = 0.35  # the width of the bars

    fig, ax = plt.subplots()
    rects1 = ax.bar(x - width/2, read_vals, width, label='Read')
    rects2 = ax.bar(x + width/2, write_vals, width, label='Write')

    # Add some text for labels, title and custom x-axis tick labels, etc.
    ax.set_ylabel('Count')
    ax.set_title('Historgram of Access Sizes: ' + str(mod))
    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=45, ha='right')
    ax.legend()

    def autolabel(rects):
        """Attach a text label above each bar in *rects*, displaying its height."""
        for rect in rects:
            height = rect.get_height()
            ax.annotate('{}'.format(height),
                        xy=(rect.get_x() + rect.get_width() / 2, height),
                        xytext=(0, 3),  # 3 points vertical offset
                        textcoords="offset points",
                        ha='center', va='bottom', rotation=0)

    autolabel(rects1)
    autolabel(rects2)

    fig.tight_layout()

    return fig
