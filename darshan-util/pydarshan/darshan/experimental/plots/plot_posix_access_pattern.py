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
            rotation=45,
        )

def plot_posix_access_pattern(record, ax=None):
    """
    Plots read/write access patterns (sequential vs consecutive access counts)
    for a given POSIX module file record.

	Args:
		record (dict): POSIX module record to plot access pattern for.

    """

    if ax is None:
        fig, ax = plt.subplots()
    else:
        fig = None

    labels = ['read', 'write']
    total_data = [record['counters']['POSIX_READS'][0],
                  record['counters']['POSIX_WRITES'][0]]
    seq_data = [record['counters']['POSIX_SEQ_READS'][0],
                record['counters']['POSIX_SEQ_WRITES'][0]]
    consec_data = [record['counters']['POSIX_CONSEC_READS'][0],
                   record['counters']['POSIX_CONSEC_WRITES'][0]]

    x = np.arange(len(labels))  # the label locations
    width = 0.2  # the width of the bars

    rects_total = ax.bar(x - width, total_data, width, label = 'total')
    rects_seq = ax.bar(x, seq_data, width, label = 'sequential')
    rects_consec = ax.bar(x + width, consec_data, width, label = 'consecutive')

    ax.set_ylabel('Count')
    ax.set_xticks(x)
    ax.set_xticklabels(labels)
    ax.legend(loc='center left', bbox_to_anchor=(1.05,.5))

    ax.spines[['right', 'top']].set_visible(False)

    autolabel(ax=ax, rects=rects_total)
    autolabel(ax=ax, rects=rects_seq)
    autolabel(ax=ax, rects=rects_consec)

    plt.tight_layout()

    if fig is not None:
        plt.close()
        return fig
