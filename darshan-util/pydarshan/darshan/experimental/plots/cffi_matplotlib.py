# -*- coding: utf-8 -*-


import matplotlib
import matplotlib.pyplot as plt
import numpy as np

import darshan.backend.cffi_backend as backend



def plot_access_histogram(log, filter=None, data=None):
    """
    Plots a histogram of access sizes for specified module.

    :param log: Handle for an opened darshan log.
    :param str filter: Name of the module to generate plot for.
    :param data: Array/Dictionary for use with custom data. 
    """

    print("log:", log)
    print("filter:", filter)
    print("data:", data)


    # defaults
    labels = ['0-100', '101-1K', '1K-10K', '10K-100K', '100K-1M', '1M-4M', '4M-10M', '10M-100M', '100M-1G', '1G+']
    read_vals = [0, 0, 0, 0, 0,  0, 0, 0, 0, 0]
    write_vals = [0, 0, 0, 0, 0,  0, 0, 0, 0, 0]


    mods = backend.log_get_modules(log)


    if str(filter).upper() == "POSIX":
        posix_record = backend.log_get_posix_record(log)
        posix = dict(zip(backend.counter_names("POSIX"), posix_record['counters']))

        read_vals = [
            posix['POSIX_SIZE_READ_0_100'],
            posix['POSIX_SIZE_READ_100_1K'],
            posix['POSIX_SIZE_READ_1K_10K'],
            posix['POSIX_SIZE_READ_10K_100K'],
            posix['POSIX_SIZE_READ_100K_1M'],
            posix['POSIX_SIZE_READ_1M_4M'],
            posix['POSIX_SIZE_READ_4M_10M'],
            posix['POSIX_SIZE_READ_10M_100M'],
            posix['POSIX_SIZE_READ_100M_1G'],
            posix['POSIX_SIZE_READ_1G_PLUS']
        ]

        write_vals = [
            posix['POSIX_SIZE_WRITE_0_100'],
            posix['POSIX_SIZE_WRITE_100_1K'],
            posix['POSIX_SIZE_WRITE_1K_10K'],
            posix['POSIX_SIZE_WRITE_10K_100K'],
            posix['POSIX_SIZE_WRITE_100K_1M'],
            posix['POSIX_SIZE_WRITE_1M_4M'],
            posix['POSIX_SIZE_WRITE_4M_10M'],
            posix['POSIX_SIZE_WRITE_10M_100M'],
            posix['POSIX_SIZE_WRITE_100M_1G'],
            posix['POSIX_SIZE_WRITE_1G_PLUS']
        ]

    elif str(filter).upper() == "MPIIO":
        mpiio_record = backend.log_get_mpiio_record(log)
        mpiio = dict(zip(backend.counter_names("mpiio"), mpiio_record['counters']))

        read_vals = [
            mpiio['MPIIO_SIZE_READ_AGG_0_100'],
            mpiio['MPIIO_SIZE_READ_AGG_100_1K'],
            mpiio['MPIIO_SIZE_READ_AGG_1K_10K'],
            mpiio['MPIIO_SIZE_READ_AGG_10K_100K'],
            mpiio['MPIIO_SIZE_READ_AGG_100K_1M'],
            mpiio['MPIIO_SIZE_READ_AGG_1M_4M'],
            mpiio['MPIIO_SIZE_READ_AGG_4M_10M'],
            mpiio['MPIIO_SIZE_READ_AGG_10M_100M'],
            mpiio['MPIIO_SIZE_READ_AGG_100M_1G'],
            mpiio['MPIIO_SIZE_READ_AGG_1G_PLUS'],
        ]

        write_vals = [
            mpiio['MPIIO_SIZE_WRITE_AGG_0_100'],
            mpiio['MPIIO_SIZE_WRITE_AGG_100_1K'],
            mpiio['MPIIO_SIZE_WRITE_AGG_1K_10K'],
            mpiio['MPIIO_SIZE_WRITE_AGG_10K_100K'],
            mpiio['MPIIO_SIZE_WRITE_AGG_100K_1M'],
            mpiio['MPIIO_SIZE_WRITE_AGG_1M_4M'],
            mpiio['MPIIO_SIZE_WRITE_AGG_4M_10M'],
            mpiio['MPIIO_SIZE_WRITE_AGG_10M_100M'],
            mpiio['MPIIO_SIZE_WRITE_AGG_100M_1G'],
            mpiio['MPIIO_SIZE_WRITE_AGG_1G_PLUS'],
        ]



    x = np.arange(len(labels))  # the label locations
    width = 0.35  # the width of the bars

    fig, ax = plt.subplots()
    rects1 = ax.bar(x - width/2, read_vals, width, label='Read')
    rects2 = ax.bar(x + width/2, write_vals, width, label='Write')

    # Add some text for labels, title and custom x-axis tick labels, etc.
    ax.set_ylabel('Count')
    ax.set_title('Historgram of Access Sizes: ' + str(filter))
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

    plt.show()
    pass


def plot_time_summary(log=None, filter=None, data=None):
    """
    TODO: Not implemented.

    :param log: Handle for an opened darshan log.
    :param str filter: Name of the module to generate plot for.
    :param data: Array/Dictionary for use with custom data. 
    """

    pass



def plot_opcounts(log=None, filter=None, data=None):
    """
    Generates a baor chart summary for operation counts.

    :param log: Handle for an opened darshan log.
    :param str filter: Name of the module to generate plot for.
    :param data: Array/Dictionary for use with custom data. 
    """

    # defaults
    labels = ['Read', 'Write', 'Open', 'Stat', 'Seek', 'Mmap', 'Fsync']
    posix_vals = [0, 0, 0, 0, 0, 0, 0]
    mpiind_vals = [0, 0, 0, 0, 0, 0, 0]
    mpicol_vals = [0, 0, 0, 0, 0, 0, 0]
    stdio_vals = [0, 0, 0, 0, 0, 0, 0]


    mods = backend.log_get_modules(log)

    # Gather POSIX
    if 'POSIX' in mods:
        posix_record = backend.log_get_posix_record(log)
        posix = dict(zip(backend.counter_names("POSIX"), posix_record['counters']))

        posix_vals = [
            posix['POSIX_READS'],
            posix['POSIX_WRITES'],
            posix['POSIX_OPENS'],
            posix['POSIX_STATS'],
            posix['POSIX_SEEKS'],
            posix['POSIX_MMAPS'],
            posix['POSIX_FSYNCS'] + posix['POSIX_FDSYNCS']
        ]

    # Gather MPIIO
    if 'MPI-IO' in mods:
        mpiio_record = backend.log_get_mpiio_record(log)
        mpiio = dict(zip(backend.counter_names("mpiio"), mpiio_record['counters']))

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
        stdio_record = backend.log_get_stdio_record(log)
        stdio = dict(zip(backend.counter_names("STDIO"), stdio_record['counters']))

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

    fig, ax = plt.subplots()
    rects1 = ax.bar(x - width/2 - width, posix_vals, width, label='POSIX')
    rects2 = ax.bar(x - width/2, mpiind_vals, width, label='MPI-IO Indep.')
    rects3 = ax.bar(x + width/2, mpicol_vals, width, label='MPI-IO Coll.')
    rects4 = ax.bar(x + width/2 + width, stdio_vals, width, label='STDIO')

    # Add some text for labels, title and custom x-axis tick labels, etc.
    ax.set_ylabel('Count')
    ax.set_title('I/O Operation Counts')
    ax.set_xticks(x)
    ax.set_xticklabels(labels)
    ax.legend()


    def autolabel(rects):
        """Attach a text label above each bar in *rects*, displaying its height."""
        for rect in rects:
            height = rect.get_height()
            ax.annotate(
                '{}'.format(height),
                xy=(rect.get_x() + rect.get_width() / 4, height),
                xytext=(0, 3),  # 3 points vertical offset
                textcoords="offset points",
                ha='center', va='bottom', rotation=0
                )


    autolabel(rects1)
    autolabel(rects2)
    autolabel(rects3)
    autolabel(rects4)

    fig.tight_layout()

    plt.show()
    pass




def plot_timeline(log=None, filter=None, data=None):
    """
    Plots a timeline of opened files.


    :param log: Handle for an opened darshan log.
    :param str filter: Name of the module to generate plot for.
    :param data: Array/Dictionary for use with custom data. 
    """

    fig, ax = plt.subplots()
    ax.broken_barh([(110, 30), (150, 10)], (10, 9), facecolors='tab:blue')
    ax.broken_barh([(10, 50), (100, 20), (130, 10)], (20, 9),
                facecolors=('tab:orange', 'tab:green', 'tab:red'))
    ax.set_ylim(5, 35)
    ax.set_xlim(0, 200)
    ax.set_xlabel('seconds since start')
    ax.set_yticks([15, 25])
    ax.set_yticklabels(['Rank 0', 'Rank 1'])
    ax.set_title('TODO: This is only a placeholder.')
    ax.grid(True)

    plt.show()
