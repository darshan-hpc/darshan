# -*- coding: utf-8 -*-


from darshan.report import *


import matplotlib
import matplotlib.pyplot as plt
import numpy as np



def plot_access_histogram(self, mod, filter=None, data=None):
    """
    Plots a histogram of access sizes for specified module.

    :param log: Handle for an opened darshan log.
    :param str filter: Name of the module to generate plot for.
    :param data: Array/Dictionary for use with custom data. 
    """

    # TODO: change to self.summary
    if 'mod_agg_iohist' in dir(self):
        print("Summarizing... iohist", mod)
        self.mod_agg_iohist(mod)
    else:
        print("Can not create summary, mod_agg_iohist aggregator is not registered with the report class.")



    # defaults
    labels = ['0-100', '101-1K', '1K-10K', '10K-100K', '100K-1M', '1M-4M', '4M-10M', '10M-100M', '100M-1G', '1G+']
    read_vals = [0, 0, 0, 0, 0,  0, 0, 0, 0, 0]
    write_vals = [0, 0, 0, 0, 0,  0, 0, 0, 0, 0]

  


    posix = self.summary['agg_iohist'][mod]

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

    #plt.show()
   
    return plt


def plot_time_summary(self, filter=None, data=None):
    """
    TODO: Not implemented.

    :param log: Handle for an opened darshan log.
    :param str filter: Name of the module to generate plot for.
    :param data: Array/Dictionary for use with custom data. 
    """

    pass



def plot_opcounts(self, filter=None, data=None, return_csv=False):
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


    # TODO: change to self.summary
    if 'agg_ioops' in dir(self):
        print("Summarizing... agg_ioops")
        self.agg_ioops()
    else:
        print("Can not create summary, agg_ioops aggregator is not registered with the report clase.")





    mods = self.summary['agg_ioops']

    # Gather POSIX
    if 'POSIX' in mods:
        #posix_record = backend.log_get_posix_record(log)
        #posix = dict(zip(backend.counter_names("POSIX"), posix_record['counters']))
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
        #mpiio_record = backend.log_get_mpiio_record(log)
        #mpiio = dict(zip(backend.counter_names("mpiio"), mpiio_record['counters']))

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
        #stdio_record = backend.log_get_stdio_record(log)
        #stdio = dict(zip(backend.counter_names("STDIO"), stdio_record['counters']))

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



    def as_csv():
        text = ""
        text += ','.join(labels) +  ',Layer' + "\n"
        text += ','.join(str(x) for x in posix_vals) + ',POSIX' + "\n"
        text += ','.join(str(x) for x in mpiind_vals) + ',MPIIND' + "\n"
        text += ','.join(str(x) for x in mpicol_vals) + ',MPICOL' + "\n"
        text += ','.join(str(x) for x in stdio_vals) + ',STDIO' + "\n"

        return text



    print(as_csv())


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
                xy=(rect.get_x() + rect.get_width() / 4 + rect.get_width(), height),
                xytext=(0, 3),  # 3 points vertical offset
                textcoords="offset points",
                ha='center', va='bottom', rotation=45
                )


    autolabel(rects1)
    autolabel(rects2)
    autolabel(rects3)
    autolabel(rects4)

    fig.tight_layout()

    #plt.show()
    

    if return_csv:
        return plt, as_csv()
    else:
        return plt




def plot_timeline(self, filter=None, data=None):
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
