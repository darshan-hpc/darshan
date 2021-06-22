import argparse
import base64
import datetime
import genshi.template
import importlib.resources as pkg_resources
import re
import io
import matplotlib as mpl
import matplotlib.pyplot as plt
import pandas
import pytz
import sys
import pprint
import pandas as pd
import seaborn as sns
import itertools, numpy
import plotnine as p9
from plotnine import *
import darshan
import darshan.templates
from PIL import Image

import matplotlib.font_manager as fm
from mpl_toolkits.mplot3d import Axes3D
import seaborn as sns
from numpy import genfromtxt
from IPython.display import set_matplotlib_formats
set_matplotlib_formats('pdf', 'png')
from palettable.cmocean.diverging import Delta_20
cmap=Delta_20.hex_colors
import matplotlib.font_manager as fm
from matplotlib import pyplot as plt
#font = fm.FontProperties(
#       family = 'Gill Sans',
#       fname = './GilliusADF-Regular.otf')

#from matplotlib import rcParams
#rcParams['font.family'] = font.get_family()
#rcParams['font.sans-serif'] = font.
cbbPalette = ["#D95F02","#7570B3","#66A61E","#E6AB02","#A6761D","#CC6666", "#9999CC", "#66CC99", "#56B4E9", "#0072B2", "#D55E00", "#CC79A7"]

#plt.rcParams['font.style'] = 'normal'
#plt.rcParams['font.serif'] = 'Times'
#from sklearn.model_selection import train_test_split
#from scipy.stats import linregress
#from scipy.stats import skew,pearsonr, kurtosis, kurtosistest, iqr, ks_2samp, ttest_ind, gaussian_kde
#from scipy.stats import bayes_mvs
#from statsmodels import robust

def setup_parser(parser):
    # setup arguments
    parser.add_argument('--input', help='darshan log file', nargs='?')
    parser.add_argument('--output', action='store', help='output file name')
    parser.add_argument('--verbose', help='', action='store_true')
    parser.add_argument('--debug', help='', action='store_true')

def init_plotting():
    #plt.rcParams['figure.figsize'] = (10, 7)
    plt.rcParams['font.size'] = 14
    plt.rcParams['axes.labelsize'] = plt.rcParams['font.size']
    plt.rcParams['axes.titlesize'] = plt.rcParams['font.size']
    plt.rcParams['legend.fontsize'] = 0.9*plt.rcParams['font.size']
    plt.rcParams['xtick.labelsize'] = plt.rcParams['font.size']
    plt.rcParams['ytick.labelsize'] = plt.rcParams['font.size']
    plt.rcParams['xtick.minor.visible']=False
    plt.rcParams['ytick.minor.visible']=False
    plt.rcParams['xtick.major.size'] = 6
    plt.rcParams['xtick.minor.size'] = 3
    plt.rcParams['xtick.major.width'] = 2
    plt.rcParams['xtick.minor.width'] = 2
    plt.rcParams['ytick.major.size'] = 6
    plt.rcParams['ytick.minor.size'] = 3
    plt.rcParams['ytick.major.width'] = 2
    plt.rcParams['ytick.minor.width'] = 2
    plt.rcParams['xtick.major.pad']='8'
    plt.rcParams['ytick.major.pad']='8'
    plt.rcParams['ytick.color'] = "#808080"
    plt.rcParams['xtick.color'] = "#808080"
    plt.rcParams['xtick.direction'] = 'in'
    plt.rcParams['ytick.direction'] = 'in'
    plt.rcParams['legend.frameon'] = False
    plt.rcParams['legend.loc'] = 'best'
    plt.rcParams['axes.linewidth'] = 3

    plt.gca().spines['bottom'].set_color('#808080')
    plt.gca().spines['left'].set_color('#808080')
    ## to hide the spines 
    #plt.gca().spines['top'].set_visible(False)
    #plt.gca().spines['right'].set_visible(False)
    plt.gca().spines['top'].set_color('#808080')
    plt.gca().spines['right'].set_color('#808080')
    plt.gca().spines['top'].set_linewidth(.8)
    plt.gca().spines['right'].set_linewidth(.8)
    plt.grid(True,linestyle='-', linewidth=0.01)
    #plt.gca().legend(ncol=4)
    #plt.gca().legend(prop={'family': 'monospace'})

def hide_spines():
    """Hides the top and rightmost axis spines from view for all active
    figures and their respective axes."""
    # Retrieve a list of all current figures.
    figures = [x for x in mpl._pylab_helpers.Gcf.get_all_fig_managers()]
    for figure in figures:
        # Get all Axis instances related to the figure.
        for ax in figure.canvas.figure.get_axes():
            
            ax.spines['left'].set_color("#808080")
            ax.spines['bottom'].set_color("#808080")
            ax.spines['top'].set_color("#808080")
            ax.spines['right'].set_color("#808080")
            #ax.spines['right'].set_visible('False')
            #ax.spines['top'].set_visible('False')
            ax.xaxis.set_ticks_position('bottom')
            ax.yaxis.set_ticks_position('left')
            ax.spines['top'].set_linewidth(0.8)
            #ax.spines['top'].set_linestyle(':')
            ax.spines['right'].set_linewidth(0.8)
            #ax.spines['right'].set_linestyle(':')
            #plt.gca().spines['bottom'].set_color('gray')
            #plt.gca().spines['left'].set_color('gray')
            #plt.grid(True,linestyle='-', linewidth=0.01)
            #plt.gca().legend(ncol=4)
            #plt.gca().legend(prop={'family': 'monospace'})

def plot_io_cost(posix_df, mpiio_df, stdio_df, runtime, nprocs):

    buf = io.BytesIO()
    fig, ax = plt.subplots()

    labels = []
    r_time = []
    w_time = []
    m_time = []
    o_time = []

    if posix_df:
        s = posix_df['fcounters'].sum(axis=0)
        
        labels.append('POSIX')
        r_time.append( (float(s['POSIX_F_READ_TIME']) / float(runtime * nprocs)) * 100.0 )
        w_time.append( (float(s['POSIX_F_WRITE_TIME']) / float(runtime * nprocs)) * 100.0 )
        m_time.append( (float(s['POSIX_F_META_TIME']) / float(runtime * nprocs)) * 100.0 )
        o_time.append( (float(runtime * nprocs - s['POSIX_F_READ_TIME'] - s['POSIX_F_WRITE_TIME'] - s['POSIX_F_META_TIME']) / float(runtime * nprocs)) * 100.0 )

    if mpiio_df:
        s = mpiio_df['fcounters'].sum(axis=0)
        
        labels.append('MPI-IO')
        r_time.append( (float(s['MPIIO_F_READ_TIME']) / float(runtime * nprocs)) * 100.0 )
        w_time.append( (float(s['MPIIO_F_WRITE_TIME']) / float(runtime * nprocs)) * 100.0 )
        m_time.append( (float(s['MPIIO_F_META_TIME']) / float(runtime * nprocs)) * 100.0 )
        o_time.append( (float(runtime * nprocs - s['MPIIO_F_READ_TIME'] - s['MPIIO_F_WRITE_TIME'] - s['MPIIO_F_META_TIME']) / float(runtime * nprocs)) * 100.0 )

    if stdio_df:
        s = stdio_df['fcounters'].sum(axis=0)
        
        labels.append('STDIO')
        r_time.append( (float(s['STDIO_F_READ_TIME']) / float(runtime * nprocs)) * 100.0 )
        w_time.append( (float(s['STDIO_F_WRITE_TIME']) / float(runtime * nprocs)) * 100.0 )
        m_time.append( (float(s['STDIO_F_META_TIME']) / float(runtime * nprocs)) * 100.0 )
        o_time.append( (float(runtime * nprocs - s['STDIO_F_READ_TIME'] - s['STDIO_F_WRITE_TIME'] - s['STDIO_F_META_TIME']) / float(runtime * nprocs)) * 100.0 )


    ax.bar(labels, r_time, label='Read', color='purple')
    ax.bar(labels, w_time, label='Write', color='green', bottom=r_time)
    ax.bar(labels, m_time, label='Metadata', color='blue', bottom=[a+b for a,b in zip(r_time,w_time)])
    ax.bar(labels, o_time, label='Other (incluing application compute)', color='orange', bottom=[a+b+c for a,b,c in zip (r_time,w_time,m_time)])


    ax.set_ylabel("Percentage of runtime")
    ax.set_title("Average I/O cost per process")
    plt.legend(loc="upper right", ncol=3, bbox_to_anchor=(0.8,1.0))
    #plt.legend(loc='upper center', bbox_to_anchor=(0., 1.05), ncol=3)
    #ax.legend(loc='best', bbox_to_anchor=(0.5, 0., 0.5, 0.5), ncol=3)
    hide_spines()
    plt.savefig(buf, format='png')
    buf.seek(0)
    encoded = base64.b64encode(buf.read())
    return encoded

def plot_op_count(posix_df, mpiio_df, stdio_df):

    buf = io.BytesIO()
    fig, ax = plt.subplots()

    labels = ['Read', 'Write', 'Open', 'Stat', 'Seek', 'Mmap', 'Fsync']
    x = numpy.arange(len(labels))
    bwidth = 0.25

    if posix_df:
        s = posix_df['counters'].sum(axis=0)
        vals = [s['POSIX_READS'], s['POSIX_WRITES'], s['POSIX_OPENS'], s['POSIX_STATS'], s['POSIX_SEEKS'], s['POSIX_MMAPS'], s['POSIX_FSYNCS']+s['POSIX_FDSYNCS']]
        ax.bar(x - 3*bwidth/2, vals, bwidth, label='POSIX')

    if mpiio_df:
        s = mpiio_df['counters'].sum(axis=0)
        vals = [s['MPIIO_INDEP_READS'], s['MPIIO_INDEP_WRITES'], s['MPIIO_INDEP_OPENS'], 0, 0, 0, s['MPIIO_SYNCS']]
        ax.bar(x - bwidth/2, vals, bwidth, label='MPI-IO Indep')
        vals = [s['MPIIO_COLL_READS'], s['MPIIO_COLL_WRITES'], s['MPIIO_COLL_OPENS'], 0, 0, 0, s['MPIIO_SYNCS']]
        ax.bar(x + bwidth/2, vals, bwidth, label='MPI-IO Coll')

    if stdio_df:
        s = stdio_df['counters'].sum(axis=0)
        vals = [s['STDIO_READS'], s['STDIO_WRITES'], s['STDIO_OPENS'], 0, s['STDIO_SEEKS'], 0, s['STDIO_FLUSHES']]
        ax.bar(x + 3*bwidth/2, vals, bwidth, label='STDIO')

    ax.set_ylabel("Ops (Total, All Processes)")
    ax.set_title("I/O Operations Counts")
    ax.set_xticks(x)
    ax.set_xticklabels(labels)
    ax.legend(loc="upper right")
    hide_spines()
    plt.savefig(buf, format='png')
    buf.seek(0)
    encoded = base64.b64encode(buf.read())
    return encoded

def data_transfer_filesystem(report, posix_df, stdio_df):

    import collections
    fs_data = collections.defaultdict(lambda: {'read':0,'write':0,'read_rt':0.,'write_rt':0.})
    total_rd_bytes = 0
    total_wr_bytes = 0

    if posix_df:
        posix_df['counters'].loc[:,'mount'] = 'Unknown'
        posix_df['counters'].loc[:,'mtype'] = 'Unknown'
        for index, row in posix_df['counters'].iterrows():
            total_rd_bytes += row['POSIX_BYTES_READ']
            total_wr_bytes += row['POSIX_BYTES_WRITTEN']
            for m in report.mounts:
                if report.name_records[row['id']].startswith(m[0]):
                    posix_df['counters'].at[index, 'mount'] = m[0]
                    posix_df['counters'].at[index, 'mtype'] = m[1]
                    fs_data[m[0]]['read'] += row['POSIX_BYTES_READ']
                    fs_data[m[0]]['write'] += row['POSIX_BYTES_WRITTEN']
                    break
    if stdio_df:
        stdio_df['counters'].loc[:,'mount'] = 'Unknown'
        stdio_df['counters'].loc[:,'mtype'] = 'Unknown'
        for index, row in stdio_df['counters'].iterrows():
            total_rd_bytes += row['STDIO_BYTES_READ']
            total_wr_bytes += row['STDIO_BYTES_WRITTEN']
            for m in report.mounts:
                if report.name_records[row['id']].startswith(m[0]):
                    stdio_df['counters'].at[index, 'mount'] = m[0]
                    stdio_df['counters'].at[index, 'mtype'] = m[1]
                    fs_data[m[0]]['read'] += row['STDIO_BYTES_READ']
                    fs_data[m[0]]['write'] += row['STDIO_BYTES_WRITTEN']
                    break

    for fs in fs_data:
        fs_data[fs]['read_rt'] = fs_data[fs]['read'] / total_rd_bytes
        fs_data[fs]['write_rt'] = fs_data[fs]['write'] / total_wr_bytes

    return fs_data

def apmpi_process(apmpi_dict):
    
    header_rec = apmpi_dict[0]
    sync_flag = header_rec["sync_flag"]
    #print(
    #    "APMPI Variance in total mpi time: ", header_rec["variance_total_mpitime"], "\n"
    #)
    #if sync_flag:
    #    print(
    #        "APMPI Variance in total mpi sync time: ",
    #        header_rec["variance_total_mpisynctime"],
    #    )
    import time
    start = time.process_time()
    df_apmpi = pd.DataFrame()
    list_mpiop = []
    list_rank = []
    for rec in apmpi_dict[1:]:  # skip the first record which is header record
        mpi_nonzero_callcount = []
        for k, v in rec["all_counters"].items():
            if k.endswith("_CALL_COUNT") and v > 0:
                mpi_nonzero_callcount.append(k[: -(len("CALL_COUNT"))])

        df_rank = pd.DataFrame()
        for mpiop in mpi_nonzero_callcount:
            ncall = mpiop
            ncount = mpiop + "CALL_COUNT"
            nsize = mpiop + "TOTAL_BYTES"
            h0 = mpiop + "MSG_SIZE_AGG_0_256"
            h1 = mpiop + "MSG_SIZE_AGG_256_1K"
            h2 = mpiop + "MSG_SIZE_AGG_1K_8K"
            h3 = mpiop + "MSG_SIZE_AGG_8K_256K"
            h4 = mpiop + "MSG_SIZE_AGG_256K_1M"
            h5 = mpiop + "MSG_SIZE_AGG_1M_PLUS"
            ntime = mpiop + "TOTAL_TIME"
            mintime = mpiop + "MIN_TIME"
            maxtime = mpiop + "MAX_TIME"
            if sync_flag:
                totalsync = mpiop + "TOTAL_SYNC_TIME"

            mpiopstat = {}
            mpiopstat["Rank"] = rec["rank"]
            mpiopstat["Node_ID"] = rec["node_name"]
            mpiopstat["Call"] = ncall[:-1]
            mpiopstat["Total_Time"] = rec["all_counters"][ntime]
            mpiopstat["Count"] = rec["all_counters"][ncount]
            mpiopstat["Total_Bytes"] = rec["all_counters"].get(nsize, None)
            mpiopstat["[0-256B]"] = rec["all_counters"].get(h0, None)
            mpiopstat["[256-1KB]"] = rec["all_counters"].get(h1, None)
            mpiopstat["[1K-8KB]"] = rec["all_counters"].get(h2, None)
            mpiopstat["[8K-256KB]"] = rec["all_counters"].get(h3, None)
            mpiopstat["[256K-1MB]"] = rec["all_counters"].get(h4, None)
            mpiopstat["[1MB+]"] = rec["all_counters"].get(h5, None)
            mpiopstat["Min_Time"] = rec["all_counters"][mintime]
            mpiopstat["Max_Time"] = rec["all_counters"][maxtime]
            if sync_flag and (totalsync in rec["all_counters"]):
                mpiopstat["Total_SYNC_Time"] = rec["all_counters"][totalsync]

            list_mpiop.append(mpiopstat)

        rankstat = {}
        rankstat["Rank"] = rec["rank"]
        rankstat["Node_ID"] = rec["node_name"]
        rankstat["Call"] = "Total_MPI_time"
        rankstat["Total_Time"] = rec["all_counters"]["MPI_TOTAL_COMM_TIME"]
        list_rank.append(rankstat)
    df_rank = pd.DataFrame(list_rank)
    avg_total_time = df_rank["Total_Time"].mean()
    max_total_time = df_rank["Total_Time"].max()
    min_total_time = df_rank["Total_Time"].min()
    max_rank = df_rank.loc[df_rank["Total_Time"].idxmax()]["Rank"]
    min_rank = df_rank.loc[df_rank["Total_Time"].idxmin()]["Rank"]
    # assumption: row index and rank id are same in df_rank 
    # .. need to check if that is an incorrect assumption
    mean_rank = (
        (df_rank["Total_Time"] - df_rank["Total_Time"].mean()).abs().argsort()[:1][0]
    )
    pd.set_option("display.max_rows", None, "display.max_columns", None)
    print(f'Phase1: {time.process_time() - start}')

    import time
    start = time.process_time()
    list_combined = list_mpiop + list_rank
    df_apmpi = pd.DataFrame(list_combined)
    df_apmpi = df_apmpi.sort_values(by=["Rank", "Total_Time"], ascending=[True, False])
    df_call = df_apmpi[['Rank', 'Call', 'Total_Time', 'Total_Bytes', 'Count']]
    df_call_nototal = df_call[df_call.Call != 'Total_MPI_time']

    df_apmpi_nototal = df_apmpi[df_apmpi.Call != 'Total_MPI_time']
    df_apmpi_nototal = df_apmpi_nototal[df_apmpi_nototal.Total_Bytes > 0]
    df_call_nonzero_bytes = df_call_nototal[df_call_nototal.Total_Bytes > 0]
    #print(df_call_nonzero_bytes)
    df_call_nonzero_bytes['bytes_per_op']=(df_call_nonzero_bytes['Total_Bytes']/df_call_nonzero_bytes['Count'])
    #df_call_time = df_call.pivot(index='Rank', columns='Call')['Total_Time'].reset_index()
    #df_call_nonzero_bytes.assign(bytes_per_op = df_call_nonzero_bytes.Total_Bytes/df_call_nonzero_bytes.Count)
    print(f'Phase2: {time.process_time() - start}')

    import time
    start = time.process_time()
    encoded = []
    buf = io.BytesIO()
    fig, ax = plt.subplots()
    # Using pandas methods and slicing to determine the order by decreasing median
    my_order=df_call_nototal.groupby(by=["Call"])["Total_Time"].median().sort_values(ascending=False).index
    apmpi_totaltime = sns.violinplot(x="Call", y="Total_Time", ax=ax, data=df_call_nototal, bw=0.2, scale = 'count', scale_hue=True, order=my_order)
    #apmpi_totaltime = sns.boxplot(x="Call", y="Total_Time", ax=ax, data=df_call_nototal, order=my_order)
    apmpi_totaltime.set_xlabel('MPI operation times across Ranks')
    apmpi_totaltime.set_ylabel('Time (seconds)')
    plt.xticks(rotation=75)
    hide_spines()
    plt.savefig(buf, format='png', bbox_inches='tight')
    buf.seek(0)
    encoded.append(base64.b64encode(buf.read()))
    print(f'Phase3: {time.process_time() - start}')
    
    import time
    start = time.process_time()
    buf = io.BytesIO()
    fig, ax = plt.subplots()
    print(f'Phase4a: {time.process_time() - start}')
    import time
    start = time.process_time()
    df_times = df_apmpi[['Rank', 'Call', 'Total_Time']]
    print(f'Phase4b: {time.process_time() - start}')
    import time
    start = time.process_time()
    df_times_unmelt = df_times.pivot_table(index='Rank', columns = 'Call')['Total_Time'].reset_index()
    print(f'Phase4c: {time.process_time() - start}')
    import time
    start = time.process_time()
    df_times_unmelt.columns.name = None
    df_times_unmelt = df_times_unmelt.sort_values(by=['Total_MPI_time'], ascending=False)
    print(f'Phase4d: {time.process_time() - start}')
    import time
    start = time.process_time()
    del df_times_unmelt['Total_MPI_time']
    df_times_unmelt.set_index('Rank')
    del df_times_unmelt['Rank']
    print(f'Phase4e: {time.process_time() - start}')
    import time
    start = time.process_time()
    df_times_unmelt = df_times_unmelt.sort_values(by=1, axis=1, ascending=False)
    print(f'Phase4f: {time.process_time() - start}')
    import time
    start = time.process_time()
    #df_times_unmelt.plot(kind="bar",stacked=True, align='center', ax=ax, colormap='tab10')#, width=dwidth)
    ## to melt again if we need to use plotnine stacked bar
    df_times_unmelt["Rank"]=df_times_unmelt.index
    df_times_melt = df_times_unmelt.melt(id_vars="Rank", value_vars=df_times_unmelt.columns.tolist()[:-1], var_name="MPI_OP", value_name ="time")
    df_times_melt['MPI_OP']=pd.Categorical(df_times_melt['MPI_OP'], categories=df_times_unmelt.columns.tolist()[:-1], ordered=True)
    df_times_melt['Rank']=pd.Categorical(df_times_melt['Rank'], categories=df_times_unmelt['Rank'], ordered=True)
    p = (ggplot(df_times_melt, aes(x='Rank', y='time', fill = 'MPI_OP', label='MPI_OP'))
 + geom_bar(stat='identity', position='stack')
#+ geom_tile()
 + ggtitle('MPI OP time distribution')
 + xlab("MPI op times on all the ranks (sorted by total MPI time)")
 + ylab("Time(seconds)")
 + scale_fill_manual(values=cbbPalette)
 + scale_x_discrete(labels = ""))
# + theme(axis.text.x = element_blank()))
    print(f'Phase4g: {time.process_time() - start}')
    import time
    start = time.process_time()
    #ax.set_xticks([])
    #ax.set_xticks([], minor=True)
    #ax.set_xticklabels([])
    #ax.set_ylabel("Time(seconds)")
    #ax.set_xlabel("MPI op times on all the ranks (sorted by total MPI time)", labelpad=20)
    #plt.legend(loc="lower left",bbox_to_anchor=(0.8,1.0))
    #ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
    #ax.legend(loc='upper left')
    #hide_spines()
    print(f'Phase4h: {time.process_time() - start}')
    import time
    start = time.process_time()
    #plt.savefig(buf, format='png', bbox_inches='tight')
    #cm = plt.get_cmap('viridis')
    #plt.imsave(buf, fig, format='png')
    #fig.canvas.print_png(buf)
    #buf.close()
    #p.save(buf, format='png',  verbose = False, height=5, width=6.5, dpi=50)
    #p.save(buf, format='png',  verbose = False, height=5, width=6.5, dpi=50)
    p.save(buf, format='png', verbose = False, dpi=80) 
    print(f'Phase4i: {time.process_time() - start}')
    import time
    start = time.process_time()
    buf.seek(0)
    encoded.append(base64.b64encode(buf.read()))
    print(f'Phase4j: {time.process_time() - start}')

    import time
    start = time.process_time()
    buf = io.BytesIO()
    fig, ax = plt.subplots()
    my_order=df_call_nonzero_bytes.groupby(by=["Call"])["bytes_per_op"].median().sort_values(ascending=False).index
    #apmpi_totalbytes = sns.violinplot(x="Call", y="bytes_per_op", ax=ax, data=df_call_nonzero_bytes, bw=0.2, scale = 'count', scale_hue=True, order=my_order)
    apmpi_totalbytes = sns.barplot(x="Call", y="bytes_per_op", ax=ax, data=df_call_nonzero_bytes, estimator=numpy.median, ci='sd', capsize=.2, color='lightblue', order=my_order)
    #apmpi_totalbytes = sns.boxplot(x="Call", y="Total_Bytes", ax=ax, data=df_call_nototal, order=my_order)
    apmpi_totalbytes.set(yscale="log")
    apmpi_totalbytes.set_xlabel('MPI operation total bytes across Ranks')
    apmpi_totaltime.set_ylabel('Accumulative Bytes per MPI OP')
    plt.xticks(rotation=75)
    hide_spines()
    plt.savefig(buf, format='png', bbox_inches='tight')
    buf.seek(0)
    encoded.append(base64.b64encode(buf.read()))
    print(f'Phase5: {time.process_time() - start}')
    
    #buf = io.BytesIO()
    #fig, ax = plt.subplots()
    #mpiops = list(df_call.Call.unique())
    #sns_dist = sns.kdeplot(data=df_call, x="Total_Time", ax=ax, cut=0, hue="Call")
    #sns_dist.set_xlabel('ECDF', size=7)
    #sns_dist.set_xlabel('Time across the ranks')
    #hide_spines()
    #plt.savefig(buf, format='png', bbox_inches='tight')
    #buf.seek(0)
    #encoded.append(base64.b64encode(buf.read()))
    
    import time
    start = time.process_time()
    buf = io.BytesIO()
    fig, ax = plt.subplots()
    df_apmpi_nototal_zerorank = df_apmpi_nototal[df_apmpi_nototal.Rank == 0][['Call', '[0-256B]', '[256-1KB]', '[1K-8KB]', '[8K-256KB]', '[256K-1MB]', '[1MB+]', 'Count']]
    #apmpi_catplot = sns.catplot(data=df_apmpi_nototal, kind="bar", x="Call", y="")
    df_apmpi_nototal_zerorank['[0-256B]'] = (df_apmpi_nototal_zerorank['[0-256B]']/df_apmpi_nototal_zerorank['Count'])*100
    df_apmpi_nototal_zerorank['[256-1KB]'] = (df_apmpi_nototal_zerorank['[256-1KB]']/df_apmpi_nototal_zerorank['Count'])*100
    df_apmpi_nototal_zerorank['[1K-8KB]'] = (df_apmpi_nototal_zerorank['[1K-8KB]']/df_apmpi_nototal_zerorank['Count'])*100
    df_apmpi_nototal_zerorank['[8K-256KB]'] = (df_apmpi_nototal_zerorank['[8K-256KB]']/df_apmpi_nototal_zerorank['Count'])*100
    df_apmpi_nototal_zerorank['[256K-1MB]'] = (df_apmpi_nototal_zerorank['[256K-1MB]']/df_apmpi_nototal_zerorank['Count'])*100
    df_apmpi_nototal_zerorank['[1MB+]'] = (df_apmpi_nototal_zerorank['[1MB+]']/df_apmpi_nototal_zerorank['Count'])*100
    #print(df_apmpi_nototal_zerorank)
    #del df_apmpi_nototal_zerorank['Count']
    #dodge_text = position_dodge(width=0.9) 
    df_apmpi_nototal_zerorank_melt = df_apmpi_nototal_zerorank.melt(id_vars=["Call", "Count"],value_vars=['[0-256B]', '[256-1KB]', '[1K-8KB]', '[8K-256KB]', '[256K-1MB]', '[1MB+]'], var_name="Message_Type", value_name="percentage")
    #print(df_apmpi_nototal_zerorank_melt)
    df_apmpi_nototal_zerorank_melt = df_apmpi_nototal_zerorank_melt.astype({"Count": int})
    msg_order = ['[0-256B]', '[256-1KB]', '[1K-8KB]', '[8K-256KB]', '[256K-1MB]', '[1MB+]']
    df_apmpi_nototal_zerorank_melt['Message_Type']=pd.Categorical(df_apmpi_nototal_zerorank_melt['Message_Type'], categories=msg_order, ordered=True)
    p = (ggplot(df_apmpi_nototal_zerorank_melt, aes(x='Call', y='percentage', fill = 'Message_Type', label='Message_Type'))
 + geom_bar(stat='identity', position="stack")
## TODO: make sure one label per stacked bar chart is placed on the top.
 + ggtitle('Message size histogram for Rank 0')
 + xlab("MPI Operation")
 + ylab("Percentage of messages of different size ranges")
# + theme(plot.title=element_text(family="Gill Sans"), text=element_text(family="Gill Sans")
 + geom_text(aes(x='Call', y=105, label='Count'), color='purple', size=8, angle=0, va='top') 
 + scale_fill_manual(values=cbbPalette))
# + scale_fill_manual(values=plotnine.scales.scale_color_cmap('tab10').palette()))
    #sns_plot = sns.scatterplot(x="Rank", y="Total_Time", ax=ax, data=df_apmpi, s=3)
    #sns_plot.set_xticklabels(sns_plot.get_xticks())
    #sns_plot.set_yticklabels(sns_plot.get_yticks())
    #sns_plot.set_xlabel('Rank')
    #sns_plot.set_ylabel('Time (seconds)')
    #sns.despine();
    #hide_spines()
    p.save(buf, verbose = False)
    #plt.savefig(buf, format='png', bbox_inches='tight')
    buf.seek(0)
    encoded.append(base64.b64encode(buf.read()))
    print(f'Phase6: {time.process_time() - start}')

    import time
    start = time.process_time()
    ## APMPI tables with stats from 3 ranks (rank with MAX mpi time, min time and average MPI time)
    #df_apmpi = df_apmpi.astype({"Count": 'Int64', "Total_Bytes": 'Int64', '[0-256B]':'Int64', '[256-1KB]':'Int64', '[1K-8KB]':'Int64', '[8K-256KB]':'Int64', '[256K-1MB]':'Int64', '[1MB+]':'Int64'})
 #   df_apmpi[["Count", "Total_Bytes", "[0-256B]", "[256-1KB]", "[1K-8KB]", "[8K-256KB]", "[256K-1MB]", "[1MB+]"]] = df_apmpi[["Count", "Total_Bytes", "[0-256B]", "[256-1KB]", "[1K-8KB]", "[8K-256KB]", "[256K-1MB]", "[1MB+]"]].apply(pd.to_numeric)
    df_apmpi = df_apmpi.round({'Total_Time':6, 'Count':6, 'Total_Bytes':6, '[0-256B]':6, '[256-1KB]':6, '[1K-8KB]':6, '[8K-256KB]':6, '[256K-1MB]':6, '[1MB+]':6, 'Min_Time':6, 'Max_Time':6})
    df_max_rank = df_apmpi.loc[df_apmpi["Rank"] == max_rank]
    df_min_rank = df_apmpi.loc[df_apmpi["Rank"] == min_rank]
    df_mean_rank = df_apmpi.loc[df_apmpi["Rank"] == mean_rank]
    df_zero_rank = df_apmpi.loc[df_apmpi["Rank"] == 0]
    #encoded.append(df_max_rank.round({'Total_Time':6, 'Count':6, 'Total_Bytes':6, '[0-256B]':6, '[256-1KB]':6, '[1K-8KB]':6, '[8K-256KB]':6, '[256K-1MB]':6, '[1MB+]':6, 'Min_Time':6, 'Max_Time':6}))
    #encoded.append(df_min_rank.round({'Total_Time':6, 'Count':6, 'Total_Bytes':6, '[0-256B]':6, '[256-1KB]':6, '[1K-8KB]':6, '[8K-256KB]':6, '[256K-1MB]':6, '[1MB+]':6, 'Min_Time':6, 'Max_Time':6}))
    #encoded.append(df_mean_rank.round({'Total_Time':6, 'Count':6, 'Total_Bytes':6, '[0-256B]':6, '[256-1KB]':6, '[1K-8KB]':6, '[8K-256KB]':6, '[256K-1MB]':6, '[1MB+]':6, 'Min_Time':6, 'Max_Time':6}))
    #encoded.append(df_zero_rank.round({'Total_Time':6, 'Count':6, 'Total_Bytes':6, '[0-256B]':6, '[256-1KB]':6, '[1K-8KB]':6, '[8K-256KB]':6, '[256K-1MB]':6, '[1MB+]':6, 'Min_Time':6, 'Max_Time':6}))
    encoded.append(df_max_rank)
    encoded.append(df_min_rank)
    encoded.append(df_mean_rank)
    encoded.append(df_zero_rank)
    print(f'Phase7: {time.process_time() - start}')
    return encoded
  
def main(args=None):

    if args is None:
        parser = argparse.ArgumentParser(description='')
        setup_parser(parser)
        args = parser.parse_args()

    if args.debug:
        print(args)
    
    init_plotting()
    variables = {}
    report = darshan.DarshanReport(args.input, read_all=True)
    #report.info()

    #
    # Setup template header variabels
    #
    variables['exe'] = report.metadata['exe']
    variables['date'] = datetime.datetime.fromtimestamp(report.metadata['job']['start_time'], pytz.utc)
    variables['jid'] = report.metadata['job']['jobid']
    variables['uid'] = report.metadata['job']['uid']
    variables['nprocs'] = report.metadata['job']['nprocs']
    etime = int(report.metadata['job']['end_time'])
    stime = int(report.metadata['job']['start_time'])
    if etime > stime:
        variables['runtime'] = etime - stime + 1
    else:
        variables['runtime'] = 0

    if 'POSIX' in report.modules:
        posix_df = report.records['POSIX'].to_df()
    else:
        posix_df = None

    if 'MPI-IO' in report.modules:
        mpiio_df = report.records['MPI-IO'].to_df()
    else:
        mpiio_df = None
    
    if 'STDIO' in report.modules:
        stdio_df = report.records['STDIO'].to_df()
    else:
        stdio_df = None
    
    if 'APXC' in report.modules:
        apxc_dict = report.records['APXC'].to_dict()
    else:
        apxc_dict = None

    if 'APMPI' in report.modules:
        apmpi_dict = report.records['APMPI'].to_dict()
    else:
        apmpi_dict = None

    #
    # Plot I/O cost
    #
    variables['plot_io_cost'] = plot_io_cost(posix_df, mpiio_df, stdio_df, int(variables['runtime']), int(variables['nprocs'])).decode('utf-8')

    #
    # Plot I/O counts
    #
    variables['plot_op_count'] = plot_op_count(posix_df, mpiio_df, stdio_df).decode('utf-8')

    variables['fs_data'] = data_transfer_filesystem(report, posix_df, stdio_df)
    apmpi_encoded = apmpi_process(apmpi_dict)
    if apmpi_encoded:
       variables['apmpi_call_times_variance'] = apmpi_encoded[0].decode('utf-8')
       variables['apmpi_call_time_distribution'] = apmpi_encoded[1].decode('utf-8')
       variables['apmpi_call_bytes_per_op'] = apmpi_encoded[2].decode('utf-8')
       variables['apmpi_call_rank0_msg_dist'] = apmpi_encoded[3].decode('utf-8')

       variables['apmpi_max_rank'] = []
       for row in apmpi_encoded[4].iterrows():
           variables['apmpi_max_rank'].append(row[1].values)
       variables['apmpi_min_rank'] = []
       for row in apmpi_encoded[5].iterrows():
           variables['apmpi_min_rank'].append(row[1].values)
       variables['apmpi_mean_rank'] = []
       for row in apmpi_encoded[6].iterrows():
           variables['apmpi_mean_rank'].append(row[1].values)
       variables['apmpi_zero_rank'] = []
       for row in apmpi_encoded[7].iterrows():
           variables['apmpi_zero_rank'].append(row[1].values)
    else:
       variables['apmpi_call_time'] = None
       variables['apmpi_rank_totaltime'] = None
       variables['apmpi_max_rank'] = None
       variables['apmpi_min_rank'] = None
       variables['apmpi_mean_rank'] = None
       variables['apmpi_zero_rank'] = None
    template_path = pkg_resources.path(darshan.templates, '')
    with template_path as path:
       loader = genshi.template.TemplateLoader(str(path))
       template = loader.load('summary.html')

       stream = template.generate(title='Darshan Job Summary', var=variables)
       with open(args.output, 'w') as f:
           f.write(stream.render('html'))
           f.close()
    return

if __name__ == "__main__":
    main()
