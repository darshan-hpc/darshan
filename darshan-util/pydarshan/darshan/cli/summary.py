import argparse
import base64
import datetime
import genshi.template
import importlib.resources as pkg_resources
import io
import matplotlib
import matplotlib.pyplot as pyplot
import numpy
import pandas
import pytz
import sys
import pprint
import pandas as pd
import seaborn as sns

import darshan
import darshan.templates

def setup_parser(parser):
    # setup arguments
    parser.add_argument('--input', help='darshan log file', nargs='?')
    parser.add_argument('--output', action='store', help='output file name')
    parser.add_argument('--verbose', help='', action='store_true')
    parser.add_argument('--debug', help='', action='store_true')

def plot_io_cost(posix_df, mpiio_df, stdio_df, runtime, nprocs):

    buf = io.BytesIO()
    fig, ax = pyplot.subplots()

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
    ax.legend(loc="upper right")
    pyplot.savefig(buf, format='png')
    buf.seek(0)
    encoded = base64.b64encode(buf.read())
    return encoded

def plot_op_count(posix_df, mpiio_df, stdio_df):

    buf = io.BytesIO()
    fig, ax = pyplot.subplots()

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
    pyplot.savefig(buf, format='png')
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
            mpiopstat["[1K-8KB]"] = rec["all_counters"].get(h2, None)
            mpiopstat["[8K-256KB]"] = rec["all_counters"].get(h3, None)
            mpiopstat["256K-1MB"] = rec["all_counters"].get(h4, None)
            mpiopstat["[>1MB]"] = rec["all_counters"].get(h5, None)
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

    list_combined = list_mpiop + list_rank
    df_apmpi = pd.DataFrame(list_combined)
    df_apmpi = df_apmpi.sort_values(by=["Rank", "Total_Time"], ascending=[True, False])
    df_call = df_apmpi[['Call', 'Total_Time']]
   
    encoded = []
    buf = io.BytesIO()
    fig, ax = pyplot.subplots()

    sns_violin = sns.violinplot(x="Call", y="Total_Time", ax=ax, data=df_call)
    sns_violin.set_xticklabels(sns_violin.get_xticklabels(), rotation=60, size=6.5)
    sns_violin.set_yticklabels(sns_violin.get_yticks(), rotation=0, size=6.5)
    sns_violin.set_xlabel('')
    sns_violin.set_ylabel('Time (seconds)', size=7)
    #sns.despine();
    pyplot.savefig(buf, format='png', bbox_inches='tight')
    buf.seek(0)
    encoded.append(base64.b64encode(buf.read()))

    buf = io.BytesIO()
    fig, ax = pyplot.subplots()
    sns_plot = sns.scatterplot(x="Rank", y="Total_Time", ax=ax, data=df_apmpi, s=3)
    sns_plot.set_xticklabels(sns_plot.get_xticklabels(), rotation=0, size=6.5)
    sns_plot.set_yticklabels(sns_plot.get_yticks(), rotation=0, size=6.5)
    sns_plot.set_xlabel('Rank', size=8)
    sns_plot.set_ylabel('Time (seconds)', size=8)
    #sns.despine();
    pyplot.savefig(buf, format='png', bbox_inches='tight')
    buf.seek(0)
    encoded.append(base64.b64encode(buf.read()))
    df_max_rank = df_apmpi.loc[df_apmpi["Rank"] == max_rank]
    df_min_rank = df_apmpi.loc[df_apmpi["Rank"] == min_rank]
    df_mean_rank = df_apmpi.loc[df_apmpi["Rank"] == mean_rank]
    encoded.append(df_max_rank.round({'Total_Time':6, 'Count':6, 'Total_Bytes':6, '[0-256B]':6, '[1K-8KB]':6, '[8K-256KB]':6, '[256K-1MB]':6, '[1MB+]':6, 'Min_Time':6, 'Max_Time':6}))
    encoded.append(df_min_rank.round({'Total_Time':6, 'Count':6, 'Total_Bytes':6, '[0-256B]':6, '[1K-8KB]':6, '[8K-256KB]':6, '[256K-1MB]':6, '[1MB+]':6, 'Min_Time':6, 'Max_Time':6}))
    encoded.append(df_mean_rank.round({'Total_Time':6, 'Count':6, 'Total_Bytes':6, '[0-256B]':6, '[1K-8KB]':6, '[8K-256KB]':6, '[256K-1MB]':6, '[1MB+]':6, 'Min_Time':6, 'Max_Time':6}))
    return encoded
  
def main(args=None):

    if args is None:
        parser = argparse.ArgumentParser(description='')
        setup_parser(parser)
        args = parser.parse_args()

    if args.debug:
        print(args)

    variables = {}
    report = darshan.DarshanReport(args.input, read_all=True)
    report.info()

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
       variables['apmpi_call_time'] = apmpi_encoded[0].decode('utf-8')
       variables['apmpi_rank_totaltime'] = apmpi_encoded[1].decode('utf-8')

       variables['apmpi_max_rank'] = []
       for row in apmpi_encoded[2].iterrows():
           variables['apmpi_max_rank'].append(row[1].values)
       variables['apmpi_min_rank'] = []
       for row in apmpi_encoded[3].iterrows():
           variables['apmpi_min_rank'].append(row[1].values)
       variables['apmpi_mean_rank'] = []
       for row in apmpi_encoded[4].iterrows():
           variables['apmpi_mean_rank'].append(row[1].values)
    else:
       variables['apmpi_call_time'] = None
       variables['apmpi_rank_totaltime'] = None
       variables['apmpi_max_rank'] = None
       variables['apmpi_min_rank'] = None
       variables['apmpi_mean_rank'] = None
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
