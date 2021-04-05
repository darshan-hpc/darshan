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

import darshan
import darshan.templates

def setup_parser(parser):
    # setup arguments
    parser.add_argument('input', help='darshan log file', nargs='?')
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
    
def main(args=None):

    if args is None:
        parser = argparse.ArgumentParser(description='')
        setup_parser(parser)
        args = parser.parse_args()

    if args.debug:
        print(args)

    variables = {}
    report = darshan.DarshanReport(args.input, read_all=True)

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

    #
    # Plot I/O cost
    #
    variables['plot_io_cost'] = plot_io_cost(posix_df, mpiio_df, stdio_df, int(variables['runtime']), int(variables['nprocs'])).decode('utf-8')

    #
    # Plot I/O counts
    #
    variables['plot_op_count'] = plot_op_count(posix_df, mpiio_df, stdio_df).decode('utf-8')

    variables['fs_data'] = data_transfer_filesystem(report, posix_df, stdio_df)

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
