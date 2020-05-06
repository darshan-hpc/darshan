from darshan.report import *

def create_time_summary(self, mode="append"):
    """
    TODO: port to new object report

    """

    raise("Not implemented.")


    # Original, Target:
    ## <type>, <app time>, <read>, <write>, <meta>
    #POSIX, 98.837925, 0.150075, 0.5991, 0.4129
    #MPI-IO, 97.293875, 0.051575, 0.126525, 2.528025
    #STDIO, 99.261425, 0, 0.738575, 0

    ctx = {}

    # convienience links
    summary = logdata['summary']
    time_summary = logdata['time-summary.dat']

    runtime = float(logdata['runtime'])
    nprocs = int(logdata['nprocs'])

    for layer in ['POSIX', 'MPIIO', 'STDIO']:
        if (layer + '_OPENS') in summary or (layer + '_INDEP_OPENS') in summary :

            entry = {
                'type': layer, 'app_time': None, 'read': None, 'write': None, 'meta': None
                }

            io_time = 0.0
            for op in ['READ', 'WRITE', 'META']:
                val = float(summary[layer + '_F_' + op + '_TIME'])
                io_time += val
                entry[op.lower()] = (val / (runtime * nprocs)) * 100

            entry['app_time'] = ((runtime * nprocs - io_time) / (runtime * nprocs)) * 100
            time_summary[layer] = entry




    # overwrite existing summary entry
    if mode == "append":
        self.summary['time_summary'] = ctx

    return ctx
