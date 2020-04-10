from darshan.report import *

def agg_ioops(self, mode='append'):
    """
    Compile the I/O operations summary for the current report.

    Args:
        mode (str): Whether to 'append' (default) or to 'return' aggregation. 

    Return:
        None or dict: Depending on mode
    """

    series = [
        {'name': 'POSIX', 'type': 'bar', 'data': [0, 0, 0, 0, 0, 0, 0] }, 
        {'name': 'MPI-IO Indep.', 'type': 'bar', 'data': [0, 0, 0, 0, 0, 0, 0] }, 
        {'name': 'MPI-IO Coll.', 'type': 'bar', 'data': [0, 0, 0, 0, 0, 0, 0] },
        {'name': 'STDIO', 'type': 'bar', 'data': [0, 0, 0, 0, 0, 0, 0] }
    ]


    # convienience
    recs = self.data['records']
    ctx = {}

    # aggragate
    mods = ['MPI-IO', 'POSIX', 'STDIO']
    for mod in mods:

        # check records for module are present
        if mod not in recs:
            continue

        agg = None
        for rec in recs[mod]:
            if agg is not None:
                agg = np.add(agg, rec['counters'])
            else:
                agg = rec['counters']


        # filter fields
        cn = backend.counter_names(mod)
        agg = dict(zip(cn, agg.tolist()))
        

        # append aggregated statistics for module to report
        if mod == 'MPI-IO':
            ctx[mod + ' Indep.'] = agg

            #agg_indep = {
            #    'Read':  agg['MPIIO_'],
            #    'Write': agg['MPIIO_'],
            #    'Open':  agg['MPIIO_'],
            #    'Stat':  agg['MPIIO_'],
            #    'Seek':  agg['MPIIO_'],
            #    'Mmap':  agg['MPIIO_'],
            #    'Fsync': agg['MPIIO_']
            #}

            #ctx[mod + ' Coll.'] = agg
            #agg_coll = {
            #    'Read':  agg['MPIIO_'],
            #    'Write': agg['MPIIO_'],
            #    'Open':  agg['MPIIO_'],
            #    'Stat':  agg['MPIIO_'],
            #    'Seek':  agg['MPIIO_'],
            #    'Mmap':  agg['MPIIO_'],
            #    'Fsync': agg['MPIIO_']
            #}

        else:
            # POSIX and STDIO share most counter names and are handled 
            # together for this reason, except for metadata/sync counter 
            tmp = {
                'Read':  agg[mod + '_READS'],
                'Write': agg[mod + '_WRITES'],
                'Open':  agg[mod + '_OPENS'],
                'Stat':  0,
                'Seek':  agg[mod + '_SEEKS'],
                'Mmap':  0,
                'Fsync': 0
            }

            if mod == 'POSIX':
                tmp['Stat']
                tmp['Stat']
                tmp['Stat']
                pass    

            elif mod == 'STDIO':
                tmp['Stat']
                tmp['Mmap']
                tmp['Fsync']
                pass

            
            ctx[mod] = agg
            ctx[mod + '_simple'] = tmp



    # cleanup and prepare for json serialization?
    tmp = json.dumps(ctx, cls=NumpyEncoder)
    ctx = json.loads(tmp)

    # reset summary target
    if mode == 'append':
        self.data['agg_ioops'] = ctx
    else:
        return ctx


