from darshan.report import *

def agg_ioops(self, mode='append'):
    """
    Compile the I/O operations summary for the current report.

    Args:
        mode (str): Whether to 'append' (default) or to 'return' aggregation. 

    Return:
        None or dict: Depending on mode
    """

    # convienience
    recs = self.records
    ctx = {}

    # aggregate
    mods = ['MPI-IO', 'POSIX', 'STDIO', "H5F", "H5D", "PNETCDF_VAR", "PNETCDF_FILE", "DFS", "DAOS"]
    for mod in mods:

        # check records for module are present
        if mod not in recs or len(recs[mod]) == 0:
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
            agg_indep = {
                'Read':  agg['MPIIO_INDEP_READS'],
                'Write': agg['MPIIO_INDEP_WRITES'],
                'Open':  agg['MPIIO_INDEP_OPENS'],
                'Stat':  0,
                'Seek':  0,
                'Mmap':  0,
                'Fsync': 0
            }

            #ctx[mod + ' Coll.'] = agg
            agg_coll = {
                'Read':  agg['MPIIO_COLL_READS'],
                'Write': agg['MPIIO_COLL_WRITES'],
                'Open':  agg['MPIIO_COLL_OPENS'],
                'Stat':  0,
                'Seek':  0,
                'Mmap':  0,
                'Fsync': agg['MPIIO_SYNCS']
            }

            ctx[mod] = agg
            ctx[mod + '_indep_simple'] = agg_indep
            ctx[mod + '_coll_simple'] = agg_coll

        elif mod == "H5F":
            tmp = {
                'Open':  agg[mod + '_OPENS'],
                'Flush': agg[mod + '_FLUSHES'],
            }
            ctx[mod] = agg
            ctx[mod + '_simple'] = tmp

        elif mod == "H5D":
            tmp = {
                'Read':  agg[mod + '_READS'],
                'Write': agg[mod + '_WRITES'],
                'Open':  agg[mod + '_OPENS'],
                'Flush': agg[mod + '_FLUSHES'],
            }
            ctx[mod] = agg
            ctx[mod + '_simple'] = tmp

        elif mod == "PNETCDF_VAR":
            tmp = {
                'Read':  agg[mod + '_INDEP_READS'],
                'Write': agg[mod + '_INDEP_WRITES'],
                'Open':  agg[mod + '_OPENS'],
                'Coll Read': agg[mod + '_COLL_READS'],
                'Coll Write': agg[mod + '_COLL_WRITES'],
                'NB Read': agg[mod + '_NB_READS'],
                'NB Write': agg[mod + '_NB_WRITES'],
            }
            ctx[mod] = agg
            ctx[mod + '_simple'] = tmp

        elif mod == "PNETCDF_FILE":
            tmp = {
                'Open':  agg[mod + '_OPENS'],
                'Sync':  agg[mod + '_SYNCS'],
                'Ind Wait':  agg[mod + '_INDEP_WAITS'],
                'Coll Wait':  agg[mod + '_COLL_WAITS'],
            }
            ctx[mod] = agg
            ctx[mod + '_simple'] = tmp

        elif mod == "DFS":
            tmp = {
                'Read':  agg[mod + '_READS'],
                'Readx':  agg[mod + '_READXS'],
                'Write': agg[mod + '_WRITES'],
                'Writex': agg[mod + '_WRITEXS'],
                'Open':  agg[mod + '_OPENS'],
                'GlobalOpen':  agg[mod + '_GLOBAL_OPENS'],
                'Lookup':  agg[mod + '_LOOKUPS'],
                'Get Size':  agg[mod + '_GET_SIZES'],
                'Punch':  agg[mod + '_PUNCHES'],
                'Remove':  agg[mod + '_REMOVES'],
                'Stat':  agg[mod + '_STATS'],
            }
            ctx[mod] = agg
            ctx[mod + '_simple'] = tmp

        elif mod == "DAOS":
            tmp = {
                'Obj Fetches':  agg[mod + '_OBJ_FETCHES'],
                'Obj Updates':  agg[mod + '_OBJ_UPDATES'],
                'Obj Opens':  agg[mod + '_OBJ_OPENS'],
                'Obj Punches':  agg[mod + '_OBJ_PUNCHES'],
                'Obj Dkey Punches':  agg[mod + '_OBJ_DKEY_PUNCHES'],
                'Obj Akey Punches':  agg[mod + '_OBJ_AKEY_PUNCHES'],
                'Obj Dkey Lists':  agg[mod + '_OBJ_DKEY_LISTS'],
                'Obj Akey Lists':  agg[mod + '_OBJ_AKEY_LISTS'],
                'Obj Recx Lists':  agg[mod + '_OBJ_RECX_LISTS'],
                'Array Reads':  agg[mod + '_ARRAY_READS'],
                'Array Writes':  agg[mod + '_ARRAY_WRITES'],
                'Array Opens':  agg[mod + '_ARRAY_OPENS'],
                'Array Get Sizes':  agg[mod + '_ARRAY_GET_SIZES'],
                'Array Set Sizes':  agg[mod + '_ARRAY_SET_SIZES'],
                'Array Stats':  agg[mod + '_ARRAY_STATS'],
                'Array Punches':  agg[mod + '_ARRAY_PUNCHES'],
                'Array Destroys':  agg[mod + '_ARRAY_DESTROYS'],
                'KV Gets':  agg[mod + '_KV_PUTS'],
                'KV Puts':  agg[mod + '_KV_GETS'],
                'KV Opens':  agg[mod + '_KV_OPENS'],
                'KV Removes':  agg[mod + '_KV_REMOVES'],
                'KV Lists':  agg[mod + '_KV_LISTS'],
                'KV Destroys':  agg[mod + '_KV_DESTROYS'],
            }
            ctx[mod] = agg
            ctx[mod + '_simple'] = tmp

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
    tmp = json.dumps(ctx, cls=DarshanReportJSONEncoder)
    ctx = json.loads(tmp)

    


    # overwrite existing summary entry
    if mode == 'append':
        self.summary['agg_ioops'] = ctx
    
    return ctx


