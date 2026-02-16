import cffi
import ctypes

import numpy as np
import darshan.backend.cffi_backend

# APMPI structure defs
structdefs = '''
struct darshan_apmpi_perf_record
{
    struct darshan_base_record base_rec;
    uint64_t counters[396];
    double fcounters[222];
    double fsynccounters[16];
    double fglobalcounters[2];
    char   node_name[128];
};
struct darshan_apmpi_header_record
{
    struct darshan_base_record base_rec;  
    int64_t magic;
    uint32_t sync_flag;
    double apmpi_f_variance_total_mpitime;
    double apmpi_f_variance_total_mpisynctime;
};

extern char *apmpi_counter_names[];
extern char *apmpi_f_mpiop_totaltime_counter_names[]; 
extern char *apmpi_f_mpiop_synctime_counter_names[];
extern char *apmpi_f_mpi_global_counter_names[];

'''

def get_apmpi_defs():
  return structdefs


# load header record
def log_get_apmpi_record(log, mod_name, structname, dtype='dict'):
    from darshan.backend.cffi_backend import ffi, libdutil, log_get_modules, counter_names, _structdefs

    modules = log_get_modules(log)

    rec = {}
    buf = ffi.new("void **")
    r = libdutil.darshan_log_get_record(log['handle'], modules[mod_name]['idx'], buf)
    mod_type = _structdefs[mod_name+"-"+structname]

    if r < 1:
        return None

    if mod_type == 'struct darshan_apmpi_header_record **':
        hdr = ffi.cast(mod_type, buf)
        rec['id'] = hdr[0].base_rec.id
        rec['rank'] = hdr[0].base_rec.rank
        rec['magic'] = hdr[0].magic
        rec['sync_flag'] = hdr[0].sync_flag
        rec['variance_total_mpitime'] = hdr[0].apmpi_f_variance_total_mpitime
        rec['variance_total_mpisynctime'] = hdr[0].apmpi_f_variance_total_mpisynctime
    else:
        prf = ffi.cast(mod_type, buf)
        rec['id'] = prf[0].base_rec.id
        rec['rank'] = prf[0].base_rec.rank
        rec['node_name'] = ffi.string(prf[0].node_name).decode("utf-8")
        
        lst = []
        for i in range(0, len(prf[0].counters)):
            lst.append(prf[0].counters[i])
        np_counters = np.array(lst, dtype=np.uint64)
        d_counters = dict(zip(counter_names(mod_name), np_counters))

        lst = []
        for i in range(0, len(prf[0].fcounters)):
            lst.append(prf[0].fcounters[i])
        np_fcounters = np.array(lst, dtype=np.float64)
        d_fcounters = dict(zip(counter_names(mod_name, fcnts=True, special='mpiop_totaltime_'), np_fcounters))

        lst = []
        for i in range(0, len(prf[0].fsynccounters)):
            lst.append(prf[0].fsynccounters[i])
        np_fsynccounters = np.array(lst, dtype=np.float64)
        d_fsynccounters = dict(zip(counter_names(mod_name, fcnts=True, special='mpiop_synctime_'), np_fsynccounters))

        lst = []
        for i in range(0, len(prf[0].fglobalcounters)):
            lst.append(prf[0].fglobalcounters[i])
        np_fglobalcounters = np.array(lst, dtype=np.float64)
        d_fglobalcounters = dict(zip(counter_names(mod_name, fcnts=True, special='mpi_global_'), np_fglobalcounters))
        
        rec['all_counters'] = {}
        rec['all_counters'].update(d_counters)
        rec['all_counters'].update(d_fcounters)
        rec['all_counters'].update(d_fsynccounters)
        rec['all_counters'].update(d_fglobalcounters)

    return rec


'''
        if dtype == 'numpy':
            rec['counters'] = np_counters
            rec['fcounters'] = np_fcounters
            rec['fsynccounters'] = np_fsynccounters
            rec['fglobalcounters'] = np_fglobalcounters
        else:
            rec['counters'] = d_counters
            rec['fcounters'] = d_fcounters
            rec['fsynccounters'] = d_fsynccounters
            rec['fglobalcounters'] = d_fglobalcounters
'''    
