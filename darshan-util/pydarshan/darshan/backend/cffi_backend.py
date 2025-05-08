# -*- coding: utf-8 -*-
"""The cfii_backend package will read a darshan log
using the functions defined in libdarshan-util.so
and is interfaced via the python CFFI module.
"""

import functools

import cffi
import ctypes

import numpy as np
import pandas as pd

from collections import namedtuple

import logging
logger = logging.getLogger(__name__)


from darshan.backend.api_def_c import load_darshan_header
from darshan.discover_darshan import find_utils
from darshan.discover_darshan import check_version

addins = ""

#
# Optional APXC module
#
try:
  from darshan.backend.apxc import *
  addins += get_apxc_defs()
except:
  pass

#
# Optional APMPI module
#
try:
  from darshan.backend.apmpi import *
  addins += get_apmpi_defs()
except:
  pass

API_def_c = load_darshan_header(addins)

ffi = cffi.FFI()
ffi.cdef(API_def_c)

libdutil = None
libdutil = find_utils(ffi, libdutil)

check_version(ffi, libdutil)


_mod_names = [
    "NULL",
    "POSIX",
    "MPI-IO",
    "H5F",
    "H5D",
    "PNETCDF_FILE",
    "PNETCDF_VAR",
    "BG/Q",
    "LUSTRE",
    "STDIO",
    "DXT_POSIX",
    "DXT_MPIIO",
    "MDHIM",
    "APXC",
    "APMPI",
    "HEATMAP",
    "DFS",
    "DAOS",
]
def mod_name_to_idx(mod_name):
    return _mod_names.index(mod_name)

_structdefs = {
    "BG/Q": "struct darshan_bgq_record **",
    "DXT_MPIIO": "struct dxt_file_record **",
    "DXT_POSIX": "struct dxt_file_record **",
    "HEATMAP": "struct darshan_heatmap_record **",
    "H5F": "struct darshan_hdf5_file **",
    "H5D": "struct darshan_hdf5_dataset **",
    "LUSTRE": "struct darshan_lustre_record **",
    "MPI-IO": "struct darshan_mpiio_file **",
    "PNETCDF_FILE": "struct darshan_pnetcdf_file **",
    "PNETCDF_VAR": "struct darshan_pnetcdf_var **",
    "POSIX": "struct darshan_posix_file **",
    "DFS": "struct darshan_dfs_file **",
    "DAOS": "struct darshan_daos_object **",
    "STDIO": "struct darshan_stdio_file **",
    "APXC-HEADER": "struct darshan_apxc_header_record **",
    "APXC-PERF": "struct darshan_apxc_perf_record **",
    "APMPI-HEADER": "struct darshan_apmpi_header_record **",
    "APMPI-PERF": "struct darshan_apmpi_perf_record **",
}



def get_lib_version():
    """
    Return the version information hardcoded into the shared library.
    
    Args:
        None
        
    Return:
        version (str): library version number
    """
    ver = ffi.new("char **")
    ver = libdutil.darshan_log_get_lib_version()
    version = ffi.string(ver).decode("utf-8")
    return version


def log_open(filename):
    """
    Opens a darshan logfile.

    Args:
        filename (str): Path to a darshan log file

    Return:
        log handle
    """
    b_fname = filename.encode()
    handle = libdutil.darshan_log_open(b_fname)
    log = {"handle": handle, 'modules': None, 'name_records': None}

    return log


def log_close(log):
    """
    Closes the logfile and releases allocated memory.
    """
    libdutil.darshan_log_close(log['handle'])
    #modules = {}
    return


def log_get_job(log):
    """
    Returns a dictionary with information about the current job.
    """

    job = {}

    jobrec = ffi.new("struct darshan_job *")
    libdutil.darshan_log_get_job(log['handle'], jobrec)

    job['uid'] = jobrec[0].uid
    job['start_time_sec'] = jobrec[0].start_time_sec
    job['start_time_nsec'] = jobrec[0].start_time_nsec
    job['end_time_sec'] = jobrec[0].end_time_sec
    job['end_time_nsec'] = jobrec[0].end_time_nsec
    job['nprocs'] = jobrec[0].nprocs
    job['jobid'] = jobrec[0].jobid

    runtime = ffi.new("double *")
    libdutil.darshan_log_get_job_runtime(log['handle'], jobrec[0], runtime)
    job['run_time'] = runtime[0]

    # dirty hack to get log format version -- we know it's currently stored at the
    # very beginning of the log handle structure, so we just cast the struct
    # pointer as a string...
    job['log_ver'] = ffi.string(ffi.cast("char *", log['handle'])).decode("utf-8")

    mstr = ffi.string(jobrec[0].metadata).decode("utf-8")
    md = {}

    for kv in mstr.split('\n')[:-1]:
        k,v = kv.split('=', maxsplit=1)
        md[k] = v

    job['metadata'] = md
    return job


def log_get_exe(log):
    """
    Get details about the executable (path and arguments)

    Args:
        log: handle returned by darshan.open

    Return:
        string: executable path and arguments
    """

    exestr = ffi.new("char[]", 4096)
    libdutil.darshan_log_get_exe(log['handle'], exestr)
    return ffi.string(exestr).decode("utf-8")


def log_get_mounts(log):
    """
    Returns a list of available mounts recorded for the log.

    Args:
        log: handle returned by darshan.open
    """

    mntlst = []
    mnts = ffi.new("struct darshan_mnt_info **")
    cnt = ffi.new("int *")
    libdutil.darshan_log_get_mounts(log['handle'], mnts, cnt)

    for i in range(0, cnt[0]):
        mntlst.append((ffi.string(mnts[0][i].mnt_path).decode("utf-8"),
            ffi.string(mnts[0][i].mnt_type).decode("utf-8")))
    libdutil.darshan_free(mnts[0])

    return mntlst


def log_get_modules(log):
    """
    Return a dictionary containing available modules including information 
    about the contents available for each module in the current log.

    Args:
        log: handle returned by darshan.open

    Return:
        dict: Modules with additional info for current log.

    """

    # use cached module index if already present
    if log['modules'] != None:
        return log['modules']

    modules = {}

    mods = ffi.new("struct darshan_mod_info **")
    cnt    = ffi.new("int *")
    libdutil.darshan_log_get_modules(log['handle'], mods, cnt)
    for i in range(0, cnt[0]):
        modules[ffi.string(mods[0][i].name).decode("utf-8")] = \
                {'len': mods[0][i].len, 'ver': mods[0][i].ver, 'idx': mods[0][i].idx,
                 'partial_flag': bool(mods[0][i].partial_flag)}

    # add to cache
    log['modules'] = modules
    libdutil.darshan_free(mods[0])

    return modules


def log_get_name_records(log):
    """
    Return a dictionary resovling hash to string (typically a filepath).

    Args:
        log: handle returned by darshan.open
        hash: hash-value (a number)

    Return:
        dict: the name records
    """

    # used cached name_records if already present
    if log['name_records'] != None:
        return log['name_records']


    name_records = {}

    nrecs = ffi.new("struct darshan_name_record **")
    cnt = ffi.new("int *")
    libdutil.darshan_log_get_name_records(log['handle'], nrecs, cnt)

    for i in range(0, cnt[0]):
        name_records[nrecs[0][i].id] = ffi.string(nrecs[0][i].name).decode("utf-8")
        libdutil.darshan_free(nrecs[0][i].name)
    libdutil.darshan_free(nrecs[0])

    # add to cache
    log['name_records'] = name_records

    return name_records



def log_lookup_name_records(log, ids=[]):
    """
    Resolve a single hash to it's name record string (typically a filepath).

    Args:
        log: handle returned by darshan.open
        hash: hash-value (a number)

    Return:
        dict: the name records
    """

    name_records = {}

    #cids = ffi.new("darshan_record_id *") * len(ids)
    whitelist = (ctypes.c_ulonglong * len(ids))(*ids)
    whitelist_cnt = len(ids)

    whitelistp = ffi.from_buffer(whitelist)

    nrecs = ffi.new("struct darshan_name_record **")
    cnt = ffi.new("int *")
    libdutil.darshan_log_get_filtered_name_records(log['handle'], nrecs, cnt, ffi.cast("darshan_record_id *", whitelistp), whitelist_cnt)

    for i in range(0, cnt[0]):
        name_records[nrecs[0][i].id] = ffi.string(nrecs[0][i].name).decode("utf-8")
        libdutil.darshan_free(nrecs[0][i].name)
    libdutil.darshan_free(nrecs[0])

    # add to cache
    log['name_records'] = name_records

    return name_records





def log_get_record(log, mod, dtype='numpy'):
    """
    Standard entry point fetch records via mod string.

    Args:
        log: Handle returned by darshan.open
        mod_name (str): Name of the Darshan module

    Return:
        log record of type dtype
    
    """

    if mod in ['LUSTRE']:
        rec = _log_get_lustre_record(log, dtype=dtype)
    elif mod in ['HEATMAP']:
        rec = _log_get_heatmap_record(log)
    elif mod in ['DXT_POSIX', 'DXT_MPIIO']:
        rec = log_get_dxt_record(log, mod, dtype=dtype)
    else:
        rec = log_get_generic_record(log, mod, dtype=dtype)

    return rec



def log_get_generic_record(log, mod_name, dtype='numpy'):
    """
    Returns a dictionary holding a generic darshan log record.

    Args:
        log: Handle returned by darshan.open
        mod_name (str): Name of the Darshan module

    Return:
        dict: generic log record

    Example:

    The typical darshan log record provides two arrays, one for integer counters
    and one for floating point counters:

    >>> darshan.log_get_generic_record(log, "POSIX", "struct darshan_posix_file **")
    {'counters': array([...], dtype=int64), 'fcounters': array([...])}

    """
    modules = log_get_modules(log)
    if mod_name not in modules:
        return None
    mod_type = _structdefs[mod_name]

    buf = ffi.new("void **")
    r = libdutil.darshan_log_get_record(log['handle'], modules[mod_name]['idx'], buf)
    if r < 1:
        return None
    rbuf = ffi.cast(mod_type, buf)

    rec = _make_generic_record(rbuf, mod_name, dtype)
    libdutil.darshan_free(buf[0])

    return rec

def _make_generic_record(rbuf, mod_name, dtype='numpy'):
    """
    Returns a record dictionary for an input record buffer for a given module.
    """
    rec = {}
    rec['id'] = rbuf[0].base_rec.id
    rec['rank'] = rbuf[0].base_rec.rank
    if mod_name == 'H5D' or mod_name == 'PNETCDF_VAR':
        rec['file_rec_id'] = rbuf[0].file_rec_id

    clst = np.copy(np.frombuffer(ffi.buffer(rbuf[0].counters), dtype=np.int64))
    flst = np.copy(np.frombuffer(ffi.buffer(rbuf[0].fcounters), dtype=np.float64))

    c_cols = counter_names(mod_name)
    fc_cols = fcounter_names(mod_name)

    if dtype == "numpy":
        rec['counters'] = clst
        rec['fcounters'] = flst

    elif dtype == "dict":
        rec['counters'] = dict(zip(c_cols, clst))
        rec['fcounters'] = dict(zip(fc_cols, flst))

    elif dtype == "pandas":
        # prepend id/rank columns
        new_cols = ["id", "rank"]
        new_c_cols = new_cols + c_cols
        new_f_cols = new_cols + fc_cols
        rec_id = np.uint64(rec["id"])
        # prepend the id/rank values
        id_rank_list = [rec["id"], rec["rank"]]
        new_clst = np.asarray([id_rank_list + clst.tolist()]).reshape(1, -1)
        new_flst = np.asarray([id_rank_list + flst.tolist()], dtype=np.float64).reshape(1, -1)
        # create the dataframes
        df_c = pd.DataFrame(data=new_clst, columns=new_c_cols)
        df_fc = pd.DataFrame(data=new_flst, columns=new_f_cols)
        # correct the data type for the file hash/id
        df_c['id'] = rec_id
        df_fc['id'] = rec_id
        # assign the dataframes to the record
        rec['counters'] = df_c
        rec['fcounters'] = df_fc
    return rec

@functools.lru_cache(maxsize=32)
def counter_names(mod_name, fcnts=False, special=''):
    """
    Returns a list of available counter names for the module.
    By default only integer counter names are listed, unless fcnts is set to
    true in which case only the floating point counter names are listed.

    Args:
        mod_name (str): Name of the module to return counter names.
        fcnts (bool): Switch to request floating point counters instead of integer. (Default: False)

    Return:
        list: Counter names as strings.

    """

    if mod_name == 'MPI-IO':
        mod_name = 'MPIIO'

    names = []
    i = 0

    if fcnts:
        F = "f_"
    else:
        F = ""

    end = "{0}_{1}{2}NUM_INDICES".format(mod_name.upper(), F.upper(), special.upper())
    var_name = "{0}_{1}{2}counter_names".format(mod_name.lower(), F.lower(), special.lower())

    while True: 
        try:
            var = getattr(libdutil, var_name)
        except:
            var = None
        if not var:
            return None
        name = ffi.string(var[i]).decode("utf-8")
        if name == end:
            break
        names.append(name)
        i += 1

    return names


@functools.lru_cache(maxsize=32)
def fcounter_names(mod_name):
    """
    Returns a list of available floating point counter names for the module.

    Args:
        mod_name (str): Name of the module to return counter names.

    Return:
        list: Available floiting point counter names as strings.

    """
    return counter_names(mod_name, fcnts=True)


def _log_get_lustre_record(log, dtype='numpy'):
    """
    Returns a darshan log record for Lustre.

    Args:
        log: handle returned by darshan.open
    """
    modules = log_get_modules(log)
    if 'LUSTRE' not in modules:
        return None

    rec = {}
    buf = ffi.new("void **")
    r = libdutil.darshan_log_get_record(log['handle'], modules['LUSTRE']['idx'], buf)
    if r < 1:
        return None
    rbuf = ffi.cast("struct darshan_lustre_record **", buf)

    rec['id'] = rbuf[0].base_rec.id
    rec['rank'] = rbuf[0].base_rec.rank

    # components
    rec['components'] = []
    ost_ids = ffi.cast("int64_t *", rbuf[0].ost_ids)
    ost_idx = 0
    for i in range(0, rbuf[0].num_comps):
        component = {}
        component['counters'] = np.copy(np.frombuffer(ffi.buffer(rbuf[0].comps[i].counters), dtype=np.int64))
        component['pool_name'] = ffi.string(rbuf[0].comps[i].pool_name).decode("utf-8")
        cdict = dict(zip(counter_names('LUSTRE_COMP'), component['counters']))
        # ost info
        stripe_count = cdict['LUSTRE_COMP_STRIPE_COUNT']
        ostlst = ffi.unpack(ost_ids + ost_idx, int(stripe_count))
        ost_idx += int(stripe_count)
        component['ost_ids'] = np.array(ostlst, dtype=np.int64)

        # dtype conversion
        if dtype == "dict":
            component.update({
                'counters': cdict,
                'ost_ids': ostlst
                })
        elif dtype == "pandas":
            df_c = pd.DataFrame(cdict, index=[0])

            # prepend id and rank
            df_c = df_c[df_c.columns[::-1]] # flip colum order
            df_c['id'] = rec['id']
            df_c['rank'] = rec['rank']
            df_c = df_c[df_c.columns[::-1]] # flip back

            # add pool_name string to df
            df_c['LUSTRE_POOL_NAME'] = component['pool_name']
            # add ost list to df
            df_c['LUSTRE_OST_IDS'] = [component['ost_ids']]
            # overwrite component with comprehensive dataframe
            component = df_c

        rec['components'].append(component)

    if dtype == "pandas":
        combined_c = None
        for component in rec['components']:
            if combined_c is None:
                combined_c = component
            else:
                combined_c = pd.concat([combined_c, component])

        rec['components'] = combined_c

    libdutil.darshan_free(buf[0])
    return rec



def log_get_dxt_record(log, mod_name, reads=True, writes=True, dtype='dict'):
    """
    Returns a dictionary holding a dxt darshan log record.

    Args:
        log: Handle returned by darshan.open
        mod_name (str): Name of the Darshan module
        mod_type (str): String containing the C type

    Return:
        dict: generic log record

    Example:

    The typical darshan log record provides two arrays, on for integer counters
    and one for floating point counters:

    >>> darshan.log_get_dxt_record(log, "DXT_POSIX", "struct dxt_file_record **")
    {'rank': 0, 'read_count': 11, 'read_segments': array([...]), ...}


    """

    modules = log_get_modules(log)
    if mod_name not in modules:
        return None
    mod_type = _structdefs[mod_name]
    #name_records = log_get_name_records(log)

    rec = {}
    buf = ffi.new("void **")
    r = libdutil.darshan_log_get_record(log['handle'], modules[mod_name]['idx'], buf)
    if r < 1:
        return None
    filerec = ffi.cast(mod_type, buf)

    rec['id'] = filerec[0].base_rec.id
    rec['rank'] = filerec[0].base_rec.rank
    rec['hostname'] = ffi.string(filerec[0].hostname).decode("utf-8")
    #rec['filename'] = name_records[rec['id']]

    wcnt = filerec[0].write_count
    rcnt = filerec[0].read_count

    rec['write_count'] = wcnt
    rec['read_count'] = rcnt
 
    rec['write_segments'] = []
    rec['read_segments'] = []


    size_of = ffi.sizeof("struct dxt_file_record")
    segments = ffi.cast("struct segment_info *", buf[0] + size_of  )


    for i in range(wcnt):
        seg = {
            "offset": segments[i].offset,
            "length": segments[i].length,
            "start_time": segments[i].start_time,
            "end_time": segments[i].end_time
        }
        rec['write_segments'].append(seg)


    for i in range(rcnt):
        i = i + wcnt
        seg = {
            "offset": segments[i].offset,
            "length": segments[i].length,
            "start_time": segments[i].start_time,
            "end_time": segments[i].end_time
        }
        rec['read_segments'].append(seg)


    if dtype == "pandas":
        rec['read_segments'] = pd.DataFrame(rec['read_segments'])
        rec['write_segments'] = pd.DataFrame(rec['write_segments'])

    libdutil.darshan_free(buf[0])
    return rec


def _log_get_heatmap_record(log):
    """
    Returns a dictionary holding a heatmap darshan log record.

    Args:
        log: Handle returned by darshan.open

    Return:
        dict: heatmap log record
    """
   
    mod_name = "HEATMAP"

    modules = log_get_modules(log)
    if mod_name not in modules:
        return None

    mod_type = _structdefs[mod_name]

    rec = {}
    buf = ffi.new("void **")
    r = libdutil.darshan_log_get_record(log['handle'], modules[mod_name]['idx'], buf)
    if r < 1:
        return None
    
    filerec = ffi.cast(mod_type, buf)

    rec['id'] = filerec[0].base_rec.id
    rec['rank'] = filerec[0].base_rec.rank

    bin_width_seconds = filerec[0].bin_width_seconds
    nbins = filerec[0].nbins
    
    rec['bin_width_seconds'] = bin_width_seconds
    rec['nbins'] = nbins

    # write/read bins
    sizeof_64 = ffi.sizeof("int64_t")
    
    write_bins = np.copy(np.frombuffer(ffi.buffer(filerec[0].write_bins, sizeof_64*nbins), dtype = np.int64))
    rec['write_bins'] = write_bins

    read_bins = np.copy(np.frombuffer(ffi.buffer(filerec[0].read_bins, sizeof_64*nbins), dtype = np.int64))
    rec['read_bins'] = read_bins
    libdutil.darshan_free(buf[0])
    
    return rec


def _df_to_rec(rec_dict, mod_name, rec_index_of_interest=None):
    """
    Pack the DataFrames-format PyDarshan data back into
    a C buffer of records that can be consumed by darshan-util
    C code.

    Parameters
    ----------
    rec_dict: dict
        Dictionary containing the counter and fcounter dataframes.

    mod_name: str
        Name of the darshan module.

    rec_index_of_interest: int or None
        If ``None``, use all records in the dataframe. Otherwise,
        repack only the the record at the provided integer index.

    Returns
    -------
    buf: Raw char array containing a buffer of record(s) or a single record.
    """
    counters_df = rec_dict["counters"]
    fcounters_df = rec_dict["fcounters"]
    counters_n_cols = counters_df.shape[1]
    fcounters_n_cols = fcounters_df.shape[1]
    id_col = counters_df.columns.get_loc("id")
    rank_col = counters_df.columns.get_loc("rank")
    if rec_index_of_interest is None:
        num_recs = counters_df.shape[0]
        # newer pandas versions can support ...
        # but we use a slice for now
        rec_index_of_interest = slice(0, counters_df.shape[0])
    else:
        num_recs = 1
    # id and rank columns are duplicated
    # in counters and fcounters
    rec_arr = np.recarray(shape=(num_recs), dtype=[("id", "<u8", (1,)),
                                                   ("rank", "<i8", (1,)),
                                                   ("counters", "<i8", (counters_n_cols - 2,)),
                                                   ("fcounters", "<f8", (fcounters_n_cols - 2,))])
    rec_arr.fcounters = fcounters_df.iloc[rec_index_of_interest, 2:].to_numpy()
    rec_arr.counters = counters_df.iloc[rec_index_of_interest, 2:].to_numpy()
    if num_recs > 1:
        rec_arr.id = counters_df.iloc[rec_index_of_interest, id_col].to_numpy().reshape((num_recs, 1))
        rec_arr.rank = counters_df.iloc[rec_index_of_interest, rank_col].to_numpy().reshape((num_recs, 1))
    else:
        rec_arr.id = counters_df.iloc[rec_index_of_interest, id_col]
        rec_arr.rank = counters_df.iloc[rec_index_of_interest, rank_col]
    buf = rec_arr.tobytes()
    return buf


def accumulate_records(rec_dict, mod_name, nprocs):
    """
    Passes a set of records (in pandas format) to the Darshan accumulator
    interface, and returns the corresponding derived metrics struct and
    summary record.

    Parameters:
        rec_dict: Dictionary containing the counter and fcounter dataframes.
        mod_name: Name of the Darshan module.
        nprocs: Number of processes participating in accumulation.

    Returns:
        namedtuple containing derived_metrics (cdata object) and
        summary_record (dict).
    """
    mod_idx = mod_name_to_idx(mod_name)
    darshan_accumulator = ffi.new("darshan_accumulator *")
    r = libdutil.darshan_accumulator_create(mod_idx, nprocs, darshan_accumulator)
    if r != 0:
        raise RuntimeError("A nonzero exit code was received from "
                           "darshan_accumulator_create() at the C level. "
                           f"This could mean that the {mod_name} module does not "
                           "support derived metric calculation, or that "
                           "another kind of error occurred. It may be possible "
                           "to retrieve additional information from the stderr "
                           "stream.")

    num_recs = rec_dict["fcounters"].shape[0]
    record_array = _df_to_rec(rec_dict, mod_name)

    r_i = libdutil.darshan_accumulator_inject(darshan_accumulator[0], record_array, num_recs)
    if r_i != 0:
        raise RuntimeError("A nonzero exit code was received from "
                           "darshan_accumulator_inject() at the C level. "
                           "It may be possible "
                           "to retrieve additional information from the stderr "
                           "stream.")
    derived_metrics = ffi.new("struct darshan_derived_metrics *")
    summary_rbuf = ffi.new(_structdefs[mod_name].replace("**", "*"))
    r = libdutil.darshan_accumulator_emit(darshan_accumulator[0],
                                          derived_metrics,
                                          summary_rbuf)
    libdutil.darshan_accumulator_destroy(darshan_accumulator[0])
    if r != 0:
        raise RuntimeError("A nonzero exit code was received from "
                           "darshan_accumulator_emit() at the C level. "
                           "It may be possible "
                           "to retrieve additional information from the stderr "
                           "stream.")

    summary_rec = _make_generic_record(summary_rbuf, mod_name, dtype='pandas')

    # create namedtuple type to hold return values
    AccumulatedRecords = namedtuple("AccumulatedRecords", ['derived_metrics', 'summary_record'])
    return AccumulatedRecords(derived_metrics, summary_rec)
