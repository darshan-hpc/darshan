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



_structdefs = {
    "BG/Q": "struct darshan_bgq_record **",
    "DXT_MPIIO": "struct dxt_file_record **",
    "DXT_POSIX": "struct dxt_file_record **",
    "H5F": "struct darshan_hdf5_file **",
    "H5D": "struct darshan_hdf5_dataset **",
    "LUSTRE": "struct darshan_lustre_record **",
    "MPI-IO": "struct darshan_mpiio_file **",
    "PNETCDF": "struct darshan_pnetcdf_file **",
    "POSIX": "struct darshan_posix_file **",
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
    job['start_time'] = jobrec[0].start_time
    job['end_time'] = jobrec[0].end_time
    job['nprocs'] = jobrec[0].nprocs
    job['jobid'] = jobrec[0].jobid

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
                {'len': mods[0][i].len, 'ver': mods[0][i].ver, 'idx': mods[0][i].idx}

    # add to cache
    log['modules'] = modules

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

    The typical darshan log record provides two arrays, on for integer counters
    and one for floating point counters:

    >>> darshan.log_get_generic_record(log, "POSIX", "struct darshan_posix_file **")
    {'counters': array([...], dtype=int64), 'fcounters': array([...])}

    """
    modules = log_get_modules(log)
    if mod_name not in modules:
        return None
    mod_type = _structdefs[mod_name]

    rec = {}
    buf = ffi.new("void **")
    r = libdutil.darshan_log_get_record(log['handle'], modules[mod_name]['idx'], buf)
    if r < 1:
        return None
    rbuf = ffi.cast(mod_type, buf)

    rec['id'] = rbuf[0].base_rec.id
    rec['rank'] = rbuf[0].base_rec.rank
    if mod_name == 'H5D':
        rec['file_rec_id'] = rbuf[0].file_rec_id

    clst = np.frombuffer(ffi.buffer(rbuf[0].counters), dtype=np.int64)
    flst = np.frombuffer(ffi.buffer(rbuf[0].fcounters), dtype=np.float64)

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

    clst = []
    for i in range(0, len(rbuf[0].counters)):
        clst.append(rbuf[0].counters[i])
    rec['counters'] = np.array(clst, dtype=np.int64)
   
    # counters
    cdict = dict(zip(counter_names('LUSTRE'), rec['counters']))

    # ost_ids 
    sizeof_64 = ffi.sizeof("int64_t")
    sizeof_base = ffi.sizeof("struct darshan_base_record")
    offset = sizeof_base + sizeof_64 * len(rbuf[0].counters)
    offset = int(offset/sizeof_64)

    ost_ids = ffi.cast("int64_t *", rbuf[0])
    ostlst = []
    for i in range(offset, cdict['LUSTRE_STRIPE_WIDTH']+offset):
        ostlst.append(ost_ids[i])
    rec['ost_ids'] = np.array(ostlst, dtype=np.int64)


    # dtype conversion
    if dtype == "dict":
        rec.update({
            'counters': cdict, 
            'ost_ids': ostlst
            })

    if dtype == "pandas":
        df_c = pd.DataFrame(cdict, index=[0])

        # prepend id and rank
        df_c = df_c[df_c.columns[::-1]] # flip colum order
        df_c['id'] = rec['id']
        df_c['rank'] = rec['rank']
        df_c = df_c[df_c.columns[::-1]] # flip back

        rec.update({
            'counters': df_c,
            'ost_ids': pd.DataFrame(rec['ost_ids'], columns=['ost_ids']),
            })

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

    return rec




