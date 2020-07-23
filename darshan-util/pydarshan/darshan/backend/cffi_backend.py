# -*- coding: utf-8 -*-

import cffi
import ctypes

import numpy as np
import pandas as pd


from darshan.api_def_c import load_darshan_header
from darshan.discover_darshan import find_utils
from darshan.discover_darshan import check_version

API_def_c = load_darshan_header()


ffi = cffi.FFI()
ffi.cdef(API_def_c)

libdutil = None
libdutil = find_utils(ffi, libdutil)

check_version(ffi, libdutil)



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
        string: executeable path and arguments
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





def log_get_dxt_record(log, mod_name, mod_type, reads=True, writes=True, mode='dict'):
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
    #name_records = log_get_name_records(log)

    rec = {}
    buf = ffi.new("void **")
    r = libdutil.darshan_log_get_record(log['handle'], modules[mod_name]['idx'], buf)
    if r < 1:
        return None
    filerec = ffi.cast(mod_type, buf)
    clst = []

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


    if mode == "pandas":
        rec['read_segments'] = pd.DataFrame(rec['read_segments'])
        rec['write_segments'] = pd.DataFrame(rec['write_segments'])

    return rec



def log_get_generic_record(log, mod_name, mod_type, mode='numpy'):
    """
    Returns a dictionary holding a generic darshan log record.

    Args:
        log: Handle returned by darshan.open
        mod_name (str): Name of the Darshan module
        mod_type (str): String containing the C type

    Return:
        dict: generic log record

    Example:

    The typical darshan log record provides two arrays, on for integer counters
    and one for floating point counters:

    >>> darshan.log_get_generic_record(log, "POSIX", "struct darshan_posix_file **")
    {'counters': array([...], dtype=int64), 'fcounters': array([...])}

    """
    modules = log_get_modules(log)

    rec = {}
    buf = ffi.new("void **")
    r = libdutil.darshan_log_get_record(log['handle'], modules[mod_name]['idx'], buf)
    if r < 1:
        return None
    rbuf = ffi.cast(mod_type, buf)

    rec['id'] = rbuf[0].base_rec.id
    rec['rank'] = rbuf[0].base_rec.rank

    clst = []
    for i in range(0, len(rbuf[0].counters)):
        clst.append(rbuf[0].counters[i])
    rec['counters'] = np.array(clst, dtype=np.int64)
    cdict = dict(zip(counter_names(mod_name), rec['counters']))

    flst = []
    for i in range(0, len(rbuf[0].fcounters)):
        flst.append(rbuf[0].fcounters[i])
    rec['fcounters'] = np.array(flst, dtype=np.float64)
    fcdict = dict(zip(fcounter_names(mod_name), rec['fcounters']))

    if mode == "dict":
        rec = {'counters': cdict, 'fcounter': fcdict}

    if mode == "pandas":
        rec = {
                'counters': pd.DataFrame(cdict, index=[0]),
                'fcounters': pd.DataFrame(fcdict, index=[0])
                }

    return rec


def counter_names(mod_name, fcnts=False):
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

    end = "{0}_{1}NUM_INDICES".format(mod_name.upper(), F.upper())
    var_name = "{0}_{1}counter_names".format(mod_name.lower(), F.lower())

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


def fcounter_names(mod_name):
    """
    Returns a list of available floating point counter names for the module.

    Args:
        mod_name (str): Name of the module to return counter names.

    Return:
        list: Available floiting point counter names as strings.

    """
    return counter_names(mod_name, fcnts=True)


def log_get_bgq_record(log):
    """
    Returns a darshan log record for BG/Q.

    Args:
        log: handle returned by darshan.open
    """
    return log_get_generic_record(log, "BG/Q", "struct darshan_bgq_record **")


def log_get_hdf5_file_record(log):
    """
    Returns a darshan log record for an HDF5 file.

    Args:
        log: handle returned by darshan.open
    """
    return log_get_generic_record(log, "H5F", "struct darshan_hdf5_file **")

def log_get_hdf5_dataset_record(log):
    """
    Returns a darshan log record for an HDF5 dataset.

    Args:
        log: handle returned by darshan.open
    """
    return log_get_generic_record(log, "H5D", "struct darshan_hdf5_dataset **")

def log_get_lustre_record(log):
    """
    Returns a darshan log record for Lustre.

    Args:
        log: handle returned by darshan.open
    """
    modules = log_get_modules(log)

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
    cdict = dict(zip(counter_names('LUSTRE'), rec['counters']))

    # FIXME
    ostlst = []
    for i in range(0, cdict['LUSTRE_STRIPE_WIDTH']):
        print(rbuf[0].ost_ids[i])
    rec['ost_ids'] = np.array(ostlst, dtype=np.int64)

    print(rec['ost_ids'])
    sys.exit()

    if mode == "dict":
        rec = {'counters': cdict, 'fcounter': fcdict}

    if mode == "pandas":
        rec = {
                'counters': pd.DataFrame(cdict, index=[0]),
                'fcounters': pd.DataFrame(fcdict, index=[0])
                }

    return rec

def log_get_mpiio_record(log):
    """
    Returns a darshan log record for MPI-IO.

    Args:
        log: handle returned by darshan.open

    Returns:
        dict: log record
    """
    return log_get_generic_record(log, "MPI-IO", "struct darshan_mpiio_file **")


def log_get_pnetcdf_record(log):
    """
    Returns a darshan log record for PnetCDF.

    Args:
        log: handle returned by darshan.open

    Returns:
        dict: log record
    """
    return log_get_generic_record(log, "PNETCDF", "struct darshan_pnetcdf_file **")


def log_get_posix_record(log):
    """
    Returns a darshan log record for 

    Args:
        log: handle returned by darshan.open

    Returns:
        dict: log record
    """
    return log_get_generic_record(log, "POSIX", "struct darshan_posix_file **")



def log_get_stdio_record(log):
    """
    Returns a darshan log record for STDIO.

    Args:
        log: handle returned by darshan.open

    Returns:
        dict: log record
    """
    return log_get_generic_record(log, "STDIO", "struct darshan_stdio_file **")
    
    

