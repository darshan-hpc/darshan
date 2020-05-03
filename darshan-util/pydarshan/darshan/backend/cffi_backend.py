# -*- coding: utf-8 -*-

import cffi
import numpy as np
import pandas as pd


from darshan.discover_darshan import discover_darshan
from darshan.api_def_c import load_darshan_header

#DARSHAN_PATH = discover_darshan()
API_def_c = load_darshan_header()


ffi = cffi.FFI()
ffi.cdef(API_def_c)

libdutil = ffi.dlopen("libdarshan-util.so")



def log_open(filename):
    """
    Opens a darshan logfile.

    Args:
        filename (str): Path to a darshan log file

    Return:
        log handle
    """
    b_fname = filename.encode()
    log = {"handle": libdutil.darshan_log_open(b_fname), 'modules': None, 'name_records': None}

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

    # used cached module index if already present
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




def log_get_dxt_record(log, mod_name, mod_type, reads=True, writes=True, mode='pandas'):
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
    name_records = log_get_name_records(log)

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
    rec['filename'] = name_records[rec['id']]

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
        seg = {
            "offset": segments[i].offset,
            "length": segments[i].length,
            "start_time": segments[i].start_time,
            "end_time": segments[i].end_time
        }
        rec['read_segments'].append(seg)


    #pd.DataFrame([rec])


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
    {'counters': array([...], dtype=uint64), 'fcounters': array([...])}


    """
    modules = log_get_modules(log)

    rec = {}
    buf = ffi.new("void **")
    r = libdutil.darshan_log_get_record(log['handle'], modules[mod_name]['idx'], buf)
    if r < 1:
        return None
    rbuf = ffi.cast(mod_type, buf)
    clst = []

    rec['id'] = rbuf[0].base_rec.id
    rec['rank'] = rbuf[0].base_rec.rank

    for i in range(0, len(rbuf[0].counters)):
        clst.append(rbuf[0].counters[i])
    rec['counters'] = np.array(clst, dtype=np.uint64)
    flst = []

    for i in range(0, len(rbuf[0].fcounters)):
        flst.append(rbuf[0].fcounters[i])
    rec['fcounters'] = np.array(clst, dtype=np.float64)

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


def log_get_hdf5_record(log):
    """
    Returns a darshan log record for HDF5.

    Args:
        log: handle returned by darshan.open
    """
    return log_get_generic_record(log, "HDF5", "struct darshan_hdf5_file **")


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


def log_get_apxc_record(log):
    """
    Returns a darshan log record for APCX.

    Args:
        log: handle returned by darshan.open

    Returns:
        dict: log record
    """
    rec = {}

    modules = log_get_modules(log)

    memory_modes = ['unknown', 'flat', 'equal', 'split', 'cache']
    cluster_modes = ['unknown', 'all2all', 'quad', 'hemi', 'snc4', 'snc2']
    buf = ffi.new("void **")
    r = libdutil.darshan_log_get_record(log['handle'], modules['DARSHAN_APXC']['idx'], buf)
    if r < 1:
        return None
    prf = ffi.cast("struct darshan_apxc_perf_record **", buf)
    hdr = ffi.cast("struct darshan_apxc_header_record **", buf)
    if hdr[0].magic == 4707761685111591494:
        mm = hdr[0].memory_mode & ~(1 << 31)
        cm = hdr[0].cluster_mode & ~(1 << 31)
        rec['nblades'] = hdr[0].nblades
        rec['nchassis'] = hdr[0].nchassis
        rec['ngroups'] = hdr[0].ngroups
        rec['memory_mode'] = memory_modes[mm]
        rec['cluster_mode'] = cluster_modes[cm]
    else:
        rec['group'] = prf[0].counters[0]
        rec['chassis'] = prf[0].counters[1]
        rec['blade'] = prf[0].counters[2]
        rec['node'] = prf[0].counters[3]
        clst = []
        for i in range(0, len(prf[0].counters)):
            clst.append(prf[0].counters[i])
        rec['counters'] = np.array(clst, dtype=np.uint64)
    return rec
