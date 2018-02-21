import cffi
import numpy

API_def_c = r"""

/* from darshan-logutils.h */
struct darshan_mnt_info
{
    char mnt_type[3031];
    char mnt_path[3031];
};

struct darshan_mod_info
{
    char *name;
    int  len;
    int  ver;
    int  idx;
};

/* from darshan-log-format.h */
struct darshan_job
{
    int64_t uid;
    int64_t start_time;
    int64_t end_time;
    int64_t nprocs;
    int64_t jobid;
    char metadata[1024];
};
struct darshan_base_record
{
    uint64_t id;
    int64_t rank;
};

struct darshan_posix_file
{
    struct darshan_base_record base_rec;
    int64_t counters[64];
    double fcounters[17];
};

struct darshan_stdio_file
{
    struct darshan_base_record base_rec;
    int64_t counters[13];
    double fcounters[15];
};

struct darshan_mpiio_file
{
    struct darshan_base_record base_rec;
    int64_t counters[51];
    double fcounters[15];
};

struct darshan_hdf5_file
{
    struct darshan_base_record base_rec;
    int64_t counters[1];
    double fcounters[2];
};

struct darshan_pnetcdf_file
{
    struct darshan_base_record base_rec;
    int64_t counters[2];
    double fcounters[2];
};

struct darshan_bgq_record
{
    struct darshan_base_record base_rec;
    int64_t counters[11];
    double fcounters[1];
};

/* from darshan-apxc-log-format.h */
struct darshan_apxc_header_record
{
    struct darshan_base_record base_rec;
    int64_t magic;
    int nblades;
    int nchassis;
    int ngroups;
    int memory_mode;
    int cluster_mode;
};
struct darshan_apxc_perf_record
{
    struct darshan_base_record base_rec;
    int64_t counters[396];
};

/* counter names */
char *apxc_counter_names[];
char *bgq_counter_names[];
char *bgq_f_counter_names[];
char *hdf5_counter_names[];
char *hdf5_f_counter_names[];
char *mpiio_counter_names[];
char *mpiio_f_counter_names[];
char *pnetcdf_counter_names[];
char *pnetcdf_f_counter_names[];
char *posix_counter_names[];
char *posix_f_counter_names[];
char *stdio_counter_names[];
char *stdio_f_counter_names[];

/* Supported Functions */
void* darshan_log_open(char *);
int darshan_log_get_job(void *, struct darshan_job *);
void darshan_log_close(void*);
int darshan_log_get_exe(void*, char *);
int darshan_log_get_mounts(void*, struct darshan_mnt_info **, int*);
void darshan_log_get_modules(void*, struct darshan_mod_info **, int*);
int darshan_log_get_record(void*, int, void **);
"""

ffi = cffi.FFI()
ffi.cdef(API_def_c)
libdutil = ffi.dlopen("/home/harms/working/darshan/cooley/install/lib/libdarshan-util.so")

modules = {}

def log_open(filename):
  b_fname = filename.encode()
  log = libdutil.darshan_log_open(b_fname)
  if log:
    mods = log_get_modules(log)
  return log

def log_close(log):
  libdutil.darshan_log_close(log)
  modules = {}
  return

def log_get_job(log):
  job = {}
  jobrec = ffi.new("struct darshan_job *")
  libdutil.darshan_log_get_job(log, jobrec)
  job['jobid'] = jobrec[0].jobid
  job['uid']   = jobrec[0].uid
  job['start_time'] = jobrec[0].start_time
  job['end_time']   = jobrec[0].end_time
  mstr = ffi.string(jobrec[0].metadata).decode("utf-8")
  md = {}
  for kv in mstr.split('\n')[:-1]:
    k,v = kv.split('=', maxsplit=1)
    md[k] = v
  job['metadata']   = md
  return job

def log_get_exe(log):
  exestr = ffi.new("char[]", 4096)
  libdutil.darshan_log_get_exe(log, exestr)
  return ffi.string(exestr).decode("utf-8")

def log_get_mounts(log):
  mntlst = []
  mnts = ffi.new("struct darshan_mnt_info **")
  cnt  = ffi.new("int *")
  libdutil.darshan_log_get_mounts(log, mnts, cnt)
  for i in range(0, cnt[0]):
      mntlst.append((ffi.string(mnts[0][i].mnt_path).decode("utf-8"),
                     ffi.string(mnts[0][i].mnt_type).decode("utf-8")))
  return mntlst

def log_get_modules(log):
  mods = ffi.new("struct darshan_mod_info **")
  cnt  = ffi.new("int *")
  libdutil.darshan_log_get_modules(log, mods, cnt)
  for i in range(0, cnt[0]):
      modules[ffi.string(mods[0][i].name).decode("utf-8")] = \
        {'len': mods[0][i].len, 'ver': mods[0][i].ver, 'idx': mods[0][i].idx}
  return modules

def log_get_generic_record(log, mod_name, mod_type):
  rec = {}
  buf = ffi.new("void **")
  r = libdutil.darshan_log_get_record(log, modules[mod_name]['idx'], buf)
  if r < 1:
    return None
  rbuf = ffi.cast(mod_type, buf)
  clst = []
  for i in range(0, len(rbuf[0].counters)):
    clst.append(rbuf[0].counters[i])
  rec['counters'] = numpy.array(clst, dtype=numpy.uint64)
  flst = []
  for i in range(0, len(rbuf[0].fcounters)):
    flst.append(rbuf[0].fcounters[i])
  rec['fcounters'] = numpy.array(clst, dtype=numpy.float64)
  return rec

def counter_names(mod_name, fcnts=False):
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
  return counter_names(mod_name, fcnts=True)

def log_get_bgq_record(log):
  return log_get_generic_record(log, "BG/Q", "struct darshan_bgq_record **")

def log_get_hdf5_record(log):
  return log_get_generic_record(log, "HDF5", "struct darshan_hdf5_file **")

def log_get_mpiio_record(log):
  return log_get_generic_record(log, "MPIIO", "struct darshan_mpiio_file **")

def log_get_pnetcdf_record(log):
  return log_get_generic_record(log, "PNETCDF", "struct darshan_pnetcdf_file **")

def log_get_posix_record(log):
  return log_get_generic_record(log, "POSIX", "struct darshan_posix_file **")

def log_get_stdio_record(log):
  return log_get_generic_record(log, "STDIO", "struct darshan_stdio_file **")

def log_get_apxc_record(log):
  rec = {}
  memory_modes = ['unknown', 'flat', 'equal', 'split', 'cache']
  cluster_modes = ['unknown', 'all2all', 'quad', 'hemi', 'snc4', 'snc2']
  buf = ffi.new("void **")
  r = libdutil.darshan_log_get_record(log, modules['DARSHAN_APXC']['idx'], buf)
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
    rec['counters'] = numpy.array(clst, dtype=numpy.uint64)
  return rec

