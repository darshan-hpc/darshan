# -*- coding: utf-8 -*-

"""
The api_def_c carries a copy of CFFI compatible headers for libdarshan-util.so.
"""


header = """/* from darshan-logutils.h */
struct darshan_mnt_info
{
    char mnt_type[3031];
    char mnt_path[3031];
};

struct darshan_mod_info
{
    char *name;
    int	len;
    int	ver;
    int	idx;
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

struct darshan_decaf_record
{
    struct darshan_base_record base_rec;
    int64_t counters[4];
    double fcounters[4];
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



struct dxt_file_record {
    struct darshan_base_record base_rec;
    int64_t shared_record;  /* -1 means it is a shared file record */
    char hostname[64];      /* size defined via macro */

    int64_t write_count;
    int64_t read_count;
};

typedef struct segment_info {
    int64_t offset;
    int64_t length;
    double start_time;
    double end_time;
} segment_info;









typedef uint64_t darshan_record_id;

struct darshan_name_record
{
    darshan_record_id id;
    char* name;
};



/* counter names */
extern char *apxc_counter_names[];
extern char *bgq_counter_names[];
extern char *bgq_f_counter_names[];
extern char *hdf5_counter_names[];
extern char *hdf5_f_counter_names[];
extern char *mpiio_counter_names[];
extern char *mpiio_f_counter_names[];
extern char *pnetcdf_counter_names[];
extern char *pnetcdf_f_counter_names[];
extern char *posix_counter_names[];
extern char *posix_f_counter_names[];
extern char *stdio_counter_names[];
extern char *stdio_f_counter_names[];
extern char *decaf_counter_names[];
extern char *decaf_f_counter_names[];

/* Supported Functions */
void* darshan_log_open(char *);
int darshan_log_get_job(void *, struct darshan_job *);
void darshan_log_close(void*);
int darshan_log_get_exe(void*, char *);
int darshan_log_get_mounts(void*, struct darshan_mnt_info **, int*);
void darshan_log_get_modules(void*, struct darshan_mod_info **, int*);
int darshan_log_get_record(void*, int, void **);

int darshan_log_get_namehash(void*, struct darshan_name_record_ref **hash);

void darshan_log_get_name_records(void*, struct darshan_name_record **, int*);

"""



def load_darshan_header():
    """
    Returns a CFFI compatible header for darshan-utlil as a string.

    :return: String with a CFFI compatible header for darshan-util.
    """
    return header
