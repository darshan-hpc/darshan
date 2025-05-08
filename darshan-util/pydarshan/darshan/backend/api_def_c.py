# -*- coding: utf-8 -*-

"""
The api_def_c carries a copy of CFFI compatible headers for libdarshan-util.so.
These definitions must match the structure definitions for the associated 
darshan release.
"""


header = """/* from darshan-logutils.h */

struct darshan_file_category_counters {
    int64_t count;                   /* number of files in this category */
    int64_t total_read_volume_bytes; /* total read traffic volume */
    int64_t total_write_volume_bytes;/* total write traffic volume */
    int64_t max_read_volume_bytes;   /* maximum read traffic volume to 1 file */
    int64_t max_write_volume_bytes;  /* maximum write traffic volume to 1 file */
    int64_t total_max_offset_bytes;  /* summation of max_offsets */
    int64_t max_offset_bytes;        /* largest max_offset */
    int64_t nprocs;                  /* how many procs accessed (-1 for "all") */
};

struct darshan_derived_metrics {
    int64_t total_bytes;
    double unique_io_total_time_by_slowest;
    double unique_rw_only_time_by_slowest;
    double unique_md_only_time_by_slowest;
    int unique_io_slowest_rank;
    double shared_io_total_time_by_slowest;
    double agg_perf_by_slowest;
    double agg_time_by_slowest;
    struct darshan_file_category_counters category_counters[7];
};

struct darshan_mnt_info
{
    char mnt_type[3015];
    char mnt_path[3015];
};

struct darshan_mod_info
{
    char *name;
    int	len;
    int	ver;
    int	idx;
    int partial_flag;
};

/* opaque accumulator reference */
struct darshan_accumulator_st;
typedef struct darshan_accumulator_st* darshan_accumulator;

/* NOTE: darshan_module_id is technically an enum in the C API, but we'll
 * just use an int for now (equivalent type) to avoid warnings from cffi
 * that we have not defined explicit enum values.  We don't need that
 * functionality.
 */
int darshan_accumulator_create(int darshan_module_id, int64_t, darshan_accumulator*);
int darshan_accumulator_inject(darshan_accumulator, void*, int);
int darshan_accumulator_emit(darshan_accumulator, struct darshan_derived_metrics*, void* aggregation_record);
int darshan_accumulator_destroy(darshan_accumulator);

/* from darshan-log-format.h */
typedef uint64_t darshan_record_id;

struct darshan_job
{
    int64_t uid;
    int64_t start_time_sec;
    int64_t start_time_nsec;
    int64_t end_time_sec;
    int64_t end_time_nsec;
    int64_t nprocs;
    int64_t jobid;
    char metadata[1024];
};

struct darshan_base_record
{
    darshan_record_id id;
    int64_t rank;
};

struct darshan_name_record
{
    darshan_record_id id;
    char *name;
};

struct darshan_posix_file
{
    struct darshan_base_record base_rec;
    int64_t counters[69];
    double fcounters[17];
};

struct darshan_dfs_file
{
    struct darshan_base_record base_rec;
    int64_t counters[52];
    double fcounters[15];
    unsigned char pool_uuid[16];
    unsigned char cont_uuid[16];
};

struct darshan_daos_object
{
    struct darshan_base_record base_rec;
    int64_t counters[64];
    double fcounters[15];
    unsigned char pool_uuid[16];
    unsigned char cont_uuid[16];
    uint64_t oid_hi;
    uint64_t oid_lo;
};

struct darshan_stdio_file
{
    struct darshan_base_record base_rec;
    int64_t counters[14];
    double fcounters[15];
};

struct darshan_mpiio_file
{
    struct darshan_base_record base_rec;
    int64_t counters[51];
    double fcounters[17];
};

struct darshan_hdf5_file
{
    struct darshan_base_record base_rec;
    int64_t counters[3];
    double fcounters[5];
};

struct darshan_hdf5_dataset
{
    struct darshan_base_record base_rec;
    uint64_t file_rec_id;
    int64_t counters[94];
    double fcounters[17];
};

struct darshan_pnetcdf_file
{
    struct darshan_base_record base_rec;
    int64_t counters[9];
    double fcounters[8];
};

struct darshan_pnetcdf_var
{
    struct darshan_base_record base_rec;
    uint64_t file_rec_id;
    int64_t counters[120];
    double fcounters[17];
};

struct darshan_bgq_record
{
    struct darshan_base_record base_rec;
    int64_t counters[11];
    double fcounters[1];
};

struct darshan_lustre_component
{
    int64_t counters[7];
    char pool_name[16];
};

struct darshan_lustre_record
{
    struct darshan_base_record base_rec;
    int64_t num_comps;
    int64_t num_stripes;
    struct darshan_lustre_component *comps;
    int64_t *ost_ids;
};

struct darshan_heatmap_record
{
    struct darshan_base_record base_rec;
    double  bin_width_seconds; /* time duration of each bin */
    int64_t nbins;             /* number of bins */
    int64_t *write_bins;       /* pointer to write bin array (trails struct in log */
    int64_t *read_bins;        /* pointer to read bin array (trails write bin array in log */
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

/* counter names */
extern char *bgq_counter_names[];
extern char *bgq_f_counter_names[];
extern char *h5d_counter_names[];
extern char *h5d_f_counter_names[];
extern char *h5f_counter_names[];
extern char *h5f_f_counter_names[];
extern char *lustre_comp_counter_names[];
extern char *mpiio_counter_names[];
extern char *mpiio_f_counter_names[];
extern char *pnetcdf_file_counter_names[];
extern char *pnetcdf_file_f_counter_names[];
extern char *pnetcdf_var_counter_names[];
extern char *pnetcdf_var_f_counter_names[];
extern char *posix_counter_names[];
extern char *posix_f_counter_names[];
extern char *dfs_counter_names[];
extern char *dfs_f_counter_names[];
extern char *daos_counter_names[];
extern char *daos_f_counter_names[];
extern char *stdio_counter_names[];
extern char *stdio_f_counter_names[];

/* Supported Functions */
void* darshan_log_open(char *);
int darshan_log_get_job(void *, struct darshan_job *);
void darshan_log_close(void*);
int darshan_log_get_exe(void*, char *);
int darshan_log_get_mounts(void*, struct darshan_mnt_info **, int*);
void darshan_log_get_modules(void*, struct darshan_mod_info **, int*);
int darshan_log_get_record(void*, int, void **);
char* darshan_log_get_lib_version(void);
int darshan_log_get_job_runtime(void *, struct darshan_job job, double *runtime);
void darshan_free(void *);

int darshan_log_get_namehash(void*, struct darshan_name_record_ref **hash);

void darshan_log_get_name_records(void*, struct darshan_name_record **, int*);
void darshan_log_get_filtered_name_records(void*, struct darshan_name_record **, int*, darshan_record_id*, int);

"""



def load_darshan_header(addins=''):
    """
    Returns a CFFI compatible header for darshan-utlil as a string.

    :return: String with a CFFI compatible header for darshan-util.
    """
    return header + addins
