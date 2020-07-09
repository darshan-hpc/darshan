# 1 "include/darshan-logutils.h"
# 1 "<built-in>"
# 1 "<command-line>"
# 31 "<command-line>"
# 1 "/usr/include/stdc-predef.h" 1 3 4
# 32 "<command-line>" 2
# 1 "include/darshan-logutils.h"
# 15 "include/darshan-logutils.h"
struct UT_hash_handle;

# 1 "include/darshan-log-format.h" 1
# 39 "include/darshan-log-format.h"
enum darshan_comp_type
{
    DARSHAN_ZLIB_COMP,
    DARSHAN_BZIP2_COMP,
    DARSHAN_NO_COMP,
};

typedef uint64_t darshan_record_id;






struct darshan_log_map
{
    uint64_t off;
    uint64_t len;
};




struct darshan_header
{
    char version_string[8];
    int64_t magic_nr;
    unsigned char comp_type;
    uint32_t partial_flag;
    struct darshan_log_map name_map;
    struct darshan_log_map mod_map[16];
    uint32_t mod_ver[16];
};



struct darshan_job
{
    int64_t uid;
    int64_t start_time;
    int64_t end_time;
    int64_t nprocs;
    int64_t jobid;
    char metadata[1024];
};


struct darshan_name_record
{
    darshan_record_id id;
    char name[1];
};


struct darshan_base_record
{
    darshan_record_id id;
    int64_t rank;
};






# 1 "include/darshan-null-log-format.h" 1
# 31 "include/darshan-null-log-format.h"
enum darshan_null_indices
{
    NULL_FOOS, NULL_FOO_MAX_DAT, NULL_NUM_INDICES,
};


enum darshan_null_f_indices
{
    NULL_F_FOO_TIMESTAMP, NULL_F_FOO_MAX_DURATION, NULL_F_NUM_INDICES,
};
# 51 "include/darshan-null-log-format.h"
struct darshan_null_record
{
    struct darshan_base_record base_rec;
    int64_t counters[NULL_NUM_INDICES];
    double fcounters[NULL_F_NUM_INDICES];
};
# 105 "include/darshan-log-format.h" 2
# 1 "include/darshan-posix-log-format.h" 1
# 148 "include/darshan-posix-log-format.h"
enum darshan_posix_indices
{
    POSIX_OPENS, POSIX_READS, POSIX_WRITES, POSIX_SEEKS, POSIX_STATS, POSIX_MMAPS, POSIX_FSYNCS, POSIX_FDSYNCS, POSIX_MODE, POSIX_BYTES_READ, POSIX_BYTES_WRITTEN, POSIX_MAX_BYTE_READ, POSIX_MAX_BYTE_WRITTEN, POSIX_CONSEC_READS, POSIX_CONSEC_WRITES, POSIX_SEQ_READS, POSIX_SEQ_WRITES, POSIX_RW_SWITCHES, POSIX_MEM_NOT_ALIGNED, POSIX_MEM_ALIGNMENT, POSIX_FILE_NOT_ALIGNED, POSIX_FILE_ALIGNMENT, POSIX_MAX_READ_TIME_SIZE, POSIX_MAX_WRITE_TIME_SIZE, POSIX_SIZE_READ_0_100, POSIX_SIZE_READ_100_1K, POSIX_SIZE_READ_1K_10K, POSIX_SIZE_READ_10K_100K, POSIX_SIZE_READ_100K_1M, POSIX_SIZE_READ_1M_4M, POSIX_SIZE_READ_4M_10M, POSIX_SIZE_READ_10M_100M, POSIX_SIZE_READ_100M_1G, POSIX_SIZE_READ_1G_PLUS, POSIX_SIZE_WRITE_0_100, POSIX_SIZE_WRITE_100_1K, POSIX_SIZE_WRITE_1K_10K, POSIX_SIZE_WRITE_10K_100K, POSIX_SIZE_WRITE_100K_1M, POSIX_SIZE_WRITE_1M_4M, POSIX_SIZE_WRITE_4M_10M, POSIX_SIZE_WRITE_10M_100M, POSIX_SIZE_WRITE_100M_1G, POSIX_SIZE_WRITE_1G_PLUS, POSIX_STRIDE1_STRIDE, POSIX_STRIDE2_STRIDE, POSIX_STRIDE3_STRIDE, POSIX_STRIDE4_STRIDE, POSIX_STRIDE1_COUNT, POSIX_STRIDE2_COUNT, POSIX_STRIDE3_COUNT, POSIX_STRIDE4_COUNT, POSIX_ACCESS1_ACCESS, POSIX_ACCESS2_ACCESS, POSIX_ACCESS3_ACCESS, POSIX_ACCESS4_ACCESS, POSIX_ACCESS1_COUNT, POSIX_ACCESS2_COUNT, POSIX_ACCESS3_COUNT, POSIX_ACCESS4_COUNT, POSIX_FASTEST_RANK, POSIX_FASTEST_RANK_BYTES, POSIX_SLOWEST_RANK, POSIX_SLOWEST_RANK_BYTES, POSIX_NUM_INDICES,
};


enum darshan_posix_f_indices
{
    POSIX_F_OPEN_START_TIMESTAMP, POSIX_F_READ_START_TIMESTAMP, POSIX_F_WRITE_START_TIMESTAMP, POSIX_F_CLOSE_START_TIMESTAMP, POSIX_F_OPEN_END_TIMESTAMP, POSIX_F_READ_END_TIMESTAMP, POSIX_F_WRITE_END_TIMESTAMP, POSIX_F_CLOSE_END_TIMESTAMP, POSIX_F_READ_TIME, POSIX_F_WRITE_TIME, POSIX_F_META_TIME, POSIX_F_MAX_READ_TIME, POSIX_F_MAX_WRITE_TIME, POSIX_F_FASTEST_RANK_TIME, POSIX_F_SLOWEST_RANK_TIME, POSIX_F_VARIANCE_RANK_TIME, POSIX_F_VARIANCE_RANK_BYTES, POSIX_F_NUM_INDICES,
};
# 167 "include/darshan-posix-log-format.h"
struct darshan_posix_file
{
    struct darshan_base_record base_rec;
    int64_t counters[POSIX_NUM_INDICES];
    double fcounters[POSIX_F_NUM_INDICES];
};
# 106 "include/darshan-log-format.h" 2
# 1 "include/darshan-mpiio-log-format.h" 1
# 128 "include/darshan-mpiio-log-format.h"
enum darshan_mpiio_indices
{
    MPIIO_INDEP_OPENS, MPIIO_COLL_OPENS, MPIIO_INDEP_READS, MPIIO_INDEP_WRITES, MPIIO_COLL_READS, MPIIO_COLL_WRITES, MPIIO_SPLIT_READS, MPIIO_SPLIT_WRITES, MPIIO_NB_READS, MPIIO_NB_WRITES, MPIIO_SYNCS, MPIIO_HINTS, MPIIO_VIEWS, MPIIO_MODE, MPIIO_BYTES_READ, MPIIO_BYTES_WRITTEN, MPIIO_RW_SWITCHES, MPIIO_MAX_READ_TIME_SIZE, MPIIO_MAX_WRITE_TIME_SIZE, MPIIO_SIZE_READ_AGG_0_100, MPIIO_SIZE_READ_AGG_100_1K, MPIIO_SIZE_READ_AGG_1K_10K, MPIIO_SIZE_READ_AGG_10K_100K, MPIIO_SIZE_READ_AGG_100K_1M, MPIIO_SIZE_READ_AGG_1M_4M, MPIIO_SIZE_READ_AGG_4M_10M, MPIIO_SIZE_READ_AGG_10M_100M, MPIIO_SIZE_READ_AGG_100M_1G, MPIIO_SIZE_READ_AGG_1G_PLUS, MPIIO_SIZE_WRITE_AGG_0_100, MPIIO_SIZE_WRITE_AGG_100_1K, MPIIO_SIZE_WRITE_AGG_1K_10K, MPIIO_SIZE_WRITE_AGG_10K_100K, MPIIO_SIZE_WRITE_AGG_100K_1M, MPIIO_SIZE_WRITE_AGG_1M_4M, MPIIO_SIZE_WRITE_AGG_4M_10M, MPIIO_SIZE_WRITE_AGG_10M_100M, MPIIO_SIZE_WRITE_AGG_100M_1G, MPIIO_SIZE_WRITE_AGG_1G_PLUS, MPIIO_ACCESS1_ACCESS, MPIIO_ACCESS2_ACCESS, MPIIO_ACCESS3_ACCESS, MPIIO_ACCESS4_ACCESS, MPIIO_ACCESS1_COUNT, MPIIO_ACCESS2_COUNT, MPIIO_ACCESS3_COUNT, MPIIO_ACCESS4_COUNT, MPIIO_FASTEST_RANK, MPIIO_FASTEST_RANK_BYTES, MPIIO_SLOWEST_RANK, MPIIO_SLOWEST_RANK_BYTES, MPIIO_NUM_INDICES,
};


enum darshan_mpiio_f_indices
{
    MPIIO_F_OPEN_TIMESTAMP, MPIIO_F_READ_START_TIMESTAMP, MPIIO_F_WRITE_START_TIMESTAMP, MPIIO_F_READ_END_TIMESTAMP, MPIIO_F_WRITE_END_TIMESTAMP, MPIIO_F_CLOSE_TIMESTAMP, MPIIO_F_READ_TIME, MPIIO_F_WRITE_TIME, MPIIO_F_META_TIME, MPIIO_F_MAX_READ_TIME, MPIIO_F_MAX_WRITE_TIME, MPIIO_F_FASTEST_RANK_TIME, MPIIO_F_SLOWEST_RANK_TIME, MPIIO_F_VARIANCE_RANK_TIME, MPIIO_F_VARIANCE_RANK_BYTES, MPIIO_F_NUM_INDICES,
};
# 147 "include/darshan-mpiio-log-format.h"
struct darshan_mpiio_file
{
    struct darshan_base_record base_rec;
    int64_t counters[MPIIO_NUM_INDICES];
    double fcounters[MPIIO_F_NUM_INDICES];
};
# 107 "include/darshan-log-format.h" 2
# 1 "include/darshan-hdf5-log-format.h" 1
# 29 "include/darshan-hdf5-log-format.h"
enum darshan_hdf5_indices
{
    HDF5_OPENS, HDF5_NUM_INDICES,
};


enum darshan_hdf5_f_indices
{
    HDF5_F_OPEN_TIMESTAMP, HDF5_F_CLOSE_TIMESTAMP, HDF5_F_NUM_INDICES,
};
# 48 "include/darshan-hdf5-log-format.h"
struct darshan_hdf5_file
{
    struct darshan_base_record base_rec;
    int64_t counters[HDF5_NUM_INDICES];
    double fcounters[HDF5_F_NUM_INDICES];
};
# 108 "include/darshan-log-format.h" 2
# 1 "include/darshan-pnetcdf-log-format.h" 1
# 31 "include/darshan-pnetcdf-log-format.h"
enum darshan_pnetcdf_indices
{
    PNETCDF_INDEP_OPENS, PNETCDF_COLL_OPENS, PNETCDF_NUM_INDICES,
};


enum darshan_pnetcdf_f_indices
{
    PNETCDF_F_OPEN_TIMESTAMP, PNETCDF_F_CLOSE_TIMESTAMP, PNETCDF_F_NUM_INDICES,
};
# 50 "include/darshan-pnetcdf-log-format.h"
struct darshan_pnetcdf_file
{
    struct darshan_base_record base_rec;
    int64_t counters[PNETCDF_NUM_INDICES];
    double fcounters[PNETCDF_F_NUM_INDICES];
};
# 109 "include/darshan-log-format.h" 2
# 1 "include/darshan-bgq-log-format.h" 1
# 47 "include/darshan-bgq-log-format.h"
enum darshan_bgq_indices
{
    BGQ_CSJOBID, BGQ_NNODES, BGQ_RANKSPERNODE, BGQ_DDRPERNODE, BGQ_INODES, BGQ_ANODES, BGQ_BNODES, BGQ_CNODES, BGQ_DNODES, BGQ_ENODES, BGQ_TORUSENABLED, BGQ_NUM_INDICES,
};


enum darshan_bgq_f_indices
{
    BGQ_F_TIMESTAMP, BGQ_F_NUM_INDICES,
};
# 67 "include/darshan-bgq-log-format.h"
struct darshan_bgq_record
{
    struct darshan_base_record base_rec;
    int64_t counters[BGQ_NUM_INDICES];
    double fcounters[BGQ_F_NUM_INDICES];
};
# 110 "include/darshan-log-format.h" 2
# 1 "include/darshan-lustre-log-format.h" 1
# 13 "include/darshan-lustre-log-format.h"
typedef int64_t OST_ID;
# 34 "include/darshan-lustre-log-format.h"
enum darshan_lustre_indices
{
    LUSTRE_OSTS, LUSTRE_MDTS, LUSTRE_STRIPE_OFFSET, LUSTRE_STRIPE_SIZE, LUSTRE_STRIPE_WIDTH, LUSTRE_NUM_INDICES,
};
# 46 "include/darshan-lustre-log-format.h"
struct darshan_lustre_record
{
    struct darshan_base_record base_rec;
    int64_t counters[LUSTRE_NUM_INDICES];
    OST_ID ost_ids[1];
};
# 111 "include/darshan-log-format.h" 2
# 1 "include/darshan-stdio-log-format.h" 1
# 75 "include/darshan-stdio-log-format.h"
enum darshan_stdio_indices
{
    STDIO_OPENS, STDIO_READS, STDIO_WRITES, STDIO_SEEKS, STDIO_FLUSHES, STDIO_BYTES_WRITTEN, STDIO_BYTES_READ, STDIO_MAX_BYTE_READ, STDIO_MAX_BYTE_WRITTEN, STDIO_FASTEST_RANK, STDIO_FASTEST_RANK_BYTES, STDIO_SLOWEST_RANK, STDIO_SLOWEST_RANK_BYTES, STDIO_NUM_INDICES,
};


enum darshan_stdio_f_indices
{
    STDIO_F_META_TIME, STDIO_F_WRITE_TIME, STDIO_F_READ_TIME, STDIO_F_OPEN_START_TIMESTAMP, STDIO_F_CLOSE_START_TIMESTAMP, STDIO_F_WRITE_START_TIMESTAMP, STDIO_F_READ_START_TIMESTAMP, STDIO_F_OPEN_END_TIMESTAMP, STDIO_F_CLOSE_END_TIMESTAMP, STDIO_F_WRITE_END_TIMESTAMP, STDIO_F_READ_END_TIMESTAMP, STDIO_F_FASTEST_RANK_TIME, STDIO_F_SLOWEST_RANK_TIME, STDIO_F_VARIANCE_RANK_TIME, STDIO_F_VARIANCE_RANK_BYTES, STDIO_F_NUM_INDICES,
};
# 95 "include/darshan-stdio-log-format.h"
struct darshan_stdio_file
{
    struct darshan_base_record base_rec;
    int64_t counters[STDIO_NUM_INDICES];
    double fcounters[STDIO_F_NUM_INDICES];
};
# 112 "include/darshan-log-format.h" 2

# 1 "include/darshan-dxt-log-format.h" 1
# 19 "include/darshan-dxt-log-format.h"
typedef struct segment_info {
    int64_t offset;
    int64_t length;
    double start_time;
    double end_time;
} segment_info;
# 36 "include/darshan-dxt-log-format.h"
struct dxt_file_record {
    struct darshan_base_record base_rec;
    int64_t shared_record;
    char hostname[64];

    int64_t write_count;
    int64_t read_count;
};
# 114 "include/darshan-log-format.h" 2
# 143 "include/darshan-log-format.h"
typedef enum
{
    DARSHAN_NULL_MOD, DARSHAN_POSIX_MOD, DARSHAN_MPIIO_MOD, DARSHAN_HDF5_MOD, DARSHAN_PNETCDF_MOD, DARSHAN_BGQ_MOD, DARSHAN_LUSTRE_MOD, DARSHAN_STDIO_MOD, DXT_POSIX_MOD, DXT_MPIIO_MOD,
} darshan_module_id;




static char * const darshan_module_names[] =
{
    "NULL", "POSIX", "MPI-IO", "HDF5", "PNETCDF", "BG/Q", "LUSTRE", "STDIO", "DXT_POSIX", "DXT_MPIIO",
};




static const int darshan_module_versions[] =
{
    1, 3, 2, 1, 1, 2, 1, 1, 1, 1,
};
# 18 "include/darshan-logutils.h" 2






struct darshan_fd_int_state;


struct darshan_fd_s
{

    char version[8];



    int swap_flag;

    int partial_flag;

    enum darshan_comp_type comp_type;

    struct darshan_log_map job_map;
    struct darshan_log_map name_map;
    struct darshan_log_map mod_map[16];

    uint32_t mod_ver[16];


    struct darshan_fd_int_state *state;
};


typedef struct darshan_fd_s* darshan_fd;

/*
struct darshan_name_record_ref
{
    struct darshan_name_record *name_record;
    UT_hash_handle hlink;
};

struct lustre_record_ref
{
 struct darshan_lustre_record *rec;
 UT_hash_handle hlink;
};
*/

/*
struct darshan_mnt_info
{
    char mnt_type[(4096 - sizeof(struct darshan_job) - 1)];
    char mnt_path[(4096 - sizeof(struct darshan_job) - 1)];
};
*/



struct darshan_mod_logutil_funcs
{







    int (*log_get_record)(
        darshan_fd fd,
        void** buf
    );





    int (*log_put_record)(
        darshan_fd fd,
        void *buf
    );






    void (*log_print_record)(
        void *rec,
        char *file_name,
        char *mnt_pt,
        char *fs_type
    );



    void (*log_print_description)(
        int ver);

    void (*log_print_diff)(
        void *rec1,
        char *name1,
        void *rec2,
        char *name2
    );

    void (*log_agg_records)(
        void *rec,
        void *agg_rec,
        int init_flag
    );
};

extern struct darshan_mod_logutil_funcs *mod_logutils[];

# 1 "include/darshan-posix-logutils.h" 1
# 10 "include/darshan-posix-logutils.h"
extern char *posix_counter_names[];
extern char *posix_f_counter_names[];

extern struct darshan_mod_logutil_funcs posix_logutils;
# 130 "include/darshan-logutils.h" 2
# 1 "include/darshan-mpiio-logutils.h" 1
# 10 "include/darshan-mpiio-logutils.h"
extern char *mpiio_counter_names[];
extern char *mpiio_f_counter_names[];

extern struct darshan_mod_logutil_funcs mpiio_logutils;
# 131 "include/darshan-logutils.h" 2
# 1 "include/darshan-hdf5-logutils.h" 1
# 10 "include/darshan-hdf5-logutils.h"
extern char *hdf5_counter_names[];
extern char *hdf5_f_counter_names[];

extern struct darshan_mod_logutil_funcs hdf5_logutils;
# 132 "include/darshan-logutils.h" 2
# 1 "include/darshan-pnetcdf-logutils.h" 1
# 10 "include/darshan-pnetcdf-logutils.h"
extern char *pnetcdf_counter_names[];
extern char *pnetcdf_f_counter_names[];

extern struct darshan_mod_logutil_funcs pnetcdf_logutils;
# 133 "include/darshan-logutils.h" 2
# 1 "include/darshan-bgq-logutils.h" 1
# 10 "include/darshan-bgq-logutils.h"
extern char *bgq_counter_names[];
extern char *bgq_f_counter_names[];

extern struct darshan_mod_logutil_funcs bgq_logutils;
# 134 "include/darshan-logutils.h" 2
# 1 "include/darshan-lustre-logutils.h" 1
# 10 "include/darshan-lustre-logutils.h"
extern char *lustre_counter_names[];

extern struct darshan_mod_logutil_funcs lustre_logutils;
# 135 "include/darshan-logutils.h" 2
# 1 "include/darshan-stdio-logutils.h" 1
# 13 "include/darshan-stdio-logutils.h"
extern char *stdio_counter_names[];
extern char *stdio_f_counter_names[];

extern struct darshan_mod_logutil_funcs stdio_logutils;
# 136 "include/darshan-logutils.h" 2


# 1 "include/darshan-dxt-logutils.h" 1
# 11 "include/darshan-dxt-logutils.h"
extern struct darshan_mod_logutil_funcs dxt_posix_logutils;
extern struct darshan_mod_logutil_funcs dxt_mpiio_logutils;

void dxt_log_print_posix_file(void *file_rec, char *file_name,
        char *mnt_pt, char *fs_type, struct lustre_record_ref *rec_ref);
void dxt_log_print_mpiio_file(void *file_rec,
        char *file_name, char *mnt_pt, char *fs_type);
# 139 "include/darshan-logutils.h" 2

darshan_fd darshan_log_open(const char *name);
darshan_fd darshan_log_create(const char *name, enum darshan_comp_type comp_type,
    int partial_flag);
int darshan_log_get_job(darshan_fd fd, struct darshan_job *job);
int darshan_log_put_job(darshan_fd fd, struct darshan_job *job);
int darshan_log_get_exe(darshan_fd fd, char *buf);
int darshan_log_put_exe(darshan_fd fd, char *buf);
int darshan_log_get_mounts(darshan_fd fd, struct darshan_mnt_info **mnt_data_array,
    int* count);
int darshan_log_put_mounts(darshan_fd fd, struct darshan_mnt_info *mnt_data_array,
    int count);
int darshan_log_get_namehash(darshan_fd fd, struct darshan_name_record_ref **hash);
int darshan_log_put_namehash(darshan_fd fd, struct darshan_name_record_ref *hash);
int darshan_log_get_mod(darshan_fd fd, darshan_module_id mod_id,
    void *mod_buf, int mod_buf_sz);
int darshan_log_put_mod(darshan_fd fd, darshan_module_id mod_id,
    void *mod_buf, int mod_buf_sz, int ver);
void darshan_log_close(darshan_fd file);
void darshan_log_print_version_warnings(const char *version_string);

