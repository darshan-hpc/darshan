/*
 * Copyright (C) 2015 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

#define _XOPEN_SOURCE 500
#define _GNU_SOURCE

#include "darshan-runtime-config.h"
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdarg.h>
#include <string.h>
#include <time.h>
#include <stdlib.h>
#include <errno.h>
#include <search.h>
#include <assert.h>
#include <pthread.h>

#include "darshan.h"
#include "darshan-dynamic.h"
#include "darshan-dxt.h"

DARSHAN_FORWARD_DECL(PMPI_File_close, int, (MPI_File *fh));
DARSHAN_FORWARD_DECL(PMPI_File_iread_at, int, (MPI_File fh, MPI_Offset offset, void *buf, int count, MPI_Datatype datatype, __D_MPI_REQUEST *request));
DARSHAN_FORWARD_DECL(PMPI_File_iread, int, (MPI_File fh, void  *buf, int  count, MPI_Datatype  datatype, __D_MPI_REQUEST  *request));
DARSHAN_FORWARD_DECL(PMPI_File_iread_shared, int, (MPI_File fh, void *buf, int count, MPI_Datatype datatype, __D_MPI_REQUEST *request));
#ifdef HAVE_MPIIO_CONST
DARSHAN_FORWARD_DECL(PMPI_File_iwrite_at, int, (MPI_File fh, MPI_Offset offset, const void *buf, int count, MPI_Datatype datatype, __D_MPI_REQUEST *request));
#else
DARSHAN_FORWARD_DECL(PMPI_File_iwrite_at, int, (MPI_File fh, MPI_Offset offset, void *buf, int count, MPI_Datatype datatype, __D_MPI_REQUEST *request));
#endif
#ifdef HAVE_MPIIO_CONST
DARSHAN_FORWARD_DECL(PMPI_File_iwrite, int, (MPI_File fh, const void *buf, int count, MPI_Datatype datatype, __D_MPI_REQUEST *request));
#else
DARSHAN_FORWARD_DECL(PMPI_File_iwrite, int, (MPI_File fh, void *buf, int count, MPI_Datatype datatype, __D_MPI_REQUEST *request));
#endif
#ifdef HAVE_MPIIO_CONST
DARSHAN_FORWARD_DECL(PMPI_File_iwrite_shared, int, (MPI_File fh, const void *buf, int count, MPI_Datatype datatype, __D_MPI_REQUEST *request));
#else
DARSHAN_FORWARD_DECL(PMPI_File_iwrite_shared, int, (MPI_File fh, void *buf, int count, MPI_Datatype datatype, __D_MPI_REQUEST *request));
#endif
#ifdef HAVE_MPIIO_CONST
DARSHAN_FORWARD_DECL(PMPI_File_open, int, (MPI_Comm comm, const char *filename, int amode, MPI_Info info, MPI_File *fh));
#else
DARSHAN_FORWARD_DECL(PMPI_File_open, int, (MPI_Comm comm, char *filename, int amode, MPI_Info info, MPI_File *fh));
#endif
DARSHAN_FORWARD_DECL(PMPI_File_read_all_begin, int, (MPI_File fh, void *buf, int count, MPI_Datatype datatype));
DARSHAN_FORWARD_DECL(PMPI_File_read_all, int, (MPI_File fh, void *buf, int count, MPI_Datatype datatype, MPI_Status *status));
DARSHAN_FORWARD_DECL(PMPI_File_read_at_all, int, (MPI_File fh, MPI_Offset offset, void *buf, int count, MPI_Datatype datatype, MPI_Status *status));
DARSHAN_FORWARD_DECL(PMPI_File_read_at_all_begin, int, (MPI_File fh, MPI_Offset offset, void *buf, int count, MPI_Datatype datatype));
DARSHAN_FORWARD_DECL(PMPI_File_read_at, int, (MPI_File fh, MPI_Offset offset, void *buf, int count, MPI_Datatype datatype, MPI_Status *status));
DARSHAN_FORWARD_DECL(PMPI_File_read, int, (MPI_File fh, void *buf, int count, MPI_Datatype datatype, MPI_Status *status));
DARSHAN_FORWARD_DECL(PMPI_File_read_ordered_begin, int, (MPI_File fh, void *buf, int count, MPI_Datatype datatype));
DARSHAN_FORWARD_DECL(PMPI_File_read_ordered, int, (MPI_File fh, void *buf, int count, MPI_Datatype datatype, MPI_Status *status));
DARSHAN_FORWARD_DECL(PMPI_File_read_shared, int, (MPI_File fh, void *buf, int count, MPI_Datatype datatype, MPI_Status *status));
#ifdef HAVE_MPIIO_CONST
DARSHAN_FORWARD_DECL(PMPI_File_set_view, int, (MPI_File fh, MPI_Offset disp, MPI_Datatype etype, MPI_Datatype filetype, const char *datarep, MPI_Info info));
#else
DARSHAN_FORWARD_DECL(PMPI_File_set_view, int, (MPI_File fh, MPI_Offset disp, MPI_Datatype etype, MPI_Datatype filetype, char *datarep, MPI_Info info));
#endif
DARSHAN_FORWARD_DECL(PMPI_File_sync, int, (MPI_File fh));
#ifdef HAVE_MPIIO_CONST
DARSHAN_FORWARD_DECL(PMPI_File_write_all_begin, int, (MPI_File fh, const void *buf, int count, MPI_Datatype datatype));
#else
DARSHAN_FORWARD_DECL(PMPI_File_write_all_begin, int, (MPI_File fh, void *buf, int count, MPI_Datatype datatype));
#endif
#ifdef HAVE_MPIIO_CONST
DARSHAN_FORWARD_DECL(PMPI_File_write_all, int, (MPI_File fh, const void *buf, int count, MPI_Datatype datatype, MPI_Status *status));
#else
DARSHAN_FORWARD_DECL(PMPI_File_write_all, int, (MPI_File fh, void *buf, int count, MPI_Datatype datatype, MPI_Status *status));
#endif
#ifdef HAVE_MPIIO_CONST
DARSHAN_FORWARD_DECL(PMPI_File_write_at_all_begin, int, (MPI_File fh, MPI_Offset offset, const void *buf, int count, MPI_Datatype datatype));
#else
DARSHAN_FORWARD_DECL(PMPI_File_write_at_all_begin, int, (MPI_File fh, MPI_Offset offset, void *buf, int count, MPI_Datatype datatype));
#endif
#ifdef HAVE_MPIIO_CONST
DARSHAN_FORWARD_DECL(PMPI_File_write_at_all, int, (MPI_File fh, MPI_Offset offset, const void *buf, int count, MPI_Datatype datatype, MPI_Status *status));
#else
DARSHAN_FORWARD_DECL(PMPI_File_write_at_all, int, (MPI_File fh, MPI_Offset offset, void *buf, int count, MPI_Datatype datatype, MPI_Status *status));
#endif
#ifdef HAVE_MPIIO_CONST
DARSHAN_FORWARD_DECL(PMPI_File_write_at, int, (MPI_File fh, MPI_Offset offset, const void *buf, int count, MPI_Datatype datatype, MPI_Status *status));
#else
DARSHAN_FORWARD_DECL(PMPI_File_write_at, int, (MPI_File fh, MPI_Offset offset, void *buf, int count, MPI_Datatype datatype, MPI_Status *status));
#endif
#ifdef HAVE_MPIIO_CONST
DARSHAN_FORWARD_DECL(PMPI_File_write, int, (MPI_File fh, const void *buf, int count, MPI_Datatype datatype, MPI_Status *status));
#else
DARSHAN_FORWARD_DECL(PMPI_File_write, int, (MPI_File fh, void *buf, int count, MPI_Datatype datatype, MPI_Status *status));
#endif
#ifdef HAVE_MPIIO_CONST
DARSHAN_FORWARD_DECL(PMPI_File_write_ordered_begin, int, (MPI_File fh, const void *buf, int count, MPI_Datatype datatype));
#else
DARSHAN_FORWARD_DECL(PMPI_File_write_ordered_begin, int, (MPI_File fh, void *buf, int count, MPI_Datatype datatype));
#endif
#ifdef HAVE_MPIIO_CONST
DARSHAN_FORWARD_DECL(PMPI_File_write_ordered, int, (MPI_File fh, const void *buf, int count, MPI_Datatype datatype, MPI_Status *status));
#else
DARSHAN_FORWARD_DECL(PMPI_File_write_ordered, int, (MPI_File fh, void *buf, int count, MPI_Datatype datatype, MPI_Status *status));
#endif
#ifdef HAVE_MPIIO_CONST
DARSHAN_FORWARD_DECL(PMPI_File_write_shared, int, (MPI_File fh, const void *buf, int count, MPI_Datatype datatype, MPI_Status *status));
#else
DARSHAN_FORWARD_DECL(PMPI_File_write_shared, int, (MPI_File fh, void *buf, int count, MPI_Datatype datatype, MPI_Status *status));
#endif

/* The mpiio_file_record_ref structure maintains necessary runtime metadata
 * for the MPIIO file record (darshan_mpiio_file structure, defined in
 * darshan-mpiio-log-format.h) pointed to by 'file_rec'. This metadata
 * assists with the instrumenting of specific statistics in the file record.
 *
 * RATIONALE: the MPIIO module needs to track some stateful, volatile 
 * information about each open file (like the current file offset, most recent 
 * access time, etc.) to aid in instrumentation, but this information can't be
 * stored in the darshan_mpiio_file struct because we don't want it to appear in
 * the final darshan log file.  We therefore associate a mpiio_file_record_ref
 * struct with each darshan_mpiio_file struct in order to track this information
 * (i.e., the mapping between mpiio_file_record_ref structs to darshan_mpiio_file
 * structs is one-to-one).
 *
 * NOTE: we use the 'darshan_record_ref' interface (in darshan-common) to
 * associate different types of handles with this mpiio_file_record_ref struct.
 * This allows us to index this struct (and the underlying file record) by using
 * either the corresponding Darshan record identifier (derived from the filename)
 * or by a generated MPI file handle, for instance. So, while there should only
 * be a single Darshan record identifier that indexes a mpiio_file_record_ref,
 * there could be multiple open file handles that index it.
 */
struct mpiio_file_record_ref
{
    struct darshan_mpiio_file *file_rec;
    enum darshan_io_type last_io_type;
    double last_meta_end;
    double last_read_end;
    double last_write_end;
    void *access_root;
    int access_count;
};

/* The mpiio_runtime structure maintains necessary state for storing
 * MPI-IO file records and for coordinating with darshan-core at 
 * shutdown time.
 */
struct mpiio_runtime
{
    void *rec_id_hash;
    void *fh_hash;
    int file_rec_count;
};

static void mpiio_runtime_initialize(
    void);
static struct mpiio_file_record_ref *mpiio_track_new_file_record(
    darshan_record_id rec_id, const char *path);
static void mpiio_finalize_file_records(
    void *rec_ref_p, void *user_ptr);
static void mpiio_record_reduction_op(
    void* infile_v, void* inoutfile_v, int *len, MPI_Datatype *datatype);
static void mpiio_shared_record_variance(
    MPI_Comm mod_comm, struct darshan_mpiio_file *inrec_array,
    struct darshan_mpiio_file *outrec_array, int shared_rec_count);
static void mpiio_cleanup_runtime(
    void);

static void mpiio_shutdown(
    MPI_Comm mod_comm, darshan_record_id *shared_recs,
    int shared_rec_count, void **mpiio_buf, int *mpiio_buf_sz);

static struct mpiio_runtime *mpiio_runtime = NULL;
static pthread_mutex_t mpiio_runtime_mutex = PTHREAD_RECURSIVE_MUTEX_INITIALIZER_NP;
static int my_rank = -1;

#define MPIIO_LOCK() pthread_mutex_lock(&mpiio_runtime_mutex)
#define MPIIO_UNLOCK() pthread_mutex_unlock(&mpiio_runtime_mutex)

#define MPIIO_PRE_RECORD() do { \
    MPIIO_LOCK(); \
    if(!darshan_core_disabled_instrumentation()) { \
        if(!mpiio_runtime) { \
            mpiio_runtime_initialize(); \
        } \
        if(mpiio_runtime) break; \
    } \
    MPIIO_UNLOCK(); \
    return(ret); \
} while(0)

#define MPIIO_POST_RECORD() do { \
    MPIIO_UNLOCK(); \
} while(0)

#define MPIIO_RECORD_OPEN(__ret, __path, __fh, __comm, __mode, __info, __tm1, __tm2) do { \
    darshan_record_id rec_id; \
    struct mpiio_file_record_ref *rec_ref; \
    char *newpath; \
    int comm_size; \
    if(__ret != MPI_SUCCESS) break; \
    newpath = darshan_clean_file_path(__path); \
    if(!newpath) newpath = (char *)__path; \
    if(darshan_core_excluded_path(newpath)) { \
        if(newpath != __path) free(newpath); \
        break; \
    } \
    rec_id = darshan_core_gen_record_id(newpath); \
    rec_ref = darshan_lookup_record_ref(mpiio_runtime->rec_id_hash, &rec_id, sizeof(darshan_record_id)); \
    if(!rec_ref) rec_ref = mpiio_track_new_file_record(rec_id, newpath); \
    if(!rec_ref) { \
        if(newpath != __path) free(newpath); \
        break; \
    } \
    rec_ref->file_rec->counters[MPIIO_MODE] = __mode; \
    PMPI_Comm_size(__comm, &comm_size); \
    if(comm_size == 1) \
        rec_ref->file_rec->counters[MPIIO_INDEP_OPENS] += 1; \
    else \
        rec_ref->file_rec->counters[MPIIO_COLL_OPENS] += 1; \
    if(__info != MPI_INFO_NULL) \
        rec_ref->file_rec->counters[MPIIO_HINTS] += 1; \
    if(rec_ref->file_rec->fcounters[MPIIO_F_OPEN_START_TIMESTAMP] == 0 || \
     rec_ref->file_rec->fcounters[MPIIO_F_OPEN_START_TIMESTAMP] > __tm1) \
        rec_ref->file_rec->fcounters[MPIIO_F_OPEN_START_TIMESTAMP] = __tm1; \
    rec_ref->file_rec->fcounters[MPIIO_F_OPEN_END_TIMESTAMP] = __tm2; \
    DARSHAN_TIMER_INC_NO_OVERLAP(rec_ref->file_rec->fcounters[MPIIO_F_META_TIME], \
        __tm1, __tm2, rec_ref->last_meta_end); \
    darshan_add_record_ref(&(mpiio_runtime->fh_hash), &__fh, sizeof(MPI_File), rec_ref); \
    if(newpath != __path) free(newpath); \
} while(0)

#define MPIIO_RECORD_READ(__ret, __fh, __count, __datatype, __counter, __tm1, __tm2) do { \
    struct mpiio_file_record_ref *rec_ref; \
    int size = 0; \
    double __elapsed = __tm2-__tm1; \
    if(__ret != MPI_SUCCESS) break; \
    rec_ref = darshan_lookup_record_ref(mpiio_runtime->fh_hash, &(__fh), sizeof(MPI_File)); \
    if(!rec_ref) break; \
    PMPI_Type_size(__datatype, &size);  \
    size = size * __count; \
    /* DXT to record detailed read tracing information */ \
    dxt_mpiio_read(rec_ref->file_rec->base_rec.id, size, __tm1, __tm2); \
    DARSHAN_BUCKET_INC(&(rec_ref->file_rec->counters[MPIIO_SIZE_READ_AGG_0_100]), size); \
    darshan_common_val_counter(&rec_ref->access_root, &rec_ref->access_count, size, \
        &(rec_ref->file_rec->counters[MPIIO_ACCESS1_ACCESS]), \
        &(rec_ref->file_rec->counters[MPIIO_ACCESS1_COUNT])); \
    rec_ref->file_rec->counters[MPIIO_BYTES_READ] += size; \
    rec_ref->file_rec->counters[__counter] += 1; \
    if(rec_ref->last_io_type == DARSHAN_IO_WRITE) \
        rec_ref->file_rec->counters[MPIIO_RW_SWITCHES] += 1; \
    rec_ref->last_io_type = DARSHAN_IO_READ; \
    if(rec_ref->file_rec->fcounters[MPIIO_F_READ_START_TIMESTAMP] == 0 || \
     rec_ref->file_rec->fcounters[MPIIO_F_READ_START_TIMESTAMP] > __tm1) \
        rec_ref->file_rec->fcounters[MPIIO_F_READ_START_TIMESTAMP] = __tm1; \
    rec_ref->file_rec->fcounters[MPIIO_F_READ_END_TIMESTAMP] = __tm2; \
    if(rec_ref->file_rec->fcounters[MPIIO_F_MAX_READ_TIME] < __elapsed) { \
        rec_ref->file_rec->fcounters[MPIIO_F_MAX_READ_TIME] = __elapsed; \
        rec_ref->file_rec->counters[MPIIO_MAX_READ_TIME_SIZE] = size; } \
    DARSHAN_TIMER_INC_NO_OVERLAP(rec_ref->file_rec->fcounters[MPIIO_F_READ_TIME], \
        __tm1, __tm2, rec_ref->last_read_end); \
} while(0)

#define MPIIO_RECORD_WRITE(__ret, __fh, __count, __datatype, __counter, __tm1, __tm2) do { \
    struct mpiio_file_record_ref *rec_ref; \
    int size = 0; \
    double __elapsed = __tm2-__tm1; \
    if(__ret != MPI_SUCCESS) break; \
    rec_ref = darshan_lookup_record_ref(mpiio_runtime->fh_hash, &(__fh), sizeof(MPI_File)); \
    if(!rec_ref) break; \
    PMPI_Type_size(__datatype, &size);  \
    size = size * __count; \
    /* DXT to record detailed write tracing information */ \
    dxt_mpiio_write(rec_ref->file_rec->base_rec.id, size, __tm1, __tm2); \
    DARSHAN_BUCKET_INC(&(rec_ref->file_rec->counters[MPIIO_SIZE_WRITE_AGG_0_100]), size); \
    darshan_common_val_counter(&rec_ref->access_root, &rec_ref->access_count, size, \
        &(rec_ref->file_rec->counters[MPIIO_ACCESS1_ACCESS]), \
        &(rec_ref->file_rec->counters[MPIIO_ACCESS1_COUNT])); \
    rec_ref->file_rec->counters[MPIIO_BYTES_WRITTEN] += size; \
    rec_ref->file_rec->counters[__counter] += 1; \
    if(rec_ref->last_io_type == DARSHAN_IO_READ) \
        rec_ref->file_rec->counters[MPIIO_RW_SWITCHES] += 1; \
    rec_ref->last_io_type = DARSHAN_IO_WRITE; \
    if(rec_ref->file_rec->fcounters[MPIIO_F_WRITE_START_TIMESTAMP] == 0 || \
     rec_ref->file_rec->fcounters[MPIIO_F_WRITE_START_TIMESTAMP] > __tm1) \
        rec_ref->file_rec->fcounters[MPIIO_F_WRITE_START_TIMESTAMP] = __tm1; \
    rec_ref->file_rec->fcounters[MPIIO_F_WRITE_END_TIMESTAMP] = __tm2; \
    if(rec_ref->file_rec->fcounters[MPIIO_F_MAX_WRITE_TIME] < __elapsed) { \
        rec_ref->file_rec->fcounters[MPIIO_F_MAX_WRITE_TIME] = __elapsed; \
        rec_ref->file_rec->counters[MPIIO_MAX_WRITE_TIME_SIZE] = size; } \
    DARSHAN_TIMER_INC_NO_OVERLAP(rec_ref->file_rec->fcounters[MPIIO_F_WRITE_TIME], \
        __tm1, __tm2, rec_ref->last_write_end); \
} while(0)

/**********************************************************
 *        Wrappers for MPI-IO functions of interest       * 
 **********************************************************/

#ifdef HAVE_MPIIO_CONST
int DARSHAN_DECL(MPI_File_open)(MPI_Comm comm, const char *filename, int amode, MPI_Info info, MPI_File *fh) 
#else
int DARSHAN_DECL(MPI_File_open)(MPI_Comm comm, char *filename, int amode, MPI_Info info, MPI_File *fh) 
#endif
{
    int ret;
    MPI_File tmp_fh;
    char* tmp;
    double tm1, tm2;

    MAP_OR_FAIL(PMPI_File_open);

    tm1 = darshan_core_wtime();
    ret = __real_PMPI_File_open(comm, filename, amode, info, fh);
    tm2 = darshan_core_wtime();

    /* use ROMIO approach to strip prefix if present */
    /* strip off prefix if there is one, but only skip prefixes
     * if they are greater than length one to allow for windows
     * drive specifications (e.g. c:\...) 
     */
    tmp = strchr(filename, ':');
    if (tmp > filename + 1) {
        filename = tmp + 1;
    }

    MPIIO_PRE_RECORD();
    tmp_fh = *fh;
    MPIIO_RECORD_OPEN(ret, filename, tmp_fh, comm, amode, info, tm1, tm2);
    MPIIO_POST_RECORD();

    return(ret);
}
#ifdef HAVE_MPIIO_CONST
DARSHAN_WRAPPER_MAP(PMPI_File_open, int,  (MPI_Comm comm, const char *filename, int amode, MPI_Info info, MPI_File *fh), MPI_File_open(comm,filename,amode,info,fh))
#else
DARSHAN_WRAPPER_MAP(PMPI_File_open, int,  (MPI_Comm comm, char *filename, int amode, MPI_Info info, MPI_File *fh), MPI_File_open(comm,filename,amode,info,fh)) 
#endif

int DARSHAN_DECL(MPI_File_read)(MPI_File fh, void *buf, int count,
    MPI_Datatype datatype, MPI_Status *status)
{
    int ret;
    double tm1, tm2;

    MAP_OR_FAIL(PMPI_File_read);

    tm1 = darshan_core_wtime();
    ret = __real_PMPI_File_read(fh, buf, count, datatype, status);
    tm2 = darshan_core_wtime();

    MPIIO_PRE_RECORD();
    MPIIO_RECORD_READ(ret, fh, count, datatype, MPIIO_INDEP_READS, tm1, tm2);
    MPIIO_POST_RECORD();

    return(ret);
}
DARSHAN_WRAPPER_MAP(PMPI_File_read, int, (MPI_File fh, void *buf, int count,
    MPI_Datatype datatype, MPI_Status *status), MPI_File_read(fh,buf,count,datatype,status))

#ifdef HAVE_MPIIO_CONST
int DARSHAN_DECL(MPI_File_write)(MPI_File fh, const void *buf, int count,
    MPI_Datatype datatype, MPI_Status *status)
#else
int DARSHAN_DECL(MPI_File_write)(MPI_File fh, void *buf, int count,
    MPI_Datatype datatype, MPI_Status *status)
#endif
{
    int ret;
    double tm1, tm2;

    MAP_OR_FAIL(PMPI_File_write);

    tm1 = darshan_core_wtime();
    ret = __real_PMPI_File_write(fh, buf, count, datatype, status);
    tm2 = darshan_core_wtime();

    MPIIO_PRE_RECORD();
    MPIIO_RECORD_WRITE(ret, fh, count, datatype, MPIIO_INDEP_WRITES, tm1, tm2);
    MPIIO_POST_RECORD();

    return(ret);
}
#ifdef HAVE_MPIIO_CONST
DARSHAN_WRAPPER_MAP(PMPI_File_write, int, (MPI_File fh, const void *buf, int count,
    MPI_Datatype datatype, MPI_Status *status), MPI_File_write(fh,buf,count,datatype,status))
#else
DARSHAN_WRAPPER_MAP(PMPI_File_write, int, (MPI_File fh, void *buf, int count,
    MPI_Datatype datatype, MPI_Status *status), MPI_File_write(fh,buf,count,datatype,status))
#endif

int DARSHAN_DECL(MPI_File_read_at)(MPI_File fh, MPI_Offset offset, void *buf,
    int count, MPI_Datatype datatype, MPI_Status *status)
{
    int ret;
    double tm1, tm2;

    MAP_OR_FAIL(PMPI_File_read_at);

    tm1 = darshan_core_wtime();
    ret = __real_PMPI_File_read_at(fh, offset, buf,
        count, datatype, status);
    tm2 = darshan_core_wtime();

    MPIIO_PRE_RECORD();
    MPIIO_RECORD_READ(ret, fh, count, datatype, MPIIO_INDEP_READS, tm1, tm2);
    MPIIO_POST_RECORD();

    return(ret);
}
DARSHAN_WRAPPER_MAP(PMPI_File_read_at, int, (MPI_File fh, MPI_Offset offset, void *buf,
    int count, MPI_Datatype datatype, MPI_Status *status), MPI_File_read_at(fh, offset, buf, count, datatype, status))

#ifdef HAVE_MPIIO_CONST
int DARSHAN_DECL(MPI_File_write_at)(MPI_File fh, MPI_Offset offset, const void *buf,
    int count, MPI_Datatype datatype, MPI_Status *status)
#else
int DARSHAN_DECL(MPI_File_write_at)(MPI_File fh, MPI_Offset offset, void *buf,
    int count, MPI_Datatype datatype, MPI_Status *status)
#endif
{
    int ret;
    double tm1, tm2;

    MAP_OR_FAIL(PMPI_File_write_at);

    tm1 = darshan_core_wtime();
    ret = __real_PMPI_File_write_at(fh, offset, buf,
        count, datatype, status);
    tm2 = darshan_core_wtime();

    MPIIO_PRE_RECORD();
    MPIIO_RECORD_WRITE(ret, fh, count, datatype, MPIIO_INDEP_WRITES, tm1, tm2);
    MPIIO_POST_RECORD();

    return(ret);
}
#ifdef HAVE_MPIIO_CONST
DARSHAN_WRAPPER_MAP(PMPI_File_write_at, int, (MPI_File fh, MPI_Offset offset, const void *buf,
    int count, MPI_Datatype datatype, MPI_Status *status), MPI_File_write_at(fh, offset, buf, count, datatype, status))
#else
DARSHAN_WRAPPER_MAP(PMPI_File_write_at, int, (MPI_File fh, MPI_Offset offset, void *buf,
    int count, MPI_Datatype datatype, MPI_Status *status), MPI_File_write_at(fh, offset, buf, count, datatype, status))
#endif

int DARSHAN_DECL(MPI_File_read_all)(MPI_File fh, void * buf, int count, MPI_Datatype datatype, MPI_Status *status)
{
    int ret;
    double tm1, tm2;

    MAP_OR_FAIL(PMPI_File_read_all);

    tm1 = darshan_core_wtime();
    ret = __real_PMPI_File_read_all(fh, buf, count,
        datatype, status);
    tm2 = darshan_core_wtime();

    MPIIO_PRE_RECORD();
    MPIIO_RECORD_READ(ret, fh, count, datatype, MPIIO_COLL_READS, tm1, tm2);
    MPIIO_POST_RECORD();

    return(ret);
}
DARSHAN_WRAPPER_MAP(PMPI_File_read_all, int, (MPI_File fh, void * buf, int count, MPI_Datatype datatype, MPI_Status *status),
        MPI_File_read_all(fh,buf,count,datatype,status))

#ifdef HAVE_MPIIO_CONST
int DARSHAN_DECL(MPI_File_write_all)(MPI_File fh, const void * buf, int count, MPI_Datatype datatype, MPI_Status *status)
#else
int DARSHAN_DECL(MPI_File_write_all)(MPI_File fh, void * buf, int count, MPI_Datatype datatype, MPI_Status *status)
#endif
{
    int ret;
    double tm1, tm2;

    MAP_OR_FAIL(PMPI_File_write_all);

    tm1 = darshan_core_wtime();
    ret = __real_PMPI_File_write_all(fh, buf, count,
        datatype, status);
    tm2 = darshan_core_wtime();

    MPIIO_PRE_RECORD();
    MPIIO_RECORD_WRITE(ret, fh, count, datatype, MPIIO_COLL_WRITES, tm1, tm2);
    MPIIO_POST_RECORD();

    return(ret);
}
#ifdef HAVE_MPIIO_CONST
DARSHAN_WRAPPER_MAP(PMPI_File_write_all, int, (MPI_File fh, const void * buf, int count, MPI_Datatype datatype, MPI_Status *status),
        MPI_File_write_all(fh, buf, count, datatype, status))
#else
DARSHAN_WRAPPER_MAP(PMPI_File_write_all, int, (MPI_File fh, void * buf, int count, MPI_Datatype datatype, MPI_Status *status),
        MPI_File_write_all(fh, buf, count, datatype, status))
#endif

int DARSHAN_DECL(MPI_File_read_at_all)(MPI_File fh, MPI_Offset offset, void * buf,
    int count, MPI_Datatype datatype, MPI_Status * status)
{
    int ret;
    double tm1, tm2;

    MAP_OR_FAIL(PMPI_File_read_at_all);

    tm1 = darshan_core_wtime();
    ret = __real_PMPI_File_read_at_all(fh, offset, buf,
        count, datatype, status);
    tm2 = darshan_core_wtime();

    MPIIO_PRE_RECORD();
    MPIIO_RECORD_READ(ret, fh, count, datatype, MPIIO_COLL_READS, tm1, tm2);
    MPIIO_POST_RECORD();

    return(ret);
}
DARSHAN_WRAPPER_MAP(PMPI_File_read_at_all, int, (MPI_File fh, MPI_Offset offset, void * buf,
    int count, MPI_Datatype datatype, MPI_Status * status),
        MPI_File_read_at_all(fh,offset,buf,count,datatype,status))

#ifdef HAVE_MPIIO_CONST
int DARSHAN_DECL(MPI_File_write_at_all)(MPI_File fh, MPI_Offset offset, const void * buf,
    int count, MPI_Datatype datatype, MPI_Status * status)
#else
int DARSHAN_DECL(MPI_File_write_at_all)(MPI_File fh, MPI_Offset offset, void * buf,
    int count, MPI_Datatype datatype, MPI_Status * status)
#endif
{
    int ret;
    double tm1, tm2;

    MAP_OR_FAIL(PMPI_File_write_at_all);

    tm1 = darshan_core_wtime();
    ret = __real_PMPI_File_write_at_all(fh, offset, buf,
        count, datatype, status);
    tm2 = darshan_core_wtime();

    MPIIO_PRE_RECORD();
    MPIIO_RECORD_WRITE(ret, fh, count, datatype, MPIIO_COLL_WRITES, tm1, tm2);
    MPIIO_POST_RECORD();

    return(ret);
}
#ifdef HAVE_MPIIO_CONST
DARSHAN_WRAPPER_MAP(PMPI_File_write_at_all, int, (MPI_File fh, MPI_Offset offset, const void * buf,
    int count, MPI_Datatype datatype, MPI_Status * status),
        MPI_File_write_at_all(fh, offset, buf, count, datatype, status))
#else
DARSHAN_WRAPPER_MAP(PMPI_File_write_at_all, int, (MPI_File fh, MPI_Offset offset, void * buf,
    int count, MPI_Datatype datatype, MPI_Status * status),
        MPI_File_write_at_all(fh, offset, buf, count, datatype, status))
#endif

int DARSHAN_DECL(MPI_File_read_shared)(MPI_File fh, void * buf, int count, MPI_Datatype datatype, MPI_Status *status)
{
    int ret;
    double tm1, tm2;

    MAP_OR_FAIL(PMPI_File_read_shared);

    tm1 = darshan_core_wtime();
    ret = __real_PMPI_File_read_shared(fh, buf, count,
        datatype, status);
    tm2 = darshan_core_wtime();

    MPIIO_PRE_RECORD();
    MPIIO_RECORD_READ(ret, fh, count, datatype, MPIIO_INDEP_READS, tm1, tm2);
    MPIIO_POST_RECORD();

    return(ret);
}
DARSHAN_WRAPPER_MAP(PMPI_File_read_shared, int, (MPI_File fh, void * buf, int count, MPI_Datatype datatype, MPI_Status *status),
        MPI_File_read_shared(fh, buf, count, datatype, status))

#ifdef HAVE_MPIIO_CONST
int DARSHAN_DECL(MPI_File_write_shared)(MPI_File fh, const void * buf, int count, MPI_Datatype datatype, MPI_Status *status)
#else
int DARSHAN_DECL(MPI_File_write_shared)(MPI_File fh, void * buf, int count, MPI_Datatype datatype, MPI_Status *status)
#endif
{
    int ret;
    double tm1, tm2;

    MAP_OR_FAIL(PMPI_File_write_shared);

    tm1 = darshan_core_wtime();
    ret = __real_PMPI_File_write_shared(fh, buf, count,
        datatype, status);
    tm2 = darshan_core_wtime();

    MPIIO_PRE_RECORD();
    MPIIO_RECORD_WRITE(ret, fh, count, datatype, MPIIO_INDEP_WRITES, tm1, tm2);
    MPIIO_POST_RECORD();

    return(ret);
}
#ifdef HAVE_MPIIO_CONST
DARSHAN_WRAPPER_MAP(PMPI_File_write_shared, int, (MPI_File fh, const void * buf, int count, MPI_Datatype datatype, MPI_Status *status),
        MPI_File_write_shared(fh, buf, count, datatype, status))
#else
DARSHAN_WRAPPER_MAP(PMPI_File_write_shared, int, (MPI_File fh, void * buf, int count, MPI_Datatype datatype, MPI_Status *status),
        MPI_File_write_shared(fh, buf, count, datatype, status))
#endif


int DARSHAN_DECL(MPI_File_read_ordered)(MPI_File fh, void * buf, int count,
    MPI_Datatype datatype, MPI_Status * status)
{
    int ret;
    double tm1, tm2;

    MAP_OR_FAIL(PMPI_File_read_ordered);

    tm1 = darshan_core_wtime();
    ret = __real_PMPI_File_read_ordered(fh, buf, count,
        datatype, status);
    tm2 = darshan_core_wtime();

    MPIIO_PRE_RECORD();
    MPIIO_RECORD_READ(ret, fh, count, datatype, MPIIO_COLL_READS, tm1, tm2);
    MPIIO_POST_RECORD();

    return(ret);
}
DARSHAN_WRAPPER_MAP(PMPI_File_read_ordered, int, (MPI_File fh, void * buf, int count,
    MPI_Datatype datatype, MPI_Status * status),
        MPI_File_read_ordered(fh, buf, count, datatype, status))

#ifdef HAVE_MPIIO_CONST
int DARSHAN_DECL(MPI_File_write_ordered)(MPI_File fh, const void * buf, int count,
    MPI_Datatype datatype, MPI_Status * status)
#else
int DARSHAN_DECL(MPI_File_write_ordered)(MPI_File fh, void * buf, int count,
    MPI_Datatype datatype, MPI_Status * status)
#endif
{
    int ret;
    double tm1, tm2;

    MAP_OR_FAIL(PMPI_File_write_ordered);

    tm1 = darshan_core_wtime();
    ret = __real_PMPI_File_write_ordered(fh, buf, count,
         datatype, status);
    tm2 = darshan_core_wtime();

    MPIIO_PRE_RECORD();
    MPIIO_RECORD_WRITE(ret, fh, count, datatype, MPIIO_COLL_WRITES, tm1, tm2);
    MPIIO_POST_RECORD();

    return(ret);
}
#ifdef HAVE_MPIIO_CONST
DARSHAN_WRAPPER_MAP(PMPI_File_write_ordered, int, (MPI_File fh, const void * buf, int count,
    MPI_Datatype datatype, MPI_Status * status),
        MPI_File_write_ordered(fh, buf, count, datatype, status))
#else
DARSHAN_WRAPPER_MAP(PMPI_File_write_ordered, int, (MPI_File fh, void * buf, int count,
    MPI_Datatype datatype, MPI_Status * status),
        MPI_File_write_ordered(fh, buf, count, datatype, status))
#endif

int DARSHAN_DECL(MPI_File_read_all_begin)(MPI_File fh, void * buf, int count, MPI_Datatype datatype)
{
    int ret;
    double tm1, tm2;

    MAP_OR_FAIL(PMPI_File_read_all_begin);

    tm1 = darshan_core_wtime();
    ret = __real_PMPI_File_read_all_begin(fh, buf, count, datatype);
    tm2 = darshan_core_wtime();

    MPIIO_PRE_RECORD();
    MPIIO_RECORD_READ(ret, fh, count, datatype, MPIIO_SPLIT_READS, tm1, tm2);
    MPIIO_POST_RECORD();

    return(ret);
}
DARSHAN_WRAPPER_MAP(PMPI_File_read_all_begin, int, (MPI_File fh, void * buf, int count, MPI_Datatype datatype),
        MPI_File_read_all_begin(fh, buf, count, datatype))

#ifdef HAVE_MPIIO_CONST
int DARSHAN_DECL(MPI_File_write_all_begin)(MPI_File fh, const void * buf, int count, MPI_Datatype datatype)
#else
int DARSHAN_DECL(MPI_File_write_all_begin)(MPI_File fh, void * buf, int count, MPI_Datatype datatype)
#endif
{
    int ret;
    double tm1, tm2;

    MAP_OR_FAIL(PMPI_File_write_all_begin);

    tm1 = darshan_core_wtime();
    ret = __real_PMPI_File_write_all_begin(fh, buf, count, datatype);
    tm2 = darshan_core_wtime();

    MPIIO_PRE_RECORD();
    MPIIO_RECORD_WRITE(ret, fh, count, datatype, MPIIO_SPLIT_WRITES, tm1, tm2);
    MPIIO_POST_RECORD();

    return(ret);
}
#ifdef HAVE_MPIIO_CONST
DARSHAN_WRAPPER_MAP(PMPI_File_write_all_begin, int, (MPI_File fh, const void * buf, int count, MPI_Datatype datatype),
        MPI_File_write_all_begin(fh, buf, count, datatype))
#else
DARSHAN_WRAPPER_MAP(PMPI_File_write_all_begin, int, (MPI_File fh, void * buf, int count, MPI_Datatype datatype),
        MPI_File_write_all_begin(fh, buf, count, datatype))
#endif

int DARSHAN_DECL(MPI_File_read_at_all_begin)(MPI_File fh, MPI_Offset offset, void * buf,
    int count, MPI_Datatype datatype)
{
    int ret;
    double tm1, tm2;

    MAP_OR_FAIL(PMPI_File_read_at_all_begin);

    tm1 = darshan_core_wtime();
    ret = __real_PMPI_File_read_at_all_begin(fh, offset, buf,
        count, datatype);
    tm2 = darshan_core_wtime();
    
    MPIIO_PRE_RECORD();
    MPIIO_RECORD_READ(ret, fh, count, datatype, MPIIO_SPLIT_READS, tm1, tm2);
    MPIIO_POST_RECORD();

    return(ret);
}
DARSHAN_WRAPPER_MAP(PMPI_File_read_at_all_begin, int, (MPI_File fh, MPI_Offset offset, void * buf,
    int count, MPI_Datatype datatype), MPI_File_read_at_all_begin(fh, offset, buf, count,
        datatype))

#ifdef HAVE_MPIIO_CONST
int DARSHAN_DECL(MPI_File_write_at_all_begin)(MPI_File fh, MPI_Offset offset, const void * buf,
    int count, MPI_Datatype datatype)
#else
int DARSHAN_DECL(MPI_File_write_at_all_begin)(MPI_File fh, MPI_Offset offset, void * buf,
    int count, MPI_Datatype datatype)
#endif
{
    int ret;
    double tm1, tm2;

    MAP_OR_FAIL(PMPI_File_write_at_all_begin);

    tm1 = darshan_core_wtime();
    ret = __real_PMPI_File_write_at_all_begin(fh, offset,
        buf, count, datatype);
    tm2 = darshan_core_wtime();

    MPIIO_PRE_RECORD();
    MPIIO_RECORD_WRITE(ret, fh, count, datatype, MPIIO_SPLIT_WRITES, tm1, tm2);
    MPIIO_POST_RECORD();

    return(ret);
}
#ifdef HAVE_MPIIO_CONST
DARSHAN_WRAPPER_MAP(PMPI_File_write_at_all_begin, int, (MPI_File fh, MPI_Offset offset, const void * buf,
    int count, MPI_Datatype datatype), MPI_File_write_at_all_begin( fh, offset, buf, count, datatype))
#else
DARSHAN_WRAPPER_MAP(PMPI_File_write_at_all_begin, int, (MPI_File fh, MPI_Offset offset, void * buf,
    int count, MPI_Datatype datatype), MPI_File_write_at_all_begin( fh, offset, buf, count, datatype))
#endif

int DARSHAN_DECL(MPI_File_read_ordered_begin)(MPI_File fh, void * buf, int count, MPI_Datatype datatype)
{
    int ret;
    double tm1, tm2;

    MAP_OR_FAIL(PMPI_File_read_ordered_begin);

    tm1 = darshan_core_wtime();
    ret = __real_PMPI_File_read_ordered_begin(fh, buf, count,
        datatype);
    tm2 = darshan_core_wtime();

    MPIIO_PRE_RECORD();
    MPIIO_RECORD_READ(ret, fh, count, datatype, MPIIO_SPLIT_READS, tm1, tm2);
    MPIIO_POST_RECORD();

    return(ret);
}
DARSHAN_WRAPPER_MAP(PMPI_File_read_ordered_begin, int, (MPI_File fh, void * buf, int count, MPI_Datatype datatype),
        MPI_File_read_ordered_begin(fh, buf, count, datatype))

#ifdef HAVE_MPIIO_CONST
int DARSHAN_DECL(MPI_File_write_ordered_begin)(MPI_File fh, const void * buf, int count, MPI_Datatype datatype)
#else
int DARSHAN_DECL(MPI_File_write_ordered_begin)(MPI_File fh, void * buf, int count, MPI_Datatype datatype)
#endif
{
    int ret;
    double tm1, tm2;

    MAP_OR_FAIL(PMPI_File_write_ordered_begin);

    tm1 = darshan_core_wtime();
    ret = __real_PMPI_File_write_ordered_begin(fh, buf, count,
        datatype);
    tm2 = darshan_core_wtime();

    MPIIO_PRE_RECORD();
    MPIIO_RECORD_WRITE(ret, fh, count, datatype, MPIIO_SPLIT_WRITES, tm1, tm2);
    MPIIO_POST_RECORD();

    return(ret);
}
#ifdef HAVE_MPIIO_CONST
DARSHAN_WRAPPER_MAP(PMPI_File_write_ordered_begin, int, (MPI_File fh, const void * buf, int count, MPI_Datatype datatype),
        MPI_File_write_ordered_begin(fh, buf, count, datatype))
#else
DARSHAN_WRAPPER_MAP(PMPI_File_write_ordered_begin, int, (MPI_File fh, void * buf, int count, MPI_Datatype datatype),
        MPI_File_write_ordered_begin(fh, buf, count, datatype))
#endif

int DARSHAN_DECL(MPI_File_iread)(MPI_File fh, void * buf, int count, MPI_Datatype datatype, __D_MPI_REQUEST * request)
{
    int ret;
    double tm1, tm2;

    MAP_OR_FAIL(PMPI_File_iread);

    tm1 = darshan_core_wtime();
    ret = __real_PMPI_File_iread(fh, buf, count, datatype, request);
    tm2 = darshan_core_wtime();

    MPIIO_PRE_RECORD();
    MPIIO_RECORD_READ(ret, fh, count, datatype, MPIIO_NB_READS, tm1, tm2);
    MPIIO_POST_RECORD();

    return(ret);
}
DARSHAN_WRAPPER_MAP(PMPI_File_iread, int, (MPI_File fh, void * buf, int count, MPI_Datatype datatype, __D_MPI_REQUEST * request),
        MPI_File_iread(fh, buf, count, datatype, request))

#ifdef HAVE_MPIIO_CONST
int DARSHAN_DECL(MPI_File_iwrite)(MPI_File fh, const void * buf, int count,
    MPI_Datatype datatype, __D_MPI_REQUEST * request)
#else
int DARSHAN_DECL(MPI_File_iwrite)(MPI_File fh, void * buf, int count,
    MPI_Datatype datatype, __D_MPI_REQUEST * request)
#endif
{
    int ret;
    double tm1, tm2;

    MAP_OR_FAIL(PMPI_File_iwrite);

    tm1 = darshan_core_wtime();
    ret = __real_PMPI_File_iwrite(fh, buf, count, datatype, request);
    tm2 = darshan_core_wtime();

    MPIIO_PRE_RECORD();
    MPIIO_RECORD_WRITE(ret, fh, count, datatype, MPIIO_NB_WRITES, tm1, tm2);
    MPIIO_POST_RECORD();

    return(ret);
}
#ifdef HAVE_MPIIO_CONST
DARSHAN_WRAPPER_MAP(PMPI_File_iwrite, int, (MPI_File fh, const void * buf, int count,
    MPI_Datatype datatype, __D_MPI_REQUEST * request),
        MPI_File_iwrite(fh, buf, count, datatype, request))
#else
DARSHAN_WRAPPER_MAP(PMPI_File_iwrite, int, (MPI_File fh, void * buf, int count,
    MPI_Datatype datatype, __D_MPI_REQUEST * request),
        MPI_File_iwrite(fh, buf, count, datatype, request))
#endif

int DARSHAN_DECL(MPI_File_iread_at)(MPI_File fh, MPI_Offset offset, void * buf,
    int count, MPI_Datatype datatype, __D_MPI_REQUEST *request)
{
    int ret;
    double tm1, tm2;

    MAP_OR_FAIL(PMPI_File_iread_at);

    tm1 = darshan_core_wtime();
    ret = __real_PMPI_File_iread_at(fh, offset, buf, count,
        datatype, request);
    tm2 = darshan_core_wtime();

    MPIIO_PRE_RECORD();
    MPIIO_RECORD_READ(ret, fh, count, datatype, MPIIO_NB_READS, tm1, tm2);
    MPIIO_POST_RECORD();

    return(ret);
}
DARSHAN_WRAPPER_MAP(PMPI_File_iread_at, int, (MPI_File fh, MPI_Offset offset, void * buf,
    int count, MPI_Datatype datatype, __D_MPI_REQUEST *request),
        MPI_File_iread_at(fh, offset,buf,count,datatype,request))

#ifdef HAVE_MPIIO_CONST
int DARSHAN_DECL(MPI_File_iwrite_at)(MPI_File fh, MPI_Offset offset, const void * buf,
    int count, MPI_Datatype datatype, __D_MPI_REQUEST *request)
#else
int DARSHAN_DECL(MPI_File_iwrite_at)(MPI_File fh, MPI_Offset offset, void * buf,
    int count, MPI_Datatype datatype, __D_MPI_REQUEST *request)
#endif
{
    int ret;
    double tm1, tm2;

    MAP_OR_FAIL(PMPI_File_iwrite_at);

    tm1 = darshan_core_wtime();
    ret = __real_PMPI_File_iwrite_at(fh, offset, buf,
        count, datatype, request);
    tm2 = darshan_core_wtime();

    MPIIO_PRE_RECORD();
    MPIIO_RECORD_WRITE(ret, fh, count, datatype, MPIIO_NB_WRITES, tm1, tm2);
    MPIIO_POST_RECORD();

    return(ret);
}
#ifdef HAVE_MPIIO_CONST
DARSHAN_WRAPPER_MAP(PMPI_File_iwrite_at, int, (MPI_File fh, MPI_Offset offset, const void * buf,
    int count, MPI_Datatype datatype, __D_MPI_REQUEST *request),
        MPI_File_iwrite_at(fh, offset, buf, count, datatype, request))
#else
DARSHAN_WRAPPER_MAP(PMPI_File_iwrite_at, int, (MPI_File fh, MPI_Offset offset, void * buf,
    int count, MPI_Datatype datatype, __D_MPI_REQUEST *request),
        MPI_File_iwrite_at(fh, offset, buf, count, datatype, request))
#endif

int DARSHAN_DECL(MPI_File_iread_shared)(MPI_File fh, void * buf, int count,
    MPI_Datatype datatype, __D_MPI_REQUEST * request)
{
    int ret;
    double tm1, tm2;

    MAP_OR_FAIL(PMPI_File_iread_shared);

    tm1 = darshan_core_wtime();
    ret = __real_PMPI_File_iread_shared(fh, buf, count,
        datatype, request);
    tm2 = darshan_core_wtime();

    MPIIO_PRE_RECORD();
    MPIIO_RECORD_READ(ret, fh, count, datatype, MPIIO_NB_READS, tm1, tm2);
    MPIIO_POST_RECORD();

    return(ret);
}
DARSHAN_WRAPPER_MAP(PMPI_File_iread_shared, int, (MPI_File fh, void * buf, int count,
    MPI_Datatype datatype, __D_MPI_REQUEST * request),
        MPI_File_iread_shared(fh, buf, count, datatype, request))

#ifdef HAVE_MPIIO_CONST
int DARSHAN_DECL(MPI_File_iwrite_shared)(MPI_File fh, const void * buf, int count,
    MPI_Datatype datatype, __D_MPI_REQUEST * request)
#else
int DARSHAN_DECL(MPI_File_iwrite_shared)(MPI_File fh, void * buf, int count,
    MPI_Datatype datatype, __D_MPI_REQUEST * request)
#endif
{
    int ret;
    double tm1, tm2;

    MAP_OR_FAIL(PMPI_File_iwrite_shared);

    tm1 = darshan_core_wtime();
    ret = __real_PMPI_File_iwrite_shared(fh, buf, count,
        datatype, request);
    tm2 = darshan_core_wtime();

    MPIIO_PRE_RECORD();
    MPIIO_RECORD_WRITE(ret, fh, count, datatype, MPIIO_NB_WRITES, tm1, tm2);
    MPIIO_POST_RECORD();

    return(ret);
}
#ifdef HAVE_MPIIO_CONST
DARSHAN_WRAPPER_MAP(PMPI_File_iwrite_shared, int, (MPI_File fh, const void * buf, int count,
    MPI_Datatype datatype, __D_MPI_REQUEST * request),
        MPI_File_iwrite_shared(fh, buf, count, datatype, request))
#else
DARSHAN_WRAPPER_MAP(PMPI_File_iwrite_shared, int, (MPI_File fh, void * buf, int count,
    MPI_Datatype datatype, __D_MPI_REQUEST * request),
        MPI_File_iwrite_shared(fh, buf, count, datatype, request))
#endif

int DARSHAN_DECL(MPI_File_sync)(MPI_File fh)
{
    int ret;
    struct mpiio_file_record_ref *rec_ref;
    double tm1, tm2;

    MAP_OR_FAIL(PMPI_File_sync);

    tm1 = darshan_core_wtime();
    ret = __real_PMPI_File_sync(fh);
    tm2 = darshan_core_wtime();

    if(ret == MPI_SUCCESS)
    {
        MPIIO_PRE_RECORD();
        rec_ref = darshan_lookup_record_ref(mpiio_runtime->fh_hash,
            &fh, sizeof(MPI_File));
        if(rec_ref)
        {
            rec_ref->file_rec->counters[MPIIO_SYNCS] += 1;
            DARSHAN_TIMER_INC_NO_OVERLAP(
                rec_ref->file_rec->fcounters[MPIIO_F_WRITE_TIME],
                tm1, tm2, rec_ref->last_write_end);
        }
        MPIIO_POST_RECORD();
    }

    return(ret);
}
DARSHAN_WRAPPER_MAP(PMPI_File_sync, int, (MPI_File fh), MPI_File_sync(fh))

#ifdef HAVE_MPIIO_CONST
int DARSHAN_DECL(MPI_File_set_view)(MPI_File fh, MPI_Offset disp, MPI_Datatype etype,
    MPI_Datatype filetype, const char *datarep, MPI_Info info)
#else
int DARSHAN_DECL(MPI_File_set_view)(MPI_File fh, MPI_Offset disp, MPI_Datatype etype,
    MPI_Datatype filetype, char *datarep, MPI_Info info)
#endif
{
    int ret;
    struct mpiio_file_record_ref *rec_ref;
    double tm1, tm2;

    MAP_OR_FAIL(PMPI_File_set_view);

    tm1 = darshan_core_wtime();
    ret = __real_PMPI_File_set_view(fh, disp, etype, filetype,
        datarep, info);
    tm2 = darshan_core_wtime();

    if(ret == MPI_SUCCESS)
    {
        MPIIO_PRE_RECORD();
        rec_ref = darshan_lookup_record_ref(mpiio_runtime->fh_hash,
            &fh, sizeof(MPI_File));
        if(rec_ref)
        {
            rec_ref->file_rec->counters[MPIIO_VIEWS] += 1;
            if(info != MPI_INFO_NULL)
            {
                rec_ref->file_rec->counters[MPIIO_HINTS] += 1;
                DARSHAN_TIMER_INC_NO_OVERLAP(
                    rec_ref->file_rec->fcounters[MPIIO_F_META_TIME],
                    tm1, tm2, rec_ref->last_meta_end);
           }
        }
        MPIIO_POST_RECORD();
    }

    return(ret);
}
#ifdef HAVE_MPIIO_CONST
DARSHAN_WRAPPER_MAP(PMPI_File_set_view, int, (MPI_File fh, MPI_Offset disp, MPI_Datatype etype,
    MPI_Datatype filetype, const char *datarep, MPI_Info info), MPI_File_set_view(fh, disp, etype, filetype, datarep, info))
#else
DARSHAN_WRAPPER_MAP(PMPI_File_set_view, int, (MPI_File fh, MPI_Offset disp, MPI_Datatype etype,
    MPI_Datatype filetype, char *datarep, MPI_Info info), MPI_File_set_view(fh, disp, etype, filetype, datarep, info))
#endif

int DARSHAN_DECL(MPI_File_close)(MPI_File *fh)
{
    int ret;
    struct mpiio_file_record_ref *rec_ref;
    MPI_File tmp_fh = *fh;
    double tm1, tm2;

    MAP_OR_FAIL(PMPI_File_close);

    tm1 = darshan_core_wtime();
    ret = __real_PMPI_File_close(fh);
    tm2 = darshan_core_wtime();

    MPIIO_PRE_RECORD();
    rec_ref = darshan_lookup_record_ref(mpiio_runtime->fh_hash,
        &tmp_fh, sizeof(MPI_File));
    if(rec_ref)
    {
        if(rec_ref->file_rec->fcounters[MPIIO_F_CLOSE_START_TIMESTAMP] == 0 ||
         rec_ref->file_rec->fcounters[MPIIO_F_CLOSE_START_TIMESTAMP] > tm1)
           rec_ref->file_rec->fcounters[MPIIO_F_CLOSE_START_TIMESTAMP] = tm1;
        rec_ref->file_rec->fcounters[MPIIO_F_CLOSE_END_TIMESTAMP] = tm2;
        DARSHAN_TIMER_INC_NO_OVERLAP(
            rec_ref->file_rec->fcounters[MPIIO_F_META_TIME],
            tm1, tm2, rec_ref->last_meta_end);
        darshan_delete_record_ref(&(mpiio_runtime->fh_hash),
            &tmp_fh, sizeof(MPI_File));
    }
    MPIIO_POST_RECORD();

    return(ret);
}
DARSHAN_WRAPPER_MAP(PMPI_File_close, int, (MPI_File *fh), MPI_File_close(fh))

/***********************************************************
 * Internal functions for manipulating MPI-IO module state *
 ***********************************************************/

/* initialize data structures and register with darshan-core component */
static void mpiio_runtime_initialize()
{
    int mpiio_buf_size;

    /* try and store the default number of records for this module */
    mpiio_buf_size = DARSHAN_DEF_MOD_REC_COUNT * sizeof(struct darshan_mpiio_file);

    /* register the mpiio module with darshan core */
    darshan_core_register_module(
        DARSHAN_MPIIO_MOD,
        &mpiio_shutdown,
        &mpiio_buf_size,
        &my_rank,
        NULL);

    /* return if darshan-core does not provide enough module memory */
    if(mpiio_buf_size < sizeof(struct darshan_mpiio_file))
    {
        darshan_core_unregister_module(DARSHAN_MPIIO_MOD);
        return;
    }

    mpiio_runtime = malloc(sizeof(*mpiio_runtime));
    if(!mpiio_runtime)
    {
        darshan_core_unregister_module(DARSHAN_MPIIO_MOD);
        return;
    }
    memset(mpiio_runtime, 0, sizeof(*mpiio_runtime));

    /* allow DXT module to initialize if needed */
    dxt_mpiio_runtime_initialize();

    return;
}

static struct mpiio_file_record_ref *mpiio_track_new_file_record(
    darshan_record_id rec_id, const char *path)
{
    struct darshan_mpiio_file *file_rec = NULL;
    struct mpiio_file_record_ref *rec_ref = NULL;
    int ret;

    rec_ref = malloc(sizeof(*rec_ref));
    if(!rec_ref)
        return(NULL);
    memset(rec_ref, 0, sizeof(*rec_ref));

    /* add a reference to this file record based on record id */
    ret = darshan_add_record_ref(&(mpiio_runtime->rec_id_hash), &rec_id,
        sizeof(darshan_record_id), rec_ref);
    if(ret == 0)
    {
        free(rec_ref);
        return(NULL);
    }

    /* register the actual file record with darshan-core so it is persisted
     * in the log file
     */
    file_rec = darshan_core_register_record(
        rec_id,
        path,
        DARSHAN_MPIIO_MOD,
        sizeof(struct darshan_mpiio_file),
        NULL);

    if(!file_rec)
    {
        darshan_delete_record_ref(&(mpiio_runtime->rec_id_hash),
            &rec_id, sizeof(darshan_record_id));
        free(rec_ref);
        return(NULL);
    }

    /* registering this file record was successful, so initialize some fields */
    file_rec->base_rec.id = rec_id;
    file_rec->base_rec.rank = my_rank;
    rec_ref->file_rec = file_rec;
    mpiio_runtime->file_rec_count++;

    return(rec_ref);
}

static void mpiio_finalize_file_records(void *rec_ref_p, void *user_ptr)
{
    struct mpiio_file_record_ref *rec_ref =
        (struct mpiio_file_record_ref *)rec_ref_p;

    tdestroy(rec_ref->access_root, free);
    return;
}

static void mpiio_record_reduction_op(
    void* infile_v,
    void* inoutfile_v,
    int *len,
    MPI_Datatype *datatype)
{
    struct darshan_mpiio_file tmp_file;
    struct darshan_mpiio_file *infile = infile_v;
    struct darshan_mpiio_file *inoutfile = inoutfile_v;
    int i, j, k;

    for(i=0; i<*len; i++)
    {
        memset(&tmp_file, 0, sizeof(struct darshan_mpiio_file));
        tmp_file.base_rec.id = infile->base_rec.id;
        tmp_file.base_rec.rank = -1;

        /* sum */
        for(j=MPIIO_INDEP_OPENS; j<=MPIIO_VIEWS; j++)
        {
            tmp_file.counters[j] = infile->counters[j] + inoutfile->counters[j];
        }

        tmp_file.counters[MPIIO_MODE] = infile->counters[MPIIO_MODE];

        /* sum */
        for(j=MPIIO_BYTES_READ; j<=MPIIO_RW_SWITCHES; j++)
        {
            tmp_file.counters[j] = infile->counters[j] + inoutfile->counters[j];
        }

        /* skip MPIIO_MAX_*_TIME_SIZE; handled in floating point section */

        for(j=MPIIO_SIZE_READ_AGG_0_100; j<=MPIIO_SIZE_WRITE_AGG_1G_PLUS; j++)
        {
            tmp_file.counters[j] = infile->counters[j] + inoutfile->counters[j];
        }

        /* first collapse any duplicates */
        for(j=MPIIO_ACCESS1_ACCESS; j<=MPIIO_ACCESS4_ACCESS; j++)
        {
            for(k=MPIIO_ACCESS1_ACCESS; k<=MPIIO_ACCESS4_ACCESS; k++)
            {
                if(infile->counters[j] == inoutfile->counters[k])
                {
                    infile->counters[j+4] += inoutfile->counters[k+4];
                    inoutfile->counters[k] = 0;
                    inoutfile->counters[k+4] = 0;
                }
            }
        }

        /* first set */
        for(j=MPIIO_ACCESS1_ACCESS; j<=MPIIO_ACCESS4_ACCESS; j++)
        {
            DARSHAN_COMMON_VAL_COUNTER_INC(&(tmp_file.counters[MPIIO_ACCESS1_ACCESS]),
                &(tmp_file.counters[MPIIO_ACCESS1_COUNT]), infile->counters[j],
                infile->counters[j+4], 0);
        }

        /* second set */
        for(j=MPIIO_ACCESS1_ACCESS; j<=MPIIO_ACCESS4_ACCESS; j++)
        {
            DARSHAN_COMMON_VAL_COUNTER_INC(&(tmp_file.counters[MPIIO_ACCESS1_ACCESS]),
                &(tmp_file.counters[MPIIO_ACCESS1_COUNT]), inoutfile->counters[j],
                inoutfile->counters[j+4], 0);
        }

        /* min non-zero (if available) value */
        for(j=MPIIO_F_OPEN_START_TIMESTAMP; j<=MPIIO_F_CLOSE_START_TIMESTAMP; j++)
        {
            if((infile->fcounters[j] < inoutfile->fcounters[j] &&
               infile->fcounters[j] > 0) || inoutfile->fcounters[j] == 0)
                tmp_file.fcounters[j] = infile->fcounters[j];
            else
                tmp_file.fcounters[j] = inoutfile->fcounters[j];
        }

        /* max */
        for(j=MPIIO_F_OPEN_END_TIMESTAMP; j<= MPIIO_F_CLOSE_END_TIMESTAMP; j++)
        {
            if(infile->fcounters[j] > inoutfile->fcounters[j])
                tmp_file.fcounters[j] = infile->fcounters[j];
            else
                tmp_file.fcounters[j] = inoutfile->fcounters[j];
        }

        /* sum */
        for(j=MPIIO_F_READ_TIME; j<=MPIIO_F_META_TIME; j++)
        {
            tmp_file.fcounters[j] = infile->fcounters[j] + inoutfile->fcounters[j];
        }

        /* max (special case) */
        if(infile->fcounters[MPIIO_F_MAX_READ_TIME] >
            inoutfile->fcounters[MPIIO_F_MAX_READ_TIME])
        {
            tmp_file.fcounters[MPIIO_F_MAX_READ_TIME] =
                infile->fcounters[MPIIO_F_MAX_READ_TIME];
            tmp_file.counters[MPIIO_MAX_READ_TIME_SIZE] =
                infile->counters[MPIIO_MAX_READ_TIME_SIZE];
        }
        else
        {
            tmp_file.fcounters[MPIIO_F_MAX_READ_TIME] =
                inoutfile->fcounters[MPIIO_F_MAX_READ_TIME];
            tmp_file.counters[MPIIO_MAX_READ_TIME_SIZE] =
                inoutfile->counters[MPIIO_MAX_READ_TIME_SIZE];
        }

        if(infile->fcounters[MPIIO_F_MAX_WRITE_TIME] >
            inoutfile->fcounters[MPIIO_F_MAX_WRITE_TIME])
        {
            tmp_file.fcounters[MPIIO_F_MAX_WRITE_TIME] =
                infile->fcounters[MPIIO_F_MAX_WRITE_TIME];
            tmp_file.counters[MPIIO_MAX_WRITE_TIME_SIZE] =
                infile->counters[MPIIO_MAX_WRITE_TIME_SIZE];
        }
        else
        {
            tmp_file.fcounters[MPIIO_F_MAX_WRITE_TIME] =
                inoutfile->fcounters[MPIIO_F_MAX_WRITE_TIME];
            tmp_file.counters[MPIIO_MAX_WRITE_TIME_SIZE] =
                inoutfile->counters[MPIIO_MAX_WRITE_TIME_SIZE];
        }

        /* min (zeroes are ok here; some procs don't do I/O) */
        if(infile->fcounters[MPIIO_F_FASTEST_RANK_TIME] <
            inoutfile->fcounters[MPIIO_F_FASTEST_RANK_TIME])
        {
            tmp_file.counters[MPIIO_FASTEST_RANK] =
                infile->counters[MPIIO_FASTEST_RANK];
            tmp_file.counters[MPIIO_FASTEST_RANK_BYTES] =
                infile->counters[MPIIO_FASTEST_RANK_BYTES];
            tmp_file.fcounters[MPIIO_F_FASTEST_RANK_TIME] =
                infile->fcounters[MPIIO_F_FASTEST_RANK_TIME];
        }
        else
        {
            tmp_file.counters[MPIIO_FASTEST_RANK] =
                inoutfile->counters[MPIIO_FASTEST_RANK];
            tmp_file.counters[MPIIO_FASTEST_RANK_BYTES] =
                inoutfile->counters[MPIIO_FASTEST_RANK_BYTES];
            tmp_file.fcounters[MPIIO_F_FASTEST_RANK_TIME] =
                inoutfile->fcounters[MPIIO_F_FASTEST_RANK_TIME];
        }

        /* max */
        if(infile->fcounters[MPIIO_F_SLOWEST_RANK_TIME] >
           inoutfile->fcounters[MPIIO_F_SLOWEST_RANK_TIME])
        {
            tmp_file.counters[MPIIO_SLOWEST_RANK] =
                infile->counters[MPIIO_SLOWEST_RANK];
            tmp_file.counters[MPIIO_SLOWEST_RANK_BYTES] =
                infile->counters[MPIIO_SLOWEST_RANK_BYTES];
            tmp_file.fcounters[MPIIO_F_SLOWEST_RANK_TIME] =
                infile->fcounters[MPIIO_F_SLOWEST_RANK_TIME];
        }
        else
        {
            tmp_file.counters[MPIIO_SLOWEST_RANK] =
                inoutfile->counters[MPIIO_SLOWEST_RANK];
            tmp_file.counters[MPIIO_SLOWEST_RANK_BYTES] =
                inoutfile->counters[MPIIO_SLOWEST_RANK_BYTES];
            tmp_file.fcounters[MPIIO_F_SLOWEST_RANK_TIME] =
                inoutfile->fcounters[MPIIO_F_SLOWEST_RANK_TIME];
        }

        /* update pointers */
        *inoutfile = tmp_file;
        inoutfile++;
        infile++;
    }

    return;
}

static void mpiio_shared_record_variance(MPI_Comm mod_comm,
    struct darshan_mpiio_file *inrec_array, struct darshan_mpiio_file *outrec_array,
    int shared_rec_count)
{
    MPI_Datatype var_dt;
    MPI_Op var_op;
    int i;
    struct darshan_variance_dt *var_send_buf = NULL;
    struct darshan_variance_dt *var_recv_buf = NULL;

    PMPI_Type_contiguous(sizeof(struct darshan_variance_dt),
        MPI_BYTE, &var_dt);
    PMPI_Type_commit(&var_dt);

    PMPI_Op_create(darshan_variance_reduce, 1, &var_op);

    var_send_buf = malloc(shared_rec_count * sizeof(struct darshan_variance_dt));
    if(!var_send_buf)
        return;

    if(my_rank == 0)
    {
        var_recv_buf = malloc(shared_rec_count * sizeof(struct darshan_variance_dt));

        if(!var_recv_buf)
            return;
    }

    /* get total i/o time variances for shared records */

    for(i=0; i<shared_rec_count; i++)
    {
        var_send_buf[i].n = 1;
        var_send_buf[i].S = 0;
        var_send_buf[i].T = inrec_array[i].fcounters[MPIIO_F_READ_TIME] +
                            inrec_array[i].fcounters[MPIIO_F_WRITE_TIME] +
                            inrec_array[i].fcounters[MPIIO_F_META_TIME];
    }

    PMPI_Reduce(var_send_buf, var_recv_buf, shared_rec_count,
        var_dt, var_op, 0, mod_comm);

    if(my_rank == 0)
    {
        for(i=0; i<shared_rec_count; i++)
        {
            outrec_array[i].fcounters[MPIIO_F_VARIANCE_RANK_TIME] =
                (var_recv_buf[i].S / var_recv_buf[i].n);
        }
    }

    /* get total bytes moved variances for shared records */

    for(i=0; i<shared_rec_count; i++)
    {
        var_send_buf[i].n = 1;
        var_send_buf[i].S = 0;
        var_send_buf[i].T = (double)
                            inrec_array[i].counters[MPIIO_BYTES_READ] +
                            inrec_array[i].counters[MPIIO_BYTES_WRITTEN];
    }

    PMPI_Reduce(var_send_buf, var_recv_buf, shared_rec_count,
        var_dt, var_op, 0, mod_comm);

    if(my_rank == 0)
    {
        for(i=0; i<shared_rec_count; i++)
        {
            outrec_array[i].fcounters[MPIIO_F_VARIANCE_RANK_BYTES] =
                (var_recv_buf[i].S / var_recv_buf[i].n);
        }
    }

    PMPI_Type_free(&var_dt);
    PMPI_Op_free(&var_op);
    free(var_send_buf);
    free(var_recv_buf);

    return;
}

static void mpiio_cleanup_runtime()
{
    darshan_clear_record_refs(&(mpiio_runtime->fh_hash), 0);
    darshan_clear_record_refs(&(mpiio_runtime->rec_id_hash), 1);

    free(mpiio_runtime);
    mpiio_runtime = NULL;

    return;
}

/* mpiio module shutdown benchmark routine */
void darshan_mpiio_shutdown_bench_setup(int test_case)
{
    char filepath[256];
    MPI_File *fh_array;
    int64_t *size_array;
    int i;
    intptr_t j;

    if(mpiio_runtime)
        mpiio_cleanup_runtime();

    mpiio_runtime_initialize();

    srand(my_rank);
    fh_array = malloc(1024 * sizeof(MPI_File));
    size_array = malloc(DARSHAN_COMMON_VAL_MAX_RUNTIME_COUNT * sizeof(int64_t));
    assert(fh_array && size_array);

    for(j = 0; j < 1024; j++)
        fh_array[j] = (MPI_File)j;
    for(i = 0; i < DARSHAN_COMMON_VAL_MAX_RUNTIME_COUNT; i++)
        size_array[i] = rand();

    switch(test_case)
    {
        case 1: /* single file-per-process */
            snprintf(filepath, 256, "fpp-0_rank-%d", my_rank);

            MPIIO_RECORD_OPEN(MPI_SUCCESS, filepath, fh_array[0], MPI_COMM_SELF,
                2, MPI_INFO_NULL, 0, 1);
            MPIIO_RECORD_WRITE(MPI_SUCCESS, fh_array[0], size_array[0], MPI_BYTE,
                MPIIO_INDEP_WRITES, 1, 2);

            break;
        case 2: /* single shared file */
            snprintf(filepath, 256, "shared-0");

            MPIIO_RECORD_OPEN(MPI_SUCCESS, filepath, fh_array[0], MPI_COMM_WORLD,
                2, MPI_INFO_NULL, 0, 1);
            MPIIO_RECORD_WRITE(MPI_SUCCESS, fh_array[0], size_array[0], MPI_BYTE,
                MPIIO_COLL_WRITES, 1, 2);

            break;
        case 3: /* 1024 unique files per proc */
            for(i = 0; i < 1024; i++)
            {
                snprintf(filepath, 256, "fpp-%d_rank-%d", i , my_rank);

                MPIIO_RECORD_OPEN(MPI_SUCCESS, filepath, fh_array[i], MPI_COMM_SELF,
                    2, MPI_INFO_NULL, 0, 1);
                MPIIO_RECORD_WRITE(MPI_SUCCESS, fh_array[i],
                    size_array[i % DARSHAN_COMMON_VAL_MAX_RUNTIME_COUNT],
                    MPI_BYTE, MPIIO_INDEP_WRITES, 1, 2);
            }

            break;
        case 4: /* 1024 shared files per proc */
            for(i = 0; i < 1024; i++)
            {
                snprintf(filepath, 256, "shared-%d", i);

                MPIIO_RECORD_OPEN(MPI_SUCCESS, filepath, fh_array[i], MPI_COMM_WORLD,
                    2, MPI_INFO_NULL, 0, 1);
                MPIIO_RECORD_WRITE(MPI_SUCCESS, fh_array[i],
                    size_array[i % DARSHAN_COMMON_VAL_MAX_RUNTIME_COUNT],
                    MPI_BYTE, MPIIO_COLL_WRITES, 1, 2);
            }
            break;
        default:
            fprintf(stderr, "Error: invalid Darshan benchmark test case.\n");
            return;
    }

    free(fh_array);
    free(size_array);

    return;
}

/********************************************************************************
 * shutdown function exported by this module for coordinating with darshan-core *
 ********************************************************************************/

static void mpiio_shutdown(
    MPI_Comm mod_comm,
    darshan_record_id *shared_recs,
    int shared_rec_count,
    void **mpiio_buf,
    int *mpiio_buf_sz)
{
    struct mpiio_file_record_ref *rec_ref;
    struct darshan_mpiio_file *mpiio_rec_buf = *(struct darshan_mpiio_file **)mpiio_buf;
    int mpiio_rec_count;
    double mpiio_time;
    struct darshan_mpiio_file *red_send_buf = NULL;
    struct darshan_mpiio_file *red_recv_buf = NULL;
    MPI_Datatype red_type;
    MPI_Op red_op;
    int i;

    MPIIO_LOCK();
    assert(mpiio_runtime);

    mpiio_rec_count = mpiio_runtime->file_rec_count;

    /* perform any final transformations on MPIIO file records before
     * writing them out to log file
     */
    darshan_iter_record_refs(mpiio_runtime->rec_id_hash,
        &mpiio_finalize_file_records, NULL);

    /* if there are globally shared files, do a shared file reduction */
    /* NOTE: the shared file reduction is also skipped if the 
     * DARSHAN_DISABLE_SHARED_REDUCTION environment variable is set.
     */
    if(shared_rec_count && !getenv("DARSHAN_DISABLE_SHARED_REDUCTION"))
    {
        /* necessary initialization of shared records */
        for(i = 0; i < shared_rec_count; i++)
        {
            rec_ref = darshan_lookup_record_ref(mpiio_runtime->rec_id_hash,
                &shared_recs[i], sizeof(darshan_record_id));
            assert(rec_ref);

            mpiio_time =
                rec_ref->file_rec->fcounters[MPIIO_F_READ_TIME] +
                rec_ref->file_rec->fcounters[MPIIO_F_WRITE_TIME] +
                rec_ref->file_rec->fcounters[MPIIO_F_META_TIME];

            /* initialize fastest/slowest info prior to the reduction */
            rec_ref->file_rec->counters[MPIIO_FASTEST_RANK] =
                rec_ref->file_rec->base_rec.rank;
            rec_ref->file_rec->counters[MPIIO_FASTEST_RANK_BYTES] =
                rec_ref->file_rec->counters[MPIIO_BYTES_READ] +
                rec_ref->file_rec->counters[MPIIO_BYTES_WRITTEN];
            rec_ref->file_rec->fcounters[MPIIO_F_FASTEST_RANK_TIME] =
                mpiio_time;

            /* until reduction occurs, we assume that this rank is both
             * the fastest and slowest. It is up to the reduction operator
             * to find the true min and max.
             */
            rec_ref->file_rec->counters[MPIIO_SLOWEST_RANK] =
                rec_ref->file_rec->counters[MPIIO_FASTEST_RANK];
            rec_ref->file_rec->counters[MPIIO_SLOWEST_RANK_BYTES] =
                rec_ref->file_rec->counters[MPIIO_FASTEST_RANK_BYTES];
            rec_ref->file_rec->fcounters[MPIIO_F_SLOWEST_RANK_TIME] =
                rec_ref->file_rec->fcounters[MPIIO_F_FASTEST_RANK_TIME];

            rec_ref->file_rec->base_rec.rank = -1;
        }

        /* sort the array of records so we get all of the shared records
         * (marked by rank -1) in a contiguous portion at end of the array
         */
        darshan_record_sort(mpiio_rec_buf, mpiio_rec_count,
            sizeof(struct darshan_mpiio_file));

        /* make send_buf point to the shared files at the end of sorted array */
        red_send_buf = &(mpiio_rec_buf[mpiio_rec_count-shared_rec_count]);

        /* allocate memory for the reduction output on rank 0 */
        if(my_rank == 0)
        {
            red_recv_buf = malloc(shared_rec_count * sizeof(struct darshan_mpiio_file));
            if(!red_recv_buf)
            {
                MPIIO_UNLOCK();
                return;
            }
        }

        /* construct a datatype for a MPIIO file record.  This is serving no purpose
         * except to make sure we can do a reduction on proper boundaries
         */
        PMPI_Type_contiguous(sizeof(struct darshan_mpiio_file),
            MPI_BYTE, &red_type);
        PMPI_Type_commit(&red_type);

        /* register a MPIIO file record reduction operator */
        PMPI_Op_create(mpiio_record_reduction_op, 1, &red_op);

        /* reduce shared MPIIO file records */
        PMPI_Reduce(red_send_buf, red_recv_buf,
            shared_rec_count, red_type, red_op, 0, mod_comm);

        /* get the time and byte variances for shared files */
        mpiio_shared_record_variance(mod_comm, red_send_buf, red_recv_buf,
            shared_rec_count);

        /* clean up reduction state */
        if(my_rank == 0)
        {
            int tmp_ndx = mpiio_rec_count - shared_rec_count;
            memcpy(&(mpiio_rec_buf[tmp_ndx]), red_recv_buf,
                shared_rec_count * sizeof(struct darshan_mpiio_file));
            free(red_recv_buf);
        }
        else
        {
            mpiio_rec_count -= shared_rec_count;
        }

        PMPI_Type_free(&red_type);
        PMPI_Op_free(&red_op);
    }

    *mpiio_buf_sz = mpiio_rec_count * sizeof(struct darshan_mpiio_file);

    /* shutdown internal structures used for instrumenting */
    mpiio_cleanup_runtime();

    MPIIO_UNLOCK();
    return;
}

/*
 * Local variables:
 *  c-indent-level: 4
 *  c-basic-offset: 4
 * End:
 *
 * vim: ts=8 sts=4 sw=4 expandtab
 */
