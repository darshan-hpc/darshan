/*
 * Copyright (C) 2025 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

#ifdef HAVE_CONFIG_H
# include <darshan-runtime-config.h>
#endif

#define _XOPEN_SOURCE 500
#define _GNU_SOURCE

#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <pthread.h>
#include <string.h>
#include <errno.h>
#include <limits.h>
#include <search.h>
#include <assert.h>

#include "uthash.h"
#include "darshan.h"
#include "darshan-dynamic.h"
#include "darshan-heatmap.h"
#include "darshan-gds-types.h"

/* The GDS module instruments NVIDIA GPUDirect Storage, i.e. the cuFile API.
 * cuFile operates on a POSIX fd registered with cuFileHandleRegister(), so
 * the record id is derived from the same file name POSIX uses.  GDS and
 * POSIX records for one file therefore share a record id.
 */

/* the POSIX module may be disabled or not linked; resolve lazily */
extern char *darshan_posix_lookup_record_name(int fd) __attribute__((weak));

DARSHAN_FORWARD_DECL(cuFileHandleRegister, CUfileError_t, (CUfileHandle_t *fh, CUfileDescr_t *descr));
DARSHAN_FORWARD_DECL(cuFileHandleDeregister, void, (CUfileHandle_t fh));
DARSHAN_FORWARD_DECL(cuFileRead, ssize_t, (CUfileHandle_t fh, void *bufPtr_base, size_t size, off_t file_offset, off_t bufPtr_offset));
DARSHAN_FORWARD_DECL(cuFileWrite, ssize_t, (CUfileHandle_t fh, const void *bufPtr_base, size_t size, off_t file_offset, off_t bufPtr_offset));
DARSHAN_FORWARD_DECL(cuFileBatchIOSubmit, CUfileError_t, (CUfileBatchHandle_t batch_idp, unsigned nr, CUfileIOParams_t *iocbp, unsigned int flags));
DARSHAN_FORWARD_DECL(cuFileReadAsync, CUfileError_t, (CUfileHandle_t fh, void *bufPtr_base, size_t *size_p, off_t *file_offset_p, off_t *bufPtr_offset_p, ssize_t *bytes_read_p, CUstream stream));
DARSHAN_FORWARD_DECL(cuFileWriteAsync, CUfileError_t, (CUfileHandle_t fh, void *bufPtr_base, size_t *size_p, off_t *file_offset_p, off_t *bufPtr_offset_p, ssize_t *bytes_written_p, CUstream stream));

/* The gds_file_record_ref structure maintains runtime state for a GDS file
 * record.  It can be found by darshan record id (rec_id_hash) or by any
 * registered cuFile handle (handle_hash); several handles may map to one
 * record.
 */
struct gds_file_record_ref
{
    struct darshan_gds_file *file_rec;
    int64_t last_byte_read;
    int64_t last_byte_written;
    enum darshan_io_type last_io_type;
    double last_meta_end;
    double last_read_end;
    double last_write_end;
    void *access_root;
    int access_count;
};

struct gds_runtime
{
    void *rec_id_hash;
    void *handle_hash; /* non-owning: key is the CUfileHandle_t value */
    int file_rec_count;
    darshan_record_id heatmap_id;
    int frozen; /* flag to indicate that the counters should no longer be modified */
};

static void gds_runtime_initialize(void);
static struct gds_file_record_ref *gds_track_new_file_record(
    darshan_record_id rec_id, const char *path);
static void gds_finalize_file_records(void *rec_ref_p, void *user_ptr);
#ifdef HAVE_MPI
static void gds_record_reduction_op(
    void* infile_v, void* inoutfile_v, int *len, MPI_Datatype *datatype);
static void gds_mpi_redux(
    void *gds_buf, MPI_Comm mod_comm,
    darshan_record_id *shared_recs, int shared_rec_count);
#endif
static void gds_output(void **gds_buf, int *gds_buf_sz);
static void gds_cleanup(void);

static struct gds_runtime *gds_runtime = NULL;
static pthread_mutex_t gds_runtime_mutex = PTHREAD_RECURSIVE_MUTEX_INITIALIZER_NP;
static int gds_runtime_init_attempted = 0;
static int my_rank = -1;

#define GDS_LOCK() pthread_mutex_lock(&gds_runtime_mutex)
#define GDS_UNLOCK() pthread_mutex_unlock(&gds_runtime_mutex)

#define GDS_WTIME() \
    __darshan_disabled ? 0 : darshan_core_wtime();

/* NOTE: unlike the DFS version this gates only on __darshan_disabled; the
 * return values of cuFile calls are not simple error codes, so each RECORD
 * macro checks success itself.
 */
#define GDS_PRE_RECORD_BODY(__retstmt) do { \
    if(!__darshan_disabled) { \
        GDS_LOCK(); \
        if(!gds_runtime && !gds_runtime_init_attempted) \
            gds_runtime_initialize(); \
        if(gds_runtime && !gds_runtime->frozen) break; \
        GDS_UNLOCK(); \
    } \
    __retstmt; \
} while(0)

#define GDS_PRE_RECORD() GDS_PRE_RECORD_BODY(errno = __saved_errno; return(ret))
#define GDS_PRE_RECORD_VOID() GDS_PRE_RECORD_BODY(errno = __saved_errno; return)

#define GDS_POST_RECORD() do { \
    GDS_UNLOCK(); \
} while(0)

/* update a read/write record and the size/access statistics shared by the
 * sync, batch and async paths.  Does not touch timers or offsets.
 */
#define GDS_UPDATE_SIZE_STATS(__rec_ref, __sz, __bucket, __iotype, __switch_from) do { \
    struct darshan_common_val_counter *__cvc; \
    int64_t __s = (__sz); \
    DARSHAN_BUCKET_INC(&((__rec_ref)->file_rec->counters[__bucket]), __s); \
    __cvc = darshan_track_common_val_counters(&(__rec_ref)->access_root, &__s, 1, \
        &(__rec_ref)->access_count); \
    if(__cvc) DARSHAN_UPDATE_COMMON_VAL_COUNTERS( \
        &((__rec_ref)->file_rec->counters[GDS_ACCESS1_ACCESS]), \
        &((__rec_ref)->file_rec->counters[GDS_ACCESS1_COUNT]), \
        __cvc->vals, 1, __cvc->freq, 0); \
    if((__rec_ref)->last_io_type == (__switch_from)) \
        (__rec_ref)->file_rec->counters[GDS_RW_SWITCHES] += 1; \
    (__rec_ref)->last_io_type = (__iotype); \
} while(0)

#define GDS_LOOKUP_HANDLE(__fh) \
    darshan_lookup_record_ref(gds_runtime->handle_hash, &(__fh), sizeof(__fh))

#define GDS_RECORD_REGISTER(__fh, __name, __tm1, __tm2) do { \
    darshan_record_id __rec_id; \
    struct gds_file_record_ref *__rec_ref; \
    __rec_id = darshan_core_gen_record_id(__name); \
    __rec_ref = darshan_lookup_record_ref(gds_runtime->rec_id_hash, \
        &__rec_id, sizeof(darshan_record_id)); \
    if(!__rec_ref) __rec_ref = gds_track_new_file_record(__rec_id, __name); \
    if(!__rec_ref) break; \
    /* if a stale mapping exists for a recycled handle value, drop it first */ \
    darshan_delete_record_ref(&(gds_runtime->handle_hash), &(__fh), sizeof(__fh)); \
    if(!darshan_add_record_ref(&(gds_runtime->handle_hash), &(__fh), \
        sizeof(__fh), __rec_ref)) break; \
    __rec_ref->file_rec->counters[GDS_HANDLE_REGISTERS] += 1; \
    if(__rec_ref->file_rec->fcounters[GDS_F_OPEN_START_TIMESTAMP] == 0 || \
     __rec_ref->file_rec->fcounters[GDS_F_OPEN_START_TIMESTAMP] > __tm1) \
        __rec_ref->file_rec->fcounters[GDS_F_OPEN_START_TIMESTAMP] = __tm1; \
    __rec_ref->file_rec->fcounters[GDS_F_OPEN_END_TIMESTAMP] = __tm2; \
    DARSHAN_TIMER_INC_NO_OVERLAP(__rec_ref->file_rec->fcounters[GDS_F_META_TIME], \
        __tm1, __tm2, __rec_ref->last_meta_end); \
} while(0)

#define GDS_RECORD_DEREGISTER(__fh, __tm1, __tm2) do { \
    struct gds_file_record_ref *__rec_ref; \
    __rec_ref = GDS_LOOKUP_HANDLE(__fh); \
    if(!__rec_ref) break; \
    __rec_ref->file_rec->counters[GDS_HANDLE_DEREGISTERS] += 1; \
    if(__rec_ref->file_rec->fcounters[GDS_F_CLOSE_START_TIMESTAMP] == 0 || \
     __rec_ref->file_rec->fcounters[GDS_F_CLOSE_START_TIMESTAMP] > __tm1) \
        __rec_ref->file_rec->fcounters[GDS_F_CLOSE_START_TIMESTAMP] = __tm1; \
    __rec_ref->file_rec->fcounters[GDS_F_CLOSE_END_TIMESTAMP] = __tm2; \
    DARSHAN_TIMER_INC_NO_OVERLAP(__rec_ref->file_rec->fcounters[GDS_F_META_TIME], \
        __tm1, __tm2, __rec_ref->last_meta_end); \
    darshan_delete_record_ref(&(gds_runtime->handle_hash), &(__fh), sizeof(__fh)); \
} while(0)

#define GDS_RECORD_READ(__ret, __fh, __off, __tm1, __tm2) do { \
    struct gds_file_record_ref *__rec_ref; \
    double __elapsed = __tm2 - __tm1; \
    int64_t __this_off = (int64_t)(__off); \
    __rec_ref = GDS_LOOKUP_HANDLE(__fh); \
    if(!__rec_ref) break; \
    if(__ret < 0) { \
        __rec_ref->file_rec->counters[GDS_READ_ERRORS] += 1; \
        break; \
    } \
    heatmap_update(gds_runtime->heatmap_id, HEATMAP_READ, __ret, __tm1, __tm2); \
    __rec_ref->file_rec->counters[GDS_READS] += 1; \
    __rec_ref->file_rec->counters[GDS_BYTES_READ] += __ret; \
    if(__this_off > __rec_ref->last_byte_read) \
        __rec_ref->file_rec->counters[GDS_SEQ_READS] += 1; \
    if(__this_off == (__rec_ref->last_byte_read + 1)) \
        __rec_ref->file_rec->counters[GDS_CONSEC_READS] += 1; \
    __rec_ref->last_byte_read = __this_off + __ret - 1; \
    if(__rec_ref->file_rec->counters[GDS_MAX_BYTE_READ] < (__this_off + __ret - 1)) \
        __rec_ref->file_rec->counters[GDS_MAX_BYTE_READ] = (__this_off + __ret - 1); \
    GDS_UPDATE_SIZE_STATS(__rec_ref, __ret, GDS_SIZE_READ_0_100, DARSHAN_IO_READ, DARSHAN_IO_WRITE); \
    if(__rec_ref->file_rec->fcounters[GDS_F_READ_START_TIMESTAMP] == 0 || \
     __rec_ref->file_rec->fcounters[GDS_F_READ_START_TIMESTAMP] > __tm1) \
        __rec_ref->file_rec->fcounters[GDS_F_READ_START_TIMESTAMP] = __tm1; \
    __rec_ref->file_rec->fcounters[GDS_F_READ_END_TIMESTAMP] = __tm2; \
    if(__rec_ref->file_rec->fcounters[GDS_F_MAX_READ_TIME] < __elapsed) { \
        __rec_ref->file_rec->fcounters[GDS_F_MAX_READ_TIME] = __elapsed; \
        __rec_ref->file_rec->counters[GDS_MAX_READ_TIME_SIZE] = __ret; } \
    DARSHAN_TIMER_INC_NO_OVERLAP(__rec_ref->file_rec->fcounters[GDS_F_READ_TIME], \
        __tm1, __tm2, __rec_ref->last_read_end); \
} while(0)

#define GDS_RECORD_WRITE(__ret, __fh, __off, __tm1, __tm2) do { \
    struct gds_file_record_ref *__rec_ref; \
    double __elapsed = __tm2 - __tm1; \
    int64_t __this_off = (int64_t)(__off); \
    __rec_ref = GDS_LOOKUP_HANDLE(__fh); \
    if(!__rec_ref) break; \
    if(__ret < 0) { \
        __rec_ref->file_rec->counters[GDS_WRITE_ERRORS] += 1; \
        break; \
    } \
    heatmap_update(gds_runtime->heatmap_id, HEATMAP_WRITE, __ret, __tm1, __tm2); \
    __rec_ref->file_rec->counters[GDS_WRITES] += 1; \
    __rec_ref->file_rec->counters[GDS_BYTES_WRITTEN] += __ret; \
    if(__this_off > __rec_ref->last_byte_written) \
        __rec_ref->file_rec->counters[GDS_SEQ_WRITES] += 1; \
    if(__this_off == (__rec_ref->last_byte_written + 1)) \
        __rec_ref->file_rec->counters[GDS_CONSEC_WRITES] += 1; \
    __rec_ref->last_byte_written = __this_off + __ret - 1; \
    if(__rec_ref->file_rec->counters[GDS_MAX_BYTE_WRITTEN] < (__this_off + __ret - 1)) \
        __rec_ref->file_rec->counters[GDS_MAX_BYTE_WRITTEN] = (__this_off + __ret - 1); \
    GDS_UPDATE_SIZE_STATS(__rec_ref, __ret, GDS_SIZE_WRITE_0_100, DARSHAN_IO_WRITE, DARSHAN_IO_READ); \
    if(__rec_ref->file_rec->fcounters[GDS_F_WRITE_START_TIMESTAMP] == 0 || \
     __rec_ref->file_rec->fcounters[GDS_F_WRITE_START_TIMESTAMP] > __tm1) \
        __rec_ref->file_rec->fcounters[GDS_F_WRITE_START_TIMESTAMP] = __tm1; \
    __rec_ref->file_rec->fcounters[GDS_F_WRITE_END_TIMESTAMP] = __tm2; \
    if(__rec_ref->file_rec->fcounters[GDS_F_MAX_WRITE_TIME] < __elapsed) { \
        __rec_ref->file_rec->fcounters[GDS_F_MAX_WRITE_TIME] = __elapsed; \
        __rec_ref->file_rec->counters[GDS_MAX_WRITE_TIME_SIZE] = __ret; } \
    DARSHAN_TIMER_INC_NO_OVERLAP(__rec_ref->file_rec->fcounters[GDS_F_WRITE_TIME], \
        __tm1, __tm2, __rec_ref->last_write_end); \
} while(0)

/* batch ops: only requested sizes are known, and there is no per-op timing */
#define GDS_RECORD_BATCH_OP(__fh, __opcode, __size) do { \
    struct gds_file_record_ref *__rec_ref; \
    int64_t __sz = (int64_t)(__size); \
    __rec_ref = GDS_LOOKUP_HANDLE(__fh); \
    if(!__rec_ref) break; \
    if((__opcode) == DARSHAN_GDS_READ) { \
        __rec_ref->file_rec->counters[GDS_BATCH_READS] += 1; \
        __rec_ref->file_rec->counters[GDS_BATCH_BYTES_READ] += __sz; \
        GDS_UPDATE_SIZE_STATS(__rec_ref, __sz, GDS_SIZE_READ_0_100, DARSHAN_IO_READ, DARSHAN_IO_WRITE); \
    } \
    else if((__opcode) == DARSHAN_GDS_WRITE) { \
        __rec_ref->file_rec->counters[GDS_BATCH_WRITES] += 1; \
        __rec_ref->file_rec->counters[GDS_BATCH_BYTES_WRITTEN] += __sz; \
        GDS_UPDATE_SIZE_STATS(__rec_ref, __sz, GDS_SIZE_WRITE_0_100, DARSHAN_IO_WRITE, DARSHAN_IO_READ); \
    } \
} while(0)

/* async ops: *size_p is only an upper bound; bytes_p is filled in later by
 * the stream, so it is never dereferenced
 */
#define GDS_RECORD_ASYNC(__fh, __is_write, __size) do { \
    struct gds_file_record_ref *__rec_ref; \
    __rec_ref = GDS_LOOKUP_HANDLE(__fh); \
    if(!__rec_ref) break; \
    if(__is_write) { \
        __rec_ref->file_rec->counters[GDS_ASYNC_WRITES] += 1; \
        __rec_ref->file_rec->counters[GDS_ASYNC_BYTES_REQ_WRITTEN] += (int64_t)(__size); \
    } else { \
        __rec_ref->file_rec->counters[GDS_ASYNC_READS] += 1; \
        __rec_ref->file_rec->counters[GDS_ASYNC_BYTES_REQ_READ] += (int64_t)(__size); \
    } \
} while(0)

/**********************************************************
 *        Wrappers for GDS functions of interest          *
 **********************************************************/

CUfileError_t DARSHAN_DECL(cuFileHandleRegister)(CUfileHandle_t *fh, CUfileDescr_t *descr)
{
    CUfileError_t ret;
    double tm1, tm2;
    int __darshan_disabled;
    int __saved_errno;
    CUfileHandle_t handle;
    char *rec_name = NULL;
    char link_path[64];
    char path_buf[PATH_MAX];
    int fd;

    MAP_OR_FAIL(cuFileHandleRegister);

    tm1 = GDS_WTIME();
    ret = __real_cuFileHandleRegister(fh, descr);
    tm2 = GDS_WTIME();
    __saved_errno = errno;

    /* validate everything before dereferencing anything */
    if(__darshan_disabled || ret.err != DARSHAN_CU_FILE_SUCCESS || !fh || !*fh ||
        !descr || descr->type != DARSHAN_CU_FILE_HANDLE_TYPE_OPAQUE_FD)
    {
        errno = __saved_errno;
        return(ret);
    }
    handle = *fh;
    fd = descr->handle.fd;

    /* resolve the file name before taking the GDS lock, so the POSIX and
     * GDS locks are never held at the same time
     */
    if(darshan_posix_lookup_record_name)
        rec_name = darshan_posix_lookup_record_name(fd);
    if(!rec_name)
    {
        ssize_t len;
        snprintf(link_path, sizeof(link_path), "/proc/self/fd/%d", fd);
        len = readlink(link_path, path_buf, sizeof(path_buf) - 1);
        if(len > 0)
        {
            path_buf[len] = '\0';
            rec_name = path_buf;
        }
    }
    if(!rec_name)
    {
        errno = __saved_errno;
        return(ret);
    }

    GDS_PRE_RECORD();
    GDS_RECORD_REGISTER(handle, rec_name, tm1, tm2);
    GDS_POST_RECORD();

    errno = __saved_errno;
    return(ret);
}

void DARSHAN_DECL(cuFileHandleDeregister)(CUfileHandle_t fh)
{
    double tm1, tm2;
    int __darshan_disabled;
    int __saved_errno;

    MAP_OR_FAIL(cuFileHandleDeregister);

    tm1 = GDS_WTIME();
    __real_cuFileHandleDeregister(fh);
    tm2 = GDS_WTIME();
    __saved_errno = errno;

    GDS_PRE_RECORD_VOID();
    GDS_RECORD_DEREGISTER(fh, tm1, tm2);
    GDS_POST_RECORD();

    errno = __saved_errno;
    return;
}

ssize_t DARSHAN_DECL(cuFileRead)(CUfileHandle_t fh, void *bufPtr_base, size_t size,
    off_t file_offset, off_t bufPtr_offset)
{
    ssize_t ret;
    double tm1, tm2;
    int __darshan_disabled;
    int __saved_errno;

    MAP_OR_FAIL(cuFileRead);

    tm1 = GDS_WTIME();
    ret = __real_cuFileRead(fh, bufPtr_base, size, file_offset, bufPtr_offset);
    tm2 = GDS_WTIME();
    __saved_errno = errno;

    GDS_PRE_RECORD();
    GDS_RECORD_READ(ret, fh, file_offset, tm1, tm2);
    GDS_POST_RECORD();

    errno = __saved_errno;
    return(ret);
}

ssize_t DARSHAN_DECL(cuFileWrite)(CUfileHandle_t fh, const void *bufPtr_base, size_t size,
    off_t file_offset, off_t bufPtr_offset)
{
    ssize_t ret;
    double tm1, tm2;
    int __darshan_disabled;
    int __saved_errno;

    MAP_OR_FAIL(cuFileWrite);

    tm1 = GDS_WTIME();
    ret = __real_cuFileWrite(fh, bufPtr_base, size, file_offset, bufPtr_offset);
    tm2 = GDS_WTIME();
    __saved_errno = errno;

    GDS_PRE_RECORD();
    GDS_RECORD_WRITE(ret, fh, file_offset, tm1, tm2);
    GDS_POST_RECORD();

    errno = __saved_errno;
    return(ret);
}

CUfileError_t DARSHAN_DECL(cuFileBatchIOSubmit)(CUfileBatchHandle_t batch_idp, unsigned nr,
    CUfileIOParams_t *iocbp, unsigned int flags)
{
    CUfileError_t ret;
    double tm1, tm2;
    int __darshan_disabled;
    int __saved_errno;
    unsigned i;

    MAP_OR_FAIL(cuFileBatchIOSubmit);

    tm1 = GDS_WTIME();
    ret = __real_cuFileBatchIOSubmit(batch_idp, nr, iocbp, flags);
    tm2 = GDS_WTIME();
    (void)tm1; (void)tm2;
    __saved_errno = errno;

    if(ret.err != DARSHAN_CU_FILE_SUCCESS || !iocbp)
    {
        errno = __saved_errno;
        return(ret);
    }

    GDS_PRE_RECORD();
    /* entries in one batch may target different handles */
    for(i = 0; i < nr; i++)
        GDS_RECORD_BATCH_OP(iocbp[i].fh, iocbp[i].opcode, iocbp[i].u.batch.size);
    GDS_POST_RECORD();

    errno = __saved_errno;
    return(ret);
}

CUfileError_t DARSHAN_DECL(cuFileReadAsync)(CUfileHandle_t fh, void *bufPtr_base,
    size_t *size_p, off_t *file_offset_p, off_t *bufPtr_offset_p,
    ssize_t *bytes_read_p, CUstream stream)
{
    CUfileError_t ret;
    double tm1, tm2;
    int __darshan_disabled;
    int __saved_errno;

    MAP_OR_FAIL(cuFileReadAsync);

    tm1 = GDS_WTIME();
    ret = __real_cuFileReadAsync(fh, bufPtr_base, size_p, file_offset_p,
        bufPtr_offset_p, bytes_read_p, stream);
    tm2 = GDS_WTIME();
    (void)tm1; (void)tm2;
    __saved_errno = errno;

    if(ret.err != DARSHAN_CU_FILE_SUCCESS || !size_p)
    {
        errno = __saved_errno;
        return(ret);
    }

    GDS_PRE_RECORD();
    GDS_RECORD_ASYNC(fh, 0, *size_p);
    GDS_POST_RECORD();

    errno = __saved_errno;
    return(ret);
}

CUfileError_t DARSHAN_DECL(cuFileWriteAsync)(CUfileHandle_t fh, void *bufPtr_base,
    size_t *size_p, off_t *file_offset_p, off_t *bufPtr_offset_p,
    ssize_t *bytes_written_p, CUstream stream)
{
    CUfileError_t ret;
    double tm1, tm2;
    int __darshan_disabled;
    int __saved_errno;

    MAP_OR_FAIL(cuFileWriteAsync);

    tm1 = GDS_WTIME();
    ret = __real_cuFileWriteAsync(fh, bufPtr_base, size_p, file_offset_p,
        bufPtr_offset_p, bytes_written_p, stream);
    tm2 = GDS_WTIME();
    (void)tm1; (void)tm2;
    __saved_errno = errno;

    if(ret.err != DARSHAN_CU_FILE_SUCCESS || !size_p)
    {
        errno = __saved_errno;
        return(ret);
    }

    GDS_PRE_RECORD();
    GDS_RECORD_ASYNC(fh, 1, *size_p);
    GDS_POST_RECORD();

    errno = __saved_errno;
    return(ret);
}

/**********************************************************
 *   Internal functions for manipulating GDS module state  *
 **********************************************************/

static void gds_runtime_initialize()
{
    int ret;
    size_t gds_rec_count;
    darshan_module_funcs mod_funcs = {
#ifdef HAVE_MPI
        .mod_redux_func = &gds_mpi_redux,
#endif
        .mod_output_func = &gds_output,
        .mod_cleanup_func = &gds_cleanup
        };

    /* if this attempt at initializing fails, we won't try again */
    gds_runtime_init_attempted = 1;

    gds_rec_count = DARSHAN_DEF_MOD_REC_COUNT;

    ret = darshan_core_register_module(
        DARSHAN_GDS_MOD,
        mod_funcs,
        sizeof(struct darshan_gds_file),
        &gds_rec_count,
        &my_rank,
        NULL);
    if(ret < 0)
        return;

    gds_runtime = malloc(sizeof(*gds_runtime));
    if(!gds_runtime)
    {
        darshan_core_unregister_module(DARSHAN_GDS_MOD);
        return;
    }
    memset(gds_runtime, 0, sizeof(*gds_runtime));

    /* register a heatmap */
    gds_runtime->heatmap_id = heatmap_register("heatmap:GDS");

    return;
}

static struct gds_file_record_ref *gds_track_new_file_record(
    darshan_record_id rec_id, const char *path)
{
    struct darshan_gds_file *file_rec = NULL;
    struct gds_file_record_ref *rec_ref = NULL;
    int ret;

    rec_ref = malloc(sizeof(*rec_ref));
    if(!rec_ref)
        return(NULL);
    memset(rec_ref, 0, sizeof(*rec_ref));

    ret = darshan_add_record_ref(&(gds_runtime->rec_id_hash), &rec_id,
        sizeof(darshan_record_id), rec_ref);
    if(ret == 0)
    {
        free(rec_ref);
        return(NULL);
    }

    file_rec = darshan_core_register_record(
        rec_id,
        path,
        DARSHAN_GDS_MOD,
        sizeof(struct darshan_gds_file),
        NULL);

    if(!file_rec)
    {
        darshan_delete_record_ref(&(gds_runtime->rec_id_hash),
            &rec_id, sizeof(darshan_record_id));
        free(rec_ref);
        return(NULL);
    }

    file_rec->base_rec.id = rec_id;
    file_rec->base_rec.rank = my_rank;
    rec_ref->file_rec = file_rec;
    gds_runtime->file_rec_count++;

    return(rec_ref);
}

static void gds_finalize_file_records(void *rec_ref_p, void *user_ptr)
{
    struct gds_file_record_ref *rec_ref =
        (struct gds_file_record_ref *)rec_ref_p;

    tdestroy(rec_ref->access_root, free);
    return;
}

/*********************************************************************************
 * shutdown functions exported by this module for coordinating with darshan-core *
 *********************************************************************************/

#ifdef HAVE_MPI
static void gds_record_reduction_op(
    void* infile_v, void* inoutfile_v, int *len, MPI_Datatype *datatype)
{
    struct darshan_gds_file tmp_file;
    struct darshan_gds_file *infile = infile_v;
    struct darshan_gds_file *inoutfile = inoutfile_v;
    int i, j, k;

    for(i=0; i<*len; i++)
    {
        memset(&tmp_file, 0, sizeof(struct darshan_gds_file));
        tmp_file.base_rec.id = infile->base_rec.id;
        tmp_file.base_rec.rank = -1;

        /* sum */
        for(j=GDS_HANDLE_REGISTERS; j<=GDS_ASYNC_BYTES_REQ_WRITTEN; j++)
        {
            tmp_file.counters[j] = infile->counters[j] + inoutfile->counters[j];
            if(tmp_file.counters[j] < 0) /* make sure invalid counters are -1 exactly */
                tmp_file.counters[j] = -1;
        }

        /* max */
        for(j=GDS_MAX_BYTE_READ; j<=GDS_MAX_BYTE_WRITTEN; j++)
        {
            tmp_file.counters[j] = (
                (infile->counters[j] > inoutfile->counters[j]) ?
                infile->counters[j] : inoutfile->counters[j]);
        }

        /* sum */
        for(j=GDS_CONSEC_READS; j<=GDS_RW_SWITCHES; j++)
        {
            tmp_file.counters[j] = infile->counters[j] + inoutfile->counters[j];
            if(tmp_file.counters[j] < 0)
                tmp_file.counters[j] = -1;
        }

        /* skip GDS_MAX_*_TIME_SIZE; handled in floating point section */

        for(j=GDS_SIZE_READ_0_100; j<=GDS_SIZE_WRITE_1G_PLUS; j++)
        {
            tmp_file.counters[j] = infile->counters[j] + inoutfile->counters[j];
        }

        /* common access counters */

        /* first collapse any duplicates */
        for(j=GDS_ACCESS1_ACCESS; j<=GDS_ACCESS4_ACCESS; j++)
        {
            for(k=GDS_ACCESS1_ACCESS; k<=GDS_ACCESS4_ACCESS; k++)
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
        for(j=GDS_ACCESS1_ACCESS; j<=GDS_ACCESS4_ACCESS; j++)
        {
            DARSHAN_UPDATE_COMMON_VAL_COUNTERS(
                &(tmp_file.counters[GDS_ACCESS1_ACCESS]),
                &(tmp_file.counters[GDS_ACCESS1_COUNT]),
                &infile->counters[j], 1, infile->counters[j+4], 1);
        }
        /* second set */
        for(j=GDS_ACCESS1_ACCESS; j<=GDS_ACCESS4_ACCESS; j++)
        {
            DARSHAN_UPDATE_COMMON_VAL_COUNTERS(
                &(tmp_file.counters[GDS_ACCESS1_ACCESS]),
                &(tmp_file.counters[GDS_ACCESS1_COUNT]),
                &inoutfile->counters[j], 1, inoutfile->counters[j+4], 1);
        }

        /* min non-zero (if available) value */
        for(j=GDS_F_OPEN_START_TIMESTAMP; j<=GDS_F_CLOSE_START_TIMESTAMP; j++)
        {
            if((infile->fcounters[j] < inoutfile->fcounters[j] &&
               infile->fcounters[j] > 0) || inoutfile->fcounters[j] == 0)
                tmp_file.fcounters[j] = infile->fcounters[j];
            else
                tmp_file.fcounters[j] = inoutfile->fcounters[j];
        }

        /* max */
        for(j=GDS_F_OPEN_END_TIMESTAMP; j<=GDS_F_CLOSE_END_TIMESTAMP; j++)
        {
            if(infile->fcounters[j] > inoutfile->fcounters[j])
                tmp_file.fcounters[j] = infile->fcounters[j];
            else
                tmp_file.fcounters[j] = inoutfile->fcounters[j];
        }

        /* sum */
        for(j=GDS_F_READ_TIME; j<=GDS_F_META_TIME; j++)
        {
            tmp_file.fcounters[j] = infile->fcounters[j] + inoutfile->fcounters[j];
        }

        /* max (special case) */
        if(infile->fcounters[GDS_F_MAX_READ_TIME] >
            inoutfile->fcounters[GDS_F_MAX_READ_TIME])
        {
            tmp_file.fcounters[GDS_F_MAX_READ_TIME] =
                infile->fcounters[GDS_F_MAX_READ_TIME];
            tmp_file.counters[GDS_MAX_READ_TIME_SIZE] =
                infile->counters[GDS_MAX_READ_TIME_SIZE];
        }
        else
        {
            tmp_file.fcounters[GDS_F_MAX_READ_TIME] =
                inoutfile->fcounters[GDS_F_MAX_READ_TIME];
            tmp_file.counters[GDS_MAX_READ_TIME_SIZE] =
                inoutfile->counters[GDS_MAX_READ_TIME_SIZE];
        }

        if(infile->fcounters[GDS_F_MAX_WRITE_TIME] >
            inoutfile->fcounters[GDS_F_MAX_WRITE_TIME])
        {
            tmp_file.fcounters[GDS_F_MAX_WRITE_TIME] =
                infile->fcounters[GDS_F_MAX_WRITE_TIME];
            tmp_file.counters[GDS_MAX_WRITE_TIME_SIZE] =
                infile->counters[GDS_MAX_WRITE_TIME_SIZE];
        }
        else
        {
            tmp_file.fcounters[GDS_F_MAX_WRITE_TIME] =
                inoutfile->fcounters[GDS_F_MAX_WRITE_TIME];
            tmp_file.counters[GDS_MAX_WRITE_TIME_SIZE] =
                inoutfile->counters[GDS_MAX_WRITE_TIME_SIZE];
        }

        /* min (zeroes are ok here; some procs don't do I/O) */
        if(infile->fcounters[GDS_F_FASTEST_RANK_TIME] <
           inoutfile->fcounters[GDS_F_FASTEST_RANK_TIME])
        {
            tmp_file.counters[GDS_FASTEST_RANK] =
                infile->counters[GDS_FASTEST_RANK];
            tmp_file.counters[GDS_FASTEST_RANK_BYTES] =
                infile->counters[GDS_FASTEST_RANK_BYTES];
            tmp_file.fcounters[GDS_F_FASTEST_RANK_TIME] =
                infile->fcounters[GDS_F_FASTEST_RANK_TIME];
        }
        else
        {
            tmp_file.counters[GDS_FASTEST_RANK] =
                inoutfile->counters[GDS_FASTEST_RANK];
            tmp_file.counters[GDS_FASTEST_RANK_BYTES] =
                inoutfile->counters[GDS_FASTEST_RANK_BYTES];
            tmp_file.fcounters[GDS_F_FASTEST_RANK_TIME] =
                inoutfile->fcounters[GDS_F_FASTEST_RANK_TIME];
        }

        /* max */
        if(infile->fcounters[GDS_F_SLOWEST_RANK_TIME] >
           inoutfile->fcounters[GDS_F_SLOWEST_RANK_TIME])
        {
            tmp_file.counters[GDS_SLOWEST_RANK] =
                infile->counters[GDS_SLOWEST_RANK];
            tmp_file.counters[GDS_SLOWEST_RANK_BYTES] =
                infile->counters[GDS_SLOWEST_RANK_BYTES];
            tmp_file.fcounters[GDS_F_SLOWEST_RANK_TIME] =
                infile->fcounters[GDS_F_SLOWEST_RANK_TIME];
        }
        else
        {
            tmp_file.counters[GDS_SLOWEST_RANK] =
                inoutfile->counters[GDS_SLOWEST_RANK];
            tmp_file.counters[GDS_SLOWEST_RANK_BYTES] =
                inoutfile->counters[GDS_SLOWEST_RANK_BYTES];
            tmp_file.fcounters[GDS_F_SLOWEST_RANK_TIME] =
                inoutfile->fcounters[GDS_F_SLOWEST_RANK_TIME];
        }

        /* update pointers */
        *inoutfile = tmp_file;
        inoutfile++;
        infile++;
    }

    return;
}
#endif

/*********************************************************************************
 * shutdown functions exported by this module for coordinating with darshan-core *
 *********************************************************************************/

#ifdef HAVE_MPI
static void gds_mpi_redux(
    void *gds_buf, MPI_Comm mod_comm,
    darshan_record_id *shared_recs, int shared_rec_count)
{
    int gds_rec_count;
    struct gds_file_record_ref *rec_ref;
    struct darshan_gds_file *gds_rec_buf = (struct darshan_gds_file *)gds_buf;
    double gds_time;
    struct darshan_gds_file *red_send_buf = NULL;
    struct darshan_gds_file *red_recv_buf = NULL;
    MPI_Datatype red_type;
    MPI_Op red_op;
    int i;

    GDS_LOCK();
    assert(gds_runtime);

    gds_rec_count = gds_runtime->file_rec_count;

    /* necessary initialization of shared records */
    for(i = 0; i < shared_rec_count; i++)
    {
        rec_ref = darshan_lookup_record_ref(gds_runtime->rec_id_hash,
            &shared_recs[i], sizeof(darshan_record_id));
        assert(rec_ref);

        gds_time =
            rec_ref->file_rec->fcounters[GDS_F_READ_TIME] +
            rec_ref->file_rec->fcounters[GDS_F_WRITE_TIME] +
            rec_ref->file_rec->fcounters[GDS_F_META_TIME];

        /* initialize fastest/slowest info prior to the reduction */
        rec_ref->file_rec->counters[GDS_FASTEST_RANK] =
            rec_ref->file_rec->base_rec.rank;
        rec_ref->file_rec->counters[GDS_FASTEST_RANK_BYTES] =
            rec_ref->file_rec->counters[GDS_BYTES_READ] +
            rec_ref->file_rec->counters[GDS_BYTES_WRITTEN];
        rec_ref->file_rec->fcounters[GDS_F_FASTEST_RANK_TIME] =
            gds_time;

        /* until reduction occurs, we assume that this rank is both
         * the fastest and slowest. It is up to the reduction operator
         * to find the true min and max.
         */
        rec_ref->file_rec->counters[GDS_SLOWEST_RANK] =
            rec_ref->file_rec->counters[GDS_FASTEST_RANK];
        rec_ref->file_rec->counters[GDS_SLOWEST_RANK_BYTES] =
            rec_ref->file_rec->counters[GDS_FASTEST_RANK_BYTES];
        rec_ref->file_rec->fcounters[GDS_F_SLOWEST_RANK_TIME] =
            rec_ref->file_rec->fcounters[GDS_F_FASTEST_RANK_TIME];

        rec_ref->file_rec->base_rec.rank = -1;
    }

    /* sort the array of records so we get all of the shared records
     * (marked by rank -1) in a contiguous portion at end of the array
     */
    darshan_record_sort(gds_rec_buf, gds_rec_count,
        sizeof(struct darshan_gds_file));

    /* make send_buf point to the shared files at the end of sorted array */
    red_send_buf = &(gds_rec_buf[gds_rec_count-shared_rec_count]);

    /* allocate memory for the reduction output on rank 0 */
    if(my_rank == 0)
    {
        red_recv_buf = malloc(shared_rec_count * sizeof(struct darshan_gds_file));
        if(!red_recv_buf)
        {
            GDS_UNLOCK();
            return;
        }
    }

    /* construct a datatype for a GDS file record.  This is serving no purpose
     * except to make sure we can do a reduction on proper boundaries
     */
    PMPI_Type_contiguous(sizeof(struct darshan_gds_file),
        MPI_BYTE, &red_type);
    PMPI_Type_commit(&red_type);

    /* register a GDS file record reduction operator */
    PMPI_Op_create(gds_record_reduction_op, 1, &red_op);

    /* reduce shared GDS file records */
    PMPI_Reduce(red_send_buf, red_recv_buf,
        shared_rec_count, red_type, red_op, 0, mod_comm);

    /* update module state to account for shared file reduction */
    if(my_rank == 0)
    {
        /* overwrite local shared records with globally reduced records */
        int tmp_ndx = gds_rec_count - shared_rec_count;
        memcpy(&(gds_rec_buf[tmp_ndx]), red_recv_buf,
            shared_rec_count * sizeof(struct darshan_gds_file));
        free(red_recv_buf);
    }
    else
    {
        /* drop shared records on non-zero ranks */
        gds_runtime->file_rec_count -= shared_rec_count;
    }

    PMPI_Type_free(&red_type);
    PMPI_Op_free(&red_op);

    GDS_UNLOCK();
    return;
}
#endif


static void gds_output(
    void **gds_buf, int *gds_buf_sz)
{
    int gds_rec_count;

    GDS_LOCK();
    assert(gds_runtime);

    gds_rec_count = gds_runtime->file_rec_count;

    /* just pass back our updated total buffer size -- no need to update buffer */
    *gds_buf_sz = gds_rec_count * sizeof(struct darshan_gds_file);

    gds_runtime->frozen = 1;

    GDS_UNLOCK();
    return;
}

static void gds_cleanup()
{
    GDS_LOCK();
    assert(gds_runtime);

    darshan_iter_record_refs(gds_runtime->rec_id_hash,
        &gds_finalize_file_records, NULL);
    /* handle_hash entries are non-owning; free record refs only via rec_id_hash */
    darshan_clear_record_refs(&(gds_runtime->handle_hash), 0);
    darshan_clear_record_refs(&(gds_runtime->rec_id_hash), 1);

    free(gds_runtime);
    gds_runtime = NULL;
    gds_runtime_init_attempted = 0;

    GDS_UNLOCK();
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
