/*
 * Copyright (C) 2020 University of Chicago.
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
#include <stdarg.h>
#include <string.h>
#include <time.h>
#include <stdlib.h>
#include <errno.h>
#include <search.h>
#include <assert.h>
#include <pthread.h>
#include <limits.h>

#include "uthash.h"
#include "darshan.h"
#include "darshan-dynamic.h"
#include "darshan-heatmap.h"

#include <daos_types.h>
#include <daos_prop.h>
#include <daos_pool.h>
#include <daos_cont.h>
#include <daos_obj.h>
#include <daos_array.h>
#include <daos_fs.h>

DARSHAN_FORWARD_DECL(dfs_mount, int, (daos_handle_t poh, daos_handle_t coh, int flags, dfs_t **dfs));
DARSHAN_FORWARD_DECL(dfs_global2local, int, (daos_handle_t poh, daos_handle_t coh, int flags, d_iov_t glob, dfs_t **dfs));
DARSHAN_FORWARD_DECL(dfs_umount, int, (dfs_t *dfs));
DARSHAN_FORWARD_DECL(dfs_lookup, int, (dfs_t *dfs, const char *path, int flags, dfs_obj_t **obj, mode_t *mode, struct stat *stbuf));
DARSHAN_FORWARD_DECL(dfs_lookup_rel, int, (dfs_t *dfs, dfs_obj_t *parent, const char *name, int flags, dfs_obj_t **obj, mode_t *mode, struct stat *stbuf));
DARSHAN_FORWARD_DECL(dfs_open, int, (dfs_t *dfs, dfs_obj_t *parent, const char *name, mode_t mode, int flags, daos_oclass_id_t cid, daos_size_t chunk_size, const char *value, dfs_obj_t **obj));
DARSHAN_FORWARD_DECL(dfs_dup, int, (dfs_t *dfs, dfs_obj_t *obj, int flags, dfs_obj_t **new_obj));
DARSHAN_FORWARD_DECL(dfs_obj_global2local, int, (dfs_t *dfs, int flags, d_iov_t glob, dfs_obj_t **obj));
DARSHAN_FORWARD_DECL(dfs_release, int, (dfs_obj_t *obj));
DARSHAN_FORWARD_DECL(dfs_read, int, (dfs_t *dfs, dfs_obj_t *obj, d_sg_list_t *sgl, daos_off_t off, daos_size_t *read_size, daos_event_t *ev));
DARSHAN_FORWARD_DECL(dfs_readx, int, (dfs_t *dfs, dfs_obj_t *obj, dfs_iod_t *iod, d_sg_list_t *sgl, daos_size_t *read_size, daos_event_t *ev));
DARSHAN_FORWARD_DECL(dfs_write, int, (dfs_t *dfs, dfs_obj_t *obj, d_sg_list_t *sgl, daos_off_t off, daos_event_t *ev));
DARSHAN_FORWARD_DECL(dfs_writex, int, (dfs_t *dfs, dfs_obj_t *obj, dfs_iod_t *iod, d_sg_list_t *sgl, daos_event_t *ev));
DARSHAN_FORWARD_DECL(dfs_get_size, int, (dfs_t *dfs, dfs_obj_t *obj, daos_size_t *size));
DARSHAN_FORWARD_DECL(dfs_punch, int, (dfs_t *dfs, dfs_obj_t *obj, daos_off_t offset, daos_size_t len));
DARSHAN_FORWARD_DECL(dfs_remove, int, (dfs_t *dfs, dfs_obj_t *parent, const char *name, bool force, daos_obj_id_t *oid));
#if 0
DARSHAN_FORWARD_DECL(dfs_stat, int, (dfs_t *dfs, dfs_obj_t *parent, const char *name, struct stat *stbuf));
#endif
DARSHAN_FORWARD_DECL(dfs_ostat, int, (dfs_t *dfs, dfs_obj_t *obj, struct stat *stbuf));
DARSHAN_FORWARD_DECL(dfs_osetattr, int, (dfs_t *dfs, dfs_obj_t *obj, struct stat *stbuf, int flags));

/* The dfs_file_record_ref structure maintains necessary runtime metadata
 * for the DFS file record (darshan_dfs_file structure, defined in
 * darshan-dfs-log-format.h) pointed to by 'file_rec'. This metadata
 * assists with the instrumenting of specific statistics in the file record.
 *
 * RATIONALE: the DFS module needs to track some stateful, volatile
 * information about each open file (like the current file offset, most recent
 * access time, etc.) to aid in instrumentation, but this information can't be
 * stored in the darshan_dfs_file struct because we don't want it to appear in
 * the final darshan log file. We therefore associate a dfs_file_record_ref
 * struct with each darshan_dfs_file struct in order to track this information
 * (i.e., the mapping between dfs_file_record_ref structs to darshan_dfs_file
 * structs is one-to-one).
 *
 * NOTE: we use the 'darshan_record_ref' interface (in darshan-common) to
 * associate different types of handles with this dfs_file_record_ref struct.
 * This allows us to index this struct (and the underlying file record) by using
 * either the corresponding Darshan record identifier (derived from the underlying
   object OID) or by a DFS file object, for instance. Note that, while there should
 * only be a single Darshan record identifier that indexes a dfs_file_record_ref,
 * there could be multiple open file objects that index it.
 */
struct dfs_file_record_ref
{
    struct darshan_dfs_file *file_rec;
    enum darshan_io_type last_io_type;
    double last_meta_end;
    double last_read_end;
    double last_write_end;
    void *access_root;
    int access_count;
};

struct dfs_mount_info
{
    uuid_t pool_uuid;
    uuid_t cont_uuid;
    UT_hash_handle hlink;
};

struct dfs_runtime
{
    struct dfs_mount_info *mount_hash;
    void *rec_id_hash;
    void *file_obj_hash;
    int file_rec_count;
    darshan_record_id heatmap_id;
    int frozen; /* flag to indicate that the counters should no longer be modified */
};

static void dfs_runtime_initialize();
static struct dfs_file_record_ref *dfs_track_new_file_record(
    darshan_record_id rec_id, const char *path, struct dfs_mount_info *mnt_info);
static void dfs_finalize_file_records(
    void *rec_ref_p, void *user_ptr);
#ifdef HAVE_MPI
static void dfs_record_reduction_op(
    void* infile_v, void* inoutfile_v, int *len, MPI_Datatype *datatype);
static void dfs_mpi_redux(
    void *dfs_buf, MPI_Comm mod_comm,
    darshan_record_id *shared_recs, int shared_rec_count);
#endif
static void dfs_output(
    void **dfs_buf, int *dfs_buf_sz);
static void dfs_cleanup(
    void);

static struct dfs_runtime *dfs_runtime = NULL;
static pthread_mutex_t dfs_runtime_mutex = PTHREAD_RECURSIVE_MUTEX_INITIALIZER_NP;
static int dfs_runtime_init_attempted = 0;
static int my_rank = -1;

#define DFS_LOCK() pthread_mutex_lock(&dfs_runtime_mutex)
#define DFS_UNLOCK() pthread_mutex_unlock(&dfs_runtime_mutex)

#define DAOS_WTIME() \
    __darshan_disabled ? 0 : darshan_core_wtime();

#define DFS_PRE_RECORD() do { \
    if(!ret && !__darshan_disabled) { \
        DFS_LOCK(); \
        if(!dfs_runtime && !dfs_runtime_init_attempted) \
            dfs_runtime_initialize(); \
        if(dfs_runtime && !dfs_runtime->frozen) break; \
        DFS_UNLOCK(); \
    } \
    return(ret); \
} while(0)

#define DFS_POST_RECORD() do { \
    DFS_UNLOCK(); \
} while(0)

#define DFS_STORE_MOUNT_INFO(__poh, __coh, __dfs_p) do { \
    int __query_ret; \
    daos_pool_info_t __pool_info; \
    daos_cont_info_t __cont_info; \
    struct dfs_mount_info *__mnt_info; \
    __query_ret = daos_pool_query(__poh, NULL, &__pool_info, NULL, NULL); \
    if(__query_ret == 0) { \
        __query_ret = daos_cont_query(__coh, &__cont_info, NULL, NULL); \
        if(__query_ret == 0) { \
            __mnt_info = malloc(sizeof(*__mnt_info)); \
            if(__mnt_info) { \
                uuid_copy(__mnt_info->pool_uuid, __pool_info.pi_uuid); \
                uuid_copy(__mnt_info->cont_uuid, __cont_info.ci_uuid); \
                HASH_ADD_KEYPTR(hlink, dfs_runtime->mount_hash, *__dfs_p, sizeof(void *), __mnt_info); \
            } \
        } \
    } \
} while(0)

#define DFS_GET_MOUNT_INFO(__dfs, __mnt_info) \
    HASH_FIND(hlink, dfs_runtime->mount_hash, __dfs, sizeof(void *), __mnt_info)

#define DFS_FREE_MOUNT_INFO(__mnt_info) do { \
    HASH_DELETE(hlink, dfs_runtime->mount_hash, __mnt_info); \
    free(__mnt_info); \
} while(0)

#define DFS_RESOLVE_OBJ_REC_NAME(__parent_obj, __name, __obj_rec_name) do { \
    struct dfs_file_record_ref *__parent_rec_ref; \
    char *__parent_rec_name = NULL; \
    if (__parent_obj) { \
        __parent_rec_ref = darshan_lookup_record_ref(dfs_runtime->file_obj_hash, \
            &__parent_obj, sizeof(__parent_obj)); \
        if(__parent_rec_ref) \
            __parent_rec_name = darshan_core_lookup_record_name(__parent_rec_ref->file_rec->base_rec.id); \
    } \
    else { \
        __parent_rec_name = "/"; \
    } \
    int __obj_rec_name_len = (__parent_rec_name ? strlen(__parent_rec_name) : 0) + strlen(__name) + 1; \
    __obj_rec_name = malloc(__obj_rec_name_len); \
    if(!__obj_rec_name) break; \
    memset(__obj_rec_name, 0, __obj_rec_name_len); \
    if(__parent_rec_name) \
        strcat(__obj_rec_name, __parent_rec_name); \
    strcat(obj_rec_name, __name); \
} while(0)

/* Generate a DFS record ID based on the OID (and pool/container IDS) */
#define ID_GLOB_SIZE (sizeof(daos_obj_id_t) + (2*sizeof(uuid_t)))
#define DFS_GEN_DARSHAN_REC_ID(__oid_p, __mnt_info, __rec_id) do { \
    unsigned char __id_glob[ID_GLOB_SIZE]; \
    memset(__id_glob, 0, ID_GLOB_SIZE); \
    if(__mnt_info) { \
        memcpy(__id_glob, __mnt_info->pool_uuid, sizeof(uuid_t)); \
        memcpy(__id_glob+sizeof(uuid_t), __mnt_info->cont_uuid, sizeof(uuid_t)); \
    } \
    memcpy(__id_glob+(2*sizeof(uuid_t)), __oid_p, sizeof(*__oid_p)); \
    __rec_id = darshan_hash(__id_glob, ID_GLOB_SIZE, 0); \
} while(0)

/* NOTE: the following macro captures details about open(), lookup(),
 *       and obj_global2local() calls. separate operation counters
 *       are maintained for each, but all calls share the same floating
 *       point counters (i.e., OPEN_START_TIMESTAMP, OPEN_END_TIMESTAMP).
 */
#define DFS_RECORD_FILE_OBJ_OPEN(__dfs, __obj_name, __counter, __obj_p, __tm1, __tm2) do { \
    struct dfs_mount_info *__mnt_info; \
    daos_obj_id_t __oid; \
    darshan_record_id __rec_id; \
    struct dfs_file_record_ref *__rec_ref; \
    DFS_GET_MOUNT_INFO(__dfs, __mnt_info); \
    if(dfs_obj2id(*__obj_p, &__oid)) break; \
    DFS_GEN_DARSHAN_REC_ID(&__oid, __mnt_info, __rec_id); \
    __rec_ref = darshan_lookup_record_ref(dfs_runtime->rec_id_hash, &__rec_id, sizeof(__rec_id)); \
    if(!__rec_ref) __rec_ref = dfs_track_new_file_record(__rec_id, __obj_name, __mnt_info); \
    DFS_RECORD_FILE_OBJREF_OPEN(__rec_ref, __counter, __obj_p, __tm1, __tm2); \
} while(0)

#define DFS_RECORD_FILE_OBJREF_OPEN(__rec_ref, __counter, __obj_p, __tm1, __tm2) do { \
    if(!__rec_ref) break; \
    __rec_ref->file_rec->counters[__counter] += 1; \
    if(__rec_ref->file_rec->fcounters[DFS_F_OPEN_START_TIMESTAMP] == 0 || \
        __rec_ref->file_rec->fcounters[DFS_F_OPEN_START_TIMESTAMP] > __tm1) \
        __rec_ref->file_rec->fcounters[DFS_F_OPEN_START_TIMESTAMP] = __tm1; \
    __rec_ref->file_rec->fcounters[DFS_F_OPEN_END_TIMESTAMP] = __tm2; \
    DARSHAN_TIMER_INC_NO_OVERLAP(__rec_ref->file_rec->fcounters[DFS_F_META_TIME], \
        __tm1, __tm2, __rec_ref->last_meta_end); \
    darshan_add_record_ref(&(dfs_runtime->file_obj_hash), __obj_p, sizeof(*__obj_p), __rec_ref); \
} while(0)

#define DFS_RECORD_READ(__obj, __read_size, __counter, __is_async, __tm1, __tm2) do { \
    struct dfs_file_record_ref *__rec_ref; \
    struct darshan_common_val_counter *__cvc; \
    double __elapsed = __tm2-__tm1; \
    int64_t __sz = (int64_t)__read_size; \
    daos_size_t __chunk_size; \
    __rec_ref = darshan_lookup_record_ref(dfs_runtime->file_obj_hash, &__obj, sizeof(__obj)); \
    if(!__rec_ref) break; \
    /* heatmap to record traffic summary */ \
    heatmap_update(dfs_runtime->heatmap_id, HEATMAP_READ, __sz, __tm1, __tm2); \
    __rec_ref->file_rec->counters[__counter] += 1; \
    if(__is_async) \
        __rec_ref->file_rec->counters[DFS_NB_READS] += 1; \
    __rec_ref->file_rec->counters[DFS_BYTES_READ] += __sz; \
    DARSHAN_BUCKET_INC(&(__rec_ref->file_rec->counters[DFS_SIZE_READ_0_100]), __sz); \
    __cvc = darshan_track_common_val_counters(&__rec_ref->access_root, &__sz, 1, \
        &__rec_ref->access_count); \
    if(__cvc) DARSHAN_UPDATE_COMMON_VAL_COUNTERS( \
        &(__rec_ref->file_rec->counters[DFS_ACCESS1_ACCESS]), \
        &(__rec_ref->file_rec->counters[DFS_ACCESS1_COUNT]), \
        __cvc->vals, 1, __cvc->freq, 0); \
    if(__rec_ref->last_io_type == DARSHAN_IO_WRITE) \
        __rec_ref->file_rec->counters[DFS_RW_SWITCHES] += 1; \
    __rec_ref->last_io_type = DARSHAN_IO_READ; \
    if(__rec_ref->file_rec->counters[DFS_CHUNK_SIZE] == 0) \
        if(dfs_get_chunk_size(__obj, &__chunk_size) == 0) \
            __rec_ref->file_rec->counters[DFS_CHUNK_SIZE] = __chunk_size; \
    if(__rec_ref->file_rec->fcounters[DFS_F_READ_START_TIMESTAMP] == 0 || \
     __rec_ref->file_rec->fcounters[DFS_F_READ_START_TIMESTAMP] > __tm1) \
        __rec_ref->file_rec->fcounters[DFS_F_READ_START_TIMESTAMP] = __tm1; \
    __rec_ref->file_rec->fcounters[DFS_F_READ_END_TIMESTAMP] = __tm2; \
    if(__rec_ref->file_rec->fcounters[DFS_F_MAX_READ_TIME] < __elapsed) { \
        __rec_ref->file_rec->fcounters[DFS_F_MAX_READ_TIME] = __elapsed; \
        __rec_ref->file_rec->counters[DFS_MAX_READ_TIME_SIZE] = __sz; \
    } \
    DARSHAN_TIMER_INC_NO_OVERLAP(__rec_ref->file_rec->fcounters[DFS_F_READ_TIME], \
        __tm1, __tm2, __rec_ref->last_read_end); \
} while(0)

#define DFS_RECORD_WRITE(__obj, __write_size, __counter, __is_async, __tm1, __tm2) do { \
    struct dfs_file_record_ref *__rec_ref; \
    struct darshan_common_val_counter *__cvc; \
    double __elapsed = __tm2-__tm1; \
    int64_t __sz = (int64_t)__write_size; \
    daos_size_t __chunk_size; \
    __rec_ref = darshan_lookup_record_ref(dfs_runtime->file_obj_hash, &__obj, sizeof(__obj)); \
    if(!__rec_ref) break; \
    /* heatmap to record traffic summary */ \
    heatmap_update(dfs_runtime->heatmap_id, HEATMAP_WRITE, __sz, __tm1, __tm2); \
    __rec_ref->file_rec->counters[__counter] += 1; \
    if(__is_async) \
        __rec_ref->file_rec->counters[DFS_NB_WRITES] += 1; \
    __rec_ref->file_rec->counters[DFS_BYTES_WRITTEN] += __sz; \
    DARSHAN_BUCKET_INC(&(__rec_ref->file_rec->counters[DFS_SIZE_WRITE_0_100]), __sz); \
    __cvc = darshan_track_common_val_counters(&__rec_ref->access_root, &__sz, 1, \
        &__rec_ref->access_count); \
    if(__cvc) DARSHAN_UPDATE_COMMON_VAL_COUNTERS( \
        &(__rec_ref->file_rec->counters[DFS_ACCESS1_ACCESS]), \
        &(__rec_ref->file_rec->counters[DFS_ACCESS1_COUNT]), \
        __cvc->vals, 1, __cvc->freq, 0); \
    if(__rec_ref->last_io_type == DARSHAN_IO_READ) \
        __rec_ref->file_rec->counters[DFS_RW_SWITCHES] += 1; \
    __rec_ref->last_io_type = DARSHAN_IO_WRITE; \
    if(__rec_ref->file_rec->counters[DFS_CHUNK_SIZE] == 0) \
        if(dfs_get_chunk_size(__obj, &__chunk_size) == 0) \
            __rec_ref->file_rec->counters[DFS_CHUNK_SIZE] = __chunk_size; \
    if(__rec_ref->file_rec->fcounters[DFS_F_WRITE_START_TIMESTAMP] == 0 || \
     __rec_ref->file_rec->fcounters[DFS_F_WRITE_START_TIMESTAMP] > __tm1) \
        __rec_ref->file_rec->fcounters[DFS_F_WRITE_START_TIMESTAMP] = __tm1; \
    __rec_ref->file_rec->fcounters[DFS_F_WRITE_END_TIMESTAMP] = __tm2; \
    if(__rec_ref->file_rec->fcounters[DFS_F_MAX_WRITE_TIME] < __elapsed) { \
        __rec_ref->file_rec->fcounters[DFS_F_MAX_WRITE_TIME] = __elapsed; \
        __rec_ref->file_rec->counters[DFS_MAX_WRITE_TIME_SIZE] = __sz; \
    } \
    DARSHAN_TIMER_INC_NO_OVERLAP(__rec_ref->file_rec->fcounters[DFS_F_WRITE_TIME], \
        __tm1, __tm2, __rec_ref->last_write_end); \
} while(0)

/*****************************************************
 *      Wrappers for DAOS functions of interest      * 
 *****************************************************/

int DARSHAN_DECL(dfs_mount)(daos_handle_t poh, daos_handle_t coh, int flags, dfs_t **dfs)
{
    int ret;

    MAP_OR_FAIL(dfs_mount);

    ret = __real_dfs_mount(poh, coh, flags, dfs);

    DFS_PRE_RECORD();
    DFS_STORE_MOUNT_INFO(poh, coh, dfs);
    DFS_POST_RECORD();

    return(ret);
}

int DARSHAN_DECL(dfs_global2local)(daos_handle_t poh, daos_handle_t coh, int flags, d_iov_t glob, dfs_t **dfs)
{
    int ret;

    MAP_OR_FAIL(dfs_global2local);

    ret = __real_dfs_global2local(poh, coh, flags, glob, dfs);

    DFS_PRE_RECORD();
    DFS_STORE_MOUNT_INFO(poh, coh, dfs);
    DFS_POST_RECORD();

    return(ret);
}

int DARSHAN_DECL(dfs_umount)(dfs_t *dfs)
{
    int ret;
    struct dfs_mount_info *mnt_info;

    MAP_OR_FAIL(dfs_umount);

    if(!__darshan_disabled)
    {
        DFS_LOCK();
        if(dfs_runtime && !dfs_runtime->frozen)
        {
            DFS_GET_MOUNT_INFO(dfs, mnt_info);
            if(mnt_info)
                DFS_FREE_MOUNT_INFO(mnt_info);
        }
        DFS_UNLOCK();
    }

    ret = __real_dfs_umount(dfs);

    return(ret);
}

int DARSHAN_DECL(dfs_lookup)(dfs_t *dfs, const char *path, int flags, dfs_obj_t **obj, mode_t *mode, struct stat *stbuf)
{
    int ret;
    double tm1, tm2;

    MAP_OR_FAIL(dfs_lookup);

    tm1 = DAOS_WTIME();
    ret = __real_dfs_lookup(dfs, path, flags, obj, mode, stbuf);
    tm2 = DAOS_WTIME();

    DFS_PRE_RECORD();
    DFS_RECORD_FILE_OBJ_OPEN(dfs, path, DFS_LOOKUPS, obj, tm1, tm2);
    DFS_POST_RECORD();

    return(ret);
}

int DARSHAN_DECL(dfs_lookup_rel)(dfs_t *dfs, dfs_obj_t *parent, const char *name, int flags, dfs_obj_t **obj, mode_t *mode, struct stat *stbuf)
{
    int ret;
    double tm1, tm2;
    char *obj_rec_name = NULL;

    MAP_OR_FAIL(dfs_lookup_rel);

    tm1 = DAOS_WTIME();
    ret = __real_dfs_lookup_rel(dfs, parent, name, flags, obj, mode, stbuf);
    tm2 = DAOS_WTIME();

    DFS_PRE_RECORD();
    DFS_RESOLVE_OBJ_REC_NAME(parent, name, obj_rec_name);
    if(obj_rec_name)
    {
        DFS_RECORD_FILE_OBJ_OPEN(dfs, obj_rec_name, DFS_LOOKUPS, obj, tm1, tm2);
        free(obj_rec_name);
    }
    DFS_POST_RECORD();

    return(ret);
}

int DARSHAN_DECL(dfs_open)(dfs_t *dfs, dfs_obj_t *parent, const char *name, mode_t mode, int flags, daos_oclass_id_t cid, daos_size_t chunk_size, const char *value, dfs_obj_t **obj)
{
    int ret;
    double tm1, tm2;
    char *obj_rec_name = NULL;

    MAP_OR_FAIL(dfs_open);

    tm1 = DAOS_WTIME();
    ret = __real_dfs_open(dfs, parent, name, mode, flags, cid, chunk_size, value, obj);
    tm2 = DAOS_WTIME();

    DFS_PRE_RECORD();
    DFS_RESOLVE_OBJ_REC_NAME(parent, name, obj_rec_name);
    if(obj_rec_name)
    {
        DFS_RECORD_FILE_OBJ_OPEN(dfs, obj_rec_name, DFS_OPENS, obj, tm1, tm2);
        free(obj_rec_name);
    }
    DFS_POST_RECORD();

    return(ret);
}

int DARSHAN_DECL(dfs_dup)(dfs_t *dfs, dfs_obj_t *obj, int flags, dfs_obj_t **new_obj)
{
    int ret;
    double tm1, tm2;
    struct dfs_file_record_ref *rec_ref = NULL;

    MAP_OR_FAIL(dfs_dup);

    tm1 = DAOS_WTIME();
    ret = __real_dfs_dup(dfs, obj, flags, new_obj);
    tm2 = DAOS_WTIME();

    DFS_PRE_RECORD();
    rec_ref = darshan_lookup_record_ref(dfs_runtime->file_obj_hash, &obj, sizeof(obj));
    DFS_RECORD_FILE_OBJREF_OPEN(rec_ref, DFS_DUPS, new_obj, tm1, tm2);
    DFS_POST_RECORD();

    return(ret);
}

int DARSHAN_DECL(dfs_obj_global2local)(dfs_t *dfs, int flags, d_iov_t glob, dfs_obj_t **obj)
{
    int ret;
    double tm1, tm2;
    char *obj_rec_name = NULL;

    MAP_OR_FAIL(dfs_obj_global2local);

    tm1 = DAOS_WTIME();
    ret = __real_dfs_obj_global2local(dfs, flags, glob, obj);
    tm2 = DAOS_WTIME();

    DFS_PRE_RECORD();
    DFS_RECORD_FILE_OBJ_OPEN(dfs, obj_rec_name, DFS_GLOBAL_OPENS, obj, tm1, tm2);
    DFS_POST_RECORD();

    return(ret);
}

int DARSHAN_DECL(dfs_release)(dfs_obj_t *obj)
{
    int ret;
    double tm1, tm2;
    struct dfs_file_record_ref *rec_ref = NULL;

    MAP_OR_FAIL(dfs_release);

    tm1 = DAOS_WTIME();
    ret = __real_dfs_release(obj);
    tm2 = DAOS_WTIME();

    DFS_PRE_RECORD();
    rec_ref = darshan_lookup_record_ref(dfs_runtime->file_obj_hash, &obj, sizeof(obj));
    if(rec_ref)
    {
        if(rec_ref->file_rec->fcounters[DFS_F_CLOSE_START_TIMESTAMP] == 0 ||
         rec_ref->file_rec->fcounters[DFS_F_CLOSE_START_TIMESTAMP] > tm1)
           rec_ref->file_rec->fcounters[DFS_F_CLOSE_START_TIMESTAMP] = tm1;
        rec_ref->file_rec->fcounters[DFS_F_CLOSE_END_TIMESTAMP] = tm2;
        DARSHAN_TIMER_INC_NO_OVERLAP(
            rec_ref->file_rec->fcounters[DFS_F_META_TIME],
            tm1, tm2, rec_ref->last_meta_end);
        darshan_delete_record_ref(&(dfs_runtime->file_obj_hash), &obj, sizeof(obj));
    }
    DFS_POST_RECORD();

    return(ret);
}

/* DAOS callback routine to measure end of async read calls */
struct dfs_read_event_tracker
{
    double tm1;
    dfs_obj_t *obj;
    int op;
    daos_size_t *read_size;
};
int darshan_dfs_read_comp_cb(void *arg, daos_event_t *ev, int ret)
{
    struct dfs_read_event_tracker *tracker = (struct dfs_read_event_tracker *)arg;

    if (ret == 0)
    {
        /* async operation completed successfully, capture Darshan statistics */
        double tm2 = darshan_core_wtime();
        DFS_RECORD_READ(tracker->obj, *(tracker->read_size), tracker->op, 1, tracker->tm1, tm2);
    }
    free(tracker);

    return 0;
}

int DARSHAN_DECL(dfs_read)(dfs_t *dfs, dfs_obj_t *obj, d_sg_list_t *sgl, daos_off_t off, daos_size_t *read_size, daos_event_t *ev)
{
    int ret;
    double tm1, tm2;
    daos_size_t rdsize;

    MAP_OR_FAIL(dfs_read);

    if (ev)
    {
        /* setup callback to record the read operation upon completion */
        struct dfs_read_event_tracker *tracker = malloc(sizeof(*tracker));
        if (tracker)
        {
            tracker->tm1 = DAOS_WTIME();
            tracker->obj = obj;
            tracker->op = DFS_READS;
            tracker->read_size = read_size;
            daos_event_register_comp_cb(ev, darshan_dfs_read_comp_cb, tracker);
        }
    }

    tm1 = DAOS_WTIME();
    ret = __real_dfs_read(dfs, obj, sgl, off, read_size, ev);
    tm2 = DAOS_WTIME();

    if (!ev)
    {
        /* only record here for synchronous I/O operations */
        DFS_PRE_RECORD();
        /* no need to calculate read_size, it's returned to user */
        rdsize = *read_size;
        DFS_RECORD_READ(obj, rdsize, DFS_READS, 0, tm1, tm2);
        DFS_POST_RECORD();
    }

    return(ret);
}

int DARSHAN_DECL(dfs_readx)(dfs_t *dfs, dfs_obj_t *obj, dfs_iod_t *iod, d_sg_list_t *sgl, daos_size_t *read_size, daos_event_t *ev)
{
    int ret;
    double tm1, tm2;
    daos_size_t rdsize;

    MAP_OR_FAIL(dfs_readx);

    if (ev)
    {
        /* setup callback to record the read operation upon completion */
        struct dfs_read_event_tracker *tracker = malloc(sizeof(*tracker));
        if (tracker)
        {
            tracker->tm1 = DAOS_WTIME();
            tracker->obj = obj;
            tracker->op = DFS_READXS;
            tracker->read_size = read_size;
            daos_event_register_comp_cb(ev, darshan_dfs_read_comp_cb, tracker);
        }
    }

    tm1 = DAOS_WTIME();
    ret = __real_dfs_readx(dfs, obj, iod, sgl, read_size, ev);
    tm2 = DAOS_WTIME();

    if (!ev)
    {
        /* only record here for synchronous I/O operations */
        DFS_PRE_RECORD();
        /* no need to calculate read_size, it's returned to user */
        rdsize = *read_size;
        DFS_RECORD_READ(obj, rdsize, DFS_READXS, 0, tm1, tm2);
        DFS_POST_RECORD();
    }

    return(ret);
}

/* DAOS callback routine to measure end of async write calls */
struct dfs_write_event_tracker
{
    double tm1;
    dfs_obj_t *obj;
    int op;
    daos_size_t write_size;
};
int darshan_dfs_write_comp_cb(void *arg, daos_event_t *ev, int ret)
{
    struct dfs_write_event_tracker *tracker = (struct dfs_write_event_tracker *)arg;

    if (ret == 0)
    {
        /* async operation completed successfully, capture Darshan statistics */
        double tm2 = darshan_core_wtime();
        DFS_RECORD_WRITE(tracker->obj, tracker->write_size, tracker->op, 1, tracker->tm1, tm2);
    }
    free(tracker);

    return 0;
}

int DARSHAN_DECL(dfs_write)(dfs_t *dfs, dfs_obj_t *obj, d_sg_list_t *sgl, daos_off_t off, daos_event_t *ev)
{
    int ret;
    double tm1, tm2;
    daos_size_t wrsize;
    int i;

    MAP_OR_FAIL(dfs_write);

    /* calculate write size first */
    for (i = 0, wrsize = 0; i < sgl->sg_nr; i++)
        wrsize += sgl->sg_iovs[i].iov_len;

    if (ev)
    {
        /* setup callback to record the write operation upon completion */
        struct dfs_write_event_tracker *tracker = malloc(sizeof(*tracker));
        if (tracker)
        {
            tracker->tm1 = DAOS_WTIME();
            tracker->obj = obj;
            tracker->op = DFS_WRITES;
            tracker->write_size = wrsize;
            daos_event_register_comp_cb(ev, darshan_dfs_write_comp_cb, tracker);
        }
    }

    tm1 = DAOS_WTIME();
    ret = __real_dfs_write(dfs, obj, sgl, off, ev);
    tm2 = DAOS_WTIME();

    if (!ev)
    {
        /* only record here for synchronous I/O operations */
        DFS_PRE_RECORD();
        DFS_RECORD_WRITE(obj, wrsize, DFS_WRITES, 0, tm1, tm2);
        DFS_POST_RECORD();
    }

    return(ret);
}

int DARSHAN_DECL(dfs_writex)(dfs_t *dfs, dfs_obj_t *obj, dfs_iod_t *iod, d_sg_list_t *sgl, daos_event_t *ev)
{
    int ret;
    double tm1, tm2;
    daos_size_t wrsize;
    int i;

    MAP_OR_FAIL(dfs_writex);

    /* calculate write size first */
    for (i = 0, wrsize = 0; i < sgl->sg_nr; i++)
        wrsize += sgl->sg_iovs[i].iov_len;

    if (ev)
    {
        /* setup callback to record the write operation upon completion */
        struct dfs_write_event_tracker *tracker = malloc(sizeof(*tracker));
        if (tracker)
        {
            tracker->tm1 = DAOS_WTIME();
            tracker->obj = obj;
            tracker->op = DFS_WRITEXS;
            tracker->write_size = wrsize;
            daos_event_register_comp_cb(ev, darshan_dfs_write_comp_cb, tracker);
        }
    }

    tm1 = DAOS_WTIME();
    ret = __real_dfs_writex(dfs, obj, iod, sgl, ev);
    tm2 = DAOS_WTIME();

    if (!ev)
    {
        /* only record here for synchronous I/O operations */
        DFS_PRE_RECORD();
        DFS_RECORD_WRITE(obj, wrsize, DFS_WRITEXS, 0, tm1, tm2);
        DFS_POST_RECORD();
    }

    return(ret);
}

int DARSHAN_DECL(dfs_get_size)(dfs_t *dfs, dfs_obj_t *obj, daos_size_t *size)
{
    int ret;
    double tm1, tm2;
    struct dfs_file_record_ref *rec_ref = NULL;

    MAP_OR_FAIL(dfs_get_size);

    tm1 = DAOS_WTIME();
    ret = __real_dfs_get_size(dfs, obj, size);
    tm2 = DAOS_WTIME();

    DFS_PRE_RECORD();
    rec_ref = darshan_lookup_record_ref(dfs_runtime->file_obj_hash, &obj, sizeof(obj));
    if(rec_ref)
    {
        rec_ref->file_rec->counters[DFS_GET_SIZES] += 1;
        DARSHAN_TIMER_INC_NO_OVERLAP(
            rec_ref->file_rec->fcounters[DFS_F_META_TIME],
            tm1, tm2, rec_ref->last_meta_end);
    }
    DFS_POST_RECORD();

    return(ret);
}

int DARSHAN_DECL(dfs_punch)(dfs_t *dfs, dfs_obj_t *obj, daos_off_t offset, daos_size_t len)
{
    int ret;
    double tm1, tm2;
    struct dfs_file_record_ref *rec_ref = NULL;

    MAP_OR_FAIL(dfs_punch);

    tm1 = DAOS_WTIME();
    ret = __real_dfs_punch(dfs, obj, offset, len);
    tm2 = DAOS_WTIME();

    DFS_PRE_RECORD();
    rec_ref = darshan_lookup_record_ref(dfs_runtime->file_obj_hash, &obj, sizeof(obj));
    if(rec_ref)
    {
        rec_ref->file_rec->counters[DFS_PUNCHES] += 1;
        DARSHAN_TIMER_INC_NO_OVERLAP(
            rec_ref->file_rec->fcounters[DFS_F_META_TIME],
            tm1, tm2, rec_ref->last_meta_end);
    }
    DFS_POST_RECORD();

    return(ret);
}

int DARSHAN_DECL(dfs_remove)(dfs_t *dfs, dfs_obj_t *parent, const char *name, bool force,
       daos_obj_id_t *oid)
{
    int ret;
    double tm1, tm2;
    daos_obj_id_t the_oid;
    struct dfs_mount_info *mnt_info;
    darshan_record_id rec_id;
    struct dfs_file_record_ref *rec_ref = NULL;
    char *obj_rec_name = NULL;

    /* ask for the OID if user doesn't -- used to compute record ID */
    if(!oid)
        oid = &the_oid;

    MAP_OR_FAIL(dfs_remove);

    tm1 = DAOS_WTIME();
    ret = __real_dfs_remove(dfs, parent, name, force, oid);
    tm2 = DAOS_WTIME();

    DFS_PRE_RECORD();
    DFS_GET_MOUNT_INFO(dfs, mnt_info);
    DFS_GEN_DARSHAN_REC_ID(oid, mnt_info, rec_id);
    rec_ref = darshan_lookup_record_ref(dfs_runtime->rec_id_hash,
        &rec_id, sizeof(rec_id));
    if(!rec_ref)
    {
        DFS_RESOLVE_OBJ_REC_NAME(parent, name, obj_rec_name);
        if(obj_rec_name)
        {
            rec_ref = dfs_track_new_file_record(rec_id, obj_rec_name, mnt_info);
            free(obj_rec_name);
        }
    }
    if(rec_ref)
    {
        rec_ref->file_rec->counters[DFS_REMOVES] += 1;
        DARSHAN_TIMER_INC_NO_OVERLAP(
            rec_ref->file_rec->fcounters[DFS_F_META_TIME],
            tm1, tm2, rec_ref->last_meta_end);
    }
    DFS_POST_RECORD();

    return(ret);
}

#if 0
/* XXX: we can't instrument this call because we have no way to obtain
 *      the associated OID, which is used to lookup the Darshan record
 */
int DARSHAN_DECL(dfs_stat)(dfs_t *dfs, dfs_obj_t *parent, const char *name, struct stat *stbuf)
{
    int ret;
    double tm1, tm2;
    struct dfs_file_record_ref *rec_ref = NULL;
    char *parent_rec_name, *rec_name;
    int rec_len;
    darshan_record_id rec_id;

    MAP_OR_FAIL(dfs_stat);

    tm1 = DAOS_WTIME();
    ret = __real_dfs_stat(dfs, parent, name, stbuf);
    tm2 = DAOS_WTIME();

    DFS_PRE_RECORD();
    DFS_RESOLVE_PARENT_REC_NAME(dfs, parent, parent_rec_name);
    if(parent_rec_name)
    {
        rec_len = strlen(parent_rec_name) + strlen(name) + 1;
        rec_name = malloc(rec_len);
        if(rec_name)
        {
            memset(rec_name, 0, rec_len);
            strcat(rec_name, parent_rec_name);
            strcat(rec_name, name);
            rec_id = darshan_core_gen_record_id(rec_name);
            rec_ref = darshan_lookup_record_ref(dfs_runtime->rec_id_hash, &rec_id, sizeof(rec_id));
            if(!rec_ref) rec_ref = dfs_track_new_file_record(rec_id, rec_name);
            if(rec_ref)
            {
                rec_ref->file_rec->counters[DFS_STATS] += 1;
                DARSHAN_TIMER_INC_NO_OVERLAP(
                    rec_ref->file_rec->fcounters[DFS_F_META_TIME],
                    tm1, tm2, rec_ref->last_meta_end);
            }
            free(rec_name);
        }
        if(!parent) free(parent_rec_name);
    }
    DFS_POST_RECORD();

    return(ret);
}
#endif

int DARSHAN_DECL(dfs_ostat)(dfs_t *dfs, dfs_obj_t *obj, struct stat *stbuf)
{
    int ret;
    double tm1, tm2;
    struct dfs_file_record_ref *rec_ref = NULL;

    MAP_OR_FAIL(dfs_ostat);

    tm1 = DAOS_WTIME();
    ret = __real_dfs_ostat(dfs, obj, stbuf);
    tm2 = DAOS_WTIME();

    DFS_PRE_RECORD();
    rec_ref = darshan_lookup_record_ref(dfs_runtime->file_obj_hash, &obj, sizeof(obj));
    if(rec_ref)
    {
        rec_ref->file_rec->counters[DFS_STATS] += 1;
        DARSHAN_TIMER_INC_NO_OVERLAP(
            rec_ref->file_rec->fcounters[DFS_F_META_TIME],
            tm1, tm2, rec_ref->last_meta_end);
    }
    DFS_POST_RECORD();

    return(ret);
}

int DARSHAN_DECL(dfs_osetattr)(dfs_t *dfs, dfs_obj_t *obj, struct stat *stbuf, int flags)
{
    int ret;
    double tm1, tm2;
    struct dfs_file_record_ref *rec_ref = NULL;

    MAP_OR_FAIL(dfs_osetattr);

    tm1 = DAOS_WTIME();
    ret = __real_dfs_osetattr(dfs, obj, stbuf, flags);
    tm2 = DAOS_WTIME();

    DFS_PRE_RECORD();
    rec_ref = darshan_lookup_record_ref(dfs_runtime->file_obj_hash, &obj, sizeof(obj));
    if(rec_ref)
    {
        rec_ref->file_rec->counters[DFS_STATS] += 1;
        DARSHAN_TIMER_INC_NO_OVERLAP(
            rec_ref->file_rec->fcounters[DFS_F_META_TIME],
            tm1, tm2, rec_ref->last_meta_end);
    }
    DFS_POST_RECORD();

    return(ret);
}

/*********************************************************
 * Internal functions for manipulating DAOS module state *
 *********************************************************/

static void dfs_runtime_initialize()
{
    int ret;
    size_t dfs_rec_count;
    darshan_module_funcs mod_funcs = {
#ifdef HAVE_MPI
        .mod_redux_func = &dfs_mpi_redux,
#endif
        .mod_output_func = &dfs_output,
        .mod_cleanup_func = &dfs_cleanup
        };

    /* if this attempt at initializing fails, we won't try again */
    dfs_runtime_init_attempted = 1;

    /* try to store a default number of records for this module */
    dfs_rec_count = DARSHAN_DEF_MOD_REC_COUNT;

    /* register the DFS module with darshan core */
    ret = darshan_core_register_module(
        DARSHAN_DFS_MOD,
        mod_funcs,
        sizeof(struct darshan_dfs_file),
        &dfs_rec_count,
        &my_rank,
        NULL);
    if(ret < 0)
        return;

    dfs_runtime = malloc(sizeof(*dfs_runtime));
    if(!dfs_runtime)
    {
        darshan_core_unregister_module(DARSHAN_DFS_MOD);
        return;
    }
    memset(dfs_runtime, 0, sizeof(*dfs_runtime));

    /* register a heatmap */
    dfs_runtime->heatmap_id = heatmap_register("heatmap:DFS");

    return;
}

static struct dfs_file_record_ref *dfs_track_new_file_record(
    darshan_record_id rec_id, const char *path, struct dfs_mount_info *mnt_info)
{
    struct darshan_dfs_file *file_rec = NULL;
    struct dfs_file_record_ref *rec_ref = NULL;
    int ret;

    rec_ref = malloc(sizeof(*rec_ref));
    if(!rec_ref)
        return(NULL);
    memset(rec_ref, 0, sizeof(*rec_ref));

    /* add a reference to this file record based on record id */
    ret = darshan_add_record_ref(&(dfs_runtime->rec_id_hash), &rec_id,
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
        DARSHAN_DFS_MOD,
        sizeof(struct darshan_dfs_file),
        NULL);

    if(!file_rec)
    {
        darshan_delete_record_ref(&(dfs_runtime->rec_id_hash),
            &rec_id, sizeof(darshan_record_id));
        free(rec_ref);
        return(NULL);
    }

    /* registering this file record was successful, so initialize some fields */
    file_rec->base_rec.id = rec_id;
    file_rec->base_rec.rank = my_rank;
    if(mnt_info)
    {
        uuid_copy(file_rec->pool_uuid, mnt_info->pool_uuid);
        uuid_copy(file_rec->cont_uuid, mnt_info->cont_uuid);
    }
    rec_ref->file_rec = file_rec;
    dfs_runtime->file_rec_count++;

    return(rec_ref);
}

static void dfs_finalize_file_records(void *rec_ref_p, void *user_ptr)
{
    struct dfs_file_record_ref *rec_ref =
        (struct dfs_file_record_ref *)rec_ref_p;

    tdestroy(rec_ref->access_root, free);
    return;
}

#ifdef HAVE_MPI
static void dfs_record_reduction_op(
    void* infile_v, void* inoutfile_v, int *len, MPI_Datatype *datatype)
{
    struct darshan_dfs_file tmp_file;
    struct darshan_dfs_file *infile = infile_v;
    struct darshan_dfs_file *inoutfile = inoutfile_v;
    int i, j, k;

    for(i=0; i<*len; i++)
    {
        memset(&tmp_file, 0, sizeof(struct darshan_dfs_file));
        tmp_file.base_rec.id = infile->base_rec.id;
        tmp_file.base_rec.rank = -1;
        uuid_copy(tmp_file.pool_uuid, infile->pool_uuid);
        uuid_copy(tmp_file.cont_uuid, infile->cont_uuid);

        /* sum */
        for(j=DFS_OPENS; j<=DFS_RW_SWITCHES; j++)
        {
            tmp_file.counters[j] = infile->counters[j] + inoutfile->counters[j];
            if(tmp_file.counters[j] < 0) /* make sure invalid counters are -1 exactly */
                tmp_file.counters[j] = -1;
        }

        /* skip DFS_MAX_*_TIME_SIZE; handled in floating point section */

        for(j=DFS_SIZE_READ_0_100; j<=DFS_SIZE_WRITE_1G_PLUS; j++)
        {
            tmp_file.counters[j] = infile->counters[j] + inoutfile->counters[j];
        }

        /* common access counters */

        /* first collapse any duplicates */
        for(j=DFS_ACCESS1_ACCESS; j<=DFS_ACCESS4_ACCESS; j++)
        {
            for(k=DFS_ACCESS1_ACCESS; k<=DFS_ACCESS4_ACCESS; k++)
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
        for(j=DFS_ACCESS1_ACCESS; j<=DFS_ACCESS4_ACCESS; j++)
        {
            DARSHAN_UPDATE_COMMON_VAL_COUNTERS(
                &(tmp_file.counters[DFS_ACCESS1_ACCESS]),
                &(tmp_file.counters[DFS_ACCESS1_COUNT]),
                &infile->counters[j], 1, infile->counters[j+4], 1);
        }
        /* second set */
        for(j=DFS_ACCESS1_ACCESS; j<=DFS_ACCESS4_ACCESS; j++)
        {
            DARSHAN_UPDATE_COMMON_VAL_COUNTERS(
                &(tmp_file.counters[DFS_ACCESS1_ACCESS]),
                &(tmp_file.counters[DFS_ACCESS1_COUNT]),
                &inoutfile->counters[j], 1, inoutfile->counters[j+4], 1);
        }

        tmp_file.counters[DFS_CHUNK_SIZE] = infile->counters[DFS_CHUNK_SIZE];

        /* min non-zero (if available) value */
        for(j=DFS_F_OPEN_START_TIMESTAMP; j<=DFS_F_CLOSE_START_TIMESTAMP; j++)
        {
            if((infile->fcounters[j] < inoutfile->fcounters[j] &&
               infile->fcounters[j] > 0) || inoutfile->fcounters[j] == 0)
                tmp_file.fcounters[j] = infile->fcounters[j];
            else
                tmp_file.fcounters[j] = inoutfile->fcounters[j];
        }

        /* max */
        for(j=DFS_F_OPEN_END_TIMESTAMP; j<=DFS_F_CLOSE_END_TIMESTAMP; j++)
        {
            if(infile->fcounters[j] > inoutfile->fcounters[j])
                tmp_file.fcounters[j] = infile->fcounters[j];
            else
                tmp_file.fcounters[j] = inoutfile->fcounters[j];
        }

        /* sum */
        for(j=DFS_F_READ_TIME; j<=DFS_F_META_TIME; j++)
        {
            tmp_file.fcounters[j] = infile->fcounters[j] + inoutfile->fcounters[j];
        }

        /* max (special case) */
        if(infile->fcounters[DFS_F_MAX_READ_TIME] >
            inoutfile->fcounters[DFS_F_MAX_READ_TIME])
        {
            tmp_file.fcounters[DFS_F_MAX_READ_TIME] =
                infile->fcounters[DFS_F_MAX_READ_TIME];
            tmp_file.counters[DFS_MAX_READ_TIME_SIZE] =
                infile->counters[DFS_MAX_READ_TIME_SIZE];
        }
        else
        {
            tmp_file.fcounters[DFS_F_MAX_READ_TIME] =
                inoutfile->fcounters[DFS_F_MAX_READ_TIME];
            tmp_file.counters[DFS_MAX_READ_TIME_SIZE] =
                inoutfile->counters[DFS_MAX_READ_TIME_SIZE];
        }

        if(infile->fcounters[DFS_F_MAX_WRITE_TIME] >
            inoutfile->fcounters[DFS_F_MAX_WRITE_TIME])
        {
            tmp_file.fcounters[DFS_F_MAX_WRITE_TIME] =
                infile->fcounters[DFS_F_MAX_WRITE_TIME];
            tmp_file.counters[DFS_MAX_WRITE_TIME_SIZE] =
                infile->counters[DFS_MAX_WRITE_TIME_SIZE];
        }
        else
        {
            tmp_file.fcounters[DFS_F_MAX_WRITE_TIME] =
                inoutfile->fcounters[DFS_F_MAX_WRITE_TIME];
            tmp_file.counters[DFS_MAX_WRITE_TIME_SIZE] =
                inoutfile->counters[DFS_MAX_WRITE_TIME_SIZE];
        }

        /* min (zeroes are ok here; some procs don't do I/O) */
        if(infile->fcounters[DFS_F_FASTEST_RANK_TIME] <
           inoutfile->fcounters[DFS_F_FASTEST_RANK_TIME])
        {
            tmp_file.counters[DFS_FASTEST_RANK] =
                infile->counters[DFS_FASTEST_RANK];
            tmp_file.counters[DFS_FASTEST_RANK_BYTES] =
                infile->counters[DFS_FASTEST_RANK_BYTES];
            tmp_file.fcounters[DFS_F_FASTEST_RANK_TIME] =
                infile->fcounters[DFS_F_FASTEST_RANK_TIME];
        }
        else
        {
            tmp_file.counters[DFS_FASTEST_RANK] =
                inoutfile->counters[DFS_FASTEST_RANK];
            tmp_file.counters[DFS_FASTEST_RANK_BYTES] =
                inoutfile->counters[DFS_FASTEST_RANK_BYTES];
            tmp_file.fcounters[DFS_F_FASTEST_RANK_TIME] =
                inoutfile->fcounters[DFS_F_FASTEST_RANK_TIME];
        }

        /* max */
        if(infile->fcounters[DFS_F_SLOWEST_RANK_TIME] >
           inoutfile->fcounters[DFS_F_SLOWEST_RANK_TIME])
        {
            tmp_file.counters[DFS_SLOWEST_RANK] =
                infile->counters[DFS_SLOWEST_RANK];
            tmp_file.counters[DFS_SLOWEST_RANK_BYTES] =
                infile->counters[DFS_SLOWEST_RANK_BYTES];
            tmp_file.fcounters[DFS_F_SLOWEST_RANK_TIME] =
                infile->fcounters[DFS_F_SLOWEST_RANK_TIME];
        }
        else
        {
            tmp_file.counters[DFS_SLOWEST_RANK] =
                inoutfile->counters[DFS_SLOWEST_RANK];
            tmp_file.counters[DFS_SLOWEST_RANK_BYTES] =
                inoutfile->counters[DFS_SLOWEST_RANK_BYTES];
            tmp_file.fcounters[DFS_F_SLOWEST_RANK_TIME] =
                inoutfile->fcounters[DFS_F_SLOWEST_RANK_TIME];
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
static void dfs_mpi_redux(
    void *dfs_buf, MPI_Comm mod_comm,
    darshan_record_id *shared_recs, int shared_rec_count)
{
    int dfs_rec_count;
    struct dfs_file_record_ref *rec_ref;
    struct darshan_dfs_file *dfs_rec_buf = (struct darshan_dfs_file *)dfs_buf;
    double dfs_time;
    struct darshan_dfs_file *red_send_buf = NULL;
    struct darshan_dfs_file *red_recv_buf = NULL;
    MPI_Datatype red_type;
    MPI_Op red_op;
    int i;

    DFS_LOCK();
    assert(dfs_runtime);

    dfs_rec_count = dfs_runtime->file_rec_count;

    /* necessary initialization of shared records */
    for(i = 0; i < shared_rec_count; i++)
    {
        rec_ref = darshan_lookup_record_ref(dfs_runtime->rec_id_hash,
            &shared_recs[i], sizeof(darshan_record_id));
        assert(rec_ref);

        dfs_time =
            rec_ref->file_rec->fcounters[DFS_F_READ_TIME] +
            rec_ref->file_rec->fcounters[DFS_F_WRITE_TIME] +
            rec_ref->file_rec->fcounters[DFS_F_META_TIME];

        /* initialize fastest/slowest info prior to the reduction */
        rec_ref->file_rec->counters[DFS_FASTEST_RANK] =
            rec_ref->file_rec->base_rec.rank;
        rec_ref->file_rec->counters[DFS_FASTEST_RANK_BYTES] =
            rec_ref->file_rec->counters[DFS_BYTES_READ] +
            rec_ref->file_rec->counters[DFS_BYTES_WRITTEN];
        rec_ref->file_rec->fcounters[DFS_F_FASTEST_RANK_TIME] =
            dfs_time;

        /* until reduction occurs, we assume that this rank is both
         * the fastest and slowest. It is up to the reduction operator
         * to find the true min and max.
         */
        rec_ref->file_rec->counters[DFS_SLOWEST_RANK] =
            rec_ref->file_rec->counters[DFS_FASTEST_RANK];
        rec_ref->file_rec->counters[DFS_SLOWEST_RANK_BYTES] =
            rec_ref->file_rec->counters[DFS_FASTEST_RANK_BYTES];
        rec_ref->file_rec->fcounters[DFS_F_SLOWEST_RANK_TIME] =
            rec_ref->file_rec->fcounters[DFS_F_FASTEST_RANK_TIME];

        rec_ref->file_rec->base_rec.rank = -1;
    }

    /* sort the array of records so we get all of the shared records
     * (marked by rank -1) in a contiguous portion at end of the array
     */
    darshan_record_sort(dfs_rec_buf, dfs_rec_count,
        sizeof(struct darshan_dfs_file));

    /* make send_buf point to the shared files at the end of sorted array */
    red_send_buf = &(dfs_rec_buf[dfs_rec_count-shared_rec_count]);

    /* allocate memory for the reduction output on rank 0 */
    if(my_rank == 0)
    {
        red_recv_buf = malloc(shared_rec_count * sizeof(struct darshan_dfs_file));
        if(!red_recv_buf)
        {
            DFS_UNLOCK();
            return;
        }
    }

    /* construct a datatype for a DFS file record.  This is serving no purpose
     * except to make sure we can do a reduction on proper boundaries
     */
    PMPI_Type_contiguous(sizeof(struct darshan_dfs_file),
        MPI_BYTE, &red_type);
    PMPI_Type_commit(&red_type);

    /* register a DFS file record reduction operator */
    PMPI_Op_create(dfs_record_reduction_op, 1, &red_op);

    /* reduce shared DFS file records */
    PMPI_Reduce(red_send_buf, red_recv_buf,
        shared_rec_count, red_type, red_op, 0, mod_comm);

    /* update module state to account for shared file reduction */
    if(my_rank == 0)
    {
        /* overwrite local shared records with globally reduced records */
        int tmp_ndx = dfs_rec_count - shared_rec_count;
        memcpy(&(dfs_rec_buf[tmp_ndx]), red_recv_buf,
            shared_rec_count * sizeof(struct darshan_dfs_file));
        free(red_recv_buf);
    }
    else
    {
        /* drop shared records on non-zero ranks */
        dfs_runtime->file_rec_count -= shared_rec_count;
    }

    PMPI_Type_free(&red_type);
    PMPI_Op_free(&red_op);

    DFS_UNLOCK();
    return;
}
#endif

static void dfs_output(
    void **dfs_buf, int *dfs_buf_sz)
{
    int dfs_rec_count;
    struct darshan_dfs_file *dfs_rec_buf = *(struct darshan_dfs_file **)dfs_buf;
    int i, j;
    int ops;

    DFS_LOCK();
    assert(dfs_runtime);

    dfs_rec_count = dfs_runtime->file_rec_count;

    /* filter out records that have been opened, but don't have any
     * I/O operations (e.g, open directories, etc.)
     */
    for(i=0; i<dfs_rec_count; i++)
    {
        for(j=DFS_READS; j<=DFS_STATS; j++)
        {
            ops = dfs_rec_buf[i].counters[j];
            if(ops) break;
        }
        if(!ops)
        {
            if(i != (dfs_rec_count-1))
            {
                memmove(&dfs_rec_buf[i], &dfs_rec_buf[i+1],
                    (dfs_rec_count-i-1)*sizeof(dfs_rec_buf[i]));
                i--;
            }
            dfs_rec_count--;
        }
    }

    /* just pass back our updated total buffer size -- no need to update buffer */
    *dfs_buf_sz = dfs_rec_count * sizeof(struct darshan_dfs_file);

    dfs_runtime->frozen = 1;

    DFS_UNLOCK();
    return;
}

static void dfs_cleanup()
{
    struct dfs_mount_info *mnt_info, *tmp;

    DFS_LOCK();
    assert(dfs_runtime);

    /* cleanup internal structures used for instrumenting */
    darshan_iter_record_refs(dfs_runtime->rec_id_hash,
        &dfs_finalize_file_records, NULL);
    darshan_clear_record_refs(&(dfs_runtime->file_obj_hash), 0);
    darshan_clear_record_refs(&(dfs_runtime->rec_id_hash), 1);

    HASH_ITER(hlink, dfs_runtime->mount_hash, mnt_info, tmp)
    {
        HASH_DELETE(hlink, dfs_runtime->mount_hash, mnt_info);
        free(mnt_info);
    }

    free(dfs_runtime);
    dfs_runtime = NULL;
    dfs_runtime_init_attempted = 0;

    DFS_UNLOCK();
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
