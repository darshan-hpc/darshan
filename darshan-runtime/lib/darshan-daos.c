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

/* container access routines intercepted for maintaining pool/container UUIDs */
DARSHAN_FORWARD_DECL(daos_cont_open, int, (daos_handle_t poh, const char *cont, unsigned int flags, daos_handle_t *coh, daos_cont_info_t *info, daos_event_t *ev));
DARSHAN_FORWARD_DECL(daos_cont_global2local, int, (daos_handle_t poh, d_iov_t glob, daos_handle_t *coh));
DARSHAN_FORWARD_DECL(daos_cont_close, int, (daos_handle_t coh, daos_event_t *ev));

/* multi-level key array API */
DARSHAN_FORWARD_DECL(daos_obj_open, int, (daos_handle_t coh, daos_obj_id_t oid, unsigned int mode, daos_handle_t *oh, daos_event_t *ev));
DARSHAN_FORWARD_DECL(daos_obj_fetch, int, (daos_handle_t oh, daos_handle_t th, uint64_t flags, daos_key_t *dkey, unsigned int nr, daos_iod_t *iods, d_sg_list_t *sgls, daos_iom_t *ioms, daos_event_t *ev));
DARSHAN_FORWARD_DECL(daos_obj_update, int, (daos_handle_t oh, daos_handle_t th, uint64_t flags, daos_key_t *dkey, unsigned int nr, daos_iod_t *iods, d_sg_list_t *sgls, daos_event_t *ev));
DARSHAN_FORWARD_DECL(daos_obj_punch, int, (daos_handle_t oh, daos_handle_t th, uint64_t flags, daos_event_t *ev));
DARSHAN_FORWARD_DECL(daos_obj_punch_dkeys, int, (daos_handle_t oh, daos_handle_t th, uint64_t flags, unsigned int nr, daos_key_t *dkeys, daos_event_t *ev));
DARSHAN_FORWARD_DECL(daos_obj_punch_akeys, int, (daos_handle_t oh, daos_handle_t th, uint64_t flags, daos_key_t *dkey, unsigned int nr, daos_key_t *akeys, daos_event_t *ev));
DARSHAN_FORWARD_DECL(daos_obj_list_dkey, int, (daos_handle_t oh, daos_handle_t th, uint32_t *nr, daos_key_desc_t *kds, d_sg_list_t *sgl, daos_anchor_t *anchor, daos_event_t *ev));
DARSHAN_FORWARD_DECL(daos_obj_list_akey, int, (daos_handle_t oh, daos_handle_t th, daos_key_t *dkey, uint32_t *nr, daos_key_desc_t *kds, d_sg_list_t *sgl, daos_anchor_t *anchor, daos_event_t *ev));
DARSHAN_FORWARD_DECL(daos_obj_list_recx, int, (daos_handle_t oh, daos_handle_t th, daos_key_t *dkey, daos_key_t *akey, daos_size_t *size, uint32_t *nr, daos_recx_t *recxs, daos_epoch_range_t *eprs, daos_anchor_t *anchor, bool incr_order, daos_event_t *ev));
DARSHAN_FORWARD_DECL(daos_obj_close, int, (daos_handle_t oh, daos_event_t *ev));

/* array API */
DARSHAN_FORWARD_DECL(daos_array_create, int, (daos_handle_t coh, daos_obj_id_t oid, daos_handle_t th, daos_size_t cell_size, daos_size_t chunk_size, daos_handle_t *oh, daos_event_t *ev));
DARSHAN_FORWARD_DECL(daos_array_open, int, (daos_handle_t coh, daos_obj_id_t oid, daos_handle_t th, unsigned int mode, daos_size_t *cell_size, daos_size_t *chunk_size, daos_handle_t *oh, daos_event_t *ev));
DARSHAN_FORWARD_DECL(daos_array_open_with_attr, int, (daos_handle_t coh, daos_obj_id_t oid, daos_handle_t th, unsigned int mode, daos_size_t cell_size, daos_size_t chunk_size, daos_handle_t *oh, daos_event_t *ev));
DARSHAN_FORWARD_DECL(daos_array_read, int, (daos_handle_t oh, daos_handle_t th, daos_array_iod_t *iod, d_sg_list_t *sgl, daos_event_t *ev));
DARSHAN_FORWARD_DECL(daos_array_write, int, (daos_handle_t oh, daos_handle_t th, daos_array_iod_t *iod, d_sg_list_t *sgl, daos_event_t *ev));
DARSHAN_FORWARD_DECL(daos_array_get_size, int, (daos_handle_t oh, daos_handle_t th, daos_size_t *size, daos_event_t *ev));
DARSHAN_FORWARD_DECL(daos_array_set_size, int, (daos_handle_t oh, daos_handle_t th, daos_size_t size, daos_event_t *ev));
DARSHAN_FORWARD_DECL(daos_array_stat, int, (daos_handle_t oh, daos_handle_t th, daos_array_stbuf_t *stbuf, daos_event_t *ev));
DARSHAN_FORWARD_DECL(daos_array_punch, int, (daos_handle_t oh, daos_handle_t th, daos_array_iod_t *iod, daos_event_t *ev));
DARSHAN_FORWARD_DECL(daos_array_destroy, int, (daos_handle_t oh, daos_handle_t th, daos_event_t *ev));
DARSHAN_FORWARD_DECL(daos_array_close, int, (daos_handle_t oh, daos_event_t *ev));

/* key-value API */
DARSHAN_FORWARD_DECL(daos_kv_open, int, (daos_handle_t coh, daos_obj_id_t oid, unsigned int mode, daos_handle_t *oh, daos_event_t *ev));
DARSHAN_FORWARD_DECL(daos_kv_get, int, (daos_handle_t oh, daos_handle_t th, uint64_t flags, const char *key, daos_size_t *size, void *buf, daos_event_t *ev));
DARSHAN_FORWARD_DECL(daos_kv_put, int, (daos_handle_t oh, daos_handle_t th, uint64_t flags, const char *key, daos_size_t size, const void *buf, daos_event_t *ev));
DARSHAN_FORWARD_DECL(daos_kv_remove, int, (daos_handle_t oh, daos_handle_t th, uint64_t flags, const char *key, daos_event_t *ev));
DARSHAN_FORWARD_DECL(daos_kv_list, int, (daos_handle_t oh, daos_handle_t th, uint32_t *nr, daos_key_desc_t *kds, d_sg_list_t *sgl, daos_anchor_t *anchor, daos_event_t *ev));
DARSHAN_FORWARD_DECL(daos_kv_destroy, int, (daos_handle_t oh, daos_handle_t th, daos_event_t *ev));
DARSHAN_FORWARD_DECL(daos_kv_close, int, (daos_handle_t oh, daos_event_t *ev));

/* The daos_object_record_ref structure maintains necessary runtime metadata
 * for the DAOS object record (darshan_daos_object structure, defined in
 * darshan-daos-log-format.h) pointed to by 'object_rec'. This metadata
 * assists with the instrumenting of specific statistics in the object record.
 *
 * RATIONALE: the DAOS module needs to track some stateful, volatile
 * information about each open object (like the most recent
 * access time, etc.) to aid in instrumentation, but this information can't be
 * stored in the darshan_daos_object struct because we don't want it to appear in
 * the final darshan log file. We therefore associate a daos_object_record_ref
 * struct with each darshan_daos_object struct in order to track this information
 * (i.e., the mapping between daos_object_record_ref structs to darshan_daos_object
 * structs is one-to-one).
 *
 * NOTE: we use the 'darshan_record_ref' interface (in darshan-common) to
 * associate different types of handles with this daos_object_record_ref struct.
 * This allows us to index this struct (and the underlying object record) by using
 * either the corresponding Darshan record identifier (derived from the object OID)
 * or by a DAOS object handle, for instance. Note that, while there should
 * only be a single Darshan record identifier that indexes a daos_object_record_ref,
 * there could be multiple open object handles that index it.
 */
struct daos_object_record_ref
{
    struct darshan_daos_object *object_rec;
    enum darshan_io_type last_io_type;
    double last_meta_end;
    double last_read_end;
    double last_write_end;
    void *access_root;
    int access_count;
};

struct daos_poolcont_info
{
    daos_handle_t coh;
    uuid_t pool_uuid;
    uuid_t cont_uuid;
    UT_hash_handle hlink;
};

struct daos_runtime
{
    struct daos_poolcont_info *poolcont_hash;
    void *rec_id_hash;
    void *oh_hash;
    int obj_rec_count;
    darshan_record_id heatmap_id;
    int frozen; /* flag to indicate that the counters should no longer be modified */
};

static void daos_runtime_initialize();
static struct daos_object_record_ref *daos_track_new_object_record(
    darshan_record_id rec_id, daos_obj_id_t oid, struct daos_poolcont_info *poolcont_info);
static void daos_finalize_object_records(
    void *rec_ref_p, void *user_ptr);
#ifdef HAVE_MPI
static void daos_record_reduction_op(
    void* inobj_v, void* inoutobj_v, int *len, MPI_Datatype *datatype);
static void daos_mpi_redux(
    void *daos_buf, MPI_Comm mod_comm,
    darshan_record_id *shared_recs, int shared_rec_count);
#endif
static void daos_output(
    void **daos_buf, int *daos_buf_sz);
static void daos_cleanup(
    void);

static struct daos_runtime *daos_runtime = NULL;
static pthread_mutex_t daos_runtime_mutex = PTHREAD_RECURSIVE_MUTEX_INITIALIZER_NP;
static int daos_runtime_init_attempted = 0;
static int my_rank = -1;

#define DAOS_LOCK() pthread_mutex_lock(&daos_runtime_mutex)
#define DAOS_UNLOCK() pthread_mutex_unlock(&daos_runtime_mutex)

#define DAOS_WTIME() \
    __darshan_disabled ? 0 : darshan_core_wtime();

#define DAOS_PRE_RECORD() do { \
    if(!ret && !__darshan_disabled) { \
        DAOS_LOCK(); \
        if(!daos_runtime && !daos_runtime_init_attempted) \
            daos_runtime_initialize(); \
        if(daos_runtime && !daos_runtime->frozen) break; \
        DAOS_UNLOCK(); \
    } \
    return(ret); \
} while(0)

#define DAOS_POST_RECORD() do { \
    DAOS_UNLOCK(); \
} while(0)

#define DAOS_STORE_POOLCONT_INFO(__poh, __coh_p) do { \
    int __query_ret; \
    daos_pool_info_t __pool_info; \
    daos_cont_info_t __cont_info; \
    struct daos_poolcont_info *__poolcont_info; \
    __query_ret = daos_pool_query(__poh, NULL, &__pool_info, NULL, NULL); \
    if(__query_ret == 0) { \
        __query_ret = daos_cont_query(*__coh_p, &__cont_info, NULL, NULL); \
        if(__query_ret == 0) { \
            __poolcont_info = malloc(sizeof(*__poolcont_info)); \
            if(__poolcont_info) { \
                uuid_copy(__poolcont_info->pool_uuid, __pool_info.pi_uuid); \
                uuid_copy(__poolcont_info->cont_uuid, __cont_info.ci_uuid); \
                __poolcont_info->coh = *__coh_p; \
                HASH_ADD(hlink, daos_runtime->poolcont_hash, coh, sizeof(*__coh_p), __poolcont_info); \
            } \
        } \
    } \
} while(0)

#define DAOS_GET_POOLCONT_INFO(__coh, __poolcont_info) \
    HASH_FIND(hlink, daos_runtime->poolcont_hash, &__coh, sizeof(__coh), __poolcont_info)

#define DAOS_FREE_POOLCONT_INFO(__poolcont_info) do { \
    HASH_DELETE(hlink, daos_runtime->poolcont_hash, __poolcont_info); \
    free(__poolcont_info); \
} while(0)

#define ID_GLOB_SIZE (sizeof(daos_obj_id_t) + (2*sizeof(uuid_t)))
#define DAOS_RECORD_OBJ_OPEN(__coh, __oh_p, __oid, __counter, __cell_sz, __chunk_sz, __is_async, __tm1, __tm2) do { \
    struct daos_poolcont_info *__poolcont_info; \
    unsigned char __id_glob[ID_GLOB_SIZE]; \
    darshan_record_id __rec_id; \
    struct daos_object_record_ref *__rec_ref; \
    DAOS_GET_POOLCONT_INFO(__coh, __poolcont_info); \
    if(!__poolcont_info) break; \
    memcpy(__id_glob, __poolcont_info->pool_uuid, sizeof(__poolcont_info->pool_uuid)); \
    memcpy(__id_glob+sizeof(__poolcont_info->pool_uuid), __poolcont_info->cont_uuid, sizeof(__poolcont_info->cont_uuid)); \
    memcpy(__id_glob+sizeof(__poolcont_info->pool_uuid)+sizeof(__poolcont_info->cont_uuid), &__oid, sizeof(__oid)); \
    __rec_id = darshan_hash(__id_glob, ID_GLOB_SIZE, 0); \
    __rec_ref = darshan_lookup_record_ref(daos_runtime->rec_id_hash, &__rec_id, \
        sizeof(darshan_record_id)); \
    if(!__rec_ref) __rec_ref = daos_track_new_object_record(__rec_id, __oid, __poolcont_info); \
    if(!__rec_ref) break; \
    __rec_ref->object_rec->counters[__counter] += 1; \
    if(__is_async) __rec_ref->object_rec->counters[DAOS_NB_OPS] += 1; \
    if(__cell_sz) __rec_ref->object_rec->counters[DAOS_ARRAY_CELL_SIZE] = __cell_sz; \
    if(__chunk_sz)  __rec_ref->object_rec->counters[DAOS_ARRAY_CHUNK_SIZE] = __chunk_sz; \
    __rec_ref->object_rec->counters[DAOS_OBJ_OTYPE] = daos_obj_id2type(__oid); \
    if(__rec_ref->object_rec->fcounters[DAOS_F_OPEN_START_TIMESTAMP] == 0 || \
     __rec_ref->object_rec->fcounters[DAOS_F_OPEN_START_TIMESTAMP] > __tm1) \
        __rec_ref->object_rec->fcounters[DAOS_F_OPEN_START_TIMESTAMP] = __tm1; \
    __rec_ref->object_rec->fcounters[DAOS_F_OPEN_END_TIMESTAMP] = __tm2; \
    DARSHAN_TIMER_INC_NO_OVERLAP(__rec_ref->object_rec->fcounters[DAOS_F_META_TIME], \
        __tm1, __tm2, __rec_ref->last_meta_end); \
    darshan_add_record_ref(&(daos_runtime->oh_hash), __oh_p, \
        sizeof(daos_handle_t), __rec_ref); \
} while(0)

#define DAOS_RECORD_OBJ_READ(__oh, __counter, __sz, __is_async, __tm1, __tm2) do { \
    int64_t __tmp_sz = (int64_t)__sz; \
    struct darshan_common_val_counter *__cvc; \
    double __elapsed = __tm2-__tm1; \
    struct daos_object_record_ref *__rec_ref; \
    __rec_ref = darshan_lookup_record_ref(daos_runtime->oh_hash, &__oh, \
        sizeof(daos_handle_t)); \
    if(!__rec_ref) break; \
    if(__counter == DAOS_ARRAY_READS) \
        __tmp_sz *= __rec_ref->object_rec->counters[DAOS_ARRAY_CELL_SIZE];\
    /* heatmap to record traffic summary */ \
    heatmap_update(daos_runtime->heatmap_id, HEATMAP_READ, __tmp_sz, __tm1, __tm2); \
    __rec_ref->object_rec->counters[__counter] += 1; \
    if(__is_async) __rec_ref->object_rec->counters[DAOS_NB_OPS] += 1; \
    __rec_ref->object_rec->counters[DAOS_BYTES_READ] += __tmp_sz; \
    DARSHAN_BUCKET_INC(&(__rec_ref->object_rec->counters[DAOS_SIZE_READ_0_100]), __tmp_sz); \
    __cvc = darshan_track_common_val_counters(&__rec_ref->access_root, &__tmp_sz, 1, \
        &__rec_ref->access_count); \
    if(__cvc) DARSHAN_UPDATE_COMMON_VAL_COUNTERS( \
        &(__rec_ref->object_rec->counters[DAOS_ACCESS1_ACCESS]), \
        &(__rec_ref->object_rec->counters[DAOS_ACCESS1_COUNT]), \
        __cvc->vals, 1, __cvc->freq, 0); \
    if(__rec_ref->last_io_type == DARSHAN_IO_WRITE) \
        __rec_ref->object_rec->counters[DAOS_RW_SWITCHES] += 1; \
    __rec_ref->last_io_type = DARSHAN_IO_READ; \
    if(__rec_ref->object_rec->fcounters[DAOS_F_READ_START_TIMESTAMP] == 0 || \
     __rec_ref->object_rec->fcounters[DAOS_F_READ_START_TIMESTAMP] > __tm1) \
        __rec_ref->object_rec->fcounters[DAOS_F_READ_START_TIMESTAMP] = __tm1; \
    __rec_ref->object_rec->fcounters[DAOS_F_READ_END_TIMESTAMP] = __tm2; \
    if(__rec_ref->object_rec->fcounters[DAOS_F_MAX_READ_TIME] < __elapsed) { \
        __rec_ref->object_rec->fcounters[DAOS_F_MAX_READ_TIME] = __elapsed; \
        __rec_ref->object_rec->counters[DAOS_MAX_READ_TIME_SIZE] = __tmp_sz; \
    } \
    DARSHAN_TIMER_INC_NO_OVERLAP(__rec_ref->object_rec->fcounters[DAOS_F_READ_TIME], \
        __tm1, __tm2, __rec_ref->last_read_end); \
} while(0)

#define DAOS_RECORD_OBJ_WRITE(__oh, __counter, __sz, __is_async, __tm1, __tm2) do { \
    int64_t __tmp_sz = (int64_t)__sz; \
    struct darshan_common_val_counter *__cvc; \
    double __elapsed = __tm2-__tm1; \
    struct daos_object_record_ref *__rec_ref; \
    __rec_ref = darshan_lookup_record_ref(daos_runtime->oh_hash, &__oh, \
        sizeof(daos_handle_t)); \
    if(!__rec_ref) break; \
    if(__counter == DAOS_ARRAY_WRITES) \
        __tmp_sz *= __rec_ref->object_rec->counters[DAOS_ARRAY_CELL_SIZE];\
    /* heatmap to record traffic summary */ \
    heatmap_update(daos_runtime->heatmap_id, HEATMAP_WRITE, __tmp_sz, __tm1, __tm2); \
    __rec_ref->object_rec->counters[__counter] += 1; \
    if(__is_async) __rec_ref->object_rec->counters[DAOS_NB_OPS] += 1; \
    __rec_ref->object_rec->counters[DAOS_BYTES_WRITTEN] += __tmp_sz; \
    DARSHAN_BUCKET_INC(&(__rec_ref->object_rec->counters[DAOS_SIZE_WRITE_0_100]), __tmp_sz); \
    __cvc = darshan_track_common_val_counters(&__rec_ref->access_root, &__tmp_sz, 1, \
        &__rec_ref->access_count); \
    if(__cvc) DARSHAN_UPDATE_COMMON_VAL_COUNTERS( \
        &(__rec_ref->object_rec->counters[DAOS_ACCESS1_ACCESS]), \
        &(__rec_ref->object_rec->counters[DAOS_ACCESS1_COUNT]), \
        __cvc->vals, 1, __cvc->freq, 0); \
    if(__rec_ref->last_io_type == DARSHAN_IO_READ) \
        __rec_ref->object_rec->counters[DAOS_RW_SWITCHES] += 1; \
    __rec_ref->last_io_type = DARSHAN_IO_WRITE; \
    if(__rec_ref->object_rec->fcounters[DAOS_F_WRITE_START_TIMESTAMP] == 0 || \
     __rec_ref->object_rec->fcounters[DAOS_F_WRITE_START_TIMESTAMP] > __tm1) \
        __rec_ref->object_rec->fcounters[DAOS_F_WRITE_START_TIMESTAMP] = __tm1; \
    __rec_ref->object_rec->fcounters[DAOS_F_WRITE_END_TIMESTAMP] = __tm2; \
    if(__rec_ref->object_rec->fcounters[DAOS_F_MAX_WRITE_TIME] < __elapsed) { \
        __rec_ref->object_rec->fcounters[DAOS_F_MAX_WRITE_TIME] = __elapsed; \
        __rec_ref->object_rec->counters[DAOS_MAX_WRITE_TIME_SIZE] = __tmp_sz; \
    } \
    DARSHAN_TIMER_INC_NO_OVERLAP(__rec_ref->object_rec->fcounters[DAOS_F_WRITE_TIME], \
        __tm1, __tm2, __rec_ref->last_write_end); \
} while(0)

#define DAOS_RECORD_OBJ_CLOSE(__oh, __tm1, __tm2) do { \
    struct daos_object_record_ref *__rec_ref; \
    __rec_ref = darshan_lookup_record_ref(daos_runtime->oh_hash, &__oh, \
        sizeof(daos_handle_t)); \
    if(!__rec_ref) break; \
    if(__rec_ref->object_rec->fcounters[DAOS_F_CLOSE_START_TIMESTAMP] == 0 || \
     __rec_ref->object_rec->fcounters[DAOS_F_CLOSE_START_TIMESTAMP] > __tm1) \
        __rec_ref->object_rec->fcounters[DAOS_F_CLOSE_START_TIMESTAMP] = __tm1; \
    __rec_ref->object_rec->fcounters[DAOS_F_CLOSE_END_TIMESTAMP] = __tm2; \
    DARSHAN_TIMER_INC_NO_OVERLAP(__rec_ref->object_rec->fcounters[DAOS_F_META_TIME], \
        __tm1, __tm2, __rec_ref->last_meta_end); \
    darshan_delete_record_ref(&(daos_runtime->oh_hash), &__oh, sizeof(daos_handle_t)); \
} while(0)

/* DAOS callback routine to measure end of async open calls */
struct daos_open_event_tracker
{
    double tm1;
    daos_handle_t coh;
    daos_obj_id_t oid;
    int op;
    int resolve_sizes;
    union
    {
        daos_size_t cell_size;
        daos_size_t *cell_size_p;
    };
    union
    {
        daos_size_t chunk_size;
        daos_size_t *chunk_size_p;
    };
    daos_handle_t *oh_p;
};
int darshan_daos_open_comp_cb(void *arg, daos_event_t *ev, int ret)
{
    struct daos_open_event_tracker *tracker = (struct daos_open_event_tracker *)arg;

    if (ret == 0)
    {
        /* async operation completed successfully, capture Darshan statistics */
        double tm2 = darshan_core_wtime();
        if (!tracker->resolve_sizes)
            DAOS_RECORD_OBJ_OPEN(tracker->coh, tracker->oh_p, tracker->oid, tracker->op,
                tracker->cell_size, tracker->chunk_size, 1, tracker->tm1, tm2);
        else
            DAOS_RECORD_OBJ_OPEN(tracker->coh, tracker->oh_p, tracker->oid, tracker->op,
                *(tracker->cell_size_p), *(tracker->chunk_size_p), 1, tracker->tm1, tm2);
    }
    free(tracker);

    return 0;
}

/* DAOS callback routine to measure end of async read calls */
struct daos_read_event_tracker
{
    double tm1;
    daos_handle_t oh;
    int op;
    union
    {
        daos_size_t read_size;
        daos_size_t *read_size_p;
    };
};
int darshan_daos_read_comp_cb(void *arg, daos_event_t *ev, int ret)
{
    struct daos_read_event_tracker *tracker = (struct daos_read_event_tracker *)arg;

    if (ret == 0)
    {
        /* async operation completed successfully, capture Darshan statistics */
        double tm2 = darshan_core_wtime();
        if (tracker->op != DAOS_KV_GETS)
            DAOS_RECORD_OBJ_READ(tracker->oh, tracker->op, tracker->read_size, 1,
                tracker->tm1, tm2);
        else
            DAOS_RECORD_OBJ_READ(tracker->oh, tracker->op, *(tracker->read_size_p), 1,
                tracker->tm1, tm2);
    }
    free(tracker);

    return 0;
}

/* DAOS callback routine to measure end of async write calls */
struct daos_write_event_tracker
{
    double tm1;
    daos_handle_t oh;
    int op;
    daos_size_t write_size;
};
int darshan_daos_write_comp_cb(void *arg, daos_event_t *ev, int ret)
{
    struct daos_write_event_tracker *tracker = (struct daos_write_event_tracker *)arg;

    if (ret == 0)
    {
        /* async operation completed successfully, capture Darshan statistics */
        double tm2 = darshan_core_wtime();
        DAOS_RECORD_OBJ_WRITE(tracker->oh, tracker->op, tracker->write_size, 1,
            tracker->tm1, tm2);
    }
    free(tracker);

    return 0;
}

/* DAOS callback routine to measure end of async "metadata" calls */
struct daos_meta_event_tracker
{
    double tm1;
    daos_handle_t oh;
    int op;
};
int darshan_daos_meta_comp_cb(void *arg, daos_event_t *ev, int ret)
{
    struct daos_meta_event_tracker *tracker = (struct daos_meta_event_tracker *)arg;

    if (ret == 0)
    {
        /* async operation completed successfully, capture Darshan statistics */
        double tm2 = darshan_core_wtime();
        struct daos_object_record_ref *rec_ref;
        rec_ref = darshan_lookup_record_ref(daos_runtime->oh_hash,
            &tracker->oh, sizeof(daos_handle_t));
        if(rec_ref)
        {
            DARSHAN_TIMER_INC_NO_OVERLAP(
                rec_ref->object_rec->fcounters[DAOS_F_META_TIME],
                tracker->tm1, tm2, rec_ref->last_meta_end);
            rec_ref->object_rec->counters[tracker->op] += 1;
            rec_ref->object_rec->counters[DAOS_NB_OPS] += 1;
        }

    }
    free(tracker);

    return 0;
}

/* DAOS callback routine to measure end of async close calls */
struct daos_close_event_tracker
{
    double tm1;
    daos_handle_t oh;
};
int darshan_daos_close_comp_cb(void *arg, daos_event_t *ev, int ret)
{
    struct daos_close_event_tracker *tracker = (struct daos_close_event_tracker *)arg;

    if (ret == 0)
    {
        /* async operation completed successfully, capture Darshan statistics */
        double tm2 = darshan_core_wtime();
        DAOS_RECORD_OBJ_CLOSE(tracker->oh, tracker->tm1, tm2);
    }
    free(tracker);

    return 0;
}

/* DAOS callback routine to capture key details from container open calls */
struct daos_contopen_event_tracker
{
    daos_handle_t poh;
    daos_handle_t *coh_p;
};
int darshan_daos_contopen_comp_cb(void *arg, daos_event_t *ev, int ret)
{
    struct daos_contopen_event_tracker *tracker = (struct daos_contopen_event_tracker *)arg;

    if (ret == 0)
    {
        /* async operation completed successfully, capture container info */
        DAOS_STORE_POOLCONT_INFO(tracker->poh, tracker->coh_p);
    }
    free(tracker);

    return 0;
}

/*****************************************************
 *      Wrappers for DAOS functions of interest      * 
 *****************************************************/

/* container access routines intercepted for maintaining pool/container UUIDs */

int DARSHAN_DECL(daos_cont_open)(daos_handle_t poh, const char *cont, unsigned int flags,
    daos_handle_t *coh, daos_cont_info_t *info, daos_event_t *ev)
{
    int ret;

    MAP_OR_FAIL(daos_cont_open);

    if(ev)
    {
        /* setup callback to capture the container open operation upon completion */
        struct daos_contopen_event_tracker *tracker = malloc(sizeof(*tracker));
        if (tracker)
        {
            tracker->poh = poh;
            tracker->coh_p = coh;
            daos_event_register_comp_cb(ev, darshan_daos_contopen_comp_cb, tracker);
        }
    }

    ret = __real_daos_cont_open(poh, cont, flags, coh, info, ev);

    if(!ev)
    {
        DAOS_PRE_RECORD();
        DAOS_STORE_POOLCONT_INFO(poh, coh);
        DAOS_POST_RECORD();
    }

    return(ret);
}

int DARSHAN_DECL(daos_cont_global2local)(daos_handle_t poh, d_iov_t glob, daos_handle_t *coh)
{
    int ret;

    MAP_OR_FAIL(daos_cont_global2local);

    ret = __real_daos_cont_global2local(poh, glob, coh);

    DAOS_PRE_RECORD();
    DAOS_STORE_POOLCONT_INFO(poh, coh);
    DAOS_POST_RECORD();

    return(ret);
}

int DARSHAN_DECL(daos_cont_close)(daos_handle_t coh, daos_event_t *ev)
{
    int ret;
    struct daos_poolcont_info *poolcont_info;

    MAP_OR_FAIL(daos_cont_close);

    if(!__darshan_disabled)
    {
        DAOS_LOCK();
        if(daos_runtime && !daos_runtime->frozen)
        {
            DAOS_GET_POOLCONT_INFO(coh, poolcont_info);
            if(poolcont_info)
                DAOS_FREE_POOLCONT_INFO(poolcont_info);
        }
        DAOS_UNLOCK();
    }

    ret = __real_daos_cont_close(coh, ev);

    return(ret);
}

/* multi-level key array API */

int DARSHAN_DECL(daos_obj_open)(daos_handle_t coh, daos_obj_id_t oid, unsigned int mode, daos_handle_t *oh, daos_event_t *ev)
{
    int ret;
    double tm1, tm2;

    MAP_OR_FAIL(daos_obj_open);

    if(ev)
    {
        /* setup callback to record the open operation upon completion */
        struct daos_open_event_tracker *tracker = malloc(sizeof(*tracker));
        if (tracker)
        {
            tracker->tm1 = DAOS_WTIME();
            tracker->coh = coh;
            tracker->oid = oid;
            tracker->op = DAOS_OBJ_OPENS;
            tracker->resolve_sizes = 0;
            tracker->cell_size = 0;
            tracker->chunk_size = 0;
            tracker->oh_p = oh;
            daos_event_register_comp_cb(ev, darshan_daos_open_comp_cb, tracker);
        }
    }

    tm1 = DAOS_WTIME();
    ret = __real_daos_obj_open(coh, oid, mode, oh, ev);
    tm2 = DAOS_WTIME();

    if(!ev)
    {
        /* only record here for synchronous I/O operations */
        DAOS_PRE_RECORD();
        DAOS_RECORD_OBJ_OPEN(coh, oh, oid, DAOS_OBJ_OPENS, 0, 0, 0, tm1, tm2);
        DAOS_POST_RECORD();
    }

    return(ret);
}

#define DAOS_OBJ_IOD_SZ(__iods, __nr, __sz) do { \
    int __i, __j; \
    __sz = 0; \
    for(__i = 0; __i < __nr; __i++) { \
        if(__iods[__i].iod_type == DAOS_IOD_SINGLE) \
            __sz += __iods[__i].iod_size; \
        else if(__iods[__i].iod_recxs) \
            for(__j = 0; __j < __iods[__i].iod_nr; __j++) \
                __sz += (__iods[__i].iod_size * __iods[__i].iod_recxs[__j].rx_nr); \
    } \
} while(0)

int DARSHAN_DECL(daos_obj_fetch)(daos_handle_t oh, daos_handle_t th, uint64_t flags,
    daos_key_t *dkey, unsigned int nr, daos_iod_t *iods, d_sg_list_t *sgls,
    daos_iom_t *ioms, daos_event_t *ev)
{
	int ret;
    double tm1, tm2;
    daos_size_t fetch_sz;

    MAP_OR_FAIL(daos_obj_fetch);

    DAOS_OBJ_IOD_SZ(iods, nr, fetch_sz);

    if(ev)
    {
        /* setup callback to record the read operation upon completion */
        struct daos_read_event_tracker *tracker = malloc(sizeof(*tracker));
        if (tracker)
        {
            tracker->tm1 = DAOS_WTIME();
            tracker->oh = oh;
            tracker->op = DAOS_OBJ_FETCHES;
            tracker->read_size = fetch_sz;
            daos_event_register_comp_cb(ev, darshan_daos_read_comp_cb, tracker);
        }
    }

    tm1 = DAOS_WTIME();
    ret = __real_daos_obj_fetch(oh, th, flags, dkey, nr, iods, sgls, ioms, ev);
    tm2 = DAOS_WTIME();

    if(!ev)
    {
        /* only record here for synchronous I/O operations */
        DAOS_PRE_RECORD();
        DAOS_RECORD_OBJ_READ(oh, DAOS_OBJ_FETCHES, fetch_sz, 0, tm1, tm2);
        DAOS_POST_RECORD();
    }

    return(ret);
}

int DARSHAN_DECL(daos_obj_update)(daos_handle_t oh, daos_handle_t th, uint64_t flags,
    daos_key_t *dkey, unsigned int nr, daos_iod_t *iods, d_sg_list_t *sgls,
    daos_event_t *ev)
{
    int ret;
    double tm1, tm2;
    daos_size_t update_sz;

    MAP_OR_FAIL(daos_obj_update);

    DAOS_OBJ_IOD_SZ(iods, nr, update_sz);

    if(ev)
    {
        /* setup callback to record the write operation upon completion */
        struct daos_write_event_tracker *tracker = malloc(sizeof(*tracker));
        if (tracker)
        {
            tracker->tm1 = DAOS_WTIME();
            tracker->oh = oh;
            tracker->op = DAOS_OBJ_UPDATES;
            tracker->write_size = update_sz;
            daos_event_register_comp_cb(ev, darshan_daos_write_comp_cb, tracker);
        }
    }

    tm1 = DAOS_WTIME();
    ret = __real_daos_obj_update(oh, th, flags, dkey, nr, iods, sgls, ev);
    tm2 = DAOS_WTIME();

    if(!ev)
    {
        /* only record here for synchronous I/O operations */
        DAOS_PRE_RECORD();
        DAOS_RECORD_OBJ_WRITE(oh, DAOS_OBJ_UPDATES, update_sz, 0, tm1, tm2);
        DAOS_POST_RECORD();
    }

    return(ret);
}

int DARSHAN_DECL(daos_obj_punch)(daos_handle_t oh, daos_handle_t th, uint64_t flags,
    daos_event_t *ev)
{
    int ret;
    double tm1, tm2;
    struct daos_object_record_ref *rec_ref;

    MAP_OR_FAIL(daos_obj_punch);

    if(ev)
    {
        /* setup callback to record the metadata operation upon completion */
        struct daos_meta_event_tracker *tracker = malloc(sizeof(*tracker));
        if (tracker)
        {
            tracker->tm1 = DAOS_WTIME();
            tracker->oh = oh;
            tracker->op = DAOS_OBJ_PUNCHES;
            daos_event_register_comp_cb(ev, darshan_daos_meta_comp_cb, tracker);
        }
    }

    tm1 = DAOS_WTIME();
    ret = __real_daos_obj_punch(oh, th, flags, ev);
    tm2 = DAOS_WTIME();

    if(!ev)
    {
        /* only record here for synchronous I/O operations */
        DAOS_PRE_RECORD();
        rec_ref = darshan_lookup_record_ref(daos_runtime->oh_hash,
            &oh, sizeof(daos_handle_t));
        if(rec_ref)
        {
            DARSHAN_TIMER_INC_NO_OVERLAP(
                rec_ref->object_rec->fcounters[DAOS_F_META_TIME],
                tm1, tm2, rec_ref->last_meta_end);
            rec_ref->object_rec->counters[DAOS_OBJ_PUNCHES] += 1;
        }
        DAOS_POST_RECORD();
    }

    return(ret);
}

int DARSHAN_DECL(daos_obj_punch_dkeys)(daos_handle_t oh, daos_handle_t th, uint64_t flags,
    unsigned int nr, daos_key_t *dkeys, daos_event_t *ev)
{
    int ret;
    double tm1, tm2;
    struct daos_object_record_ref *rec_ref;

    MAP_OR_FAIL(daos_obj_punch_dkeys);

    if(ev)
    {
        /* setup callback to record the metadata operation upon completion */
        struct daos_meta_event_tracker *tracker = malloc(sizeof(*tracker));
        if (tracker)
        {
            tracker->tm1 = DAOS_WTIME();
            tracker->oh = oh;
            tracker->op = DAOS_OBJ_DKEY_PUNCHES;
            daos_event_register_comp_cb(ev, darshan_daos_meta_comp_cb, tracker);
        }
    }

    tm1 = DAOS_WTIME();
    ret = __real_daos_obj_punch_dkeys(oh, th, flags, nr, dkeys, ev);
    tm2 = DAOS_WTIME();

    if(!ev)
    {
        /* only record here for synchronous I/O operations */
        DAOS_PRE_RECORD();
        rec_ref = darshan_lookup_record_ref(daos_runtime->oh_hash,
            &oh, sizeof(daos_handle_t));
        if(rec_ref)
        {
            DARSHAN_TIMER_INC_NO_OVERLAP(
                rec_ref->object_rec->fcounters[DAOS_F_META_TIME],
                tm1, tm2, rec_ref->last_meta_end);
            rec_ref->object_rec->counters[DAOS_OBJ_DKEY_PUNCHES] += 1;
        }
        DAOS_POST_RECORD();
    }

    return(ret);
}

int DARSHAN_DECL(daos_obj_punch_akeys)(daos_handle_t oh, daos_handle_t th, uint64_t flags,
    daos_key_t *dkey, unsigned int nr, daos_key_t *akeys, daos_event_t *ev)
{
    int ret;
    double tm1, tm2;
    struct daos_object_record_ref *rec_ref;

    MAP_OR_FAIL(daos_obj_punch_akeys);

    if(ev)
    {
        /* setup callback to record the metadata operation upon completion */
        struct daos_meta_event_tracker *tracker = malloc(sizeof(*tracker));
        if (tracker)
        {
            tracker->tm1 = DAOS_WTIME();
            tracker->oh = oh;
            tracker->op = DAOS_OBJ_AKEY_PUNCHES;
            daos_event_register_comp_cb(ev, darshan_daos_meta_comp_cb, tracker);
        }
    }

    tm1 = DAOS_WTIME();
    ret = __real_daos_obj_punch_akeys(oh, th, flags, dkey, nr, akeys, ev);
    tm2 = DAOS_WTIME();

    if(!ev)
    {
        /* only record here for synchronous I/O operations */
        DAOS_PRE_RECORD();
        rec_ref = darshan_lookup_record_ref(daos_runtime->oh_hash,
            &oh, sizeof(daos_handle_t));
        if(rec_ref)
        {
            DARSHAN_TIMER_INC_NO_OVERLAP(
                rec_ref->object_rec->fcounters[DAOS_F_META_TIME],
                tm1, tm2, rec_ref->last_meta_end);
            rec_ref->object_rec->counters[DAOS_OBJ_AKEY_PUNCHES] += 1;
        }
        DAOS_POST_RECORD();
    }

    return(ret);
}


int DARSHAN_DECL(daos_obj_list_dkey)(daos_handle_t oh, daos_handle_t th, uint32_t *nr,
    daos_key_desc_t *kds, d_sg_list_t *sgl, daos_anchor_t *anchor, daos_event_t *ev)
{
    int ret;
    double tm1, tm2;
    struct daos_object_record_ref *rec_ref;

    MAP_OR_FAIL(daos_obj_list_dkey);

    if(ev)
    {
        /* setup callback to record the metadata operation upon completion */
        struct daos_meta_event_tracker *tracker = malloc(sizeof(*tracker));
        if (tracker)
        {
            tracker->tm1 = DAOS_WTIME();
            tracker->oh = oh;
            tracker->op = DAOS_OBJ_DKEY_LISTS;
            daos_event_register_comp_cb(ev, darshan_daos_meta_comp_cb, tracker);
        }
    }

    tm1 = DAOS_WTIME();
    ret = __real_daos_obj_list_dkey(oh, th, nr, kds, sgl, anchor, ev);
    tm2 = DAOS_WTIME();

    if(!ev)
    {
        /* only record here for synchronous I/O operations */
        DAOS_PRE_RECORD();
        rec_ref = darshan_lookup_record_ref(daos_runtime->oh_hash,
            &oh, sizeof(daos_handle_t));
        if(rec_ref)
        {
            DARSHAN_TIMER_INC_NO_OVERLAP(
                rec_ref->object_rec->fcounters[DAOS_F_META_TIME],
                tm1, tm2, rec_ref->last_meta_end);
            rec_ref->object_rec->counters[DAOS_OBJ_DKEY_LISTS] += 1;
        }
        DAOS_POST_RECORD();
    }

    return(ret);
}

int DARSHAN_DECL(daos_obj_list_akey)(daos_handle_t oh, daos_handle_t th,
    daos_key_t *dkey, uint32_t *nr, daos_key_desc_t *kds, d_sg_list_t *sgl,
    daos_anchor_t *anchor, daos_event_t *ev)
{
    int ret;
    double tm1, tm2;
    struct daos_object_record_ref *rec_ref;

    MAP_OR_FAIL(daos_obj_list_akey);

    if(ev)
    {
        /* setup callback to record the metadata operation upon completion */
        struct daos_meta_event_tracker *tracker = malloc(sizeof(*tracker));
        if (tracker)
        {
            tracker->tm1 = DAOS_WTIME();
            tracker->oh = oh;
            tracker->op = DAOS_OBJ_AKEY_LISTS;
            daos_event_register_comp_cb(ev, darshan_daos_meta_comp_cb, tracker);
        }
    }

    tm1 = DAOS_WTIME();
    ret = __real_daos_obj_list_akey(oh, th, dkey, nr, kds, sgl, anchor, ev);
    tm2 = DAOS_WTIME();

    if(!ev)
    {
        /* only record here for synchronous I/O operations */
        DAOS_PRE_RECORD();
        rec_ref = darshan_lookup_record_ref(daos_runtime->oh_hash,
            &oh, sizeof(daos_handle_t));
        if(rec_ref)
        {
            DARSHAN_TIMER_INC_NO_OVERLAP(
                rec_ref->object_rec->fcounters[DAOS_F_META_TIME],
                tm1, tm2, rec_ref->last_meta_end);
            rec_ref->object_rec->counters[DAOS_OBJ_AKEY_LISTS] += 1;
        }
        DAOS_POST_RECORD();
    }

    return(ret);
}

int DARSHAN_DECL(daos_obj_list_recx)(daos_handle_t oh, daos_handle_t th,
    daos_key_t *dkey, daos_key_t *akey, daos_size_t *size, uint32_t *nr,
    daos_recx_t *recxs, daos_epoch_range_t *eprs, daos_anchor_t *anchor,
    bool incr_order, daos_event_t *ev)
{
    int ret;
    double tm1, tm2;
    struct daos_object_record_ref *rec_ref;

    MAP_OR_FAIL(daos_obj_list_recx);

    if(ev)
    {
        /* setup callback to record the metadata operation upon completion */
        struct daos_meta_event_tracker *tracker = malloc(sizeof(*tracker));
        if (tracker)
        {
            tracker->tm1 = DAOS_WTIME();
            tracker->oh = oh;
            tracker->op = DAOS_OBJ_RECX_LISTS;
            daos_event_register_comp_cb(ev, darshan_daos_meta_comp_cb, tracker);
        }
    }

    tm1 = DAOS_WTIME();
    ret = __real_daos_obj_list_recx(oh, th, dkey, akey, size, nr, recxs,
        eprs, anchor, incr_order, ev);
    tm2 = DAOS_WTIME();

    if(!ev)
    {
        /* only record here for synchronous I/O operations */
        DAOS_PRE_RECORD();
        rec_ref = darshan_lookup_record_ref(daos_runtime->oh_hash,
            &oh, sizeof(daos_handle_t));
        if(rec_ref)
        {
            DARSHAN_TIMER_INC_NO_OVERLAP(
                rec_ref->object_rec->fcounters[DAOS_F_META_TIME],
                tm1, tm2, rec_ref->last_meta_end);
            rec_ref->object_rec->counters[DAOS_OBJ_RECX_LISTS] += 1;
        }
        DAOS_POST_RECORD();
    }

    return(ret);
}

int DARSHAN_DECL(daos_obj_close)(daos_handle_t oh, daos_event_t *ev)
{
    int ret;
    double tm1, tm2;

    MAP_OR_FAIL(daos_obj_close);

    if(ev)
    {
        /* setup callback to record the close operation upon completion */
        struct daos_close_event_tracker *tracker = malloc(sizeof(*tracker));
        if (tracker)
        {
            tracker->tm1 = DAOS_WTIME();
            tracker->oh = oh;
            daos_event_register_comp_cb(ev, darshan_daos_close_comp_cb, tracker);
        }
    }

    tm1 = DAOS_WTIME();
    ret = __real_daos_obj_close(oh, ev);
    tm2 = DAOS_WTIME();

    if(!ev)
    {
        /* only record here for synchronous I/O operations */
        DAOS_PRE_RECORD();
        DAOS_RECORD_OBJ_CLOSE(oh, tm1, tm2);
        DAOS_POST_RECORD();
    }

    return(ret);
}

/* array API */

int DARSHAN_DECL(daos_array_create)(daos_handle_t coh, daos_obj_id_t oid,
    daos_handle_t th, daos_size_t cell_size, daos_size_t chunk_size,
    daos_handle_t *oh, daos_event_t *ev)
{
    int ret;
    double tm1, tm2;

    MAP_OR_FAIL(daos_array_create);

    if(ev)
    {
        /* setup callback to record the open operation upon completion */
        struct daos_open_event_tracker *tracker = malloc(sizeof(*tracker));
        if (tracker)
        {
            tracker->tm1 = DAOS_WTIME();
            tracker->coh = coh;
            tracker->oid = oid;
            tracker->op = DAOS_ARRAY_OPENS;
            tracker->resolve_sizes = 0;
            tracker->cell_size = cell_size;
            tracker->chunk_size = chunk_size;
            tracker->oh_p = oh;
            daos_event_register_comp_cb(ev, darshan_daos_open_comp_cb, tracker);
        }
    }

    tm1 = DAOS_WTIME();
    ret = __real_daos_array_create(coh, oid, th, cell_size, chunk_size, oh, ev);
    tm2 = DAOS_WTIME();

    if(!ev)
    {
        /* only record here for synchronous I/O operations */
        DAOS_PRE_RECORD();
        DAOS_RECORD_OBJ_OPEN(coh, oh, oid, DAOS_ARRAY_OPENS, cell_size, chunk_size, 0, tm1, tm2);
        DAOS_POST_RECORD();
    }

    return(ret);
}

int DARSHAN_DECL(daos_array_open)(daos_handle_t coh, daos_obj_id_t oid,
    daos_handle_t th, unsigned int mode, daos_size_t *cell_size, daos_size_t *chunk_size,
    daos_handle_t *oh, daos_event_t *ev)
{
    int ret;
    double tm1, tm2;

    MAP_OR_FAIL(daos_array_open);

    if(ev)
    {
        /* setup callback to record the open operation upon completion */
        struct daos_open_event_tracker *tracker = malloc(sizeof(*tracker));
        if (tracker)
        {
            tracker->tm1 = DAOS_WTIME();
            tracker->coh = coh;
            tracker->oid = oid;
            tracker->op = DAOS_ARRAY_OPENS;
            tracker->resolve_sizes = 1;
            tracker->cell_size_p = cell_size;
            tracker->chunk_size_p = chunk_size;
            tracker->oh_p = oh;
            daos_event_register_comp_cb(ev, darshan_daos_open_comp_cb, tracker);
        }
    }

    tm1 = DAOS_WTIME();
    ret = __real_daos_array_open(coh, oid, th, mode, cell_size, chunk_size, oh, ev);
    tm2 = DAOS_WTIME();

    if(!ev)
    {
        /* only record here for synchronous I/O operations */
        DAOS_PRE_RECORD();
        DAOS_RECORD_OBJ_OPEN(coh, oh, oid, DAOS_ARRAY_OPENS, *cell_size, *chunk_size, 0, tm1, tm2);
        DAOS_POST_RECORD();
    }

    return(ret);
}

int DARSHAN_DECL(daos_array_open_with_attr)(daos_handle_t coh, daos_obj_id_t oid, daos_handle_t th, unsigned int mode, daos_size_t cell_size, daos_size_t chunk_size, daos_handle_t *oh, daos_event_t *ev)
{
    int ret;
    double tm1, tm2;

    MAP_OR_FAIL(daos_array_open_with_attr);

    if(ev)
    {
        /* setup callback to record the open operation upon completion */
        struct daos_open_event_tracker *tracker = malloc(sizeof(*tracker));
        if (tracker)
        {
            tracker->tm1 = DAOS_WTIME();
            tracker->coh = coh;
            tracker->oid = oid;
            tracker->op = DAOS_ARRAY_OPENS;
            tracker->resolve_sizes = 0;
            tracker->cell_size = cell_size;
            tracker->chunk_size = chunk_size;
            tracker->oh_p = oh;
            daos_event_register_comp_cb(ev, darshan_daos_open_comp_cb, tracker);
        }
    }

    tm1 = DAOS_WTIME();
    ret = __real_daos_array_open_with_attr(coh, oid, th, mode, cell_size, chunk_size, oh, ev);
    tm2 = DAOS_WTIME();

    if(!ev)
    {
        /* only record here for synchronous I/O operations */
        DAOS_PRE_RECORD();
        DAOS_RECORD_OBJ_OPEN(coh, oh, oid, DAOS_ARRAY_OPENS, cell_size, chunk_size, 0, tm1, tm2);
        DAOS_POST_RECORD();
    }

    return(ret);
}

/* XXX daos_array_global2local not supported, as there is no way to map from a
 *     global representation to underlying object ID used to reference record
 */

#define DAOS_ARRAY_IOD_SZ(__iod, __sz) do { \
    int __i; \
    __sz = 0; \
    for(__i = 0; __i < __iod->arr_nr; __i++) \
        __sz += __iod->arr_rgs[__i].rg_len; \
} while(0)

int DARSHAN_DECL(daos_array_read)(daos_handle_t oh, daos_handle_t th,
    daos_array_iod_t *iod, d_sg_list_t *sgl, daos_event_t *ev)
{
    int ret;
    double tm1, tm2;
    daos_size_t read_sz;

    MAP_OR_FAIL(daos_array_read);

    DAOS_ARRAY_IOD_SZ(iod, read_sz);

    if(ev)
    {
        /* setup callback to record the read operation upon completion */
        struct daos_read_event_tracker *tracker = malloc(sizeof(*tracker));
        if (tracker)
        {
            tracker->tm1 = DAOS_WTIME();
            tracker->oh = oh;
            tracker->op = DAOS_ARRAY_READS;
            tracker->read_size = read_sz;
            daos_event_register_comp_cb(ev, darshan_daos_read_comp_cb, tracker);
        }
    }

    tm1 = DAOS_WTIME();
    ret = __real_daos_array_read(oh, th, iod, sgl, ev);
    tm2 = DAOS_WTIME();

    if(!ev)
    {
        /* only record here for synchronous I/O operations */
        DAOS_PRE_RECORD();
        DAOS_RECORD_OBJ_READ(oh, DAOS_ARRAY_READS, read_sz, 0, tm1, tm2);
        DAOS_POST_RECORD();
    }

    return(ret);
}

int DARSHAN_DECL(daos_array_write)(daos_handle_t oh, daos_handle_t th,
    daos_array_iod_t *iod, d_sg_list_t *sgl, daos_event_t *ev)
{
    int ret;
    double tm1, tm2;
    daos_size_t write_sz;

    MAP_OR_FAIL(daos_array_write);

    DAOS_ARRAY_IOD_SZ(iod, write_sz);

    if(ev)
    {
        /* setup callback to record the write operation upon completion */
        struct daos_write_event_tracker *tracker = malloc(sizeof(*tracker));
        if (tracker)
        {
            tracker->tm1 = DAOS_WTIME();
            tracker->oh = oh;
            tracker->op = DAOS_ARRAY_WRITES;
            tracker->write_size = write_sz;
            daos_event_register_comp_cb(ev, darshan_daos_write_comp_cb, tracker);
        }
    }

    tm1 = DAOS_WTIME();
    ret = __real_daos_array_write(oh, th, iod, sgl, ev);
    tm2 = DAOS_WTIME();

    if(!ev)
    {
        /* only record here for synchronous I/O operations */
        DAOS_PRE_RECORD();
        DAOS_RECORD_OBJ_WRITE(oh, DAOS_ARRAY_WRITES, write_sz, 0, tm1, tm2);
        DAOS_POST_RECORD();
    }

    return(ret);
}

int DARSHAN_DECL(daos_array_get_size)(daos_handle_t oh, daos_handle_t th,
    daos_size_t *size, daos_event_t *ev)
{
    int ret;
    double tm1, tm2;
    struct daos_object_record_ref *rec_ref;

    MAP_OR_FAIL(daos_array_get_size);

    if(ev)
    {
        /* setup callback to record the metadata operation upon completion */
        struct daos_meta_event_tracker *tracker = malloc(sizeof(*tracker));
        if (tracker)
        {
            tracker->tm1 = DAOS_WTIME();
            tracker->oh = oh;
            tracker->op = DAOS_ARRAY_GET_SIZES;
            daos_event_register_comp_cb(ev, darshan_daos_meta_comp_cb, tracker);
        }
    }

    tm1 = DAOS_WTIME();
    ret = __real_daos_array_get_size(oh, th, size, ev);
    tm2 = DAOS_WTIME();

    if(!ev)
    {
        /* only record here for synchronous I/O operations */
        DAOS_PRE_RECORD();
        rec_ref = darshan_lookup_record_ref(daos_runtime->oh_hash,
            &oh, sizeof(daos_handle_t));
        if(rec_ref)
        {
            DARSHAN_TIMER_INC_NO_OVERLAP(
                rec_ref->object_rec->fcounters[DAOS_F_META_TIME],
                tm1, tm2, rec_ref->last_meta_end);
            rec_ref->object_rec->counters[DAOS_ARRAY_GET_SIZES] += 1;
        }
        DAOS_POST_RECORD();
    }

    return(ret);
}

int DARSHAN_DECL(daos_array_set_size)(daos_handle_t oh, daos_handle_t th,
    daos_size_t size, daos_event_t *ev)
{
    int ret;
    double tm1, tm2;
    struct daos_object_record_ref *rec_ref;

    MAP_OR_FAIL(daos_array_set_size);

    if(ev)
    {
        /* setup callback to record the metadata operation upon completion */
        struct daos_meta_event_tracker *tracker = malloc(sizeof(*tracker));
        if (tracker)
        {
            tracker->tm1 = DAOS_WTIME();
            tracker->oh = oh;
            tracker->op = DAOS_ARRAY_SET_SIZES;
            daos_event_register_comp_cb(ev, darshan_daos_meta_comp_cb, tracker);
        }
    }

    tm1 = DAOS_WTIME();
    ret = __real_daos_array_set_size(oh, th, size, ev);
    tm2 = DAOS_WTIME();

    if(!ev)
    {
        /* only record here for synchronous I/O operations */
        DAOS_PRE_RECORD();
        rec_ref = darshan_lookup_record_ref(daos_runtime->oh_hash,
            &oh, sizeof(daos_handle_t));
        if(rec_ref)
        {
            DARSHAN_TIMER_INC_NO_OVERLAP(
                rec_ref->object_rec->fcounters[DAOS_F_META_TIME],
                tm1, tm2, rec_ref->last_meta_end);
            rec_ref->object_rec->counters[DAOS_ARRAY_SET_SIZES] += 1;
        }
        DAOS_POST_RECORD();
    }

    return(ret);
}

int DARSHAN_DECL(daos_array_stat)(daos_handle_t oh, daos_handle_t th, daos_array_stbuf_t *stbuf, daos_event_t *ev)
{
    int ret;
    double tm1, tm2;
    struct daos_object_record_ref *rec_ref;

    MAP_OR_FAIL(daos_array_stat);

    if(ev)
    {
        /* setup callback to record the metadata operation upon completion */
        struct daos_meta_event_tracker *tracker = malloc(sizeof(*tracker));
        if (tracker)
        {
            tracker->tm1 = DAOS_WTIME();
            tracker->oh = oh;
            tracker->op = DAOS_ARRAY_STATS;
            daos_event_register_comp_cb(ev, darshan_daos_meta_comp_cb, tracker);
        }
    }

    tm1 = DAOS_WTIME();
    ret = __real_daos_array_stat(oh, th, stbuf, ev);
    tm2 = DAOS_WTIME();

    if(!ev)
    {
        /* only record here for synchronous I/O operations */
        DAOS_PRE_RECORD();
        rec_ref = darshan_lookup_record_ref(daos_runtime->oh_hash,
            &oh, sizeof(daos_handle_t));
        if(rec_ref)
        {
            DARSHAN_TIMER_INC_NO_OVERLAP(
                rec_ref->object_rec->fcounters[DAOS_F_META_TIME],
                tm1, tm2, rec_ref->last_meta_end);
            rec_ref->object_rec->counters[DAOS_ARRAY_STATS] += 1;
        }
        DAOS_POST_RECORD();
    }

    return(ret);
}

int DARSHAN_DECL(daos_array_punch)(daos_handle_t oh, daos_handle_t th,
    daos_array_iod_t *iod, daos_event_t *ev)
{
    int ret;
    double tm1, tm2;
    struct daos_object_record_ref *rec_ref;

    MAP_OR_FAIL(daos_array_punch);

    if(ev)
    {
        /* setup callback to record the metadata operation upon completion */
        struct daos_meta_event_tracker *tracker = malloc(sizeof(*tracker));
        if (tracker)
        {
            tracker->tm1 = DAOS_WTIME();
            tracker->oh = oh;
            tracker->op = DAOS_ARRAY_PUNCHES;
            daos_event_register_comp_cb(ev, darshan_daos_meta_comp_cb, tracker);
        }
    }

    tm1 = DAOS_WTIME();
    ret = __real_daos_array_punch(oh, th, iod, ev);
    tm2 = DAOS_WTIME();

    if(!ev)
    {
        /* only record here for synchronous I/O operations */
        DAOS_PRE_RECORD();
        rec_ref = darshan_lookup_record_ref(daos_runtime->oh_hash,
            &oh, sizeof(daos_handle_t));
        if(rec_ref)
        {
            DARSHAN_TIMER_INC_NO_OVERLAP(
                rec_ref->object_rec->fcounters[DAOS_F_META_TIME],
                tm1, tm2, rec_ref->last_meta_end);
            rec_ref->object_rec->counters[DAOS_ARRAY_PUNCHES] += 1;
        }
        DAOS_POST_RECORD();
    }

    return(ret);
}

int DARSHAN_DECL(daos_array_destroy)(daos_handle_t oh, daos_handle_t th,
    daos_event_t *ev)
{
    int ret;
    double tm1, tm2;
    struct daos_object_record_ref *rec_ref;

    MAP_OR_FAIL(daos_array_destroy);

    if(ev)
    {
        /* setup callback to record the metadata operation upon completion */
        struct daos_meta_event_tracker *tracker = malloc(sizeof(*tracker));
        if (tracker)
        {
            tracker->tm1 = DAOS_WTIME();
            tracker->oh = oh;
            tracker->op = DAOS_ARRAY_DESTROYS;
            daos_event_register_comp_cb(ev, darshan_daos_meta_comp_cb, tracker);
        }
    }

    tm1 = DAOS_WTIME();
    ret = __real_daos_array_destroy(oh, th, ev);
    tm2 = DAOS_WTIME();

    if(!ev)
    {
        /* only record here for synchronous I/O operations */
        DAOS_PRE_RECORD();
        rec_ref = darshan_lookup_record_ref(daos_runtime->oh_hash,
            &oh, sizeof(daos_handle_t));
        if(rec_ref)
        {
            DARSHAN_TIMER_INC_NO_OVERLAP(
                rec_ref->object_rec->fcounters[DAOS_F_META_TIME],
                tm1, tm2, rec_ref->last_meta_end);
            rec_ref->object_rec->counters[DAOS_ARRAY_DESTROYS] += 1;
        }
        DAOS_POST_RECORD();
    }

    return(ret);
}

int DARSHAN_DECL(daos_array_close)(daos_handle_t oh, daos_event_t *ev)
{
    int ret;
    double tm1, tm2;

    MAP_OR_FAIL(daos_array_close);

    if(ev)
    {
        /* setup callback to record the close operation upon completion */
        struct daos_close_event_tracker *tracker = malloc(sizeof(*tracker));
        if (tracker)
        {
            tracker->tm1 = DAOS_WTIME();
            tracker->oh = oh;
            daos_event_register_comp_cb(ev, darshan_daos_close_comp_cb, tracker);
        }
    }

    tm1 = DAOS_WTIME();
    ret = __real_daos_array_close(oh, ev);
    tm2 = DAOS_WTIME();

    if(!ev)
    {
        /* only record here for synchronous I/O operations */
        DAOS_PRE_RECORD();
        DAOS_RECORD_OBJ_CLOSE(oh, tm1, tm2);
        DAOS_POST_RECORD();
    }

    return(ret);
}

/* key-value API */

int DARSHAN_DECL(daos_kv_open)(daos_handle_t coh, daos_obj_id_t oid, unsigned int mode,
    daos_handle_t *oh, daos_event_t *ev)
{
    int ret;
    double tm1, tm2;

    MAP_OR_FAIL(daos_kv_open);

    if(ev)
    {
        /* setup callback to record the open operation upon completion */
        struct daos_open_event_tracker *tracker = malloc(sizeof(*tracker));
        if (tracker)
        {
            tracker->tm1 = DAOS_WTIME();
            tracker->coh = coh;
            tracker->oid = oid;
            tracker->op = DAOS_KV_OPENS;
            tracker->resolve_sizes = 0;
            tracker->cell_size = 0;
            tracker->chunk_size = 0;
            tracker->oh_p = oh;
            daos_event_register_comp_cb(ev, darshan_daos_open_comp_cb, tracker);
        }
    }

    tm1 = DAOS_WTIME();
    ret = __real_daos_kv_open(coh, oid, mode, oh, ev);
    tm2 = DAOS_WTIME();

    if(!ev)
    {
        /* only record here for synchronous I/O operations */
        DAOS_PRE_RECORD();
        DAOS_RECORD_OBJ_OPEN(coh, oh, oid, DAOS_KV_OPENS, 0, 0, 0, tm1, tm2);
        DAOS_POST_RECORD();
    }

    return(ret);
}

int DARSHAN_DECL(daos_kv_get)(daos_handle_t oh, daos_handle_t th, uint64_t flags,
    const char *key, daos_size_t *size, void *buf, daos_event_t *ev)
{
    int ret;
    double tm1, tm2;

    MAP_OR_FAIL(daos_kv_get);

    if(ev)
    {
        /* setup callback to record the read operation upon completion */
        struct daos_read_event_tracker *tracker = malloc(sizeof(*tracker));
        if (tracker)
        {
            tracker->tm1 = DAOS_WTIME();
            tracker->oh = oh;
            tracker->op = DAOS_KV_GETS;
            tracker->read_size_p = size;
            daos_event_register_comp_cb(ev, darshan_daos_read_comp_cb, tracker);
        }
    }

    tm1 = DAOS_WTIME();
    ret = __real_daos_kv_get(oh, th, flags, key, size, buf, ev);
    tm2 = DAOS_WTIME();

    if(!ev)
    {
        /* only record here for synchronous I/O operations */
        DAOS_PRE_RECORD();
        DAOS_RECORD_OBJ_READ(oh, DAOS_KV_GETS, *(size), 0, tm1, tm2);
        DAOS_POST_RECORD();
    }

    return(ret);
}

int DARSHAN_DECL(daos_kv_put)(daos_handle_t oh, daos_handle_t th, uint64_t flags,
    const char *key, daos_size_t size, const void *buf, daos_event_t *ev)
{
    int ret;
    double tm1, tm2;

    MAP_OR_FAIL(daos_kv_put);

    if(ev)
    {
        /* setup callback to record the write operation upon completion */
        struct daos_write_event_tracker *tracker = malloc(sizeof(*tracker));
        if (tracker)
        {
            tracker->tm1 = DAOS_WTIME();
            tracker->oh = oh;
            tracker->op = DAOS_KV_PUTS;
            tracker->write_size = size;
            daos_event_register_comp_cb(ev, darshan_daos_write_comp_cb, tracker);
        }
    }

    tm1 = DAOS_WTIME();
    ret = __real_daos_kv_put(oh, th, flags, key, size, buf, ev);
    tm2 = DAOS_WTIME();

    if(!ev)
    {
        /* only record here for synchronous I/O operations */
        DAOS_PRE_RECORD();
        DAOS_RECORD_OBJ_WRITE(oh, DAOS_KV_PUTS, size, 0, tm1, tm2);
        DAOS_POST_RECORD();
    }

    return(ret);
}

int DARSHAN_DECL(daos_kv_remove)(daos_handle_t oh, daos_handle_t th, uint64_t flags,
    const char *key, daos_event_t *ev)
{
    int ret;
    double tm1, tm2;
    struct daos_object_record_ref *rec_ref;

    MAP_OR_FAIL(daos_kv_remove);

    if(ev)
    {
        /* setup callback to record the metadata operation upon completion */
        struct daos_meta_event_tracker *tracker = malloc(sizeof(*tracker));
        if (tracker)
        {
            tracker->tm1 = DAOS_WTIME();
            tracker->oh = oh;
            tracker->op = DAOS_KV_REMOVES;
            daos_event_register_comp_cb(ev, darshan_daos_meta_comp_cb, tracker);
        }
    }

    tm1 = DAOS_WTIME();
    ret = __real_daos_kv_remove(oh, th, flags, key, ev);
    tm2 = DAOS_WTIME();

    if(!ev)
    {
        /* only record here for synchronous I/O operations */
        DAOS_PRE_RECORD();
        rec_ref = darshan_lookup_record_ref(daos_runtime->oh_hash,
            &oh, sizeof(daos_handle_t));
        if(rec_ref)
        {
            DARSHAN_TIMER_INC_NO_OVERLAP(
                rec_ref->object_rec->fcounters[DAOS_F_META_TIME],
                tm1, tm2, rec_ref->last_meta_end);
            rec_ref->object_rec->counters[DAOS_KV_REMOVES] += 1;
        }
        DAOS_POST_RECORD();
    }

    return(ret);
}

int DARSHAN_DECL(daos_kv_list)(daos_handle_t oh, daos_handle_t th, uint32_t *nr,
    daos_key_desc_t *kds, d_sg_list_t *sgl, daos_anchor_t *anchor, daos_event_t *ev)
{
    int ret;
    double tm1, tm2;
    struct daos_object_record_ref *rec_ref;

    MAP_OR_FAIL(daos_kv_list);

    if(ev)
    {
        /* setup callback to record the metadata operation upon completion */
        struct daos_meta_event_tracker *tracker = malloc(sizeof(*tracker));
        if (tracker)
        {
            tracker->tm1 = DAOS_WTIME();
            tracker->oh = oh;
            tracker->op = DAOS_KV_LISTS;
            daos_event_register_comp_cb(ev, darshan_daos_meta_comp_cb, tracker);
        }
    }

    tm1 = DAOS_WTIME();
    ret = __real_daos_kv_list(oh, th, nr, kds, sgl, anchor, ev);
    tm2 = DAOS_WTIME();

    if(!ev)
    {
        /* only record here for synchronous I/O operations */
        DAOS_PRE_RECORD();
        rec_ref = darshan_lookup_record_ref(daos_runtime->oh_hash,
            &oh, sizeof(daos_handle_t));
        if(rec_ref)
        {
            DARSHAN_TIMER_INC_NO_OVERLAP(
                rec_ref->object_rec->fcounters[DAOS_F_META_TIME],
                tm1, tm2, rec_ref->last_meta_end);
            rec_ref->object_rec->counters[DAOS_KV_LISTS] += 1;
        }
        DAOS_POST_RECORD();
    }

    return(ret);
}

int DARSHAN_DECL(daos_kv_destroy)(daos_handle_t oh, daos_handle_t th, daos_event_t *ev)
{
    int ret;
    double tm1, tm2;
    struct daos_object_record_ref *rec_ref;

    MAP_OR_FAIL(daos_kv_destroy);

    if(ev)
    {
        /* setup callback to record the metadata operation upon completion */
        struct daos_meta_event_tracker *tracker = malloc(sizeof(*tracker));
        if (tracker)
        {
            tracker->tm1 = DAOS_WTIME();
            tracker->oh = oh;
            tracker->op = DAOS_KV_DESTROYS;
            daos_event_register_comp_cb(ev, darshan_daos_meta_comp_cb, tracker);
        }
    }

    tm1 = DAOS_WTIME();
    ret = __real_daos_kv_destroy(oh, th, ev);
    tm2 = DAOS_WTIME();

    if(!ev)
    {
        /* only record here for synchronous I/O operations */
        DAOS_PRE_RECORD();
        rec_ref = darshan_lookup_record_ref(daos_runtime->oh_hash,
            &oh, sizeof(daos_handle_t));
        if(rec_ref)
        {
            DARSHAN_TIMER_INC_NO_OVERLAP(
                rec_ref->object_rec->fcounters[DAOS_F_META_TIME],
                tm1, tm2, rec_ref->last_meta_end);
            rec_ref->object_rec->counters[DAOS_KV_DESTROYS] += 1;
        }
        DAOS_POST_RECORD();
    }

    return(ret);
}

int DARSHAN_DECL(daos_kv_close)(daos_handle_t oh, daos_event_t *ev)
{
    int ret;
    double tm1, tm2;

    MAP_OR_FAIL(daos_kv_close);

    if(ev)
    {
        /* setup callback to record the close operation upon completion */
        struct daos_close_event_tracker *tracker = malloc(sizeof(*tracker));
        if (tracker)
        {
            tracker->tm1 = DAOS_WTIME();
            tracker->oh = oh;
            daos_event_register_comp_cb(ev, darshan_daos_close_comp_cb, tracker);
        }
    }

    tm1 = DAOS_WTIME();
    ret = __real_daos_kv_close(oh, ev);
    tm2 = DAOS_WTIME();

    if(!ev)
    {
        /* only record here for synchronous I/O operations */
        DAOS_PRE_RECORD();
        DAOS_RECORD_OBJ_CLOSE(oh, tm1, tm2);
        DAOS_POST_RECORD();
    }

    return(ret);
}

/*********************************************************
 * Internal functions for manipulating DAOS module state *
 *********************************************************/

static void daos_runtime_initialize()
{
    int ret;
    size_t daos_rec_count;
    darshan_module_funcs mod_funcs = {
#ifdef HAVE_MPI
        .mod_redux_func = &daos_mpi_redux,
#endif
        .mod_output_func = &daos_output,
        .mod_cleanup_func = &daos_cleanup
        };

    /* if this attempt at initializing fails, we won't try again */
    daos_runtime_init_attempted = 1;

    /* try to store a default number of records for this module */
    daos_rec_count = DARSHAN_DEF_MOD_REC_COUNT;

    /* register the DAOS module with darshan core */
    ret = darshan_core_register_module(
        DARSHAN_DAOS_MOD,
        mod_funcs,
        sizeof(struct darshan_daos_object),
        &daos_rec_count,
        &my_rank,
        NULL);
    if(ret < 0)
        return;

    daos_runtime = malloc(sizeof(*daos_runtime));
    if(!daos_runtime)
    {
        darshan_core_unregister_module(DARSHAN_DAOS_MOD);
        return;
    }
    memset(daos_runtime, 0, sizeof(*daos_runtime));

    /* register a heatmap */
    daos_runtime->heatmap_id = heatmap_register("heatmap:DAOS");

    return;
}

static struct daos_object_record_ref *daos_track_new_object_record(
    darshan_record_id rec_id, daos_obj_id_t oid, struct daos_poolcont_info *poolcont_info)
{
    struct darshan_daos_object *object_rec = NULL;
    struct daos_object_record_ref *rec_ref = NULL;
    int ret;

    rec_ref = malloc(sizeof(*rec_ref));
    if(!rec_ref)
        return(NULL);
    memset(rec_ref, 0, sizeof(*rec_ref));

    /* add a reference to this object record based on record id */
    ret = darshan_add_record_ref(&(daos_runtime->rec_id_hash), &rec_id,
        sizeof(darshan_record_id), rec_ref);
    if(ret == 0)
    {
        free(rec_ref);
        return(NULL);
    }

    /* register the actual object record with darshan-core so it is persisted
     * in the log file
     */
    object_rec = darshan_core_register_record(
        rec_id,
        NULL,
        DARSHAN_DAOS_MOD,
        sizeof(struct darshan_daos_object),
        NULL);

    if(!object_rec)
    {
        darshan_delete_record_ref(&(daos_runtime->rec_id_hash),
            &rec_id, sizeof(darshan_record_id));
        free(rec_ref);
        return(NULL);
    }

    /* registering this object record was successful, so initialize some fields */
    object_rec->base_rec.id = rec_id;
    object_rec->base_rec.rank = my_rank;
    uuid_copy(object_rec->pool_uuid, poolcont_info->pool_uuid);
    uuid_copy(object_rec->cont_uuid, poolcont_info->cont_uuid);
    object_rec->oid_hi = oid.hi;
    object_rec->oid_lo = oid.lo;
    rec_ref->object_rec = object_rec;
    daos_runtime->obj_rec_count++;

    return(rec_ref);
}

static void daos_finalize_object_records(void *rec_ref_p, void *user_ptr)
{
    struct daos_object_record_ref *rec_ref =
        (struct daos_object_record_ref *)rec_ref_p;

    tdestroy(rec_ref->access_root, free);
    return;
}

#ifdef HAVE_MPI
static void daos_record_reduction_op(
    void* inobj_v, void* inoutobj_v, int *len, MPI_Datatype *datatype)
{
    struct darshan_daos_object tmp_obj;
    struct darshan_daos_object *inobj = inobj_v;
    struct darshan_daos_object *inoutobj = inoutobj_v;
    int i, j, k;

    for(i=0; i<*len; i++)
    {
        memset(&tmp_obj, 0, sizeof(struct darshan_daos_object));
        tmp_obj.base_rec.id = inobj->base_rec.id;
        tmp_obj.base_rec.rank = -1;
        uuid_copy(tmp_obj.pool_uuid, inobj->pool_uuid);
        uuid_copy(tmp_obj.cont_uuid, inobj->cont_uuid);
        tmp_obj.oid_hi = inobj->oid_hi;
        tmp_obj.oid_lo = inobj->oid_lo;

        /* sum */
        for(j=DAOS_OBJ_OPENS; j<=DAOS_RW_SWITCHES; j++)
        {
            tmp_obj.counters[j] = inobj->counters[j] + inoutobj->counters[j];
            if(tmp_obj.counters[j] < 0) /* make sure invalid counters are -1 exactly */
                tmp_obj.counters[j] = -1;
        }

        /* skip DAOS_MAX_*_TIME_SIZE; handled in floating point section */

        for(j=DAOS_SIZE_READ_0_100; j<=DAOS_SIZE_WRITE_1G_PLUS; j++)
        {
            tmp_obj.counters[j] = inobj->counters[j] + inoutobj->counters[j];
        }

        /* common access counters */

        /* first collapse any duplicates */
        for(j=DAOS_ACCESS1_ACCESS; j<=DAOS_ACCESS4_ACCESS; j++)
        {
            for(k=DAOS_ACCESS1_ACCESS; k<=DAOS_ACCESS4_ACCESS; k++)
            {
                if(inobj->counters[j] == inoutobj->counters[k])
                {
                    inobj->counters[j+4] += inoutobj->counters[k+4];
                    inoutobj->counters[k] = 0;
                    inoutobj->counters[k+4] = 0;
                }
            }
        }

        /* first set */
        for(j=DAOS_ACCESS1_ACCESS; j<=DAOS_ACCESS4_ACCESS; j++)
        {
            DARSHAN_UPDATE_COMMON_VAL_COUNTERS(
                &(tmp_obj.counters[DAOS_ACCESS1_ACCESS]),
                &(tmp_obj.counters[DAOS_ACCESS1_COUNT]),
                &inobj->counters[j], 1, inobj->counters[j+4], 1);
        }
        /* second set */
        for(j=DAOS_ACCESS1_ACCESS; j<=DAOS_ACCESS4_ACCESS; j++)
        {
            DARSHAN_UPDATE_COMMON_VAL_COUNTERS(
                &(tmp_obj.counters[DAOS_ACCESS1_ACCESS]),
                &(tmp_obj.counters[DAOS_ACCESS1_COUNT]),
                &inoutobj->counters[j], 1, inoutobj->counters[j+4], 1);
        }

        tmp_obj.counters[DAOS_OBJ_OTYPE] = inobj->counters[DAOS_OBJ_OTYPE];
        tmp_obj.counters[DAOS_ARRAY_CELL_SIZE] = inobj->counters[DAOS_ARRAY_CELL_SIZE];
        tmp_obj.counters[DAOS_ARRAY_CHUNK_SIZE] = inobj->counters[DAOS_ARRAY_CHUNK_SIZE];

        /* min non-zero (if available) value */
        for(j=DAOS_F_OPEN_START_TIMESTAMP; j<=DAOS_F_CLOSE_START_TIMESTAMP; j++)
        {
            if((inobj->fcounters[j] < inoutobj->fcounters[j] &&
               inobj->fcounters[j] > 0) || inoutobj->fcounters[j] == 0)
                tmp_obj.fcounters[j] = inobj->fcounters[j];
            else
                tmp_obj.fcounters[j] = inoutobj->fcounters[j];
        }

        /* max */
        for(j=DAOS_F_OPEN_END_TIMESTAMP; j<=DAOS_F_CLOSE_END_TIMESTAMP; j++)
        {
            if(inobj->fcounters[j] > inoutobj->fcounters[j])
                tmp_obj.fcounters[j] = inobj->fcounters[j];
            else
                tmp_obj.fcounters[j] = inoutobj->fcounters[j];
        }

        /* sum */
        for(j=DAOS_F_READ_TIME; j<=DAOS_F_META_TIME; j++)
        {
            tmp_obj.fcounters[j] = inobj->fcounters[j] + inoutobj->fcounters[j];
        }

        /* max (special case) */
        if(inobj->fcounters[DAOS_F_MAX_READ_TIME] >
            inoutobj->fcounters[DAOS_F_MAX_READ_TIME])
        {
            tmp_obj.fcounters[DAOS_F_MAX_READ_TIME] =
                inobj->fcounters[DAOS_F_MAX_READ_TIME];
            tmp_obj.counters[DAOS_MAX_READ_TIME_SIZE] =
                inobj->counters[DAOS_MAX_READ_TIME_SIZE];
        }
        else
        {
            tmp_obj.fcounters[DAOS_F_MAX_READ_TIME] =
                inoutobj->fcounters[DAOS_F_MAX_READ_TIME];
            tmp_obj.counters[DAOS_MAX_READ_TIME_SIZE] =
                inoutobj->counters[DAOS_MAX_READ_TIME_SIZE];
        }

        if(inobj->fcounters[DAOS_F_MAX_WRITE_TIME] >
            inoutobj->fcounters[DAOS_F_MAX_WRITE_TIME])
        {
            tmp_obj.fcounters[DAOS_F_MAX_WRITE_TIME] =
                inobj->fcounters[DAOS_F_MAX_WRITE_TIME];
            tmp_obj.counters[DAOS_MAX_WRITE_TIME_SIZE] =
                inobj->counters[DAOS_MAX_WRITE_TIME_SIZE];
        }
        else
        {
            tmp_obj.fcounters[DAOS_F_MAX_WRITE_TIME] =
                inoutobj->fcounters[DAOS_F_MAX_WRITE_TIME];
            tmp_obj.counters[DAOS_MAX_WRITE_TIME_SIZE] =
                inoutobj->counters[DAOS_MAX_WRITE_TIME_SIZE];
        }

        /* min (zeroes are ok here; some procs don't do I/O) */
        if(inobj->fcounters[DAOS_F_FASTEST_RANK_TIME] <
           inoutobj->fcounters[DAOS_F_FASTEST_RANK_TIME])
        {
            tmp_obj.counters[DAOS_FASTEST_RANK] =
                inobj->counters[DAOS_FASTEST_RANK];
            tmp_obj.counters[DAOS_FASTEST_RANK_BYTES] =
                inobj->counters[DAOS_FASTEST_RANK_BYTES];
            tmp_obj.fcounters[DAOS_F_FASTEST_RANK_TIME] =
                inobj->fcounters[DAOS_F_FASTEST_RANK_TIME];
        }
        else
        {
            tmp_obj.counters[DAOS_FASTEST_RANK] =
                inoutobj->counters[DAOS_FASTEST_RANK];
            tmp_obj.counters[DAOS_FASTEST_RANK_BYTES] =
                inoutobj->counters[DAOS_FASTEST_RANK_BYTES];
            tmp_obj.fcounters[DAOS_F_FASTEST_RANK_TIME] =
                inoutobj->fcounters[DAOS_F_FASTEST_RANK_TIME];
        }

        /* max */
        if(inobj->fcounters[DAOS_F_SLOWEST_RANK_TIME] >
           inoutobj->fcounters[DAOS_F_SLOWEST_RANK_TIME])
        {
            tmp_obj.counters[DAOS_SLOWEST_RANK] =
                inobj->counters[DAOS_SLOWEST_RANK];
            tmp_obj.counters[DAOS_SLOWEST_RANK_BYTES] =
                inobj->counters[DAOS_SLOWEST_RANK_BYTES];
            tmp_obj.fcounters[DAOS_F_SLOWEST_RANK_TIME] =
                inobj->fcounters[DAOS_F_SLOWEST_RANK_TIME];
        }
        else
        {
            tmp_obj.counters[DAOS_SLOWEST_RANK] =
                inoutobj->counters[DAOS_SLOWEST_RANK];
            tmp_obj.counters[DAOS_SLOWEST_RANK_BYTES] =
                inoutobj->counters[DAOS_SLOWEST_RANK_BYTES];
            tmp_obj.fcounters[DAOS_F_SLOWEST_RANK_TIME] =
                inoutobj->fcounters[DAOS_F_SLOWEST_RANK_TIME];
        }

        /* update pointers */
        *inoutobj = tmp_obj;
        inoutobj++;
        inobj++;
    }

    return;
}
#endif

/*********************************************************************************
 * shutdown functions exported by this module for coordinating with darshan-core *
 *********************************************************************************/

#ifdef HAVE_MPI
static void daos_mpi_redux(
    void *daos_buf, MPI_Comm mod_comm,
    darshan_record_id *shared_recs, int shared_rec_count)
{
    int daos_rec_count;
    struct daos_object_record_ref *rec_ref;
    struct darshan_daos_object *daos_rec_buf = (struct darshan_daos_object *)daos_buf;
    double daos_time;
    struct darshan_daos_object *red_send_buf = NULL;
    struct darshan_daos_object *red_recv_buf = NULL;
    MPI_Datatype red_type;
    MPI_Op red_op;
    int i;

    DAOS_LOCK();
    assert(daos_runtime);

    daos_rec_count = daos_runtime->obj_rec_count;

    /* necessary initialization of shared records */
    for(i = 0; i < shared_rec_count; i++)
    {
        rec_ref = darshan_lookup_record_ref(daos_runtime->rec_id_hash,
            &shared_recs[i], sizeof(darshan_record_id));
        assert(rec_ref);

        daos_time =
            rec_ref->object_rec->fcounters[DAOS_F_READ_TIME] +
            rec_ref->object_rec->fcounters[DAOS_F_WRITE_TIME] +
            rec_ref->object_rec->fcounters[DAOS_F_META_TIME];

        /* initialize fastest/slowest info prior to the reduction */
        rec_ref->object_rec->counters[DAOS_FASTEST_RANK] =
            rec_ref->object_rec->base_rec.rank;
        rec_ref->object_rec->counters[DAOS_FASTEST_RANK_BYTES] =
            rec_ref->object_rec->counters[DAOS_BYTES_READ] +
            rec_ref->object_rec->counters[DAOS_BYTES_WRITTEN];
        rec_ref->object_rec->fcounters[DAOS_F_FASTEST_RANK_TIME] =
            daos_time;

        /* until reduction occurs, we assume that this rank is both
         * the fastest and slowest. It is up to the reduction operator
         * to find the true min and max.
         */
        rec_ref->object_rec->counters[DAOS_SLOWEST_RANK] =
            rec_ref->object_rec->counters[DAOS_FASTEST_RANK];
        rec_ref->object_rec->counters[DAOS_SLOWEST_RANK_BYTES] =
            rec_ref->object_rec->counters[DAOS_FASTEST_RANK_BYTES];
        rec_ref->object_rec->fcounters[DAOS_F_SLOWEST_RANK_TIME] =
            rec_ref->object_rec->fcounters[DAOS_F_FASTEST_RANK_TIME];

        rec_ref->object_rec->base_rec.rank = -1;
    }

    /* sort the array of records so we get all of the shared records
     * (marked by rank -1) in a contiguous portion at end of the array
     */
    darshan_record_sort(daos_rec_buf, daos_rec_count,
        sizeof(struct darshan_daos_object));

    /* make send_buf point to the shared records at the end of sorted array */
    red_send_buf = &(daos_rec_buf[daos_rec_count-shared_rec_count]);

    /* allocate memory for the reduction output on rank 0 */
    if(my_rank == 0)
    {
        red_recv_buf = malloc(shared_rec_count * sizeof(struct darshan_daos_object));
        if(!red_recv_buf)
        {
            DAOS_UNLOCK();
            return;
        }
    }

    /* construct a datatype for a DAOS object record.  This is serving no purpose
     * except to make sure we can do a reduction on proper boundaries
     */
    PMPI_Type_contiguous(sizeof(struct darshan_daos_object),
        MPI_BYTE, &red_type);
    PMPI_Type_commit(&red_type);

    /* register a DAOS object record reduction operator */
    PMPI_Op_create(daos_record_reduction_op, 1, &red_op);

    /* reduce shared DAOS object records */
    PMPI_Reduce(red_send_buf, red_recv_buf,
        shared_rec_count, red_type, red_op, 0, mod_comm);

    /* update module state to account for shared file reduction */
    if(my_rank == 0)
    {
        /* overwrite local shared records with globally reduced records */
        int tmp_ndx = daos_rec_count - shared_rec_count;
        memcpy(&(daos_rec_buf[tmp_ndx]), red_recv_buf,
            shared_rec_count * sizeof(struct darshan_daos_object));
        free(red_recv_buf);
    }
    else
    {
        /* drop shared records on non-zero ranks */
        daos_runtime->obj_rec_count -= shared_rec_count;
    }

    PMPI_Type_free(&red_type);
    PMPI_Op_free(&red_op);

    DAOS_UNLOCK();
    return;
}
#endif

static void daos_output(
    void **daos_buf, int *daos_buf_sz)
{
    int daos_rec_count;
    struct darshan_daos_object *daos_rec_buf = *(struct darshan_daos_object **)daos_buf;
    int i, j;
    int ops;

    DAOS_LOCK();
    assert(daos_runtime);

    daos_rec_count = daos_runtime->obj_rec_count;

    /* filter out records that have been opened, but don't have any
     * I/O operations
     */
    for(i=0; i<daos_rec_count; i++)
    {
        for(j=DAOS_OBJ_FETCHES; j<=DAOS_OBJ_RECX_LISTS; j++)
        {
            ops = daos_rec_buf[i].counters[j];
            if(ops) break;
        }
        if(!ops)
        {
            for(j=DAOS_ARRAY_READS; j<=DAOS_ARRAY_DESTROYS; j++)
            {
                ops = daos_rec_buf[i].counters[j];
                if(ops) break;
            }
        }
        if(!ops)
        {
            for(j=DAOS_KV_GETS; j<=DAOS_KV_DESTROYS; j++)
            {
                ops = daos_rec_buf[i].counters[j];
                if(ops) break;
            }
        }
        if(!ops)
        {
            if(i != (daos_rec_count-1))
            {
                memmove(&daos_rec_buf[i], &daos_rec_buf[i+1],
                    (daos_rec_count-i-1)*sizeof(daos_rec_buf[i]));
                i--;
            }
            daos_rec_count--;
        }
    }

    /* just pass back our updated total buffer size -- no need to update buffer */
    *daos_buf_sz = daos_rec_count * sizeof(struct darshan_daos_object);

    daos_runtime->frozen = 1;

    DAOS_UNLOCK();
    return;
}

static void daos_cleanup()
{
    struct daos_poolcont_info *poolcont_info, *tmp;

    DAOS_LOCK();
    assert(daos_runtime);

    /* cleanup internal structures used for instrumenting */
    darshan_iter_record_refs(daos_runtime->rec_id_hash,
        &daos_finalize_object_records, NULL);
    darshan_clear_record_refs(&(daos_runtime->oh_hash), 0);
    darshan_clear_record_refs(&(daos_runtime->rec_id_hash), 1);

    HASH_ITER(hlink, daos_runtime->poolcont_hash, poolcont_info, tmp)
    {
        HASH_DELETE(hlink, daos_runtime->poolcont_hash, poolcont_info);
        free(poolcont_info);
    }

    free(daos_runtime);
    daos_runtime = NULL;
    daos_runtime_init_attempted = 0;

    DAOS_UNLOCK();
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
