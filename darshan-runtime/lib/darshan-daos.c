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

#include <daos_types.h>
#include <daos_prop.h>
#include <daos_pool.h>
#include <daos_cont.h>
#include <daos_obj.h>
#include <daos_array.h>

/* multi-level key array API */
DARSHAN_FORWARD_DECL(daos_obj_open, int, (daos_handle_t coh, daos_obj_id_t oid, unsigned int mode, daos_handle_t *oh, daos_event_t *ev));
DARSHAN_FORWARD_DECL(daos_obj_fetch, int, (daos_handle_t oh, daos_handle_t th, uint64_t flags, daos_key_t *dkey, unsigned int nr, daos_iod_t *iods, d_sg_list_t *sgls, daos_iom_t *ioms, daos_event_t *ev));
DARSHAN_FORWARD_DECL(daos_obj_update, int, (daos_handle_t oh, daos_handle_t th, uint64_t flags, daos_key_t *dkey, unsigned int nr, daos_iod_t *iods, d_sg_list_t *sgls, daos_event_t *ev));
DARSHAN_FORWARD_DECL(daos_obj_close, int, (daos_handle_t oh, daos_event_t *ev));

/* array API */
DARSHAN_FORWARD_DECL(daos_array_create, int, (daos_handle_t coh, daos_obj_id_t oid, daos_handle_t th, daos_size_t cell_size, daos_size_t chunk_size, daos_handle_t *oh, daos_event_t *ev));
DARSHAN_FORWARD_DECL(daos_array_open, int, (daos_handle_t coh, daos_obj_id_t oid, daos_handle_t th, unsigned int mode, daos_size_t *cell_size, daos_size_t *chunk_size, daos_handle_t *oh, daos_event_t *ev));
DARSHAN_FORWARD_DECL(daos_array_open_with_attr, int, (daos_handle_t coh, daos_obj_id_t oid, daos_handle_t th, unsigned int mode, daos_size_t cell_size, daos_size_t chunk_size, daos_handle_t *oh, daos_event_t *ev));
DARSHAN_FORWARD_DECL(daos_array_read, int, (daos_handle_t oh, daos_handle_t th, daos_array_iod_t *iod, d_sg_list_t *sgl, daos_event_t *ev));
DARSHAN_FORWARD_DECL(daos_array_write, int, (daos_handle_t oh, daos_handle_t th, daos_array_iod_t *iod, d_sg_list_t *sgl, daos_event_t *ev));
DARSHAN_FORWARD_DECL(daos_array_close, int, (daos_handle_t oh, daos_event_t *ev));

/* XXX key-value API */

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
 * either the corresponding Darshan record identifier (derived from the XXX ???)
 * or by a XXX DAOS object handle, for instance. Note that, while there should
 * only be a single Darshan record identifier that indexes a daos_object_record_ref,
 * there could be multiple XXX open file objects that index it.
 */
struct daos_object_record_ref
{
    struct darshan_daos_object *object_rec;
    double last_meta_end;
    double last_read_end;
    double last_write_end;
};

struct daos_runtime
{
    void *rec_id_hash;
    void *oh_hash;
    int obj_rec_count;
    int frozen; /* flag to indicate that the counters should no longer be modified */
};

static void daos_runtime_initialize();
static struct daos_object_record_ref *daos_track_new_object_record(
    darshan_record_id rec_id, daos_obj_id_t oid);
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

// XXX update 0 to include container/pool
// XXX flags, iom for obj_open
// XXX handle array vs single val input of iods/sgls for different APIs
// XXX handle th
#define ID_GLOB_SIZE ((0*sizeof(uuid_t)) + sizeof(daos_obj_id_t))
#define DAOS_RECORD_OBJ_OPEN(__oh_p, __oid, __counter, __tm1, __tm2) do { \
    unsigned char __id_glob[ID_GLOB_SIZE]; \
    darshan_record_id __rec_id; \
    struct daos_object_record_ref *__rec_ref; \
    memcpy(__id_glob, &__oid, sizeof(daos_obj_id_t)); \
    __rec_id = darshan_hash(__id_glob, ID_GLOB_SIZE, 0); \
    __rec_ref = darshan_lookup_record_ref(daos_runtime->rec_id_hash, &__rec_id, \
        sizeof(darshan_record_id)); \
    if(!__rec_ref) __rec_ref = daos_track_new_object_record(__rec_id, __oid); \
    if(!__rec_ref) break; \
    __rec_ref->object_rec->counters[__counter] += 1; \
    if(__rec_ref->object_rec->fcounters[DAOS_F_OPEN_START_TIMESTAMP] == 0 || \
     __rec_ref->object_rec->fcounters[DAOS_F_OPEN_START_TIMESTAMP] > __tm1) \
        __rec_ref->object_rec->fcounters[DAOS_F_OPEN_START_TIMESTAMP] = __tm1; \
    __rec_ref->object_rec->fcounters[DAOS_F_OPEN_END_TIMESTAMP] = __tm2; \
    DARSHAN_TIMER_INC_NO_OVERLAP(__rec_ref->object_rec->fcounters[DAOS_F_META_TIME], \
        __tm1, __tm2, __rec_ref->last_meta_end); \
    darshan_add_record_ref(&(daos_runtime->oh_hash), __oh_p, \
        sizeof(daos_handle_t), __rec_ref); \
} while(0)

#define DAOS_RECORD_OBJ_READ(__oh, __counter, __sz, __tm1, __tm2) do { \
    struct daos_object_record_ref *__rec_ref; \
    __rec_ref = darshan_lookup_record_ref(daos_runtime->oh_hash, &__oh, \
        sizeof(daos_handle_t)); \
    if(!__rec_ref) break; \
    __rec_ref->object_rec->counters[__counter] += 1; \
    __rec_ref->object_rec->counters[DAOS_BYTES_READ] += __sz; \
    if(__rec_ref->object_rec->fcounters[DAOS_F_READ_START_TIMESTAMP] == 0 || \
     __rec_ref->object_rec->fcounters[DAOS_F_READ_START_TIMESTAMP] > __tm1) \
        __rec_ref->object_rec->fcounters[DAOS_F_READ_START_TIMESTAMP] = __tm1; \
    __rec_ref->object_rec->fcounters[DAOS_F_READ_END_TIMESTAMP] = __tm2; \
    DARSHAN_TIMER_INC_NO_OVERLAP(__rec_ref->object_rec->fcounters[DAOS_F_READ_TIME], \
        __tm1, __tm2, __rec_ref->last_read_end); \
} while(0)

#define DAOS_RECORD_OBJ_WRITE(__oh, __counter, __sz, __tm1, __tm2) do { \
    struct daos_object_record_ref *__rec_ref; \
    __rec_ref = darshan_lookup_record_ref(daos_runtime->oh_hash, &__oh, \
        sizeof(daos_handle_t)); \
    if(!__rec_ref) break; \
    __rec_ref->object_rec->counters[__counter] += 1; \
    __rec_ref->object_rec->counters[DAOS_BYTES_WRITTEN] += __sz; \
    if(__rec_ref->object_rec->fcounters[DAOS_F_WRITE_START_TIMESTAMP] == 0 || \
     __rec_ref->object_rec->fcounters[DAOS_F_WRITE_START_TIMESTAMP] > __tm1) \
        __rec_ref->object_rec->fcounters[DAOS_F_WRITE_START_TIMESTAMP] = __tm1; \
    __rec_ref->object_rec->fcounters[DAOS_F_WRITE_END_TIMESTAMP] = __tm2; \
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

/*****************************************************
 *      Wrappers for DAOS functions of interest      * 
 *****************************************************/

/* multi-level key array API */

int DARSHAN_DECL(daos_obj_open)(daos_handle_t coh, daos_obj_id_t oid, unsigned int mode, daos_handle_t *oh, daos_event_t *ev)
{
    int ret;
    double tm1, tm2;

    MAP_OR_FAIL(daos_obj_open);

    tm1 = DAOS_WTIME();
    ret = __real_daos_obj_open(coh, oid, mode, oh, ev);
    tm2 = DAOS_WTIME();

    DAOS_PRE_RECORD();
    if(!ret)
        DAOS_RECORD_OBJ_OPEN(oh, oid, DAOS_OBJ_OPENS, tm1, tm2);
    DAOS_POST_RECORD();

    return(ret);
}

#define DAOS_OBJ_IOD_SZ(__iods, __nr, __sz) do { \
    int __i, __j; \
    __sz = 0; \
    for(__i = 0; __i < __nr; __i++) { \
        if(__iods[__i].iod_size == DAOS_REC_ANY) { \
            __sz = -1; \
            break; \
        } \
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

    tm1 = DAOS_WTIME();
    ret = __real_daos_obj_fetch(oh, th, flags, dkey, nr, iods, sgls, ioms, ev);
    tm2 = DAOS_WTIME();

    DAOS_PRE_RECORD();
    if(!ret)
    {
        DAOS_OBJ_IOD_SZ(iods, nr, fetch_sz);
        DAOS_RECORD_OBJ_READ(oh, DAOS_OBJ_FETCHES, fetch_sz, tm1, tm2);
    }
    DAOS_POST_RECORD();

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

    tm1 = DAOS_WTIME();
    ret = __real_daos_obj_update(oh, th, flags, dkey, nr, iods, sgls, ev);
    tm2 = DAOS_WTIME();

    DAOS_PRE_RECORD();
    if(!ret)
    {
        DAOS_OBJ_IOD_SZ(iods, nr, update_sz);
        DAOS_RECORD_OBJ_WRITE(oh, DAOS_OBJ_UPDATES, update_sz, tm1, tm2);
    }
    DAOS_POST_RECORD();

    return(ret);
}

int DARSHAN_DECL(daos_obj_close)(daos_handle_t oh, daos_event_t *ev)
{
    int ret;
    double tm1, tm2;

    MAP_OR_FAIL(daos_obj_close);

    tm1 = DAOS_WTIME();
    ret = __real_daos_obj_close(oh, ev);
    tm2 = DAOS_WTIME();

    DAOS_PRE_RECORD();
    if(!ret)
        DAOS_RECORD_OBJ_CLOSE(oh, tm1, tm2);
    DAOS_POST_RECORD();

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

    tm1 = DAOS_WTIME();
    ret = __real_daos_array_create(coh, oid, th, cell_size, chunk_size, oh, ev);
    tm2 = DAOS_WTIME();

    DAOS_PRE_RECORD();
    if(!ret)
        DAOS_RECORD_OBJ_OPEN(oh, oid, DAOS_ARRAY_OPENS, tm1, tm2);
    DAOS_POST_RECORD();

    return(ret);
}

int DARSHAN_DECL(daos_array_open)(daos_handle_t coh, daos_obj_id_t oid,
    daos_handle_t th, unsigned int mode, daos_size_t *cell_size, daos_size_t *chunk_size,
    daos_handle_t *oh, daos_event_t *ev)
{
    int ret;
    double tm1, tm2;

    MAP_OR_FAIL(daos_array_open);

    tm1 = DAOS_WTIME();
    ret = __real_daos_array_open(coh, oid, th, mode, cell_size, chunk_size, oh, ev);
    tm2 = DAOS_WTIME();

    DAOS_PRE_RECORD();
    if(!ret)
        DAOS_RECORD_OBJ_OPEN(oh, oid, DAOS_ARRAY_OPENS, tm1, tm2);
    DAOS_POST_RECORD();

    return(ret);
}

int DARSHAN_DECL(daos_array_open_with_attr)(daos_handle_t coh, daos_obj_id_t oid, daos_handle_t th, unsigned int mode, daos_size_t cell_size, daos_size_t chunk_size, daos_handle_t *oh, daos_event_t *ev)
{
    int ret;
    double tm1, tm2;

    MAP_OR_FAIL(daos_array_open_with_attr);

    tm1 = DAOS_WTIME();
    ret = __real_daos_array_open_with_attr(coh, oid, th, mode, cell_size, chunk_size, oh, ev);
    tm2 = DAOS_WTIME();

    DAOS_PRE_RECORD();
    if(!ret)
        DAOS_RECORD_OBJ_OPEN(oh, oid, DAOS_ARRAY_OPENS, tm1, tm2);
    DAOS_POST_RECORD();
    darshan_core_fprintf(stderr, "arr open cell size = %lu\n", cell_size);

    return(ret);
}

int DARSHAN_DECL(daos_array_read)(daos_handle_t oh, daos_handle_t th,
    daos_array_iod_t *iod, d_sg_list_t *sgl, daos_event_t *ev)
{
    int ret;
    double tm1, tm2;

    MAP_OR_FAIL(daos_array_read);

    tm1 = DAOS_WTIME();
    ret = __real_daos_array_read(oh, th, iod, sgl, ev);
    tm2 = DAOS_WTIME();

    DAOS_PRE_RECORD();
    if(!ret)
    {
        DAOS_RECORD_OBJ_READ(oh, DAOS_ARRAY_READS, iod->arr_nr_read, tm1, tm2);
    }
    DAOS_POST_RECORD();

    return(ret);
}

int DARSHAN_DECL(daos_array_write)(daos_handle_t oh, daos_handle_t th,
    daos_array_iod_t *iod, d_sg_list_t *sgl, daos_event_t *ev)
{
    int ret;
    double tm1, tm2;
    int i;
    daos_size_t arr_nr_written = 0;

    MAP_OR_FAIL(daos_array_write);

    tm1 = DAOS_WTIME();
    ret = __real_daos_array_write(oh, th, iod, sgl, ev);
    tm2 = DAOS_WTIME();

    DAOS_PRE_RECORD();
    if(!ret)
    {
        for(i = 0; i < iod->arr_nr; i++)
        {
            arr_nr_written += iod->arr_rgs[i].rg_len;
        }
        DAOS_RECORD_OBJ_WRITE(oh, DAOS_ARRAY_WRITES, arr_nr_written, tm1, tm2);
    }
    DAOS_POST_RECORD();

    return(ret);
}

int DARSHAN_DECL(daos_array_close)(daos_handle_t oh, daos_event_t *ev)
{
    int ret;
    double tm1, tm2;

    MAP_OR_FAIL(daos_array_close);

    tm1 = DAOS_WTIME();
    ret = __real_daos_array_close(oh, ev);
    tm2 = DAOS_WTIME();

    DAOS_PRE_RECORD();
    if(!ret)
        DAOS_RECORD_OBJ_CLOSE(oh, tm1, tm2);
    DAOS_POST_RECORD();

    return(ret);
}

/* XXX key-value API */

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

    return;
}

static struct daos_object_record_ref *daos_track_new_object_record(
    darshan_record_id rec_id, daos_obj_id_t oid)
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
    object_rec->oid_hi = oid.hi;
    object_rec->oid_lo = oid.lo;
    rec_ref->object_rec = object_rec;
    daos_runtime->obj_rec_count++;

    return(rec_ref);
}

static void daos_finalize_object_records(void *rec_ref_p, void *user_ptr)
{
#if 0
    struct dfs_file_record_ref *rec_ref =
        (struct dfs_file_record_ref *)rec_ref_p;

    tdestroy(rec_ref->access_root, free);
#endif
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

        /* sum */
        for(j=DAOS_OBJ_OPENS; j<=DAOS_OBJ_UPDATES; j++)
        {
            tmp_obj.counters[j] = inobj->counters[j] + inoutobj->counters[j];
            if(tmp_obj.counters[j] < 0) /* make sure invalid counters are -1 exactly */
                tmp_obj.counters[j] = -1;
        }

        /* min non-zero (if available) value */
        for(j=DAOS_F_OPEN_START_TIMESTAMP; j<=DAOS_F_OPEN_START_TIMESTAMP; j++)
        {
            if((inobj->fcounters[j] < inoutobj->fcounters[j] &&
               inobj->fcounters[j] > 0) || inoutobj->fcounters[j] == 0)
                tmp_obj.fcounters[j] = inobj->fcounters[j];
            else
                tmp_obj.fcounters[j] = inoutobj->fcounters[j];
        }

        /* max */
        for(j=DAOS_F_OPEN_END_TIMESTAMP; j<=DAOS_F_OPEN_END_TIMESTAMP; j++)
        {
            if(inobj->fcounters[j] > inoutobj->fcounters[j])
                tmp_obj.fcounters[j] = inobj->fcounters[j];
            else
                tmp_obj.fcounters[j] = inoutobj->fcounters[j];
        }

        /* sum */
        for(j=DAOS_F_META_TIME; j<=DAOS_F_META_TIME; j++)
        {
            tmp_obj.fcounters[j] = inobj->fcounters[j] + inoutobj->fcounters[j];
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

#if 0
        daos_time =
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
#endif

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

    /* clean up reduction state */
    if(my_rank == 0)
    {
        int tmp_ndx = daos_rec_count - shared_rec_count;
        memcpy(&(daos_rec_buf[tmp_ndx]), red_recv_buf,
            shared_rec_count * sizeof(struct darshan_daos_object));
        free(red_recv_buf);
    }
    else
    {
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

    DAOS_LOCK();
    assert(daos_runtime);

    /* just pass back our updated total buffer size -- no need to update buffer */
    daos_rec_count = daos_runtime->obj_rec_count;
    *daos_buf_sz = daos_rec_count * sizeof(struct darshan_daos_object);

    daos_runtime->frozen = 1;

    DAOS_UNLOCK();
    return;
}

static void daos_cleanup()
{
    DAOS_LOCK();
    assert(daos_runtime);

    /* cleanup internal structures used for instrumenting */
    darshan_iter_record_refs(daos_runtime->rec_id_hash,
        &daos_finalize_object_records, NULL);
    darshan_clear_record_refs(&(daos_runtime->oh_hash), 0);
    darshan_clear_record_refs(&(daos_runtime->rec_id_hash), 1);

    free(daos_runtime);
    daos_runtime = NULL;

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
