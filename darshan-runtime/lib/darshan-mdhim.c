/*
 * Copyright (C) 2015 University of Chicago.
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
#include <assert.h>

#include <mpi.h>
#include <mdhim.h>

#include "darshan.h"
#include "darshan-dynamic.h"

#define RECORD_STRING "total-mdhim-obj-stats"

/* The DARSHAN_FORWARD_DECL macro (defined in darshan.h) is used to provide forward
 * declarations for wrapped funcions, regardless of whether Darshan is used with
 * statically or dynamically linked executables.
 */
DARSHAN_FORWARD_DECL(mdhimPut, mdhim_rm_t *, (mdhim_t *md,
            index_t *index, void *primary_key, size_t primary_key_len,
            void *value, int value_len));

DARSHAN_FORWARD_DECL(mdhimGet, mdhim_grm_t *, (mdhim_t *md,
        index_t *index, void *key, size_t key_len, int op));

DARSHAN_FORWARD_DECL(mdhimInit, int, (mdhim_t *md, mdhim_options_t *opts));

/* The mdhim_record_ref structure maintains necessary runtime metadata
 * for the MDHIM module record (darshan_mdhim_record structure, defined in
 * darshan-mdhim-log-format.h) pointed to by 'record_p'. This metadata
 * assists with the instrumenting of specific statistics in the record.
 *
 * RATIONALE: the MDHIM module needs to track some stateful, volatile
 * information about each record it has registered (for instance, most
 * recent  access time, amount of bytes transferred) to aid in
 * instrumentation, but this information can't be stored in the
 * darshan_mdhim_record struct
 * because we don't want it to appear in the final darshan log file.  We
 * therefore associate a mdhim_record_ref struct with each
 * darshan_mdhim_record struct in order to track this information (i.e.,
 * the mapping between mdhim_record_ref structs to darshan_mdhim_record
 * structs is one-to-one).
 *
 * NOTE: we use the 'darshan_record_ref' interface (in darshan-common)
 * to associate different types of handles with this mdhim_record_ref
 * struct.  This allows us to index this struct (and the underlying
 * record) by using either the corresponding Darshan record identifier
 * or by any other arbitrary handle.
 */
struct mdhim_record_ref
{
    /* Darshan record for the MDHIM example module */
    struct darshan_mdhim_record *record_p;

    /* ... other runtime data ... */
};

/* The mdhim_runtime structure maintains necessary state for storing
 * MDHIM records and for coordinating with darshan-core at shutdown time.
 */
struct mdhim_runtime
{
    /* rec_id_hash is a pointer to a hash table of MDHIM module record
     * references, indexed by Darshan record id
     */
    void *rec_id_hash;
    /* number of records currently tracked */
    int rec_count;
    int record_size;
};

/* internal helper functions for the MDHIM module */
static void mdhim_runtime_initialize(
    void);
static struct mdhim_record_ref *mdhim_track_new_record(
    darshan_record_id rec_id, int nr_servers, const char *name);

/* forward declaration for MDHIM functions needed to interface
 * with darshan-core
 */
static void mdhim_mpi_redux(
    void *mdhim_buf, MPI_Comm mod_comm,
    darshan_record_id *shared_recs, int shared_rec_count);
static void mdhim_output(
    void **mdhim_buf, int *mdhim_buf_sz);
static void mdhim_cleanup(
    void);

/* mdhim_runtime is the global data structure encapsulating "MDHIM"
 * module state */
static struct mdhim_runtime *mdhim_runtime = NULL;
/* The mdhim_runtime_mutex is a lock used when updating the
 * mdhim_runtime global structure (or any other global data structures).
 * This is necessary to avoid race conditions as multiple threads may
 * execute function wrappers and update module state.
 * NOTE: Recursive mutexes are used in case functions wrapped by this
 * module call other wrapped functions that would result in deadlock,
 * otherwise. This mechanism may not be necessary for all
 * instrumentation modules.
 */
static pthread_mutex_t mdhim_runtime_mutex = PTHREAD_RECURSIVE_MUTEX_INITIALIZER_NP;
/* my_rank indicates the MPI rank of this process */
static int my_rank = -1;

/* macros for obtaining/releasing the "MDHIM" module lock */
#define MDHIM_LOCK() pthread_mutex_lock(&mdhim_runtime_mutex)
#define MDHIM_UNLOCK() pthread_mutex_unlock(&mdhim_runtime_mutex)

/* the MDHIM_PRE_RECORD macro is executed before performing MDHIM
 * module instrumentation of a call. It obtains a lock for updating
 * module data strucutres, and ensure the MDHIM module has been properly
 * initialized before instrumenting.
 */
#define MDHIM_PRE_RECORD() do { \
    MDHIM_LOCK(); \
    if(!darshan_core_disabled_instrumentation()) { \
        if(!mdhim_runtime) mdhim_runtime_initialize(); \
        if(mdhim_runtime) break; \
    } \
    MDHIM_UNLOCK(); \
} while(0)

/* the MDHIM_POST_RECORD macro is executed after performing MDHIM
 * module instrumentation. It simply releases the module lock.
 */
#define MDHIM_POST_RECORD() do { \
    MDHIM_UNLOCK(); \
} while(0)

/* macro for instrumenting the "MDHIM" module's put function */
#define MDHIM_RECORD_PUT(__ret, __md, __id, __vallen, __tm1, __tm2) do{ \
    darshan_record_id rec_id; \
    struct mdhim_record_ref *rec_ref; \
    double __elapsed = __tm2 - __tm1; \
    /* if put returns error (return code < 0), don't instrument anything */ \
    if(__ret < 0) break; \
    /* posix uses '__name' to generate a unique Darshan record id */ \
    /* but mdhim doesn't use string names for its keyval store  */ \
    rec_id = darshan_core_gen_record_id(RECORD_STRING); \
    /* look up a record reference for this record id using darshan rec_ref interface */ \
    rec_ref = darshan_lookup_record_ref(mdhim_runtime->rec_id_hash, &rec_id, sizeof(darshan_record_id)); \
    /* if no reference was found, that's odd: was init not called? */ \
    if(!rec_ref) break; \
    /* increment counter indicating number of calls to 'put' */ \
    rec_ref->record_p->counters[MDHIM_PUTS] += 1; \
    /* store max data value for calls to 'put', and corresponding time duration */ \
    if(rec_ref->record_p->counters[MDHIM_PUT_MAX_SIZE] < __vallen) { \
        rec_ref->record_p->counters[MDHIM_PUT_MAX_SIZE] = __vallen; \
        rec_ref->record_p->fcounters[MDHIM_F_PUT_MAX_DURATION] = __elapsed; \
    } \
    /* store timestamp of first call to 'put' */ \
    if(rec_ref->record_p->fcounters[MDHIM_F_PUT_TIMESTAMP] == 0 || \
     rec_ref->record_p->fcounters[MDHIM_F_PUT_TIMESTAMP] > __tm1) \
        rec_ref->record_p->fcounters[MDHIM_F_PUT_TIMESTAMP] = __tm1; \
    /* record which server gets this request */ \
    rec_ref->record_p->server_histogram[(__id)]++; \
} while(0)

/* macro for instrumenting the "MDHIM" module's get function */
#define MDHIM_RECORD_GET(__ret, __md, __id, __keylen, __tm1, __tm2) do{ \
    darshan_record_id rec_id; \
    struct mdhim_record_ref *rec_ref; \
    double __elapsed = __tm2 - __tm1; \
    /* if get returns error (return code < 0), don't instrument anything */ \
    if(__ret == NULL) break; \
    /* posix uses '__name' to generate a unique Darshan record id */ \
    /* but mdhim doesn't use string names for its keyval store  */ \
    rec_id = darshan_core_gen_record_id(RECORD_STRING); \
    /* look up a record reference for this record id using darshan rec_ref interface */ \
    rec_ref = darshan_lookup_record_ref(mdhim_runtime->rec_id_hash, &rec_id, sizeof(darshan_record_id)); \
    /* if no reference was found, we're in trouble */ \
    if(!rec_ref) break; \
    /* increment counter indicating number of calls to 'get' */ \
    rec_ref->record_p->counters[MDHIM_GETS] += 1; \
    /* store max data value for calls to 'get', and corresponding time duration */ \
    if(rec_ref->record_p->counters[MDHIM_GET_MAX_SIZE] < __keylen) { \
        rec_ref->record_p->counters[MDHIM_GET_MAX_SIZE] = __keylen; \
        rec_ref->record_p->fcounters[MDHIM_F_GET_MAX_DURATION] = __elapsed; \
    } \
    /* store timestamp of first call to 'get' */ \
    if(rec_ref->record_p->fcounters[MDHIM_F_GET_TIMESTAMP] == 0 || \
     rec_ref->record_p->fcounters[MDHIM_F_GET_TIMESTAMP] > __tm1) \
        rec_ref->record_p->fcounters[MDHIM_F_GET_TIMESTAMP] = __tm1; \
    /* server distribution */ \
    rec_ref->record_p->server_histogram[(__id)]++; \
} while(0)

/**********************************************************
 *    Wrappers for "MDHIM" module functions of interest    * 
 **********************************************************/

/* The DARSHAN_DECL macro provides the appropriate wrapper function
 * names, depending on whether the Darshan library is statically or
 * dynamically linked.
 */

int DARSHAN_DECL(mdhimInit)(mdhim_t *md, mdhim_options_t *opts)
{
    /* not counting or tracking anything in this routine, but grabbing a
     * bit of information about the mdhim instance */

    int ret;
    darshan_record_id rec_id;
    struct mdhim_record_ref *rec_ref;
    int nr_servers;

    MPI_Comm_size(opts->comm, &nr_servers);

    MDHIM_PRE_RECORD();
    /* posix uses '__name' to generate a unique Darshan record id
       but mdhim doesn't use string names for its keyval store. Assumes
       one MDHIM instance */
    rec_id = darshan_core_gen_record_id(RECORD_STRING);
    /* look up a record reference for this record id using darshan
     * rec_ref interface */
    rec_ref = darshan_lookup_record_ref(mdhim_runtime->rec_id_hash,
            &rec_id, sizeof(darshan_record_id));
    /* if no reference was found, track a new one for this record */
    if(!rec_ref) rec_ref = mdhim_track_new_record(rec_id,
            nr_servers, RECORD_STRING);
    /* if we still don't have a valid reference, well that's too dang bad */
    if (rec_ref) rec_ref->record_p->counters[MDHIM_SERVERS] = nr_servers;

    MDHIM_POST_RECORD();

    MAP_OR_FAIL(mdhimInit);
    ret = __real_mdhimInit(md, opts);
    return ret;

}
mdhim_rm_t *DARSHAN_DECL(mdhimPut)(mdhim_t *md,
        index_t *index,
        void *key, size_t key_len,
        void *value, size_t value_len)
{
    mdhim_rm_t *ret;
    double tm1, tm2;

    /* The MAP_OR_FAIL macro attempts to obtain the address of the actual
     * underlying put function call (__real_put), in the case of LD_PRELOADing
     * the Darshan library. For statically linked executables, this macro is
     * just a NOP. 
     */
    MAP_OR_FAIL(mdhimPut);

    /* In general, Darshan wrappers begin by calling the real version of the
     * given wrapper function. Timers are used to record the duration of this
     * operation. */
    tm1 = darshan_core_wtime();
    ret = __real_mdhimPut(md, index, key, key_len, value, value_len);
    tm2 = darshan_core_wtime();

    int server_id = mdhimWhichDB(md, key, key_len);

    MDHIM_PRE_RECORD();
    /* Call macro for instrumenting data for mdhimPut function calls. */
    /* TODO: call the mdhim hash routines and instrument which servers
     * get this request */
    MDHIM_RECORD_PUT(ret, md, server_id, value_len, tm1, tm2);

    MDHIM_POST_RECORD();

    return(ret);
}

mdhim_grm_t * DARSHAN_DECL(mdhimGet)(mdhim_t *md,
        index_t *index, void *key, size_t key_len,
        enum TransportGetMessageOp op)
{
    mdhim_grm_t *ret;
    double tm1, tm2;

    MAP_OR_FAIL(mdhimGet);

    /* In general, Darshan wrappers begin by calling the real version of the
     * given wrapper function. Timers are used to record the duration of this
     * operation. */
    tm1 = darshan_core_wtime();
    ret = __real_mdhimGet(md, index, key, key_len, op);
    tm2 = darshan_core_wtime();

    int server_id = mdhimWhichDB(md, key, key_len);

    MDHIM_PRE_RECORD();
    /* Call macro for instrumenting data for get function calls. */
    MDHIM_RECORD_GET(ret, md, server_id, key_len, tm1, tm2);
    MDHIM_POST_RECORD();
    return ret;
}

/**********************************************************
 * Internal functions for manipulating MDHIM module state *
 **********************************************************/

/* Initialize internal MDHIM module data structures and register with
 * darshan-core. */
static void mdhim_runtime_initialize()
{
    size_t mdhim_buf_size;
    darshan_module_funcs mod_funcs = {
    .mod_redux_func = &mdhim_mpi_redux,
    .mod_output_func = &mdhim_output,
    .mod_cleanup_func = &mdhim_cleanup
    };

    /* try and store a default number of records for this module */
    mdhim_buf_size = DARSHAN_DEF_MOD_REC_COUNT *
        sizeof(struct darshan_mdhim_record);

    /* register the MDHIM module with the darshan-core component */
    darshan_core_register_module(
        DARSHAN_MDHIM_MOD,   /* Darshan module identifier, defined in darshan-log-format.h */
        mod_funcs,
        &mdhim_buf_size,
        &my_rank,
        NULL);


    /* initialize module's global state */
    mdhim_runtime = calloc(1, sizeof(*mdhim_runtime));
    if(!mdhim_runtime)
    {
        darshan_core_unregister_module(DARSHAN_MDHIM_MOD);
        return;
    }
    return;
}

/* allocate and track a new MDHIM module record */
static struct mdhim_record_ref *mdhim_track_new_record(
    darshan_record_id rec_id, int nr_servers, const char *name)
{
    struct darshan_mdhim_record *record_p = NULL;
    struct mdhim_record_ref *rec_ref = NULL;
    int ret;
    size_t rec_size;

    rec_ref = calloc(1, sizeof(*rec_ref));
    if(!rec_ref)
        return(NULL);

    /* allocate a new MDHIM record reference and add it to the hash
     * table, using the Darshan record identifier as the handle
     */
    ret = darshan_add_record_ref(&(mdhim_runtime->rec_id_hash), &rec_id,
        sizeof(darshan_record_id), rec_ref);
    if(ret == 0)
    {
        free(rec_ref);
        return(NULL);
    }

    rec_size = MDHIM_RECORD_SIZE(nr_servers);
    /* register the actual file record with darshan-core so it is persisted
     * in the log file
     */
    record_p = darshan_core_register_record(
        rec_id,
        name,
        DARSHAN_MDHIM_MOD,
        rec_size,
        NULL);

    if(!record_p)
    {
        /* if registration fails, delete record reference and return */
        darshan_delete_record_ref(&(mdhim_runtime->rec_id_hash),
            &rec_id, sizeof(darshan_record_id));
        free(rec_ref);
        return(NULL);
    }

    /* registering this file record was successful, so initialize some fields */
    record_p->base_rec.id = rec_id;
    record_p->base_rec.rank = my_rank;
    rec_ref->record_p = record_p;
    mdhim_runtime->rec_count++;


    /* return pointer to the record reference */
    return(rec_ref);
}

static void mdhim_record_reduction_op(void *infile_v, void *inoutfile_v,
        int *len, MPI_Datatype *datatype)
{
    struct darshan_mdhim_record *tmp_rec;
    struct darshan_mdhim_record *inrec = infile_v;
    struct darshan_mdhim_record *inoutrec = inoutfile_v;
    int i, j;

    for (i=0; i< *len; i++) {
        /* can't use 'sizeof': server count historgram */
        tmp_rec = calloc(1,
                MDHIM_RECORD_SIZE(inrec->counters[MDHIM_SERVERS]));
        tmp_rec->base_rec.id = inrec->base_rec.id;
        tmp_rec->base_rec.rank = -1;

        for (j=MDHIM_PUTS; j<=MDHIM_GETS; j++) {
            tmp_rec->counters[j] = inrec->counters[j] +
                inoutrec->counters[j];
        }

        for (j=MDHIM_PUT_MAX_SIZE; j<=MDHIM_GET_MAX_SIZE; j++) {
            tmp_rec->counters[j] = (
                (inrec->counters[j] > inoutrec->counters[j] ) ?
                inrec->counters[j] :
                inoutrec->counters[j]);
        }
        tmp_rec->counters[MDHIM_SERVERS] = inrec->counters[MDHIM_SERVERS];

        /* min non-zero value */
        for (j=MDHIM_F_PUT_TIMESTAMP; j<=MDHIM_F_GET_TIMESTAMP; j++)
        {
            if (( inrec->fcounters[j] < inoutrec->fcounters[j] &&
                        inrec->fcounters[j] > 0)
                    || inoutrec->fcounters[j] == 0)
                tmp_rec->fcounters[j] = inrec->fcounters[j];
            else
                tmp_rec->fcounters[j] = inoutrec->fcounters[j];
        }
        /* max */
        for (j=MDHIM_F_PUT_MAX_DURATION; j<=MDHIM_F_GET_MAX_DURATION; j++)
        {
            tmp_rec->fcounters[j] = (
                    (inrec->fcounters[j] > inoutrec->fcounters[j]) ?
                        inrec->fcounters[j] :
                        inoutrec->fcounters[j]);
        }
        /* dealing with server histogram a little odd.  Every client kept track
         * of which servers it sent to, so we'll simply sum them all up.  The
         * data lives at the end of the struct (remember, alocated based on
         * MDHIM_RECORD_SIZE macro)  */
        for (j=0; j< tmp_rec->counters[MDHIM_SERVERS]; j++) {
            tmp_rec->server_histogram[j] = inrec->server_histogram[j] +
                inoutrec->server_histogram[j];
        }
        memcpy(inoutrec, tmp_rec,
                MDHIM_RECORD_SIZE(tmp_rec->counters[MDHIM_SERVERS]));
        free(tmp_rec);

        /* updating not as simple as incrementing, unfortunately */
        infile_v = (char *) infile_v +
            MDHIM_RECORD_SIZE(tmp_rec->counters[MDHIM_SERVERS]);
        inoutfile_v = (char *)inoutfile_v +
            MDHIM_RECORD_SIZE(tmp_rec->counters[MDHIM_SERVERS]);
    }
    return;
}
/*****************************************************************************
 * functions exported by the MDHIM module for coordinating with darshan-core *
 *****************************************************************************/

static void mdhim_mpi_redux(
    void *mdhim_buf, MPI_Comm mod_comm,
    darshan_record_id *shared_recs, int shared_rec_count)
{
    int i, nr_servers=0;
    struct mdhim_record_ref *rec_ref;
    struct darshan_mdhim_record *red_send_buf = NULL;
    struct darshan_mdhim_record *red_recv_buf = NULL;
    MPI_Datatype red_type;
    MPI_Op red_op;
    /* walking through these arrays will be awkward if there is more than one
     * record: the 'server_histogram' field is variable */
    struct darshan_mdhim_record *mdhim_rec_buf =
        *(struct darshan_mdhim_record **)mdhim_buf;
    int mdhim_rec_count;

    MDHIM_LOCK();
    assert(mdhim_runtime);

    mdhim_rec_count = mdhim_runtime->rec_count;

    /* taking the approach in darshan-mpiio.c, except MDHIM is always a
     * "shared file" for now. */
    assert(mdhim_rec_count == shared_rec_count);

    if (shared_rec_count && !getenv("DARSHAN_DISABLE_SHARED_REDUCTION"))
    {
        /* can the number of mdhim servers change? I suppose if there were
         * multiple mdhim instances, each instance could have a different
         * number of servers.  If that's the case, I'll have to make some of
         * the memory allocations variable (and I don't do that yet) */
        rec_ref = darshan_lookup_record_ref(mdhim_runtime->rec_id_hash,
                &shared_recs[0], sizeof(darshan_record_id));
        rec_ref->record_p->base_rec.rank = -1;
        nr_servers = rec_ref->record_p->counters[MDHIM_SERVERS];
        mdhim_runtime->record_size = MDHIM_RECORD_SIZE(nr_servers);

        /* there is probably only one shared record, but go ahead and
         * check for any other shared records, setting their rank to -1.
         * We will remove those from the report later */
        /* starting from '1' since we grabbed the first record above */
        for (i=1; i< shared_rec_count; i++)
        {
            rec_ref = darshan_lookup_record_ref(mdhim_runtime->rec_id_hash,
                    &shared_recs[i], sizeof(darshan_record_id));
            assert(rec_ref);
            assert(nr_servers == rec_ref->record_p->counters[MDHIM_SERVERS]);
            rec_ref->record_p->base_rec.rank = -1;
        }

        darshan_record_sort(mdhim_rec_buf, mdhim_runtime->rec_count,
                MDHIM_RECORD_SIZE(nr_servers));

        /* make send_buf point to shared files at end */
        red_send_buf = &(mdhim_rec_buf[mdhim_rec_count-shared_rec_count]);

        if (my_rank == 0)
        {
            red_recv_buf = malloc(shared_rec_count *
                    MDHIM_RECORD_SIZE(nr_servers));
            if (!red_recv_buf)
            {
                MDHIM_UNLOCK();
                return;
            }
        }

        PMPI_Type_contiguous(MDHIM_RECORD_SIZE(nr_servers),
                MPI_BYTE, &red_type);
        PMPI_Type_commit(&red_type);
        PMPI_Op_create(mdhim_record_reduction_op, 1, &red_op);
        PMPI_Reduce(red_send_buf, red_recv_buf,
                shared_rec_count, red_type, red_op, 0, mod_comm);

        if (my_rank == 0)
        {
            memcpy(&(mdhim_rec_buf[0]), red_recv_buf,
                    shared_rec_count *
                    MDHIM_RECORD_SIZE(nr_servers));
            free(red_recv_buf);
        }
        else
        {
            /* drop shared records from non-root ranks or we'll end up writing too
             * much */
            mdhim_runtime->rec_count -= shared_rec_count;
        }

        PMPI_Type_free(&red_type);
        PMPI_Op_free(&red_op);
    }

    MDHIM_UNLOCK();
    return;
}

/* Pass output data for the MDHIM module back to darshan-core to log to file
 */
static void mdhim_output(
    void **mdhim_buf,
    int *mdhim_buf_sz)
{
    MDHIM_LOCK();
    assert(mdhim_runtime);

    *mdhim_buf_sz = mdhim_runtime->rec_count * mdhim_runtime->record_size;

    MDHIM_UNLOCK();
    return;
}

static void mdhim_cleanup()
{
    MDHIM_LOCK();
    assert(mdhim_runtime);

    /* iterate the hash of record references and free them */
    darshan_clear_record_refs(&(mdhim_runtime->rec_id_hash), 1);

    free(mdhim_runtime);
    mdhim_runtime = NULL;

    MDHIM_UNLOCK();
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
