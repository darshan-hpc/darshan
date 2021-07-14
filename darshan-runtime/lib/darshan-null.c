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

#include "darshan.h"
#include "darshan-dynamic.h"

/* The "NULL" module is an example instrumentation module implementation provided
 * with Darshan, primarily to indicate how arbitrary modules may be integrated
 * into Darshan. In particular, this module demonstrates how to develop wrapper
 * functions for intercepting functions of interest, how to best manage necessary
 * runtime data structures, and how to coordinate with the darshan-core component,
 * among other things. This module is not linked with the darshan-runtime library; 
 * it is intended mainly to serve as a basic stubbed out module implementation
 * that may be reused and expanded on by developers adding new instrumentation modules.
 */

/* The DARSHAN_FORWARD_DECL macro (defined in darshan.h) is used to provide forward
 * declarations for wrapped funcions, regardless of whether Darshan is used with
 * statically or dynamically linked executables.
 */
DARSHAN_FORWARD_DECL(foo, int, (const char *name, int arg1));

/* The null_record_ref structure maintains necessary runtime metadata
 * for the NULL module record (darshan_null_record structure, defined in
 * darshan-null-log-format.h) pointed to by 'record_p'. This metadata
 * assists with the instrumenting of specific statistics in the record.
 *
 * RATIONALE: the NULL module needs to track some stateful, volatile 
 * information about each record it has registered (for instance, most
 * recent  access time, amount of bytes transferred) to aid in instrumentation, 
 * but this information can't be stored in the darshan_null_record struct
 * because we don't want it to appear in the final darshan log file.  We 
 * therefore associate a null_record_ref struct with each darshan_null_record
 * struct in order to track this information (i.e., the mapping between
 * null_record_ref structs to darshan_null_record structs is one-to-one).
 *
 * NOTE: we use the 'darshan_record_ref' interface (in darshan-common) to
 * associate different types of handles with this null_record_ref struct.
 * This allows us to index this struct (and the underlying record) by using
 * either the corresponding Darshan record identifier or by any other arbitrary
 * handle. For the NULL module, the only handle we use to track record
 * references are Darshan record identifiers.
 */
struct null_record_ref
{
    /* Darshan record for the "NULL" example module */
    struct darshan_null_record *record_p;

    /* ... other runtime data ... */
};

/* The null_runtime structure maintains necessary state for storing
 * NULL records and for coordinating with darshan-core at shutdown time.
 */
struct null_runtime
{
    /* rec_id_hash is a pointer to a hash table of NULL module record
     * references, indexed by Darshan record id
     */
    void *rec_id_hash;
    /* number of records currently tracked */
    int rec_count;
};

/* internal helper functions for the NULL module */
static void null_runtime_initialize(
    void);
static struct null_record_ref *null_track_new_record(
    darshan_record_id rec_id, const char *name);

/* forward declaration for NULL output/cleanup functions needed to interface
 * with darshan-core
 */
static void null_output(void **null_buf, int *null_buf_sz);
static void null_cleanup(void);

/* null_runtime is the global data structure encapsulating "NULL" module state */
static struct null_runtime *null_runtime = NULL;
/* The null_runtime_mutex is a lock used when updating the null_runtime global
 * structure (or any other global data structures). This is necessary to avoid race
 * conditions as multiple threads may execute function wrappers and update module state.
 * NOTE: Recursive mutexes are used in case functions wrapped by this module call
 * other wrapped functions that would result in deadlock, otherwise. This mechanism
 * may not be necessary for all instrumentation modules.
 */
static pthread_mutex_t null_runtime_mutex = PTHREAD_RECURSIVE_MUTEX_INITIALIZER_NP;
/* my_rank indicates the MPI rank of this process */
static int my_rank = -1;

/* macros for obtaining/releasing the "NULL" module lock */
#define NULL_LOCK() pthread_mutex_lock(&null_runtime_mutex)
#define NULL_UNLOCK() pthread_mutex_unlock(&null_runtime_mutex)

/* the NULL_PRE_RECORD macro is executed before performing NULL
 * module instrumentation of a call. It obtains a lock for updating
 * module data strucutres, and ensure the NULL module has been properly
 * initialized before instrumenting.
 */
#define NULL_PRE_RECORD() do { \
    NULL_LOCK(); \
    if(!darshan_core_disabled_instrumentation()) { \
        if(!null_runtime) null_runtime_initialize(); \
        if(null_runtime) break; \
    } \
    NULL_UNLOCK(); \
    return(ret); \
} while(0)

/* the NULL_POST_RECORD macro is executed after performing NULL
 * module instrumentation. It simply releases the module lock.
 */
#define NULL_POST_RECORD() do { \
    NULL_UNLOCK(); \
} while(0)

/* macro for instrumenting the "NULL" module's foo function */
#define NULL_RECORD_FOO(__ret, __name, __dat, __tm1, __tm2) do{ \
    darshan_record_id rec_id; \
    struct null_record_ref *rec_ref; \
    double __elapsed = __tm2 - __tm1; \
    /* if foo returns error (return code < 0), don't instrument anything */ \
    if(__ret < 0) break; \
    /* use '__name' to generate a unique Darshan record id */ \
    rec_id = darshan_core_gen_record_id(__name); \
    /* look up a record reference for this record id using darshan rec_ref interface */ \
    rec_ref = darshan_lookup_record_ref(null_runtime->rec_id_hash, &rec_id, sizeof(darshan_record_id)); \
    /* if no reference was found, track a new one for this record */ \
    if(!rec_ref) rec_ref = null_track_new_record(rec_id, __name); \
    /* if we still don't have a valid reference, back out */ \
    if(!rec_ref) break; \
    /* increment counter indicating number of calls to 'foo' */ \
    rec_ref->record_p->counters[NULL_FOOS] += 1; \
    /* store max data value for calls to 'foo', and corresponding time duration */ \
    if(rec_ref->record_p->counters[NULL_FOO_MAX_DAT] < __dat) { \
        rec_ref->record_p->counters[NULL_FOO_MAX_DAT] = __dat; \
        rec_ref->record_p->fcounters[NULL_F_FOO_MAX_DURATION] = __elapsed; \
    } \
    /* store timestamp of first call to 'foo' */ \
    if(rec_ref->record_p->fcounters[NULL_F_FOO_TIMESTAMP] == 0 || \
     rec_ref->record_p->fcounters[NULL_F_FOO_TIMESTAMP] > __tm1) \
        rec_ref->record_p->fcounters[NULL_F_FOO_TIMESTAMP] = __tm1; \
} while(0)

/**********************************************************
 *    Wrappers for "NULL" module functions of interest    * 
 **********************************************************/

/* The DARSHAN_DECL macro provides the appropriate wrapper function names,
 * depending on whether the Darshan library is statically or dynamically linked.
 */
int DARSHAN_DECL(foo)(const char* name, int arg1)
{
    ssize_t ret;
    double tm1, tm2;

    /* The MAP_OR_FAIL macro attempts to obtain the address of the actual
     * underlying foo function call (__real_foo), in the case of LD_PRELOADing
     * the Darshan library. For statically linked executables, this macro is
     * just a NOP. 
     */
    MAP_OR_FAIL(foo);

    /* In general, Darshan wrappers begin by calling the real version of the
     * given wrapper function. Timers are used to record the duration of this
     * operation. */
    tm1 = darshan_core_wtime();
    ret = __real_foo(name, arg1);
    tm2 = darshan_core_wtime();

    NULL_PRE_RECORD();
    /* Call macro for instrumenting data for foo function calls. */
    NULL_RECORD_FOO(ret, name, arg1, tm1, tm2);
    NULL_POST_RECORD();

    return(ret);
}

/**********************************************************
 * Internal functions for manipulating NULL module state *
 **********************************************************/

/* Initialize internal NULL module data structures and register with darshan-core. */
static void null_runtime_initialize()
{
    size_t null_buf_size;
    darshan_module_funcs mod_funcs = {
#ifdef HAVE_MPI
    /* NOTE: the redux function can be used to run collective operations prior to
     * shutting down the module. Typically, module developers will want to run a
     * reduction on shared data records (passed in in the 'shared_recs' array),
     * but other collective routines can be run here as well. For a detailed
     * example illustrating how to run shared file reductions, consider the
     * POSIX or MPIIO instrumentation modules, as they both implement this
     * functionality.
     */
    .mod_redux_func = NULL,
#endif
    .mod_output_func = &null_output,
    .mod_cleanup_func = &null_cleanup
    };

    /* try and store a default number of records for this module */
    null_buf_size = DARSHAN_DEF_MOD_REC_COUNT * sizeof(struct darshan_null_record);

    /* register the NULL module with the darshan-core component */
    darshan_core_register_module(
        DARSHAN_NULL_MOD,   /* Darshan module identifier, defined in darshan-log-format.h */
        mod_funcs,
        &null_buf_size,
        &my_rank,
        NULL);

    /* initialize module's global state */
    null_runtime = malloc(sizeof(*null_runtime));
    if(!null_runtime)
    {
        darshan_core_unregister_module(DARSHAN_NULL_MOD);
        return;
    }
    memset(null_runtime, 0, sizeof(*null_runtime));

    return;
}

/* allocate and track a new NULL module record */
static struct null_record_ref *null_track_new_record(
    darshan_record_id rec_id, const char *name)
{
    struct darshan_null_record *record_p = NULL;
    struct null_record_ref *rec_ref = NULL;
    int ret;

    rec_ref = malloc(sizeof(*rec_ref));
    if(!rec_ref)
        return(NULL);
    memset(rec_ref, 0, sizeof(*rec_ref));

    /* allocate a new NULL record reference and add it to the hash
     * table, using the Darshan record identifier as the handle
     */
    ret = darshan_add_record_ref(&(null_runtime->rec_id_hash), &rec_id,
        sizeof(darshan_record_id), rec_ref);
    if(ret == 0)
    {
        free(rec_ref);
        return(NULL);
    }

    /* register the actual file record with darshan-core so it is persisted
     * in the log file
     */
    record_p = darshan_core_register_record(
        rec_id,
        name,
        DARSHAN_NULL_MOD,
        sizeof(struct darshan_null_record),
        NULL);

    if(!record_p)
    {
        /* if registration fails, delete record reference and return */
        darshan_delete_record_ref(&(null_runtime->rec_id_hash),
            &rec_id, sizeof(darshan_record_id));
        free(rec_ref);
        return(NULL);
    }

    /* registering this file record was successful, so initialize some fields */
    record_p->base_rec.id = rec_id;
    record_p->base_rec.rank = my_rank;
    rec_ref->record_p = record_p;
    null_runtime->rec_count++;

    /* return pointer to the record reference */
    return(rec_ref);
}

/**************************************************************************************
 *    functions exported by the "NULL" module for coordinating with darshan-core      *
 **************************************************************************************/

/* Pass output data for the "NULL" module back to darshan-core to log to file
 */
static void null_output(
    void **null_buf,
    int *null_buf_sz)
{
    NULL_LOCK();
    assert(null_runtime);

    /* Just set the output size according to the number of records currently
     * being tracked. In general, the module can decide to throw out records
     * that have been previously registered by shuffling around memory in
     * 'null_buf' -- 'null_buf' and 'null_buf_sz' both are passed as pointers
     * so they can be updated by the shutdown function potentially. 
     */
    *null_buf_sz = null_runtime->rec_count * sizeof(struct darshan_null_record);

    NULL_UNLOCK();
    return;
}

/* Cleanup/free internal data structures
 */
static void null_cleanup()
{
    /* cleanup internal structures used for instrumenting */
    NULL_LOCK();

    /* iterate the hash of record references and free them */
    darshan_clear_record_refs(&(null_runtime->rec_id_hash), 1);

    free(null_runtime);
    null_runtime = NULL;

    NULL_UNLOCK();
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
