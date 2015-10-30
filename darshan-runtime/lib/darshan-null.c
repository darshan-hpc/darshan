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
#include <stdlib.h>
#include <pthread.h>
#include <assert.h>

#include "uthash.h"
#include "darshan.h"
#include "darshan-null-log-format.h"

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
 * declarations for wrapped funcions, regardless if Darshan is used with statically
 * or dynamically linked executables.
 */
DARSHAN_FORWARD_DECL(foo, int, (const char *name, int arg1, int arg2));

/* The null_record_runtime structure maintains necessary runtime metadata
 * for a "NULL" module data record (darshan_null_record structure, defined
 * in darshan-null-log-format.h). This metadata assists with the instrumenting
 * of specific statistics in the file record.
 *
 * RATIONALE: In general, a module may need to track some stateful, volatile 
 * information regarding specific I/O statistics to aid in the instrumentation
 * process. However, this information should not be stored in the darshan_null_record
 * struct because we don't want it to appear in the final darshan log file.
 * We therefore associate a null_record_runtime structure with each darshan_null_record
 * structure in order to track this information.
 *
 * NOTE: The null_record_runtime struct contains a pointer to a darshan_null_record
 * struct (see the *record_p member) rather than simply embedding an entire
 * darshan_null_record struct.  This is done so that all of the darshan_null_record
 * structs can be kept contiguous in memory as a single array to simplify
 * reduction, compression, and storage.
 */
struct null_record_runtime
{
    /* Darshan record for the "NULL" example module */
    struct darshan_null_record* record_p;

    /* ... other runtime data ... */

    /* hash table link for this record */
    /* NOTE: it is entirely up to the module developer how to persist module
     * records in memory as the instrumented application runs. These records
     * could just as easily be stored in an array or linked list. That said,
     * the data structure selection should be mindful of the resulting memory
     * footprint and search time complexity to attempt minimize Darshan overheads.
     * hash table and linked list implementations are available in uthash.h and
     * utlist.h, respectively.
     */
    UT_hash_handle hlink;
};

/* The null_runtime structure simply encapsulates global data structures needed
 * by the module for instrumenting functions of interest and providing the output
 * I/O data for this module to the darshan-core component at shutdown time.
 */
struct null_runtime
{
    /* runtime_record_array is the array of runtime records for the "NULL" module. */
    struct null_record_runtime* runtime_record_array;
    /* record_array is the array of high-level Darshan records for the "NULL" module,
     * each corresponding to the the runtime record structure stored at the same array
     * index in runtime_record_array.
     */
    struct darshan_null_record* record_array;
    /* file_array_size is the maximum amount of records that can be stored in 
     * record_array (and consequentially, runtime_record_array).
     */
    int rec_array_size;
    /* file_array_ndx is the current index into both runtime_record_array and
     * record_array.
     */
    int rec_array_ndx;
    /* record_hash is a pointer to a hash table of null_record_runtime structures
     * currently maintained by the "NULL" module.
     */
    struct null_record_runtime* record_hash;
};

/* null_runtime is the global data structure encapsulating "NULL" module state */
static struct null_runtime *null_runtime = NULL;
/* The null_runtime_mutex is a lock used when updating the null_runtime global
 * structure (or any other global data structures). This is necessary to avoid race
 * conditions as multiple threads execute function wrappers and update module state.
 * NOTE: Recursive mutexes are used in case functions wrapped by this module call
 * other wrapped functions that would result in deadlock, otherwise. This mechanism
 * may not be necessary for all instrumentation modules.
 */
static pthread_mutex_t null_runtime_mutex = PTHREAD_RECURSIVE_MUTEX_INITIALIZER_NP;
/* the instrumentation_disabled flag is used to toggle wrapper functions on/off */
static int instrumentation_disabled = 0;
/* my_rank indicates the MPI rank of this process */
static int my_rank = -1;

/* internal helper functions for the "NULL" module */
static void null_runtime_initialize(void);
static struct null_record_runtime* null_record_by_name(const char *name);

/* forward declaration for module functions needed to interface with darshan-core */
static void null_begin_shutdown(void);
static void null_get_output_data(MPI_Comm mod_comm, darshan_record_id *shared_recs,
    int shared_rec_count, void **null_buf, int *null_buf_sz);
static void null_shutdown(void);

/* macros for obtaining/releasing the "NULL" module lock */
#define NULL_LOCK() pthread_mutex_lock(&null_runtime_mutex)
#define NULL_UNLOCK() pthread_mutex_unlock(&null_runtime_mutex)

/* macro for instrumenting the "NULL" module's foo function */
/* NOTE: this macro makes use of the DARSHAN_COUNTER_* macros defined
 * and documented in darshan.h.
 */
#define NULL_RECORD_FOO(__ret, __name, __dat, __tm1, __tm2) do{ \
    struct null_record_runtime* rec; \
    double elapsed = __tm2 - __tm1; \
    /* if foo returns error (return code < 0), don't instrument anything */ \
    if(__ret < 0) break; \
    /* use '__name' to lookup a corresponding "NULL" record */ \
    rec = null_record_by_name(__name); \
    if(!rec) break; \
    /* increment counter indicating number of calls to 'bar' */ \
    rec->record_p->counters[NULL_BARS] += 1; \
    /* store data value for most recent call to 'bar' */ \
    rec->record_p->counters[NULL_BAR_DAT] = __dat; \
    /* store timestamp of most recent call to 'bar' */ \
    rec->record_p->fcounters[NULL_F_BAR_TIMESTAMP] = __tm1; \
    /* store duration of most recent call to 'bar' */ \
    rec->record_p->fcounters[NULL_F_BAR_DURATION] = elapsed; \
} while(0)

/**********************************************************
 *    Wrappers for "NULL" module functions of interest    * 
 **********************************************************/

/* The DARSHAN_DECL macro provides the appropriate wrapper function names,
 * depending on whether the Darshan library is statically or dynamically linked.
 */
int DARSHAN_DECL(foo)(const char* name, int arg1, int arg2)
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
    ret = __real_foo(name, arg1, arg2);
    tm2 = darshan_core_wtime();

    NULL_LOCK();

    /* Before attempting to instrument I/O statistics for function foo, make
     * sure the "NULL" module runtime environment has been initialized. 
     * NOTE: this runtime environment is initialized only once -- if the
     * appropriate structures have already been initialized, this function simply
     * returns.
     */
    null_runtime_initialize();

    /* Call macro for instrumenting data for foo function calls. */
    NULL_RECORD_FOO(ret, name, arg1+arg2, tm1, tm2);

    NULL_UNLOCK();

    return(ret);
}

/**********************************************************
 * Internal functions for manipulating POSIX module state *
 **********************************************************/

/* Initialize internal POSIX module data structures and register with darshan-core. */
static void null_runtime_initialize()
{
    /* struct of function pointers for interfacing with darshan-core */
    struct darshan_module_funcs null_mod_fns =
    {
        .begin_shutdown = &null_begin_shutdown,
        .get_output_data = &null_get_output_data,
        .shutdown = &null_shutdown
    };
    int mem_limit; /* max. memory this module can consume, dictated by darshan-core */

    /* don't do anything if already initialized or instrumenation is disabled */
    if(null_runtime || instrumentation_disabled)
        return;

    /* register the "NULL" module with the darshan-core component */
    darshan_core_register_module(
        DARSHAN_NULL_MOD,   /* Darshan module identifier, defined in darshan-log-format.h */
        &null_mod_fns,
        &my_rank,
        &mem_limit,
        NULL);

    /* return if no memory assigned by darshan-core */
    if(mem_limit == 0)
        return;

    /* initialize module's global state */
    null_runtime = malloc(sizeof(*null_runtime));
    if(!null_runtime)
        return;
    memset(null_runtime, 0, sizeof(*null_runtime));

    /* Set the maximum number of data records this module may track, as indicated
     * by mem_limit (set by darshan-core).
     * NOTE: We interpret the maximum memory limit to be related to the maximum
     * amount of data which may be written to log by a single process for a given
     * module. We therefore use this maximum memory limit to determine how many
     * darshan_null_record structures we can track per process.
     */
    null_runtime->rec_array_size = mem_limit / sizeof(struct darshan_null_record);
    null_runtime->rec_array_ndx = 0;

    /* allocate both record arrays (runtime and high-level records) */
    null_runtime->runtime_record_array = malloc(null_runtime->rec_array_size *
                                                sizeof(struct null_record_runtime));
    null_runtime->record_array = malloc(null_runtime->rec_array_size *
                                        sizeof(struct darshan_null_record));
    if(!null_runtime->runtime_record_array || !null_runtime->record_array)
    {
        null_runtime->rec_array_size = 0;
        return;
    }
    memset(null_runtime->runtime_record_array, 0, null_runtime->rec_array_size *
           sizeof(struct null_record_runtime));
    memset(null_runtime->record_array, 0, null_runtime->rec_array_size *
           sizeof(struct darshan_null_record));

    return;
}

/* Search for and return a "NULL" module record corresponding to name parameter. */
static struct null_record_runtime* null_record_by_name(const char *name)
{
    struct null_record_runtime *rec = NULL;
    darshan_record_id rec_id;
    int limit_flag;

    /* Don't search for a record if the "NULL" module is not initialized or
     * if instrumentation has been toggled off.
     */
    if(!null_runtime || instrumentation_disabled)
        return(NULL);

    /* stop tracking new records if we are tracking our maximum count */
    limit_flag = (null_runtime->rec_array_ndx >= null_runtime->rec_array_size);

    /* get a unique record identifier for this record from darshan-core */
    darshan_core_register_record(
        (void*)name,
        strlen(name),
        DARSHAN_NULL_MOD,
        1,
        limit_flag,
        &rec_id,
        NULL);

    /* the file record id is set to 0 if no memory is available for tracking
     * new records -- just fall through and ignore this record
     */
    if(rec_id == 0)
    {
        return(NULL);
    }

    /* search the hash table for this file record, and return if found */
    HASH_FIND(hlink, null_runtime->record_hash, &rec_id, sizeof(darshan_record_id), rec);
    if(rec)
    {
        return(rec);
    }

    /* no existing record, assign a new one from the global array */
    rec = &(null_runtime->runtime_record_array[null_runtime->rec_array_ndx]);
    rec->record_p = &(null_runtime->record_array[null_runtime->rec_array_ndx]);

    /* set the darshan record id and corresponding process rank for this record */
    rec->record_p->f_id = rec_id;
    rec->record_p->rank = my_rank;

    /* add new record to file hash table */
    HASH_ADD(hlink, null_runtime->record_hash, record_p->f_id, sizeof(darshan_record_id), rec);
    null_runtime->rec_array_ndx++;

    return(rec);
}

/******************************************************************************
 * Functions exported by the "NULL" module for coordinating with darshan-core *
 ******************************************************************************/

/* Perform any necessary steps prior to shutting down for the "NULL" module. */
static void null_begin_shutdown()
{
    assert(null_runtime);

    NULL_LOCK();

    /* In general, we want to disable all wrappers while Darshan shuts down. 
     * This is to avoid race conditions and ensure data consistency, as
     * executing wrappers could potentially modify module state while Darshan
     * is in the process of shutting down. 
     */
    instrumentation_disabled = 1;

    /* ... any other code which needs to be executed before beginning shutdown process ... */

    NULL_UNLOCK();

    return;
}

/* Pass output data for the "NULL" module back to darshan-core to log to file. */
static void null_get_output_data(
    MPI_Comm mod_comm,
    darshan_record_id *shared_recs,
    int shared_rec_count,
    void **null_buf,
    int *null_buf_sz)
{
    assert(null_runtime);

    /* NOTE: this function can be used to run collective operations prior to
     * shutting down the module, as implied by the MPI communicator passed in
     * as the first agrument. Typically, module developers will want to run a
     * reduction on shared data records (passed in in the 'shared_recs' array),
     * but other collective routines can be run here as well. For a detailed
     * example illustrating how to run shared file reductions, consider the
     * POSIX or MPIIO instrumentation modules, as they both implement this
     * functionality.
     */

    /* Just set the output buffer to point at the array of the "NULL" module's
     * I/O records, and set the output size according to the number of records
     * currently being tracked.
     */
    *null_buf = (void *)(null_runtime->record_array);
    *null_buf_sz = null_runtime->rec_array_ndx * sizeof(struct darshan_null_record);

    return;
}

/* Shutdown the "NULL" module by freeing up all data structures. */
static void null_shutdown()
{
    assert(null_runtime);

    HASH_CLEAR(hlink, null_runtime->record_hash); /* these hash entries are freed all at once below */

    free(null_runtime->runtime_record_array);
    free(null_runtime->record_array);
    free(null_runtime);
    null_runtime = NULL;

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
