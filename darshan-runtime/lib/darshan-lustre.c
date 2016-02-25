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
#include <time.h>
#include <stdlib.h>
#include <assert.h>
#include <pthread.h>

#include "uthash.h"

#include "darshan.h"
#include "darshan-dynamic.h"

/* we just use a simple array for storing records. the POSIX module
 * only calls into the Lustre module for new records, so we will never
 * have to search for an existing Lustre record (assuming the Lustre
 * data remains immutable as it is now).
 */
struct lustre_runtime
{
    struct darshan_lustre_record *record_array;
    int record_array_size;
    int record_array_ndx;
};

static struct lustre_runtime *lustre_runtime = NULL;
static pthread_mutex_t lustre_runtime_mutex = PTHREAD_RECURSIVE_MUTEX_INITIALIZER_NP;
static int instrumentation_disabled = 0;
static int my_rank = -1;

static void lustre_runtime_initialize(void);

static void lustre_begin_shutdown(void);
static void lustre_get_output_data(MPI_Comm mod_comm, darshan_record_id *shared_recs,
    int shared_rec_count, void **lustre_buf, int *lustre_buf_sz);
static void lustre_shutdown(void);

#define LUSTRE_LOCK() pthread_mutex_lock(&lustre_runtime_mutex)
#define LUSTRE_UNLOCK() pthread_mutex_unlock(&lustre_runtime_mutex)

/* TODO: is there any way we can further compact Lustre data to save space?
 * e.g., are all files in the same directory guaranteed same striping parameters?
 * if so, can we store stripe parameters on per-directory basis and the OST
 * list on a per-file basis? maybe the storage savings are small enough this isn't
 * worth it, but nice to keep in mind
 */

void darshan_instrument_lustre_file(char *filepath)
{
    struct darshan_lustre_record *rec;
    darshan_record_id rec_id;

    LUSTRE_LOCK();
    /* make sure the lustre module is already initialized */
    lustre_runtime_initialize();

    /* if the array is full, we just back out */
    if(lustre_runtime->record_array_ndx >= lustre_runtime->record_array_size)
        return;

    /* register a Lustre file record with Darshan */
    darshan_core_register_record(
        (void *)filepath,
        strlen(filepath),
        DARSHAN_LUSTRE_MOD,
        1,
        0,
        &rec_id,
        NULL);

    /* if record id is 0, darshan has no more memory for instrumenting */
    if(rec_id == 0)
        return;

    /* allocate a new lustre record and append it to the array */
    rec = &(lustre_runtime->record_array[lustre_runtime->record_array_ndx++]);
    rec->rec_id = rec_id;
    rec->rank = my_rank;

    /* TODO: gather lustre data, store in record hash */
    /* counters in lustre_ref->record->counters */
    rec->counters[LUSTRE_TEST_COUNTER] = 88;

    LUSTRE_UNLOCK();
    return;
}

static void lustre_runtime_initialize()
{
    int mem_limit;
    struct darshan_module_funcs lustre_mod_fns =
    {
        .begin_shutdown = &lustre_begin_shutdown,
        .get_output_data = &lustre_get_output_data,
        .shutdown = &lustre_shutdown
    };

    /* don't do anything if already initialized or instrumenation is disabled */
    if(lustre_runtime || instrumentation_disabled)
        return;

    /* register the lustre module with darshan-core */
    darshan_core_register_module(
        DARSHAN_LUSTRE_MOD,
        &lustre_mod_fns,
        &my_rank,
        &mem_limit,
        NULL);

    /* return if no memory assigned by darshan core */
    if(mem_limit == 0)
        return;

    lustre_runtime = malloc(sizeof(*lustre_runtime));
    if(!lustre_runtime)
        return;
    memset(lustre_runtime, 0, sizeof(*lustre_runtime));

    /* allocate array of Lustre records according to the amount of memory
     * assigned by Darshan
     */
    lustre_runtime->record_array_size = mem_limit / sizeof(struct darshan_lustre_record);

    lustre_runtime->record_array = malloc(lustre_runtime->record_array_size *
                                          sizeof(struct darshan_lustre_record));
    if(!lustre_runtime->record_array)
    {
        lustre_runtime->record_array_size = 0;
        return;
    }
    memset(lustre_runtime->record_array, 0, lustre_runtime->record_array_size *
        sizeof(struct darshan_lustre_record));

    return;
}

/**************************************************************************
 * Functions exported by Lustre module for coordinating with darshan-core *
 **************************************************************************/

static void lustre_begin_shutdown(void)
{
    assert(lustre_runtime);

    LUSTRE_LOCK();
    /* disable further instrumentation while Darshan shuts down */
    instrumentation_disabled = 1;
    LUSTRE_UNLOCK();

    return;
}

static void lustre_get_output_data(
    MPI_Comm mod_comm,
    darshan_record_id *shared_recs,
    int shared_rec_count,
    void **lustre_buf,
    int *lustre_buf_sz)
{
    assert(lustre_runtime);

    /* TODO: determine lustre record shared across all processes,
     * and have only rank 0 write these records out. No shared 
     * reductions should be necessary as the Lustre data for a
     * given file should be the same on each process
     */

    *lustre_buf = (void *)(lustre_runtime->record_array);
    *lustre_buf_sz = lustre_runtime->record_array_ndx * sizeof(struct darshan_lustre_record);

    return;
}

static void lustre_shutdown(void)
{
    assert(lustre_runtime);

    /* TODO: free data structures */
    free(lustre_runtime->record_array);
    free(lustre_runtime);
    lustre_runtime = NULL;

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
