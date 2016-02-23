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

/* Lustre record data is currently immutable, so once it's set it should be
 * final. Since we won't likely need to search for records after they've been
 * created, a simple array is fine for storing them instead of a hash table
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

void darshan_instrument_lustre_file(char *filepath)
{
    /* make sure the lustre module is already initialized */
    lustre_runtime_initialize();

    /* TODO: implement gathering of lustre data */

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

    return;
}

static void lustre_shutdown(void)
{
    assert(lustre_runtime);

    /* TODO: free data structures */

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
