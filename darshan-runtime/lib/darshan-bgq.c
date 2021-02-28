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
#include <assert.h>

#include "uthash.h"
#include "darshan.h"
#include "darshan-dynamic.h"

#include <mpix.h>
#include <spi/include/kernel/location.h>
#include <spi/include/kernel/process.h>
#include <firmware/include/personality.h>

/*
 * Simple module which captures BG/Q hardware specific information about 
 * the job.
 * 
 * This module does not intercept any system calls. It just pulls data
 * from the personality struct at initialization.
 */


/*
 * Global runtime struct for tracking data needed at runtime
 */
struct bgq_runtime
{
    struct darshan_bgq_record *record;
};

static struct bgq_runtime *bgq_runtime = NULL;
static pthread_mutex_t bgq_runtime_mutex = PTHREAD_RECURSIVE_MUTEX_INITIALIZER_NP;

/* my_rank indicates the MPI rank of this process */
static int my_rank = -1;

/* internal helper functions for the BGQ module */
void bgq_runtime_initialize(void);

/* forward declaration for functions needed to interface with darshan-core */
static void bgq_mpi_redux(
    void *buffer,
    MPI_Comm mod_comm,
    darshan_record_id *shared_recs,
    int shared_rec_count);
static void bgq_output(
    void **buffer,
    int *size);
static void bgq_cleanup(
    void);

/* macros for obtaining/releasing the BGQ module lock */
#define BGQ_LOCK() pthread_mutex_lock(&bgq_runtime_mutex)
#define BGQ_UNLOCK() pthread_mutex_unlock(&bgq_runtime_mutex)

/*
 * Function which updates all the counter data
 */
static void capture(struct darshan_bgq_record *rec, darshan_record_id rec_id)
{
    Personality_t person;
    int r;

    rec->counters[BGQ_CSJOBID] = Kernel_GetJobID();
    rec->counters[BGQ_RANKSPERNODE] = Kernel_ProcessCount();
    rec->counters[BGQ_INODES] = MPIX_IO_node_id();

    r = Kernel_GetPersonality(&person, sizeof(person));
    if (r == 0)
    {
        rec->counters[BGQ_NNODES] = ND_TORUS_SIZE(person.Network_Config);
        rec->counters[BGQ_ANODES] = person.Network_Config.Anodes;
        rec->counters[BGQ_BNODES] = person.Network_Config.Bnodes;
        rec->counters[BGQ_CNODES] = person.Network_Config.Cnodes;
        rec->counters[BGQ_DNODES] = person.Network_Config.Dnodes;
        rec->counters[BGQ_ENODES] = person.Network_Config.Enodes;
        rec->counters[BGQ_TORUSENABLED] =
            (((person.Network_Config.NetFlags & ND_ENABLE_TORUS_DIM_A) == ND_ENABLE_TORUS_DIM_A) << 0) |
            (((person.Network_Config.NetFlags & ND_ENABLE_TORUS_DIM_B) == ND_ENABLE_TORUS_DIM_B) << 1) |
            (((person.Network_Config.NetFlags & ND_ENABLE_TORUS_DIM_C) == ND_ENABLE_TORUS_DIM_C) << 2) |
            (((person.Network_Config.NetFlags & ND_ENABLE_TORUS_DIM_D) == ND_ENABLE_TORUS_DIM_D) << 3) |
            (((person.Network_Config.NetFlags & ND_ENABLE_TORUS_DIM_E) == ND_ENABLE_TORUS_DIM_E) << 4);

        rec->counters[BGQ_DDRPERNODE] = person.DDR_Config.DDRSizeMB;
    }

    rec->base_rec.id = rec_id;
    rec->base_rec.rank = my_rank;
    rec->fcounters[BGQ_F_TIMESTAMP] = darshan_core_wtime();

    return;
}

/**********************************************************
 * Internal functions for manipulating BGQ module state *
 **********************************************************/

void bgq_runtime_initialize()
{
    size_t bgq_buf_size;
    darshan_record_id rec_id;
    darshan_module_funcs mod_funcs = {
#ifdef HAVE_MPI
        .mod_redux_func = &bgq_mpi_redux,
#endif
        .mod_output_func = &bgq_output,
        .mod_output_func = &bgq_cleanup
        };

    BGQ_LOCK();

    /* don't do anything if already initialized */
    if(bgq_runtime)
    {
        BGQ_UNLOCK();
        return;
    }

    /* we just need to store one single record */
    bgq_buf_size = sizeof(struct darshan_bgq_record);

    /* register the BG/Q module with the darshan-core component */
    darshan_core_register_module(
        DARSHAN_BGQ_MOD,
        mod_funcs,
        &bgq_buf_size,
        &my_rank,
        NULL);

    /* initialize module's global state */
    bgq_runtime = malloc(sizeof(*bgq_runtime));
    if(!bgq_runtime)
    {
        darshan_core_unregister_module(DARSHAN_BGQ_MOD);
        BGQ_UNLOCK();
        return;
    }
    memset(bgq_runtime, 0, sizeof(*bgq_runtime));

    rec_id = darshan_core_gen_record_id("darshan-bgq-record");

    /* register the bgq file record with darshan-core */
    bgq_runtime->record = darshan_core_register_record(
        rec_id,
        NULL,
        DARSHAN_BGQ_MOD,
        sizeof(struct darshan_bgq_record),
        NULL);
    if(!(bgq_runtime->record))
    {
        darshan_core_unregister_module(DARSHAN_BGQ_MOD);
        free(bgq_runtime);
        bgq_runtime = NULL;
        BGQ_UNLOCK();
        return;
    }

    capture(bgq_runtime->record, rec_id);

    BGQ_UNLOCK();

    return;
}

static int cmpr(const void *p1, const void *p2)
{
    const uint64_t *a = (uint64_t*) p1;
    const uint64_t *b = (uint64_t*) p2;
    return ((*a == *b) ?  0 : ((*a < *b) ? -1 : 1));
}

/********************************************************************************
 *      functions exported by this module for coordinating with darshan-core    *
 ********************************************************************************/

static void bgq_mpi_redux(
    void *posix_buf,
    MPI_Comm mod_comm,
    darshan_record_id *shared_recs,
    int shared_rec_count)
{
    int nprocs;
    int result;
    uint64_t *ion_ids;

    BGQ_LOCK();
    assert(bgq_runtime);

    if (my_rank == 0)
    {
        bgq_runtime->record->base_rec.rank = -1;

        PMPI_Comm_size(mod_comm, &nprocs);
        ion_ids = malloc(sizeof(*ion_ids)*nprocs);
        result = (ion_ids != NULL);
        if(!result)
            bgq_runtime->record->counters[BGQ_INODES] = -1;
    }
    PMPI_Bcast(&result, 1, MPI_INT, 0, mod_comm);

    /* caclulate the number of I/O nodes */
    if (result)
    {
        int i, found;
        uint64_t val;

        PMPI_Gather(&bgq_runtime->record->counters[BGQ_INODES],
                                      1,
                                      MPI_LONG_LONG_INT,
                                      ion_ids,
                                      1,
                                      MPI_LONG_LONG_INT,
                                      0,
                                      mod_comm);
        if (my_rank == 0)
        {
            qsort(ion_ids, nprocs, sizeof(*ion_ids), cmpr);
            for (i = 1, val = ion_ids[0], found = 1; i < nprocs; i++)
            {
                if (val != ion_ids[i])
                {
                    val = ion_ids[i];
                    found += 1;
                }
            }
            bgq_runtime->record->counters[BGQ_INODES] = found;
        }
    }

    BGQ_UNLOCK();

    return;
}

/* Pass output data for the BGQ module back to darshan-core to log to file. */
static void bgq_output(
    void **buffer,
    int *size)
{
    BGQ_LOCK();
    assert(bgq_runtime);

    /* non-zero ranks throw out their BGQ record */
    if (my_rank != 0)
    {
        *buffer = NULL;
        *size   = 0;
    }

    BGQ_UNLOCK();
    return;
}

static void bgq_cleanup()
{
    BGQ_LOCK();
    assert(bgq_runtime);

    free(bgq_runtime);
    bgq_runtime = NULL;

    BGQ_UNLOCK();
    return;
}

#if 0
static void bgq_record_reduction_op(
    void* infile_v,
    void* inoutfile_v,
    int* len,
    MPI_Datatype *datatype)
{
    int i;
    int j;
    struct darshan_bgq_record *infile = infile_v;
    struct darshan_bgq_record *inoutfile = inoutfile_v;

    for (i = 0; i<*len; i++)
    {
        for (j = 0; j < BGQ_NUM_INDICES; j++)
        {
            if (infile->counters[j] != inoutfile->counters[j])
            {
                // unexpected
                fprintf(stderr,
                        "%lu counter mismatch: %d [%lu] [%lu]\n",
                        infile->f_id,
                        j,
                        infile->counters[j],
                        inoutfile->counters[j]);
            }
        }
        infile++;
        inoutfile++;
    }

    return;
}
#endif

/*
 * Local variables:
 *  c-indent-level: 4
 *  c-basic-offset: 4
 * End:
 *
 * vim: ts=8 sts=4 sw=4 expandtab
 */
