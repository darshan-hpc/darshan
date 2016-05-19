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
#include <sys/ioctl.h>

/* XXX stick this into autoconf .h */
#include <lustre/lustreapi.h>

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

void darshan_instrument_lustre_file(const char* filepath, int fd)
{
    struct darshan_lustre_record *rec;
    struct darshan_fs_info fs_info;
    darshan_record_id rec_id;
    struct lov_user_md *lum;
    size_t lumsize = sizeof(struct lov_user_md) +
        LOV_MAX_STRIPE_COUNT * sizeof(struct lov_user_ost_data);

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
        &fs_info);

    /* if record id is 0, darshan has no more memory for instrumenting */
    if(rec_id == 0)
        return;

    /* allocate a new lustre record and append it to the array */
    rec = &(lustre_runtime->record_array[lustre_runtime->record_array_ndx++]);
    rec->rec_id = rec_id;
    rec->rank = my_rank;

    /* TODO: gather lustre data, store in record hash */
    /* counters in lustre_ref->record->counters */
    rec->counters[LUSTRE_OSTS] = fs_info.ost_count;
    rec->counters[LUSTRE_MDTS] = fs_info.mdt_count;

    /* we must map darshan_lustre_record (or darshan_posix_file, or filename) to an fd */
    rec->counters[LUSTRE_STRIPE_SIZE] = -1;
    rec->counters[LUSTRE_STRIPE_WIDTH] = -1;
    rec->counters[LUSTRE_STRIPE_OFFSET] = -1;

    if ( (lum = calloc(1, lumsize)) != NULL ) {
        lum->lmm_magic = LOV_USER_MAGIC;
        /* don't care about the return code for ioctl */
        ioctl( fd, LL_IOC_LOV_GETSTRIPE, (void *)lum );
        rec->counters[LUSTRE_STRIPE_SIZE] = lum->lmm_stripe_size;
        rec->counters[LUSTRE_STRIPE_WIDTH] = lum->lmm_stripe_count;
        rec->counters[LUSTRE_STRIPE_OFFSET] = lum->lmm_stripe_offset;
        /* todo: add explicit list of OSTs */
        free(lum);
    }

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
    struct hdf5_file_runtime *file;
    int i;
    struct darshan_hdf5_file *red_send_buf = NULL;
    struct darshan_hdf5_file *red_recv_buf = NULL;
    MPI_Datatype red_type;
    MPI_Op red_op;

    assert(lustre_runtime);

    /* if there are globally shared files, do a shared file reduction */
    /* NOTE: the shared file reduction is also skipped if the 
     * DARSHAN_DISABLE_SHARED_REDUCTION environment variable is set.
     */
    if(shared_rec_count && !getenv("DARSHAN_DISABLE_SHARED_REDUCTION"))
    {
        /* necessary initialization of shared records */
        for(i = 0; i < shared_rec_count; i++)
        {
            HASH_FIND(hlink, lustre_runtime->file_hash, &shared_recs[i],
                sizeof(darshan_record_id), file);
            assert(file);

            file->file_record->rank = -1;
        }

/*******************************************************************************
 * resume editing here!
 *
 * TODO: determine lustre record shared across all processes,
 * and have only rank 0 write these records out. No shared 
 * reductions should be necessary as the Lustre data for a
 * given file should be the same on each process
 ******************************************************************************/

        /* sort the array of files descending by rank so that we get all of the 
         * shared files (marked by rank -1) in a contiguous portion at end 
         * of the array
         */
        qsort(hdf5_runtime->file_record_array, hdf5_runtime->file_array_ndx,
            sizeof(struct darshan_hdf5_file), hdf5_record_compare);

        /* make *send_buf point to the shared files at the end of sorted array */
        red_send_buf =
            &(hdf5_runtime->file_record_array[hdf5_runtime->file_array_ndx-shared_rec_count]);

        /* allocate memory for the reduction output on rank 0 */
        if(my_rank == 0)
        {
            red_recv_buf = malloc(shared_rec_count * sizeof(struct darshan_hdf5_file));
            if(!red_recv_buf)
            {
                return;
            }
        }

        /* construct a datatype for a HDF5 file record.  This is serving no purpose
         * except to make sure we can do a reduction on proper boundaries
         */
        DARSHAN_MPI_CALL(PMPI_Type_contiguous)(sizeof(struct darshan_hdf5_file),
            MPI_BYTE, &red_type);
        DARSHAN_MPI_CALL(PMPI_Type_commit)(&red_type);

        /* register a HDF5 file record reduction operator */
        DARSHAN_MPI_CALL(PMPI_Op_create)(hdf5_record_reduction_op, 1, &red_op);

        /* reduce shared HDF5 file records */
        DARSHAN_MPI_CALL(PMPI_Reduce)(red_send_buf, red_recv_buf,
            shared_rec_count, red_type, red_op, 0, mod_comm);

        /* clean up reduction state */
        if(my_rank == 0)
        {
            int tmp_ndx = hdf5_runtime->file_array_ndx - shared_rec_count;
            memcpy(&(hdf5_runtime->file_record_array[tmp_ndx]), red_recv_buf,
                shared_rec_count * sizeof(struct darshan_hdf5_file));
            free(red_recv_buf);
        }
        else
        {
            hdf5_runtime->file_array_ndx -= shared_rec_count;
        }

        DARSHAN_MPI_CALL(PMPI_Type_free)(&red_type);
        DARSHAN_MPI_CALL(PMPI_Op_free)(&red_op);
    }

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
