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
#include <lustre/lustre_user.h>

#include "uthash.h"

#include "darshan.h"
#include "darshan-dynamic.h"

struct lustre_record_runtime
{
    struct darshan_lustre_record *record;
    UT_hash_handle hlink;
};

/* we just use a simple array for storing records. the POSIX module
 * only calls into the Lustre module for new records, so we will never
 * have to search for an existing Lustre record (assuming the Lustre
 * data remains immutable as it is now).
 */
struct lustre_runtime
{
    struct darshan_lustre_record *record_array;
    struct lustre_record_runtime *record_runtime_array;
    int record_array_size;
    int record_array_ndx;
    struct lustre_record_runtime *record_hash;
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
static int lustre_record_compare(const void* a_p, const void* b_p);
static void lustre_record_reduction_op(void* infile_v, void* inoutfile_v,
    int *len, MPI_Datatype *datatype);

#define LUSTRE_LOCK() pthread_mutex_lock(&lustre_runtime_mutex)
#define LUSTRE_UNLOCK() pthread_mutex_unlock(&lustre_runtime_mutex)

void darshan_instrument_lustre_file(const char* filepath, int fd)
{
    struct lustre_record_runtime *rec_rt;
    struct darshan_fs_info fs_info;
    darshan_record_id rec_id;
    int limit_flag;

    LUSTRE_LOCK();
    /* make sure the lustre module is already initialized */
    lustre_runtime_initialize();

    /* if the array is full, we just back out */
    limit_flag = (lustre_runtime->record_array_ndx >= lustre_runtime->record_array_size);

    /* register a Lustre file record with Darshan */
    fs_info.fs_type = -1;
    darshan_core_register_record(
        (void *)filepath,
        strlen(filepath),
        DARSHAN_LUSTRE_MOD,
        1,
        limit_flag,
        &rec_id,
        &fs_info);

    /* if record id is 0, darshan has no more memory for instrumenting */
    if(rec_id == 0)
    {
        LUSTRE_UNLOCK();
        return;
    }

    /* search the hash table for this file record, and initialize if not found */
    HASH_FIND(hlink, lustre_runtime->record_hash, &rec_id, sizeof(darshan_record_id), rec_rt );
    if ( !rec_rt ) {
        struct darshan_lustre_record *rec;
        struct lov_user_md *lum;
        size_t lumsize = sizeof(struct lov_user_md) +
            LOV_MAX_STRIPE_COUNT * sizeof(struct lov_user_ost_data);

        /* allocate a new lustre record and append it to the array */
        rec_rt = &(lustre_runtime->record_runtime_array[lustre_runtime->record_array_ndx]);
        rec_rt->record = &(lustre_runtime->record_array[lustre_runtime->record_array_ndx]);
        rec = rec_rt->record;
        rec->rec_id = rec_id;
        rec->rank = my_rank;

        /* implicit assumption here that none of these counters will change
         * after the first time a file is opened.  This may not always be
         * true in the future */
        if ( fs_info.fs_type != -1 ) 
        {
            rec->counters[LUSTRE_OSTS] = fs_info.ost_count;
            rec->counters[LUSTRE_MDTS] = fs_info.mdt_count;
        }
        else
        {
            rec->counters[LUSTRE_OSTS] = -1;
            rec->counters[LUSTRE_MDTS] = -1;
        }

        if ( (lum = calloc(1, lumsize)) != NULL ) {
            lum->lmm_magic = LOV_USER_MAGIC;
            /* don't care about the return code for ioctl */
            ioctl( fd, LL_IOC_LOV_GETSTRIPE, (void *)lum );
            rec->counters[LUSTRE_STRIPE_SIZE] = lum->lmm_stripe_size;
            rec->counters[LUSTRE_STRIPE_WIDTH] = lum->lmm_stripe_count;
            rec->counters[LUSTRE_STRIPE_OFFSET] = 0; /* this currently doesn't work; lum->lmm_objects[0].l_ost_idx isn't being populated */
            /* TODO: add explicit list of OSTs */
            free(lum);
        }
        else
        {
            rec->counters[LUSTRE_STRIPE_SIZE] = -1;
            rec->counters[LUSTRE_STRIPE_WIDTH] = -1;
            rec->counters[LUSTRE_STRIPE_OFFSET] = -1;
        }
        HASH_ADD(hlink, lustre_runtime->record_hash, record->rec_id, sizeof(darshan_record_id), rec_rt);
        lustre_runtime->record_array_ndx++;
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

    lustre_runtime->record_runtime_array = malloc(lustre_runtime->record_array_size *
                                          sizeof(struct lustre_record_runtime));
    if(!lustre_runtime->record_runtime_array)
    {
        lustre_runtime->record_array_size = 0;
        return;
    }
    memset(lustre_runtime->record_runtime_array, 0, lustre_runtime->record_array_size *
        sizeof(struct lustre_record_runtime));

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
    struct lustre_record_runtime *file;
    int i;
    struct darshan_lustre_record *red_send_buf = NULL;
    struct darshan_lustre_record *red_recv_buf = NULL;
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
            HASH_FIND(hlink, lustre_runtime->record_hash, &shared_recs[i],
                sizeof(darshan_record_id), file);
            assert(file);

            file->record->rank = -1;
        }

        /* sort the array of files descending by rank so that we get all of the 
         * shared files (marked by rank -1) in a contiguous portion at end 
         * of the array
         */
        qsort(lustre_runtime->record_array, lustre_runtime->record_array_ndx,
            sizeof(struct darshan_lustre_record), lustre_record_compare);

        /* make *send_buf point to the shared files at the end of sorted array */
        red_send_buf =
            &(lustre_runtime->record_array[lustre_runtime->record_array_ndx-shared_rec_count]);

        /* allocate memory for the reduction output on rank 0 */
        if(my_rank == 0)
        {
            red_recv_buf = malloc(shared_rec_count * sizeof(struct darshan_lustre_record));
            if(!red_recv_buf)
            {
                return;
            }
        }

        DARSHAN_MPI_CALL(PMPI_Type_contiguous)(sizeof(struct darshan_lustre_record),
            MPI_BYTE, &red_type);
        DARSHAN_MPI_CALL(PMPI_Type_commit)(&red_type);
        DARSHAN_MPI_CALL(PMPI_Op_create)(lustre_record_reduction_op, 1, &red_op);
        DARSHAN_MPI_CALL(PMPI_Reduce)(red_send_buf, red_recv_buf,
            shared_rec_count, red_type, red_op, 0, mod_comm);

        /* clean up reduction state */
        if(my_rank == 0)
        {
            int tmp_ndx = lustre_runtime->record_array_ndx - shared_rec_count;
            memcpy(&(lustre_runtime->record_array[tmp_ndx]), red_recv_buf,
                shared_rec_count * sizeof(struct darshan_lustre_record));
            free(red_recv_buf);
        }
        else
        {
            lustre_runtime->record_array_ndx -= shared_rec_count;
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

    HASH_CLEAR(hlink, lustre_runtime->record_hash);
    free(lustre_runtime->record_array);
    free(lustre_runtime->record_runtime_array);
    free(lustre_runtime);
    lustre_runtime = NULL;

    return;
}

/* compare function for sorting file records by descending rank */
static int lustre_record_compare(const void* a_p, const void* b_p)
{
    const struct darshan_lustre_record* a = a_p;
    const struct darshan_lustre_record* b = b_p;

    if(a->rank < b->rank)
        return 1;
    if(a->rank > b->rank)
        return -1;

    return 0;
}

/* this is just boilerplate reduction code that isn't currently used */
static void lustre_record_reduction_op(void* infile_v, void* inoutfile_v,
    int *len, MPI_Datatype *datatype)
{
    struct darshan_lustre_record tmp_record;
    struct darshan_lustre_record *infile = infile_v;
    struct darshan_lustre_record *inoutfile = inoutfile_v;
    int i, j;

    assert(lustre_runtime);

    for( i=0; i<*len; i++ )
    {
        memset(&tmp_record, 0, sizeof(struct darshan_lustre_record));
        tmp_record.rec_id = infile->rec_id;
        tmp_record.rank = -1;

        /* preserve only rank 0's value */
        for( j = LUSTRE_OSTS; j < LUSTRE_NUM_INDICES; j++)
        {
            if ( my_rank == 0 ) 
            {
                tmp_record.counters[j] = infile->counters[j];
            }
            else
            {
                tmp_record.counters[j] = inoutfile->counters[j];
            }
        }

        /* update pointers */
        *inoutfile = tmp_record;
        inoutfile++;
        infile++;
    }

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
