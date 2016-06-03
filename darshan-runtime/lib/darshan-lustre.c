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
#include "darshan-lustre.h"

struct lustre_runtime *lustre_runtime = NULL;
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
#define LUSTRE_RECORD_SIZE( osts ) ( sizeof(struct darshan_lustre_record) + sizeof(int64_t) * (osts - 1) )

void darshan_instrument_lustre_file(const char* filepath, int fd)
{
    struct lustre_record_runtime *rec_rt;
    struct darshan_lustre_record *rec;
    struct darshan_fs_info fs_info;
    darshan_record_id rec_id;
    int limit_flag;
    int i;
    struct lov_user_md *lum;
    size_t lumsize = sizeof(struct lov_user_md) +
        LOV_MAX_STRIPE_COUNT * sizeof(struct lov_user_ost_data);
    size_t rec_size;

    LUSTRE_LOCK();
    /* make sure the lustre module is already initialized */
    lustre_runtime_initialize();

    /* if we can't issue ioctl, we have no counter data at all */
    if ( (lum = calloc(1, lumsize)) == NULL )
        return;

    /* find out the OST count of this file so we can allocate memory */
    lum->lmm_magic = LOV_USER_MAGIC;
    lum->lmm_stripe_count = LOV_MAX_STRIPE_COUNT;

    /* -1 means ioctl failed, likely because file isn't on Lustre */
    if ( ioctl( fd, LL_IOC_LOV_GETSTRIPE, (void *)lum ) == -1 )
    {
        free(lum);
        return;
    }

    rec_size = LUSTRE_RECORD_SIZE( lum->lmm_stripe_count );

    {
        /* broken out for clarity */
        void *end_of_new_record = (char*)lustre_runtime->next_free_record + rec_size;
        void *end_of_rec_buffer = (char*)lustre_runtime->record_buffer + lustre_runtime->record_buffer_max;
        limit_flag = ( end_of_new_record > end_of_rec_buffer );
    }

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
        free(lum);
        LUSTRE_UNLOCK();
        return;
    }

    /* search the hash table for this file record, and initialize if not found */
    HASH_FIND(hlink, lustre_runtime->record_runtime_hash, &rec_id, sizeof(darshan_record_id), rec_rt );
    if ( !rec_rt ) {
        /* allocate a new lustre record and append it to the array */
        rec_rt = &(lustre_runtime->record_runtime_array[lustre_runtime->record_count]);
        rec_rt->record = lustre_runtime->next_free_record;
        rec_rt->record_size = rec_size;
        lustre_runtime->next_free_record = (char*)(lustre_runtime->next_free_record) + rec_size;
        lustre_runtime->record_buffer_used += rec_size;
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

        rec->counters[LUSTRE_STRIPE_SIZE] = lum->lmm_stripe_size;
        rec->counters[LUSTRE_STRIPE_WIDTH] = lum->lmm_stripe_count;
        rec->counters[LUSTRE_STRIPE_OFFSET] = lum->lmm_stripe_offset;
        for ( i = 0; i < lum->lmm_stripe_count; i++ )
            rec->ost_ids[i] = lum->lmm_objects[i].l_ost_idx;
        free(lum);

        HASH_ADD(hlink, lustre_runtime->record_runtime_hash, record->rec_id, sizeof(darshan_record_id), rec_rt);

        lustre_runtime->record_count++;
    }

    LUSTRE_UNLOCK();
    return;
}

static void lustre_runtime_initialize()
{
    int mem_limit;
    int max_records;
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

    /* allocate the full size of the memory limit we are given */
    lustre_runtime->record_buffer= malloc(mem_limit);
    if(!lustre_runtime->record_buffer)
    {
        lustre_runtime->record_buffer_max = 0;
        return;
    }
    lustre_runtime->record_buffer_max = mem_limit;
    lustre_runtime->next_free_record = lustre_runtime->record_buffer;
    memset(lustre_runtime->record_buffer, 0, lustre_runtime->record_buffer_max);

    /* Allocate array of Lustre runtime data.  We calculate the maximum possible
     * number of records that will fit into mem_limit by assuming that each
     * record has the minimum possible OST count, then allocate that many 
     * runtime records.  record_buffer will always run out of memory before
     * we overflow record_runtime_array.
     */
    max_records = mem_limit / sizeof(struct darshan_lustre_record);
    lustre_runtime->record_runtime_array =
        malloc( max_records * sizeof(struct lustre_record_runtime));
    if(!lustre_runtime->record_runtime_array)
    {
        lustre_runtime->record_buffer_max = 0;
        free( lustre_runtime->record_buffer );
        return;
    }
    memset(lustre_runtime->record_runtime_array, 0,
        max_records * sizeof(struct lustre_record_runtime));

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

/* XXX - still need to update 5/26/2016 */
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
            HASH_FIND(hlink, lustre_runtime->record_runtime_hash, &shared_recs[i],
                sizeof(darshan_record_id), file);
            assert(file);

            file->record->rank = -1;
        }

        /* sort the array of files descending by rank so that we get all of the 
         * shared files (marked by rank -1) in a contiguous portion at end 
         * of the array
         */
        sort_lustre_records();

        /* make *send_buf point to the shared files at the end of sorted array
        red_send_buf =
            &(lustre_runtime->record_runtime_array[lustre_runtime->record_count-shared_rec_count]);
*******************************************************************************/

        /* allocate memory for the reduction output on rank 0 
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
*******************************************************************************/

        /* clean up reduction state 
        if(my_rank == 0)
        {
            memcpy(&(lustre_runtime->record_array[lustre_runtime->record_count - shared_rec_count]), red_recv_buf,
                shared_rec_count * sizeof(struct darshan_lustre_record));
            free(red_recv_buf);
        }
        else
        {
            lustre_runtime->record_count -= shared_rec_count;
        }

        DARSHAN_MPI_CALL(PMPI_Type_free)(&red_type);
        DARSHAN_MPI_CALL(PMPI_Op_free)(&red_op);
*******************************************************************************/
    }

    *lustre_buf = (void *)(lustre_runtime->record_buffer);
    *lustre_buf_sz = lustre_runtime->record_buffer_used;

    return;
}

static void lustre_shutdown(void)
{
    assert(lustre_runtime);

    HASH_CLEAR(hlink, lustre_runtime->record_runtime_hash);
    free(lustre_runtime->record_runtime_array);
    free(lustre_runtime->record_buffer);
    free(lustre_runtime);
    lustre_runtime = NULL;

    return;
}

/* compare function for sorting file records by descending rank */
static int lustre_record_compare(const void* a_p, const void* b_p)
{
    const struct lustre_record_runtime* a = a_p;
    const struct lustre_record_runtime* b = b_p;

    if(a->record->rank < b->record->rank)
        return 1;
    if(a->record->rank > b->record->rank)
        return -1;

    return 0;
}

/*
 * Sort the record_runtimes and records by MPI rank to facilitate shared redux.
 * This requires craftiness and additional heap utilization because the records
 * (but not record_runtimes) have variable size.  Currently has to temporarily
 * duplicate the entire record_buffer; there is room for more memory-efficient
 * optimization if this becomes a scalability issue.
 */
int sort_lustre_records()
{
    int i;
    struct darshan_lustre_record *rec;
    struct lustre_record_runtime *rec_rt, *tmp_rec_rt;
    char  *new_buf, *p;

    /* Create a new buffer to store an entire replica of record_buffer.  Since
     * we know the exact size of record_buffer's useful data at this point, we
     * can allocate the exact amount we need instead of record_buffer_max */
    new_buf = malloc(lustre_runtime->record_buffer_used);
    p = new_buf;
    if ( !new_buf )
        return 1;

    /* qsort breaks the hash table, so delete it now to free its memory buffers
     * and prevent later confusion */
    HASH_ITER( hlink, lustre_runtime->record_runtime_hash, rec_rt, tmp_rec_rt )
        HASH_DELETE( hlink, lustre_runtime->record_runtime_hash, rec_rt );

    /* sort the runtime records, which is has fixed-length elements */
    qsort(
        lustre_runtime->record_runtime_array,
        lustre_runtime->record_count,
        sizeof(struct lustre_record_runtime),
        lustre_record_compare
    );

    /* rebuild the hash and array with the qsorted runtime records */
    for ( i = 0; i < lustre_runtime->record_count; i++ )
    {
        rec_rt = &(lustre_runtime->record_runtime_array[i]);
        HASH_ADD(hlink, lustre_runtime->record_runtime_hash, record->rec_id, sizeof(darshan_record_id), rec_rt );
    }

    /* create reordered record buffer, then copy it back in place */
    for ( i = 0; i < lustre_runtime->record_count; i++ )
    {
        rec_rt = &(lustre_runtime->record_runtime_array[i]);
        memcpy( p, rec_rt->record, rec_rt->record_size );
        /* fix record pointers within each runtime record too - pre-emptively
         * point them at where they will live in record_buffer after we memcpy
         * below */
        rec_rt->record = (struct darshan_lustre_record *)((char*)(lustre_runtime->record_buffer) + (p - new_buf));

        p += rec_rt->record_size;
    }
    memcpy( 
        lustre_runtime->record_buffer, 
        new_buf, 
        lustre_runtime->record_buffer_used );

    free(new_buf);
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
 *  Dump the memory structure of our records and runtime records
 */
void print_lustre_runtime( void )
{
    int i, j;
    struct darshan_lustre_record *rec;

    /* print what we just loaded */
    for ( i = 0; i < lustre_runtime->record_count; i++ )
    {
        rec = (lustre_runtime->record_runtime_array[i]).record;
        printf( "File %2d\n", i );
        for ( j = 0; j < LUSTRE_NUM_INDICES; j++ )
        {
            printf( "  Counter %2d: %10ld, addr %ld\n", 
                j, 
                rec->counters[j],
                (char*)(&(rec->counters[j])) - (char*)(lustre_runtime->record_buffer) );
        }
        for ( j = 0; j < rec->counters[LUSTRE_STRIPE_WIDTH]; j++ )
        {
            printf( "  Stripe  %2d: %10ld, addr %ld\n", 
                j, 
                rec->ost_ids[j],
                (char*)(&(rec->ost_ids[j])) - (char*)(lustre_runtime->record_buffer) );
        }
    }
    return;
}

/*
 *  Dump the order in which records appear in memory
 */
void print_array( void )
{
    int i;
    struct lustre_record_runtime *rec_rt;
    printf("*** DUMPING RECORD LIST BY ARRAY SEQUENCE\n");
    for ( i = 0; i < lustre_runtime->record_count; i++ )
    {
        rec_rt = &(lustre_runtime->record_runtime_array[i]);
        printf( "*** record %d rank %d osts %d\n", 
            rec_rt->record->rec_id, 
            rec_rt->record->rank,
            rec_rt->record->counters[LUSTRE_STRIPE_WIDTH]);
    }
}
void print_hash( void )
{
    struct lustre_record_runtime *rec_rt, *tmp_rec_rt;
    printf("*** DUMPING RECORD LIST BY HASH SEQUENCE\n");
    HASH_ITER( hlink, lustre_runtime->record_runtime_hash, rec_rt, tmp_rec_rt )
    {
        printf( "*** record %d rank %d osts %d\n", 
            rec_rt->record->rec_id, 
            rec_rt->record->rank,
            rec_rt->record->counters[LUSTRE_STRIPE_WIDTH]);
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
