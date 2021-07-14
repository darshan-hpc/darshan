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
#include <sys/types.h>
#include <sys/stat.h>
#include <time.h>
#include <stdlib.h>
#include <assert.h>
#include <pthread.h>
#include <limits.h>
#include <sys/xattr.h>

#include <lustre/lustreapi.h>

#include "darshan.h"
#include "darshan-dynamic.h"
#include "darshan-lustre.h"

static void lustre_runtime_initialize(
    void);
static void lustre_subtract_shared_rec_size(
    void *rec_ref_p, void *user_ptr);
static void lustre_set_rec_ref_pointers(
    void *rec_ref_p, void *user_ptr);
static int lustre_record_compare(
    const void* a_p, const void* b_p);
int sort_lustre_records(
    void);

#ifdef HAVE_MPI
static void lustre_mpi_redux(
    void *lustre_buf, MPI_Comm mod_comm,
    darshan_record_id *shared_recs, int shared_rec_count);
#endif
static void lustre_output(
    void **lustre_buf, int *lustre_buf_sz);
static void lustre_cleanup(
    void);

struct lustre_runtime *lustre_runtime = NULL;
static pthread_mutex_t lustre_runtime_mutex = PTHREAD_RECURSIVE_MUTEX_INITIALIZER_NP;
static int my_rank = -1;

#define LUSTRE_LOCK() pthread_mutex_lock(&lustre_runtime_mutex)
#define LUSTRE_UNLOCK() pthread_mutex_unlock(&lustre_runtime_mutex)

void darshan_instrument_lustre_file(const char* filepath, int fd)
{
    struct lustre_record_ref *rec_ref;
    struct darshan_lustre_record *rec;
    struct darshan_fs_info fs_info;
    darshan_record_id rec_id;
    int i;
    void *lustre_xattr_val;
    size_t lustre_xattr_size = XATTR_SIZE_MAX;
    struct llapi_layout *lustre_layout;
    uint64_t stripe_size;
    uint64_t stripe_count;
    uint64_t tmp_ost;
    size_t rec_size;
    int ret;

    LUSTRE_LOCK();

    /* try to init module if not already */
    if(!lustre_runtime) lustre_runtime_initialize();

    /* if we aren't initialized, just back out */
    if(!lustre_runtime)
    {
        LUSTRE_UNLOCK();
        return;
    }

    /* search the hash table for this file record, and initialize if not found */
    rec_id = darshan_core_gen_record_id(filepath);
    rec_ref = darshan_lookup_record_ref(lustre_runtime->record_id_hash,
        &rec_id, sizeof(darshan_record_id));
    if(!rec_ref)
    {
        if ( (lustre_xattr_val = calloc(1, lustre_xattr_size)) == NULL )
        {
            LUSTRE_UNLOCK();
            return;
        }

        /* -1 means fgetxattr failed, likely because file isn't on Lustre, but maybe because
         * the Lustre version doesn't support this method of obtaining striping info
         */
        if ( (lustre_xattr_size = fgetxattr( fd, "lustre.lov", lustre_xattr_val, lustre_xattr_size)) == -1 )
        {
            free(lustre_xattr_val);
            LUSTRE_UNLOCK();
            return;
        }

        /* get corresponding Lustre file layout, then extract stripe params */
        if ( (lustre_layout = llapi_layout_get_by_xattr(lustre_xattr_val, lustre_xattr_size, 0)) == NULL)
        {
            free(lustre_xattr_val);
            LUSTRE_UNLOCK();
            return;
        }
        if (llapi_layout_stripe_size_get(lustre_layout, &stripe_size) == -1)
        {
            llapi_layout_free(lustre_layout);
            free(lustre_xattr_val);
            LUSTRE_UNLOCK();
            return;
        }
        if (llapi_layout_stripe_count_get(lustre_layout, &stripe_count) == -1)
        {
            llapi_layout_free(lustre_layout);
            free(lustre_xattr_val);
            LUSTRE_UNLOCK();
            return;
        }

        /* allocate and add a new record reference */
        rec_ref = malloc(sizeof(*rec_ref));
        if(!rec_ref)
        {
            llapi_layout_free(lustre_layout);
            free(lustre_xattr_val);
            LUSTRE_UNLOCK();
            return;
        }
    
        ret = darshan_add_record_ref(&(lustre_runtime->record_id_hash),
            &rec_id, sizeof(darshan_record_id), rec_ref);
        if(ret == 0)
        {
            free(rec_ref);
            llapi_layout_free(lustre_layout);
            free(lustre_xattr_val);
            LUSTRE_UNLOCK();
            return;
        }

        rec_size = LUSTRE_RECORD_SIZE( stripe_count );

        /* register a Lustre file record with Darshan */
        fs_info.fs_type = -1;
        rec = darshan_core_register_record(
                rec_id,
                filepath,
                DARSHAN_LUSTRE_MOD,
                rec_size,
                &fs_info);

        /* if NULL, darshan has no more memory for instrumenting */
        if(rec == NULL)
        {
            darshan_delete_record_ref(&(lustre_runtime->record_id_hash),
                &rec_id, sizeof(darshan_record_id));
            free(rec_ref);
            llapi_layout_free(lustre_layout);
            free(lustre_xattr_val);
            LUSTRE_UNLOCK();
            return;
        }

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

        rec->counters[LUSTRE_STRIPE_SIZE] = stripe_size;
        rec->counters[LUSTRE_STRIPE_WIDTH] = stripe_count;
        rec->counters[LUSTRE_STRIPE_OFFSET] = -1; // no longer captured
        for ( i = 0; i < stripe_count; i++ )
        {
            if (llapi_layout_ost_index_get(lustre_layout, i, &tmp_ost) == -1)
            {
                darshan_delete_record_ref(&(lustre_runtime->record_id_hash),
                    &rec_id, sizeof(darshan_record_id));
                free(rec_ref);
                llapi_layout_free(lustre_layout);
                free(lustre_xattr_val);
                LUSTRE_UNLOCK();
                return;
            }
            rec->ost_ids[i] = (int64_t)tmp_ost;
        }
        free(lustre_xattr_val);
        llapi_layout_free(lustre_layout);

        rec->base_rec.id = rec_id;
        rec->base_rec.rank = my_rank;
        rec_ref->record = rec;
        rec_ref->record_size = rec_size;
        lustre_runtime->record_count++;
    }

    LUSTRE_UNLOCK();
    return;
}

static void lustre_runtime_initialize()
{
    size_t lustre_buf_size;
    darshan_module_funcs mod_funcs = {
#ifdef HAVE_MPI
        .mod_redux_func = &lustre_mpi_redux,
#endif
        .mod_output_func = &lustre_output,
        .mod_cleanup_func = &lustre_cleanup
        };


    /* try and store a default number of records for this module, assuming
     * each file uses 64 OSTs
     */
    lustre_buf_size = DARSHAN_DEF_MOD_REC_COUNT * LUSTRE_RECORD_SIZE(64);

    /* register the lustre module with darshan-core */
    darshan_core_register_module(
        DARSHAN_LUSTRE_MOD,
        mod_funcs,
        &lustre_buf_size,
        &my_rank,
        NULL);

    lustre_runtime = malloc(sizeof(*lustre_runtime));
    if(!lustre_runtime)
    {
        darshan_core_unregister_module(DARSHAN_LUSTRE_MOD);
        return;
    }
    memset(lustre_runtime, 0, sizeof(*lustre_runtime));

    return;
}

/**************************************************************************
 * Functions exported by Lustre module for coordinating with darshan-core *
 **************************************************************************/

#ifdef HAVE_MPI
static void lustre_mpi_redux(
    void *posix_buf,
    MPI_Comm mod_comm,
    darshan_record_id *shared_recs,
    int shared_rec_count)
{
    struct lustre_record_ref *rec_ref;
    int i;

    LUSTRE_LOCK();
    assert(lustre_runtime);

    /* if there are globally shared files, do a shared file reduction */
    if (shared_rec_count)
    {
        /* necessary initialization of shared records */
        for(i = 0; i < shared_rec_count; i++)
        {
            rec_ref = darshan_lookup_record_ref(lustre_runtime->record_id_hash,
                &shared_recs[i], sizeof(darshan_record_id));
            /* As in other modules, it should not be possible to lose a
             * record after we have already performed a collective to
             * identify that it is shared with other ranks.  We print an
             * error msg and continue rather than asserting in this case,
             * though, see #243.
             */
            if(rec_ref)
                rec_ref->record->base_rec.rank = -1;
            else
                darshan_core_fprintf(stderr, "WARNING: unexpected condition in Darshan, possibly triggered by memory corruption.  Darshan log may be incorrect.\n");
        }
    }

    LUSTRE_UNLOCK();
    return;
}
#endif

static void lustre_output(
    void **lustre_buf,
    int *lustre_buf_sz)
{
    LUSTRE_LOCK();
    assert(lustre_runtime);

    lustre_runtime->record_buffer = *lustre_buf;
    lustre_runtime->record_buffer_size = *lustre_buf_sz;

    /* sort the array of files descending by rank so that we get all of the 
     * shared files (marked by rank -1) in a contiguous portion at end 
     * of the array
     */
    sort_lustre_records();

    /* simply drop all shared records from the end of the record array on
     * non-root ranks simply by recalculating the size of the buffer
     */
    if (my_rank != 0)
    {
        darshan_iter_record_refs(lustre_runtime->record_id_hash, 
            &lustre_subtract_shared_rec_size, NULL);
    }

    /* modify output buffer size to account for any shared records that were removed */
    *lustre_buf_sz = lustre_runtime->record_buffer_size;

    LUSTRE_UNLOCK();
    return;
}

static void lustre_cleanup()
{
    LUSTRE_LOCK();
    assert(lustre_runtime);

    /* cleanup data structures */
    darshan_clear_record_refs(&(lustre_runtime->record_id_hash), 1);
    free(lustre_runtime);
    lustre_runtime = NULL;

    LUSTRE_UNLOCK();
    return;
}

static void lustre_subtract_shared_rec_size(void *rec_ref_p, void *user_ptr)
{
    struct lustre_record_ref *l_rec_ref = (struct lustre_record_ref *)rec_ref_p;

    if(l_rec_ref->record->base_rec.rank == -1)
        lustre_runtime->record_buffer_size -=
            LUSTRE_RECORD_SIZE( l_rec_ref->record->counters[LUSTRE_STRIPE_WIDTH] );
}

static void lustre_set_rec_ref_pointers(void *rec_ref_p, void *user_ptr)
{
    lustre_runtime->record_ref_array[lustre_runtime->record_ref_array_ndx] = rec_ref_p;
    lustre_runtime->record_ref_array_ndx++;
    return;
}

/* compare function for sorting file records by descending rank */
static int lustre_record_compare(const void* a_p, const void* b_p)
{
    const struct lustre_record_ref* a = *((struct lustre_record_ref **)a_p);
    const struct lustre_record_ref* b = *((struct lustre_record_ref **)b_p);

    if (a->record->base_rec.rank < b->record->base_rec.rank)
        return 1;
    if (a->record->base_rec.rank > b->record->base_rec.rank)
        return -1;

    /* if ( a->record->rank == b->record->rank ) we MUST do a secondary
     * sort so that the order of qsort is fully deterministic and consistent
     * across all MPI ranks.  Without a secondary sort, the sort order can
     * be affected by rank-specific variations (e.g., the order in which
     * files are first opened).
     */
    /* sort by ascending darshan record ids */
    if (a->record->base_rec.id > b->record->base_rec.id)
        return 1;
    if (a->record->base_rec.id < b->record->base_rec.id)
        return -1;
    
    return 0;
}

/*
 * Sort the record_references and records by MPI rank to facilitate shared redux.
 * This requires craftiness and additional heap utilization because the records
 * (but not record_runtimes) have variable size.  Currently has to temporarily
 * duplicate the entire record_buffer; there is room for more memory-efficient
 * optimization if this becomes a scalability issue.
 */
int sort_lustre_records()
{
    int i;
    struct lustre_record_ref *rec_ref;
    char  *new_buf, *p;

    /* Create a new buffer to store an entire replica of record_buffer.  Since
     * we know the exact size of record_buffer's useful data at this point, we
     * can allocate the exact amount we need */
    new_buf = malloc(lustre_runtime->record_buffer_size);
    p = new_buf;
    if ( !new_buf )
        return 1;

    /* allocate array of record reference pointers that we want to sort */
    lustre_runtime->record_ref_array = malloc(lustre_runtime->record_count *
        sizeof(*(lustre_runtime->record_ref_array)));
    if( !lustre_runtime->record_ref_array )
    {
        free(new_buf);
        return 1;
    }

    /* build the array of record reference pointers we want to sort */
    darshan_iter_record_refs(lustre_runtime->record_id_hash,
        &lustre_set_rec_ref_pointers, NULL);

    /* qsort breaks the hash table, so delete it now to free its memory buffers
     * and prevent later confusion */
    darshan_clear_record_refs(&(lustre_runtime->record_id_hash), 0);

    /* sort the runtime records, which is has fixed-length elements */
    qsort(
        lustre_runtime->record_ref_array,
        lustre_runtime->record_count,
        sizeof(struct lustre_record_ref *),
        lustre_record_compare
    );

    /* rebuild the hash with the qsorted runtime records, and
     * create reordered record buffer
     */
    for ( i = 0; i < lustre_runtime->record_count; i++ )
    {
        rec_ref = lustre_runtime->record_ref_array[i];

        /* add this record reference back to the hash table */
        darshan_add_record_ref(&(lustre_runtime->record_id_hash),
            &(rec_ref->record->base_rec.id), sizeof(darshan_record_id), rec_ref);

        memcpy( p, rec_ref->record, rec_ref->record_size );
        /* fix record pointers within each record reference too - pre-emptively
         * point them at where they will live in record_buffer after we memcpy
         * below */
        rec_ref->record = (struct darshan_lustre_record *)
            ((char*)(lustre_runtime->record_buffer) + (p - new_buf));
        p += rec_ref->record_size;
    }

    /* copy sorted records back over to Lustre's record buffer */
    memcpy( 
        lustre_runtime->record_buffer, 
        new_buf, 
        lustre_runtime->record_buffer_size );

    free(new_buf);
    free(lustre_runtime->record_ref_array);
    return 0;
}

#if 0
static void lustre_record_reduction_op(
    void* infile_v, void* inoutfile_v, int *len, MPI_Datatype *datatype);

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
        tmp_record.base_rec.id = infile->base_rec.id;
        tmp_record.base_rec.rank = -1;

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
            printf( "  Counter %-2d: %10ld, addr %ld\n", 
                j, 
                rec->counters[j],
                (char*)(&(rec->counters[j])) - (char*)(lustre_runtime->record_buffer) );
        }
        for ( j = 0; j < rec->counters[LUSTRE_STRIPE_WIDTH]; j++ )
        {
            if ( j > 0 && j % 2 == 0 ) printf("\n");
            printf( "  Stripe  %-2d: %10ld, addr %-9d", 
                j, 
                rec->ost_ids[j],
                (char*)(&(rec->ost_ids[j])) - (char*)(lustre_runtime->record_buffer) );
        }
        printf( "\n" );
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
    HASH_ITER( hlink, lustre_runtime->record_runtim_hash, rec_rt, tmp_rec_rt )
    {
        printf( "*** record %d rank %d osts %d\n", 
            rec_rt->record->rec_id, 
            rec_rt->record->rank,
            rec_rt->record->counters[LUSTRE_STRIPE_WIDTH]);
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
