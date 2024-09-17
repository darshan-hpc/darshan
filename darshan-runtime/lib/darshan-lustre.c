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

static void lustre_runtime_initialize(
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

struct lustre_record_ref
{
    struct darshan_lustre_record *record;
};

struct lustre_runtime
{
    void *record_id_hash;
    int frozen; /* flag to indicate that the counters should no longer be modified */
};

struct lustre_runtime *lustre_runtime = NULL;
static pthread_mutex_t lustre_runtime_mutex = PTHREAD_RECURSIVE_MUTEX_INITIALIZER_NP;
static int lustre_runtime_init_attempted = 0;
static int my_rank = -1;

#define LUSTRE_LOCK() pthread_mutex_lock(&lustre_runtime_mutex)
#define LUSTRE_UNLOCK() pthread_mutex_unlock(&lustre_runtime_mutex)

static void darshan_get_lustre_layout_size(struct llapi_layout *lustre_layout,
    int *num_comps, int *num_stripes)
{
    bool is_composite;
    int ret;
    uint64_t stripe_pattern, stripe_count;
    int tmp_comps = 0;
    int tmp_stripes = 0;

    *num_comps = 0;
    *num_stripes = 0;

    is_composite = llapi_layout_is_composite(lustre_layout);
    if (is_composite)
    {
        /* iterate starting with the first omponent */
        ret = llapi_layout_comp_use(lustre_layout, LLAPI_LAYOUT_COMP_USE_FIRST);
        if (ret != 0)
            return;
    }

    do {
        /* grab important parameters regarding this stripe component */
        ret = llapi_layout_pattern_get(lustre_layout, &stripe_pattern);
        if (ret != 0)
            return;
        if (!(stripe_pattern & LLAPI_LAYOUT_MDT))
        {
            ret = llapi_layout_stripe_count_get(lustre_layout, &stripe_count);
            if (ret != 0)
                return;
            tmp_stripes += stripe_count;
        }
        tmp_comps++;

        if (is_composite)
        {
            /* move on to the next component in the composite layout */
            ret = llapi_layout_comp_use(lustre_layout, LLAPI_LAYOUT_COMP_USE_NEXT);
        }
        else break;
    } while(ret == 0);

    *num_comps = tmp_comps;
    *num_stripes = tmp_stripes;
    return;
}

static void darshan_get_lustre_layout_components(struct llapi_layout *lustre_layout,
    struct lustre_record_ref *rec_ref)
{
    bool is_composite;
    int ret;
    uint64_t stripe_size;
    uint64_t stripe_count;
    uint64_t stripe_pattern;
    uint32_t flags;
    uint64_t ext_start, ext_end;
    uint32_t mirror_id;
    uint64_t i, tmp_ost;
    int comps_idx = 0, osts_idx = 0;
    struct darshan_lustre_component *comps =
        (struct darshan_lustre_component *)&(rec_ref->record->comps);
    OST_ID *osts = (OST_ID *)(comps + rec_ref->record->num_comps);

    is_composite = llapi_layout_is_composite(lustre_layout);
    if (is_composite)
    {
        /* iterate starting with the first omponent */
        ret = llapi_layout_comp_use(lustre_layout, LLAPI_LAYOUT_COMP_USE_FIRST);
        if (ret != 0)
            return;
    }

    do {
        /* grab important parameters regarding this stripe component */
        ret = llapi_layout_stripe_size_get(lustre_layout, &stripe_size);
        ret += llapi_layout_stripe_count_get(lustre_layout, &stripe_count);
        ret += llapi_layout_pattern_get(lustre_layout, &stripe_pattern);
        if (stripe_pattern & LLAPI_LAYOUT_MDT)
            stripe_count = 0;
        ret += llapi_layout_comp_flags_get(lustre_layout, &flags);
        ret += llapi_layout_comp_extent_get(lustre_layout, &ext_start, &ext_end);
        ret += llapi_layout_mirror_id_get(lustre_layout, &mirror_id);
        /* record info on this component iff:
         *  - the layout isn't composite _OR_ the composite layout component is
         *    initialized (actively used for this file)
         *  - the above functions querying stripe params returned no error
         *  - there is enough room in the record buf to store the OST list
         */
        if ((!is_composite || (flags & LCME_FL_INIT)) &&
            (ret == 0) &&
            (osts_idx + stripe_count <= rec_ref->record->num_stripes))
        {
            comps[comps_idx].counters[LUSTRE_COMP_STRIPE_SIZE] = (int64_t)stripe_size;
            comps[comps_idx].counters[LUSTRE_COMP_STRIPE_COUNT] = (int64_t)stripe_count;
            comps[comps_idx].counters[LUSTRE_COMP_STRIPE_PATTERN] = (int64_t)stripe_pattern;
            comps[comps_idx].counters[LUSTRE_COMP_FLAGS] = (int64_t)flags;
            comps[comps_idx].counters[LUSTRE_COMP_EXT_START] = (int64_t)ext_start;
            comps[comps_idx].counters[LUSTRE_COMP_EXT_END] = (int64_t)ext_end;
            comps[comps_idx].counters[LUSTRE_COMP_MIRROR_ID] = (int64_t)mirror_id;
            /* also get the pool name associated with the component */
            llapi_layout_pool_name_get(lustre_layout, comps[comps_idx].pool_name,
                sizeof(comps[comps_idx].pool_name)-1);

            /* get the list of OSTs allocated for this component */
            for(i = 0; i < stripe_count; i++, osts_idx++)
            {
                if (llapi_layout_ost_index_get(lustre_layout, i, &tmp_ost) == -1)
                    osts[osts_idx] = -1;
                else
                    osts[osts_idx] = (OST_ID)tmp_ost;
            }
            comps_idx++;
        }

        if (is_composite)
        {
            /* move on to the next component in the composite layout */
            ret = llapi_layout_comp_use(lustre_layout, LLAPI_LAYOUT_COMP_USE_NEXT);
        }
        else break;
    } while(ret == 0 && comps_idx < rec_ref->record->num_comps);

    /* no more components to gather info on, set the rest as invalid */
    /* NOTE: we will attempt to truncate unused components at shutdown time */
    for (; comps_idx < rec_ref->record->num_comps; comps_idx++)
    {
        comps[comps_idx].counters[LUSTRE_COMP_STRIPE_SIZE] = -1;
    }

    return;
}

void darshan_instrument_lustre_file(darshan_record_id rec_id, int fd)
{
    void *lustre_xattr_val;
    size_t lustre_xattr_size = XATTR_SIZE_MAX;
    struct llapi_layout *lustre_layout;
    int num_comps, num_stripes;
    size_t rec_size;
    struct darshan_lustre_record *rec;
    struct lustre_record_ref *rec_ref;
    int ret;

    LUSTRE_LOCK();

    /* try to init module if not already */
    if(!lustre_runtime && !lustre_runtime_init_attempted)
        lustre_runtime_initialize();

    /* if we aren't initialized, just back out */
    if(!lustre_runtime || lustre_runtime->frozen)
    {
        LUSTRE_UNLOCK();
        return;
    }

    if ((lustre_xattr_val = calloc(1, lustre_xattr_size)) == NULL)
    {
        LUSTRE_UNLOCK();
        return;
    }

    /* -1 means fgetxattr failed, likely because file isn't on Lustre, but maybe because
     * the Lustre version doesn't support this method of obtaining striping info
     */
    if ((lustre_xattr_size = fgetxattr(fd, "lustre.lov", lustre_xattr_val, lustre_xattr_size)) == -1)
    {
        free(lustre_xattr_val);
        LUSTRE_UNLOCK();
        return;
    }

    /* get corresponding Lustre file layout, then extract stripe params */
    if ((lustre_layout = llapi_layout_get_by_xattr(lustre_xattr_val, lustre_xattr_size, 0)) == NULL)
    {
        free(lustre_xattr_val);
        LUSTRE_UNLOCK();
        return;
    }
    free(lustre_xattr_val);

    /* search the hash table for this file record, and initialize if not found */
    rec_ref = darshan_lookup_record_ref(lustre_runtime->record_id_hash,
        &rec_id, sizeof(darshan_record_id));
    if(!rec_ref)
    {

        /* iterate file layout components to determine total record size */
        darshan_get_lustre_layout_size(lustre_layout, &num_comps, &num_stripes);
        if(num_comps == 0)
        {
            llapi_layout_free(lustre_layout);
            LUSTRE_UNLOCK();
            return;
        }
        rec_size = LUSTRE_RECORD_SIZE(num_comps, num_stripes);

        /* allocate and add a new record reference */
        rec_ref = malloc(sizeof(*rec_ref));
        if(!rec_ref)
        {
            llapi_layout_free(lustre_layout);
            LUSTRE_UNLOCK();
            return;
        }
    
        ret = darshan_add_record_ref(&(lustre_runtime->record_id_hash),
            &rec_id, sizeof(darshan_record_id), rec_ref);
        if(ret == 0)
        {
            free(rec_ref);
            llapi_layout_free(lustre_layout);
            LUSTRE_UNLOCK();
            return;
        }

        /* register a Lustre file record with Darshan */
        rec = darshan_core_register_record(
                rec_id,
                NULL, /* either POSIX or STDIO already registered the name */
                DARSHAN_LUSTRE_MOD,
                rec_size,
                NULL);
        if(rec == NULL)
        {
            /* if NULL, darshan has no more memory for instrumenting */
            darshan_delete_record_ref(&(lustre_runtime->record_id_hash),
                &rec_id, sizeof(darshan_record_id));
            free(rec_ref);
            llapi_layout_free(lustre_layout);
            LUSTRE_UNLOCK();
            return;
        }

        /* set base record */
        rec->base_rec.id = rec_id;
        rec->base_rec.rank = my_rank;
        rec_ref->record = rec;
        rec_ref->record->num_comps = num_comps;
        rec_ref->record->num_stripes = num_stripes;
    }

    /* fill in record buffer with component info and OST list */
    darshan_get_lustre_layout_components(lustre_layout, rec_ref);
    llapi_layout_free(lustre_layout);

    LUSTRE_UNLOCK();
    return;
}

static void lustre_runtime_initialize()
{
    int ret;
    size_t lustre_rec_count;
    darshan_module_funcs mod_funcs = {
#ifdef HAVE_MPI
        .mod_redux_func = &lustre_mpi_redux,
#endif
        .mod_output_func = &lustre_output,
        .mod_cleanup_func = &lustre_cleanup
        };

    /* if this attempt at initializing fails, we won't try again */
    lustre_runtime_init_attempted = 1;

    /* try and store a default number of records for this module */
    lustre_rec_count = DARSHAN_DEF_MOD_REC_COUNT;

    /* register the lustre module with darshan-core */
    /* NOTE: For simplicity, we assume each Lustre file record is just 1KiB
     *       when requesting memory here. These record sizes are variable
     *       length (depending on the number of file layout components and
     *       the number of OSTs used by each component), so we can't really
     *       preallocate memory for a fixed number of records at init time
     *       like we traditionally do in other modules.
     */
    #define DEF_LUSTRE_RECORD_SIZE 1024
    ret = darshan_core_register_module(
        DARSHAN_LUSTRE_MOD,
        mod_funcs,
        DEF_LUSTRE_RECORD_SIZE,
        &lustre_rec_count,
        &my_rank,
        NULL);
    if(ret < 0)
        return;

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

    LUSTRE_UNLOCK();
    return;
}
#endif

struct lustre_buf_state
{
    void *buf;
    size_t buf_size;
};
static void lustre_serialize_records(void *rec_ref_p, void *user_ptr)
{
    struct lustre_record_ref *rec_ref = (struct lustre_record_ref *)rec_ref_p;
    struct lustre_buf_state *buf_state = (struct lustre_buf_state *)user_ptr;
    void *output_buf = buf_state->buf + buf_state->buf_size;
    int i;
    int num_stripes = 0;
    size_t record_size;
    struct darshan_lustre_component *comps =
        (struct darshan_lustre_component *)&(rec_ref->record->comps);
    OST_ID *osts = (OST_ID *)(comps + rec_ref->record->num_comps);

    /* skip shared records on non-zero ranks */
    if (my_rank > 0 && rec_ref->record->base_rec.rank == -1)
        return;

    /* update record size to reflect final number of components/stripes */
    for (i = 0; i < rec_ref->record->num_comps; i++)
    {
        /* inactive components have strip size set to -1 when instrumenting */
        if (comps[i].counters[LUSTRE_COMP_STRIPE_SIZE] == -1)
        {
            /* truncate components and set final component and stripe count */
            rec_ref->record->num_comps = i;
            /* move OST list up in record buffer to overwrite unused components */
            memmove(comps + i, osts, num_stripes * sizeof(*osts));
            break;
        }
        num_stripes += comps[i].counters[LUSTRE_COMP_STRIPE_COUNT];
    }
    rec_ref->record->num_stripes = num_stripes;

    record_size = LUSTRE_RECORD_SIZE(rec_ref->record->num_comps, rec_ref->record->num_stripes);

    /* determine whether this record needs to be shifted back in the final record buffer */
    /* NOTE: this happens when preceding records in the output buffer have been shifted
     * down in size
     */
    if (rec_ref->record != output_buf)
    {
        memmove(output_buf, rec_ref->record, record_size);
        rec_ref->record = output_buf;
    }
    buf_state->buf_size += record_size;
}

static void lustre_output(
    void **lustre_buf,
    int *lustre_buf_sz)
{
    struct lustre_buf_state buf_state;

    LUSTRE_LOCK();
    assert(lustre_runtime);

    buf_state.buf = *lustre_buf;
    buf_state.buf_size = 0;
    /* serialize records into final output buffer */
    darshan_iter_record_refs(lustre_runtime->record_id_hash,
        &lustre_serialize_records, &buf_state);

    /* update output buffer size, which may have shrank */
    *lustre_buf_sz = buf_state.buf_size;

    lustre_runtime->frozen = 1;

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
    lustre_runtime_init_attempted = 0;

    LUSTRE_UNLOCK();
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
