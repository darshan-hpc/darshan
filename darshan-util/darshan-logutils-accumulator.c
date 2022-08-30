/*
 * Copyright (C) 2022 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

/* This function implements the accumulator API (darshan_accumlator*)
 * functions in darshan-logutils.h.
 */

#include <stdlib.h>
#include <assert.h>

#include "darshan-logutils.h"
#include "uthash-1.9.2/src/uthash.h"

#define max(a,b) (((a) > (b)) ? (a) : (b))

/* struct to track per-file metrics */
typedef struct file_hash_entry_s
{
    UT_hash_handle hlink;
    darshan_record_id rec_id;
    int64_t r_bytes;     /* bytes read */
    int64_t w_bytes;     /* bytes written */
    int64_t max_offset;  /* maximum offset accessed */
    int64_t nprocs;      /* nprocs that accessed it */
} file_hash_entry_t;

/* accumulator state */
struct darshan_accumulator_st {
    darshan_module_id module_id;
    int64_t job_nprocs;
    void* agg_record;
    int num_records;
    file_hash_entry_t *file_hash_table;

    /* amount of time consumed by slowest rank in shared files, across all
     * shared files observed
     */
    double shared_io_total_time_by_slowest;
    /* how many total bytes were read or written? */
    int64_t total_bytes;
    /* for non-shared files, how long did each rank spend in IO? */
    double *rank_cumul_io_total_time;
    double *rank_cumul_rw_only_time;
    double *rank_cumul_md_only_time;
};

int darshan_accumulator_create(darshan_module_id id,
                               int64_t job_nprocs,
                               darshan_accumulator*   new_accumulator)
{
    *new_accumulator = NULL;

    if(id >= DARSHAN_KNOWN_MODULE_COUNT)
        return(-1);

    if(!mod_logutils[id]->log_agg_records ||
       !mod_logutils[id]->log_sizeof_record ||
       !mod_logutils[id]->log_record_metrics) {
        /* this module doesn't support this operation */
        return(-1);
    }

    *new_accumulator = calloc(1, sizeof(struct darshan_accumulator_st));
    if(!(*new_accumulator))
        return(-1);

    (*new_accumulator)->module_id = id;
    (*new_accumulator)->job_nprocs = job_nprocs;
    (*new_accumulator)->agg_record = calloc(1, DEF_MOD_BUF_SIZE);
    if(!(*new_accumulator)->agg_record) {
        free(*new_accumulator);
        *new_accumulator = NULL;
        return(-1);
    }
    /* 3 arrays handled in one malloc */
    (*new_accumulator)->rank_cumul_io_total_time = calloc(job_nprocs*3, sizeof(double));
    if(!(*new_accumulator)->rank_cumul_io_total_time) {
        free((*new_accumulator)->agg_record);
        free(*new_accumulator);
        *new_accumulator = NULL;
        return(-1);
    }
    (*new_accumulator)->rank_cumul_rw_only_time = &((*new_accumulator)->rank_cumul_io_total_time[job_nprocs]);
    (*new_accumulator)->rank_cumul_md_only_time = &((*new_accumulator)->rank_cumul_io_total_time[job_nprocs*2]);

    return(0);
}

int darshan_accumulator_inject(darshan_accumulator acc,
                               void*               record_array,
                               int                 record_count)
{
    int i;
    void* new_record = record_array;
    uint64_t rec_id;
    int64_t r_bytes;
    int64_t w_bytes;
    int64_t max_offset;
    int64_t nprocs;
    int64_t rank;
    double io_total_time;
    double md_only_time;
    double rw_only_time;
    int ret;
    file_hash_entry_t *hfile = NULL;

    if(!mod_logutils[acc->module_id]->log_agg_records ||
       !mod_logutils[acc->module_id]->log_sizeof_record ||
       !mod_logutils[acc->module_id]->log_record_metrics) {
        /* this module doesn't support this operation */
        return(-1);
    }

    for(i=0; i<record_count; i++) {
        /* accumulate aggregate record */
        if(acc->num_records == 0)
            mod_logutils[acc->module_id]->log_agg_records(new_record, acc->agg_record, 1);
        else
            mod_logutils[acc->module_id]->log_agg_records(new_record, acc->agg_record, 0);
        acc->num_records++;

        /* retrieve generic metrics from record */
        ret = mod_logutils[acc->module_id]->log_record_metrics( new_record,
            &rec_id, &r_bytes, &w_bytes, &max_offset, &io_total_time,
            &md_only_time, &rw_only_time, &rank, &nprocs);
        if(ret < 0)
            return(-1);

        /* accumulate performance metrics */

        /* total bytes moved */
        acc->total_bytes += (r_bytes + w_bytes);

        if(rank < 0) {
            /* sum the slowest I/O time across all shared files */
            acc->shared_io_total_time_by_slowest += io_total_time;
        }
        else {
            /* sum per-rank I/O times (including meta and rw breakdown) for
             * each rank separately
             */
            assert(rank < acc->job_nprocs);
            acc->rank_cumul_io_total_time[rank] += io_total_time;
            acc->rank_cumul_rw_only_time[rank] += rw_only_time;
            acc->rank_cumul_md_only_time[rank] += md_only_time;
        }

        /* track in hash table for per-file metrics; there may be multiple
         * records that refer to the same file */
        HASH_FIND(hlink, acc->file_hash_table, &rec_id, sizeof(rec_id), hfile);
        if(!hfile) {
            /* first time we've seen this file in this accumulator */
            hfile = calloc(1, sizeof(*hfile));
            if(!hfile) {
                return(-1);
            }

            /* add to hash table */
            hfile->rec_id = rec_id;
            HASH_ADD(hlink, acc->file_hash_table, rec_id, sizeof(rec_id), hfile);
        }

        /* we have hfile at this point (either existing or newly created);
         * increment metrics
         */
        hfile->r_bytes += r_bytes;
        hfile->w_bytes += w_bytes;
        if(max_offset == -1)
            hfile->max_offset = -1; /* this module doesn't support this */
        else
            hfile->max_offset = max(hfile->max_offset, max_offset);
        if (nprocs == -1)
            hfile->nprocs = -1; /* globally shared */
        else
            hfile->nprocs += nprocs; /* partially shared or unique, as far as we
                                        know so far */

        /* advance to next record */
        new_record += mod_logutils[acc->module_id]->log_sizeof_record(new_record);
    }

    return(0);
}

/* NOTE: use -1 for procs to indicate that the file was globally shared.
 * This will be marked in the category counters if we find a file hash that
 * was globally shared or if the proc value gets incremented to cover all
 * processes in the job.
 */
#define CATEGORY_INC(__cat_counters_p, __fhe_p, __job_nprocs) \
do{\
    if(!(__cat_counters_p)) \
        break; \
    __cat_counters_p->count++; \
    __cat_counters_p->total_read_volume_bytes += __fhe_p->r_bytes; \
    __cat_counters_p->total_write_volume_bytes += __fhe_p->w_bytes; \
    __cat_counters_p->max_read_volume_bytes = \
        max(__cat_counters_p->max_read_volume_bytes, __fhe_p->r_bytes); \
    __cat_counters_p->max_write_volume_bytes = \
        max(__cat_counters_p->max_write_volume_bytes, __fhe_p->w_bytes); \
    if(__fhe_p->max_offset == -1) {\
        __cat_counters_p->total_max_offset_bytes = -1; \
        __cat_counters_p->max_offset_bytes = -1; \
    }\
    else {\
        __cat_counters_p->total_max_offset_bytes += __fhe_p->max_offset; \
        __cat_counters_p->max_offset_bytes = \
            max(__cat_counters_p->max_offset_bytes, __fhe_p->max_offset); \
    }\
    if(__fhe_p->nprocs > 0 && __cat_counters_p->nprocs > -1) \
        __cat_counters_p->nprocs += __fhe_p->nprocs; \
    if(__fhe_p->nprocs < 0 || __cat_counters_p->nprocs >= __job_nprocs) \
        __cat_counters_p->nprocs = -1; \
}while(0)

int darshan_accumulator_emit(darshan_accumulator             acc,
                             struct darshan_derived_metrics* metrics,
                             void*                           summation_record)
{
    file_hash_entry_t *curr = NULL;
    file_hash_entry_t *tmp_file = NULL;
    struct darshan_file_category_counters* cat_counters;
    int64_t i;

    memset(metrics, 0, sizeof(*metrics));

    /* walk hash table to construct metrics by file category */
    HASH_ITER(hlink, acc->file_hash_table, curr, tmp_file)
    {
        /* all files */
        cat_counters = &metrics->category_counters[DARSHAN_ALL_FILES];
        CATEGORY_INC(cat_counters, curr, acc->job_nprocs);

        /* read-only, write-only, and read-write */
        if(curr->r_bytes > 0 && curr->w_bytes == 0)
            cat_counters = &metrics->category_counters[DARSHAN_RO_FILES];
        else if(curr->w_bytes > 0 && curr->r_bytes == 0)
            cat_counters = &metrics->category_counters[DARSHAN_WO_FILES];
        else if(curr->w_bytes > 0 && curr->r_bytes > 0)
            cat_counters = &metrics->category_counters[DARSHAN_RW_FILES];
        else
            cat_counters = NULL;
        CATEGORY_INC(cat_counters, curr, acc->job_nprocs);

        /* unique, shared, and partially shared */
        if(curr->nprocs == 1)
            cat_counters = &metrics->category_counters[DARSHAN_UNIQ_FILES];
        else if(curr->nprocs == -1)
            cat_counters = &metrics->category_counters[DARSHAN_SHARED_FILES];
        else
            cat_counters = &metrics->category_counters[DARSHAN_PART_SHARED_FILES];
        CATEGORY_INC(cat_counters, curr, acc->job_nprocs);
    }

    /* copy out aggregate record we have been accumulating so far */
    memcpy(summation_record, acc->agg_record, mod_logutils[acc->module_id]->log_sizeof_record(acc->agg_record));

    /* calculate derived performance metrics */
    metrics->total_bytes = acc->total_bytes;
    metrics->shared_io_total_time_by_slowest
        = acc->shared_io_total_time_by_slowest;
    /* determine which rank had the slowest path through unique files */
    for (i = 0; i < acc->job_nprocs; i++) {
        if (acc->rank_cumul_io_total_time[i]
            > metrics->unique_io_total_time_by_slowest) {
            metrics->unique_io_total_time_by_slowest
                = acc->rank_cumul_io_total_time[i];
            metrics->unique_rw_only_time_by_slowest
                = acc->rank_cumul_rw_only_time[i];
            metrics->unique_md_only_time_by_slowest
                = acc->rank_cumul_md_only_time[i];
            metrics->unique_io_slowest_rank = i;
        }
    }

    /* aggregate io time is estimated as the time consumed by the slowest
     * rank in unique files plus the time consumed by the slowest rank in in
     * each shared file
     */
    metrics->agg_time_by_slowest = metrics->unique_io_total_time_by_slowest +
        metrics->shared_io_total_time_by_slowest;
    /* aggregate rate is total bytes deviced by above; guard against divide
     * by zero calculation, though
     */
    if (metrics->agg_time_by_slowest)
        metrics->agg_perf_by_slowest
            = ((double)metrics->total_bytes / 1048576.0)
            / metrics->agg_time_by_slowest;

    return(0);
}

int darshan_accumulator_destroy(darshan_accumulator acc)
{
    file_hash_entry_t *curr = NULL;
    file_hash_entry_t *tmp_file = NULL;

    if(!acc)
        return(0);

    /* three arrays, but handled by one malloc (see _create()) */
    if(acc->rank_cumul_io_total_time)
        free(acc->rank_cumul_io_total_time);

    if(acc->agg_record)
        free(acc->agg_record);

    /* walk file hash table, freeing memory as we go */
    HASH_ITER(hlink, acc->file_hash_table, curr, tmp_file)
    {
        HASH_DELETE(hlink, acc->file_hash_table, curr);
        free(curr);
    }

    free(acc);

    return(0);
}
