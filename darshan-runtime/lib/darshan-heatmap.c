/*
 * Copyright (C) 2021 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

#ifdef HAVE_CONFIG_H
# include <darshan-runtime-config.h>
#endif

#define _XOPEN_SOURCE 500
#define _GNU_SOURCE

#include <stdlib.h>
#include <pthread.h>
#include <assert.h>
#include <math.h>
#include <stdint.h>
#ifdef HAVE_STDATOMIC_H
#include <stdatomic.h>
#endif

#include "darshan.h"
#include "darshan-heatmap.h"

/* If set, this is the globally (across all ranks) agreed-upon timestamp to
 * use as the end time for normalizing and pruning heatmap bins consistently.
 * If not set, use locally derived end timestamp during output fn.
 */
double g_end_timestamp = 0;

/* maximum number of bins per record */
/* TODO: make this tunable at runtime */
/* TODO: safety check that total record size plus trailing bins doesn't
 * exceed DEF_MOD_BUF_SIZE.  If it does, the log will still be technically
 * valid but the default darshan-parser will not be able to display it.
 */
#define DARSHAN_MAX_HEATMAP_BINS 200

/* initial width of each bin, as floating point seconds */
/* TODO: make this tunable at runtime */
#define DARSHAN_INITIAL_BIN_WIDTH_SECONDS 0.1

/* maximum number of distinct heatmaps that we will track (there is a
 * heatmap per module that interacts with it, not per file, so we should not
 * need many).  If this limit is exceeded then the darshan core will mark
 * the "partial" flag for the log so that we will be able to tell that the
 * limit has been hit.
 */
/* TODO: make this tunable at runtime */
#define DARSHAN_MAX_HEATMAPS 8

/* structure to track heatmaps at runtime */
struct heatmap_record_ref
{
    struct darshan_heatmap_record* heatmap_rec;
};

/* The heatmap_runtime structure maintains necessary state for storing
 * heatmap records and for coordinating with darshan-core at shutdown time.
 */
struct heatmap_runtime
{
    void *rec_id_hash;
    int rec_count;
    int frozen; /* flag to indicate that the counters should no longer be modified */
};

static struct heatmap_runtime *heatmap_runtime = NULL;
static int my_rank = -1;

static struct heatmap_record_ref *heatmap_track_new_record(
    darshan_record_id rec_id, const char *name);
static void collapse_heatmap(struct darshan_heatmap_record *rec);
#ifdef HAVE_MPI
static void heatmap_mpi_redux(
    void *stdio_buf, MPI_Comm mod_comm,
    darshan_record_id *shared_recs, int shared_rec_count);
#endif

#ifdef HAVE_STDATOMIC_H
atomic_flag heatmap_runtime_mutex;
#define HEATMAP_LOCK() \
    while (atomic_flag_test_and_set(&heatmap_runtime_mutex))
#define HEATMAP_UNLOCK() \
    atomic_flag_clear(&heatmap_runtime_mutex)
#else
static pthread_mutex_t heatmap_runtime_mutex = PTHREAD_MUTEX_INITIALIZER;
#define HEATMAP_LOCK() pthread_mutex_lock(&heatmap_runtime_mutex)
#define HEATMAP_UNLOCK() pthread_mutex_unlock(&heatmap_runtime_mutex)
#endif

/* note that if the break condition is triggered in this macro, then it
 * will exit the do/while loop holding a lock that will be released in
 * POST_RECORD().  Otherwise it will release the lock here (if held) and
 * return immediately without reaching the POST_RECORD() macro.
 */
/* NOTE: unlike other modules, the PRE_RECORD here does not attempt to
 * initialize this module if it isn't already.  That should have been done
 * in the _register() call before reaching this point.  Skipping the
 * initialization attempt here makes it safe to use atomics or spinlocks
 * in the critical wrapper path.
 */
#define HEATMAP_PRE_RECORD() do { \
    HEATMAP_LOCK(); \
    if(heatmap_runtime && !heatmap_runtime->frozen) break; \
    HEATMAP_UNLOCK(); \
    return(ret); \
} while(0)

/* same as above but for void fns */
#define HEATMAP_PRE_RECORD_VOID() do { \
    HEATMAP_LOCK(); \
    if(heatmap_runtime && !heatmap_runtime->frozen) break; \
    HEATMAP_UNLOCK(); \
    return; \
} while(0)

#define HEATMAP_POST_RECORD() do { \
    HEATMAP_UNLOCK(); \
} while(0)

static void heatmap_output(
    void **heatmap_buf,
    int *heatmap_buf_sz)
{
    struct darshan_heatmap_record* rec;
    struct darshan_heatmap_record* next_rec;
    void* contig_buf_ptr;
    int i,j;
    double end_timestamp;
    unsigned long this_size;
    int tmp_nbins;
    int empty;

    HEATMAP_LOCK();
    assert(heatmap_runtime);

    *heatmap_buf_sz = 0;

    /* freeze instrumentation if it's not already */
    heatmap_runtime->frozen = 1;

    /* use coordinated end timestamp if available, otherwise local time */
    if(g_end_timestamp)
        end_timestamp = g_end_timestamp;
    else
        end_timestamp = darshan_core_wtime();


    /* iterate through records (heatmap histograms) to drop any that contain
     * no data
     */
    for(i=0; i<heatmap_runtime->rec_count; i++)
    {
        do {
            rec = (struct darshan_heatmap_record*)((uintptr_t)*heatmap_buf + i*(sizeof(*rec) + DARSHAN_MAX_HEATMAP_BINS*2*sizeof(int64_t)));
            next_rec = (struct darshan_heatmap_record*)((uintptr_t)*heatmap_buf + (i+1)*(sizeof(*rec) + DARSHAN_MAX_HEATMAP_BINS*2*sizeof(int64_t)));

            empty = 1;
            for(j=0; j<DARSHAN_MAX_HEATMAP_BINS; j++)
            {
                if(rec->write_bins[j] > 0 || rec->read_bins[j] > 0) {
                    empty = 0;
                    break;
                }
            }
            /* reduce record count if this heatmap is empty */
            if(empty) {
                heatmap_runtime->rec_count--;
                /* if there are more heatmaps after this one, shift them all down */
                if (i < heatmap_runtime->rec_count) {
                    memmove(rec, next_rec,
                            (heatmap_runtime->rec_count - i) *
                            (sizeof(*rec) + DARSHAN_MAX_HEATMAP_BINS * 2 * sizeof(int64_t)));
                    /* fix pointers in any heatmaps that were compacted */
                    for (j = 0; j < heatmap_runtime->rec_count - i; j++) {
                        rec->write_bins
                            = (int64_t*)((uintptr_t)rec + sizeof(*rec));
                        rec->read_bins
                            = (int64_t*)((uintptr_t)rec + sizeof(*rec)
                              + DARSHAN_MAX_HEATMAP_BINS * sizeof(int64_t));
                        rec = (struct
                              darshan_heatmap_record*)((uintptr_t)rec
                              + (sizeof(*rec) + DARSHAN_MAX_HEATMAP_BINS
                              * 2 * sizeof(int64_t)));
                    }
                }
            }
            /* repeat in this i position as long as we find empty heatmaps */
        } while(empty && i<heatmap_runtime->rec_count);
    }

    /* iterate through records (heatmap histograms) to normalize bin widths
     * and compact memory
     */
    contig_buf_ptr = *heatmap_buf;
    for(i=0; i<heatmap_runtime->rec_count; i++)
    {
        rec = (struct darshan_heatmap_record*)((uintptr_t)*heatmap_buf + i*(sizeof(*rec) + DARSHAN_MAX_HEATMAP_BINS*2*sizeof(int64_t)));

        /* Collapse records if needed until the total histogram time range
         * extends to end of execution time.  This will ensure that all of
         * the heatmap records have a consistent size
         */
        while(end_timestamp > rec->bin_width_seconds * DARSHAN_MAX_HEATMAP_BINS)
            collapse_heatmap(rec);

        tmp_nbins= ceil(end_timestamp/rec->bin_width_seconds);

        /* are there bins beyond the execution time of the program? */
        if(tmp_nbins < rec->nbins)
        {
            /* truncate bins so that we don't report any beyond the time when
             * instrumentation stopped
             */
            rec->nbins = tmp_nbins;
            /* shift read_bins down so that memory remains contiguous even
             * if nbins has been reduced
             */
            memmove(&rec->write_bins[rec->nbins], rec->read_bins,
                rec->nbins*sizeof(int64_t));
            rec->read_bins = &rec->write_bins[rec->nbins];
        }

        /* now shift the entire record + bins as a contiguous block down in
         * the buffer so that the entire buffer is contiguous
         */
        this_size = sizeof(*rec) + rec->nbins * 2 * sizeof(uint64_t);
        memmove(contig_buf_ptr, rec, this_size);
        contig_buf_ptr += this_size;
        *heatmap_buf_sz += this_size;
    }

    HEATMAP_UNLOCK();

    return;
}

static void heatmap_cleanup()
{
    HEATMAP_LOCK();
    assert(heatmap_runtime);

    /* cleanup internal structures used for instrumenting */
    darshan_clear_record_refs(&(heatmap_runtime->rec_id_hash), 1);

    free(heatmap_runtime);
    heatmap_runtime = NULL;

    HEATMAP_UNLOCK();
    return;
}

struct heatmap_runtime* heatmap_runtime_initialize(void)
{
    struct heatmap_runtime* tmp_runtime;
    int ret;
    /* NOTE: this module generates one record per module that uses it, so
     * the memory requirements should be modest
     */
    size_t heatmap_buf_size = sizeof(struct darshan_heatmap_record) + 2*DARSHAN_MAX_HEATMAP_BINS*sizeof(int64_t);
    size_t heatmap_rec_count = DARSHAN_MAX_HEATMAPS;

    darshan_module_funcs mod_funcs = {
#ifdef HAVE_MPI
        .mod_redux_func = heatmap_mpi_redux,
#endif
        .mod_output_func = heatmap_output,
        .mod_cleanup_func = heatmap_cleanup
    };

    /* register the heatmap module with darshan core */
    /* note that we aren't holding a lock in this module at this point, but
     * the core will serialize internally and return if this module is
     * already registered */
    ret = darshan_core_register_module(
        DARSHAN_HEATMAP_MOD,
        mod_funcs,
        heatmap_buf_size,
        &heatmap_rec_count,
        &my_rank,
        NULL);
    if(ret < 0)
        return(NULL);

    tmp_runtime = malloc(sizeof(*tmp_runtime));
    if(!tmp_runtime)
    {
        darshan_core_unregister_module(DARSHAN_HEATMAP_MOD);
        return(NULL);
    }
    memset(tmp_runtime, 0, sizeof(*tmp_runtime));

    return(tmp_runtime);
}

darshan_record_id heatmap_register(const char* name)
{
    struct heatmap_record_ref *rec_ref;
    darshan_record_id ret = 0;
    struct heatmap_runtime* tmp_runtime;

    HEATMAP_LOCK();

    if(!heatmap_runtime) {
        /* module not initialized. Drop atomic lock and try to do so */
        HEATMAP_UNLOCK();

        tmp_runtime = heatmap_runtime_initialize();

        HEATMAP_LOCK();
        /* see if someone beat us to it */
        if(heatmap_runtime && tmp_runtime)
            free(tmp_runtime);
        else
            heatmap_runtime = tmp_runtime;
    }

    /* if we exit the above logic without anyone initializing, then we
     * silently return
     */
    if(!heatmap_runtime) {
        HEATMAP_UNLOCK();
        return(0);
    }

    /* generate id for this heatmap */
    ret = darshan_core_gen_record_id(name);

    /* go ahead and instantiate a record now, rather than waiting until the
     * _update() call
     */
    rec_ref = darshan_lookup_record_ref(heatmap_runtime->rec_id_hash, &ret, sizeof(darshan_record_id));
    if(!rec_ref) rec_ref = heatmap_track_new_record(ret, name);

    HEATMAP_UNLOCK();

    return(ret);
}

static void collapse_heatmap(struct darshan_heatmap_record *rec)
{
    int i;

    /* collapse write bins */
    for(i=0; i<DARSHAN_MAX_HEATMAP_BINS; i+=2)
    {
        rec->write_bins[i] += rec->write_bins[i+1]; /* accumulate adjacent bins */
        rec->write_bins[i/2] = rec->write_bins[i];  /* shift down */
    }
    /* zero out second half of heatmap */
    memset(&rec->write_bins[DARSHAN_MAX_HEATMAP_BINS/2], 0, (DARSHAN_MAX_HEATMAP_BINS/2)*sizeof(int64_t));

    /* collapse read bins */
    for(i=0; i<DARSHAN_MAX_HEATMAP_BINS; i+=2)
    {
        rec->read_bins[i] += rec->read_bins[i+1]; /* accumulate adjacent bins */
        rec->read_bins[i/2] = rec->read_bins[i];  /* shift down */
    }
    /* zero out second half of heatmap */
    memset(&rec->read_bins[DARSHAN_MAX_HEATMAP_BINS/2], 0, (DARSHAN_MAX_HEATMAP_BINS/2)*sizeof(int64_t));

    /* double bin width */
    rec->bin_width_seconds *= 2.0;

    return;
}

void heatmap_update(darshan_record_id heatmap_id, int rw_flag,
    int64_t size, double start_time, double end_time)
{
    struct heatmap_record_ref *rec_ref;
    int bin_index = 0;
    double top_boundary, bottom_boundary, seconds_in_bin;
    int64_t intermediate_bytes;

    /* if size is zero, we have no work to do here */
    if(size == 0) return;

    HEATMAP_PRE_RECORD_VOID();

    rec_ref = darshan_lookup_record_ref(heatmap_runtime->rec_id_hash, &heatmap_id, sizeof(darshan_record_id));
    /* the heatmap should have already been instantiated in the register
     * function; something is wrong if we can't find it now
     */
    if(!rec_ref) { HEATMAP_POST_RECORD(); return; }

    /* is current update out of bounds with histogram size?  if so, collapse */
    while(end_time > rec_ref->heatmap_rec->bin_width_seconds * DARSHAN_MAX_HEATMAP_BINS)
        collapse_heatmap(rec_ref->heatmap_rec);

    /* once we fall through to this point, we know that the current heatmap
     * granularity is sufficiently large to hold this update
     */

    /* loop through bins to be updated (a given access may cross bin
     * boundaries) */
    /* note: counting on the below type conversion to round down to lower
     * integer */
    for(bin_index = start_time/rec_ref->heatmap_rec->bin_width_seconds; bin_index < (int)(end_time/rec_ref->heatmap_rec->bin_width_seconds + 1); bin_index++)
    {
        /* starting assumption about how much time this update spent in
         * current bin
         */
        seconds_in_bin = rec_ref->heatmap_rec->bin_width_seconds;
        /* calculate where bin starts and stops */
        bottom_boundary = bin_index * rec_ref->heatmap_rec->bin_width_seconds;
        top_boundary = bottom_boundary + rec_ref->heatmap_rec->bin_width_seconds;
        /* truncate if update started after bottom boundary */
        if(start_time > bottom_boundary)
            seconds_in_bin -= start_time-bottom_boundary;
        /* truncate if update ended before top boundary */
        if(end_time < top_boundary)
            seconds_in_bin -= top_boundary-end_time;

        if(seconds_in_bin < 0){
            /* this should never happen; really this is an assertion
             * condition but here we just bail out to avoid disrupting the
             * application.
             */
            HEATMAP_POST_RECORD();
            return;
        }

        if(end_time > start_time)
            intermediate_bytes = round(size * (seconds_in_bin/(end_time-start_time)));
        else
            intermediate_bytes = size;

        /* proportionally assign bytes to this bin */
        if(rw_flag == HEATMAP_WRITE)
            rec_ref->heatmap_rec->write_bins[bin_index] +=
                intermediate_bytes;
        else
            rec_ref->heatmap_rec->read_bins[bin_index] +=
                intermediate_bytes;
    }

    HEATMAP_POST_RECORD();

    return;
}

static struct heatmap_record_ref *heatmap_track_new_record(
    darshan_record_id rec_id, const char *name)
{
    struct darshan_heatmap_record *heatmap_rec = NULL;
    struct heatmap_record_ref *rec_ref = NULL;
    int ret;

    rec_ref = malloc(sizeof(*rec_ref));
    if(!rec_ref)
        return(NULL);
    memset(rec_ref, 0, sizeof(*rec_ref));

    /* add a reference to this record */
    ret = darshan_add_record_ref(&(heatmap_runtime->rec_id_hash), &rec_id,
        sizeof(darshan_record_id), rec_ref);
    if(ret == 0)
    {
        free(rec_ref);
        return(NULL);
    }

    /* register with darshan-core so it is persisted in the log file */
    /* include enough space for 2x number of heatmap bins (read and write) */
    heatmap_rec = darshan_core_register_record(
        rec_id,
        name,
        DARSHAN_HEATMAP_MOD,
        sizeof(struct darshan_heatmap_record)+(2*DARSHAN_MAX_HEATMAP_BINS*sizeof(int64_t)),
        NULL);

    if(!heatmap_rec)
    {
        darshan_delete_record_ref(&(heatmap_runtime->rec_id_hash),
            &rec_id, sizeof(darshan_record_id));
        free(rec_ref);
        return(NULL);
    }

    /* registering this file record was successful, so initialize some fields */
    heatmap_rec->base_rec.id = rec_id;
    heatmap_rec->base_rec.rank = my_rank;
    heatmap_rec->bin_width_seconds = DARSHAN_INITIAL_BIN_WIDTH_SECONDS;
    heatmap_rec->nbins = DARSHAN_MAX_HEATMAP_BINS;
    heatmap_rec->write_bins = (int64_t*)((uintptr_t)heatmap_rec + sizeof(*heatmap_rec));
    heatmap_rec->read_bins = (int64_t*)((uintptr_t)heatmap_rec + sizeof(*heatmap_rec) + heatmap_rec->nbins*sizeof(int64_t));
    rec_ref->heatmap_rec = heatmap_rec;
    heatmap_runtime->rec_count++;

    return(rec_ref);
}

#ifdef HAVE_MPI
static void heatmap_mpi_redux(
    void *stdio_buf, MPI_Comm mod_comm,
    darshan_record_id *shared_recs, int shared_rec_count)
{
    double end_timestamp;

    /* NOTE: no actual record reduction here.  We are just using this as an
     * opportunity to agree on shutdown times.
     */

    HEATMAP_LOCK();
    assert(heatmap_runtime);
    heatmap_runtime->frozen = 1;
    HEATMAP_UNLOCK();

    /* check time locally */
    end_timestamp = darshan_core_wtime();

    /* reduce across all ranks, take maximum (it's Ok if this rank doesn't
     * have data out that far; it will be zeroed anyway)
     */
    PMPI_Allreduce(&end_timestamp, &g_end_timestamp, 1, MPI_DOUBLE,
        MPI_MAX, mod_comm);
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
