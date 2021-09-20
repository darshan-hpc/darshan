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
#include <stdlib.h>
#include <string.h>
#include <limits.h>
#include <search.h>
#include <assert.h>

#include "uthash.h"

#include "darshan.h"

/* track opaque record referencre using a hash link */
struct darshan_record_ref_tracker
{
    void *rec_ref_p;
    UT_hash_handle hlink;
};

void *darshan_lookup_record_ref(void *hash_head, void *handle, size_t handle_sz)
{
    struct darshan_record_ref_tracker *ref_tracker;
    struct darshan_record_ref_tracker *ref_tracker_head =
        (struct darshan_record_ref_tracker *)hash_head;

    /* search the hash table for the given handle */
    HASH_FIND(hlink, ref_tracker_head, handle, handle_sz, ref_tracker);
    if(ref_tracker)
        return(ref_tracker->rec_ref_p);
    else
        return(NULL);
}

int darshan_add_record_ref(void **hash_head_p, void *handle, size_t handle_sz,
    void *rec_ref_p)
{
    struct darshan_record_ref_tracker *ref_tracker;
    struct darshan_record_ref_tracker *ref_tracker_head =
        *(struct darshan_record_ref_tracker **)hash_head_p;
    void *handle_p;

    /* allocate a reference tracker, with room to store the handle at the end */
    ref_tracker = malloc(sizeof(*ref_tracker) + handle_sz);
    if(!ref_tracker)
        return(0);
    memset(ref_tracker, 0, sizeof(*ref_tracker) + handle_sz);

    /* initialize the reference tracker and add it to the hash table */
    ref_tracker->rec_ref_p = rec_ref_p;
    handle_p = (char *)ref_tracker + sizeof(*ref_tracker);
    memcpy(handle_p, handle, handle_sz);
    HASH_ADD_KEYPTR(hlink, ref_tracker_head, handle_p, handle_sz, ref_tracker);
    *hash_head_p = ref_tracker_head;
    return(1);
}

void *darshan_delete_record_ref(void **hash_head_p, void *handle, size_t handle_sz)
{
    struct darshan_record_ref_tracker *ref_tracker;
    struct darshan_record_ref_tracker *ref_tracker_head =
        *(struct darshan_record_ref_tracker **)hash_head_p;
    void *rec_ref_p;

    /* find the reference tracker for this handle */
    HASH_FIND(hlink, ref_tracker_head, handle, handle_sz, ref_tracker);
    if(!ref_tracker)
        return(NULL);

    /* if found, delete from hash table and return the record reference pointer */
    HASH_DELETE(hlink, ref_tracker_head, ref_tracker);
    *hash_head_p = ref_tracker_head;
    rec_ref_p = ref_tracker->rec_ref_p;
    free(ref_tracker);

    return(rec_ref_p);
}

void darshan_clear_record_refs(void **hash_head_p, int free_flag)
{
    struct darshan_record_ref_tracker *ref_tracker, *tmp;
    struct darshan_record_ref_tracker *ref_tracker_head =
        *(struct darshan_record_ref_tracker **)hash_head_p;

    /* iterate the hash table and remove/free all reference trackers */
    HASH_ITER(hlink, ref_tracker_head, ref_tracker, tmp)
    {
        HASH_DELETE(hlink, ref_tracker_head, ref_tracker);
        if(free_flag)
            free(ref_tracker->rec_ref_p);
        free(ref_tracker);
    }
    *hash_head_p = ref_tracker_head;

    return;
}

void darshan_iter_record_refs(void *hash_head, void (*iter_action)(void *, void *),
    void *user_ptr)
{
    struct darshan_record_ref_tracker *ref_tracker, *tmp;
    struct darshan_record_ref_tracker *ref_tracker_head =
        (struct darshan_record_ref_tracker *)hash_head;

    /* iterate the hash table, performing the given action for each reference
     * tracker's corresponding record reference pointer
     */
    HASH_ITER(hlink, ref_tracker_head, ref_tracker, tmp)
    {
        iter_action(ref_tracker->rec_ref_p, user_ptr);
    }

    return;
}

char* darshan_clean_file_path(const char* path)
{
    char* newpath = NULL;
    char* cwd = NULL;
    char* filter = NULL;

    /* NOTE: the last check in this if statement is for path strings that
     * begin with the '<' character.  We assume that these are special
     * reserved paths used by Darshan, like <STDIN>.
     */
    if(!path || strlen(path) < 1 || path[0] == '<')
        return(NULL);

    if(path[0] == '/')
    {
        /* it is already an absolute path */
        newpath = malloc(strlen(path)+1);
        if(newpath)
        {
            strcpy(newpath, path);
        }
    }
    else
    {
        /* handle relative path */
        cwd = malloc(PATH_MAX);
        if(cwd)
        {
            if(getcwd(cwd, PATH_MAX))
            {
                newpath = malloc(strlen(path) + strlen(cwd) + 2);
                if(newpath)
                {
                    sprintf(newpath, "%s/%s", cwd, path);
                }
            }
            free(cwd);
        }
    }

    if(!newpath)
        return(NULL);

    /* filter out any double slashes */
    while((filter = strstr(newpath, "//")))
    {
        /* shift down one character */
        memmove(filter, &filter[1], (strlen(&filter[1]) + 1));
    }

    /* filter out any /./ instances */
    while((filter = strstr(newpath, "/./")))
    {
        /* shift down two characters */
        memmove(filter, &filter[2], (strlen(&filter[2]) + 1));
    }

    /* return result */
    return(newpath);
}

/* compare function for sorting file records according to their 
 * darshan_base_record structure. Records are sorted first by
 * descending rank (to get all shared records, with rank set to -1, in
 * a contiguous region at the end of the record buffer) then
 * by ascending record identifiers (which are just unsigned integers).
 */
static int darshan_base_record_compare(const void* a_p, const void* b_p)
{
    const struct darshan_base_record *a = a_p;
    const struct darshan_base_record *b = b_p;

    if(a->rank < b->rank)
        return(1);
    if(a->rank > b->rank)
        return(-1);

    /* same rank, sort by ascending record ids */
    if(a->id > b->id)
        return(1);
    if(a->id < b->id)
        return(-1);

    return(0);
}

void darshan_record_sort(void *rec_buf, int rec_count, int rec_size)
{
    qsort(rec_buf, rec_count, rec_size, darshan_base_record_compare);
    return;
}

static int darshan_common_vals_compare(const void *a_p, const void *b_p)
{
    const struct darshan_common_val_counter* a = a_p;
    const struct darshan_common_val_counter* b = b_p;

    return(memcmp(a->vals, b->vals, sizeof(*(a->vals)) * a->nvals));
}

struct darshan_common_val_counter *darshan_track_common_val_counters(
    void **common_val_root, int64_t *vals, int nvals, int *common_val_count)
{
    struct darshan_common_val_counter* counter;
    struct darshan_common_val_counter* found = NULL;
    struct darshan_common_val_counter tmp_counter;
    void* tmp;

    assert(nvals <= DARSHAN_COMMON_VAL_MAX_NCOUNTERS);

    /* check to see if this val is already recorded */
    memcpy(tmp_counter.vals, vals, sizeof(*vals) * nvals);
    tmp_counter.nvals = nvals;
    tmp_counter.freq = 1;
    tmp = tfind(&tmp_counter, common_val_root, darshan_common_vals_compare);
    if(tmp)
    {
        found = *(struct darshan_common_val_counter**)tmp;
        found->freq++;
    }
    else if(*common_val_count < DARSHAN_COMMON_VAL_MAX_RUNTIME_COUNT)
    {
        /* we can add a new one as long as we haven't hit the limit */
        counter = malloc(sizeof(*counter));
        if(!counter)
        {
            return(NULL);
        }

        memcpy(counter->vals, vals, sizeof(*vals) * nvals);
        counter->nvals = nvals;
        counter->freq = 1;

        tmp = tsearch(counter, common_val_root, darshan_common_vals_compare);
        found = *(struct darshan_common_val_counter**)tmp;
        /* if we get a new answer out here we are in trouble; this was
         * already checked with the tfind()
         */
        assert(found == counter);

        (*common_val_count)++;
    }

    return(found);
}

#ifdef HAVE_MPI
void darshan_variance_reduce(void *invec, void *inoutvec, int *len,
    MPI_Datatype *dt)
{
    int i;
    struct darshan_variance_dt *X = invec;
    struct darshan_variance_dt *Y = inoutvec;
    struct darshan_variance_dt  Z;

    for (i=0; i<*len; i++,X++,Y++)
    {
        Z.n = X->n + Y->n;
        Z.T = X->T + Y->T;
        Z.S = X->S + Y->S + (X->n/(Y->n*Z.n)) *
           ((Y->n/X->n)*X->T - Y->T) * ((Y->n/X->n)*X->T - Y->T);

        *Y = Z;
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
