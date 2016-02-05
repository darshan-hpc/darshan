/*
 * Copyright (C) 2015 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */
#define _XOPEN_SOURCE 500
#define _GNU_SOURCE

#include "darshan-runtime-config.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <limits.h>
#include <search.h>
#include <assert.h>

#include "darshan.h"

static int darshan_common_val_compare(const void* a_p, const void* b_p);
static void darshan_common_val_walker(const void* nodep, const VISIT which,
    const int depth);

char* darshan_clean_file_path(const char* path)
{
    char* newpath = NULL;
    char* cwd = NULL;
    char* filter = NULL;

    if(!path || strlen(path) < 1)
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

/* HACK: global variables for determining 4 most common values */
static int64_t* walker_val_p = NULL;
static int64_t* walker_cnt_p = NULL;

void darshan_common_val_counter(void **common_val_root, int *common_val_count,
    int64_t val, int64_t *common_val_p, int64_t *common_cnt_p)
{
    struct darshan_common_val_counter* counter;
    struct darshan_common_val_counter* found;
    struct darshan_common_val_counter tmp_counter;
    void* tmp;

    /* don't count any values of 0 */
    if(val == 0)
        return;

    /* check to see if this val is already recorded */
    tmp_counter.val = val;
    tmp_counter.freq = 1;
    tmp = tfind(&tmp_counter, common_val_root, darshan_common_val_compare);
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
            return;
        }

        counter->val = val;
        counter->freq = 1;

        tmp = tsearch(counter, common_val_root, darshan_common_val_compare);
        found = *(struct darshan_common_val_counter**)tmp;
        /* if we get a new answer out here we are in trouble; this was
         * already checked with the tfind()
         */
        assert(found == counter);

        (*common_val_count)++;
    }

#ifdef __DARSHAN_ENABLE_MMAP_LOGS
    /* if we are using darshan's mmap feature, update common access
     * counters as we go
     */
    DARSHAN_COMMON_VAL_COUNTER_INC(common_val_p, common_cnt_p,
        found->val, found->freq, 1);
#endif

    return;
}

void darshan_walk_common_vals(void *common_val_root, int64_t *val_p,
    int64_t *cnt_p)
{
    walker_val_p = val_p;
    walker_cnt_p = cnt_p;

    twalk(common_val_root, darshan_common_val_walker);
    return;
}

static void darshan_common_val_walker(const void *nodep, const VISIT which,
    const int depth)
{
    struct darshan_common_val_counter* counter;

    switch (which)
    {
        case postorder:
        case leaf:
            counter = *(struct darshan_common_val_counter**)nodep;
            DARSHAN_COMMON_VAL_COUNTER_INC(walker_val_p, walker_cnt_p,
                counter->val, counter->freq, 0);
        default:
            break;
    }

    return;
}

static int darshan_common_val_compare(const void *a_p, const void *b_p)
{
    const struct darshan_common_val_counter* a = a_p;
    const struct darshan_common_val_counter* b = b_p;

    if(a->val < b->val)
        return(-1);
    if(a->val > b->val)
        return(1);
    return(0);
}

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

/*
 * Local variables:
 *  c-indent-level: 4
 *  c-basic-offset: 4
 * End:
 *
 * vim: ts=8 sts=4 sw=4 expandtab
 */
