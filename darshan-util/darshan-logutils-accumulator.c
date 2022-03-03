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

struct darshan_accumulator_st {
    darshan_module_id module_id;
    void* agg_record;
    int num_records;
};

int darshan_accumulator_create(darshan_module_id id,
                               darshan_accumulator*   new_accumulator)
{
    /* TODO: assertion on max available mod id */

    *new_accumulator = calloc(1, sizeof(struct darshan_accumulator_st));
    if(!(*new_accumulator))
        return(-1);

    (*new_accumulator)->module_id = id;
    (*new_accumulator)->agg_record = calloc(1, DEF_MOD_BUF_SIZE);
    if(!(*new_accumulator)->agg_record) {
        free(*new_accumulator);
        return(-1);
    }

    return(0);
}

int darshan_accumulator_inject(darshan_accumulator acc,
                               void*               record_array,
                               int                 record_count)
{
    int i;
    void* new_record = record_array;

    if(!mod_logutils[acc->module_id]->log_agg_records ||
       !mod_logutils[acc->module_id]->log_sizeof_record) {
        /* this module doesn't support this operation */
        return(-1);
    }

    for(i=0; i<record_count; i++) {
        if(acc->num_records == 0)
            mod_logutils[acc->module_id]->log_agg_records(new_record, acc->agg_record, 1);
        else
            mod_logutils[acc->module_id]->log_agg_records(new_record, acc->agg_record, 0);
        acc->num_records++;

        /* advance to next record */
        new_record += mod_logutils[acc->module_id]->log_sizeof_record(new_record);
    }

    return(0);
}

int darshan_accumulator_emit(darshan_accumulator             acc,
                             struct darshan_derived_metrics* metrics,
                             void*                           summation_record)
{
    memcpy(summation_record, acc->agg_record, mod_logutils[acc->module_id]->log_sizeof_record(acc->agg_record));
    return(0);
}

int darshan_accumulator_destroy(darshan_accumulator accumulator)
{
    if(accumulator->agg_record)
        free(accumulator->agg_record);
    free(accumulator);

    return(0);
}
