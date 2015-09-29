/*
 * Copyright (C) 2015 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

#ifndef __DARSHAN_NULL_LOG_FORMAT_H
#define __DARSHAN_NULL_LOG_FORMAT_H

#include "darshan-log-format.h"

#define NULL_COUNTERS \
    /* count of number of 'bar' function calls */\
    X(NULL_BARS) \
    /* arbitrary data value set by last call to 'bar' */\
    X(NULL_BAR_DAT) \
    /* end of counters */\
    X(NULL_NUM_INDICES)

#define NULL_F_COUNTERS \
    /* timestamp of the first call to function 'bar' */\
    X(NULL_F_BAR_TIMESTAMP) \
    /* timer indicating duration of last call to 'bar' */\
    X(NULL_F_BAR_DURATION) \
    /* end of counters */\
    X(NULL_F_NUM_INDICES)

#define X(a) a,
/* integer counters for the "NULL" example module */
enum darshan_null_indices
{
    NULL_COUNTERS
};

/* floating point counters for the "NULL" example module */
enum darshan_null_f_indices
{
    NULL_F_COUNTERS
};
#undef X

/* the darshan_null_record structure encompasses the high-level data/counters
 * which would actually be logged to file by Darshan for the "NULL" example
 * module. This example implementation logs the following data for each
 * record:
 *      - a corresponding Darshan record identifier
 *      - the rank of the process responsible for the record
 *      - integer I/O counters (operation counts, I/O sizes, etc.)
 *      - floating point I/O counters (timestamps, cumulative timers, etc.)
 */
struct darshan_null_record
{
    darshan_record_id f_id;
    int64_t rank;
    int64_t counters[NULL_NUM_INDICES];
    double fcounters[NULL_F_NUM_INDICES];
};

#endif /* __DARSHAN_NULL_LOG_FORMAT_H */
