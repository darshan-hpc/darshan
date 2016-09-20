/*
 * Copyright (C) 2015 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

#ifndef __DARSHAN_NULL_LOG_FORMAT_H
#define __DARSHAN_NULL_LOG_FORMAT_H

/* current log format version, to support backwards compatibility */
#define DARSHAN_NULL_VER 1

#define NULL_COUNTERS \
    /* number of 'foo' function calls */\
    X(NULL_FOOS) \
    /* maximum data value set by calls to 'foo' */\
    X(NULL_FOO_MAX_DAT) \
    /* end of counters */\
    X(NULL_NUM_INDICES)

#define NULL_F_COUNTERS \
    /* timestamp of the first call to function 'foo' */\
    X(NULL_F_FOO_TIMESTAMP) \
    /* timer indicating duration of call to 'foo' with max NULL_FOO_MAX_DAT value */\
    X(NULL_F_FOO_MAX_DURATION) \
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
 *      - a darshan_base_record structure, which contains the record id & rank
 *      - integer I/O counters (operation counts, I/O sizes, etc.)
 *      - floating point I/O counters (timestamps, cumulative timers, etc.)
 */
struct darshan_null_record
{
    struct darshan_base_record base_rec;
    int64_t counters[NULL_NUM_INDICES];
    double fcounters[NULL_F_NUM_INDICES];
};

#endif /* __DARSHAN_NULL_LOG_FORMAT_H */
