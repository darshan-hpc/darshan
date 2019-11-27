/*
 * Copyright (C) 2015 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

#ifndef __DARSHAN_ABCXYZ_LOG_FORMAT_H
#define __DARSHAN_ABCXYZ_LOG_FORMAT_H

/* current log format version, to support backwards compatibility */
#define DARSHAN_ABCXYZ_VER 1

#define ABCXYZ_COUNTERS \
    /* number of 'foo' function calls */\
    X(ABCXYZ_FOOS) \
    /* maximum data value set by calls to 'foo' */\
    X(ABCXYZ_FOO_MAX_DAT) \
    /* end of counters */\
    X(ABCXYZ_NUM_INDICES)

#define ABCXYZ_F_COUNTERS \
    /* timestamp of the first call to function 'foo' */\
    X(ABCXYZ_F_FOO_TIMESTAMP) \
    /* timer indicating duration of call to 'foo' with max ABCXYZ_FOO_MAX_DAT value */\
    X(ABCXYZ_F_FOO_MAX_DURATION) \
    /* end of counters */\
    X(ABCXYZ_F_NUM_INDICES)

#define X(a) a,
/* integer counters for the "ABCXYZ" example module */
enum darshan_abcxyz_indices
{
    ABCXYZ_COUNTERS
};

/* floating point counters for the "ABCXYZ" example module */
enum darshan_abcxyz_f_indices
{
    ABCXYZ_F_COUNTERS
};
#undef X

/* the darshan_null_record structure encompasses the high-level data/counters
 * which would actually be logged to file by Darshan for the "ABCXYZ" example
 * module. This example implementation logs the following data for each
 * record:
 *      - a darshan_base_record structure, which contains the record id & rank
 *      - integer I/O counters (operation counts, I/O sizes, etc.)
 *      - floating point I/O counters (timestamps, cumulative timers, etc.)
 */
struct darshan_abcxyz_record
{
    struct darshan_base_record base_rec;
    int64_t counters[ABCXYZ_NUM_INDICES];
    double fcounters[ABCXYZ_F_NUM_INDICES];
};

#endif /* __DARSHAN_ABCXYZ_LOG_FORMAT_H */
