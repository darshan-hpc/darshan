/*
 * Copyright (C) 2015 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

#ifndef __DARSHAN_STDIO_LOG_FORMAT_H
#define __DARSHAN_STDIO_LOG_FORMAT_H

/* current log format version, to support backwards compatibility */
#define DARSHAN_STDIO_VER 1

#define STDIO_COUNTERS \
    /* count of number of 'bar' function calls */\
    X(STDIO_BARS) \
    /* arbitrary data value set by last call to 'bar' */\
    X(STDIO_BAR_DAT) \
    /* end of counters */\
    X(STDIO_NUM_INDICES)

#define STDIO_F_COUNTERS \
    /* timestamp of the first call to function 'bar' */\
    X(STDIO_F_BAR_TIMESTAMP) \
    /* timer indicating duration of last call to 'bar' */\
    X(STDIO_F_BAR_DURATION) \
    /* end of counters */\
    X(STDIO_F_NUM_INDICES)

#define X(a) a,
/* integer counters for the "STDIO" example module */
enum darshan_stdio_indices
{
    STDIO_COUNTERS
};

/* floating point counters for the "STDIO" example module */
enum darshan_stdio_f_indices
{
    STDIO_F_COUNTERS
};
#undef X

/* the darshan_stdio_record structure encompasses the high-level data/counters
 * which would actually be logged to file by Darshan for the "STDIO"
 * module. This logs the following data for each record:
 *      - a corresponding Darshan record identifier
 *      - the rank of the process responsible for the record
 *      - integer I/O counters (operation counts, I/O sizes, etc.)
 *      - floating point I/O counters (timestamps, cumulative timers, etc.)
 */
struct darshan_stdio_record
{
    darshan_record_id f_id;
    int64_t rank;
    int64_t counters[STDIO_NUM_INDICES];
    double fcounters[STDIO_F_NUM_INDICES];
};

#endif /* __DARSHAN_STDIO_LOG_FORMAT_H */
