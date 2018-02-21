/*
 * Copyright (C) 2015 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

#ifndef __DARSHAN_MDHIM_LOG_FORMAT_H
#define __DARSHAN_MDHIM_LOG_FORMAT_H

/* current log format version, to support backwards compatibility */
#define DARSHAN_MDHIM_VER 1

#define MDHIM_COUNTERS \
    /* number of 'put' function calls */\
    X(MDHIM_PUTS) \
    /* larget payload for a 'put' */ \
    X(MDHIM_PUT_MAX_SIZE)\
    /* number of 'get' function calls */\
    X(MDHIM_GETS) \
    /* largest get */ \
    X(MDHIM_GET_MAX_SIZE) \
    /* end of counters */ \
    X(MDHIM_NUM_INDICES)

#define MDHIM_F_COUNTERS \
    /* timestamp of the first call to a 'put' function */\
    X(MDHIM_F_PUT_TIMESTAMP) \
    /* timer indicating duration of call to 'foo' with max MDHIM_PUT_MAX_DAT value */\
    X(MDHIM_F_PUT_MAX_DURATION) \
    X(MDHIM_F_GET_TIMESTAMP) \
    X(MDHIM_F_GET_MAX_DURATION) \
    /* end of counters */\
    X(MDHIM_F_NUM_INDICES)

#define X(a) a,
/* integer counters for the "NULL" example module */
enum darshan_mdhim_indices
{
    MDHIM_COUNTERS
};

/* floating point counters for the "NULL" example module */
enum darshan_mdhim_f_indices
{
    MDHIM_F_COUNTERS
};
#undef X

/* the darshan_mdhim_record structure encompasses the high-level data/counters
 * which would actually be logged to file by Darshan for the "MDHIM"
 * module. This implementation logs the following data for each
 * record:
 *      - a darshan_base_record structure, which contains the record id & rank
 *      - integer I/O counters (operation counts, I/O sizes, etc.)
 *      - floating point I/O counters (timestamps, cumulative timers, etc.)
 */
struct darshan_mdhim_record
{
    struct darshan_base_record base_rec;
    int64_t counters[MDHIM_NUM_INDICES];
    double fcounters[MDHIM_F_NUM_INDICES];
};

#endif /* __DARSHAN_MDHIM_LOG_FORMAT_H */
