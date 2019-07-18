/*
 * Copyright (C) 2015 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

#ifndef __DARSHAN_STDIO_LOG_FORMAT_H
#define __DARSHAN_STDIO_LOG_FORMAT_H

/* current log format version, to support backwards compatibility */
#define DARSHAN_STDIO_VER 2

#define STDIO_COUNTERS \
    /* count of fopens (INCLUDING fdopen operations)  */\
    X(STDIO_OPENS) \
    /* count of fdopens */\
    X(STDIO_FDOPENS) \
    /* number of reads */ \
    X(STDIO_READS) \
    /* number of writes */ \
    X(STDIO_WRITES) \
    /* count of seeks */\
    X(STDIO_SEEKS) \
    /* count of flushes */\
    X(STDIO_FLUSHES) \
    /* total bytes written */ \
    X(STDIO_BYTES_WRITTEN) \
    /* total bytes read */ \
    X(STDIO_BYTES_READ) \
    /* maximum byte (offset) read */\
    X(STDIO_MAX_BYTE_READ) \
    /* maximum byte (offset) written */\
    X(STDIO_MAX_BYTE_WRITTEN) \
    /* rank and number of bytes moved for fastest/slowest ranks */\
    X(STDIO_FASTEST_RANK) \
    X(STDIO_FASTEST_RANK_BYTES) \
    X(STDIO_SLOWEST_RANK) \
    X(STDIO_SLOWEST_RANK_BYTES) \
    /* end of counters */\
    X(STDIO_NUM_INDICES)

#define STDIO_F_COUNTERS \
    /* cumulative meta time */\
    X(STDIO_F_META_TIME) \
    /* cumulative write time */\
    X(STDIO_F_WRITE_TIME) \
    /* cumulative read time */\
    X(STDIO_F_READ_TIME) \
    /* timestamp of first open */\
    X(STDIO_F_OPEN_START_TIMESTAMP) \
    /* timestamp of first close */\
    X(STDIO_F_CLOSE_START_TIMESTAMP) \
    /* timestamp of first write */\
    X(STDIO_F_WRITE_START_TIMESTAMP) \
    /* timestamp of first read */\
    X(STDIO_F_READ_START_TIMESTAMP) \
    /* timestamp of last open completion */\
    X(STDIO_F_OPEN_END_TIMESTAMP) \
    /* timestamp of last close completion */\
    X(STDIO_F_CLOSE_END_TIMESTAMP) \
    /* timestamp of last write completion */\
    X(STDIO_F_WRITE_END_TIMESTAMP) \
    /* timestamp of last read completion */\
    X(STDIO_F_READ_END_TIMESTAMP) \
    /* total i/o and meta time consumed for fastest/slowest ranks */\
    X(STDIO_F_FASTEST_RANK_TIME) \
    X(STDIO_F_SLOWEST_RANK_TIME) \
    /* variance of total i/o time and bytes moved across all ranks */\
    /* NOTE: for shared records only */\
    X(STDIO_F_VARIANCE_RANK_TIME) \
    X(STDIO_F_VARIANCE_RANK_BYTES) \
    /* end of counters */\
    X(STDIO_F_NUM_INDICES)

#define X(a) a,
/* integer counters for the "STDIO" module */
enum darshan_stdio_indices
{
    STDIO_COUNTERS
};

/* floating point counters for the "STDIO" module */
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
struct darshan_stdio_file
{
    struct darshan_base_record base_rec;
    int64_t counters[STDIO_NUM_INDICES];
    double fcounters[STDIO_F_NUM_INDICES];
};

#endif /* __DARSHAN_STDIO_LOG_FORMAT_H */
