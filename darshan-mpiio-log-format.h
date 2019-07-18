/*
 * Copyright (C) 2015 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

#ifndef __DARSHAN_MPIIO_LOG_FORMAT_H
#define __DARSHAN_MPIIO_LOG_FORMAT_H

/* current MPI-IO log format version */
#define DARSHAN_MPIIO_VER 3

/* TODO: maybe use a counter to track cases in which a derived datatype is used? */

#define MPIIO_COUNTERS \
    /* count of MPI independent opens */\
    X(MPIIO_INDEP_OPENS) \
    /* count of MPI collective opens */\
    X(MPIIO_COLL_OPENS) \
    /* count of MPI independent reads */\
    X(MPIIO_INDEP_READS) \
    /* count of MPI independent writes */\
    X(MPIIO_INDEP_WRITES) \
    /* count of MPI collective reads */\
    X(MPIIO_COLL_READS) \
    /* count of MPI collective writes */\
    X(MPIIO_COLL_WRITES) \
    /* count of MPI split collective reads */\
    X(MPIIO_SPLIT_READS) \
    /* count of MPI split collective writes */\
    X(MPIIO_SPLIT_WRITES) \
    /* count of MPI nonblocking reads */\
    X(MPIIO_NB_READS) \
    /* count of MPI nonblocking writes */\
    X(MPIIO_NB_WRITES) \
    /* count of MPI file syncs */\
    X(MPIIO_SYNCS) \
    /* count of MPI hints used */\
    X(MPIIO_HINTS) \
    /* count of MPI set view calls */\
    X(MPIIO_VIEWS) \
    /* MPI-IO access mode of the file */\
    X(MPIIO_MODE) \
    /* total bytes read at MPI-IO layer */\
    X(MPIIO_BYTES_READ) \
    /* total bytes written at MPI-IO layer */\
    X(MPIIO_BYTES_WRITTEN) \
    /* number of times switching between MPI read and write */\
    X(MPIIO_RW_SWITCHES) \
    /* sizes of the maximum read/write operations */\
    X(MPIIO_MAX_READ_TIME_SIZE) \
    X(MPIIO_MAX_WRITE_TIME_SIZE) \
    /* buckets for MPI read size ranges */\
    X(MPIIO_SIZE_READ_AGG_0_100) \
    X(MPIIO_SIZE_READ_AGG_100_1K) \
    X(MPIIO_SIZE_READ_AGG_1K_10K) \
    X(MPIIO_SIZE_READ_AGG_10K_100K) \
    X(MPIIO_SIZE_READ_AGG_100K_1M) \
    X(MPIIO_SIZE_READ_AGG_1M_4M) \
    X(MPIIO_SIZE_READ_AGG_4M_10M) \
    X(MPIIO_SIZE_READ_AGG_10M_100M) \
    X(MPIIO_SIZE_READ_AGG_100M_1G) \
    X(MPIIO_SIZE_READ_AGG_1G_PLUS) \
    /* buckets for MPI write size ranges */\
    X(MPIIO_SIZE_WRITE_AGG_0_100) \
    X(MPIIO_SIZE_WRITE_AGG_100_1K) \
    X(MPIIO_SIZE_WRITE_AGG_1K_10K) \
    X(MPIIO_SIZE_WRITE_AGG_10K_100K) \
    X(MPIIO_SIZE_WRITE_AGG_100K_1M) \
    X(MPIIO_SIZE_WRITE_AGG_1M_4M) \
    X(MPIIO_SIZE_WRITE_AGG_4M_10M) \
    X(MPIIO_SIZE_WRITE_AGG_10M_100M) \
    X(MPIIO_SIZE_WRITE_AGG_100M_1G) \
    X(MPIIO_SIZE_WRITE_AGG_1G_PLUS) \
    /* the four most frequently appearing MPI access sizes */\
    X(MPIIO_ACCESS1_ACCESS) \
    X(MPIIO_ACCESS2_ACCESS) \
    X(MPIIO_ACCESS3_ACCESS) \
    X(MPIIO_ACCESS4_ACCESS) \
    /* count of each of the most frequent MPI access sizes */\
    X(MPIIO_ACCESS1_COUNT) \
    X(MPIIO_ACCESS2_COUNT) \
    X(MPIIO_ACCESS3_COUNT) \
    X(MPIIO_ACCESS4_COUNT) \
    /* rank and number of bytes moved for fastest/slowest ranks */\
    X(MPIIO_FASTEST_RANK) \
    X(MPIIO_FASTEST_RANK_BYTES) \
    X(MPIIO_SLOWEST_RANK) \
    X(MPIIO_SLOWEST_RANK_BYTES) \
    /* end of counters */\
    X(MPIIO_NUM_INDICES)

#define MPIIO_F_COUNTERS \
    /* timestamp of first open */\
    X(MPIIO_F_OPEN_START_TIMESTAMP) \
    /* timestamp of first read */\
    X(MPIIO_F_READ_START_TIMESTAMP) \
    /* timestamp of first write */\
    X(MPIIO_F_WRITE_START_TIMESTAMP) \
    /* timestamp of first close */\
    X(MPIIO_F_CLOSE_START_TIMESTAMP) \
    /* timestamp of last open */\
    X(MPIIO_F_OPEN_END_TIMESTAMP) \
    /* timestamp of last read */\
    X(MPIIO_F_READ_END_TIMESTAMP) \
    /* timestamp of last write */\
    X(MPIIO_F_WRITE_END_TIMESTAMP) \
    /* timestamp of last close */\
    X(MPIIO_F_CLOSE_END_TIMESTAMP) \
    /* cumulative MPI-IO read time */\
    X(MPIIO_F_READ_TIME) \
    /* cumulative MPI-IO write time */\
    X(MPIIO_F_WRITE_TIME) \
    /* cumulative MPI-IO meta time */\
    X(MPIIO_F_META_TIME) \
    /* maximum MPI-IO read duration */\
    X(MPIIO_F_MAX_READ_TIME) \
    /* maximum MPI-IO write duration */\
    X(MPIIO_F_MAX_WRITE_TIME) \
    /* total i/o and meta time for fastest/slowest ranks */\
    X(MPIIO_F_FASTEST_RANK_TIME) \
    X(MPIIO_F_SLOWEST_RANK_TIME) \
    /* variance of total i/o time and bytes moved across all ranks */\
    /* NOTE: for shared records only */\
    X(MPIIO_F_VARIANCE_RANK_TIME) \
    X(MPIIO_F_VARIANCE_RANK_BYTES) \
    /* end of counters*/\
    X(MPIIO_F_NUM_INDICES)

#define X(a) a,
/* integer statistics for MPI-IO file records */
enum darshan_mpiio_indices
{
    MPIIO_COUNTERS
};

/* floating point statistics for MPI-IO file records */
enum darshan_mpiio_f_indices
{
    MPIIO_F_COUNTERS
};
#undef X

/* file record structure for MPI-IO files. a record is created and stored for
 * every MPI-IO file opened by the original application. For the MPI-IO module,
 * the record includes:
 *      - a darshan_base_record structure, which contains the record id & rank
 *      - integer file I/O statistics (open, read/write counts, etc)
 *      - floating point I/O statistics (timestamps, cumulative timers, etc.)
 */
struct darshan_mpiio_file
{
    struct darshan_base_record base_rec;
    int64_t counters[MPIIO_NUM_INDICES];
    double fcounters[MPIIO_F_NUM_INDICES];
};

#endif /* __DARSHAN_MPIIO_LOG_FORMAT_H */
