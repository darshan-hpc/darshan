/*
 *  (C) 2009 by Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
 */

#ifndef __DARSHAN_POSIX_LOG_FORMAT_H
#define __DARSHAN_POSIX_LOG_FORMAT_H

#include "darshan-log-format.h"

/* integer statistics for POSIX file records */
enum darshan_posix_indices
{
    POSIX_OPENS,              /* count of posix opens */
    POSIX_READS,              /* count of posix reads */
    POSIX_WRITES,             /* count of posix writes */
    POSIX_SEEKS,              /* count of posix seeks */
    POSIX_STATS,              /* count of posix stat/lstat/fstats */
#if 0
    POSIX_MMAPS,              /* count of posix mmaps */
#endif
    POSIX_FOPENS,             /* count of posix fopens */
    POSIX_FREADS,             /* count of posix freads */
    POSIX_FWRITES,            /* count of posix fwrites */
    POSIX_FSEEKS,             /* count of posix fseeks */
    POSIX_FSYNCS,             /* count of posix fsyncs */
    POSIX_FDSYNCS,            /* count of posix fdatasyncs */
    POSIX_MODE,               /* mode of file */
    POSIX_BYTES_READ,         /* total bytes read */
    POSIX_BYTES_WRITTEN,      /* total bytes written */
    POSIX_MAX_BYTE_READ,      /* highest offset byte read */
    POSIX_MAX_BYTE_WRITTEN,   /* highest offset byte written */
    POSIX_CONSEC_READS,       /* count of consecutive reads */
    POSIX_CONSEC_WRITES,      /* count of consecutive writes */ 
    POSIX_SEQ_READS,          /* count of sequential reads */
    POSIX_SEQ_WRITES,         /* count of sequential writes */
    POSIX_RW_SWITCHES,        /* number of times switched between read and write */
#if 0
    MEM_NOT_ALIGNED,          /* count of accesses not mem aligned */
    MEM_ALIGNMENT,            /* mem alignment in bytes */
    FILE_NOT_ALIGNED,         /* count of accesses not file aligned */
#endif
    FILE_ALIGNMENT,           /* file alignment in bytes */
    POSIX_MAX_READ_TIME_SIZE,
    POSIX_MAX_WRITE_TIME_SIZE,
#if 0
    /* buckets */
    SIZE_READ_0_100,          /* count of posix read size ranges */
    SIZE_READ_100_1K,
    SIZE_READ_1K_10K,
    SIZE_READ_10K_100K,
    SIZE_READ_100K_1M,
    SIZE_READ_1M_4M,
    SIZE_READ_4M_10M,
    SIZE_READ_10M_100M,
    SIZE_READ_100M_1G,
    SIZE_READ_1G_PLUS,
    /* buckets */
    SIZE_WRITE_0_100,         /* count of posix write size ranges */
    SIZE_WRITE_100_1K,
    SIZE_WRITE_1K_10K,
    SIZE_WRITE_10K_100K,
    SIZE_WRITE_100K_1M,
    SIZE_WRITE_1M_4M,
    SIZE_WRITE_4M_10M,
    SIZE_WRITE_10M_100M,
    SIZE_WRITE_100M_1G,
    SIZE_WRITE_1G_PLUS,
    /* counters */
    STRIDE1_STRIDE,           /* the four most frequently appearing strides */
    STRIDE2_STRIDE,
    STRIDE3_STRIDE,
    STRIDE4_STRIDE,
    STRIDE1_COUNT,            /* count of each of the most frequent strides */
    STRIDE2_COUNT,
    STRIDE3_COUNT,
    STRIDE4_COUNT,
    ACCESS1_ACCESS,           /* the four most frequently appearing access sizes */
    ACCESS2_ACCESS,
    ACCESS3_ACCESS,
    ACCESS4_ACCESS,
    ACCESS1_COUNT,            /* count of each of the most frequent access sizes */
    ACCESS2_COUNT,
    ACCESS3_COUNT,
    ACCESS4_COUNT,
    DEVICE,                   /* device id reported by stat */
#endif
    SIZE_AT_OPEN,
#if 0
    FASTEST_RANK,
    FASTEST_RANK_BYTES,
    SLOWEST_RANK,
    SLOWEST_RANK_BYTES,
#endif

    POSIX_NUM_INDICES,
};

/* floating point statistics for POSIX file records */
enum darshan_posix_f_indices
{
    /* NOTE: adjust cp_normalize_timestamps() function if any TIMESTAMPS are
     * added or modified in this list
     */
    POSIX_F_OPEN_TIMESTAMP = 0,    /* timestamp of first open */
    POSIX_F_READ_START_TIMESTAMP,  /* timestamp of first read */
    POSIX_F_WRITE_START_TIMESTAMP, /* timestamp of first write */
    POSIX_F_READ_END_TIMESTAMP,    /* timestamp of last read */
    POSIX_F_WRITE_END_TIMESTAMP,   /* timestamp of last write */
    POSIX_F_CLOSE_TIMESTAMP,       /* timestamp of last close */
    POSIX_F_READ_TIME,             /* cumulative posix read time */
    POSIX_F_WRITE_TIME,            /* cumulative posix write time */
    POSIX_F_META_TIME,             /* cumulative posix meta time */
    POSIX_F_MAX_READ_TIME,
    POSIX_F_MAX_WRITE_TIME,
#if 0
    /* Total I/O and meta time consumed by fastest and slowest ranks, 
     * reported in either MPI or POSIX time depending on how the file 
     * was accessed.
     */
    F_FASTEST_RANK_TIME,
    F_SLOWEST_RANK_TIME,
    F_VARIANCE_RANK_TIME,
    F_VARIANCE_RANK_BYTES,
#endif

    POSIX_F_NUM_INDICES,
};

/* file record structure for POSIX files. a record is created and stored for
 * every POSIX file opened by the original application. For the POSIX module,
 * the record includes:
 *      - a corresponding record identifier (created by hashing the file path)
 *      - the rank of the process which opened the file (-1 for shared files)
 *      - integer file I/O statistics (open, read/write counts, etc)
 *      - floating point I/O statistics (timestamps, cumulative timers, etc.)
 */
struct darshan_posix_file
{
    darshan_record_id f_id;
    int64_t rank;
    int64_t counters[POSIX_NUM_INDICES];
    double fcounters[POSIX_F_NUM_INDICES];
};

#endif /* __DARSHAN_POSIX_LOG_FORMAT_H */
