/*
 *  (C) 2009 by Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
 */

#ifndef __DARSHAN_POSIX_LOG_FORMAT_H
#define __DARSHAN_POSIX_LOG_FORMAT_H

/* current POSIX log format version */
#define DARSHAN_POSIX_VER 4

#define POSIX_COUNTERS \
    /* count of posix opens (INCLUDING fileno and dup operations) */\
    X(POSIX_OPENS) \
    /* count of number of filenos */\
    X(POSIX_FILENOS) \
    /* count of number of dups */\
    X(POSIX_DUPS) \
    /* count of posix reads */\
    X(POSIX_READS) \
    /* count of posix writes */\
    X(POSIX_WRITES) \
    /* count of posix seeks */\
    X(POSIX_SEEKS) \
    /* count of posix stat/lstat/fstats */\
    X(POSIX_STATS) \
    /* count of posix mmaps */\
    X(POSIX_MMAPS) \
    /* count of posix fsyncs */\
    X(POSIX_FSYNCS) \
    /* count of posix fdatasyncs */\
    X(POSIX_FDSYNCS) \
    /* count of renames (as source file) */\
    X(POSIX_RENAME_SOURCES) \
    /* count of renames (as target file) */\
    X(POSIX_RENAME_TARGETS) \
    /* Darshan record ID of first rename source */\
    X(POSIX_RENAMED_FROM) \
    /* mode of file */\
    X(POSIX_MODE) \
    /* total bytes read */\
    X(POSIX_BYTES_READ) \
    /* total bytes written */\
    X(POSIX_BYTES_WRITTEN) \
    /* highest offset byte read */\
    X(POSIX_MAX_BYTE_READ) \
    /* highest offset byte written */\
    X(POSIX_MAX_BYTE_WRITTEN) \
    /* count of consecutive reads */\
    X(POSIX_CONSEC_READS) \
    /* count of consecutive writes */\
    X(POSIX_CONSEC_WRITES) \
    /* count of sequential reads */\
    X(POSIX_SEQ_READS) \
    /* count of sequential writes */\
    X(POSIX_SEQ_WRITES) \
    /* number of times switched between read and write */\
    X(POSIX_RW_SWITCHES) \
    /* count of accesses not mem aligned */\
    X(POSIX_MEM_NOT_ALIGNED) \
    /* mem alignment in bytes */\
    X(POSIX_MEM_ALIGNMENT) \
    /* count of accesses not file aligned */\
    X(POSIX_FILE_NOT_ALIGNED) \
    /* file alignment in bytes */\
    X(POSIX_FILE_ALIGNMENT) \
    X(POSIX_MAX_READ_TIME_SIZE) \
    X(POSIX_MAX_WRITE_TIME_SIZE) \
    /* buckets for POSIX read size ranges */\
    X(POSIX_SIZE_READ_0_100) \
    X(POSIX_SIZE_READ_100_1K) \
    X(POSIX_SIZE_READ_1K_10K) \
    X(POSIX_SIZE_READ_10K_100K) \
    X(POSIX_SIZE_READ_100K_1M) \
    X(POSIX_SIZE_READ_1M_4M) \
    X(POSIX_SIZE_READ_4M_10M) \
    X(POSIX_SIZE_READ_10M_100M) \
    X(POSIX_SIZE_READ_100M_1G) \
    X(POSIX_SIZE_READ_1G_PLUS) \
    /* buckets for POSIX write size ranges */\
    X(POSIX_SIZE_WRITE_0_100) \
    X(POSIX_SIZE_WRITE_100_1K) \
    X(POSIX_SIZE_WRITE_1K_10K) \
    X(POSIX_SIZE_WRITE_10K_100K) \
    X(POSIX_SIZE_WRITE_100K_1M) \
    X(POSIX_SIZE_WRITE_1M_4M) \
    X(POSIX_SIZE_WRITE_4M_10M) \
    X(POSIX_SIZE_WRITE_10M_100M) \
    X(POSIX_SIZE_WRITE_100M_1G) \
    X(POSIX_SIZE_WRITE_1G_PLUS) \
    /* the four most frequently appearing strides */\
    X(POSIX_STRIDE1_STRIDE) \
    X(POSIX_STRIDE2_STRIDE) \
    X(POSIX_STRIDE3_STRIDE) \
    X(POSIX_STRIDE4_STRIDE) \
    /* count of each of the most frequent strides */\
    X(POSIX_STRIDE1_COUNT) \
    X(POSIX_STRIDE2_COUNT) \
    X(POSIX_STRIDE3_COUNT) \
    X(POSIX_STRIDE4_COUNT) \
    /* the four most frequently appearing access sizes */\
    X(POSIX_ACCESS1_ACCESS) \
    X(POSIX_ACCESS2_ACCESS) \
    X(POSIX_ACCESS3_ACCESS) \
    X(POSIX_ACCESS4_ACCESS) \
    /* count of each of the most frequent access sizes */\
    X(POSIX_ACCESS1_COUNT) \
    X(POSIX_ACCESS2_COUNT) \
    X(POSIX_ACCESS3_COUNT) \
    X(POSIX_ACCESS4_COUNT) \
    /* rank and number of bytes moved for fastest/slowest ranks */\
    X(POSIX_FASTEST_RANK) \
    X(POSIX_FASTEST_RANK_BYTES) \
    X(POSIX_SLOWEST_RANK) \
    X(POSIX_SLOWEST_RANK_BYTES) \
    /* end of counters */\
    X(POSIX_NUM_INDICES)

#define POSIX_F_COUNTERS \
    /* timestamp of first open */\
    X(POSIX_F_OPEN_START_TIMESTAMP) \
    /* timestamp of first read */\
    X(POSIX_F_READ_START_TIMESTAMP) \
    /* timestamp of first write */\
    X(POSIX_F_WRITE_START_TIMESTAMP) \
    /* timestamp of first close */\
    X(POSIX_F_CLOSE_START_TIMESTAMP) \
    /* timestamp of last open */\
    X(POSIX_F_OPEN_END_TIMESTAMP) \
    /* timestamp of last read */\
    X(POSIX_F_READ_END_TIMESTAMP) \
    /* timestamp of last write */\
    X(POSIX_F_WRITE_END_TIMESTAMP) \
    /* timestamp of last close */\
    X(POSIX_F_CLOSE_END_TIMESTAMP) \
    /* cumulative posix read time */\
    X(POSIX_F_READ_TIME) \
    /* cumulative posix write time */\
    X(POSIX_F_WRITE_TIME) \
    /* cumulative posix meta time */\
    X(POSIX_F_META_TIME) \
    /* maximum posix read duration */\
    X(POSIX_F_MAX_READ_TIME) \
    /* maximum posix write duration */\
    X(POSIX_F_MAX_WRITE_TIME) \
    /* total i/o and meta time consumed for fastest/slowest ranks */\
    X(POSIX_F_FASTEST_RANK_TIME) \
    X(POSIX_F_SLOWEST_RANK_TIME) \
    /* variance of total i/o time and bytes moved across all ranks */\
    /* NOTE: for shared records only */\
    X(POSIX_F_VARIANCE_RANK_TIME) \
    X(POSIX_F_VARIANCE_RANK_BYTES) \
    /* end of counters */\
    X(POSIX_F_NUM_INDICES)

#define X(a) a,
/* integer statistics for POSIX file records */
enum darshan_posix_indices
{
    POSIX_COUNTERS
};

/* floating point statistics for POSIX file records */
enum darshan_posix_f_indices
{
    POSIX_F_COUNTERS
};
#undef X

/* file record structure for POSIX files. a record is created and stored for
 * every POSIX file opened by the original application. For the POSIX module,
 * the record includes:
 *      - a darshan_base_record structure, which contains the record id & rank
 *      - integer file I/O statistics (open, read/write counts, etc)
 *      - floating point I/O statistics (timestamps, cumulative timers, etc.)
 */
struct darshan_posix_file
{
    struct darshan_base_record base_rec;
    int64_t counters[POSIX_NUM_INDICES];
    double fcounters[POSIX_F_NUM_INDICES];
};

#endif /* __DARSHAN_POSIX_LOG_FORMAT_H */
