/*
 *  (C) 2025 by Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
 */

#ifndef __DARSHAN_GDS_LOG_FORMAT_H
#define __DARSHAN_GDS_LOG_FORMAT_H

/* current GDS (NVIDIA GPUDirect Storage, cuFile API) log format version */
#define DARSHAN_GDS_VER 1

#define GDS_COUNTERS \
    /* count of cuFileHandleRegister calls */\
    X(GDS_HANDLE_REGISTERS) \
    /* count of cuFileHandleDeregister calls */\
    X(GDS_HANDLE_DEREGISTERS) \
    /* count of cuFileRead calls */\
    X(GDS_READS) \
    /* count of cuFileWrite calls */\
    X(GDS_WRITES) \
    /* count of cuFileRead calls that returned an error */\
    X(GDS_READ_ERRORS) \
    /* count of cuFileWrite calls that returned an error */\
    X(GDS_WRITE_ERRORS) \
    /* count of read operations submitted via cuFileBatchIOSubmit */\
    X(GDS_BATCH_READS) \
    /* count of write operations submitted via cuFileBatchIOSubmit */\
    X(GDS_BATCH_WRITES) \
    /* count of cuFileReadAsync calls */\
    X(GDS_ASYNC_READS) \
    /* count of cuFileWriteAsync calls */\
    X(GDS_ASYNC_WRITES) \
    /* total bytes read by cuFileRead */\
    X(GDS_BYTES_READ) \
    /* total bytes written by cuFileWrite */\
    X(GDS_BYTES_WRITTEN) \
    /* total bytes requested by batch read operations */\
    X(GDS_BATCH_BYTES_READ) \
    /* total bytes requested by batch write operations */\
    X(GDS_BATCH_BYTES_WRITTEN) \
    /* total bytes requested by async reads (upper bound; completion not tracked) */\
    X(GDS_ASYNC_BYTES_REQ_READ) \
    /* total bytes requested by async writes (upper bound; completion not tracked) */\
    X(GDS_ASYNC_BYTES_REQ_WRITTEN) \
    /* highest offset byte read */\
    X(GDS_MAX_BYTE_READ) \
    /* highest offset byte written */\
    X(GDS_MAX_BYTE_WRITTEN) \
    /* count of consecutive reads */\
    X(GDS_CONSEC_READS) \
    /* count of consecutive writes */\
    X(GDS_CONSEC_WRITES) \
    /* count of sequential reads */\
    X(GDS_SEQ_READS) \
    /* count of sequential writes */\
    X(GDS_SEQ_WRITES) \
    /* number of times switched between read and write */\
    X(GDS_RW_SWITCHES) \
    /* size of the slowest read */\
    X(GDS_MAX_READ_TIME_SIZE) \
    /* size of the slowest write */\
    X(GDS_MAX_WRITE_TIME_SIZE) \
    /* buckets for read size ranges */\
    X(GDS_SIZE_READ_0_100) \
    X(GDS_SIZE_READ_100_1K) \
    X(GDS_SIZE_READ_1K_10K) \
    X(GDS_SIZE_READ_10K_100K) \
    X(GDS_SIZE_READ_100K_1M) \
    X(GDS_SIZE_READ_1M_4M) \
    X(GDS_SIZE_READ_4M_10M) \
    X(GDS_SIZE_READ_10M_100M) \
    X(GDS_SIZE_READ_100M_1G) \
    X(GDS_SIZE_READ_1G_PLUS) \
    /* buckets for write size ranges */\
    X(GDS_SIZE_WRITE_0_100) \
    X(GDS_SIZE_WRITE_100_1K) \
    X(GDS_SIZE_WRITE_1K_10K) \
    X(GDS_SIZE_WRITE_10K_100K) \
    X(GDS_SIZE_WRITE_100K_1M) \
    X(GDS_SIZE_WRITE_1M_4M) \
    X(GDS_SIZE_WRITE_4M_10M) \
    X(GDS_SIZE_WRITE_10M_100M) \
    X(GDS_SIZE_WRITE_100M_1G) \
    X(GDS_SIZE_WRITE_1G_PLUS) \
    /* the four most frequently appearing access sizes */\
    X(GDS_ACCESS1_ACCESS) \
    X(GDS_ACCESS2_ACCESS) \
    X(GDS_ACCESS3_ACCESS) \
    X(GDS_ACCESS4_ACCESS) \
    /* count of each of the most frequent access sizes */\
    X(GDS_ACCESS1_COUNT) \
    X(GDS_ACCESS2_COUNT) \
    X(GDS_ACCESS3_COUNT) \
    X(GDS_ACCESS4_COUNT) \
    /* rank and number of bytes moved for fastest/slowest ranks */\
    X(GDS_FASTEST_RANK) \
    X(GDS_FASTEST_RANK_BYTES) \
    X(GDS_SLOWEST_RANK) \
    X(GDS_SLOWEST_RANK_BYTES) \
    /* end of counters */\
    X(GDS_NUM_INDICES)

/* 'open' means cuFileHandleRegister, 'close' means cuFileHandleDeregister */
#define GDS_F_COUNTERS \
    X(GDS_F_OPEN_START_TIMESTAMP) \
    X(GDS_F_READ_START_TIMESTAMP) \
    X(GDS_F_WRITE_START_TIMESTAMP) \
    X(GDS_F_CLOSE_START_TIMESTAMP) \
    X(GDS_F_OPEN_END_TIMESTAMP) \
    X(GDS_F_READ_END_TIMESTAMP) \
    X(GDS_F_WRITE_END_TIMESTAMP) \
    X(GDS_F_CLOSE_END_TIMESTAMP) \
    X(GDS_F_READ_TIME) \
    X(GDS_F_WRITE_TIME) \
    X(GDS_F_META_TIME) \
    X(GDS_F_MAX_READ_TIME) \
    X(GDS_F_MAX_WRITE_TIME) \
    X(GDS_F_FASTEST_RANK_TIME) \
    X(GDS_F_SLOWEST_RANK_TIME) \
    /* end of counters */\
    X(GDS_F_NUM_INDICES)

#define X(a) a,
/* integer statistics for GDS file records */
enum darshan_gds_indices
{
    GDS_COUNTERS
};

/* floating point statistics for GDS file records */
enum darshan_gds_f_indices
{
    GDS_F_COUNTERS
};
#undef X

/* file record structure for GDS files. The record id is the same as the
 * POSIX record id for the underlying file.
 */
struct darshan_gds_file
{
    struct darshan_base_record base_rec;
    int64_t counters[GDS_NUM_INDICES];
    double fcounters[GDS_F_NUM_INDICES];
};

#endif /* __DARSHAN_GDS_LOG_FORMAT_H */
