/*
 *  (C) 2020 by Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
 */

#ifndef __DARSHAN_DFS_LOG_FORMAT_H
#define __DARSHAN_DFS_LOG_FORMAT_H

/* current DFS log format version */
#define DARSHAN_DFS_VER 1

#define DFS_COUNTERS \
    /* count of dfs opens */\
    X(DFS_OPENS) \
    /* count of dfs global (global2local) opens */\
    X(DFS_GLOBAL_OPENS) \
    /* count of dfs lookups */\
    X(DFS_LOOKUPS) \
    /* count of dfs dups */\
    X(DFS_DUPS) \
    /* count of dfs reads */\
    X(DFS_READS) \
    /* count of dfs readxs */\
    X(DFS_READXS) \
    /* count of dfs writes */\
    X(DFS_WRITES) \
    /* count of dfs writexs */\
    X(DFS_WRITEXS) \
    /* count of total non-blocking dfs reads (including read/readx) */\
    X(DFS_NB_READS) \
    /* count of total non-blocking dfs writes (including write/writex) */\
    X(DFS_NB_WRITES) \
    /* count of dfs get sizes */\
    X(DFS_GET_SIZES) \
    /* count of dfs punches */\
    X(DFS_PUNCHES) \
    /* count of dfs removes */\
    X(DFS_REMOVES) \
    /* count of dfs stats */\
    X(DFS_STATS) \
    /* total bytes read */\
    X(DFS_BYTES_READ) \
    /* total bytes written */\
    X(DFS_BYTES_WRITTEN) \
    /* number of times switched between read and write */\
    X(DFS_RW_SWITCHES) \
    X(DFS_MAX_READ_TIME_SIZE) \
    X(DFS_MAX_WRITE_TIME_SIZE) \
    /* buckets for dfs read size ranges */\
    X(DFS_SIZE_READ_0_100) \
    X(DFS_SIZE_READ_100_1K) \
    X(DFS_SIZE_READ_1K_10K) \
    X(DFS_SIZE_READ_10K_100K) \
    X(DFS_SIZE_READ_100K_1M) \
    X(DFS_SIZE_READ_1M_4M) \
    X(DFS_SIZE_READ_4M_10M) \
    X(DFS_SIZE_READ_10M_100M) \
    X(DFS_SIZE_READ_100M_1G) \
    X(DFS_SIZE_READ_1G_PLUS) \
    /* buckets for dfs write size ranges */\
    X(DFS_SIZE_WRITE_0_100) \
    X(DFS_SIZE_WRITE_100_1K) \
    X(DFS_SIZE_WRITE_1K_10K) \
    X(DFS_SIZE_WRITE_10K_100K) \
    X(DFS_SIZE_WRITE_100K_1M) \
    X(DFS_SIZE_WRITE_1M_4M) \
    X(DFS_SIZE_WRITE_4M_10M) \
    X(DFS_SIZE_WRITE_10M_100M) \
    X(DFS_SIZE_WRITE_100M_1G) \
    X(DFS_SIZE_WRITE_1G_PLUS) \
    /* the four most frequently appearing access sizes */\
    X(DFS_ACCESS1_ACCESS) \
    X(DFS_ACCESS2_ACCESS) \
    X(DFS_ACCESS3_ACCESS) \
    X(DFS_ACCESS4_ACCESS) \
    /* count of each of the most frequent access sizes */\
    X(DFS_ACCESS1_COUNT) \
    X(DFS_ACCESS2_COUNT) \
    X(DFS_ACCESS3_COUNT) \
    X(DFS_ACCESS4_COUNT) \
    /* dfs file chunk size */\
    X(DFS_CHUNK_SIZE) \
    /* rank and number of bytes moved for fastest/slowest ranks */\
    X(DFS_FASTEST_RANK) \
    X(DFS_FASTEST_RANK_BYTES) \
    X(DFS_SLOWEST_RANK) \
    X(DFS_SLOWEST_RANK_BYTES) \
    /* end of counters */\
    X(DFS_NUM_INDICES)

#define DFS_F_COUNTERS \
    /* timestamp of first open */\
    X(DFS_F_OPEN_START_TIMESTAMP) \
    /* timestamp of first read */\
    X(DFS_F_READ_START_TIMESTAMP) \
    /* timestamp of first write */\
    X(DFS_F_WRITE_START_TIMESTAMP) \
    /* timestamp of first close */\
    X(DFS_F_CLOSE_START_TIMESTAMP) \
    /* timestamp of last open */\
    X(DFS_F_OPEN_END_TIMESTAMP) \
    /* timestamp of last read */\
    X(DFS_F_READ_END_TIMESTAMP) \
    /* timestamp of last write */\
    X(DFS_F_WRITE_END_TIMESTAMP) \
    /* timestamp of last close */\
    X(DFS_F_CLOSE_END_TIMESTAMP) \
    /* cumulative dfs read time */\
    X(DFS_F_READ_TIME) \
    /* cumulative dfs write time */\
    X(DFS_F_WRITE_TIME) \
    /* cumulative dfs meta time */\
    X(DFS_F_META_TIME) \
    /* maximum dfs read duration */\
    X(DFS_F_MAX_READ_TIME) \
    /* maximum dfs write duration */\
    X(DFS_F_MAX_WRITE_TIME) \
    /* total i/o and meta time consumed for fastest/slowest ranks */\
    X(DFS_F_FASTEST_RANK_TIME) \
    X(DFS_F_SLOWEST_RANK_TIME) \
    /* end of counters */\
    X(DFS_F_NUM_INDICES)

#define X(a) a,
/* integer statistics for DFS file records */
enum darshan_dfs_indices
{
    DFS_COUNTERS
};

/* floating point statistics for DFS file records */
enum darshan_dfs_f_indices
{
   DFS_F_COUNTERS
};
#undef X

/* file record structure for DFS files. a record is created and stored for
 * every DFS file opened by the original application. For the DFS module,
 * the record includes:
 *      - a darshan_base_record structure, which contains the record id & rank
 *      - integer file I/O statistics (open, read/write counts, etc)
 *      - floating point I/O statistics (timestamps, cumulative timers, etc.)
 *      - pool and container UUIDs
 */
struct darshan_dfs_file
{
    struct darshan_base_record base_rec;
    int64_t counters[DFS_NUM_INDICES];
    double fcounters[DFS_F_NUM_INDICES];
    unsigned char pool_uuid[16];
    unsigned char cont_uuid[16];
};

#endif /* __DARSHAN_DFS_LOG_FORMAT_H */
