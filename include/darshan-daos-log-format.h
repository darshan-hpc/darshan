/*
 *  (C) 2020 by Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
 */

#ifndef __DARSHAN_DAOS_LOG_FORMAT_H
#define __DARSHAN_DAOS_LOG_FORMAT_H

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
    /* end of counters */\
    X(DFS_NUM_INDICES)

#define DFS_F_COUNTERS \
    /* timestamp of first open */\
    X(DFS_F_OPEN_START_TIMESTAMP) \
    /* timestamp of first close */\
    X(DFS_F_CLOSE_START_TIMESTAMP) \
    /* timestamp of last open */\
    X(DFS_F_OPEN_END_TIMESTAMP) \
    /* timestamp of last close */\
    X(DFS_F_CLOSE_END_TIMESTAMP) \
    /* cumulative dfs meta time */\
    X(DFS_F_META_TIME) \
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
 */
struct darshan_dfs_file
{
    struct darshan_base_record base_rec;
    int64_t counters[DFS_NUM_INDICES];
    double fcounters[DFS_F_NUM_INDICES];
};

#endif /* __DARSHAN_DFS_LOG_FORMAT_H */
