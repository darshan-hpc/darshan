/*
 *  (C) 2020 by Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
 */

#ifndef __DARSHAN_DAOS_LOG_FORMAT_H
#define __DARSHAN_DAOS_LOG_FORMAT_H

/* current DAOS log format version */
#define DARSHAN_DAOS_VER 1

#define DAOS_COUNTERS \
    /* count of daos obj opens */\
    X(DAOS_OBJ_OPENS) \
    /* count of daos obj fetches */\
    X(DAOS_OBJ_FETCHES) \
    /* count of daos obj updates */\
    X(DAOS_OBJ_UPDATES) \
    /* count of daos obj punches */\
    X(DAOS_OBJ_PUNCHES) \
    /* count of daos obj dkey punches */\
    X(DAOS_OBJ_DKEY_PUNCHES) \
    /* count of daos obj akey punches */\
    X(DAOS_OBJ_AKEY_PUNCHES) \
    /* count of daos obj dkey lists */\
    X(DAOS_OBJ_DKEY_LISTS) \
    /* count of daos obj akey lists */\
    X(DAOS_OBJ_AKEY_LISTS) \
    /* count of daos obj recx lists */\
    X(DAOS_OBJ_RECX_LISTS) \
    /* count of daos array opens */\
    X(DAOS_ARRAY_OPENS) \
    /* count of daos array reads */\
    X(DAOS_ARRAY_READS) \
    /* count of daos array writes */\
    X(DAOS_ARRAY_WRITES) \
    /* count of daos array get sizes */\
    X(DAOS_ARRAY_GET_SIZES) \
    /* count of daos array set sizes */\
    X(DAOS_ARRAY_SET_SIZES) \
    /* count of daos array stats */\
    X(DAOS_ARRAY_STATS) \
    /* count of daos array punches */\
    X(DAOS_ARRAY_PUNCHES) \
    /* count of daos array destroys */\
    X(DAOS_ARRAY_DESTROYS) \
    /* count of daos kv opens */\
    X(DAOS_KV_OPENS) \
    /* count of daos kv gets */\
    X(DAOS_KV_GETS) \
    /* count of daos kv puts */\
    X(DAOS_KV_PUTS) \
    /* count of daos kv removes */\
    X(DAOS_KV_REMOVES) \
    /* count of daos kv lists */\
    X(DAOS_KV_LISTS) \
    /* count of daos kv destroys */\
    X(DAOS_KV_DESTROYS) \
    /* count of daos non-blocking operations */\
    X(DAOS_NB_OPS) \
    /* total bytes read */\
    X(DAOS_BYTES_READ) \
    /* total bytes written */\
    X(DAOS_BYTES_WRITTEN) \
    /* number of times switched between read and write */\
    X(DAOS_RW_SWITCHES) \
    X(DAOS_MAX_READ_TIME_SIZE) \
    X(DAOS_MAX_WRITE_TIME_SIZE) \
    /* buckets for daos read size ranges */\
    X(DAOS_SIZE_READ_0_100) \
    X(DAOS_SIZE_READ_100_1K) \
    X(DAOS_SIZE_READ_1K_10K) \
    X(DAOS_SIZE_READ_10K_100K) \
    X(DAOS_SIZE_READ_100K_1M) \
    X(DAOS_SIZE_READ_1M_4M) \
    X(DAOS_SIZE_READ_4M_10M) \
    X(DAOS_SIZE_READ_10M_100M) \
    X(DAOS_SIZE_READ_100M_1G) \
    X(DAOS_SIZE_READ_1G_PLUS) \
    /* buckets for daos write size ranges */\
    X(DAOS_SIZE_WRITE_0_100) \
    X(DAOS_SIZE_WRITE_100_1K) \
    X(DAOS_SIZE_WRITE_1K_10K) \
    X(DAOS_SIZE_WRITE_10K_100K) \
    X(DAOS_SIZE_WRITE_100K_1M) \
    X(DAOS_SIZE_WRITE_1M_4M) \
    X(DAOS_SIZE_WRITE_4M_10M) \
    X(DAOS_SIZE_WRITE_10M_100M) \
    X(DAOS_SIZE_WRITE_100M_1G) \
    X(DAOS_SIZE_WRITE_1G_PLUS) \
    /* the four most frequently appearing access sizes */\
    X(DAOS_ACCESS1_ACCESS) \
    X(DAOS_ACCESS2_ACCESS) \
    X(DAOS_ACCESS3_ACCESS) \
    X(DAOS_ACCESS4_ACCESS) \
    /* count of each of the most frequent access sizes */\
    X(DAOS_ACCESS1_COUNT) \
    X(DAOS_ACCESS2_COUNT) \
    X(DAOS_ACCESS3_COUNT) \
    X(DAOS_ACCESS4_COUNT) \
    /* daos obj otype id */\
    X(DAOS_OBJ_OTYPE) \
    /* cell size of the daos array */\
    X(DAOS_ARRAY_CELL_SIZE) \
    /* chunk size of the daos array */\
    X(DAOS_ARRAY_CHUNK_SIZE) \
    /* rank and number of bytes moved for fastest/slowest ranks */\
    X(DAOS_FASTEST_RANK) \
    X(DAOS_FASTEST_RANK_BYTES) \
    X(DAOS_SLOWEST_RANK) \
    X(DAOS_SLOWEST_RANK_BYTES) \
    /* end of counters */\
    X(DAOS_NUM_INDICES)

#define DAOS_F_COUNTERS \
    /* timestamp of first open */\
    X(DAOS_F_OPEN_START_TIMESTAMP) \
    /* timestamp of first read */\
    X(DAOS_F_READ_START_TIMESTAMP) \
    /* timestamp of first write */\
    X(DAOS_F_WRITE_START_TIMESTAMP) \
    /* timestamp of first close */\
    X(DAOS_F_CLOSE_START_TIMESTAMP) \
    /* timestamp of last open */\
    X(DAOS_F_OPEN_END_TIMESTAMP) \
    /* timestamp of last read */\
    X(DAOS_F_READ_END_TIMESTAMP) \
    /* timestamp of last write */\
    X(DAOS_F_WRITE_END_TIMESTAMP) \
    /* timestamp of last close */\
    X(DAOS_F_CLOSE_END_TIMESTAMP) \
    /* cumulative daos read time */\
    X(DAOS_F_READ_TIME) \
    /* cumulative daos write time */\
    X(DAOS_F_WRITE_TIME) \
    /* cumulative daos meta time */\
    X(DAOS_F_META_TIME) \
    /* maximum daos read duration */\
    X(DAOS_F_MAX_READ_TIME) \
    /* maximum daos write duration */\
    X(DAOS_F_MAX_WRITE_TIME) \
    /* total i/o and meta time consumed for fastest/slowest ranks */\
    X(DAOS_F_FASTEST_RANK_TIME) \
    X(DAOS_F_SLOWEST_RANK_TIME) \
    /* end of counters */\
    X(DAOS_F_NUM_INDICES)

#define X(a) a,
/* integer statistics for DAOS object records */
enum darshan_daos_indices
{
    DAOS_COUNTERS
};

/* floating point statistics for DAOS object records */
enum darshan_daos_f_indices
{
   DAOS_F_COUNTERS
};
#undef X

/* record structure for DAOS objects. a record is created and stored for
 * every DAOS object opened by the original application. For the DAOS module,
 * the record includes:
 *      - a darshan_base_record structure, which contains the record id & rank
 *      - integer I/O statistics (open, read/write counts, etc)
 *      - floating point I/O statistics (timestamps, cumulative timers, etc.)
 *      - pool and container UUIDs
 *      - 128-bit OID (upper 64-bits in oid_hi and lower 64-bits in oid_lo)
 */
struct darshan_daos_object
{
    struct darshan_base_record base_rec;
    int64_t counters[DAOS_NUM_INDICES];
    double fcounters[DAOS_F_NUM_INDICES];
    unsigned char pool_uuid[16];
    unsigned char cont_uuid[16];
    uint64_t oid_hi;
    uint64_t oid_lo;
};

#endif /* __DARSHAN_DAOS_LOG_FORMAT_H */
