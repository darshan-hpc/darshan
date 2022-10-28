/*
 * Copyright (C) 2015 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

#ifndef __DARSHAN_PNETCDF_LOG_FORMAT_H
#define __DARSHAN_PNETCDF_LOG_FORMAT_H

/* current PnetCDF log format version */
#define DARSHAN_PNETCDF_FILE_VER 3
#define DARSHAN_PNETCDF_VAR_VER 1

#define PNETCDF_VAR_MAX_NDIMS 5

/* counters for file level activities */
#define PNETCDF_FILE_COUNTERS \
    /* count of file creates */\
    X(PNETCDF_FILE_CREATES) \
    /* count of file opens */\
    X(PNETCDF_FILE_OPENS) \
    /* count of redef calls */\
    X(PNETCDF_FILE_REDEFS) \
    /* count of independent waits */\
    X(PNETCDF_FILE_INDEP_WAITS) \
    /* count of collective waits */\
    X(PNETCDF_FILE_COLL_WAITS) \
    /* count of file syncs */\
    X(PNETCDF_FILE_SYNCS) \
    /* total bytes read (includes internal library metadata I/O) */\
    X(PNETCDF_FILE_BYTES_READ) \
    /* total bytes written (includes internal library metadata I/O) */\
    X(PNETCDF_FILE_BYTES_WRITTEN) \
    /* count of file wait failures */\
    X(PNETCDF_FILE_WAIT_FAILURES) \
    /* end of counters */\
    X(PNETCDF_FILE_NUM_INDICES)

/* timestamps for file level activities */
#define PNETCDF_FILE_F_COUNTERS \
    /* timestamp of first open/create */\
    X(PNETCDF_FILE_F_OPEN_START_TIMESTAMP) \
    /* timestamp of first close */\
    X(PNETCDF_FILE_F_CLOSE_START_TIMESTAMP) \
    /* timestamp of first wait */\
    X(PNETCDF_FILE_F_WAIT_START_TIMESTAMP) \
    /* timestamp of last open/create */\
    X(PNETCDF_FILE_F_OPEN_END_TIMESTAMP) \
    /* timestamp of last close */\
    X(PNETCDF_FILE_F_CLOSE_END_TIMESTAMP) \
    /* timestamp of last wait */\
    X(PNETCDF_FILE_F_WAIT_END_TIMESTAMP) \
    /* cumulative meta time */\
    X(PNETCDF_FILE_F_META_TIME) \
    /* cumulative wait time */\
    X(PNETCDF_FILE_F_WAIT_TIME) \
    /* end of counters*/\
    X(PNETCDF_FILE_F_NUM_INDICES)

/* counters for variable level activities */
#define PNETCDF_VAR_COUNTERS \
    /* count of variable define/inquire operations */\
    X(PNETCDF_VAR_OPENS) \
    /* count of independent reads */\
    X(PNETCDF_VAR_INDEP_READS) \
    /* count of independent writes */\
    X(PNETCDF_VAR_INDEP_WRITES) \
    /* count of collective reads */\
    X(PNETCDF_VAR_COLL_READS) \
    /* count of collective writes */\
    X(PNETCDF_VAR_COLL_WRITES) \
    /* count of non-blocking reads */\
    X(PNETCDF_VAR_NB_READS) \
    /* count of non-blocking writes */\
    X(PNETCDF_VAR_NB_WRITES) \
    /* total bytes read (not including internal library metadata I/O) */\
    X(PNETCDF_VAR_BYTES_READ) \
    /* total bytes written (not including internal library metadata I/O) */\
    X(PNETCDF_VAR_BYTES_WRITTEN) \
    /* number of times switching between read and write */\
    X(PNETCDF_VAR_RW_SWITCHES) \
    /* number of calls to put var APIs */\
    X(PNETCDF_VAR_PUT_VAR) \
    /* number of calls to put var1 APIs */\
    X(PNETCDF_VAR_PUT_VAR1) \
    /* number of calls to put vara APIs */\
    X(PNETCDF_VAR_PUT_VARA) \
    /* number of calls to put vars APIs */\
    X(PNETCDF_VAR_PUT_VARS) \
    /* number of calls to put varm APIs */\
    X(PNETCDF_VAR_PUT_VARM) \
    /* number of calls to put varn APIs */\
    X(PNETCDF_VAR_PUT_VARN) \
    /* number of calls to put vard APIs */\
    X(PNETCDF_VAR_PUT_VARD) \
    /* number of calls to get var APIs */\
    X(PNETCDF_VAR_GET_VAR) \
    /* number of calls to get var1 APIs */\
    X(PNETCDF_VAR_GET_VAR1) \
    /* number of calls to get vara APIs */\
    X(PNETCDF_VAR_GET_VARA) \
    /* number of calls to get vars APIs */\
    X(PNETCDF_VAR_GET_VARS) \
    /* number of calls to get varm APIs */\
    X(PNETCDF_VAR_GET_VARM) \
    /* number of calls to get varn APIs */\
    X(PNETCDF_VAR_GET_VARN) \
    /* number of calls to get vard APIs */\
    X(PNETCDF_VAR_GET_VARD) \
    /* number of calls to iput var APIs */\
    X(PNETCDF_VAR_IPUT_VAR) \
    /* number of calls to iput var1 APIs */\
    X(PNETCDF_VAR_IPUT_VAR1) \
    /* number of calls to iput vara APIs */\
    X(PNETCDF_VAR_IPUT_VARA) \
    /* number of calls to iput vars APIs */\
    X(PNETCDF_VAR_IPUT_VARS) \
    /* number of calls to iput varm APIs */\
    X(PNETCDF_VAR_IPUT_VARM) \
    /* number of calls to iput varn APIs */\
    X(PNETCDF_VAR_IPUT_VARN) \
    /* number of calls to iget var APIs */\
    X(PNETCDF_VAR_IGET_VAR) \
    /* number of calls to iget var1 APIs */\
    X(PNETCDF_VAR_IGET_VAR1) \
    /* number of calls to iget vara APIs */\
    X(PNETCDF_VAR_IGET_VARA) \
    /* number of calls to iget vars APIs */\
    X(PNETCDF_VAR_IGET_VARS) \
    /* number of calls to iget varm APIs */\
    X(PNETCDF_VAR_IGET_VARM) \
    /* number of calls to iget varn APIs */\
    X(PNETCDF_VAR_IGET_VARN) \
    /* number of calls to bput var APIs */\
    X(PNETCDF_VAR_BPUT_VAR) \
    /* number of calls to bput var1 APIs */\
    X(PNETCDF_VAR_BPUT_VAR1) \
    /* number of calls to bput vara APIs */\
    X(PNETCDF_VAR_BPUT_VARA) \
    /* number of calls to bput vars APIs */\
    X(PNETCDF_VAR_BPUT_VARS) \
    /* number of calls to bput varm APIs */\
    X(PNETCDF_VAR_BPUT_VARM) \
    /* number of calls to bput varn APIs */\
    X(PNETCDF_VAR_BPUT_VARN) \
    /* sizes of the slowest read/write operations */\
    X(PNETCDF_VAR_MAX_READ_TIME_SIZE) \
    X(PNETCDF_VAR_MAX_WRITE_TIME_SIZE) \
    /* buckets for read size ranges */\
    X(PNETCDF_VAR_SIZE_READ_AGG_0_100) \
    X(PNETCDF_VAR_SIZE_READ_AGG_100_1K) \
    X(PNETCDF_VAR_SIZE_READ_AGG_1K_10K) \
    X(PNETCDF_VAR_SIZE_READ_AGG_10K_100K) \
    X(PNETCDF_VAR_SIZE_READ_AGG_100K_1M) \
    X(PNETCDF_VAR_SIZE_READ_AGG_1M_4M) \
    X(PNETCDF_VAR_SIZE_READ_AGG_4M_10M) \
    X(PNETCDF_VAR_SIZE_READ_AGG_10M_100M) \
    X(PNETCDF_VAR_SIZE_READ_AGG_100M_1G) \
    X(PNETCDF_VAR_SIZE_READ_AGG_1G_PLUS) \
    /* buckets for write size ranges */\
    X(PNETCDF_VAR_SIZE_WRITE_AGG_0_100) \
    X(PNETCDF_VAR_SIZE_WRITE_AGG_100_1K) \
    X(PNETCDF_VAR_SIZE_WRITE_AGG_1K_10K) \
    X(PNETCDF_VAR_SIZE_WRITE_AGG_10K_100K) \
    X(PNETCDF_VAR_SIZE_WRITE_AGG_100K_1M) \
    X(PNETCDF_VAR_SIZE_WRITE_AGG_1M_4M) \
    X(PNETCDF_VAR_SIZE_WRITE_AGG_4M_10M) \
    X(PNETCDF_VAR_SIZE_WRITE_AGG_10M_100M) \
    X(PNETCDF_VAR_SIZE_WRITE_AGG_100M_1G) \
    X(PNETCDF_VAR_SIZE_WRITE_AGG_1G_PLUS) \
    /* the four most frequently appearing PnetCDF accesses, identified by
     * access size and access lengths (count * block) and strides
     * in last 5 hyperslab dimensions */\
    X(PNETCDF_VAR_ACCESS1_ACCESS) \
    X(PNETCDF_VAR_ACCESS1_LENGTH_D1) \
    X(PNETCDF_VAR_ACCESS1_LENGTH_D2) \
    X(PNETCDF_VAR_ACCESS1_LENGTH_D3) \
    X(PNETCDF_VAR_ACCESS1_LENGTH_D4) \
    X(PNETCDF_VAR_ACCESS1_LENGTH_D5) \
    X(PNETCDF_VAR_ACCESS1_STRIDE_D1) \
    X(PNETCDF_VAR_ACCESS1_STRIDE_D2) \
    X(PNETCDF_VAR_ACCESS1_STRIDE_D3) \
    X(PNETCDF_VAR_ACCESS1_STRIDE_D4) \
    X(PNETCDF_VAR_ACCESS1_STRIDE_D5) \
    X(PNETCDF_VAR_ACCESS2_ACCESS) \
    X(PNETCDF_VAR_ACCESS2_LENGTH_D1) \
    X(PNETCDF_VAR_ACCESS2_LENGTH_D2) \
    X(PNETCDF_VAR_ACCESS2_LENGTH_D3) \
    X(PNETCDF_VAR_ACCESS2_LENGTH_D4) \
    X(PNETCDF_VAR_ACCESS2_LENGTH_D5) \
    X(PNETCDF_VAR_ACCESS2_STRIDE_D1) \
    X(PNETCDF_VAR_ACCESS2_STRIDE_D2) \
    X(PNETCDF_VAR_ACCESS2_STRIDE_D3) \
    X(PNETCDF_VAR_ACCESS2_STRIDE_D4) \
    X(PNETCDF_VAR_ACCESS2_STRIDE_D5) \
    X(PNETCDF_VAR_ACCESS3_ACCESS) \
    X(PNETCDF_VAR_ACCESS3_LENGTH_D1) \
    X(PNETCDF_VAR_ACCESS3_LENGTH_D2) \
    X(PNETCDF_VAR_ACCESS3_LENGTH_D3) \
    X(PNETCDF_VAR_ACCESS3_LENGTH_D4) \
    X(PNETCDF_VAR_ACCESS3_LENGTH_D5) \
    X(PNETCDF_VAR_ACCESS3_STRIDE_D1) \
    X(PNETCDF_VAR_ACCESS3_STRIDE_D2) \
    X(PNETCDF_VAR_ACCESS3_STRIDE_D3) \
    X(PNETCDF_VAR_ACCESS3_STRIDE_D4) \
    X(PNETCDF_VAR_ACCESS3_STRIDE_D5) \
    X(PNETCDF_VAR_ACCESS4_ACCESS) \
    X(PNETCDF_VAR_ACCESS4_LENGTH_D1) \
    X(PNETCDF_VAR_ACCESS4_LENGTH_D2) \
    X(PNETCDF_VAR_ACCESS4_LENGTH_D3) \
    X(PNETCDF_VAR_ACCESS4_LENGTH_D4) \
    X(PNETCDF_VAR_ACCESS4_LENGTH_D5) \
    X(PNETCDF_VAR_ACCESS4_STRIDE_D1) \
    X(PNETCDF_VAR_ACCESS4_STRIDE_D2) \
    X(PNETCDF_VAR_ACCESS4_STRIDE_D3) \
    X(PNETCDF_VAR_ACCESS4_STRIDE_D4) \
    X(PNETCDF_VAR_ACCESS4_STRIDE_D5) \
    /* count of each of the most frequent PnetCDF access sizes */\
    X(PNETCDF_VAR_ACCESS1_COUNT) \
    X(PNETCDF_VAR_ACCESS2_COUNT) \
    X(PNETCDF_VAR_ACCESS3_COUNT) \
    X(PNETCDF_VAR_ACCESS4_COUNT) \
    /* variable's number of dimensions */\
    X(PNETCDF_VAR_NDIMS) \
    /* variable's number of elements */\
    X(PNETCDF_VAR_NPOINTS) \
    /* size of variable elements in bytes */\
    X(PNETCDF_VAR_DATATYPE_SIZE) \
    /* flag indicating whether variable is a record variable */\
    X(PNETCDF_VAR_IS_RECORD_VAR) \
    /* rank and number of bytes moved for fastest/slowest ranks */\
    X(PNETCDF_VAR_FASTEST_RANK) \
    X(PNETCDF_VAR_FASTEST_RANK_BYTES) \
    X(PNETCDF_VAR_SLOWEST_RANK) \
    X(PNETCDF_VAR_SLOWEST_RANK_BYTES) \
    /* end of counters*/\
    X(PNETCDF_VAR_NUM_INDICES)

/* timestamps for variable level activities */
#define PNETCDF_VAR_F_COUNTERS \
    /* timestamp of first variable open (defined or inquired) */\
    X(PNETCDF_VAR_F_OPEN_START_TIMESTAMP) \
    /* timestamp of first variable read */\
    X(PNETCDF_VAR_F_READ_START_TIMESTAMP) \
    /* timestamp of first variable write */\
    X(PNETCDF_VAR_F_WRITE_START_TIMESTAMP) \
    /* timestamp of first variable close */\
    X(PNETCDF_VAR_F_CLOSE_START_TIMESTAMP) \
    /* timestamp of last variable open */\
    X(PNETCDF_VAR_F_OPEN_END_TIMESTAMP) \
    /* timestamp of last variable read */\
    X(PNETCDF_VAR_F_READ_END_TIMESTAMP) \
    /* timestamp of last variable write */\
    X(PNETCDF_VAR_F_WRITE_END_TIMESTAMP) \
    /* timestamp of last variable close */\
    X(PNETCDF_VAR_F_CLOSE_END_TIMESTAMP) \
    /* cumulative variable read time */\
    X(PNETCDF_VAR_F_READ_TIME) \
    /* cumulative variable write time */\
    X(PNETCDF_VAR_F_WRITE_TIME) \
    /* cumulative variable meta time */\
    X(PNETCDF_VAR_F_META_TIME) \
    /* maximum variable read duration */\
    X(PNETCDF_VAR_F_MAX_READ_TIME) \
    /* maximum variable write duration */\
    X(PNETCDF_VAR_F_MAX_WRITE_TIME) \
    /* total i/o and meta time for fastest/slowest ranks */\
    X(PNETCDF_VAR_F_FASTEST_RANK_TIME) \
    X(PNETCDF_VAR_F_SLOWEST_RANK_TIME) \
    /* variance of total i/o time and bytes moved across all ranks */\
    /* NOTE: for shared records only */\
    X(PNETCDF_VAR_F_VARIANCE_RANK_TIME) \
    X(PNETCDF_VAR_F_VARIANCE_RANK_BYTES) \
    /* end of counters*/\
    X(PNETCDF_VAR_F_NUM_INDICES)

#define X(a) a,
/* integer statistics for PnetCDF file records */
enum darshan_pnetcdf_file_indices
{
    PNETCDF_FILE_COUNTERS
};

/* integer statistics for PnetCDF variable records */
enum darshan_pnetcdf_var_indices
{
    PNETCDF_VAR_COUNTERS
};

/* floating point statistics for PNETCDF file records */
enum darshan_pnetcdf_file_f_indices
{
    PNETCDF_FILE_F_COUNTERS
};

/* floating point statistics for PnetCDF variable records */
enum darshan_pnetcdf_var_f_indices
{
    PNETCDF_VAR_F_COUNTERS
};
#undef X

/* record structure for PnetCDF files. a record is created and stored for
 * every PnetCDF file opened by the original application. For the PnetCDF module,
 * the record includes:
 *      - a darshan_base_record structure, which contains the record id & rank
 *      - integer file I/O statistics (open, read/write counts, etc)
 *      - floating point I/O statistics (timestamps, cumulative timers, etc.)
 */
struct darshan_pnetcdf_file
{
    struct darshan_base_record base_rec;
    int64_t counters[PNETCDF_FILE_NUM_INDICES];
    double fcounters[PNETCDF_FILE_F_NUM_INDICES];
};

/* record structure for PnetCDF variables. a record is created and stored for
 * every PnetCDF variable opened by the original application. For the PnetCDF module,
 * the record includes:
 *      - a darshan_base_record structure, which contains the record id & rank
 *      - the Darshan record ID of the file the variable belongs to
 *      - integer variable I/O statistics (open, read/write counts, etc)
 *      - floating point variable I/O statistics (timestamps, cumulative timers, etc.)
 */
struct darshan_pnetcdf_var
{
    struct darshan_base_record base_rec;
    uint64_t file_rec_id;
    int64_t counters[PNETCDF_VAR_NUM_INDICES];
    double fcounters[PNETCDF_VAR_F_NUM_INDICES];
};

#endif /* __DARSHAN_PNETCDF_LOG_FORMAT_H */
