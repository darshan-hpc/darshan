/*
 * Copyright (C) 2015 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

#ifndef __DARSHAN_HDF5_LOG_FORMAT_H
#define __DARSHAN_HDF5_LOG_FORMAT_H

/* current HDF5 log format versions */
#define DARSHAN_H5F_VER 3
#define DARSHAN_H5D_VER 2

#define H5D_MAX_NDIMS 5

#define H5F_COUNTERS \
    /* count of HDF5 file opens/creates */\
    X(H5F_OPENS) \
    /* count of HDF5 file flushes */\
    X(H5F_FLUSHES) \
    /* flag indicating whether MPI-IO is used for accessing this file */\
    X(H5F_USE_MPIIO) \
    /* end of counters */\
    X(H5F_NUM_INDICES)

#define H5F_F_COUNTERS \
    /* timestamp of first file open */\
    X(H5F_F_OPEN_START_TIMESTAMP) \
    /* timestamp of first file close */\
    X(H5F_F_CLOSE_START_TIMESTAMP) \
    /* timestamp of last file open */\
    X(H5F_F_OPEN_END_TIMESTAMP) \
    /* timestamp of last file close */\
    X(H5F_F_CLOSE_END_TIMESTAMP) \
    /* cumulative H5F meta time */\
    X(H5F_F_META_TIME) \
    /* end of counters*/\
    X(H5F_F_NUM_INDICES)

#define H5D_COUNTERS \
    /* count of HDF5 dataset opens/creates */\
    X(H5D_OPENS) \
    /* count of HDF5 datset reads */\
    X(H5D_READS) \
    /* count of HDF5 datset writes */\
    X(H5D_WRITES) \
    /* count of HDF5 dataset flushes */\
    X(H5D_FLUSHES) \
    /* total bytes read */\
    X(H5D_BYTES_READ) \
    /* total bytes written */\
    X(H5D_BYTES_WRITTEN) \
    /* number of times switching between H5D read and write */\
    X(H5D_RW_SWITCHES) \
    /* number of read/write ops with regular hyperslab selections */\
    X(H5D_REGULAR_HYPERSLAB_SELECTS) \
    /* number of read/write ops with irregular hyperslab selections */\
    X(H5D_IRREGULAR_HYPERSLAB_SELECTS) \
    /* number of read/write ops with point selections */\
    X(H5D_POINT_SELECTS) \
    /* sizes of the maximum read/write operations */\
    X(H5D_MAX_READ_TIME_SIZE) \
    X(H5D_MAX_WRITE_TIME_SIZE) \
    /* buckets for H5D read size ranges */\
    X(H5D_SIZE_READ_AGG_0_100) \
    X(H5D_SIZE_READ_AGG_100_1K) \
    X(H5D_SIZE_READ_AGG_1K_10K) \
    X(H5D_SIZE_READ_AGG_10K_100K) \
    X(H5D_SIZE_READ_AGG_100K_1M) \
    X(H5D_SIZE_READ_AGG_1M_4M) \
    X(H5D_SIZE_READ_AGG_4M_10M) \
    X(H5D_SIZE_READ_AGG_10M_100M) \
    X(H5D_SIZE_READ_AGG_100M_1G) \
    X(H5D_SIZE_READ_AGG_1G_PLUS) \
    /* buckets for H5D write size ranges */\
    X(H5D_SIZE_WRITE_AGG_0_100) \
    X(H5D_SIZE_WRITE_AGG_100_1K) \
    X(H5D_SIZE_WRITE_AGG_1K_10K) \
    X(H5D_SIZE_WRITE_AGG_10K_100K) \
    X(H5D_SIZE_WRITE_AGG_100K_1M) \
    X(H5D_SIZE_WRITE_AGG_1M_4M) \
    X(H5D_SIZE_WRITE_AGG_4M_10M) \
    X(H5D_SIZE_WRITE_AGG_10M_100M) \
    X(H5D_SIZE_WRITE_AGG_100M_1G) \
    X(H5D_SIZE_WRITE_AGG_1G_PLUS) \
    /* the four most frequently appearing H5D accesses, identified by
     * access size and access lengths (count * block) and strides
     * in last 5 hyperslab dimensions */\
    X(H5D_ACCESS1_ACCESS) \
    X(H5D_ACCESS1_LENGTH_D1) \
    X(H5D_ACCESS1_LENGTH_D2) \
    X(H5D_ACCESS1_LENGTH_D3) \
    X(H5D_ACCESS1_LENGTH_D4) \
    X(H5D_ACCESS1_LENGTH_D5) \
    X(H5D_ACCESS1_STRIDE_D1) \
    X(H5D_ACCESS1_STRIDE_D2) \
    X(H5D_ACCESS1_STRIDE_D3) \
    X(H5D_ACCESS1_STRIDE_D4) \
    X(H5D_ACCESS1_STRIDE_D5) \
    X(H5D_ACCESS2_ACCESS) \
    X(H5D_ACCESS2_LENGTH_D1) \
    X(H5D_ACCESS2_LENGTH_D2) \
    X(H5D_ACCESS2_LENGTH_D3) \
    X(H5D_ACCESS2_LENGTH_D4) \
    X(H5D_ACCESS2_LENGTH_D5) \
    X(H5D_ACCESS2_STRIDE_D1) \
    X(H5D_ACCESS2_STRIDE_D2) \
    X(H5D_ACCESS2_STRIDE_D3) \
    X(H5D_ACCESS2_STRIDE_D4) \
    X(H5D_ACCESS2_STRIDE_D5) \
    X(H5D_ACCESS3_ACCESS) \
    X(H5D_ACCESS3_LENGTH_D1) \
    X(H5D_ACCESS3_LENGTH_D2) \
    X(H5D_ACCESS3_LENGTH_D3) \
    X(H5D_ACCESS3_LENGTH_D4) \
    X(H5D_ACCESS3_LENGTH_D5) \
    X(H5D_ACCESS3_STRIDE_D1) \
    X(H5D_ACCESS3_STRIDE_D2) \
    X(H5D_ACCESS3_STRIDE_D3) \
    X(H5D_ACCESS3_STRIDE_D4) \
    X(H5D_ACCESS3_STRIDE_D5) \
    X(H5D_ACCESS4_ACCESS) \
    X(H5D_ACCESS4_LENGTH_D1) \
    X(H5D_ACCESS4_LENGTH_D2) \
    X(H5D_ACCESS4_LENGTH_D3) \
    X(H5D_ACCESS4_LENGTH_D4) \
    X(H5D_ACCESS4_LENGTH_D5) \
    X(H5D_ACCESS4_STRIDE_D1) \
    X(H5D_ACCESS4_STRIDE_D2) \
    X(H5D_ACCESS4_STRIDE_D3) \
    X(H5D_ACCESS4_STRIDE_D4) \
    X(H5D_ACCESS4_STRIDE_D5) \
    /* count of each of the most frequent H5D access sizes */\
    X(H5D_ACCESS1_COUNT) \
    X(H5D_ACCESS2_COUNT) \
    X(H5D_ACCESS3_COUNT) \
    X(H5D_ACCESS4_COUNT) \
    /* number of dimensions in dataset's dataspace */\
    X(H5D_DATASPACE_NDIMS) \
    /* number of points in dataset's dataspace */\
    X(H5D_DATASPACE_NPOINTS) \
    /* size of dataset elements in bytes */\
    X(H5D_DATATYPE_SIZE) \
    /* chunk sizes in the last 5 dimensions of the dataset */\
    /* NOTE: D1 is the last dimension (i.e., row) , D2 is 2nd to last (i.e., column), and so on */\
    X(H5D_CHUNK_SIZE_D1) \
    X(H5D_CHUNK_SIZE_D2) \
    X(H5D_CHUNK_SIZE_D3) \
    X(H5D_CHUNK_SIZE_D4) \
    X(H5D_CHUNK_SIZE_D5) \
    /* flag indicating use of MPI-IO collectives */\
    X(H5D_USE_MPIIO_COLLECTIVE) \
    /* flag indicating whether deprecated create/open calls were used */\
    X(H5D_USE_DEPRECATED) \
    /* rank and number of bytes moved for fastest/slowest ranks */\
    X(H5D_FASTEST_RANK) \
    X(H5D_FASTEST_RANK_BYTES) \
    X(H5D_SLOWEST_RANK) \
    X(H5D_SLOWEST_RANK_BYTES) \
    /* end of counters */\
    X(H5D_NUM_INDICES)

#define H5D_F_COUNTERS \
    /* timestamp of first dataset open */\
    X(H5D_F_OPEN_START_TIMESTAMP) \
    /* timestamp of first dataset read */\
    X(H5D_F_READ_START_TIMESTAMP) \
    /* timestamp of first dataset write */\
    X(H5D_F_WRITE_START_TIMESTAMP) \
    /* timestamp of first dataset close */\
    X(H5D_F_CLOSE_START_TIMESTAMP) \
    /* timestamp of last dataset open */\
    X(H5D_F_OPEN_END_TIMESTAMP) \
    /* timestamp of last dataset read */\
    X(H5D_F_READ_END_TIMESTAMP) \
    /* timestamp of last dataset write */\
    X(H5D_F_WRITE_END_TIMESTAMP) \
    /* timestamp of last dataset close */\
    X(H5D_F_CLOSE_END_TIMESTAMP) \
    /* cumulative H5D read time */\
    X(H5D_F_READ_TIME) \
    /* cumulative H5D write time */\
    X(H5D_F_WRITE_TIME) \
    /* cumulative H5D meta time */\
    X(H5D_F_META_TIME) \
    /* maximum H5D read duration */\
    X(H5D_F_MAX_READ_TIME) \
    /* maximum H5D write duration */\
    X(H5D_F_MAX_WRITE_TIME) \
    /* total i/o and meta time for fastest/slowest ranks */\
    X(H5D_F_FASTEST_RANK_TIME) \
    X(H5D_F_SLOWEST_RANK_TIME) \
    /* variance of total i/o time and bytes moved across all ranks */\
    /* NOTE: for shared records only */\
    X(H5D_F_VARIANCE_RANK_TIME) \
    X(H5D_F_VARIANCE_RANK_BYTES) \
    /* end of counters*/\
    X(H5D_F_NUM_INDICES)

#define X(a) a,
/* integer statistics for HDF5 file records */
enum darshan_h5f_indices
{
    H5F_COUNTERS
};
/* integer statistics for HDF5 dataset records */
enum darshan_h5d_indices
{
    H5D_COUNTERS
};

/* floating point statistics for HDF5 file records */
enum darshan_h5f_f_indices
{
    H5F_F_COUNTERS
};
/* floating point statistics for HDF5 dataset records */
enum darshan_h5d_f_indices
{
    H5D_F_COUNTERS
};
#undef X

/* record structure for HDF5 files. a record is created and stored for
 * every HDF5 file opened by the original application. For the HDF5 module,
 * the record includes:
 *      - a darshan_base_record structure, which contains the record id & rank
 *      - integer file I/O statistics (open, read/write counts, etc)
 *      - floating point file I/O statistics (timestamps, cumulative timers, etc.)
 */
struct darshan_hdf5_file
{
    struct darshan_base_record base_rec;
    int64_t counters[H5F_NUM_INDICES];
    double fcounters[H5F_F_NUM_INDICES];
};

/* record structure for HDF5 datasets. a record is created and stored for
 * every HDF5 dataset opened by the original application. For the HDF5 module,
 * the record includes:
 *      - a darshan_base_record structure, which contains the record id & rank
 *      - the Darshan record ID of the file the dataset belongs to
 *      - integer dataset I/O statistics (open, read/write counts, etc)
 *      - floating point dataset I/O statistics (timestamps, cumulative timers, etc.)
 */
struct darshan_hdf5_dataset
{
    struct darshan_base_record base_rec;
    uint64_t file_rec_id;
    int64_t counters[H5D_NUM_INDICES];
    double fcounters[H5D_F_NUM_INDICES];
};

#endif /* __DARSHAN_HDF5_LOG_FORMAT_H */
