/*
 * Copyright (C) 2015 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

#ifndef __DARSHAN_HDF5_LOG_FORMAT_H
#define __DARSHAN_HDF5_LOG_FORMAT_H

/* current HDF5 log format version */
#define DARSHAN_HDF5_VER 2

#define HDF5_COUNTERS \
    /* count of HDF5 opens */\
    X(HDF5_OPENS) \
    /* end of counters */\
    X(HDF5_NUM_INDICES)

#define HDF5_F_COUNTERS \
    /* timestamp of first open */\
    X(HDF5_F_OPEN_START_TIMESTAMP) \
    /* timestamp of first close */\
    X(HDF5_F_CLOSE_START_TIMESTAMP) \
    /* timestamp of last open */\
    X(HDF5_F_OPEN_END_TIMESTAMP) \
    /* timestamp of last close */\
    X(HDF5_F_CLOSE_END_TIMESTAMP) \
    /* end of counters*/\
    X(HDF5_F_NUM_INDICES)

#define X(a) a,
/* integer statistics for HDF5 file records */
enum darshan_hdf5_indices
{
    HDF5_COUNTERS
};

/* floating point statistics for HDF5 file records */
enum darshan_hdf5_f_indices
{
    HDF5_F_COUNTERS
};
#undef X

/* file record structure for HDF5 files. a record is created and stored for
 * every HDF5 file opened by the original application. For the HDF5 module,
 * the record includes:
 *      - a darshan_base_record structure, which contains the record id & rank
 *      - integer file I/O statistics (open, read/write counts, etc)
 *      - floating point I/O statistics (timestamps, cumulative timers, etc.)
 */
struct darshan_hdf5_file
{
    struct darshan_base_record base_rec;
    int64_t counters[HDF5_NUM_INDICES];
    double fcounters[HDF5_F_NUM_INDICES];
};

#endif /* __DARSHAN_HDF5_LOG_FORMAT_H */
