/*
 * Copyright (C) 2015 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

#ifndef __DARSHAN_PNETCDF_LOG_FORMAT_H
#define __DARSHAN_PNETCDF_LOG_FORMAT_H

#include "darshan-log-format.h"

#define PNETCDF_COUNTERS \
    /* count of PNETCDF independent opens */\
    X(PNETCDF_INDEP_OPENS) \
    /* count of PNETCDF collective opens */\
    X(PNETCDF_COLL_OPENS) \
    /* end of counters */\
    X(PNETCDF_NUM_INDICES)

#define PNETCDF_F_COUNTERS \
    /* timestamp of first open */\
    X(PNETCDF_F_OPEN_TIMESTAMP) \
    /* timestamp of last close */\
    X(PNETCDF_F_CLOSE_TIMESTAMP) \
    /* end of counters*/\
    X(PNETCDF_F_NUM_INDICES)

#define X(a) a,
/* integer statistics for PNETCDF file records */
enum darshan_pnetcdf_indices
{
    PNETCDF_COUNTERS
};

/* floating point statistics for PNETCDF file records */
enum darshan_pnetcdf_f_indices
{
    PNETCDF_F_COUNTERS
};
#undef X

/* file record structure for PNETCDF files. a record is created and stored for
 * every PNETCDF file opened by the original application. For the PNETCDF module,
 * the record includes:
 *      - a corresponding record identifier (created by hashing the file path)
 *      - the rank of the process which opened the file (-1 for shared files)
 *      - integer file I/O statistics (open, read/write counts, etc)
 *      - floating point I/O statistics (timestamps, cumulative timers, etc.)
 */
struct darshan_pnetcdf_file
{
    darshan_record_id f_id;
    int64_t rank;
    int64_t counters[PNETCDF_NUM_INDICES];
    double fcounters[PNETCDF_F_NUM_INDICES];
};

#endif /* __DARSHAN_PNETCDF_LOG_FORMAT_H */
