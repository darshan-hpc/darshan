/*
 * Copyright (C) 2015 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

#ifndef __DARSHAN_PNETCDF_LOG_FORMAT_H
#define __DARSHAN_PNETCDF_LOG_FORMAT_H

/* current PNETCDF log format version */
#define DARSHAN_PNETCDF_VER 2

#define PNETCDF_COUNTERS \
    /* count of PNETCDF independent opens */\
    X(PNETCDF_INDEP_OPENS) \
    /* count of PNETCDF collective opens */\
    X(PNETCDF_COLL_OPENS) \
    /* end of counters */\
    X(PNETCDF_NUM_INDICES)

#define PNETCDF_F_COUNTERS \
    /* timestamp of first open */\
    X(PNETCDF_F_OPEN_START_TIMESTAMP) \
    /* timestamp of first close */\
    X(PNETCDF_F_CLOSE_START_TIMESTAMP) \
    /* timestamp of last open */\
    X(PNETCDF_F_OPEN_END_TIMESTAMP) \
    /* timestamp of last close */\
    X(PNETCDF_F_CLOSE_END_TIMESTAMP) \
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
 *      - a darshan_base_record structure, which contains the record id & rank
 *      - integer file I/O statistics (open, read/write counts, etc)
 *      - floating point I/O statistics (timestamps, cumulative timers, etc.)
 */
struct darshan_pnetcdf_file
{
    struct darshan_base_record base_rec;
    int64_t counters[PNETCDF_NUM_INDICES];
    double fcounters[PNETCDF_F_NUM_INDICES];
};

#endif /* __DARSHAN_PNETCDF_LOG_FORMAT_H */
