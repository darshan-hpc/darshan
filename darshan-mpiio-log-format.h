/*
 * Copyright (C) 2015 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

#ifndef __DARSHAN_MPIIO_LOG_FORMAT_H
#define __DARSHAN_MPIIO_LOG_FORMAT_H

#include "darshan-log-format.h"

enum darshan_mpiio_indices
{
    DARSHAN_MPIIO_INDEP_OPENS,   /* independent opens */
    DARSHAN_MPIIO_COLL_OPENS,    /* collective opens */
    DARSHAN_MPIIO_HINTS,    /* how many times hints were used */

    DARSHAN_MPIIO_NUM_INDICES,
};

enum darshan_mpiio_f_indices
{
    DARSHAN_MPIIO_F_META_TIME,      /* cumulative metadata time */
    DARSHAN_MPIIO_F_OPEN_TIMESTAMP, /* first open timestamp */

    DARSHAN_MPIIO_F_NUM_INDICES,
};

struct darshan_mpiio_file
{
    darshan_record_id f_id;
    int64_t rank;
    int64_t counters[DARSHAN_MPIIO_NUM_INDICES];
    double fcounters[DARSHAN_MPIIO_F_NUM_INDICES];
};

#endif /* __DARSHAN_MPIIO_LOG_FORMAT_H */
