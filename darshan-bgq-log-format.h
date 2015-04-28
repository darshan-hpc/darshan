/*
 * Copyright (C) 2015 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

#ifndef __DARSHAN_BGQ_LOG_FORMAT_H
#define __DARSHAN_BGQ_LOG_FORMAT_H

#include "darshan-log-format.h"

/* integer counters for the "BGQ" example module */
enum darshan_bgq_indices
{
    BGQ_CSJOBID,       // control system jobid
    BGQ_NNODES,        // number of BGQ compute nodes
    BGQ_RANKSPERNODE,  // number of MPI ranks per node
    BGQ_DDRPERNODE,    // size in MB of DDR3 per node
    BGQ_INODES,        // number of i/o nodes
    BGQ_ANODES,        // dimension of A torus
    BGQ_BNODES,        // dimension of B torus
    BGQ_CNODES,        // dimension of C torus
    BGQ_DNODES,        // dimension of D torus
    BGQ_ENODES,        // dimension of E torus
    BGQ_TORUSENABLED,  // which dimensions are torus

    BGQ_NUM_INDICES,
};

/* floating point counters for the "BGQ" example module */
enum darshan_bgq_f_indices
{
    BGQ_F_TIMESTAMP,     // timestamp when data collected
    BGQ_F_NUM_INDICES,
};

/* the darshan_bgq_record structure encompasses the high-level data/counters
 * which would actually be logged to file by Darshan for the "BGQ" example
 * module. This example implementation logs the following data for each
 * record:
 *      - a corresponding Darshan record identifier
 *      - the rank of the process responsible for the record
 *      - integer I/O counters (operation counts, I/O sizes, etc.)
 *      - floating point I/O counters (timestamps, cumulative timers, etc.)
 */
struct darshan_bgq_record
{
    darshan_record_id f_id;
    int64_t rank;
    int alignment;
    int64_t counters[BGQ_NUM_INDICES];
    double fcounters[BGQ_F_NUM_INDICES];
};

#endif /* __DARSHAN_BGQ_LOG_FORMAT_H */
