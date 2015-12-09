/*
 * Copyright (C) 2015 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

#ifndef __DARSHAN_BGQ_LOG_FORMAT_H
#define __DARSHAN_BGQ_LOG_FORMAT_H

#include "darshan-log-format.h"


#define BGQ_COUNTERS \
    X(BGQ_CSJOBID, "control system jobid") \
    X(BGQ_NNODES, "number of BGQ compute nodes") \
    X(BGQ_RANKSPERNODE, "number of MPI ranks per node") \
    X(BGQ_DDRPERNODE, "size in MB of DDR3 per node") \
    X(BGQ_INODES, "number of i/o nodes") \
    X(BGQ_ANODES, "dimension of A torus") \
    X(BGQ_BNODES, "dimension of B torus") \
    X(BGQ_CNODES, "dimension of C torus") \
    X(BGQ_DNODES, "dimension of D torus") \
    X(BGQ_ENODES, "dimension of E torus") \
    X(BGQ_TORUSENABLED, "which dimensions are torus") \
    X(BGQ_NUM_INDICES, "end of counters")

#define BGQ_F_COUNTERS \
    X(BGQ_F_TIMESTAMP, "timestamp when data was collected") \
    X(BGQ_F_NUM_INDICES, "end of counters")

#define X(a, b) a,
/* integer counters for the "BGQ" example module */
enum darshan_bgq_indices
{
    BGQ_COUNTERS
};

/* floating point counters for the "BGQ" example module */
enum darshan_bgq_f_indices
{
    BGQ_F_COUNTERS
};
#undef X

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
    struct darshan_base_record base_rec;
    int alignment;
    int64_t counters[BGQ_NUM_INDICES];
    double fcounters[BGQ_F_NUM_INDICES];
};

#endif /* __DARSHAN_BGQ_LOG_FORMAT_H */
