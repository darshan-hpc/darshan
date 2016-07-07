/*
 * Copyright (C) 2015 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

#ifndef __DARSHAN_BGQ_LOG_FORMAT_H
#define __DARSHAN_BGQ_LOG_FORMAT_H

/* current BGQ log format version */
#define DARSHAN_BGQ_VER 2

#define BGQ_COUNTERS \
    /* control system jobid */\
    X(BGQ_CSJOBID) \
    /* number of BGQ compute nodes */\
    X(BGQ_NNODES) \
    /* number of MPI ranks per node */\
    X(BGQ_RANKSPERNODE) \
    /* size in MB of DDR3 per node */\
    X(BGQ_DDRPERNODE) \
    /* number of i/o nodes */\
    X(BGQ_INODES) \
    /* dimension of A torus */\
    X(BGQ_ANODES) \
    /* dimension of B torus */\
    X(BGQ_BNODES) \
    /* dimension of C torus */\
    X(BGQ_CNODES) \
    /* dimension of D torus */\
    X(BGQ_DNODES) \
    /* dimension of E torus */\
    X(BGQ_ENODES) \
    /* which dimensions are torus */\
    X(BGQ_TORUSENABLED) \
    /* end of counters */\
    X(BGQ_NUM_INDICES)

#define BGQ_F_COUNTERS \
    /* timestamp when data was collected */\
    X(BGQ_F_TIMESTAMP) \
    /* end of counters */\
    X(BGQ_F_NUM_INDICES)

#define X(a) a,
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
 *      - a darshan_base_record structure, which contains the record id & rank
 *      - integer I/O counters (operation counts, I/O sizes, etc.)
 *      - floating point I/O counters (timestamps, cumulative timers, etc.)
 */
struct darshan_bgq_record
{
    struct darshan_base_record base_rec;
    int64_t counters[BGQ_NUM_INDICES];
    double fcounters[BGQ_F_NUM_INDICES];
};

#endif /* __DARSHAN_BGQ_LOG_FORMAT_H */
