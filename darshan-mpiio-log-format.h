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
    MPIIO_INDEP_OPENS,  /* count of MPI independent opens */
    MPIIO_COLL_OPENS,   /* count of MPI collective opens */
    MPIIO_INDEP_READS,  /* count of MPI independent reads */
    MPIIO_INDEP_WRITES, /* count of MPI independent writes */
    MPIIO_COLL_READS,   /* count of MPI collective reads */
    MPIIO_COLL_WRITES,  /* count of MPI collective writes */
    MPIIO_SPLIT_READS,  /* count of MPI split collective reads */
    MPIIO_SPLIT_WRITES, /* count of MPI split collective writes */
    MPIIO_NB_READS,     /* count of MPI nonblocking reads */
    MPIIO_NB_WRITES,    /* count of MPI nonblocking writes */
    MPIIO_SYNCS,        /* count of MPI file syncs */
    MPIIO_HINTS,        /* count of MPI hints used */
    MPIIO_VIEWS,        /* count of MPI set view calls */
#if 0
    /* type categories */
    MPIIO_COMBINER_NAMED,           /* count of each MPI datatype category */
    MPIIO_COMBINER_DUP,
    MPIIO_COMBINER_CONTIGUOUS,
    MPIIO_COMBINER_VECTOR,
    MPIIO_COMBINER_HVECTOR_INTEGER,
    MPIIO_COMBINER_HVECTOR,
    MPIIO_COMBINER_INDEXED,
    MPIIO_COMBINER_HINDEXED_INTEGER,
    MPIIO_COMBINER_HINDEXED,
    MPIIO_COMBINER_INDEXED_BLOCK,
    MPIIO_COMBINER_STRUCT_INTEGER,
    MPIIO_COMBINER_STRUCT,
    MPIIO_COMBINER_SUBARRAY,
    MPIIO_COMBINER_DARRAY,
    MPIIO_COMBINER_F90_REAL,
    MPIIO_COMBINER_F90_COMPLEX,
    MPIIO_COMBINER_F90_INTEGER,
    MPIIO_COMBINER_RESIZED,
    /* buckets */
    MPIIO_SIZE_READ_AGG_0_100,       /* count of MPI read size ranges */
    MPIIO_SIZE_READ_AGG_100_1K,
    MPIIO_SIZE_READ_AGG_1K_10K,
    MPIIO_SIZE_READ_AGG_10K_100K,
    MPIIO_SIZE_READ_AGG_100K_1M,
    MPIIO_SIZE_READ_AGG_1M_4M,
    MPIIO_SIZE_READ_AGG_4M_10M,
    MPIIO_SIZE_READ_AGG_10M_100M,
    MPIIO_SIZE_READ_AGG_100M_1G,
    MPIIO_SIZE_READ_AGG_1G_PLUS,
    /* buckets */
    MPIIO_SIZE_WRITE_AGG_0_100,      /* count of MPI write size ranges */
    MPIIO_SIZE_WRITE_AGG_100_1K,
    MPIIO_SIZE_WRITE_AGG_1K_10K,
    MPIIO_SIZE_WRITE_AGG_10K_100K,
    MPIIO_SIZE_WRITE_AGG_100K_1M,
    MPIIO_SIZE_WRITE_AGG_1M_4M,
    MPIIO_SIZE_WRITE_AGG_4M_10M,
    MPIIO_SIZE_WRITE_AGG_10M_100M,
    MPIIO_SIZE_WRITE_AGG_100M_1G,
    MPIIO_SIZE_WRITE_AGG_1G_PLUS,
    /* buckets */
    MPIIO_EXTENT_READ_0_100,          /* count of MPI read extent ranges */
    MPIIO_EXTENT_READ_100_1K,
    MPIIO_EXTENT_READ_1K_10K,
    MPIIO_EXTENT_READ_10K_100K,
    MPIIO_EXTENT_READ_100K_1M,
    MPIIO_EXTENT_READ_1M_4M,
    MPIIO_EXTENT_READ_4M_10M,
    MPIIO_EXTENT_READ_10M_100M,
    MPIIO_EXTENT_READ_100M_1G,
    MPIIO_EXTENT_READ_1G_PLUS,
    /* buckets */
    MPIIO_EXTENT_WRITE_0_100,         /* count of MPI write extent ranges */
    MPIIO_EXTENT_WRITE_100_1K,
    MPIIO_EXTENT_WRITE_1K_10K,
    MPIIO_EXTENT_WRITE_10K_100K,
    MPIIO_EXTENT_WRITE_100K_1M,
    MPIIO_EXTENT_WRITE_1M_4M,
    MPIIO_EXTENT_WRITE_4M_10M,
    MPIIO_EXTENT_WRITE_10M_100M,
    MPIIO_EXTENT_WRITE_100M_1G,
    MPIIO_EXTENT_WRITE_1G_PLUS,
#endif

    MPIIO_NUM_INDICES,
};

enum darshan_mpiio_f_indices
{
    MPIIO_F_OPEN_TIMESTAMP,
#if 0
    MPIIO_F_READ_START_TIMESTAMP,
    MPIIO_F_WRITE_START_TIMESTAMP,
    MPIIO_F_READ_END_TIMESTAMP,
    MPIIO_F_WRITE_END_TIMESTAMP,
#endif
    MPIIO_F_CLOSE_TIMESTAMP,
#if 0
    MPIIO_F_READ_TIME,
    MPIIO_F_WRITE_TIME,
#endif
    MPIIO_F_META_TIME,

    MPIIO_F_NUM_INDICES,
};

struct darshan_mpiio_file
{
    darshan_record_id f_id;
    int64_t rank;
    int64_t counters[MPIIO_NUM_INDICES];
    double fcounters[MPIIO_F_NUM_INDICES];
};

#endif /* __DARSHAN_MPIIO_LOG_FORMAT_H */
