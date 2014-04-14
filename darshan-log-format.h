/*
 *  (C) 2009 by Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
 */

#ifndef __DARSHAN_LOG_FORMAT_H
#define __DARSHAN_LOG_FORMAT_H

#include <unistd.h>
#include <sys/types.h>
#include <stdint.h>
#include <inttypes.h>

#if !defined PRId64
#error failed to detect PRId64
#endif
#if !defined PRIu64
#error failed to detect PRIu64
#endif

/* update this on file format changes */
#define CP_VERSION "2.05"

/* magic number for validating output files and checking byte order */
#define CP_MAGIC_NR 6567223

/* size (in bytes) of job record */
#define CP_JOB_RECORD_SIZE 4096

/* max length of exe string within job record (not counting '\0') */
#define CP_EXE_LEN (CP_JOB_RECORD_SIZE - sizeof(struct darshan_job) - 1)

/* size (in bytes) of each file record */
#define CP_FILE_RECORD_SIZE (sizeof(struct darshan_file))

/* max length of name suffix string within file record (not counting '\0') */
#define CP_NAME_SUFFIX_LEN 15

/* per file statistics */
enum darshan_indices
{
    CP_INDEP_OPENS = 0,          /* count of MPI independent opens */
    CP_COLL_OPENS,               /* count of MPI collective opens */
    CP_INDEP_READS,              /* count of independent MPI reads */
    CP_INDEP_WRITES,             /* count of independent MPI writes */
    CP_COLL_READS,               /* count of collective MPI reads */
    CP_COLL_WRITES,              /* count of collective MPI writes */
    CP_SPLIT_READS,              /* count of split collective MPI reads */
    CP_SPLIT_WRITES,             /* count of split collective MPI writes */
    CP_NB_READS,                 /* count of nonblocking MPI reads */
    CP_NB_WRITES,                /* count of nonblocking MPI writes */
    CP_SYNCS,
    CP_POSIX_READS,              /* count of posix reads */
    CP_POSIX_WRITES,             /* count of posix writes */
    CP_POSIX_OPENS,              /* count of posix opens */
    CP_POSIX_SEEKS,              /* count of posix seeks */
    CP_POSIX_STATS,              /* count of posix stat/lstat/fstats */
    CP_POSIX_MMAPS,              /* count of posix mmaps */
    CP_POSIX_FREADS,
    CP_POSIX_FWRITES,
    CP_POSIX_FOPENS,
    CP_POSIX_FSEEKS,
    CP_POSIX_FSYNCS,
    CP_POSIX_FDSYNCS,
    /* pnetcdf stuff */
    CP_INDEP_NC_OPENS,
    CP_COLL_NC_OPENS,
    /* HDF5 stuff */
    CP_HDF5_OPENS,
    /* type categories */
    CP_COMBINER_NAMED,           /* count of each MPI datatype category */
    CP_COMBINER_DUP,
    CP_COMBINER_CONTIGUOUS,
    CP_COMBINER_VECTOR,
    CP_COMBINER_HVECTOR_INTEGER,
    CP_COMBINER_HVECTOR,
    CP_COMBINER_INDEXED,
    CP_COMBINER_HINDEXED_INTEGER,
    CP_COMBINER_HINDEXED,
    CP_COMBINER_INDEXED_BLOCK,
    CP_COMBINER_STRUCT_INTEGER,
    CP_COMBINER_STRUCT,
    CP_COMBINER_SUBARRAY,
    CP_COMBINER_DARRAY,
    CP_COMBINER_F90_REAL,
    CP_COMBINER_F90_COMPLEX,
    CP_COMBINER_F90_INTEGER,
    CP_COMBINER_RESIZED,
    CP_HINTS,                     /* count of MPI hints used */
    CP_VIEWS,                     /* count of MPI set view calls */
    CP_MODE,                      /* mode of file */
    CP_BYTES_READ,                /* total bytes read */
    CP_BYTES_WRITTEN,             /* total bytes written */
    CP_MAX_BYTE_READ,             /* highest offset byte read */
    CP_MAX_BYTE_WRITTEN,          /* highest offset byte written */
    CP_CONSEC_READS,              /* count of consecutive reads */
    CP_CONSEC_WRITES,             /* count of consecutive writes */
    CP_SEQ_READS,                 /* count of sequential reads */
    CP_SEQ_WRITES,                /* count of sequential writes */
    CP_RW_SWITCHES,               /* number of times switched between read and write */
    CP_MEM_NOT_ALIGNED,           /* count of accesses not mem aligned */
    CP_MEM_ALIGNMENT,             /* mem alignment in bytes */
    CP_FILE_NOT_ALIGNED,          /* count of accesses not file aligned */
    CP_FILE_ALIGNMENT,            /* file alignment in bytes */
    CP_MAX_READ_TIME_SIZE,
    CP_MAX_WRITE_TIME_SIZE,
    /* buckets */
    CP_SIZE_READ_0_100,           /* count of posix read size ranges */
    CP_SIZE_READ_100_1K,
    CP_SIZE_READ_1K_10K,
    CP_SIZE_READ_10K_100K,
    CP_SIZE_READ_100K_1M,
    CP_SIZE_READ_1M_4M,
    CP_SIZE_READ_4M_10M,
    CP_SIZE_READ_10M_100M,
    CP_SIZE_READ_100M_1G,
    CP_SIZE_READ_1G_PLUS,
    /* buckets */
    CP_SIZE_WRITE_0_100,          /* count of posix write size ranges */
    CP_SIZE_WRITE_100_1K,
    CP_SIZE_WRITE_1K_10K,
    CP_SIZE_WRITE_10K_100K,
    CP_SIZE_WRITE_100K_1M,
    CP_SIZE_WRITE_1M_4M,
    CP_SIZE_WRITE_4M_10M,
    CP_SIZE_WRITE_10M_100M,
    CP_SIZE_WRITE_100M_1G,
    CP_SIZE_WRITE_1G_PLUS,
    /* buckets */
    CP_SIZE_READ_AGG_0_100,       /* count of MPI read size ranges */
    CP_SIZE_READ_AGG_100_1K,
    CP_SIZE_READ_AGG_1K_10K,
    CP_SIZE_READ_AGG_10K_100K,
    CP_SIZE_READ_AGG_100K_1M,
    CP_SIZE_READ_AGG_1M_4M,
    CP_SIZE_READ_AGG_4M_10M,
    CP_SIZE_READ_AGG_10M_100M,
    CP_SIZE_READ_AGG_100M_1G,
    CP_SIZE_READ_AGG_1G_PLUS,
    /* buckets */
    CP_SIZE_WRITE_AGG_0_100,      /* count of MPI write size ranges */
    CP_SIZE_WRITE_AGG_100_1K,
    CP_SIZE_WRITE_AGG_1K_10K,
    CP_SIZE_WRITE_AGG_10K_100K,
    CP_SIZE_WRITE_AGG_100K_1M,
    CP_SIZE_WRITE_AGG_1M_4M,
    CP_SIZE_WRITE_AGG_4M_10M,
    CP_SIZE_WRITE_AGG_10M_100M,
    CP_SIZE_WRITE_AGG_100M_1G,
    CP_SIZE_WRITE_AGG_1G_PLUS,
    /* buckets */
    CP_EXTENT_READ_0_100,          /* count of MPI read extent ranges */
    CP_EXTENT_READ_100_1K,
    CP_EXTENT_READ_1K_10K,
    CP_EXTENT_READ_10K_100K,
    CP_EXTENT_READ_100K_1M,
    CP_EXTENT_READ_1M_4M,
    CP_EXTENT_READ_4M_10M,
    CP_EXTENT_READ_10M_100M,
    CP_EXTENT_READ_100M_1G,
    CP_EXTENT_READ_1G_PLUS,
    /* buckets */
    CP_EXTENT_WRITE_0_100,         /* count of MPI write extent ranges */
    CP_EXTENT_WRITE_100_1K,
    CP_EXTENT_WRITE_1K_10K,
    CP_EXTENT_WRITE_10K_100K,
    CP_EXTENT_WRITE_100K_1M,
    CP_EXTENT_WRITE_1M_4M,
    CP_EXTENT_WRITE_4M_10M,
    CP_EXTENT_WRITE_10M_100M,
    CP_EXTENT_WRITE_100M_1G,
    CP_EXTENT_WRITE_1G_PLUS,
    /* counters */
    CP_STRIDE1_STRIDE,             /* the four most frequently appearing strides */
    CP_STRIDE2_STRIDE,
    CP_STRIDE3_STRIDE,
    CP_STRIDE4_STRIDE,
    CP_STRIDE1_COUNT,              /* count of each of the most frequent strides */
    CP_STRIDE2_COUNT,
    CP_STRIDE3_COUNT,
    CP_STRIDE4_COUNT,
    CP_ACCESS1_ACCESS,             /* the four most frequently appearing access sizes */
    CP_ACCESS2_ACCESS,
    CP_ACCESS3_ACCESS,
    CP_ACCESS4_ACCESS,
    CP_ACCESS1_COUNT,              /* count of each of the most frequent access sizes */
    CP_ACCESS2_COUNT,
    CP_ACCESS3_COUNT,
    CP_ACCESS4_COUNT,
    CP_DEVICE,                     /* device id reported by stat */
    CP_SIZE_AT_OPEN,
    CP_FASTEST_RANK,
    CP_FASTEST_RANK_BYTES,
    CP_SLOWEST_RANK,
    CP_SLOWEST_RANK_BYTES,

    CP_NUM_INDICES,
};

/* floating point statistics */
enum f_darshan_indices
{
    /* NOTE: adjust cp_normalize_timestamps() function if any TIMESTAMPS are
     * added or modified in this list
     */
    CP_F_OPEN_TIMESTAMP = 0,    /* timestamp of first open */
    CP_F_READ_START_TIMESTAMP,  /* timestamp of first read */
    CP_F_WRITE_START_TIMESTAMP, /* timestamp of first write */
    CP_F_CLOSE_TIMESTAMP,       /* timestamp of last close */
    CP_F_READ_END_TIMESTAMP,    /* timestamp of last read */
    CP_F_WRITE_END_TIMESTAMP,   /* timestamp of last write */
    CP_F_POSIX_READ_TIME,       /* cumulative posix read time */
    CP_F_POSIX_WRITE_TIME,      /* cumulative posix write time */
    CP_F_POSIX_META_TIME,       /* cumulative posix meta time */
    CP_F_MPI_META_TIME,         /* cumulative mpi-io meta time */
    CP_F_MPI_READ_TIME,         /* cumulative mpi-io read time */
    CP_F_MPI_WRITE_TIME,        /* cumulative mpi-io write time */
    CP_F_MAX_READ_TIME,
    CP_F_MAX_WRITE_TIME,
    /* Total I/O and meta time consumed by fastest and slowest ranks, 
     * reported in either MPI or POSIX time depending on how the file 
     * was accessed.
     */
    CP_F_FASTEST_RANK_TIME,     
    CP_F_SLOWEST_RANK_TIME,
    CP_F_VARIANCE_RANK_TIME,
    CP_F_VARIANCE_RANK_BYTES,

    CP_F_NUM_INDICES,
};

/* statistics for any kind of file */
struct darshan_file
{
    uint64_t hash;
    int64_t rank;
    char name_suffix[CP_NAME_SUFFIX_LEN+1];
    int64_t counters[CP_NUM_INDICES];
    double fcounters[CP_F_NUM_INDICES];
};

/* statistics for the job as a whole */
#define DARSHAN_JOB_METADATA_LEN 1024 /* including null terminator! */
struct darshan_job
{
    char version_string[8];
    int64_t magic_nr;
    int64_t uid;
    int64_t start_time;
    int64_t end_time;
    int64_t nprocs;
    int64_t jobid;
    char metadata[DARSHAN_JOB_METADATA_LEN];
};

/* This macro can be used to identify files that have been opened using
 * pnetcdf, hdf5, or mpi-io, but were never opened at the posix level.  As a
 * result the record will not necessarily have all of the expected fields
 * populated.
 */
#define CP_FILE_PARTIAL(__file)((((__file)->counters[CP_POSIX_OPENS] || (__file)->counters[CP_POSIX_FOPENS] || (__file)->counters[CP_POSIX_STATS]) ? 0 : 1))

#endif /* __DARSHAN_LOG_FORMAT_H */
