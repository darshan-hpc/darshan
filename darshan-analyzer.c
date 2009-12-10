#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <ftw.h>
#include <zlib.h>

#include "darshan-log-format.h"

// old format

enum darshan_indices_v121
{
    V121_CP_INDEP_OPENS = 0,          /* count of MPI independent opens */
    V121_CP_COLL_OPENS,               /* count of MPI collective opens */
    V121_CP_INDEP_READS,              /* count of independent MPI reads */
    V121_CP_INDEP_WRITES,             /* count of independent MPI writes */
    V121_CP_COLL_READS,               /* count of collective MPI reads */
    V121_CP_COLL_WRITES,              /* count of collective MPI writes */
    V121_CP_SPLIT_READS,              /* count of split collective MPI reads */
    V121_CP_SPLIT_WRITES,             /* count of split collective MPI writes */
    V121_CP_NB_READS,                 /* count of nonblocking MPI reads */
    V121_CP_NB_WRITES,                /* count of nonblocking MPI writes */
    V121_CP_SYNCS,
    V121_CP_POSIX_READS,              /* count of posix reads */
    V121_CP_POSIX_WRITES,             /* count of posix writes */
    V121_CP_POSIX_OPENS,              /* count of posix opens */
    V121_CP_POSIX_SEEKS,              /* count of posix seeks */
    V121_CP_POSIX_STATS,              /* count of posix stat/lstat/fstats */
    V121_CP_POSIX_MMAPS,              /* count of posix mmaps */
    V121_CP_POSIX_FREADS,
    V121_CP_POSIX_FWRITES,
    V121_CP_POSIX_FOPENS,
    V121_CP_POSIX_FSEEKS,
    V121_CP_POSIX_FSYNCS,
    V121_CP_POSIX_FDSYNCS,
    /* type categories */
    V121_CP_COMBINER_NAMED,           /* count of each MPI datatype category */
    V121_CP_COMBINER_DUP,
    V121_CP_COMBINER_CONTIGUOUS,
    V121_CP_COMBINER_VECTOR,
    V121_CP_COMBINER_HVECTOR_INTEGER,
    V121_CP_COMBINER_HVECTOR,
    V121_CP_COMBINER_INDEXED,
    V121_CP_COMBINER_HINDEXED_INTEGER,
    V121_CP_COMBINER_HINDEXED,
    V121_CP_COMBINER_INDEXED_BLOCK,
    V121_CP_COMBINER_STRUCT_INTEGER,
    V121_CP_COMBINER_STRUCT,
    V121_CP_COMBINER_SUBARRAY,
    V121_CP_COMBINER_DARRAY,
    V121_CP_COMBINER_F90_REAL,
    V121_CP_COMBINER_F90_COMPLEX,
    V121_CP_COMBINER_F90_INTEGER,
    V121_CP_COMBINER_RESIZED,
    V121_CP_HINTS,                     /* count of MPI hints used */
    V121_CP_VIEWS,                     /* count of MPI set view calls */
    V121_CP_MODE,                      /* mode of file */
    V121_CP_BYTES_READ,                /* total bytes read */
    V121_CP_BYTES_WRITTEN,             /* total bytes written */
    V121_CP_MAX_BYTE_READ,             /* highest offset byte read */
    V121_CP_MAX_BYTE_WRITTEN,          /* highest offset byte written */
    V121_CP_CONSEC_READS,              /* count of consecutive reads */
    V121_CP_CONSEC_WRITES,             /* count of consecutive writes */
    V121_CP_SEQ_READS,                 /* count of sequential reads */
    V121_CP_SEQ_WRITES,                /* count of sequential writes */
    V121_CP_RW_SWITCHES,               /* number of times switched between read and write */
    V121_CP_MEM_NOT_ALIGNED,           /* count of accesses not mem aligned */
    V121_CP_MEM_ALIGNMENT,             /* mem alignment in bytes */
    V121_CP_FILE_NOT_ALIGNED,          /* count of accesses not file aligned */
    V121_CP_FILE_ALIGNMENT,            /* file alignment in bytes */
    /* buckets */
    V121_CP_SIZE_READ_0_100,           /* count of posix read size ranges */
    V121_CP_SIZE_READ_100_1K,
    V121_CP_SIZE_READ_1K_10K,
    V121_CP_SIZE_READ_10K_100K,
    V121_CP_SIZE_READ_100K_1M,
    V121_CP_SIZE_READ_1M_4M,
    V121_CP_SIZE_READ_4M_10M,
    V121_CP_SIZE_READ_10M_100M,
    V121_CP_SIZE_READ_100M_1G,
    V121_CP_SIZE_READ_1G_PLUS,
    /* buckets */
    V121_CP_SIZE_WRITE_0_100,          /* count of posix write size ranges */
    V121_CP_SIZE_WRITE_100_1K,
    V121_CP_SIZE_WRITE_1K_10K,
    V121_CP_SIZE_WRITE_10K_100K,
    V121_CP_SIZE_WRITE_100K_1M,
    V121_CP_SIZE_WRITE_1M_4M,
    V121_CP_SIZE_WRITE_4M_10M,
    V121_CP_SIZE_WRITE_10M_100M,
    V121_CP_SIZE_WRITE_100M_1G,
    V121_CP_SIZE_WRITE_1G_PLUS,
    /* buckets */
    V121_CP_SIZE_READ_AGG_0_100,       /* count of MPI read size ranges */
    V121_CP_SIZE_READ_AGG_100_1K,
    V121_CP_SIZE_READ_AGG_1K_10K,
    V121_CP_SIZE_READ_AGG_10K_100K,
    V121_CP_SIZE_READ_AGG_100K_1M,
    V121_CP_SIZE_READ_AGG_1M_4M,
    V121_CP_SIZE_READ_AGG_4M_10M,
    V121_CP_SIZE_READ_AGG_10M_100M,
    V121_CP_SIZE_READ_AGG_100M_1G,
    V121_CP_SIZE_READ_AGG_1G_PLUS,
    V121_CP_SIZE_WRITE_AGG_0_100,      /* count of MPI write size ranges */
    V121_CP_SIZE_WRITE_AGG_100_1K,
    V121_CP_SIZE_WRITE_AGG_1K_10K,
    V121_CP_SIZE_WRITE_AGG_10K_100K,
    V121_CP_SIZE_WRITE_AGG_100K_1M,
    V121_CP_SIZE_WRITE_AGG_1M_4M,
    V121_CP_SIZE_WRITE_AGG_4M_10M,
    V121_CP_SIZE_WRITE_AGG_10M_100M,
    V121_CP_SIZE_WRITE_AGG_100M_1G,
    V121_CP_SIZE_WRITE_AGG_1G_PLUS,
    /* buckets */
    V121_CP_EXTENT_READ_0_100,          /* count of MPI read extent ranges */
    V121_CP_EXTENT_READ_100_1K,
    V121_CP_EXTENT_READ_1K_10K,
    V121_CP_EXTENT_READ_10K_100K,
    V121_CP_EXTENT_READ_100K_1M,
    V121_CP_EXTENT_READ_1M_4M,
    V121_CP_EXTENT_READ_4M_10M,
    V121_CP_EXTENT_READ_10M_100M,
    V121_CP_EXTENT_READ_100M_1G,
    V121_CP_EXTENT_READ_1G_PLUS,
    /* buckets */
    V121_CP_EXTENT_WRITE_0_100,         /* count of MPI write extent ranges */
    V121_CP_EXTENT_WRITE_100_1K,
    V121_CP_EXTENT_WRITE_1K_10K,
    V121_CP_EXTENT_WRITE_10K_100K,
    V121_CP_EXTENT_WRITE_100K_1M,
    V121_CP_EXTENT_WRITE_1M_4M,
    V121_CP_EXTENT_WRITE_4M_10M,
    V121_CP_EXTENT_WRITE_10M_100M,
    V121_CP_EXTENT_WRITE_100M_1G,
    V121_CP_EXTENT_WRITE_1G_PLUS,
    /* counters */
    V121_CP_STRIDE1_STRIDE,             /* the four most frequently appearing strides */
    V121_CP_STRIDE2_STRIDE,
    V121_CP_STRIDE3_STRIDE,
    V121_CP_STRIDE4_STRIDE,
    V121_CP_STRIDE1_COUNT,              /* count of each of the most frequent strides */
    V121_CP_STRIDE2_COUNT,
    V121_CP_STRIDE3_COUNT,
    V121_CP_STRIDE4_COUNT,
    V121_CP_ACCESS1_ACCESS,             /* the four most frequently appearing access sizes */
    V121_CP_ACCESS2_ACCESS,
    V121_CP_ACCESS3_ACCESS,
    V121_CP_ACCESS4_ACCESS,
    V121_CP_ACCESS1_COUNT,              /* count of each of the most frequent access sizes */
    V121_CP_ACCESS2_COUNT,
    V121_CP_ACCESS3_COUNT,
    V121_CP_ACCESS4_COUNT,
    V121_CP_NUM_INDICES,
};

/* floating point statistics */
enum f_darshan_indices_v121
{
    V121_CP_F_OPEN_TIMESTAMP = 0,    /* timestamp of first open */
    V121_CP_F_READ_START_TIMESTAMP,  /* timestamp of first read */
    V121_CP_F_WRITE_START_TIMESTAMP, /* timestamp of first write */
    V121_CP_F_CLOSE_TIMESTAMP,       /* timestamp of last close */
    V121_CP_F_READ_END_TIMESTAMP,    /* timestamp of last read */
    V121_CP_F_WRITE_END_TIMESTAMP,   /* timestamp of last write */
    V121_CP_F_POSIX_READ_TIME,       /* cumulative posix read time */
    V121_CP_F_POSIX_WRITE_TIME,      /* cumulative posix write time */
    V121_CP_F_POSIX_META_TIME,       /* cumulative posix meta time */
    V121_CP_F_MPI_META_TIME,         /* cumulative mpi-io meta time */
    V121_CP_F_MPI_READ_TIME,         /* cumulative mpi-io read time */
    V121_CP_F_MPI_WRITE_TIME,        /* cumulative mpi-io write time */
    V121_CP_F_NUM_INDICES,
};

/* statistics for any kind of file */
struct darshan_file_v121
{
    uint64_t hash;
    int rank;
    int64_t counters[V121_CP_NUM_INDICES];
    double fcounters[V121_CP_F_NUM_INDICES];
    char name_suffix[CP_NAME_SUFFIX_LEN+1];
};

// old format

#define BUCKET1 0.20
#define BUCKET2 0.40
#define BUCKET3 0.60
#define BUCKET4 0.80

char * base = NULL;

int total_single = 0;
int total_multi  = 0;
int total_mpio   = 0;
int total_pnet   = 0;
int total_hdf5   = 0;
int total_count  = 0;

int bucket1 = 0;
int bucket2 = 0;
int bucket3 = 0;
int bucket4 = 0;
int bucket5 = 0;
int fail    = 0;

int process_log(const char *fname, double *io_ratio, int *used_mpio, int *used_pnet, int *used_hdf5, int *used_multi, int *used_single)
{
    struct darshan_job job;
    struct darshan_file cp_file;
    struct darshan_file_v121 cp_file_v121;
    char tmp_string[1024];
    gzFile zfile;
    int ret;
    int old;
    int f_count;
    double total_io_time;
    double total_job_time;

    zfile = gzopen(fname, "r");
    if (zfile == NULL)
    {
        perror("gzopen");
        return -1;
    }

    ret = gzread(zfile, &job, sizeof(job));
    if (ret < sizeof(job))
    {
        perror("gzread");
        fprintf(stderr, "%s\n", fname);
        gzclose(zfile);
        return -1;
    }

    if (strcmp(job.version_string, CP_VERSION) == 0)
    {
        old = 0;
    }
    else if (strcmp(job.version_string, "1.21") == 0)
    {
        old = 1;
    }
    else
    {
        printf("unknown version: %s\n", job.version_string);
        gzclose(zfile);
        return -1;
    }

    ret = gzread(zfile, tmp_string, (CP_EXE_LEN+1));
    if (ret < (CP_EXE_LEN+1))
    {
        perror("gzread");
        fprintf(stderr, "%s\n", fname);
        gzclose(zfile);
        return -1;
    }

    f_count = 0;
    total_io_time = 0.0;

    if (old)
    {

    while ((ret = gzread(zfile, &cp_file_v121, sizeof(cp_file_v121))) == sizeof(cp_file_v121))
    {
        f_count   += 1;

        if (cp_file_v121.rank == -1)
            *used_single = 1;
        else
            *used_multi = 1;

        *used_mpio += cp_file_v121.counters[V121_CP_INDEP_OPENS];
        *used_mpio += cp_file_v121.counters[V121_CP_COLL_OPENS];

        total_io_time += cp_file_v121.fcounters[V121_CP_F_POSIX_READ_TIME];
        total_io_time += cp_file_v121.fcounters[V121_CP_F_POSIX_WRITE_TIME];
        total_io_time += cp_file_v121.fcounters[V121_CP_F_POSIX_META_TIME];
    }

    }
    else
    {
    while ((ret = gzread(zfile, &cp_file, sizeof(cp_file))) == sizeof(cp_file))
    {
        f_count   += 1;

        if (cp_file.rank == -1)
            *used_single = 1;
        else
            *used_multi = 1;

        *used_mpio += cp_file.counters[CP_INDEP_OPENS];
        *used_mpio += cp_file.counters[CP_COLL_OPENS];
        *used_pnet += cp_file.counters[CP_INDEP_NC_OPENS];
        *used_pnet += cp_file.counters[CP_COLL_NC_OPENS];
        *used_hdf5 += cp_file.counters[CP_HDF5_OPENS];

        total_io_time += cp_file.fcounters[CP_F_POSIX_READ_TIME];
        total_io_time += cp_file.fcounters[CP_F_POSIX_WRITE_TIME];
        total_io_time += cp_file.fcounters[CP_F_POSIX_META_TIME];
    }

    }

    total_job_time = (double)job.end_time - (double)job.start_time;
    if (total_job_time < 1.0)
    {
        total_job_time = 1.0;
    }

    if (f_count > 0)
    {
        *io_ratio = total_io_time/total_job_time;
    }
    else
    {
        *io_ratio = 0.0;
    }

    gzclose(zfile);

    return 0;
}

int tree_walk (const char *fpath, const struct stat *sb, int typeflag)
{
    double io_ratio = 0.0;
    int used_mpio = 0;
    int used_pnet = 0;
    int used_hdf5 = 0;
    int used_multi = 0;
    int used_single = 0;

    if (typeflag != FTW_F) return 0;

    process_log(fpath,&io_ratio,&used_mpio,&used_pnet,&used_hdf5,&used_multi,&used_single);

    total_count++;

    if (used_mpio > 0) total_mpio++;
    if (used_pnet > 0) total_pnet++;
    if (used_hdf5 > 0) total_hdf5++;
    if (used_single > 0) total_single++;
    if (used_multi  > 0) total_multi++;

    if (io_ratio <= BUCKET1)
        bucket1++;
    else if ((io_ratio > BUCKET1) && (io_ratio <= BUCKET2))
        bucket2++;
    else if ((io_ratio > BUCKET2) && (io_ratio <= BUCKET3))
        bucket3++;
    else if ((io_ratio > BUCKET3) && (io_ratio <= BUCKET4))
        bucket4++;
    else if (io_ratio > BUCKET4)
        bucket5++;
    else
    {
        printf("iorat: %lf\n", io_ratio);
        fail++;
    }

    return 0;
}

int main(int argc, char **argv)
{
    int ret = 0;

    if(argc != 2)
    {
        fprintf(stderr, "Error: bad arguments.\n");
        return(-1);
    }

    base = argv[1];

    ret = ftw(base, tree_walk, 512);
    if(ret != 0)
    {
        fprintf(stderr, "Error: failed to walk path: %s\n", base);
        return(-1);
    }

    printf ("   log: %s\n", base);
    printf (" total: %d\n", total_count);
    printf ("single: %lf [%d]\n", (double)total_single/(double)total_count, total_single);
    printf (" multi: %lf [%d]\n", (double)total_multi/(double)total_count, total_multi);
    printf ("  mpio: %lf [%d]\n", (double)total_mpio/(double)total_count, total_mpio);
    printf ("  pnet: %lf [%d]\n", (double)total_pnet/(double)total_count, total_pnet);
    printf ("  hdf5: %lf [%d]\n", (double)total_hdf5/(double)total_count, total_hdf5);
    printf ("%.2lf-%.2lf: %d\n", (double)0.0,     (double)BUCKET1, bucket1);
    printf ("%.2lf-%.2lf: %d\n", (double)BUCKET1, (double)BUCKET2, bucket2);
    printf ("%.2lf-%.2lf: %d\n", (double)BUCKET2, (double)BUCKET3, bucket3);
    printf ("%.2lf-%.2lf: %d\n", (double)BUCKET3, (double)BUCKET4, bucket4);
    printf ("%.2lf-%.2lf: %d\n", (double)BUCKET4, (double)100.0,   bucket5);
    return 0;
}
