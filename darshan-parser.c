#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <zlib.h>
#include <time.h>

#include "darshan-log-format.h"

int main(int argc, char **argv)
{
    gzFile file;
    int ret;
    struct darshan_job job;
    struct darshan_file cp_file;
    char tmp_string[1024];
    int no_files_flag = 0;
    time_t tmp_time = 0;

    if(argc != 2)
    {
        fprintf(stderr, "Usage: %s <filename>\n", argv[0]);
        return(-1);
    }

    file = gzopen(argv[1], "r");
    if(!file)
    {
        perror("gzopen");
        return(-1);
    }
   
    /* read job info */
    ret = gzread(file, &job, sizeof(job));
    if(ret < sizeof(job))
    {
        if(gzeof(file))
        {
            fprintf(stderr, "Error: invalid log file (too short).\n");
            gzclose(file);
            return(-1);
        }
        perror("gzread");
        gzclose(file);
        return(-1);
    }

    /* check version string */
    if(strcmp(job.version_string, CP_VERSION))
    {
        fprintf(stderr, "Error: incompatible darshan file.\n");
        fprintf(stderr, "Error: expected version %s, but got %s\n", CP_VERSION, job.version_string);
        gzclose(file);
        return(-1);
    }

    /* read trailing exe string */
    ret = gzread(file, tmp_string, (CP_EXE_LEN + 1));
    if(ret < (CP_EXE_LEN + 1))
    {
        if(gzeof(file))
        {
            no_files_flag = 1;
        }
        perror("gzread");
        gzclose(file);
        return(-1);
    }

    printf("# darshan log version: %s\n", CP_VERSION);
    printf("# size of file statistics: %d bytes\n", sizeof(cp_file));
    printf("# size of job statistics: %d bytes\n", sizeof(job));
    printf("# exe: %s\n", tmp_string);
    printf("# uid: %d\n", job.uid);
    printf("# start_time: %ld\n", (long)job.start_time);
    tmp_time = (time_t)job.start_time;
    printf("# start_time_asci: %s", ctime(&tmp_time));
    printf("# end_time: %ld\n", (long)job.end_time);
    tmp_time = (time_t)job.end_time;
    printf("# end_time_asci: %s", ctime(&tmp_time));
    printf("# nprocs: %d\n", job.nprocs);
    printf("# run time: %ld\n", (long)(job.end_time - job.start_time + 1));
   
    if(no_files_flag)
    {
        /* it looks like the app didn't open any files */
        printf("# no files opened.\n");
        gzclose(file);
        return(0);
    }

    printf("\n# description of columns:\n");
    printf("#   <rank>: MPI rank.  -1 indicates that the file is shared\n");
    printf("#      across all processes and statistics are aggregated.\n");
    printf("#   <file>: hash of file path.  0 indicates that statistics\n");
    printf("#      are condensed to refer to all files opened at the given\n");
    printf("#      process.\n");
    printf("#   <counter> and <value>: statistical counters.\n");
    printf("#   <name suffix>: last %d characters of file name.\n", CP_NAME_SUFFIX_LEN);
    printf("\n# description of counters:\n");
    printf("#   CP_POSIX_*: posix operation counts.\n");
    printf("#   CP_COLL_*: MPI collective operation counts.\n");
    printf("#   CP_INDEP_*: MPI independent operation counts.\n");
    printf("#   CP_SPIT_*: MPI split collective operation counts.\n");
    printf("#   CP_NB_*: MPI non blocking operation counts.\n");
    printf("#   READS,WRITES,OPENS,SEEKS,STATS, and MMAPS are types of operations.\n");
    printf("#   CP_COMBINER_*: combiner counts for MPI mem and file datatypes.\n");
    printf("#   CP_HINTS: number of times MPI hints were used.\n");
    printf("#   CP_VIEWS: number of times MPI file views were used.\n");
    printf("#   CP_MODE: mode that file was opened in.\n");
    printf("#   CP_BYTES_*: total bytes read and written.\n");
    printf("#   CP_MAX_BYTE_*: highest offset byte read and written.\n");
    printf("#   CP_CONSEC_*: number of exactly adjacent reads and writes.\n");
    printf("#   CP_SEQ_*: number of reads and writes from increasing offsets.\n");
    printf("#   CP_RW_SWITCHES: number of times access alternated between read and write.\n");
    printf("#   CP_*_ALIGNMENT: memory and file alignment.\n");
    printf("#   CP_*_NOT_ALIGNED: number of reads and writes that were not aligned.\n");
    printf("#   CP_SIZE_READ_*: histogram of read access sizes.\n");
    printf("#   CP_SIZE_READ_AGG_*: histogram of MPI datatype total sizes.\n");
    printf("#   CP_EXTENT_READ_*: histogram of MPI datatype extents.\n");
    printf("#   CP_STRIDE*_STRIDE: the four most common strides detected.\n");
    printf("#   CP_STRIDE*_COUNT: count of the four most common strides.\n");
    printf("#   CP_ACCESS*_ACCESS: the four most common access sizes.\n");
    printf("#   CP_ACCESS*_COUNT: count of the four most common access sizes.\n");
    printf("#   CP_F_OPEN_TIMESTAMP: timestamp of first open (mpi or posix).\n");
    printf("#   CP_F_*_START_TIMESTAMP: timestamp of first read/write (mpi or posix).\n");
    printf("#   CP_F_*_END_TIMESTAMP: timestamp of last read/write (mpi or posix).\n");
    printf("#   CP_F_CLOSE_TIMESTAMP: timestamp of last close (mpi or posix).\n");
    printf("#   CP_F_POSIX_READ/WRITE_TIME: cumulative time spent in posix reads or writes.\n");
    printf("#   CP_F_MPI_READ/WRITE_TIME: cumulative time spent in mpi-io reads or writes.\n");
    printf("#   CP_F_POSIX_META_TIME: cumulative time spent in posix open, close, fsync, stat and seek, .\n");
    printf("#   CP_F_MPI_META_TIME: cumulative time spent in mpi-io open, close, set_view, and sync.\n");

    printf("\n");

    CP_PRINT_HEADER();

    while((ret = gzread(file, &cp_file, sizeof(cp_file))) == sizeof(cp_file))
    {
        CP_PRINT(&job, &cp_file, CP_POSIX_READS);
        CP_PRINT(&job, &cp_file, CP_POSIX_WRITES);
        CP_PRINT(&job, &cp_file, CP_POSIX_OPENS);
        CP_PRINT(&job, &cp_file, CP_POSIX_SEEKS);
        CP_PRINT(&job, &cp_file, CP_POSIX_STATS);
        CP_PRINT(&job, &cp_file, CP_POSIX_MMAPS);
        CP_PRINT(&job, &cp_file, CP_POSIX_FREADS);
        CP_PRINT(&job, &cp_file, CP_POSIX_FWRITES);
        CP_PRINT(&job, &cp_file, CP_POSIX_FOPENS);
        CP_PRINT(&job, &cp_file, CP_POSIX_FSEEKS);
        CP_PRINT(&job, &cp_file, CP_POSIX_FSYNCS);
        CP_PRINT(&job, &cp_file, CP_POSIX_FDSYNCS);
        CP_PRINT(&job, &cp_file, CP_INDEP_OPENS);
        CP_PRINT(&job, &cp_file, CP_COLL_OPENS);
        CP_PRINT(&job, &cp_file, CP_INDEP_READS);
        CP_PRINT(&job, &cp_file, CP_INDEP_WRITES);
        CP_PRINT(&job, &cp_file, CP_COLL_READS);
        CP_PRINT(&job, &cp_file, CP_COLL_WRITES);
        CP_PRINT(&job, &cp_file, CP_SPLIT_READS);
        CP_PRINT(&job, &cp_file, CP_SPLIT_WRITES);
        CP_PRINT(&job, &cp_file, CP_NB_READS);
        CP_PRINT(&job, &cp_file, CP_NB_WRITES);
        CP_PRINT(&job, &cp_file, CP_SYNCS);
        CP_PRINT(&job, &cp_file, CP_COMBINER_NAMED);
        CP_PRINT(&job, &cp_file, CP_COMBINER_DUP);
        CP_PRINT(&job, &cp_file, CP_COMBINER_CONTIGUOUS);
        CP_PRINT(&job, &cp_file, CP_COMBINER_VECTOR);
        CP_PRINT(&job, &cp_file, CP_COMBINER_HVECTOR_INTEGER);
        CP_PRINT(&job, &cp_file, CP_COMBINER_HVECTOR);
        CP_PRINT(&job, &cp_file, CP_COMBINER_INDEXED);
        CP_PRINT(&job, &cp_file, CP_COMBINER_HINDEXED_INTEGER);
        CP_PRINT(&job, &cp_file, CP_COMBINER_HINDEXED);
        CP_PRINT(&job, &cp_file, CP_COMBINER_INDEXED_BLOCK);
        CP_PRINT(&job, &cp_file, CP_COMBINER_STRUCT_INTEGER);
        CP_PRINT(&job, &cp_file, CP_COMBINER_STRUCT);
        CP_PRINT(&job, &cp_file, CP_COMBINER_SUBARRAY);
        CP_PRINT(&job, &cp_file, CP_COMBINER_DARRAY);
        CP_PRINT(&job, &cp_file, CP_COMBINER_F90_REAL);
        CP_PRINT(&job, &cp_file, CP_COMBINER_F90_COMPLEX);
        CP_PRINT(&job, &cp_file, CP_COMBINER_F90_INTEGER);
        CP_PRINT(&job, &cp_file, CP_COMBINER_RESIZED);
        CP_PRINT(&job, &cp_file, CP_HINTS);
        CP_PRINT(&job, &cp_file, CP_VIEWS);
        CP_PRINT(&job, &cp_file, CP_MODE);
        CP_PRINT(&job, &cp_file, CP_BYTES_READ);
        CP_PRINT(&job, &cp_file, CP_BYTES_WRITTEN);
        CP_PRINT(&job, &cp_file, CP_MAX_BYTE_READ);
        CP_PRINT(&job, &cp_file, CP_MAX_BYTE_WRITTEN);
        CP_PRINT(&job, &cp_file, CP_CONSEC_READS);
        CP_PRINT(&job, &cp_file, CP_CONSEC_WRITES);
        CP_PRINT(&job, &cp_file, CP_SEQ_READS);
        CP_PRINT(&job, &cp_file, CP_SEQ_WRITES);
        CP_PRINT(&job, &cp_file, CP_RW_SWITCHES);
        CP_PRINT(&job, &cp_file, CP_MEM_NOT_ALIGNED);
        CP_PRINT(&job, &cp_file, CP_MEM_ALIGNMENT);
        CP_PRINT(&job, &cp_file, CP_FILE_NOT_ALIGNED);
        CP_PRINT(&job, &cp_file, CP_FILE_ALIGNMENT);
        CP_PRINT(&job, &cp_file, CP_SIZE_READ_0_100);
        CP_PRINT(&job, &cp_file, CP_SIZE_READ_100_1K);
        CP_PRINT(&job, &cp_file, CP_SIZE_READ_1K_10K);
        CP_PRINT(&job, &cp_file, CP_SIZE_READ_10K_100K);
        CP_PRINT(&job, &cp_file, CP_SIZE_READ_100K_1M);
        CP_PRINT(&job, &cp_file, CP_SIZE_READ_1M_4M);
        CP_PRINT(&job, &cp_file, CP_SIZE_READ_4M_10M);
        CP_PRINT(&job, &cp_file, CP_SIZE_READ_10M_100M);
        CP_PRINT(&job, &cp_file, CP_SIZE_READ_100M_1G);
        CP_PRINT(&job, &cp_file, CP_SIZE_READ_1G_PLUS);
        CP_PRINT(&job, &cp_file, CP_SIZE_READ_AGG_0_100);
        CP_PRINT(&job, &cp_file, CP_SIZE_READ_AGG_100_1K);
        CP_PRINT(&job, &cp_file, CP_SIZE_READ_AGG_1K_10K);
        CP_PRINT(&job, &cp_file, CP_SIZE_READ_AGG_10K_100K);
        CP_PRINT(&job, &cp_file, CP_SIZE_READ_AGG_100K_1M);
        CP_PRINT(&job, &cp_file, CP_SIZE_READ_AGG_1M_4M);
        CP_PRINT(&job, &cp_file, CP_SIZE_READ_AGG_4M_10M);
        CP_PRINT(&job, &cp_file, CP_SIZE_READ_AGG_10M_100M);
        CP_PRINT(&job, &cp_file, CP_SIZE_READ_AGG_100M_1G);
        CP_PRINT(&job, &cp_file, CP_SIZE_READ_AGG_1G_PLUS);
        CP_PRINT(&job, &cp_file, CP_EXTENT_READ_0_100);
        CP_PRINT(&job, &cp_file, CP_EXTENT_READ_100_1K);
        CP_PRINT(&job, &cp_file, CP_EXTENT_READ_1K_10K);
        CP_PRINT(&job, &cp_file, CP_EXTENT_READ_10K_100K);
        CP_PRINT(&job, &cp_file, CP_EXTENT_READ_100K_1M);
        CP_PRINT(&job, &cp_file, CP_EXTENT_READ_1M_4M);
        CP_PRINT(&job, &cp_file, CP_EXTENT_READ_4M_10M);
        CP_PRINT(&job, &cp_file, CP_EXTENT_READ_10M_100M);
        CP_PRINT(&job, &cp_file, CP_EXTENT_READ_100M_1G);
        CP_PRINT(&job, &cp_file, CP_EXTENT_READ_1G_PLUS);
        CP_PRINT(&job, &cp_file, CP_SIZE_WRITE_0_100);
        CP_PRINT(&job, &cp_file, CP_SIZE_WRITE_100_1K);
        CP_PRINT(&job, &cp_file, CP_SIZE_WRITE_1K_10K);
        CP_PRINT(&job, &cp_file, CP_SIZE_WRITE_10K_100K);
        CP_PRINT(&job, &cp_file, CP_SIZE_WRITE_100K_1M);
        CP_PRINT(&job, &cp_file, CP_SIZE_WRITE_1M_4M);
        CP_PRINT(&job, &cp_file, CP_SIZE_WRITE_4M_10M);
        CP_PRINT(&job, &cp_file, CP_SIZE_WRITE_10M_100M);
        CP_PRINT(&job, &cp_file, CP_SIZE_WRITE_100M_1G);
        CP_PRINT(&job, &cp_file, CP_SIZE_WRITE_1G_PLUS);
        CP_PRINT(&job, &cp_file, CP_SIZE_WRITE_AGG_0_100);
        CP_PRINT(&job, &cp_file, CP_SIZE_WRITE_AGG_100_1K);
        CP_PRINT(&job, &cp_file, CP_SIZE_WRITE_AGG_1K_10K);
        CP_PRINT(&job, &cp_file, CP_SIZE_WRITE_AGG_10K_100K);
        CP_PRINT(&job, &cp_file, CP_SIZE_WRITE_AGG_100K_1M);
        CP_PRINT(&job, &cp_file, CP_SIZE_WRITE_AGG_1M_4M);
        CP_PRINT(&job, &cp_file, CP_SIZE_WRITE_AGG_4M_10M);
        CP_PRINT(&job, &cp_file, CP_SIZE_WRITE_AGG_10M_100M);
        CP_PRINT(&job, &cp_file, CP_SIZE_WRITE_AGG_100M_1G);
        CP_PRINT(&job, &cp_file, CP_SIZE_WRITE_AGG_1G_PLUS);
        CP_PRINT(&job, &cp_file, CP_EXTENT_WRITE_0_100);
        CP_PRINT(&job, &cp_file, CP_EXTENT_WRITE_100_1K);
        CP_PRINT(&job, &cp_file, CP_EXTENT_WRITE_1K_10K);
        CP_PRINT(&job, &cp_file, CP_EXTENT_WRITE_10K_100K);
        CP_PRINT(&job, &cp_file, CP_EXTENT_WRITE_100K_1M);
        CP_PRINT(&job, &cp_file, CP_EXTENT_WRITE_1M_4M);
        CP_PRINT(&job, &cp_file, CP_EXTENT_WRITE_4M_10M);
        CP_PRINT(&job, &cp_file, CP_EXTENT_WRITE_10M_100M);
        CP_PRINT(&job, &cp_file, CP_EXTENT_WRITE_100M_1G);
        CP_PRINT(&job, &cp_file, CP_EXTENT_WRITE_1G_PLUS);
        CP_PRINT(&job, &cp_file, CP_STRIDE1_STRIDE);
        CP_PRINT(&job, &cp_file, CP_STRIDE1_COUNT);
        CP_PRINT(&job, &cp_file, CP_STRIDE2_STRIDE);
        CP_PRINT(&job, &cp_file, CP_STRIDE2_COUNT);
        CP_PRINT(&job, &cp_file, CP_STRIDE3_STRIDE);
        CP_PRINT(&job, &cp_file, CP_STRIDE3_COUNT);
        CP_PRINT(&job, &cp_file, CP_STRIDE4_STRIDE);
        CP_PRINT(&job, &cp_file, CP_STRIDE4_COUNT);
        CP_PRINT(&job, &cp_file, CP_ACCESS1_ACCESS);
        CP_PRINT(&job, &cp_file, CP_ACCESS1_COUNT);
        CP_PRINT(&job, &cp_file, CP_ACCESS2_ACCESS);
        CP_PRINT(&job, &cp_file, CP_ACCESS2_COUNT);
        CP_PRINT(&job, &cp_file, CP_ACCESS3_ACCESS);
        CP_PRINT(&job, &cp_file, CP_ACCESS3_COUNT);
        CP_PRINT(&job, &cp_file, CP_ACCESS4_ACCESS);
        CP_PRINT(&job, &cp_file, CP_ACCESS4_COUNT);

        CP_F_PRINT(&job, &cp_file, CP_F_OPEN_TIMESTAMP);
        CP_F_PRINT(&job, &cp_file, CP_F_CLOSE_TIMESTAMP);
        CP_F_PRINT(&job, &cp_file, CP_F_READ_START_TIMESTAMP);
        CP_F_PRINT(&job, &cp_file, CP_F_READ_END_TIMESTAMP);
        CP_F_PRINT(&job, &cp_file, CP_F_WRITE_START_TIMESTAMP);
        CP_F_PRINT(&job, &cp_file, CP_F_WRITE_END_TIMESTAMP);
        CP_F_PRINT(&job, &cp_file, CP_F_POSIX_READ_TIME);
        CP_F_PRINT(&job, &cp_file, CP_F_POSIX_WRITE_TIME);
        CP_F_PRINT(&job, &cp_file, CP_F_POSIX_META_TIME);
        CP_F_PRINT(&job, &cp_file, CP_F_MPI_META_TIME);
        CP_F_PRINT(&job, &cp_file, CP_F_MPI_READ_TIME);
        CP_F_PRINT(&job, &cp_file, CP_F_MPI_WRITE_TIME);
    }

    if(ret > 0 && ret < sizeof(cp_file))
    {
        fprintf(stderr, "Error: log file has invalid size.\n");
        gzclose(file);
        return(-1);
    }
    if(!gzeof(file))
    {
        perror("gzread");
        gzclose(file);
        return(-1);
    }

    gzclose(file);
    return(0);
}
