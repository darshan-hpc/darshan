/*
 *  (C) 2009 by Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
 */

#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <zlib.h>
#include <time.h>
#include <stdlib.h>

#include "darshan-logutils.h"

int main(int argc, char **argv)
{
    int ret;
    struct darshan_job job;
    struct darshan_file cp_file;
    char tmp_string[1024];
    int no_files_flag = 0;
    time_t tmp_time = 0;
    darshan_fd file;
    int i;
    int mount_count;
    int* devs;
    char** mnt_pts;
    char** fs_types;

    if(argc != 2)
    {
        fprintf(stderr, "Usage: %s <filename>\n", argv[0]);
        return(-1);
    }

    file = darshan_log_open(argv[1]);
    if(!file)
    {
        perror("darshan_log_open");
        return(-1);
    }
   
    /* read job info */
    ret = darshan_log_getjob(file, &job);
    if(ret < 0)
    {
        fprintf(stderr, "Error: unable to read job information from log file.\n");
        darshan_log_close(file);
        return(-1);
    }

    /* warn user about any missing information in this log format */
    darshan_log_print_version_warnings(&job);

    ret = darshan_log_getexe(file, tmp_string, &no_files_flag);
    if(ret < 0)
    {
        fprintf(stderr, "Error: unable to read trailing job information.\n");
        darshan_log_close(file);
        return(-1);
    }

    /* print job summary */
    printf("# darshan log version: %s\n", job.version_string);
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
 
    /* print table of mounted file systems */
    ret = darshan_log_getmounts(file, &devs, &mnt_pts, &fs_types, &mount_count,
        &no_files_flag);
    printf("\n# mounted file systems (device, fs type, and mount point)\n");
    printf("# -------------------------------------------------------\n");
    for(i=0; i<mount_count; i++)
    {
        printf("# mount entry: %d\t%s\t%s\n", devs[i], mnt_pts[i], fs_types[i]);
    }
  
    if(no_files_flag)
    {
        /* it looks like the app didn't open any files */
        printf("# no files opened.\n");
        darshan_log_close(file);
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
    printf("#   CP_*_NC_OPENS: number of indep. and collective pnetcdf opens.\n");
    printf("#   CP_HDF5_OPENS: number of hdf5 opens.\n");
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
    printf("#   CP_MAX_*_TIME_SIZE: size of the slowest read and write operations.\n");
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
    printf("#   CP_MAX_*_TIME: duration of the slowest read and write operations.\n");

    printf("\n");

    CP_PRINT_HEADER();

    while((ret = darshan_log_getfile(file, &job, &cp_file)) == 1)
    {
        char* mnt_pt = NULL;
        char* fs_type = NULL;
        
        for(i=0; i<mount_count; i++)
        {
            if(cp_file.counters[CP_DEVICE] == devs[i])
            {
                mnt_pt = mnt_pts[i];
                fs_type = fs_types[i];
                break;
            }
        }
        if(!mnt_pt)
            mnt_pt = "UNKNOWN";
        if(!fs_type)
            fs_type = "UNKNOWN";

        for(i=0; i<CP_NUM_INDICES; i++)
        {
            CP_PRINT(&job, &cp_file, i, mnt_pt, fs_type);
        }
        for(i=0; i<CP_F_NUM_INDICES; i++)
        {
            CP_F_PRINT(&job, &cp_file, i, mnt_pt, fs_type);
        }
    }

    if(ret < 0)
    {
        fprintf(stderr, "Error: failed to parse log file.\n");
        return(-1);
    }

    for(i=0; i<mount_count; i++)
    {
        free(mnt_pts[i]);
        free(fs_types[i]);
    }
    free(devs);
    free(mnt_pts);
    free(fs_types);
 
    darshan_log_close(file);
    return(0);
}
