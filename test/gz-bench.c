/*
 *  (C) 2009 by Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
 */

#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <zlib.h>

#include "darshan-log-format.h"

#define BUFSIZE (10*1024*1024)

int main(int argc, char **argv)
{
    gzFile file;
    int ret;
    struct darshan_job job;
    struct darshan_file cp_file;
    char tmp_string[1024];
    int no_files_flag = 0;
    void* big_buffer;
    void* gz_buffer;
    int size;
    int num_records;
    int i;
    z_stream tmp_stream;
    double percentage;

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

    printf("# size of file statistics: %zu bytes\n", sizeof(cp_file));
    printf("# exe: %s\n", tmp_string);
    printf("# uid: %PRId64\n", job.uid);
    printf("# start_time: %ld\n", (long)job.start_time);
    printf("# end_time: %ld\n", (long)job.end_time);
    printf("# nprocs: %PRId64\n", job.nprocs);
    printf("# run time: %ld\n", (long)(job.end_time - job.start_time + 1));
   
    if(no_files_flag)
    {
        /* it looks like the app didn't open any files */
        printf("# no files opened.\n");
        gzclose(file);
        return(0);
    }

    /* allocate buffers to experiment with compression */
    big_buffer = malloc(BUFSIZE);
    if(!big_buffer)
    {
        perror("malloc");
        gzclose(file);
        return(-1);
    }
    gz_buffer = malloc(BUFSIZE);
    if(!gz_buffer)
    {
        perror("malloc");
        gzclose(file);
        return(-1);
    }

    /* read in as many file records as we can */
    size = gzread(file, big_buffer, BUFSIZE);
    if(size < 1 || (size % sizeof(cp_file)))
    {
        fprintf(stderr, "Error: got weird size out of file: %d.\n", size);
        gzclose(file);
        return(-1);
    }

    num_records = size/sizeof(cp_file);
    printf("# number of records available: %d\n", num_records);
    printf("# using gzip default compression settings.\n");
    printf("<records>\t<native size>\t<compressed size>\t<percentage>\n");

    /* loop through and compress various numbers of records */
    /* TODO: we could loop more and time this as well to get timing info
     * while we are at it?
     */
    for(i=1; i<=num_records; i++)
    {
        memset(&tmp_stream, 0, sizeof(tmp_stream));
        
        /* set up state for compression */
        tmp_stream.zalloc = Z_NULL;
        tmp_stream.zfree = Z_NULL;
        tmp_stream.opaque = Z_NULL;

        /* TODO: what about trying different compression/memory settings? */
        ret = deflateInit2(&tmp_stream, Z_DEFAULT_COMPRESSION, Z_DEFLATED, 
            31, 8, Z_DEFAULT_STRATEGY);
        if(ret != Z_OK)
        {
            fprintf(stderr, "Error: deflateInit2() failure.\n");
            gzclose(file);
            return(-1);
        }

        tmp_stream.next_in = big_buffer;
        tmp_stream.avail_in = i*sizeof(cp_file);
        tmp_stream.next_out = gz_buffer;
        tmp_stream.avail_out = BUFSIZE;

        ret = deflate(&tmp_stream, Z_FINISH);
        if(ret != Z_STREAM_END)
        {
            fprintf(stderr, "Error: deflate() failure.\n");
            gzclose(file);
            return(-1);
        }

        /* NOTE: the results of deflate in this example can be appended to 
         * a .gz file and maintain gzip compatibility
         */

        percentage = ((double)(i*sizeof(cp_file)));
        percentage -= tmp_stream.total_out;
        percentage = percentage / (i*sizeof(cp_file));

        printf("%d\t%zu\t%lu\t%f\n", i, i*sizeof(cp_file), tmp_stream.total_out,
            percentage);
        
        deflateEnd(&tmp_stream);
    }

    gzclose(file);
    return(0);
}
