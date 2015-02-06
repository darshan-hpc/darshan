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
#include <getopt.h>
#include <assert.h>

#include "darshan-logutils.h"
#include "uthash-1.9.2/src/uthash.h"

int main(int argc, char **argv)
{
    int ret;
    char *filename;
    darshan_fd file;
    struct darshan_header header;
    struct darshan_job job;
    struct darshan_record_ref *rec_map = NULL;
    struct darshan_record_ref *ref, *tmp;
    struct darshan_posix_file next_rec;

    assert(argc == 2);
    filename = argv[1];

    struct stat sbuf;
    stat(filename, &sbuf);

    printf("\nDarshan log file size: %"PRId64"\n", sbuf.st_size);
    printf("size of header: %"PRId64"\n", sizeof(struct darshan_header));
    printf("size of index map: %"PRId64"\n", 2*sizeof(int64_t));
    printf("size of job data: %"PRId64"\n", sizeof(struct darshan_job));

    file = darshan_log_open(filename, "r");
    if(!file)
    {
        fprintf(stderr, "darshan_log_open() failed to open %s\n.", filename);
        return(-1);
    }

    /* read the log header */
    ret = darshan_log_getheader(file, &header);
    if(ret < 0)
    {
        fprintf(stderr, "darshan_log_getheader() failed to read log header.\n");
        darshan_log_close(file);
        return(-1);
    }

    printf("\n*** HEADER DATA ***\n");
    printf("\tver=%s\n\tmagic_nr=%"PRId64"\n\tcomp_type=%d\n\tmod_count=%d\n",
        header.version_string, header.magic_nr, header.comp_type, header.mod_count);    

    /* read job info */
    ret = darshan_log_getjob(file, &job);
    if(ret < 0)
    {
        fprintf(stderr, "darshan_log_getjob() failed to read job data.\n");
        darshan_log_close(file);
        return(-1);
    }

    printf("\n*** JOB DATA ***\n");
    printf(
        "\tuid=%"PRId64"\n\tstart_time=%"PRId64"\n\tend_time=%"PRId64"\n\tnprocs=%"PRId64"\n\tjobid=%"PRId64"\n",
        job.uid, job.start_time, job.end_time, job.nprocs, job.jobid);    

    /* read record map */
    ret = darshan_log_getmap(file, &rec_map);
    if(ret < 0)
    {
        fprintf(stderr, "darshan_log_getmap() failed to read record map.\n");
        darshan_log_close(file);
        return(-1);
    }

    /* try to retrieve first record (may not exist) */
    ret = darshan_log_getfile(file, &next_rec);
    if(ret < 0)
    {
        fprintf(stderr, "Error: failed to parse log file.\n");
        fflush(stderr);
        return(-1);
    }
    if(ret == 0)
    {
        /* it looks like the app didn't open any files */
        printf("# no files opened.\n");
        darshan_log_close(file);
        return(0);
    }

    /* iterate the posix file records stored in the darshan log */
    printf("\n*** FILE RECORD DATA ***\n");
    int i = 0;
    do{
        struct darshan_record_ref *ref;

        /* get the pathname for this record */
        HASH_FIND(hlink, rec_map, &next_rec.f_id, sizeof(darshan_record_id), ref);
        assert(ref);

        printf("\tRecord %d: id=%"PRIu64" (path=%s, rank=%"PRId64")\n",
            i, next_rec.f_id, ref->rec.name, next_rec.rank);

        i++;
    } while((ret = darshan_log_getfile(file, &next_rec)) == 1);

    darshan_log_close(file);

    return(0);
}
