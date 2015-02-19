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
    int i;
    char *filename;
    char tmp_string[4096];
    darshan_fd file;
    struct darshan_header header;
    struct darshan_job job;
    struct darshan_record_ref *rec_hash = NULL;
    struct darshan_record_ref *ref, *tmp;
    int mount_count;
    char** mnt_pts;
    char** fs_types;
    struct darshan_posix_file next_rec;
    time_t tmp_time = 0;
    char *token;
    char *save;
    char buffer[DARSHAN_JOB_METADATA_LEN];

    assert(argc == 2);
    filename = argv[1];

    struct stat sbuf;
    stat(filename, &sbuf);

    file = darshan_log_open(filename, "r");
    if(!file)
    {
        fprintf(stderr, "darshan_log_open() failed to open %s\n.", filename);
        return(-1);
    }

    /* read darshan log header */
    ret = darshan_log_getheader(file, &header);
    if(ret < 0)
    {
        fprintf(stderr, "darshan_log_getheader() failed to read log header.\n");
        darshan_log_close(file);
        return(-1);
    }

    /* read darshan job info */
    ret = darshan_log_getjob(file, &job);
    if(ret < 0)
    {
        fprintf(stderr, "darshan_log_getjob() failed to read job data.\n");
        darshan_log_close(file);
        return(-1);
    }

    /* get the original command line for this job */
    ret = darshan_log_getexe(file, tmp_string);
    if(ret < 0)
    {
        fprintf(stderr, "Error: unable to read trailing job information.\n");
        darshan_log_close(file);
        return(-1);
    }

    /* print job summary */
    printf("# darshan log version: %s\n", header.version_string);
    printf("# size of POSIX file statistics: %zu bytes\n", sizeof(next_rec));
    printf("# size of job statistics: %zu bytes\n", sizeof(job));
    printf("# exe: %s\n", tmp_string);
    printf("# uid: %" PRId64 "\n", job.uid);
    printf("# jobid: %" PRId64 "\n", job.jobid);
    printf("# start_time: %" PRId64 "\n", job.start_time);
    tmp_time += job.start_time;
    printf("# start_time_asci: %s", ctime(&tmp_time));
    printf("# end_time: %" PRId64 "\n", job.end_time);
    tmp_time = 0;
    tmp_time += job.end_time;
    printf("# end_time_asci: %s", ctime(&tmp_time));
    printf("# nprocs: %" PRId64 "\n", job.nprocs);
    printf("# run time: %" PRId64 "\n", job.end_time - job.start_time + 1);
    for(token=strtok_r(job.metadata, "\n", &save);
        token != NULL;
        token=strtok_r(NULL, "\n", &save))
    {
        char *key;
        char *value;
        /* NOTE: we intentionally only split on the first = character.
         * There may be additional = characters in the value portion
         * (for example, when storing mpi-io hints).
         */
        strcpy(buffer, token);
        key = buffer;
        value = index(buffer, '=');
        if(!value)
            continue;
        /* convert = to a null terminator to split key and value */
        value[0] = '\0';
        value++;
        printf("# metadata: %s = %s\n", key, value);
    }

    /* get the mount information for this log */
    ret = darshan_log_getmounts(file, &mnt_pts, &fs_types, &mount_count);
    if(ret < 0)
    {
        fprintf(stderr, "darshan_log_getmounts() failed to read mount information.\n");
        darshan_log_close(file);
        return(-1);
    }

    /* print table of mounted file systems */
    printf("\n# mounted file systems (mount point and fs type)\n");
    printf("# -------------------------------------------------------\n");
    for(i=0; i<mount_count; i++)
    {
        printf("# mount entry:\t%s\t%s\n", mnt_pts[i], fs_types[i]);
    }

    /* read hash of darshan records */
    ret = darshan_log_gethash(file, &rec_hash);
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
    i = 0;
    do{
        struct darshan_record_ref *ref;

        /* get the pathname for this record */
        HASH_FIND(hlink, rec_hash, &next_rec.f_id, sizeof(darshan_record_id), ref);
        assert(ref);

        printf("\tRecord %d: id=%"PRIu64" (path=%s, rank=%"PRId64")\n",
            i, next_rec.f_id, ref->rec.name, next_rec.rank);
        printf("\t\tPOSIX_OPENS:\t%"PRIu64"\n\t\tF_OPEN_TIMESTAMP:\t%lf\n\t\tF_CLOSE_TIMESTAMP:\t%lf\n",
            next_rec.counters[CP_POSIX_OPENS], next_rec.fcounters[CP_F_OPEN_TIMESTAMP],
            next_rec.fcounters[CP_F_CLOSE_TIMESTAMP]);

        i++;
    } while((ret = darshan_log_getfile(file, &next_rec)) == 1);

    /* free mount info */
    for(i=0; i<mount_count; i++)
    {
        free(mnt_pts[i]);
        free(fs_types[i]);
    }
    if(mount_count > 0)
    {
        free(mnt_pts);
        free(fs_types);
    }

    darshan_log_close(file);

    return(0);
}
