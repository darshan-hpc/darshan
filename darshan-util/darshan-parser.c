/*
 * Copyright (C) 2015 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
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

#include "uthash-1.9.2/src/uthash.h"

#include "darshan-logutils.h"

int main(int argc, char **argv)
{
    int ret;
    int i, j;
    char *filename;
    char tmp_string[4096] = {0};
    darshan_fd fd;
    struct darshan_header header;
    struct darshan_job job;
    struct darshan_record_ref *rec_hash = NULL;
    struct darshan_record_ref *ref, *tmp;
    int mount_count;
    char** mnt_pts;
    char** fs_types;
    time_t tmp_time = 0;
    char *token;
    char *save;
    char buffer[DARSHAN_JOB_METADATA_LEN];
    int empty_mods = 0;
    char *mod_buf;
    int mod_buf_sz;

    /* TODO: argument parsing */
    assert(argc == 2);
    filename = argv[1];

    fd = darshan_log_open(filename, "r");
    if(!fd)
    {
        fprintf(stderr, "darshan_log_open() failed to open %s\n.", filename);
        return(-1);
    }

    /* read darshan log header */
    ret = darshan_log_getheader(fd, &header);
    if(ret < 0)
    {
        fprintf(stderr, "darshan_log_getheader() failed to read log header.\n");
        darshan_log_close(fd);
        return(-1);
    }

    /* read darshan job info */
    ret = darshan_log_getjob(fd, &job);
    if(ret < 0)
    {
        fprintf(stderr, "darshan_log_getjob() failed to read job data.\n");
        darshan_log_close(fd);
        return(-1);
    }

    /* get the original command line for this job */
    ret = darshan_log_getexe(fd, tmp_string);
    if(ret < 0)
    {
        fprintf(stderr, "Error: unable to read trailing job information.\n");
        darshan_log_close(fd);
        return(-1);
    }

    /* get the mount information for this log */
    ret = darshan_log_getmounts(fd, &mnt_pts, &fs_types, &mount_count);
    if(ret < 0)
    {
        fprintf(stderr, "darshan_log_getmounts() failed to read mount information.\n");
        darshan_log_close(fd);
        return(-1);
    }

    /* read hash of darshan records */
    ret = darshan_log_gethash(fd, &rec_hash);
    if(ret < 0)
    {
        fprintf(stderr, "darshan_log_getmap() failed to read record map.\n");
        darshan_log_close(fd);
        return(-1);
    }

    /* print job summary */
    printf("# darshan log version: %s\n", header.version_string);
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

    /* print breakdown of each log file component's contribution to file size */
    printf("\n# log file component sizes (compressed)\n");
    printf("# -------------------------------------------------------\n");
    printf("# header: %zu bytes (uncompressed)\n", sizeof(struct darshan_header));
    printf("# job data: %zu bytes\n", header.rec_map.off - sizeof(struct darshan_header));
    printf("# record table: %zu bytes\n", header.rec_map.len);
    for(i=0; i<DARSHAN_MAX_MODS; i++)
    {
        if(header.mod_map[i].len)
        {
            printf("# %s module: %zu bytes\n", darshan_module_names[i],
                header.mod_map[i].len);
        }
    }

    /* print table of mounted file systems */
    printf("\n# mounted file systems (mount point and fs type)\n");
    printf("# -------------------------------------------------------\n");
    for(i=0; i<mount_count; i++)
    {
        printf("# mount entry:\t%s\t%s\n", mnt_pts[i], fs_types[i]);
    }

    mod_buf = malloc(DARSHAN_DEF_COMP_BUF_SZ);
    if(!mod_buf)
        return(-1);

    DARSHAN_PRINT_HEADER();
    /* TODO: does each module print header of what each counter means??? */

    for(i=0; i<DARSHAN_MAX_MODS; i++)
    {
        int mod_bytes_left;
        void *mod_buf_p;
        void *rec_p = NULL;
        darshan_record_id rec_id;

        memset(mod_buf, 0, DARSHAN_DEF_COMP_BUF_SZ);
        mod_buf_sz = DARSHAN_DEF_COMP_BUF_SZ;

        /* check each module for any data */
        ret = darshan_log_getmod(fd, i, mod_buf, &mod_buf_sz);
        if(ret < 0)
        {
            fprintf(stderr, "Error: failed to get module %s data\n",
                darshan_module_names[i]);
            fflush(stderr);
            darshan_log_close(fd);
            return(-1);
        }
        else if(ret == 0)
        {
            /* skip modules not present in log file */
            empty_mods++;
            continue;
        }

        /* skip modules with no defined logutil handlers */
        if(!mod_logutils[i])
        {
            fprintf(stderr, "Warning: no log utility handlers defined "
                "for module %s, SKIPPING\n", darshan_module_names[i]);
            continue;
        }

        /* this module has data to be parsed and printed */
        mod_buf_p = mod_buf;
        mod_bytes_left = mod_buf_sz;

        /* loop over each of this module's records and print them */
        while (mod_bytes_left > 0)
        {
            char *mnt_pt = NULL;
            char *fs_type = NULL;

            ret = mod_logutils[i]->log_get_record(fd, &mod_buf_p, &mod_bytes_left,
                &rec_p, &rec_id);
            if(ret < 0)
            {
                fprintf(stderr, "Error: failed to parse module %s data record\n",
                    darshan_module_names[i]);
                fflush(stderr);
                darshan_log_close(fd);
                return(-1);
            }

            /* get the pathname for this record */
            HASH_FIND(hlink, rec_hash, &rec_id, sizeof(darshan_record_id), ref);
            assert(ref);

            /* get mount point and fs type associated with this record */
            for(j=0; j<mount_count; j++)
            {
                if(strncmp(mnt_pts[j], ref->rec.name, strlen(mnt_pts[j])) == 0)
                {
                    mnt_pt = mnt_pts[j];
                    fs_type = fs_types[j];
                    break;
                }
            }
            if(!mnt_pt)
                mnt_pt = "UNKNOWN";
            if(!fs_type)
                fs_type = "UNKNOWN";

            /* print the corresponding module data for this record */
            mod_logutils[i]->log_print_record(rec_p, ref->rec.name,
                mnt_pt, fs_type);
        }
    }
    if(empty_mods == DARSHAN_MAX_MODS)
        printf("# no module data available.\n");

    /* free record hash data */
    HASH_ITER(hlink, rec_hash, ref, tmp)
    {
        HASH_DELETE(hlink, rec_hash, ref);
        free(ref->rec.name);
        free(ref);
    }

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

    free(mod_buf);
    darshan_log_close(fd);

    return(0);
}
