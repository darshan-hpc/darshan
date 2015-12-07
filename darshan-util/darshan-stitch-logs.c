    #include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <glob.h>
#include <string.h>

#include "darshan-logutils.h"

#define DEF_MOD_BUF_SIZE 1024 /* 1 KiB is enough for all current mod records ... */

int logfile_path_comp(const void *a, const void *b)
{
    char *pathA = *(char **)a;
    char *pathB = *(char **)b;
    char *pathA_rank_s, *pathB_rank_s;
    int pathA_rank, pathB_rank;

    /* extract the process rank number from end of each log file path */
    pathA_rank_s = strrchr(pathA, '.') + 1;
    pathA_rank = atoi(pathA_rank_s);
    pathB_rank_s = strrchr(pathB, '.') + 1;
    pathB_rank = atoi(pathB_rank_s);

    if(pathA_rank < pathB_rank)
        return(-1);
    else if(pathA_rank > pathB_rank)
        return(1);
    else
        return(0);
}

int main(int argc, char *argv[])
{
    char *tmplog_dir;
    int job_id;
    glob_t globbuf;
    char glob_pstr[512];
    char *stitch_logname = "/tmp/test123.darshan"; /* XXX default + configurable? */
    darshan_fd in_fd, stitch_fd;
    struct darshan_job in_job, stitch_job;
    char stitch_exe[DARSHAN_EXE_LEN+1];
    char **stitch_mnt_pts;
    char **stitch_fs_types;
    int stitch_mnt_count = 0;
    struct darshan_record_ref *in_hash = NULL;
    struct darshan_record_ref *stitch_hash = NULL;
    struct darshan_record_ref *ref, *tmp, *found;
    darshan_record_id rec_id;
    char *mod_buf;
    int i, j;
    int ret;

    /* TODO: are there any checks we should do to ensure tmp logs belong to the same job */
    /* we can't specifically check the job id, since the pid is used if no job scheduler */

    /* TODO: how do we set the output logfile name to be unique, and have necessary semantic info contained */

    if(argc != 3)
    {
        fprintf(stderr, "Usage: ./darshan-stitch-tmplogs <tmp_dir> <job_id>\n"
            "\t<tmp_dir> is the directory containing the temporary Darshan logs\n"
            "\t<job_id> is the job id of the logs we are trying to stitch\n");
        return(0);
    }

    /* grab command line arguments */
    tmplog_dir = argv[1];
    job_id = atoi(argv[2]);

    /* construct the list of input log files to stitch together */
    snprintf(glob_pstr, 512, "%s/darshan_job%d*", tmplog_dir, job_id);

    ret = glob(glob_pstr, GLOB_NOSORT, NULL, &globbuf);
    if(ret != 0)
    {
        fprintf(stderr,
            "Error: unable to construct list of input Darshan log files.\n");
        return(-1);
    }

    /* sort the file list according to the rank id appended to each logfile name */
    /* NOTE: we don't rely on glob's default alphabetic sorting, because it won't
     * sort by ascending ranks if pid's are used for job ids, for instance
     */
    qsort(globbuf.gl_pathv, globbuf.gl_pathc, sizeof(char *), logfile_path_comp);

    memset(&stitch_job, 0, sizeof(struct darshan_job));

    /* first pass at stitching together logs:
     *      - compose output job-level metadata structure (including exe & mount data)
     *      - compose output record_id->file_name mapping 
     */
    for(i = 0; i < globbuf.gl_pathc; i++)
    {
        memset(&in_job, 0, sizeof(struct darshan_job));

        in_fd = darshan_log_open(globbuf.gl_pathv[i]);
        if(in_fd == NULL)
        {
            fprintf(stderr,
                "Error: unable to open input Darshan log file %s.\n",
                globbuf.gl_pathv[i]);
            globfree(&globbuf);
            return(-1);
        }

        /* read job-level metadata from the input file */
        ret = darshan_log_getjob(in_fd, &in_job);
        if(ret < 0)
        {
            fprintf(stderr,
                "Error: unable to read job data from input Darshan log file %s.\n",
                globbuf.gl_pathv[i]);
            darshan_log_close(in_fd);
            globfree(&globbuf);
            return(-1);
        }

        if(i == 0)
        {
            /* get job data, exe, & mounts directly from the first input log */
            memcpy(&stitch_job, &in_job, sizeof(struct darshan_job));

            ret = darshan_log_getexe(in_fd, stitch_exe);
            if(ret < 0)
            {
                fprintf(stderr,
                    "Error: unable to read exe string from Darshan log file %s.\n",
                    globbuf.gl_pathv[i]);
                darshan_log_close(in_fd);
                globfree(&globbuf);
                return(-1);
            }

            ret = darshan_log_getmounts(in_fd, &stitch_mnt_pts,
                &stitch_fs_types, &stitch_mnt_count);
            if(ret < 0)
            {
                fprintf(stderr,
                    "Error: unable to read mount info from Darshan log file %s.\n",
                    globbuf.gl_pathv[i]);
                darshan_log_close(in_fd);
                globfree(&globbuf);
                return(-1);
            }
        }
        else
        {
            /* potentially update job timestamps using remaining logs */
            if(in_job.start_time < stitch_job.start_time)
                stitch_job.start_time = in_job.start_time;
            if(in_job.end_time > stitch_job.end_time)
                stitch_job.end_time = in_job.end_time;
        }

        ret = darshan_log_gethash(in_fd, &in_hash);
        if(ret < 0)
        {
            fprintf(stderr,
                "Error: unable to read job data from input Darshan log file %s.\n",
                globbuf.gl_pathv[i]);
            darshan_log_close(in_fd);
            globfree(&globbuf);
            return(-1);
        }

        /* iterate the input hash, copying over record_id->file_name mappings
         * that have not already been copied to the output hash
         */
        HASH_ITER(hlink, in_hash, ref, tmp)
        {
            HASH_FIND(hlink, stitch_hash, &(ref->rec.id),
                sizeof(darshan_record_id), found);
            if(!found)
            {
                HASH_ADD(hlink, stitch_hash, rec.id,
                    sizeof(darshan_record_id), ref);
            }
            else if(strcmp(ref->rec.name, found->rec.name))
            {
                fprintf(stderr,
                    "Error: invalid Darshan record table entry.\n");
                darshan_log_close(in_fd);
                globfree(&globbuf);
                return(-1);
            }
        }

        darshan_log_close(in_fd);
    }

    /* create the output "stitched together" log */
    stitch_fd = darshan_log_create(stitch_logname, DARSHAN_ZLIB_COMP, 1);
    if(stitch_fd == NULL)
    {
        fprintf(stderr, "Error: unable to create output darshan log.\n");
        globfree(&globbuf);
        return(-1);
    }

    /* write the darshan job info, exe string, and mount data to output file */
    ret = darshan_log_putjob(stitch_fd, &stitch_job);
    if(ret < 0)
    {
        fprintf(stderr, "Error: unable to write job data to output darshan log.\n");
        globfree(&globbuf);
        darshan_log_close(stitch_fd);
        unlink(stitch_logname);
        return(-1);
    }

    ret = darshan_log_putexe(stitch_fd, stitch_exe);
    if(ret < 0)
    {
        fprintf(stderr, "Error: unable to write exe string to output darshan log.\n");
        globfree(&globbuf);
        darshan_log_close(stitch_fd);
        unlink(stitch_logname);
        return(-1);
    }

    ret = darshan_log_putmounts(stitch_fd, stitch_mnt_pts, stitch_fs_types, stitch_mnt_count);
    if(ret < 0)
    {
        fprintf(stderr, "Error: unable to write mount data to output darshan log.\n");
        globfree(&globbuf);
        darshan_log_close(stitch_fd);
        unlink(stitch_logname);
        return(-1);
    }

    /* write the stitched together table of records to output file */
    ret = darshan_log_puthash(stitch_fd, stitch_hash);
    if(ret < 0)
    {
        fprintf(stderr, "Error: unable to write record table to output darshan log.\n");
        globfree(&globbuf);
        darshan_log_close(stitch_fd);
        unlink(stitch_logname);
        return(-1);
    }

    mod_buf = malloc(DEF_MOD_BUF_SIZE);
    if(!mod_buf)
    {
        globfree(&globbuf);
        darshan_log_close(stitch_fd);
        unlink(stitch_logname);
        return(-1);
    }
    memset(mod_buf, 0, DEF_MOD_BUF_SIZE);

    for(i = 0; i < DARSHAN_MPIIO_MOD; i++)
    {
        if(!mod_logutils[i]) continue;

#if 0
        /* XXX first build shared record list? */
        for(j = 0; j < globbuf.gl_pathc; j++)
        {

        }

        /* XXX second aggregate shared records ? */
        for(j = 0; j < globbuf.gl_pathc; j++)
        {

        }
#endif

        /* XXX third write each rank's blobs, with rank 0 writing the shared ones ? */
        for(j = 0; j < globbuf.gl_pathc; j++)
        {
            in_fd = darshan_log_open(globbuf.gl_pathv[j]);
            if(in_fd == NULL)
            {
                fprintf(stderr,
                    "Error: unable to open input Darshan log file %s.\n",
                    globbuf.gl_pathv[j]);
                free(mod_buf);
                globfree(&globbuf);
                darshan_log_close(in_fd);
                unlink(stitch_logname);
                return(-1);
            }

            /* loop over module records and write them to output file */
            while((ret = mod_logutils[i]->log_get_record(in_fd, mod_buf, &rec_id)) == 1)
            {
                ret = mod_logutils[i]->log_put_record(stitch_fd, mod_buf);
                if(ret < 0)
                {
                    fprintf(stderr,
                        "Error: unable to write %s module record to output log file %s.\n",
                        darshan_module_names[i], globbuf.gl_pathv[j]);
                    free(mod_buf);
                    darshan_log_close(in_fd);
                    unlink(stitch_logname);
                    return(-1);
                }

                memset(mod_buf, 0, DEF_MOD_BUF_SIZE);
            }
            if(ret < 0)
            {
                fprintf(stderr,
                    "Error: unable to read %s module record from input log file %s.\n",
                    darshan_module_names[i], globbuf.gl_pathv[j]);
                free(mod_buf);
                darshan_log_close(in_fd);
                unlink(stitch_logname);
                return(-1);
            }

            darshan_log_close(in_fd);
        }
    }

    darshan_log_close(stitch_fd);
    globfree(&globbuf);
    free(mod_buf);

    return(0);
}
