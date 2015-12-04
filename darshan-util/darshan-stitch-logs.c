#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <glob.h>
#include <string.h>
#include <assert.h>

#include "darshan-logutils.h"

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
    darshan_fd out_fd;
    char *out_logname; /* XXX default + configurable? */
    int ret;
    int i, j;
    char *tmplog_dir;
    int job_id;
    glob_t globbuf;
    char glob_pstr[512];
    int tmp_fd;
    struct darshan_job in_job, out_job;
    char in_exe_mnt[DARSHAN_EXE_LEN+1];
    char *mnt_s, *pos;
    char out_exe[DARSHAN_EXE_LEN+1];
    char **out_mnt_pts;
    char **out_fs_types;
    int out_mnt_count = 0;

    /* TODO: are there any checks we should do to ensure tmp logs belong to the same job */
    /* we can't specifically check the job id, since the pid is used if no job scheduler */

    /* TODO: how do we set the output logfile name to be unique, and have necessary semantic info contained */

    /* TODO: more thorough way of cleaning up (free, close, etc.) */

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

    memset(&out_job, 0, sizeof(struct darshan_job));

    /* first pass at stitching together logs:
     *      - compose a final job-level metadata structure
     */
    for(i = 0; i < globbuf.gl_pathc; i++)
    {
        memset(&in_job, 0, sizeof(struct darshan_job));

        tmp_fd = open(globbuf.gl_pathv[i], O_RDONLY);
        if(tmp_fd < 0)
        {
            fprintf(stderr,
                "Error: unable to open Darshan log file %s.\n",
                globbuf.gl_pathv[i]);
            return(-1);
        }

        /* read job-level metadata from the input file */
        ret = pread(tmp_fd, &in_job, sizeof(struct darshan_job), 0);
        if(ret < sizeof(struct darshan_job))
        {
            fprintf(stderr,
                "Error: unable to read job data from Darshan log file %s\n",
                globbuf.gl_pathv[i]);
            return(-1);
        }

        if(i == 0)
        {
            /* get all job data from the first input log */
            memcpy(&out_job, &in_job, sizeof(struct darshan_job));

            ret = pread(tmp_fd, in_exe_mnt, DARSHAN_EXE_LEN+1, sizeof(struct darshan_job));
            if(ret < DARSHAN_EXE_LEN+1)
            {
                fprintf(stderr,
                    "Error: unable to read exe & mount data from Darshan log file %s\n",
                    globbuf.gl_pathv[i]);
                return(-1);
            }

            /* get the exe string */
            mnt_s = strchr(in_exe_mnt, '\n');
            memcpy(out_exe, in_exe_mnt, mnt_s-in_exe_mnt);
            out_exe[mnt_s-in_exe_mnt] = '\0';

            /* build mount structures */
            pos = mnt_s;
            while((pos = strchr(pos, '\n')) != NULL)
            {
                pos++;
                out_mnt_count++;
            }

            if(out_mnt_count)
            {
                out_mnt_pts = malloc(out_mnt_count * sizeof(char *));
                assert(out_mnt_pts);
                out_fs_types = malloc(out_mnt_count * sizeof(char *));
                assert(out_fs_types);

                /* work backwards through the table and parse each line (except for
                 * first, which holds command line information)
                 */
                j = 0;
                while((pos = strrchr(in_exe_mnt, '\n')) != NULL)
                {
                    /* overestimate string lengths */
                    out_mnt_pts[j] = malloc(DARSHAN_EXE_LEN);
                    assert(out_mnt_pts[j]);
                    out_fs_types[j] = malloc(DARSHAN_EXE_LEN);
                    assert(out_fs_types[j]);

                    ret = sscanf(++pos, "%s\t%s", out_fs_types[j], out_mnt_pts[j]);
                    if(ret != 2)
                    {
                        fprintf(stderr,
                            "Error: poorly formatted mount table in darshan log file %s.\n",
                            globbuf.gl_pathv[i]);
                        return(-1);
                    }
                    pos--;
                    *pos = '\0';
                    j++;
                }
            }
        }
        else
        {
            /* potentially update job timestamps */
            if(in_job.start_time < out_job.start_time)
                out_job.start_time = in_job.start_time;
            if(in_job.end_time > out_job.end_time)
                out_job.end_time = in_job.end_time;
        }

        close(tmp_fd);
    }

    /* create the output "stitched together" log */
    out_fd = darshan_log_create("/tmp/test123.darshan", DARSHAN_ZLIB_COMP, 1);
    if(out_fd == NULL)
    {
        fprintf(stderr, "Error: unable to create output darshan log.\n");
        return(-1);
    }

    /* write the darshan job info, exe string, and mount data to output file */
    ret = darshan_log_putjob(out_fd, &out_job);
    if(ret < 0)
    {
        fprintf(stderr, "Error: unable to write job data to output darshan log.\n");
        return(-1);
    }

    ret = darshan_log_putexe(out_fd, out_exe);
    if(ret < 0)
    {
        fprintf(stderr, "Error: unable to write exe string to output darshan log.\n");
        return(-1);
    }

    ret = darshan_log_putmounts(out_fd, out_mnt_pts, out_fs_types, out_mnt_count);
    if(ret < 0)
    {
        fprintf(stderr, "Error: unable to write mount data to output darshan log.\n");
        return(-1);
    }

    void *mod_buf = malloc(2*1024*1024);
    assert(mod_buf);

    /* second pass at stitching together logs:
     *      - append module data to output log file
     */
    for(i = 0; i < globbuf.gl_pathc; i++)
    {
        tmp_fd = open(globbuf.gl_pathv[i], O_RDONLY);
        if(tmp_fd < 0)
        {
            fprintf(stderr,
                "Error: unable to open Darshan log file %s.\n",
                globbuf.gl_pathv[i]);
            return(-1);
        }

        ret = pread(tmp_fd, mod_buf, 2*1024*1024, 4*1024);
        if(ret < (2*1024*1024))
        {
            fprintf(stderr,
                "Error: unable to read module data from Darshan log file %s\n",
                globbuf.gl_pathv[i]);
            return(-1);
        }

        /* iterate and write file records */
        void *mod_buf_p = mod_buf;
        while(1)
        {
            
#if 0
            ret = darshan_log_put_posix_file(out_fd, VOID*);
            if(ret < 0)
            {
                fprintf(stderr,
                    "Error: unable to write module record to output darshan log.\n");
                return(-1);
            }
#endif 

        }

        close(tmp_fd);
    }

    darshan_log_close(out_fd);

    free(mod_buf);
    globfree(&globbuf);

    return(0);
}

#if 0
    /* print out job info to sanity check */
    time_t tmp_time = 0;
    char *token;
    char *save;
    char buffer[DARSHAN_JOB_METADATA_LEN];
    #include <time.h>
    printf("# exe: %s\n", out_exe);
    printf("# uid: %" PRId64 "\n", out_job.uid);
    printf("# jobid: %" PRId64 "\n", out_job.jobid);
    printf("# start_time: %" PRId64 "\n", out_job.start_time);
    tmp_time += out_job.start_time;
    printf("# start_time_asci: %s", ctime(&tmp_time));
    printf("# end_time: %" PRId64 "\n", out_job.end_time);
    tmp_time = 0;
    tmp_time += out_job.end_time;
    printf("# end_time_asci: %s", ctime(&tmp_time));
    printf("# nprocs: %" PRId64 "\n", out_job.nprocs);
    printf("# run time: %" PRId64 "\n", out_job.end_time - out_job.start_time + 1);
    for(token=strtok_r(out_job.metadata, "\n", &save);
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

    /* print table of mounted file systems */
    printf("\n# mounted file systems (mount point and fs type)\n");
    printf("# -------------------------------------------------------\n");
    for(j=0; j<out_mnt_count; j++)
    {
        printf("# mount entry:\t%s\t%s\n", out_mnt_pts[j], out_fs_types[j]);
    }
#endif
