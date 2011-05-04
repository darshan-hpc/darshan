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


int usage (char *exename)
{
    fprintf(stderr, "Usage: %s [options] <infile> <outfile>\n", exename);
    fprintf(stderr, "       Converts darshan log from infile to outfile.\n");
    fprintf(stderr, "       No options supported yet; this utility simply\n");
    fprintf(stderr, "       rewrites the log file into the newest format.\n");

    exit(1);
}

void parse_args (int argc, char **argv, char **infile, char **outfile)
{
    int index;
    static struct option long_opts[] =
    {
        {"help",  0, NULL, 0}
    };

    while(1)
    {
        int c = getopt_long(argc, argv, "", long_opts, &index);

        if (c == -1) break;

        switch(c)
        {
            case 0:
            case '?':
            default:
                usage(argv[0]);
                break;
        }
    }

    if (optind + 2 == argc)
    {
        *infile = argv[optind];
        *outfile = argv[optind+1];
    }
    else
    {
        usage(argv[0]);
    }

    return;
}

int main(int argc, char **argv)
{
    int ret;
    char *infile_name;
    char *outfile_name;
    struct darshan_job job;
    struct darshan_file cp_file;
    char tmp_string[1024];
    int no_files_flag = 0;
    darshan_fd infile;
    darshan_fd outfile;
    int i;
    int mount_count;
    int64_t* devs;
    char** mnt_pts;
    char** fs_types;
    int last_rank = 0;

    parse_args(argc, argv, &infile_name, &outfile_name);

    infile = darshan_log_open(infile_name, "r");
    if(!infile)
    {
        perror("darshan_log_open");
        return(-1);
    }
 
    /* TODO: safety check that outfile_name doesn't exist; we don't want to
     * overwrite something by accident.
     */
    outfile = darshan_log_open(outfile_name, "w");
    if(!outfile)
    {
        perror("darshan_log_open");
        return(-1);
    }

    /* TODO: for now this tool is just reading the input file and throwing
     * away the data.  Need to write the log_put*() functions and use this
     * program as a test harness
     */
  
    /* read job info */
    ret = darshan_log_getjob(infile, &job);
    if(ret < 0)
    {
        fprintf(stderr, "Error: unable to read job information from log file.\n");
        darshan_log_close(infile);
        return(-1);
    }

    ret = darshan_log_putjob(outfile, &job);
    if (ret < 0)
    {
        fprintf(stderr, "Error: unable to write job information to log file.\n");
        darshan_log_close(outfile);
        return(-1);
    }

    /* warn user about any missing information in this log format */
    darshan_log_print_version_warnings(&job);

    /* TODO: yuck.  It would be nice to have this tool write into the newest
     * format always, but that may lose some information about what fields
     * are and are not valid.  Need to think through the various old log
     * format scenarios.
     */
    ret = darshan_log_getexe(infile, tmp_string, &no_files_flag);
    if(ret < 0)
    {
        fprintf(stderr, "Error: unable to read trailing job information.\n");
        darshan_log_close(infile);
        return(-1);
    }

    ret = darshan_log_putexe(outfile, tmp_string);
    if(ret < 0)
    {
        fprintf(stderr, "Error: unable to write trailing job information.\n");
        darshan_log_close(outfile);
        return(-1);
    }
   
    ret = darshan_log_getmounts(infile, &devs, &mnt_pts, &fs_types, &mount_count,
        &no_files_flag);
    if(ret < 0)
    {
        fprintf(stderr, "Error: unable to read trailing job information.\n");
        darshan_log_close(infile);
        return(-1);
    }

    ret = darshan_log_putmounts(outfile, devs, mnt_pts, fs_types, mount_count);
    if(ret < 0)
    {
        fprintf(stderr, "Error: unable to write mount information.\n");
        darshan_log_close(outfile);
        return(-1);
    }

    if(no_files_flag)
    {
        darshan_log_close(infile);
        darshan_log_close(outfile);
    }

    while((ret = darshan_log_getfile(infile, &job, &cp_file)) == 1)
    {
        if(cp_file.rank != -1 && cp_file.rank < last_rank)
        {
            fprintf(stderr, "Error: log file contains out of order rank data.\n");
            fflush(stderr);
            return(-1);
        }
        if(cp_file.rank != -1)
            last_rank = cp_file.rank;

        ret = darshan_log_putfile(outfile, &job, &cp_file);
        if (ret < 0)
        {
            fprintf(stderr, "Error: failed to write file record.\n");
            break;
        }
    }

    if(ret < 0)
    {
        fprintf(stderr, "Error: failed to process log file.\n");
        fflush(stderr);
    }

    for(i=0; i<mount_count; i++)
    {
        free(mnt_pts[i]);
        free(fs_types[i]);
    }
    if(mount_count > 0)
    {
        free(devs);
        free(mnt_pts);
        free(fs_types);
    }
 
    darshan_log_close(infile);
    darshan_log_close(outfile);

    return(ret);
}

