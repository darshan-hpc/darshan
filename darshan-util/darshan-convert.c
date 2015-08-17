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
#include <errno.h>

#include "darshan-logutils.h"

extern uint32_t darshan_hashlittle(const void *key, size_t length, uint32_t initval);

int usage (char *exename)
{
    fprintf(stderr, "Usage: %s [options] <infile> <outfile>\n", exename);
    fprintf(stderr, "       Converts darshan log from infile to outfile.\n");
    fprintf(stderr, "       rewrites the log file into the newest format.\n");
    fprintf(stderr, "       --obfuscate Obfuscate items in the log.\n");
    fprintf(stderr, "       --key <key> Key to use when obfuscating.\n");
    fprintf(stderr, "       --annotate <string> Additional metadata to add.\n");
    fprintf(stderr, "       --file <hash> Limit output to specified (hashed) file only.\n");
    fprintf(stderr, "       --reset-md Reset old metadata during conversion.\n");

    exit(1);
}

void parse_args (int argc, char **argv, char **infile, char **outfile,
                 int *obfuscate, int *reset_md, int *key, char **annotate, uint64_t* hash)
{
    int index;
    int ret;

    static struct option long_opts[] =
    {
        {"annotate", 1, NULL, 'a'},
        {"obfuscate", 0, NULL, 'o'},
        {"reset-md", 0, NULL, 'r'},
        {"key", 1, NULL, 'k'},
        {"file", 1, NULL, 'f'},
        {"help",  0, NULL, 0},
        { 0, 0, 0, 0 }
    };

    *reset_md = 0;
    *hash = 0;

    while(1)
    {
        int c = getopt_long(argc, argv, "", long_opts, &index);

        if (c == -1) break;

        switch(c)
        {
            case 'a':
                *annotate = optarg;
                break;
            case 'o':
                *obfuscate = 1;
                break;
            case 'r':
                *reset_md = 1;
                break;
            case 'k':
                *key = atoi(optarg);
                break;
            case 'f':
                ret = sscanf(optarg, "%" PRIu64, hash);
                if(ret != 1)
                    usage(argv[0]);
                break;
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

#if 0
static void reset_md_job(struct darshan_job *job)
{
    job->metadata[0] = '\0';
    return;
}

void obfuscate_job(int key, struct darshan_job *job)
{
    job->uid   = (int64_t) darshan_hashlittle(&job->uid, sizeof(job->uid), key);
    if (job->jobid != 0)
    {
        job->jobid = (int64_t) darshan_hashlittle(&job->jobid, sizeof(job->jobid), key);
    }

    return;
}

void obfuscate_exe(int key, char *exe)
{
    uint32_t hashed;

    hashed = darshan_hashlittle(exe, strlen(exe), key);
    memset(exe, 0, strlen(exe));
    sprintf(exe, "%u", hashed);

    return;
}

void obfuscate_file(int key, struct darshan_file *file)
{
    uint32_t hashed;

    hashed = darshan_hashlittle(file->name_suffix, sizeof(file->name_suffix), key);
    memset(file->name_suffix, 0, sizeof(file->name_suffix));
    sprintf(file->name_suffix, "%u", hashed);

    return;
}

void add_annotation (char *annotation,
                     struct darshan_job *job)
{
    char *token;
    char *save;
    int len;
    
    /* check for newline in existing metadata, insert if needed */
    len = strlen(job->metadata);
    if(len > 0 && len < sizeof(job->metadata))
    {
        if(job->metadata[len-1] != '\n')
        {
            job->metadata[len] = '\n';
            job->metadata[len+1] = '\0';
        }
    }

    /* determine remaining space in metadata string */
    int remaining = sizeof(job->metadata) - strlen(job->metadata);

    for(token=strtok_r(annotation, "\t", &save);
        token != NULL;
        token=strtok_r(NULL, "\t", &save))
    {
        if ((strlen(token)+1) < remaining)
        {
            strcat(job->metadata, token);
            strcat(job->metadata, "\n");
            remaining -= (strlen(token)+1);
        }
        else
        {
            fprintf(stderr,
                    "not enough space left in metadata for: current=%s token=%s (remain=%d:need=%d)\n",
                    job->metadata, token, remaining-1, strlen(token)+1);
        }
    }

    return;
}
#endif

int main(int argc, char **argv)
{
    int ret;
    char *infile_name;
    char *outfile_name;
    struct darshan_header header;
    struct darshan_job job;
    char tmp_string[4096];
    darshan_fd infile;
    darshan_fd outfile;
    int i;
    int mount_count;
    char** mnt_pts;
    struct darshan_record_ref *rec_hash = NULL;
    struct darshan_record_ref *ref, *tmp;
    char *mod_buf;
    int mod_buf_sz;
    char** fs_types;
    int obfuscate = 0;
    int key = 0;
    char *annotation = NULL;
    uint64_t hash;
    int reset_md = 0;

    parse_args(argc, argv, &infile_name, &outfile_name, &obfuscate, &reset_md, &key, &annotation, &hash);

    infile = darshan_log_open(infile_name, "r");
    if(!infile)
    {
        fprintf(stderr, "darshan_log_open() failed to open %s\n.", infile_name);
        return(-1);
    }
 
    outfile = darshan_log_open(outfile_name, "w");
    if(!outfile)
    {
        fprintf(stderr, "darshan_log_open() failed to open %s\n.", outfile_name);
        darshan_log_close(infile);
        return(-1);
    }

    /* read header from input file */
    ret = darshan_log_getheader(infile, &header);
    if(ret < 0)
    {
        fprintf(stderr, "Error: unable to read header from input log file %s.\n", infile_name);
        darshan_log_close(infile);
        darshan_log_close(outfile);
        return(-1);
    }

    /* NOTE: we do not write the header to the output file until the end, as
     * the mapping data stored in this structure may change in the conversion
     * process (particularly, if we are converting between libz/bz2 compression)
     */

    /* read job info */
    ret = darshan_log_getjob(infile, &job);
    if(ret < 0)
    {
        fprintf(stderr, "Error: unable to read job information from log file.\n");
        darshan_log_close(infile);
        darshan_log_close(outfile);
        return(-1);
    }

#if 0
    if (reset_md) reset_md_job(&job);
    if (obfuscate) obfuscate_job(key, &job);
    if (annotation) add_annotation(annotation, &job);
#endif

    ret = darshan_log_putjob(outfile, &job);
    if (ret < 0)
    {
        fprintf(stderr, "Error: unable to write job information to log file.\n");
        darshan_log_close(infile);
        darshan_log_close(outfile);
        return(-1);
    }

    ret = darshan_log_getexe(infile, tmp_string);
    if(ret < 0)
    {
        fprintf(stderr, "Error: unable to read trailing job information.\n");
        darshan_log_close(infile);
        darshan_log_close(outfile);
        return(-1);
    }

#if 0
    if (obfuscate) obfuscate_exe(key, tmp_string);
#endif

    ret = darshan_log_putexe(outfile, tmp_string);
    if(ret < 0)
    {
        fprintf(stderr, "Error: unable to write trailing job information.\n");
        darshan_log_close(infile);
        darshan_log_close(outfile);
        return(-1);
    }

    ret = darshan_log_getmounts(infile, &mnt_pts, &fs_types, &mount_count);
    if(ret < 0)
    {
        fprintf(stderr, "Error: unable to read trailing job information.\n");
        darshan_log_close(infile);
        darshan_log_close(outfile);
        return(-1);
    }

    ret = darshan_log_putmounts(outfile, mnt_pts, fs_types, mount_count);
    if(ret < 0)
    {
        fprintf(stderr, "Error: unable to write mount information.\n");
        darshan_log_close(infile);
        darshan_log_close(outfile);
        return(-1);
    }

    ret = darshan_log_gethash(infile, &rec_hash);
    if(ret < 0)
    {
        fprintf(stderr, "Error: unable to read darshan record hash.\n");
        darshan_log_close(infile);
        darshan_log_close(outfile);
        return(-1);
    }

    ret = darshan_log_puthash(outfile, rec_hash);
    if(ret < 0)
    {
        fprintf(stderr, "Error: unable to write darshan record hash.\n");
        darshan_log_close(infile);
        darshan_log_close(outfile);
        return(-1);
    }

    mod_buf = malloc(DARSHAN_DEF_COMP_BUF_SZ);
    if(!mod_buf)
        return(-1);

    for(i=0; i<DARSHAN_MAX_MODS; i++)
    {
        memset(mod_buf, 0, DARSHAN_DEF_COMP_BUF_SZ);
        mod_buf_sz = DARSHAN_DEF_COMP_BUF_SZ;

        /* check each module for any data */
        ret = darshan_log_getmod(infile, i, mod_buf, &mod_buf_sz);
        if(ret < 0)
        {
            fprintf(stderr, "Error: failed to get module %s data.\n",
                darshan_module_names[i]);
            darshan_log_close(infile);
            darshan_log_close(outfile);
            return(-1);
        }
        else if(ret == 0)
        {
            /* skip modules not present in log file */
            continue;
        }

        /* we have module data to convert */
        ret = darshan_log_putmod(outfile, i, mod_buf, mod_buf_sz);
        if(ret < 0)
        {
            fprintf(stderr, "Error: failed to put module %s data.\n",
                darshan_module_names[i]);
            darshan_log_close(infile);
            darshan_log_close(outfile);
            return(-1);
        }
    }
    free(mod_buf);

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

    HASH_ITER(hlink, rec_hash, ref, tmp)
    {
        HASH_DELETE(hlink, rec_hash, ref);
        free(ref->rec.name);
        free(ref);
    }

    /* write header to output file */
    ret = darshan_log_putheader(outfile);
    if(ret < 0)
    {
        fprintf(stderr, "Error: unable to write header to output log file %s.\n", outfile_name);
        darshan_log_close(infile);
        darshan_log_close(outfile);
        return(-1);
    }

    darshan_log_close(infile);
    darshan_log_close(outfile);

    return(ret);
}

/*
 * Local variables:
 *  c-indent-level: 4
 *  c-basic-offset: 4
 * End:
 *
 * vim: ts=8 sts=4 sw=4 expandtab
 */
