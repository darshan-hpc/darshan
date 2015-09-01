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
    fprintf(stderr, "       --bzip2 Use bzip2 compression instead of zlib.\n");
    fprintf(stderr, "       --obfuscate Obfuscate items in the log.\n");
    fprintf(stderr, "       --key <key> Key to use when obfuscating.\n");
    fprintf(stderr, "       --annotate <string> Additional metadata to add.\n");
    fprintf(stderr, "       --file <hash> Limit output to specified (hashed) file only.\n");
    fprintf(stderr, "       --reset-md Reset old metadata during conversion.\n");

    exit(1);
}

void parse_args (int argc, char **argv, char **infile, char **outfile,
                 int *bzip2, int *obfuscate, int *reset_md, int *key,
                 char **annotate, uint64_t* hash)
{
    int index;
    int ret;

    static struct option long_opts[] =
    {
        {"bzip2", 0, NULL, 'b'},
        {"annotate", 1, NULL, 'a'},
        {"obfuscate", 0, NULL, 'o'},
        {"reset-md", 0, NULL, 'r'},
        {"key", 1, NULL, 'k'},
        {"file", 1, NULL, 'f'},
        {"help",  0, NULL, 0},
        { 0, 0, 0, 0 }
    };

    *bzip2 = 0;
    *obfuscate = 0;
    *reset_md = 0;
    *key = 0;
    *hash = 0;

    while(1)
    {
        int c = getopt_long(argc, argv, "", long_opts, &index);

        if (c == -1) break;

        switch(c)
        {
            case 'b':
                *bzip2 = 1;
                break;
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

void obfuscate_filenames(int key, struct darshan_record_ref *rec_hash)
{
    struct darshan_record_ref *ref, *tmp;
    uint32_t hashed;
    char tmp_string[32];

    HASH_ITER(hlink, rec_hash, ref, tmp)
    {
        hashed = darshan_hashlittle(ref->rec.name, strlen(ref->rec.name), key);
        sprintf(tmp_string, "%u", hashed);
        free(ref->rec.name);
        ref->rec.name = malloc(strlen(tmp_string));
        assert(ref->rec.name);
        memcpy(ref->rec.name, tmp_string, strlen(tmp_string));
    }

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
                    job->metadata, token, remaining-1, (int)strlen(token)+1);
        }
    }

    return;
}

static void remove_hash_recs(struct darshan_record_ref **rec_hash, darshan_record_id hash)
{
    struct darshan_record_ref *ref, *tmp;

    HASH_ITER(hlink, *rec_hash, ref, tmp)
    {
        if(ref->rec.id != hash)
        {
            HASH_DELETE(hlink, *rec_hash, ref);
            free(ref->rec.name);
            free(ref);
        }
    }

    return;
}

int main(int argc, char **argv)
{
    int ret;
    char *infile_name;
    char *outfile_name;
    struct darshan_header header;
    struct darshan_job job;
    char tmp_string[4096] = {0};
    darshan_fd infile;
    darshan_fd outfile;
    int i;
    int mount_count;
    char** mnt_pts;
    char** fs_types;
    struct darshan_record_ref *rec_hash = NULL;
    struct darshan_record_ref *ref, *tmp;
    char *mod_buf;
    int mod_buf_sz;
    enum darshan_comp_type comp_type;
    int bzip2;
    int obfuscate;
    int key;
    char *annotation = NULL;
    darshan_record_id hash;
    int reset_md;

    parse_args(argc, argv, &infile_name, &outfile_name, &bzip2, &obfuscate,
               &reset_md, &key, &annotation, &hash);

    infile = darshan_log_open(infile_name);
    if(!infile)
    {
        fprintf(stderr, "darshan_log_open() failed to open %s\n.", infile_name);
        return(-1);
    }
 
    comp_type = bzip2 ? comp_type = DARSHAN_BZIP2_COMP : DARSHAN_ZLIB_COMP;
    outfile = darshan_log_create(outfile_name, comp_type);
    if(!outfile)
    {
        fprintf(stderr, "darshan_log_create() failed to create %s\n.", outfile_name);
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

    if (reset_md) reset_md_job(&job);
    if (obfuscate) obfuscate_job(key, &job);
    if (annotation) add_annotation(annotation, &job);

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

    if (obfuscate) obfuscate_exe(key, tmp_string);

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

    /* NOTE: obfuscating filepaths breaks the ability to map files
     * to the corresponding FS & mount info maintained by darshan
     */
    if(obfuscate) obfuscate_filenames(key, rec_hash);
    if(hash) remove_hash_recs(&rec_hash, hash);

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
        int mod_bytes_left;
        int mod_bytes_left_save;
        void *mod_buf_p;
        void *rec_p = NULL;
        darshan_record_id rec_id;

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

        /* skip modules with no defined logutil handlers */
        if(!mod_logutils[i])
        {
            fprintf(stderr, "Warning: no log utility handlers defined "
                "for module %s, SKIPPING\n", darshan_module_names[i]);
            continue;
        }

        /* we have module data to convert */
        /* NOTE: it is necessary to iterate through each module's
         * records to correct any endianness issues before writing
         * this data back to file
         */
        mod_bytes_left = mod_buf_sz;
        mod_buf_p = mod_buf;
        while(mod_bytes_left > 0)
        {
            mod_bytes_left_save = mod_bytes_left;
            ret = mod_logutils[i]->log_get_record(&mod_buf_p, &mod_bytes_left,
                &rec_p, &rec_id, infile->swap_flag);
            if(ret < 0)
            {
                fprintf(stderr, "Error: failed to parse module %s data record\n",
                    darshan_module_names[i]);
                darshan_log_close(infile);
                darshan_log_close(outfile);
                return(-1);
            }

            if(hash == rec_id)
            {
                mod_buf_p = rec_p;
                mod_buf_sz = mod_bytes_left_save - mod_bytes_left;
                break;
            }
            else if(mod_bytes_left == 0)
            {
                mod_buf_p = mod_buf;
            }
        }

        ret = darshan_log_putmod(outfile, i, mod_buf_p, mod_buf_sz);
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
