/*
 * Copyright (C) 2015 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */
#ifdef HAVE_CONFIG_H
# include "darshan-util-config.h"
#endif

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

void obfuscate_filenames(int key, struct darshan_name_record_ref *name_hash, struct darshan_mnt_info *mnt_data_array, int mount_count )
{
    struct darshan_name_record_ref *ref, *tmp;
    uint32_t hashed;
    char tmp_string[PATH_MAX+128] = {0};
    darshan_record_id tmp_id;

    HASH_ITER(hlink, name_hash, ref, tmp)
    {
        /* find file system */
        int j;
        char *mnt_pt = NULL;

        /* get mount point and fs type associated with this record */
        for(j=0; j<mount_count; j++)
        {
            if(strncmp(mnt_data_array[j].mnt_path, ref->name_record->name,
                strlen(mnt_data_array[j].mnt_path)) == 0)
            {
                mnt_pt = mnt_data_array[j].mnt_path;
                break;
            }
        }

        tmp_id = ref->name_record->id;
        hashed = darshan_hashlittle(ref->name_record->name,
            strlen(ref->name_record->name), key);
        if ( mnt_pt != NULL ) 
        {
            sprintf(tmp_string, "%s/%u", mnt_pt, hashed);
        }
        else 
        {
            sprintf(tmp_string, "%u", hashed);
        }
        free(ref->name_record);
        ref->name_record = malloc(sizeof(struct darshan_name_record) +
            strlen(tmp_string));
        assert(ref->name_record);
        ref->name_record->id = tmp_id;
        strcpy(ref->name_record->name, tmp_string);
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

static void remove_hash_recs(struct darshan_name_record_ref **name_hash,
    darshan_record_id hash)
{
    struct darshan_name_record_ref *ref, *tmp;

    HASH_ITER(hlink, *name_hash, ref, tmp)
    {
        if(ref->name_record->id != hash)
        {
            HASH_DELETE(hlink, *name_hash, ref);
            free(ref->name_record);
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
    struct darshan_job job;
    char tmp_string[4096] = {0};
    darshan_fd infile;
    darshan_fd outfile;
    int i;
    int mount_count;
    struct darshan_mnt_info *mnt_data_array;
    struct darshan_name_record_ref *name_hash = NULL;
    struct darshan_name_record_ref *ref, *tmp;
    char *mod_buf, *tmp_mod_buf;
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
        return(-1);
 
    comp_type = bzip2 ? DARSHAN_BZIP2_COMP : DARSHAN_ZLIB_COMP;
    outfile = darshan_log_create(outfile_name, comp_type, infile->partial_flag);
    if(!outfile)
    {
        darshan_log_close(infile);
        return(-1);
    }

    /* read job info */
    ret = darshan_log_get_job(infile, &job);
    if(ret < 0)
    {
        darshan_log_close(infile);
        darshan_log_close(outfile);
        unlink(outfile_name);
        return(-1);
    }

    if (reset_md) reset_md_job(&job);
    if (obfuscate) obfuscate_job(key, &job);
    if (annotation) add_annotation(annotation, &job);

    ret = darshan_log_put_job(outfile, &job);
    if (ret < 0)
    {
        darshan_log_close(infile);
        darshan_log_close(outfile);
        return(-1);
    }

    ret = darshan_log_get_exe(infile, tmp_string);
    if(ret < 0)
    {
        darshan_log_close(infile);
        darshan_log_close(outfile);
        unlink(outfile_name);
        return(-1);
    }

    if (obfuscate) obfuscate_exe(key, tmp_string);

    ret = darshan_log_put_exe(outfile, tmp_string);
    if(ret < 0)
    {
        darshan_log_close(infile);
        darshan_log_close(outfile);
        return(-1);
    }

    ret = darshan_log_get_mounts(infile, &mnt_data_array, &mount_count);
    if(ret < 0)
    {
        darshan_log_close(infile);
        darshan_log_close(outfile);
        unlink(outfile_name);
        return(-1);
    }

    ret = darshan_log_put_mounts(outfile, mnt_data_array, mount_count);
    if(ret < 0)
    {
        darshan_log_close(infile);
        darshan_log_close(outfile);
        return(-1);
    }

    ret = darshan_log_get_namehash(infile, &name_hash);
    if(ret < 0)
    {
        darshan_log_close(infile);
        darshan_log_close(outfile);
        unlink(outfile_name);
        return(-1);
    }

    /* NOTE: obfuscating filepaths breaks the ability to map files
     * to the corresponding FS & mount info maintained by darshan
     */
    if(obfuscate) obfuscate_filenames(key, name_hash, mnt_data_array, mount_count );
    if(hash) remove_hash_recs(&name_hash, hash);

    ret = darshan_log_put_namehash(outfile, name_hash);
    if(ret < 0)
    {
        darshan_log_close(infile);
        darshan_log_close(outfile);
        unlink(outfile_name);
        return(-1);
    }

    mod_buf = malloc(DEF_MOD_BUF_SIZE);
    if(!mod_buf)
    {
        darshan_log_close(infile);
        darshan_log_close(outfile);
        unlink(outfile_name);
        return(-1);
    }

    /* loop over each module and convert it's data to the new format */
    for(i=0; i<DARSHAN_MAX_MODS; i++)
    {
        struct darshan_base_record *base_rec;

        /* check each module for any data */
        if(infile->mod_map[i].len == 0)
            continue;
        else if(!mod_logutils[i])
        {
            fprintf(stderr, "Warning: no log utility handlers defined "
                "for module %s, SKIPPING.\n", darshan_module_names[i]);
            continue;
        }

        /* for dxt, don't use static record buffer and instead have
         * darshan-logutils malloc us memory for the trace data
         */
        if(i == DXT_POSIX_MOD || i == DXT_MPIIO_MOD)
        {
            tmp_mod_buf = NULL;
        }
        else
        {
            tmp_mod_buf = mod_buf;
            memset(tmp_mod_buf, 0, DEF_MOD_BUF_SIZE);
        }

        /* loop over each of the module's records and convert */
        while((ret = mod_logutils[i]->log_get_record(infile, (void **)&tmp_mod_buf)) == 1)
        {
            base_rec = (struct darshan_base_record *)tmp_mod_buf;

            if(!hash || hash == base_rec->id)
            {
                ret = mod_logutils[i]->log_put_record(outfile, tmp_mod_buf);
                if(ret < 0)
                {
                    if(i == DXT_POSIX_MOD || i == DXT_MPIIO_MOD)
                        free(tmp_mod_buf);
                    darshan_log_close(infile);
                    darshan_log_close(outfile);
                    unlink(outfile_name);
                    return(-1);
                }
            }

            if(i == DXT_POSIX_MOD || i == DXT_MPIIO_MOD)
            {
                free(tmp_mod_buf);
                tmp_mod_buf = NULL;
            }
            else
            {
                memset(tmp_mod_buf, 0, DEF_MOD_BUF_SIZE);
            }
        }
        if(ret < 0)
        {
            fprintf(stderr, "Error: failed to parse %s module record.\n",
                darshan_module_names[i]);
            darshan_log_close(infile);
            darshan_log_close(outfile);
            unlink(outfile_name);
            return(-1);
        }
    }

    darshan_log_close(infile);
    darshan_log_close(outfile);

    if(mount_count > 0)
        free(mnt_data_array);

    HASH_ITER(hlink, name_hash, ref, tmp)
    {
        HASH_DELETE(hlink, name_hash, ref);
        free(ref->name_record);
        free(ref);
    }

    free(mod_buf);

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
