/*
 * Copyright (C) 2016 University of Chicago.
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

struct lustre_record_ref
{
    UT_hash_handle hlink;
    struct darshan_lustre_record *rec;
};

extern void dxt_logutils_cleanup();

int usage (char *exename)
{
    fprintf(stderr, "Usage: %s <filename>\n", exename);

    exit(1);
}

int main(int argc, char **argv)
{
    int ret;
    int i, j;
    char *filename;
    char *comp_str;
    char tmp_string[4096] = {0};
    darshan_fd fd;
    struct darshan_job job;
    struct darshan_name_record_ref *name_hash = NULL;
    struct darshan_name_record_ref *name_rec_ref, *tmp_name_rec_ref;
    int mount_count;
    struct darshan_mnt_info *mnt_data_array;
    time_t tmp_time = 0;
    int64_t run_time = 0;
    char *token;
    char *save;
    char buffer[DARSHAN_JOB_METADATA_LEN];
    struct lustre_record_ref *lustre_rec_ref, *tmp_lustre_rec_ref;
    struct lustre_record_ref *lustre_rec_hash = NULL;
    struct darshan_base_record *base_rec;
    char *mod_buf = NULL;
    char *mnt_pt;
    char *fs_type;
    char *rec_name;

    if(argc != 2)
        usage(argv[0]);

    filename = argv[1];

    fd = darshan_log_open(filename);
    if(!fd)
        return(-1);

    /* read darshan job info */
    ret = darshan_log_get_job(fd, &job);
    if(ret < 0)
    {
        darshan_log_close(fd);
        return(-1);
    }

    /* get the original command line for this job */
    ret = darshan_log_get_exe(fd, tmp_string);
    if(ret < 0)
    {
        darshan_log_close(fd);
        return(-1);
    }

    /* get the mount information for this log */
    ret = darshan_log_get_mounts(fd, &mnt_data_array, &mount_count);
    if(ret < 0)
    {
        darshan_log_close(fd);
        return(-1);
    }

    /* read hash of darshan records */
    ret = darshan_log_get_namehash(fd, &name_hash);
    if(ret < 0)
        goto cleanup;

    /* print any warnings related to this log file version */
    darshan_log_print_version_warnings(fd->version);

    if(fd->comp_type == DARSHAN_ZLIB_COMP)
        comp_str = "ZLIB";
    else if (fd->comp_type == DARSHAN_BZIP2_COMP)
        comp_str = "BZIP2";
    else if (fd->comp_type == DARSHAN_NO_COMP)
        comp_str = "NONE";
    else
        comp_str = "UNKNOWN";

    /* print job summary */
    printf("# darshan log version: %s\n", fd->version);
    printf("# compression method: %s\n", comp_str);
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
    if(job.end_time >= job.start_time)
        run_time = job.end_time - job.start_time + 1;
    printf("# run time: %" PRId64 "\n", run_time);
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

    /* print breakdown of each log file region's contribution to file size */
    printf("\n# log file regions\n");
    printf("# -------------------------------------------------------\n");
    printf("# header: %zu bytes (uncompressed)\n", sizeof(struct darshan_header));
    printf("# job data: %zu bytes (compressed)\n", fd->job_map.len);
    printf("# record table: %zu bytes (compressed)\n", fd->name_map.len);
    for(i=0; i<DARSHAN_MAX_MODS; i++)
    {
        if(fd->mod_map[i].len)
        {
            printf("# %s module: %zu bytes (compressed), ver=%d\n",
                darshan_module_names[i], fd->mod_map[i].len, fd->mod_ver[i]);
        }
    }

    /* print table of mounted file systems */
    printf("\n# mounted file systems (mount point and fs type)\n");
    printf("# -------------------------------------------------------\n");
    for(i=0; i<mount_count; i++)
    {
        printf("# mount entry:\t%s\t%s\n", mnt_data_array[i].mnt_path,
            mnt_data_array[i].mnt_type);
    }

    /* just exit if there is no DXT data in this log file */
    if(fd->mod_map[DXT_POSIX_MOD].len == 0 && fd->mod_map[DXT_MPIIO_MOD].len == 0)
    {
        printf("\n# no DXT module data available for this Darshan log.\n");
        goto cleanup;
    }

    if(fd->mod_map[DARSHAN_LUSTRE_MOD].len > 0 &&
        mod_logutils[DARSHAN_LUSTRE_MOD])
    {
        /* get lustre module data, put in hash table */
        while(1)
        {
            lustre_rec_ref = malloc(sizeof(*lustre_rec_ref));
            assert(lustre_rec_ref);
            memset(lustre_rec_ref, 0, sizeof(*lustre_rec_ref));

            ret = mod_logutils[DARSHAN_LUSTRE_MOD]->log_get_record(fd,
                (void **)&(lustre_rec_ref->rec));
            if(ret < 1)
            {
                if(ret == -1)
                {
                    fprintf(stderr, "Error: failed to parse Lustre module record.\n");
                    goto cleanup;
                }
                break;
            }
            else
            {
                HASH_ADD(hlink, lustre_rec_hash, rec->base_rec.id,
                    sizeof(darshan_record_id), lustre_rec_ref);
            }
        } 
    }

    mod_buf = malloc(DEF_MOD_BUF_SIZE);
    if (!mod_buf)
        goto cleanup;

    /* iterate over DXT POSIX records and print them out */
    if(fd->mod_map[DXT_POSIX_MOD].len > 0 && mod_logutils[DXT_POSIX_MOD])
    {
        while(1)
        {
            mnt_pt = NULL;
            fs_type = NULL;
            rec_name = NULL;

            ret = mod_logutils[DXT_POSIX_MOD]->log_get_record(fd, (void **)&mod_buf);
            if(ret < 1)
            {
                if(ret == -1)
                {
                    fprintf(stderr, "Error: failed to parse DXT POSIX module record.\n");
                    goto cleanup;
                }
                break;
            }
            else
            {
                /* got a record */
                base_rec = (struct darshan_base_record *)mod_buf;

                /* get the pathname for this record */
                HASH_FIND(hlink, name_hash, &(base_rec->id),
                    sizeof(darshan_record_id), name_rec_ref);
                if(name_rec_ref)
                {
                    rec_name = name_rec_ref->name_record->name;

                    /* get mount point and fs type associated with this record */
                    for(j=0; j<mount_count; j++)
                    {
                        if(strncmp(mnt_data_array[j].mnt_path, rec_name,
                            strlen(mnt_data_array[j].mnt_path)) == 0)
                        {
                            mnt_pt = mnt_data_array[j].mnt_path;
                            fs_type = mnt_data_array[j].mnt_type;
                            break;
                        }
                    }
                }
                if(!mnt_pt)
                    mnt_pt = "UNKNOWN";
                if(!fs_type)
                    fs_type = "UNKNOWN";

                /* look for corresponding lustre record and print DXT data */
                HASH_FIND(hlink, lustre_rec_hash, &(base_rec->id),
                    sizeof(darshan_record_id), lustre_rec_ref);
                if(lustre_rec_ref)
                {
                    /* Lustre record found, data in lustre_rec_ref->rec */
                }
                else
                {
                    /* no Lustre record found */
                }
            }
        }
    }

    /* iterate over DXT MPI-IO records and print them out */
    if(fd->mod_map[DXT_MPIIO_MOD].len > 0 && mod_logutils[DXT_MPIIO_MOD])
    {
        while(1)
        {
            mnt_pt = NULL;
            fs_type = NULL;
            rec_name = NULL;

            ret = mod_logutils[DXT_MPIIO_MOD]->log_get_record(fd, (void **)&mod_buf);
            if(ret < 1)
            {
                if(ret == -1)
                {
                    fprintf(stderr, "Error: failed to parse DXT POSIX module record.\n");
                    goto cleanup;
                }
                break;
            }
            else
            {
                /* got a record */
                base_rec = (struct darshan_base_record *)mod_buf;

                /* get the pathname for this record */
                HASH_FIND(hlink, name_hash, &(base_rec->id),
                    sizeof(darshan_record_id), name_rec_ref);
                if(name_rec_ref)
                {
                    rec_name = name_rec_ref->name_record->name;

                    /* get mount point and fs type associated with this record */
                    for(j=0; j<mount_count; j++)
                    {   
                        if(strncmp(mnt_data_array[j].mnt_path, rec_name,
                            strlen(mnt_data_array[j].mnt_path)) == 0)
                        {   
                            mnt_pt = mnt_data_array[j].mnt_path;
                            fs_type = mnt_data_array[j].mnt_type;
                            break;
                        }
                    }
                }
                if(!mnt_pt)
                    mnt_pt = "UNKNOWN";
                if(!fs_type)
                    fs_type = "UNKNOWN";
            }
        }
    }

    ret = 0;

cleanup:
    free(mod_buf);

    darshan_log_close(fd);

    /* free record hash data */
    HASH_ITER(hlink, name_hash, name_rec_ref, tmp_name_rec_ref)
    {
        HASH_DELETE(hlink, name_hash, name_rec_ref);
        free(name_rec_ref->name_record);
        free(name_rec_ref);
    }

    /* free lustre record data */
    HASH_ITER(hlink, lustre_rec_hash, lustre_rec_ref, tmp_lustre_rec_ref)
    {
        HASH_DELETE(hlink, lustre_rec_hash, lustre_rec_ref);
        free(lustre_rec_ref->rec);
        free(lustre_rec_ref);
    }

    /* free mount info */
    if(mount_count > 0)
    {
        free(mnt_data_array);
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
