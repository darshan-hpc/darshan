/*
 *  (C) 2009 by Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
 */

#define _GNU_SOURCE
#include "darshan-util-config.h"
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <stdlib.h>
#include <unistd.h>
#include <inttypes.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>

#include <zlib.h>
#ifdef HAVE_LIBBZ2
#include <bzlib.h>
#endif

#include "darshan-logutils.h"

struct darshan_fd_s
{
    int pf;
    int64_t pos;
    char version[8];
    int swap_flag;
    char *exe_mnt_data;
    struct darshan_log_map job_map;
    struct darshan_log_map rec_map;
    struct darshan_log_map mod_map[DARSHAN_MAX_MODS];
};

static int darshan_log_seek(darshan_fd fd, off_t offset);
static int darshan_log_read(darshan_fd fd, void *buf, int len);
static int darshan_log_write(darshan_fd fd, void *buf, int len);


/* darshan_log_open()
 *
 * open a darshan log file for reading/writing
 *
 * returns 0 on success, -1 on failure
 */
darshan_fd darshan_log_open(const char *name, const char *mode)
{
    int o_flags;

    /* we only allows "w" or "r" modes, nothing fancy */
    assert(strlen(mode) == 1);
    assert(mode[0] == 'r' || mode[0] == 'w');
    if(mode[0] == 'r')
        o_flags = O_RDONLY;
    else
        o_flags = O_WRONLY;

    darshan_fd tmp_fd = malloc(sizeof(*tmp_fd));
    if(!tmp_fd)
        return(NULL);
    memset(tmp_fd, 0, sizeof(*tmp_fd));

    tmp_fd->pf = open(name, o_flags);
    if(tmp_fd->pf < 0)
    {
        free(tmp_fd);
        return(NULL);
    }

    return(tmp_fd);
}

/* darshan_log_getheader()
 *
 * read the header of the darshan log and set internal data structures
 * NOTE: this function must be called before reading other portions of the log
 *
 * returns 0 on success, -1 on failure
 */
int darshan_log_getheader(darshan_fd fd, struct darshan_header *header)
{
    int i;
    int ret;

    ret = darshan_log_seek(fd, 0);
    if(ret < 0)
    {
        fprintf(stderr, "Error: unable to seek in darshan log file.\n");
        return(ret);
    }

    /* read header from log file */
    ret = darshan_log_read(fd, header, sizeof(*header));
    if(ret < sizeof(*header))
    {
        fprintf(stderr, "Error: invalid darshan log file (failed to read header).\n");
        return(-1);
    }

    /* save the version string */
    strncpy(fd->version, header->version_string, 8);

    if(header->magic_nr == CP_MAGIC_NR)
    {
        /* no byte swapping needed, this file is in host format already */
        fd->swap_flag = 0;
    }
    else
    {
        /* try byte swapping */
        DARSHAN_BSWAP64(&header->magic_nr);
        if(header->magic_nr == CP_MAGIC_NR)
        {
            fd->swap_flag = 1;

            /* swap the log map variables in the header */
            DARSHAN_BSWAP64(&header->rec_map.off);
            DARSHAN_BSWAP64(&header->rec_map.len);
            for(i=0;i<DARSHAN_MAX_MODS;i++)
            {
                DARSHAN_BSWAP64(&header->mod_map[i].off);
                DARSHAN_BSWAP64(&header->mod_map[i].len);
            }
        }
        else
        {
            /* otherwise this file is just broken */
            fprintf(stderr, "Error: bad magic number in darshan log file.\n");
            return(-1);
        }
    }

    /* save the mapping of data within log file to this file descriptor */
    fd->job_map.off = sizeof(struct darshan_header);
    fd->job_map.len = header->rec_map.off - fd->job_map.off;
    memcpy(&fd->rec_map, &header->rec_map, sizeof(struct darshan_log_map));
    memcpy(&fd->mod_map, &header->mod_map, DARSHAN_MAX_MODS * sizeof(struct darshan_log_map));

    return(0);
}

/* darshan_log_getjob()
 *
 * read job level metadata from the darshan log file
 *
 * returns 0 on success, -1 on failure
 */
int darshan_log_getjob(darshan_fd fd, struct darshan_job *job)
{
    char job_buf[CP_JOB_RECORD_SIZE] = {0};
    int ret;

    ret = darshan_log_seek(fd, fd->job_map.off);
    if(ret < 0)
    {
        fprintf(stderr, "Error: unable to seek in darshan log file.\n");
        return(ret);
    }

    /* read the job data from the log file */
    ret = darshan_log_read(fd, job_buf, fd->job_map.len);
    if(ret < fd->job_map.len)
    {
        fprintf(stderr, "Error: invalid darshan log file (failed to read job data).\n");
        return(-1);
    }

    memcpy(job, job_buf, sizeof(*job));

    if(fd->swap_flag)
    {
        /* swap bytes if necessary */
        DARSHAN_BSWAP64(&job->uid);
        DARSHAN_BSWAP64(&job->start_time);
        DARSHAN_BSWAP64(&job->end_time);
        DARSHAN_BSWAP64(&job->nprocs);
        DARSHAN_BSWAP64(&job->jobid);
    }

    /* save trailing job data, so exe and mount information can be retrieved later */
    fd->exe_mnt_data = malloc(CP_EXE_LEN+1);
    if(!fd->exe_mnt_data)
        return(-1);
    memcpy(fd->exe_mnt_data, &job_buf[sizeof(*job)], CP_EXE_LEN+1);

    return(0);
}

#if 0
#ifdef HAVE_STRNDUP
    metadata = strndup(job->metadata, sizeof(job->metadata));
#else
    metadata = strdup(job->metadata);
#endif
    char *kv;
    char *key;
    char *value;
    char *save;

    for(kv=strtok_r(metadata, "\n", &save);
        kv != NULL;
        kv=strtok_r(NULL, "\n", &save))
    {
        /* NOTE: we intentionally only split on the first = character.
         * There may be additional = characters in the value portion
         * (for example, when storing mpi-io hints).
         */
        strcpy(buffer, kv);
        key = buffer;
        value = index(buffer, '=');
        if(!value)
            continue;
        /* convert = to a null terminator to split key and value */
        value[0] = '\0';
        value++;
        if (strcmp(key, "prev_ver") == 0)
        {
            strncpy(job->version_string, value, sizeof(job->version_string));
        }
    }
    free(metadata);
#endif

int darshan_log_getexe(darshan_fd fd, char *buf)
{
    char *newline;

    /* TODO: try reading log job one more time to set this buffer up */
    if(!fd->exe_mnt_data)
        return(-1);

    newline = strchr(fd->exe_mnt_data, '\n');

    /* copy over the exe string */
    if(newline)
        memcpy(buf, fd->exe_mnt_data, (newline - fd->exe_mnt_data));

    return (0);
}

/* darshan_log_getmounts()
 * 
 * retrieves mount table information from the log.  Note that mnt_pts and
 * fs_types are arrays that will be allocated by the function and must be
 * freed by the caller.  count will indicate the size of the arrays
 */
int darshan_log_getmounts(darshan_fd fd, char*** mnt_pts,
    char*** fs_types, int* count)
{
    int ret;
    char *pos;
    int array_index = 0;

    /* TODO: try reading log job one more time to set this buffer up */
    if(!fd->exe_mnt_data)
        return(-1);

    /* count entries */
    *count = 0;
    pos = fd->exe_mnt_data;
    while((pos = strchr(pos, '\n')) != NULL)
    {
        pos++;
        (*count)++;
    }

    if(*count == 0)
    {
        /* no mount entries present */
        return(0);
    }

    /* allocate output arrays */
    *mnt_pts = malloc((*count)*sizeof(char*));
    assert(*mnt_pts);
    *fs_types = malloc((*count)*sizeof(char*));
    assert(*fs_types);

    /* work backwards through the table and parse each line (except for
     * first, which holds command line information)
     */
    while((pos = strrchr(fd->exe_mnt_data, '\n')) != NULL)
    {
        /* overestimate string lengths */
        (*mnt_pts)[array_index] = malloc(CP_EXE_LEN);
        assert((*mnt_pts)[array_index]);
        (*fs_types)[array_index] = malloc(CP_EXE_LEN);
        assert((*fs_types)[array_index]);

        ret = sscanf(++pos, "%s\t%s", (*fs_types)[array_index],
            (*mnt_pts)[array_index]);
        if(ret != 2)
        {
            fprintf(stderr, "Error: poorly formatted mount table in log file.\n");
            return(-1);
        }
        pos--;
        *pos = '\0';
        array_index++;
    }

    return(0);
}

/* darshan_log_gethash()
 *
 * read the hash of records from the darshan log file
 *
 * returns 0 on success, -1 on failure
 */
int darshan_log_gethash(darshan_fd fd, struct darshan_record_ref **hash)
{
    unsigned char *hash_buf;
    unsigned char *buf_ptr;
    darshan_record_id *rec_id_ptr;
    uint32_t *path_len_ptr;
    char *path_ptr;
    struct darshan_record_ref *ref;
    int ret;

    ret = darshan_log_seek(fd, fd->rec_map.off);
    if(ret < 0)
    {
        fprintf(stderr, "Error: unable to seek in darshan log file.\n");
        return(ret);
    }

    /* allocate a buffer to store the (serialized) darshan record hash */
    hash_buf = malloc(fd->rec_map.len);
    if(!hash_buf)
        return(-1);

    /* read the record map from the log file */
    ret = darshan_log_read(fd, hash_buf, fd->rec_map.len);
    if(ret < fd->rec_map.len)
    {
        fprintf(stderr, "Error: invalid darshan log file (failed to read record hash).\n");
        free(hash_buf);
        return(-1);
    }

    buf_ptr = hash_buf;
    while(buf_ptr < (hash_buf + fd->rec_map.len))
    {
        /* get pointers for each field of this darshan record */
        /* NOTE: darshan record hash serialization method: 
         *          ... darshan_record_id | (uint32_t) path_len | path ...
         */
        rec_id_ptr = (darshan_record_id *)buf_ptr;
        buf_ptr += sizeof(darshan_record_id);
        path_len_ptr = (uint32_t *)buf_ptr;
        buf_ptr += sizeof(uint32_t);
        path_ptr = (char *)buf_ptr;
        buf_ptr += *path_len_ptr;

        ref = malloc(sizeof(*ref));
        if(!ref)
        {
            free(hash_buf);
            return(-1);
        }
        ref->rec.name = malloc(*path_len_ptr + 1);
        if(!ref->rec.name)
        {
            free(hash_buf);
            free(ref);
            return(-1);
        }

        if(fd->swap_flag)
        {
            /* we need to sort out endianness issues before deserializing */
            DARSHAN_BSWAP64(rec_id_ptr);
            DARSHAN_BSWAP32(path_len_ptr);
        }

        /* set the fields for this record */
        ref->rec.id = *rec_id_ptr;
        memcpy(ref->rec.name, path_ptr, *path_len_ptr);
        ref->rec.name[*path_len_ptr] = '\0';

        /* add this record to the hash */
        HASH_ADD(hlink, *hash, rec.id, sizeof(darshan_record_id), ref);
    }

    free(hash_buf);

    return(0);
}

/* TODO: hardcoded for posix -- what can we do generally?
 *       different function for each module and a way to map to this function?
 */
int darshan_log_getfile(darshan_fd fd, struct darshan_posix_file *file)
{
    int ret;
    const char* err_string;
    int i;

    if(fd->pos < fd->mod_map[0].off)
    {
        ret = darshan_log_seek(fd, fd->mod_map[0].off);
        if(ret < 0)
        {
            fprintf(stderr, "Error: unable to seek in darshan log file.\n");
            return(ret);
        }
    }

    /* reset file record, so that diff compares against a zero'd out record
     * if file is missing
     */
    memset(file, 0, sizeof(*file));

    ret = darshan_log_read(fd, file, sizeof(*file));
    if(ret == sizeof(*file))
    {
        /* got exactly one, correct size record */
        if(fd->swap_flag)
        {
            /* swap bytes if necessary */
            DARSHAN_BSWAP64(&file->f_id);
            DARSHAN_BSWAP64(&file->rank);
            for(i=0; i<CP_NUM_INDICES; i++)
                DARSHAN_BSWAP64(&file->counters[i]);
            for(i=0; i<CP_F_NUM_INDICES; i++)
                DARSHAN_BSWAP64(&file->fcounters[i]);
        }
        return(1);
    }


    if(ret > 0)
    {
        /* got a short read */
        fprintf(stderr, "Error: invalid file record (too small)\n");
        return(-1);
    }

    if(ret == 0)
    {
        /* hit end of file */
        return(0);
    }

    /* all other errors */
    err_string = strerror(errno);
    fprintf(stderr, "Error: %s\n", err_string);
    return(-1);
}

/* darshan_log_close()
 *
 * close an open darshan file descriptor
 *
 * returns 0 on success, -1 on failure
 */
void darshan_log_close(darshan_fd fd)
{
    if(fd->pf)
        close(fd->pf);

    if(fd->exe_mnt_data)
        free(fd->exe_mnt_data);

    free(fd);
}

/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

/* return amount written on success, -1 on failure.
 */
static int darshan_log_write(darshan_fd fd, void* buf, int len)
{
    int ret;

    if(fd->pf)
    {
        ret = write(fd->pf, buf, len);
        if(ret > 0)
            fd->pos += ret;
        return(ret);
    }

    return(-1);
}

/* return amount read on success, 0 on EOF, -1 on failure.
 */
static int darshan_log_read(darshan_fd fd, void* buf, int len)
{
    int ret;

    if(fd->pf)
    {
        ret = read(fd->pf, buf, len);
        if(ret > 0)
            fd->pos += ret;
        return(ret);
    }

    return(-1);
}

/* return 0 on successful seek to offset, -1 on failure.
 */
static int darshan_log_seek(darshan_fd fd, off_t offset)
{
    off_t ret_off;

    if(fd->pos == offset)
        return(0);

    ret_off = lseek(fd->pf, offset, SEEK_SET);
    if(ret_off == offset)
    {
        fd->pos = offset;
        return(0);
    }

    return(-1);
}
