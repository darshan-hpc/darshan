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
    char mode[2];
    int swap_flag;
    char version[8];
    char* name;
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

    /* TODO: why is mode needed??? */
    /* TODO: why is name needed??? */
    tmp_fd->mode[0] = mode[0];
    tmp_fd->mode[1] = mode[1];
    tmp_fd->name  = strdup(name);
    if(!tmp_fd->name)
    {
        free(tmp_fd);
        return(NULL);
    }

    tmp_fd->pf = open(name, o_flags);
    if(tmp_fd->pf < 0)
    {
        free(tmp_fd->name);
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
    int ret;

    ret = darshan_log_seek(fd, fd->job_map.off);
    if(ret < 0)
    {
        fprintf(stderr, "Error: unable to seek in darshan log file.\n");
        return(ret);
    }

    /* read the job data from the log file */
    ret = darshan_log_read(fd, job, fd->job_map.len);
    if(ret < fd->job_map.len)
    {
        fprintf(stderr, "Error: invalid darshan log file (failed to read job data).\n");
        return(-1);
    }

    if(fd->swap_flag)
    {
        /* swap bytes if necessary */
        DARSHAN_BSWAP64(&job->uid);
        DARSHAN_BSWAP64(&job->start_time);
        DARSHAN_BSWAP64(&job->end_time);
        DARSHAN_BSWAP64(&job->nprocs);
        DARSHAN_BSWAP64(&job->jobid);
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

#if 0
int darshan_log_getexe(darshan_fd fd, char *buf)
{
    int ret;
    char* newline;

    ret = darshan_log_seek(fd, fd->job_struct_size);
    if(ret < 0)
        return(ret);

    ret = darshan_log_read(fd, buf, (fd->COMPAT_CP_EXE_LEN + 1));
    if (ret < (fd->COMPAT_CP_EXE_LEN + 1))
    {
        perror("darshan_log_read");
        return(-1);
    }

    /* this call is only supposed to return the exe string, but starting in
     * log format 1.23 there could be a table of mount entry information
     * after the exe.  Look for newline character and truncate there.
     */
    newline = strchr(buf, '\n');
    if(newline)
        *newline = '\0';

    return (0);
}
#endif

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

    free(fd->name);
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
