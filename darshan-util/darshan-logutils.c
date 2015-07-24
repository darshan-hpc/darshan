/*
 * Copyright (C) 2015 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
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

#include "darshan-logutils.h"

static int darshan_log_seek(darshan_fd fd, off_t offset);
static int darshan_log_read(darshan_fd fd, void *buf, int len);
//static int darshan_log_write(darshan_fd fd, void *buf, int len);

/* TODO: can we make this s.t. we don't care about ordering (i.e., X macro it ) */
struct darshan_mod_logutil_funcs *mod_logutils[DARSHAN_MAX_MODS] =
{
    NULL,   /* NULL */
    &posix_logutils,    /* POSIX */
    &mpiio_logutils,   /* MPI-IO */
    NULL,   /* HDF5 */
    NULL,   /* PNETCDF */
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL
};

/* darshan_log_open()
 *
 * open a darshan log file for reading/writing
 *
 * returns 0 on success, -1 on failure
 */
darshan_fd darshan_log_open(const char *name, const char *mode)
{
    darshan_fd tmp_fd;

    /* we only allows "w" or "r" modes, nothing fancy */
    assert(strlen(mode) == 1);
    assert(mode[0] == 'r' || mode[0] == 'w');

    tmp_fd = malloc(sizeof(*tmp_fd));
    if(!tmp_fd)
        return(NULL);
    memset(tmp_fd, 0, sizeof(*tmp_fd));

    tmp_fd->gzf = gzopen(name, mode);
    if(!tmp_fd->gzf)
    {
        free(tmp_fd);
        tmp_fd = NULL;
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
        return(-1);
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

    if(header->magic_nr == DARSHAN_MAGIC_NR)
    {
        /* no byte swapping needed, this file is in host format already */
        fd->swap_flag = 0;
    }
    else
    {
        /* try byte swapping */
        DARSHAN_BSWAP64(&header->magic_nr);
        if(header->magic_nr == DARSHAN_MAGIC_NR)
        {
            fd->swap_flag = 1;

            /* swap the log map variables in the header */
            DARSHAN_BSWAP64(&header->rec_map.off);
            DARSHAN_BSWAP64(&header->rec_map.len);
            for(i = 0; i < DARSHAN_MAX_MODS; i++)
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

    ret = darshan_log_seek(fd, sizeof(struct darshan_header));
    if(ret < 0)
    {
        fprintf(stderr, "Error: unable to seek in darshan log file.\n");
        return(-1);
    }

    /* read the job data from the log file */
    ret = darshan_log_read(fd, job, sizeof(*job));
    if(ret < sizeof(*job))
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

int darshan_log_getexe(darshan_fd fd, char *buf)
{
    int tmp_off = sizeof(struct darshan_header) + sizeof(struct darshan_job);
    int ret;
    char *newline;

    ret = darshan_log_seek(fd, tmp_off);
    if(ret < 0)
    {
        fprintf(stderr, "Error: unable to seek in darshan log file.\n");
        return(-1);
    }

    /* read the trailing exe data from the darshan log */
    ret = darshan_log_read(fd, buf, DARSHAN_EXE_LEN+1);
    if(ret < DARSHAN_EXE_LEN+1)
    {
        fprintf(stderr, "Error: invalid darshan log file (failed to read exe string).\n");
        return(-1);
    }

    /* mount info is stored after the exe string, so truncate there */
    newline = strchr(buf, '\n');
    if(newline)
        *newline = '\0';

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
    int tmp_off = sizeof(struct darshan_header) + sizeof(struct darshan_job);
    int ret;
    char *pos;
    int array_index = 0;
    char buf[DARSHAN_EXE_LEN+1];

    ret = darshan_log_seek(fd, tmp_off);
    if(ret < 0)
    {
        fprintf(stderr, "Error: unable to seek in darshan log file.\n");
        return(-1);
    }

    /* read the trailing mount data from the darshan log */
    ret = darshan_log_read(fd, buf, DARSHAN_EXE_LEN+1);
    if(ret < DARSHAN_EXE_LEN+1)
    {
        fprintf(stderr, "Error: invalid darshan log file (failed to read mount info).\n");
        return(-1);
    }

    /* count entries */
    *count = 0;
    pos = buf;
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
    while((pos = strrchr(buf, '\n')) != NULL)
    {
        /* overestimate string lengths */
        (*mnt_pts)[array_index] = malloc(DARSHAN_EXE_LEN);
        assert((*mnt_pts)[array_index]);
        (*fs_types)[array_index] = malloc(DARSHAN_EXE_LEN);
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
    char *hash_buf;
    int hash_buf_sz = fd->rec_map.len;
    char *buf_ptr;
    darshan_record_id *rec_id_ptr;
    uint32_t *path_len_ptr;
    char *path_ptr;
    struct darshan_record_ref *ref;
    int ret;

    hash_buf = malloc(hash_buf_sz);
    if(!hash_buf)
        return(-1);

    ret = darshan_log_seek(fd, fd->rec_map.off);
    if(ret < 0)
    {
        fprintf(stderr, "Error: unable to seek in darshan log file.\n");
        return(-1);
    }

    /* read the record hash from the log file */
    ret = darshan_log_read(fd, hash_buf, fd->rec_map.len);
    if(ret < fd->rec_map.len)
    {
        fprintf(stderr, "Error: invalid darshan log file (failed to read record hash).\n");
        return(-1);
    }

    buf_ptr = hash_buf;
    while(buf_ptr < (hash_buf + hash_buf_sz))
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

        if(fd->swap_flag)
        {
            /* we need to sort out endianness issues before deserializing */
            DARSHAN_BSWAP64(rec_id_ptr);
            DARSHAN_BSWAP32(path_len_ptr);
        }

        HASH_FIND(hlink, *hash, rec_id_ptr, sizeof(darshan_record_id), ref);
        if(!ref)
        {
            ref = malloc(sizeof(*ref));
            if(!ref)
            {
                return(-1);
            }
            ref->rec.name = malloc(*path_len_ptr + 1);
            if(!ref->rec.name)
            {
                free(ref);
                return(-1);
            }

            /* set the fields for this record */
            ref->rec.id = *rec_id_ptr;
            memcpy(ref->rec.name, path_ptr, *path_len_ptr);
            ref->rec.name[*path_len_ptr] = '\0';

            /* add this record to the hash */
            HASH_ADD(hlink, *hash, rec.id, sizeof(darshan_record_id), ref);
        }
    }

    return(0);
}

int darshan_log_get_moddat(darshan_fd fd, darshan_module_id mod_id,
    void *moddat_buf, int moddat_buf_sz)
{
    int mod_buf_end = fd->mod_map[mod_id].off + fd->mod_map[mod_id].len;
    int ret;

    if(!fd->mod_map[mod_id].len || fd->pos == mod_buf_end)
        return(0); /* no (more) data corresponding to this mod_id */

    /* only seek to start of module data if current log file position 
     * is not within the given mod_id's range. This allows one to
     * repeatedly call this function and get chunks of a module's
     * data piecemeal.
     */
    if((fd->pos < fd->mod_map[mod_id].off) || (fd->pos > mod_buf_end))
    {
        ret = darshan_log_seek(fd, fd->mod_map[mod_id].off);
        if(ret < 0)
        {
            fprintf(stderr, "Error: unable to seek in darshan log file.\n");
            return(-1);
        }
    }

    /* read the record hash from the log file */
    ret = darshan_log_read(fd, moddat_buf, moddat_buf_sz);
    if(ret != moddat_buf_sz)
    {
        fprintf(stderr,
            "Error: invalid darshan log file (failed to read module %s data).\n",
            darshan_module_names[mod_id]);
        return(-1);
    }

    return(1);
}

/* darshan_log_close()
 *
 * close an open darshan file descriptor
 *
 * returns 0 on success, -1 on failure
 */
void darshan_log_close(darshan_fd fd)
{
    if(fd->gzf)
        gzclose(fd->gzf);

    free(fd);

    return;
}

/* **************************************************** */

/* return 0 on successful seek to offset, -1 on failure.
 */
static int darshan_log_seek(darshan_fd fd, off_t offset)
{
    z_off_t zoff = 0;
    z_off_t zoff_ret = 0;

    if(fd->pos == offset)
        return(0);

    if(fd->gzf)
    {
        zoff += offset;
        zoff_ret = gzseek(fd->gzf, zoff, SEEK_SET);
        if(zoff_ret == zoff)
        {
            fd->pos = offset;
            return(0);
        }
        return(-1);
    }

    return(-1);
}

#if 0
/* return amount written on success, -1 on failure.
 */
static int darshan_log_write(darshan_fd fd, void* buf, int len)
{
    int ret;

    if(fd->gzf)
    {
        ret = gzwrite(fd->gzf, buf, len);
        if(ret > 0)
            fd->pos += ret;
        return(ret);
    }

    return(-1);
}
#endif

/* return amount read on success, 0 on EOF, -1 on failure.
 */
static int darshan_log_read(darshan_fd fd, void* buf, int len)
{
    int ret;

    if(fd->gzf)
    {
        ret = gzread(fd->gzf, buf, len);
        if(ret > 0)
            fd->pos += ret;
        return(ret);
    }

    return(-1);
}

/*
 * Local variables:
 *  c-indent-level: 4
 *  c-basic-offset: 4
 * End:
 *
 * vim: ts=8 sts=4 sw=4 expandtab
 */
