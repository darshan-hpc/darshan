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
static int darshan_log_write(darshan_fd fd, void *buf, int len);
static int darshan_decompress_buf(char* comp_buf, int comp_buf_sz,
    char* decomp_buf, int* inout_decomp_buf_sz,
    enum darshan_comp_type comp_type);
static int darshan_compress_buf(char* decomp_buf, int decomp_buf_sz,
    char* comp_buf, int* inout_comp_buf_sz,
    enum darshan_comp_type comp_type);
static int darshan_zlib_decomp(char* comp_buf, int comp_buf_sz,
    char* decomp_buf, int* inout_decomp_buf_sz);
static int darshan_zlib_comp(char* decomp_buf, int decomp_buf_sz,
    char* comp_buf, int* inout_comp_buf_sz);
#ifdef HAVE_LIBBZ2
static int darshan_bzip2_decomp(char* comp_buf, int comp_buf_sz,
    char* decomp_buf, int* inout_decomp_buf_sz);
static int darshan_bzip2_comp(char* decomp_buf, int decomp_buf_sz,
    char* comp_buf, int* inout_comp_buf_sz);
#endif


/* TODO: can we make this s.t. we don't care about ordering (i.e., X macro it ) */
struct darshan_mod_logutil_funcs *mod_logutils[DARSHAN_MAX_MODS] =
{
    NULL,               /* NULL */
    &posix_logutils,    /* POSIX */
    &mpiio_logutils,    /* MPI-IO */
    &hdf5_logutils,     /* HDF5 */
    &pnetcdf_logutils,  /* PNETCDF */
    &bgq_logutils,      /* BG/Q */
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
 * open an existing darshan log file for reading only
 *
 * returns 0 on success, -1 on failure
 */
darshan_fd darshan_log_open(const char *name)
{
    darshan_fd tmp_fd;

    tmp_fd = malloc(sizeof(*tmp_fd));
    if(!tmp_fd)
        return(NULL);
    memset(tmp_fd, 0, sizeof(*tmp_fd));

    tmp_fd->fildes = open(name, O_RDONLY);
    if(tmp_fd->fildes < 0)
    {
        perror("darshan_log_open: ");
        free(tmp_fd);
        tmp_fd = NULL;
    }

    return(tmp_fd);
}

/* darshan_log_create()
 *
 * create a darshan log file for writing with the given compression method
 *
 * returns 0 on success, -1 on failure
 */
darshan_fd darshan_log_create(const char *name, enum darshan_comp_type comp_type)
{
    darshan_fd tmp_fd;

    tmp_fd = malloc(sizeof(*tmp_fd));
    if(!tmp_fd)
        return(NULL);
    memset(tmp_fd, 0, sizeof(*tmp_fd));

    /* TODO: permissions when creating?  umask */
    /* when writing, we create the log file making sure not to overwrite
     * an existing log
     */
    tmp_fd->comp_type = comp_type;
    tmp_fd->fildes = open(name, O_WRONLY | O_CREAT | O_EXCL, 0400);
    if(tmp_fd->fildes < 0)
    {
        perror("darshan_log_create: ");
        free(tmp_fd);
        tmp_fd = NULL;
    }

    return(tmp_fd);
}

/* darshan_log_getheader()
 *
 * read the header of the darshan log and set internal data structures
 * NOTE: this function must be called before reading other portions of the log
 * NOTE: this is the only portion of the darshan log which is uncompressed
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

    /* read uncompressed header from log file */
    ret = darshan_log_read(fd, header, sizeof(*header));
    if(ret != sizeof(*header))
    {
        fprintf(stderr, "Error: failed to read darshan log file header.\n");
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

    fd->comp_type = header->comp_type;

    /* save the mapping of data within log file to this file descriptor */
    fd->job_map.off = sizeof(struct darshan_header);
    fd->job_map.len = header->rec_map.off - fd->job_map.off;
    memcpy(&fd->rec_map, &header->rec_map, sizeof(struct darshan_log_map));
    memcpy(&fd->mod_map, &header->mod_map, DARSHAN_MAX_MODS * sizeof(struct darshan_log_map));

    return(0);
}

/* darshan_log_putheader()
 *
 * write a darshan header to log file
 * NOTE: the header is not passed in, and is instead built using
 * contents of the given file descriptor
 *
 * returns 0 on success, -1 on failure
 */
int darshan_log_putheader(darshan_fd fd)
{
    struct darshan_header header;
    int ret;

    ret = darshan_log_seek(fd, 0);
    if(ret < 0)
    {
        fprintf(stderr, "Error: unable to seek in darshan log file.\n");
        return(-1);
    }

    memset(&header, 0, sizeof(header));
    strcpy(header.version_string, DARSHAN_LOG_VERSION);
    header.magic_nr = DARSHAN_MAGIC_NR;
    header.comp_type = fd->comp_type;

    /* copy the mapping information to the header */
    memcpy(&header.rec_map, &fd->rec_map, sizeof(struct darshan_log_map));
    memcpy(&header.mod_map, &fd->mod_map, DARSHAN_MAX_MODS * sizeof(struct darshan_log_map));

    /* write header to file */
    ret = darshan_log_write(fd, &header, sizeof(header));
    if(ret != sizeof(header))
    {
        fprintf(stderr, "Error: failed to write Darshan log file header.\n");
        return(-1);
    }

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
    char *comp_buf;
    char job_buf[DARSHAN_JOB_RECORD_SIZE] = {0};
    int job_buf_sz = DARSHAN_JOB_RECORD_SIZE;
    int ret;

    assert(fd->job_map.len > 0 && fd->job_map.off > 0);

    /* allocate buffer to store compressed job info */
    comp_buf = malloc(fd->job_map.len);
    if(!comp_buf)
        return(-1);

    ret = darshan_log_seek(fd, fd->job_map.off);
    if(ret < 0)
    {
        fprintf(stderr, "Error: unable to seek in darshan log file.\n");
        free(comp_buf);
        return(-1);
    }

    /* read the compressed job data from the log file */
    ret = darshan_log_read(fd, comp_buf, fd->job_map.len);
    if(ret != fd->job_map.len)
    {
        fprintf(stderr, "Error: failed to read darshan log file job data.\n");
        free(comp_buf);
        return(-1);
    }

    /* decompress the job data */
    ret = darshan_decompress_buf(comp_buf, fd->job_map.len,
        job_buf, &job_buf_sz, fd->comp_type);
    if(ret < 0)
    {
        fprintf(stderr, "Error: failed to decompress darshan job data.\n");
        free(comp_buf);
        return(-1);
    }
    free(comp_buf);

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

    /* save trailing exe & mount information, so it can be retrieved later */
    if(!fd->exe_mnt_data)
        fd->exe_mnt_data = malloc(DARSHAN_EXE_LEN+1);
    if(!fd->exe_mnt_data)
        return(-1);
    memcpy(fd->exe_mnt_data, &job_buf[sizeof(*job)], DARSHAN_EXE_LEN+1);

    return(0);
}

/* darshan_log_putjob()
 *
 * write job level metadat to darshan log file
 *
 * returns 0 on success, -1 on failure
 */
int darshan_log_putjob(darshan_fd fd, struct darshan_job *job)
{
    struct darshan_job job_copy;
    char *comp_buf;
    int comp_buf_sz;
    int len;
    int ret;

    ret = darshan_log_seek(fd, sizeof(struct darshan_header));
    if(ret < 0)
    {
        fprintf(stderr, "Error: unable to seek in darshan log file.\n");
        return(-1);
    }

    memset(&job_copy, 0, sizeof(*job));
    memcpy(&job_copy, job, sizeof(*job));

    /* check for newline in existing metadata, add if needed */
    len = strlen(job_copy.metadata);
    if(len > 0 && len < DARSHAN_JOB_METADATA_LEN)
    {
        if(job_copy.metadata[len-1] != '\n')
        {
            job_copy.metadata[len] = '\n';
            job_copy.metadata[len+1] = '\0';
        }
    }

    comp_buf = malloc(sizeof(*job));
    if(!comp_buf)
        return(-1);
    comp_buf_sz = sizeof(*job);

    /* compress the job data */
    ret = darshan_compress_buf((char*)&job_copy, sizeof(*job),
        comp_buf, &comp_buf_sz, fd->comp_type);
    if(ret < 0)
    {
        fprintf(stderr, "Error: failed to decompress darshan job data.\n");
        free(comp_buf);
        return(-1);
    }    

    /* write job data to file */
    ret = darshan_log_write(fd, comp_buf, comp_buf_sz);
    if(ret != comp_buf_sz)
    {
        fprintf(stderr, "Error: failed to write darshan log file job data.\n");
        free(comp_buf);
        return(-1);
    }

    free(comp_buf);
    return(0);
}

/* darshan_log_getexe()
 *
 * reads the application exe name from darshan log file
 * 
 * returns 0 on success, -1 on failure 
 */
int darshan_log_getexe(darshan_fd fd, char *buf)
{
    char *newline;
    int ret;

    /* if the exe/mount data has not been saved yet, read in the job info */
    if(!fd->exe_mnt_data)
    {
        struct darshan_job job;
        ret = darshan_log_getjob(fd, &job);

        if(ret < 0 || !fd->exe_mnt_data)
            return(-1);
    }

    /* exe string is located before the first line break */
    newline = strchr(fd->exe_mnt_data, '\n');

    /* copy over the exe string */
    if(newline)
        memcpy(buf, fd->exe_mnt_data, (newline - fd->exe_mnt_data));

    return (0);
}

/* darshan_log_putexe()
 *
 * wrties the application exe name to darshan log file
 * NOTE: this needs to be called immediately following put_job as it
 * expects the final pointer to be positioned immediately following
 * the darshan job information
 *
 * returns 0 on success, -1 on failure 
 */
int darshan_log_putexe(darshan_fd fd, char *buf)
{
    int len;
    int ret;
    char comp_buf[DARSHAN_EXE_LEN] = {0};
    int comp_buf_sz = DARSHAN_EXE_LEN;

    len = strlen(buf);

    /* compress the input exe string */
    ret = darshan_compress_buf(buf, len, comp_buf, &comp_buf_sz, fd->comp_type);
    if(ret < 0)
    {
        fprintf(stderr, "Error: unable to compress exe string.\n");
        return(-1);
    }

    ret = darshan_log_write(fd, comp_buf, comp_buf_sz);
    if(ret != comp_buf_sz)
    {
        fprintf(stderr, "Error: failed to write exe string to darshan log file.\n");
        return(-1);
    }

    return(0);
}

/* darshan_log_getmounts()
 * 
 * retrieves mount table information from the log. Note that mnt_pts and
 * fs_types are arrays that will be allocated by the function and must be
 * freed by the caller. count will indicate the size of the arrays
 *
 * returns 0 on success, -1 on failure
 */
int darshan_log_getmounts(darshan_fd fd, char*** mnt_pts,
    char*** fs_types, int* count)
{
    char *pos;
    int array_index = 0;
    int ret;

    /* if the exe/mount data has not been saved yet, read in the job info */
    if(!fd->exe_mnt_data)
    {
        struct darshan_job job;
        ret = darshan_log_getjob(fd, &job);

        if(ret < 0 || !fd->exe_mnt_data)
            return(-1);
    }

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
        (*mnt_pts)[array_index] = malloc(DARSHAN_EXE_LEN);
        assert((*mnt_pts)[array_index]);
        (*fs_types)[array_index] = malloc(DARSHAN_EXE_LEN);
        assert((*fs_types)[array_index]);

        ret = sscanf(++pos, "%s\t%s", (*fs_types)[array_index],
            (*mnt_pts)[array_index]);
        if(ret != 2)
        {
            fprintf(stderr, "Error: poorly formatted mount table in darshan log file.\n");
            return(-1);
        }
        pos--;
        *pos = '\0';
        array_index++;
    }

    return(0);
}

/* darshan_log_putmounts()
 *
 * writes mount information to the darshan log file
 * NOTE: this function call should follow immediately after the call
 * to darshan_log_putexe(), as it assumes the darshan log file pointer
 * is pointing to the offset immediately following the exe string
 *
 * returns 0 on success, -1 on failure
 */
int darshan_log_putmounts(darshan_fd fd, char** mnt_pts, char** fs_types, int count)
{
    int i;
    char line[1024];
    char mnt_dat[DARSHAN_EXE_LEN] = {0};
    int mnt_dat_sz = 0;
    char comp_buf[DARSHAN_EXE_LEN] = {0};
    int comp_buf_sz = DARSHAN_EXE_LEN;
    char *tmp;
    int ret;

    /* write each mount entry to file */
    tmp = mnt_dat;
    for(i=count-1; i>=0; i--)
    {
        sprintf(line, "\n%s\t%s", fs_types[i], mnt_pts[i]);

        memcpy(tmp, line, strlen(line));
        tmp += strlen(line);
        mnt_dat_sz += strlen(line);
    }

    ret = darshan_compress_buf(mnt_dat, mnt_dat_sz, comp_buf, &comp_buf_sz, fd->comp_type);
    if(ret < 0)
    {
        fprintf(stderr, "Error: unable to compress mount data.\n");
        return(-1);
    }

    ret = darshan_log_write(fd, comp_buf, comp_buf_sz);
    if (ret != comp_buf_sz)
    {
        fprintf(stderr, "Error: failed to write darshan log mount data.\n");
        return(-1);
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
    char *comp_buf;
    char *hash_buf;
    int hash_buf_sz;
    char *buf_ptr;
    darshan_record_id *rec_id_ptr;
    uint32_t *path_len_ptr;
    char *path_ptr;
    struct darshan_record_ref *ref;
    int ret;

    /* allocate buffers to store compressed and decompressed record
     * hash data
     */
    comp_buf = malloc(fd->rec_map.len);
    hash_buf = malloc(DARSHAN_DEF_COMP_BUF_SZ);
    if(!comp_buf || !hash_buf)
        return(-1);
    hash_buf_sz = DARSHAN_DEF_COMP_BUF_SZ;
    memset(hash_buf, 0, DARSHAN_DEF_COMP_BUF_SZ);

    ret = darshan_log_seek(fd, fd->rec_map.off);
    if(ret < 0)
    {
        fprintf(stderr, "Error: unable to seek in darshan log file.\n");
        free(comp_buf);
        free(hash_buf);
        return(-1);
    }

    /* read the record hash from the log file */
    ret = darshan_log_read(fd, comp_buf, fd->rec_map.len);
    if(ret != fd->rec_map.len)
    {
        fprintf(stderr, "Error: failed to read record hash from darshan log file.\n");
        free(comp_buf);
        free(hash_buf);
        return(-1);
    }

    /* decompress the record hash buffer */
    ret = darshan_decompress_buf(comp_buf, fd->rec_map.len,
        hash_buf, &hash_buf_sz, fd->comp_type);
    if(ret < 0)
    {
        fprintf(stderr, "Error: unable to decompress darshan record map data.\n");
        free(comp_buf);
        free(hash_buf);
        return(-1);
    }
    free(comp_buf);

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
                free(hash_buf);
                return(-1);
            }
            ref->rec.name = malloc(*path_len_ptr + 1);
            if(!ref->rec.name)
            {
                free(ref);
                free(hash_buf);
                return(-1);
            }

            /* set the fields for this record */
            ref->rec.id = *rec_id_ptr;
            memcpy(ref->rec.name, path_ptr, *path_len_ptr);
            ref->rec.name[*path_len_ptr] = '\0';

            /* add this record to the hash */
            HASH_ADD(hlink, *hash, rec.id, sizeof(darshan_record_id), ref);
        }

        buf_ptr += *path_len_ptr;
    }

    free(hash_buf);
    return(0);
}

/* darshan_log_puthash()
 *
 * writes the hash table of records to the darshan log file
 * NOTE: this function call should follow immediately after the call
 * to darshan_log_putmounts(), as it assumes the darshan log file pointer
 * is pointing to the offset immediately following the mount information
 *
 * returns 0 on success, -1 on failure
 */
int darshan_log_puthash(darshan_fd fd, struct darshan_record_ref *hash)
{
    size_t hash_buf_sz;
    char *hash_buf;
    char *hash_buf_off;
    struct darshan_record_ref *ref, *tmp;
    uint32_t name_len;
    size_t record_sz;
    char *comp_buf;
    int comp_buf_sz;
    int ret;

    /* allocate a buffer to store 2 MiB worth of record data */
    /* NOTE: this buffer may be reallocated if estimate is too small */
    hash_buf_sz = 2 * 1024 * 1024;
    hash_buf = malloc(hash_buf_sz);
    if(!hash_buf)
    {
        return(-1);
    }

    /* serialize the record hash into a buffer for writing */
    hash_buf_off = hash_buf;
    HASH_ITER(hlink, hash, ref, tmp)
    {
        name_len = strlen(ref->rec.name);
        record_sz = sizeof(darshan_record_id) + sizeof(uint32_t) + name_len;
        /* make sure there is room in the buffer for this record */
        if((hash_buf_off + record_sz) > (hash_buf + hash_buf_sz))
        {
            char *tmp_buf;
            size_t old_buf_sz;

            /* if no room, reallocate the hash buffer at twice the current size */
            old_buf_sz = hash_buf_off - hash_buf;
            hash_buf_sz *= 2;
            tmp_buf = malloc(hash_buf_sz);
            if(!tmp_buf)
            {
                free(hash_buf);
                return(-1);
            }

            memcpy(tmp_buf, hash_buf, old_buf_sz);
            free(hash_buf);
            hash_buf = tmp_buf;
            hash_buf_off = hash_buf + old_buf_sz;
        }

        /* now serialize the record into the hash buffer.
         * NOTE: darshan record hash serialization method: 
         *          ... darshan_record_id | (uint32_t) path_len | path ...
         */
        *((darshan_record_id *)hash_buf_off) = ref->rec.id;
        hash_buf_off += sizeof(darshan_record_id);
        *((uint32_t *)hash_buf_off) = name_len;
        hash_buf_off += sizeof(uint32_t);
        memcpy(hash_buf_off, ref->rec.name, name_len);
        hash_buf_off += name_len;
    }
    hash_buf_sz = hash_buf_off - hash_buf;

    comp_buf = malloc(DARSHAN_DEF_COMP_BUF_SZ);
    if(!comp_buf)
        return(-1);
    comp_buf_sz = DARSHAN_DEF_COMP_BUF_SZ;

    /* compress the record hash */
    ret = darshan_compress_buf(hash_buf, hash_buf_sz,
        comp_buf, &comp_buf_sz, fd->comp_type);
    if(ret < 0)
    {
        fprintf(stderr, "Error: unable to compress darshan record hash.\n");
        free(comp_buf);
        free(hash_buf);
        return(-1);
    }

    /* set the appropriate mapping info for the record hash in the file descriptor */
    fd->rec_map.off = fd->pos;
    fd->rec_map.len = comp_buf_sz;

    /* write the record hash to file */
    ret = darshan_log_write(fd, comp_buf, comp_buf_sz);
    if(ret != comp_buf_sz)
    {
        fprintf(stderr, "Error: failed to write record hash to darshan log file.\n");
        free(comp_buf);
        free(hash_buf);
        return(-1);
    }

    free(comp_buf);
    free(hash_buf);

    return(0);
}

/* darshan_log_getmod()
 *
 * returns 1 on successful read of module data, 0 on no data, -1 on failure
 */
int darshan_log_getmod(darshan_fd fd, darshan_module_id mod_id,
    void *mod_buf, int *mod_buf_sz)
{
    char *comp_buf;
    int ret;

    if(mod_id < 0 || mod_id >= DARSHAN_MAX_MODS)
    {
        fprintf(stderr, "Error: invalid Darshan module id.\n");
        return(-1);
    }

    if(fd->mod_map[mod_id].len == 0)
        return(0); /* no data corresponding to this mod_id */

    comp_buf = malloc(fd->mod_map[mod_id].len);
    if(!comp_buf)
        return(-1);

    ret = darshan_log_seek(fd, fd->mod_map[mod_id].off);
    if(ret < 0)
    {
        fprintf(stderr, "Error: unable to seek in darshan log file.\n");
        free(comp_buf);
        return(ret);
    }

    /* read this module's data from the log file */
    ret = darshan_log_read(fd, comp_buf, fd->mod_map[mod_id].len);
    if(ret != fd->mod_map[mod_id].len)
    {
        fprintf(stderr,
            "Error: failed to read module %s data from darshan log file.\n",
            darshan_module_names[mod_id]);
        free(comp_buf);
        return(-1);
    }

    /* decompress this module's data */
    ret = darshan_decompress_buf(comp_buf, fd->mod_map[mod_id].len,
        mod_buf, mod_buf_sz, fd->comp_type);
    if(ret < 0)
    {
        fprintf(stderr, "Error: unable to decompress module %s data.\n",
            darshan_module_names[mod_id]);
        free(comp_buf);
        return(-1);
    }
    free(comp_buf);

    return(1);
}

/* darshan_log_putmod()
 *
 * write a chunk of module data to the darshan log file
 * NOTE: this function call should be called directly after the
 * put_hash() function, as it expects the file pointer to be
 * positioned directly past the record hash location. Also,
 * for a set of modules with data to write to file, this function
 * should be called in order of increasing module identifiers,
 * as the darshan log file format expects this ordering.
 *
 * returns 0 on success, -1 on failure
 */
int darshan_log_putmod(darshan_fd fd, darshan_module_id mod_id,
    void *mod_buf, int mod_buf_sz)
{
    char *comp_buf;
    int comp_buf_sz;
    int ret;

    if(mod_id < 0 || mod_id >= DARSHAN_MAX_MODS)
    {
        fprintf(stderr, "Error: invalid Darshan module id.\n");
        return(-1);
    }

    comp_buf = malloc(DARSHAN_DEF_COMP_BUF_SZ);
    if(!comp_buf)
        return(-1);
    comp_buf_sz = DARSHAN_DEF_COMP_BUF_SZ;

    /* compress the module's data */
    ret = darshan_compress_buf(mod_buf, mod_buf_sz,
        comp_buf, &comp_buf_sz, fd->comp_type);
    if(ret < 0)
    {
        fprintf(stderr, "Error: unable to compress module %s data.\n",
            darshan_module_names[mod_id]);
        free(comp_buf);
        return(-1);
    }

    /* set the appropriate mapping information for this module */
    fd->mod_map[mod_id].off = fd->pos;
    fd->mod_map[mod_id].len = comp_buf_sz;

    /* write the module chunk to the log file */
    ret = darshan_log_write(fd, comp_buf, comp_buf_sz);
    if(ret != comp_buf_sz)
    {
        fprintf(stderr,
            "Error: failed to write module %s data to darshan log file.\n",
            darshan_module_names[mod_id]);
        free(comp_buf);
        return(-1);
    }

    free(comp_buf);
    return(0);
}

/* darshan_log_close()
 *
 * close an open darshan file descriptor
 *
 * returns 0 on success, -1 on failure
 */
void darshan_log_close(darshan_fd fd)
{
    if(fd->fildes)
        close(fd->fildes);

    if(fd->exe_mnt_data)
        free(fd->exe_mnt_data);

    free(fd);

    return;
}

/* **************************************************** */

/* return 0 on successful seek to offset, -1 on failure.
 */
static int darshan_log_seek(darshan_fd fd, off_t offset)
{
    off_t ret_off;

    if(fd->pos == offset)
        return(0);

    ret_off = lseek(fd->fildes, offset, SEEK_SET);
    if(ret_off == offset)
    {
        fd->pos = offset;
        return(0);
    }

    return(-1);
}

/* return amount read on success, 0 on EOF, -1 on failure.
 */
static int darshan_log_read(darshan_fd fd, void* buf, int len)
{
    int ret;

    /* read data from the log file using the given map */
    ret = read(fd->fildes, buf, len);
    if(ret > 0)
        fd->pos += ret;

    return(ret);
}

/* return amount written on success, -1 on failure.
 */
static int darshan_log_write(darshan_fd fd, void* buf, int len)
{
    int ret;

    ret = write(fd->fildes, buf, len);
    if(ret > 0)
        fd->pos += ret;

    return(ret);
}

/* return 0 on successful decompression, -1 on failure
 */
static int darshan_decompress_buf(char* comp_buf, int comp_buf_sz,
    char* decomp_buf, int* inout_decomp_buf_sz,
    enum darshan_comp_type comp_type)
{
    int ret;

    switch(comp_type)
    {
        case DARSHAN_ZLIB_COMP:
            ret = darshan_zlib_decomp(comp_buf, comp_buf_sz,
                decomp_buf, inout_decomp_buf_sz);
            break;
#ifdef HAVE_LIBBZ2
        case DARSHAN_BZIP2_COMP:
            ret = darshan_bzip2_decomp(comp_buf, comp_buf_sz,
                decomp_buf, inout_decomp_buf_sz);
            break;
#endif
        default:
            fprintf(stderr, "Error: invalid decompression method.\n");
            return(-1);
    }

    return(ret);
}

static int darshan_compress_buf(char* decomp_buf, int decomp_buf_sz,
    char* comp_buf, int* inout_comp_buf_sz,
    enum darshan_comp_type comp_type)
{
    int ret;

    switch(comp_type)
    {
        case DARSHAN_ZLIB_COMP:
            ret = darshan_zlib_comp(decomp_buf, decomp_buf_sz,
                comp_buf, inout_comp_buf_sz);
            break;
#ifdef HAVE_LIBBZ2
        case DARSHAN_BZIP2_COMP:
            ret = darshan_bzip2_comp(decomp_buf, decomp_buf_sz,
                comp_buf, inout_comp_buf_sz);
            break;
#endif
        default:
            fprintf(stderr, "Error: invalid compression method.\n");
            return(-1);
    }

    return(ret);

}

static int darshan_zlib_decomp(char* comp_buf, int comp_buf_sz,
    char* decomp_buf, int* inout_decomp_buf_sz)
{
    int ret;
    int total_out = 0;
    z_stream tmp_stream;

    memset(&tmp_stream, 0, sizeof(tmp_stream));
    tmp_stream.zalloc = Z_NULL;
    tmp_stream.zfree = Z_NULL;
    tmp_stream.opaque = Z_NULL;
    tmp_stream.next_in = (unsigned char*)comp_buf;
    tmp_stream.avail_in = comp_buf_sz;
    tmp_stream.next_out = (unsigned char*)decomp_buf;
    tmp_stream.avail_out = *inout_decomp_buf_sz;

    /* initialize the zlib decompression parameters */
    /* TODO: check these parameters? */
    //ret = inflateInit2(&tmp_stream, 31);
    ret = inflateInit(&tmp_stream);
    if(ret != Z_OK)
    {
        return(-1);
    }

    /* while we have not finished consuming all of the compressed input data */
    while(tmp_stream.avail_in)
    {
        if(tmp_stream.avail_out == 0)
        {
            /* We ran out of buffer space for decompression.  In theory,
             * we could just alloc more space, but probably just easier
             * to bump up the default size of the output buffer.
             */
            inflateEnd(&tmp_stream);
            return(-1);
        }

        /* decompress data */
        ret = inflate(&tmp_stream, Z_FINISH);
        if(ret != Z_STREAM_END)
        {
            inflateEnd(&tmp_stream);
            return(-1);
        }

        total_out += tmp_stream.total_out;
        if(tmp_stream.avail_in)
            inflateReset(&tmp_stream);
    }
    inflateEnd(&tmp_stream);

    *inout_decomp_buf_sz = total_out;
    return(0);
}

static int darshan_zlib_comp(char* decomp_buf, int decomp_buf_sz,
    char* comp_buf, int* inout_comp_buf_sz)
{
    int ret;
    z_stream tmp_stream;

    memset(&tmp_stream, 0, sizeof(tmp_stream));
    tmp_stream.zalloc = Z_NULL;
    tmp_stream.zfree = Z_NULL;
    tmp_stream.opaque = Z_NULL;
    tmp_stream.next_in = (unsigned char*)decomp_buf;
    tmp_stream.avail_in = decomp_buf_sz;
    tmp_stream.next_out = (unsigned char*)comp_buf;
    tmp_stream.avail_out = *inout_comp_buf_sz;

    ret = deflateInit(&tmp_stream, Z_DEFAULT_COMPRESSION);
    if(ret != Z_OK)
    {
        return(-1);
    }

    /* compress data */
    ret = deflate(&tmp_stream, Z_FINISH);
    if(ret != Z_STREAM_END)
    {
        deflateEnd(&tmp_stream);
        return(-1);
    }
    deflateEnd(&tmp_stream);

    *inout_comp_buf_sz = tmp_stream.total_out;
    return(0);
}

#ifdef HAVE_LIBBZ2
static int darshan_bzip2_decomp(char* comp_buf, int comp_buf_sz,
    char* decomp_buf, int* inout_decomp_buf_sz)
{
    int ret;
    int total_out = 0;
    bz_stream tmp_stream;

    memset(&tmp_stream, 0, sizeof(tmp_stream));
    tmp_stream.bzalloc = NULL;
    tmp_stream.bzfree = NULL;
    tmp_stream.opaque = NULL;
    tmp_stream.next_in = comp_buf;
    tmp_stream.avail_in = comp_buf_sz;
    tmp_stream.next_out = decomp_buf;
    tmp_stream.avail_out = *inout_decomp_buf_sz;

    ret = BZ2_bzDecompressInit(&tmp_stream, 1, 0);
    if(ret != BZ_OK)
    {
        return(-1);
    }

    /* while we have not finished consuming all of the uncompressed input data */
    while(tmp_stream.avail_in)
    {
        if(tmp_stream.avail_out == 0)
        {
            /* We ran out of buffer space for decompression.  In theory,
             * we could just alloc more space, but probably just easier
             * to bump up the default size of the output buffer.
             */
            BZ2_bzDecompressEnd(&tmp_stream);
            return(-1);
        }

        /* decompress data */
        ret = BZ2_bzDecompress(&tmp_stream);
        if(ret != BZ_STREAM_END)
        {
            BZ2_bzDecompressEnd(&tmp_stream);
            return(-1);
        }

        assert(tmp_stream.total_out_hi32 == 0);
        total_out += tmp_stream.total_out_lo32;
        if(tmp_stream.avail_in)
        {
            /* reinitialize bzip2 stream, we have more data to
             * decompress
             */
            BZ2_bzDecompressEnd(&tmp_stream);
            ret = BZ2_bzDecompressInit(&tmp_stream, 1, 0);
            if(ret != BZ_OK)
            {
                return(-1);
            }
        }
    }
    BZ2_bzDecompressEnd(&tmp_stream);

    *inout_decomp_buf_sz = total_out;
    return(0);
}

static int darshan_bzip2_comp(char* decomp_buf, int decomp_buf_sz,
    char* comp_buf, int* inout_comp_buf_sz)
{
    int ret;
    bz_stream tmp_stream;

    memset(&tmp_stream, 0, sizeof(tmp_stream));
    tmp_stream.bzalloc = NULL;
    tmp_stream.bzfree = NULL;
    tmp_stream.opaque = NULL;
    tmp_stream.next_in = decomp_buf;
    tmp_stream.avail_in = decomp_buf_sz;
    tmp_stream.next_out = comp_buf;
    tmp_stream.avail_out = *inout_comp_buf_sz;

    ret = BZ2_bzCompressInit(&tmp_stream, 9, 1, 30);
    if(ret != BZ_OK)
    {
        return(-1);
    }

    /* compress data */
    do
    {
        ret = BZ2_bzCompress(&tmp_stream, BZ_FINISH);
        if(ret < 0)
        {
            BZ2_bzCompressEnd(&tmp_stream);
            return(-1);
        }
    } while (ret != BZ_STREAM_END);
    BZ2_bzCompressEnd(&tmp_stream);

    assert(tmp_stream.total_out_hi32 == 0);
    *inout_comp_buf_sz = tmp_stream.total_out_lo32;
    return(0);
}
#endif

/*
 * Local variables:
 *  c-indent-level: 4
 *  c-basic-offset: 4
 * End:
 *
 * vim: ts=8 sts=4 sw=4 expandtab
 */
