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
    /* TODO: ultimately store indices here */
};

static int darshan_log_seek(darshan_fd fd, off_t offset);
static int darshan_log_read(darshan_fd fd, void *buf, int len);
static int darshan_log_write(darshan_fd fd, void *buf, int len);
//static const char* darshan_log_error(darshan_fd fd, int* errnum);

/* a rather crude API for accessing raw binary darshan files */
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

int darshan_log_getheader(darshan_fd file, struct darshan_header *header)
{
    int ret;

    ret = darshan_log_seek(file, 0);
    if(ret < 0)
    {
        fprintf(stderr, "Error: unable to seek in darshan log file.\n");
        return(ret);
    }

    /* read header from log file */
    ret = darshan_log_read(file, header, sizeof(*header));
    if(ret < sizeof(*header))
    {
        fprintf(stderr, "Error: invalid darshan log file (failed to read header).\n");
        return(-1);
    }

    /* save the version string -- this can be used to support multiple log versions */
    strncpy(file->version, header->version_string, 8);

    if(header->magic_nr == CP_MAGIC_NR)
    {
        /* no byte swapping needed, this file is in host format already */
        file->swap_flag = 0;
        return(0);
    }

    /* try byte swapping */
    DARSHAN_BSWAP64(&header->magic_nr);
    if(header->magic_nr == CP_MAGIC_NR)
    {
        file->swap_flag = 1;
        return(0);
    }

    /* otherwise this file is just broken */
    fprintf(stderr, "Error: bad magic number in darshan log file.\n");
    return(-1);
}

/* darshan_log_getjob()
 *
 * returns 0 on success, -1 on failure
 */
int darshan_log_getjob(darshan_fd file, struct darshan_job *job)
{
    int ret;
    char buffer[DARSHAN_JOB_METADATA_LEN];

    ret = darshan_log_seek(file, sizeof(struct darshan_header));
    if(ret < 0)
    {
        fprintf(stderr, "Error: unable to seek in darshan log file.\n");
        return(ret);
    }

    /* read the job data from the log file */
    ret = darshan_log_read(file, job, sizeof(*job));
    if(ret < sizeof(*job))
    {
        fprintf(stderr, "Error: invalid darshan log file (failed to read job data).\n");
        return(-1);
    }

    if(file->swap_flag)
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

int darshan_log_getmap(darshan_fd file, unsigned char **map_buf)
{
    int ret;
    struct stat sbuf;
    int map_buf_size;

    ret = darshan_log_seek(file, sizeof(struct darshan_header) + CP_JOB_RECORD_SIZE);
    if(ret < 0)
    {
        fprintf(stderr, "Error: unable to seek in darshan log file.\n");
        return(ret);
    }

    /* TODO: use indices map rather than stat to determine offsets */
    /* allocate a buffer to store the (serialized) darshan record map */
    /* NOTE: caller's responsibility to free this allocated map buffer */
    fstat(file->pf, &sbuf);
    map_buf_size = sbuf.st_size - (sizeof(struct darshan_header) + CP_JOB_RECORD_SIZE);
    *map_buf = malloc(map_buf_size);
    if(!(*map_buf))
        return(-1);

    /* read the record map from the log file */
    ret = darshan_log_read(file, *map_buf, map_buf_size);
    if(ret < map_buf_size)
    {
        fprintf(stderr, "Error: invalid darshan log file (failed to read record map).\n");
        return(-1);
    }

    if(file->swap_flag)
    {
        /* we need to sort out endianness issues before passing back the serialized buffer */
        /* NOTE: darshan record map serialization method: 
         *          ... darshan_record_id | (uint32_t) path_len | path ...
         */
        unsigned char *buf_ptr = *map_buf;
        darshan_record_id *rec_id_ptr;
        uint32_t *path_len_ptr;

        while(buf_ptr < (*map_buf + map_buf_size))
        {
            rec_id_ptr = (darshan_record_id *)buf_ptr;
            buf_ptr += sizeof(darshan_record_id);
            path_len_ptr = (uint32_t *)buf_ptr;
            buf_ptr += sizeof(uint32_t);
            buf_ptr += *path_len_ptr;

            DARSHAN_BSWAP64(rec_id_ptr);
            DARSHAN_BSWAP32(path_len_ptr);
        }
    }

    return(0);
}

/* TODO: implement */
/* TODO: could this could be used in darshan-runtime? do we refactor so we aren't maintaining in 2 spots? */
int darshan_log_build_map(unsigned char *map_buf, int map_buf_size, some_struct *rec_hash)
{
    unsigned char *buf_ptr;

    return(0);
}

/* TODO: implement */
/* TODO: could this could be used in darshan-runtime? do we refactor so we aren't maintaining in 2 spots? */
int darshan_log_destroy_map()
{
    return(0);
}

#if 0
/* darshan_log_getfile()
 *
 * return 1 if file record found, 0 on eof, and -1 on error
 */
int darshan_log_getfile(darshan_fd fd, struct darshan_job *job, struct darshan_file *file)
{
    int ret;

    ret = getfile_internal(fd, job, file);

    return(ret);
}

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

void darshan_log_close(darshan_fd file)
{
    if(file->pf)
        close(file->pf);

    free(file->name);
    free(file);
}

#if 0
/* darshan_log_print_version_warnings()
 *
 * Print summary of any problems with the detected log format
 */
void darshan_log_print_version_warnings(struct darshan_job *job)
{
    if(strcmp(job->version_string, "2.05") == 0)
    {
        /* current version */
        return;
    }

    if(strcmp(job->version_string, "2.04") == 0)
    {
        printf("# WARNING: version 2.04 log format has the following limitations:\n");
        printf("# - CP_F_SLOWEST_RANK_TIME and CP_F_FASTEST_RANK_TIME only report elapsed time at the POSIX level.\n");
        return;
    }

    if(strcmp(job->version_string, "2.03") == 0)
    {
        /* no meaningful change to interpretation of log file, 2.03 just
         * increased the header space available for annotations.
         */
        printf("# WARNING: version 2.03 log format has the following limitations:\n");
        printf("# - CP_F_SLOWEST_RANK_TIME and CP_F_FASTEST_RANK_TIME only report elapsed time at the POSIX level.\n");
        return;
    }

    if(strcmp(job->version_string, "2.02") == 0)
    {
        printf("# WARNING: version 2.01 log format has the following limitations:\n");
        printf("# - *_TIMESTAMP fields are not normalized relative to MPI_Init() time.\n");
        printf("# - CP_F_SLOWEST_RANK_TIME and CP_F_FASTEST_RANK_TIME only report elapsed time at the POSIX level.\n");
        return;
    }

    if(strcmp(job->version_string, "2.01") == 0)
    {
        printf("# WARNING: version 2.01 log format has the following limitations:\n");
        printf("# - *_TIMESTAMP fields are not normalized relative to MPI_Init() time.\n");
        printf("# - inaccurate statistics in some multi-threaded cases.\n");
        printf("# - CP_F_SLOWEST_RANK_TIME and CP_F_FASTEST_RANK_TIME only report elapsed time at the POSIX level.\n");
        return;
    }

    if(strcmp(job->version_string, "2.00") == 0)
    {
        printf("# WARNING: version 2.00 log format has the following limitations:\n");
        printf("# - *_TIMESTAMP fields are not normalized relative to MPI_Init() time.\n");
        printf("# - inaccurate statistics in some multi-threaded cases.\n");
        printf("# - CP_F_SLOWEST_RANK_TIME and CP_F_FASTEST_RANK_TIME only report elapsed time at the POSIX level.\n");
        return;
    }
 
    if(strcmp(job->version_string, "1.24") == 0)
    {
        printf("# WARNING: version 1.24 log format does not support the following parameters:\n");
        printf("#   CP_FASTEST_RANK\n");
        printf("#   CP_FASTEST_RANK_BYTES\n");
        printf("#   CP_SLOWEST_RANK\n");
        printf("#   CP_SLOWEST_RANK_BYTES\n");
        printf("#   CP_F_FASTEST_RANK_TIME\n");
        printf("#   CP_F_SLOWEST_RANK_TIME\n");
        printf("#   CP_F_VARIANCE_RANK_TIME\n");
        printf("#   CP_F_VARIANCE_RANK_BYTES\n");
        printf("# WARNING: version 1.24 log format has the following limitations:\n");
        printf("# - *_TIMESTAMP fields are not normalized relative to MPI_Init() time.\n");
        printf("# - does not store the job id in the file.\n");
        printf("# - inaccurate statistics in some multi-threaded cases.\n");
        printf("# - CP_F_SLOWEST_RANK_TIME and CP_F_FASTEST_RANK_TIME only report elapsed time at the POSIX level.\n");
        return;
    }
    
    if(strcmp(job->version_string, "1.23") == 0)
    {
        printf("# WARNING: version 1.23 log format does not support the following parameters:\n");
        printf("#   CP_FASTEST_RANK\n");
        printf("#   CP_FASTEST_RANK_BYTES\n");
        printf("#   CP_SLOWEST_RANK\n");
        printf("#   CP_SLOWEST_RANK_BYTES\n");
        printf("#   CP_F_FASTEST_RANK_TIME\n");
        printf("#   CP_F_SLOWEST_RANK_TIME\n");
        printf("#   CP_F_VARIANCE_RANK_TIME\n");
        printf("#   CP_F_VARIANCE_RANK_BYTES\n");
        printf("# WARNING: version 1.23 log format has the following limitations:\n");
        printf("# - *_TIMESTAMP fields are not normalized relative to MPI_Init() time.\n");
        printf("# - may have incorrect mount point mappings for files with rank > 0.\n");
        printf("# - does not store the job id in the file.\n");
        printf("# - inaccurate statistics in some multi-threaded cases.\n");
        printf("# - CP_F_SLOWEST_RANK_TIME and CP_F_FASTEST_RANK_TIME only report elapsed time at the POSIX level.\n");
        return;
    }

    if(strcmp(job->version_string, "1.22") == 0)
    {
        printf("# WARNING: version 1.22 log format does not support the following parameters:\n");
        printf("#   CP_DEVICE\n");
        printf("#   CP_SIZE_AT_OPEN\n");
        printf("#   CP_FASTEST_RANK\n");
        printf("#   CP_FASTEST_RANK_BYTES\n");
        printf("#   CP_SLOWEST_RANK\n");
        printf("#   CP_SLOWEST_RANK_BYTES\n");
        printf("#   CP_F_FASTEST_RANK_TIME\n");
        printf("#   CP_F_SLOWEST_RANK_TIME\n");
        printf("#   CP_F_VARIANCE_RANK_TIME\n");
        printf("#   CP_F_VARIANCE_RANK_BYTES\n");
        printf("# WARNING: version 1.22 log format has the following limitations:\n");
        printf("# - *_TIMESTAMP fields are not normalized relative to MPI_Init() time.\n");
        printf("# - does not record mounted file systems, mount points, or fs types.\n");
        printf("# - attributes syncs to cumulative metadata time, rather than cumulative write time.\n");
        printf("# - does not store the job id in the file.\n");
        printf("# - inaccurate statistics in some multi-threaded cases.\n");
        printf("# - CP_F_SLOWEST_RANK_TIME and CP_F_FASTEST_RANK_TIME only report elapsed time at the POSIX level.\n");
        return;
    }

    if(strcmp(job->version_string, "1.21") == 0)
    {
        printf("# WARNING: version 1.21 log format does not support the following parameters:\n");
        printf("#   CP_INDEP_NC_OPENS\n");
        printf("#   CP_COLL_NC_OPENS\n");
        printf("#   CP_HDF5_OPENS\n");
        printf("#   CP_MAX_READ_TIME_SIZE\n");
        printf("#   CP_MAX_WRITE_TIME_SIZE\n");
        printf("#   CP_DEVICE\n");
        printf("#   CP_SIZE_AT_OPEN\n");
        printf("#   CP_F_MAX_READ_TIME\n");
        printf("#   CP_F_MAX_WRITE_TIME\n");
        printf("#   CP_FASTEST_RANK\n");
        printf("#   CP_FASTEST_RANK_BYTES\n");
        printf("#   CP_SLOWEST_RANK\n");
        printf("#   CP_SLOWEST_RANK_BYTES\n");
        printf("#   CP_F_FASTEST_RANK_TIME\n");
        printf("#   CP_F_SLOWEST_RANK_TIME\n");
        printf("#   CP_F_VARIANCE_RANK_TIME\n");
        printf("#   CP_F_VARIANCE_RANK_BYTES\n");
        printf("# WARNING: version 1.21 log format has the following limitations:\n");
        printf("# - *_TIMESTAMP fields are not normalized relative to MPI_Init() time.\n");
        printf("# - does not record mounted file systems, mount points, or fs types.\n");
        printf("# - attributes syncs to cumulative metadata time, rather than cumulative write time.\n");
        printf("# - does not store the job id in the file.\n");
        printf("# - inaccurate statistics in some multi-threaded cases.\n");
        printf("# - CP_F_SLOWEST_RANK_TIME and CP_F_FASTEST_RANK_TIME only report elapsed time at the POSIX level.\n");
        return;
    }

    fprintf(stderr, "Error: version %s not supported by parser.\n",
        job->version_string);
    return;
}

/* shift_missing_1_21()
 *
 * translates indices to account for counters that weren't present in log
 * format 1.21
 */
/*******************************
 * version 1.21 to 2.00 differences 
 * - added:
 *   - CP_INDEP_NC_OPENS
 *   - CP_COLL_NC_OPENS
 *   - CP_HDF5_OPENS
 *   - CP_MAX_READ_TIME_SIZE
 *   - CP_MAX_WRITE_TIME_SIZE
 *   - CP_DEVICE
 *   - CP_SIZE_AT_OPEN
 *   - CP_FASTEST_RANK
 *   - CP_FASTEST_RANK_BYTES
 *   - CP_SLOWEST_RANK
 *   - CP_SLOWEST_RANK_BYTES
 *   - CP_F_MAX_READ_TIME
 *   - CP_F_MAX_WRITE_TIME
 *   - CP_F_FASTEST_RANK_TIME
 *   - CP_F_SLOWEST_RANK_TIME
 *   - CP_F_VARIANCE_RANK_TIME
 *   - CP_F_VARIANCE_RANK_BYTES
 * - changed params:
 *   - CP_FILE_RECORD_SIZE: 1184 to 1328
 *   - CP_NUM_INDICES: 133 to 144
 *   - CP_F_NUM_INDICES: 12 to 18
 */
static void shift_missing_1_21(struct darshan_file* file)
{
    int c_index = 0;
    int missing_counters[] = {
        CP_INDEP_NC_OPENS,
        CP_COLL_NC_OPENS,
        CP_HDF5_OPENS,
        CP_MAX_READ_TIME_SIZE,
        CP_MAX_WRITE_TIME_SIZE,
        CP_DEVICE,
        CP_SIZE_AT_OPEN,
        CP_FASTEST_RANK,
        CP_FASTEST_RANK_BYTES,
        CP_SLOWEST_RANK,
        CP_SLOWEST_RANK_BYTES,
        -1};
    int missing_f_counters[] = {
        CP_F_MAX_READ_TIME,
        CP_F_MAX_WRITE_TIME,
        CP_F_FASTEST_RANK_TIME,
        CP_F_SLOWEST_RANK_TIME,
        CP_F_VARIANCE_RANK_TIME,
        CP_F_VARIANCE_RANK_BYTES,
        -1};

    c_index = 0;
    while(missing_counters[c_index] != -1)
    {
        int missing_counter = missing_counters[c_index];
        c_index++;
        if(missing_counter < (CP_NUM_INDICES - 1))
        {
            /* shift down */
            memmove(&file->counters[missing_counter+1],
                &file->counters[missing_counter],
                (CP_NUM_INDICES-missing_counter-1)*sizeof(int64_t));
        }
        /* zero out missing counter */
        file->counters[missing_counter] = 0;
    }

    c_index = 0;
    while(missing_f_counters[c_index] != -1)
    {
        int missing_counter = missing_f_counters[c_index];
        c_index++;
        if(missing_counter < (CP_F_NUM_INDICES - 1))
        {
            /* shift down */
            memmove(&file->fcounters[missing_counter+1],
                &file->fcounters[missing_counter],
                (CP_F_NUM_INDICES-missing_counter-1)*sizeof(double));
        }
        /* zero out missing counter */
        file->fcounters[missing_counter] = 0;
    }

    return;
}

/* shift_missing_1_22()
 *
 * translates indices to account for counters that weren't present in log
 * format 1.22
 */
/*******************************
 * version 1.22 to 2.00 differences
 *
 * - added:
 *   - CP_DEVICE
 *   - CP_SIZE_AT_OPEN
 *   - CP_FASTEST_RANK
 *   - CP_FASTEST_RANK_BYTES
 *   - CP_SLOWEST_RANK
 *   - CP_SLOWEST_RANK_BYTES
 *   - CP_F_FASTEST_RANK_TIME
 *   - CP_F_SLOWEST_RANK_TIME
 *   - CP_F_VARIANCE_RANK_TIME
 *   - CP_F_VARIANCE_RANK_BYTES
 * - changed params:
 *   - CP_FILE_RECORD_SIZE: 1240 to 1328
 *   - CP_NUM_INDICES: 138 to 144
 *   - CP_F_NUM_INDICES: 14 to 18
 */
static void shift_missing_1_22(struct darshan_file* file)
{
    int c_index = 0;
    int missing_counters[] = {
        CP_DEVICE,
        CP_SIZE_AT_OPEN,
        CP_FASTEST_RANK,
        CP_FASTEST_RANK_BYTES,
        CP_SLOWEST_RANK,
        CP_SLOWEST_RANK_BYTES,
        -1};
    int missing_f_counters[] = {
        CP_F_FASTEST_RANK_TIME,
        CP_F_SLOWEST_RANK_TIME,
        CP_F_VARIANCE_RANK_TIME,
        CP_F_VARIANCE_RANK_BYTES,
        -1};

    c_index = 0;
    while(missing_counters[c_index] != -1)
    {
        int missing_counter = missing_counters[c_index];
        c_index++;
        if(missing_counter < (CP_NUM_INDICES - 1))
        {
            /* shift down */
            memmove(&file->counters[missing_counter+1],
                &file->counters[missing_counter],
                (CP_NUM_INDICES-missing_counter-1)*sizeof(int64_t));
        }
        /* zero out missing counter */
        file->counters[missing_counter] = 0;
    }

    c_index = 0;
    while(missing_f_counters[c_index] != -1)
    {
        int missing_counter = missing_f_counters[c_index];
        c_index++;
        if(missing_counter < (CP_F_NUM_INDICES - 1))
        {
            /* shift down */
            memmove(&file->fcounters[missing_counter+1],
                &file->fcounters[missing_counter],
                (CP_F_NUM_INDICES-missing_counter-1)*sizeof(double));
        }
        /* zero out missing counter */
        file->fcounters[missing_counter] = 0;
    }

    return;
}

/* shift_missing_1_24()
 *
 * translates indices to account for counters that weren't present in log
 * format 1.24
 */
/*******************************
 * version 1.24 to 2.00 differences
 *
 * - added:
 *   - CP_FASTEST_RANK
 *   - CP_FASTEST_RANK_BYTES
 *   - CP_SLOWEST_RANK
 *   - CP_SLOWEST_RANK_BYTES
 *   - CP_F_FASTEST_RANK_TIME
 *   - CP_F_SLOWEST_RANK_TIME
 *   - CP_F_VARIANCE_RANK_TIME
 *   - CP_F_VARIANCE_RANK_BYTES
 * - changed params:
 *   - CP_FILE_RECORD_SIZE: ? to 1328
 *   - CP_NUM_INDICES: 140 to 144
 *   - CP_F_NUM_INDICES: 14 to 18
 */
static void shift_missing_1_24(struct darshan_file* file)
{
    int c_index = 0;
    int missing_counters[] = {
        CP_FASTEST_RANK,
        CP_FASTEST_RANK_BYTES,
        CP_SLOWEST_RANK,
        CP_SLOWEST_RANK_BYTES,
        -1};
    int missing_f_counters[] = {
        CP_F_FASTEST_RANK_TIME,
        CP_F_SLOWEST_RANK_TIME,
        CP_F_VARIANCE_RANK_TIME,
        CP_F_VARIANCE_RANK_BYTES,
        -1};

    c_index = 0;
    while(missing_counters[c_index] != -1)
    {
        int missing_counter = missing_counters[c_index];
        c_index++;
        if(missing_counter < (CP_NUM_INDICES - 1))
        {
            /* shift down */
            memmove(&file->counters[missing_counter+1],
                &file->counters[missing_counter],
                (CP_NUM_INDICES-missing_counter-1)*sizeof(int64_t));
        }
        /* zero out missing counter */
        file->counters[missing_counter] = 0;
    }

    c_index = 0;
    while(missing_f_counters[c_index] != -1)
    {
        int missing_counter = missing_f_counters[c_index];
        c_index++;
        if(missing_counter < (CP_F_NUM_INDICES - 1))
        {
            /* shift down */
            memmove(&file->fcounters[missing_counter+1],
                &file->fcounters[missing_counter],
                (CP_F_NUM_INDICES-missing_counter-1)*sizeof(double));
        }
        /* zero out missing counter */
        file->fcounters[missing_counter] = 0;
    }

    return;
}

static int getfile_internal_204(darshan_fd fd, struct darshan_job *job, 
    struct darshan_file *file)
{
    int ret;
    const char* err_string;
    int i;

    if(fd->pos < CP_JOB_RECORD_SIZE)
    {
        ret = darshan_log_seek(fd, CP_JOB_RECORD_SIZE);
        if(ret < 0)
            return(ret);
    }

    /* reset file record, so that diff compares against a zero'd out record
     * if file is missing
     */
    memset(file, 0, sizeof(&file));

    ret = darshan_log_read(fd, file, sizeof(*file));
    if(ret == sizeof(*file))
    {
        /* got exactly one, correct size record */
        if(fd->swap_flag)
        {
            /* swap bytes if necessary */
            DARSHAN_BSWAP64(&file->hash);
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
    err_string = darshan_log_error(fd, &ret);
    fprintf(stderr, "Error: %s\n", err_string);
    return(-1);
}

static int getfile_internal_200(darshan_fd fd, struct darshan_job *job, 
    struct darshan_file *file)
{
    int ret;
    const char* err_string;
    int i;

    if(fd->pos < CP_JOB_RECORD_SIZE_200)
    {
        ret = darshan_log_seek(fd, CP_JOB_RECORD_SIZE_200);
        if(ret < 0)
            return(ret);
    }

    /* reset file record, so that diff compares against a zero'd out record
     * if file is missing
     */
    memset(file, 0, sizeof(&file));

    ret = darshan_log_read(fd, file, sizeof(*file));
    if(ret == sizeof(*file))
    {
        /* got exactly one, correct size record */
        if(fd->swap_flag)
        {
            /* swap bytes if necessary */
            DARSHAN_BSWAP64(&file->hash);
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
    err_string = darshan_log_error(fd, &ret);
    fprintf(stderr, "Error: %s\n", err_string);
    return(-1);
}

static int getfile_internal_124(darshan_fd fd, struct darshan_job *job, 
    struct darshan_file *file)
{
    int ret;

    ret = getfile_internal_1x(fd, job, file, 140, 14);
    if(ret <= 0)
        return(ret);

    shift_missing_1_24(file);

    return(1);
}

static int getfile_internal_122(darshan_fd fd, struct darshan_job *job, 
    struct darshan_file *file)
{
    int ret;

    ret = getfile_internal_1x(fd, job, file, 138, 14);
    if(ret <= 0)
        return(ret);

    shift_missing_1_22(file);

    return(1);
}

static int getfile_internal_121(darshan_fd fd, struct darshan_job *job, 
    struct darshan_file *file)
{
    int ret;

    ret = getfile_internal_1x(fd, job, file, 133, 12);
    if(ret <= 0)
        return(ret);

    shift_missing_1_21(file);

    return(1);
}

static int getfile_internal_1x(darshan_fd fd, struct darshan_job *job, 
    struct darshan_file *file, int n_counters, int n_fcounters)
{
    char* buffer;
    int ret;
    const char* err_string;
    int i;
    uint64_t hash;
    int32_t rank;
    int64_t* counters;
    double* fcounters;
    char* name_suffix;
    int FILE_SIZE_1x = (32 + n_counters*8 + n_fcounters*8);

    memset(file, 0, sizeof(*file));

    /* set file pointer if this is the first file record; otherwise pick up
     * where we left off last time
     */
    if(fd->pos < CP_JOB_RECORD_SIZE_1x)
    {
        ret = darshan_log_seek(fd, CP_JOB_RECORD_SIZE_1x);
        if(ret < 0)
            return(ret);
    }
    
    /* space for file struct, int64 array, and double array */
    buffer = (char*)malloc(FILE_SIZE_1x);
    if(!buffer)
    {
        return(-1);
    }

    ret = darshan_log_read(fd, buffer, FILE_SIZE_1x);

    if(ret > 0 && ret < FILE_SIZE_1x)
    {
        /* got a short read */
        fprintf(stderr, "Error: invalid file record (too small)\n");
        free(buffer);
        return(-1);
    }
    else if(ret == 0)
    {
        /* hit end of file */
        free(buffer);
        return(0);
    }
    else if(ret <= 0)
    {
        /* all other errors */
        err_string = darshan_log_error(fd, &ret);
        fprintf(stderr, "Error: %s\n", err_string);
        free(buffer);
        return(-1);
    }

    /* got exactly one, correct size record */
    hash = *((int64_t*)&buffer[0]);
    rank = *((int32_t*)&buffer[8]);
    counters = ((int64_t*)&buffer[16]);
    fcounters = ((double*)&buffer[16 + n_counters*8]);
    name_suffix = &buffer[16 + n_counters*8 + n_fcounters*8];


    if(fd->swap_flag)
    {
        /* swap bytes if necessary */
        DARSHAN_BSWAP64(&hash);
        DARSHAN_BSWAP32(&rank);
        for(i=0; i<n_counters; i++)
            DARSHAN_BSWAP64(&counters[i]);
        for(i=0; i<n_fcounters; i++)
            DARSHAN_BSWAP64(&fcounters[i]);
    }

    /* assign into new format */
    file->hash = hash;
    file->rank += rank;
    memcpy(file->counters, counters, n_counters*8);
    memcpy(file->fcounters, fcounters, n_fcounters*8);
    memcpy(file->name_suffix, name_suffix, 12);

    free(buffer);
    return(1);
}
#endif

/* ** SDS ** */
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

/* ** SDS ** */
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

#if 0
static const char* darshan_log_error(darshan_fd fd, int* errnum)
{
    if(fd->gzf)
    {
        return(gzerror(fd->gzf, errnum));
    }

#ifdef HAVE_LIBBZ2
    if(fd->bzf)
    {
        return(BZ2_bzerror(fd->bzf, errnum));
    }
#endif

    *errnum = 0;
    return(NULL);
}
#endif

/* ** SDS ** */
/* return 0 on successful seek to offset, -1 on failure.
 */
static int darshan_log_seek(darshan_fd fd, off_t offset)
{
    off_t ret_off;

    /* TODO: need to look at each use case here -- do I have everything right? */

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
