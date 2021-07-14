/*
 * Copyright (C) 2015 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

#ifdef HAVE_CONFIG_H
# include "darshan-util-config.h"
#endif

#define _GNU_SOURCE
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

/* integer counter name strings for the STDIO module */
#define X(a) #a,
char *stdio_counter_names[] = {
    STDIO_COUNTERS
};

/* floating point counter name strings for the STDIO module */
char *stdio_f_counter_names[] = {
    STDIO_F_COUNTERS
};
#undef X

#define DARSHAN_STDIO_FILE_SIZE_1 240

/* prototypes for each of the STDIO module's logutil functions */
static int darshan_log_get_stdio_record(darshan_fd fd, void** stdio_buf_p);
static int darshan_log_put_stdio_record(darshan_fd fd, void* stdio_buf);
static void darshan_log_print_stdio_record(void *file_rec,
    char *file_name, char *mnt_pt, char *fs_type);
static void darshan_log_print_stdio_description(int ver);
static void darshan_log_print_stdio_record_diff(void *file_rec1, char *file_name1,
    void *file_rec2, char *file_name2);
static void darshan_log_agg_stdio_records(void *rec, void *agg_rec, int init_flag);

/* structure storing each function needed for implementing the darshan
 * logutil interface. these functions are used for reading, writing, and
 * printing module data in a consistent manner.
 */
struct darshan_mod_logutil_funcs stdio_logutils =
{
    .log_get_record = &darshan_log_get_stdio_record,
    .log_put_record = &darshan_log_put_stdio_record,
    .log_print_record = &darshan_log_print_stdio_record,
    .log_print_description = &darshan_log_print_stdio_description,
    .log_print_diff = &darshan_log_print_stdio_record_diff,
    .log_agg_records = &darshan_log_agg_stdio_records
};

/* retrieve a STDIO record from log file descriptor 'fd', storing the
 * data in the buffer address pointed to by 'stdio_buf_p'. Return 1 on
 * successful record read, 0 on no more data, and -1 on error.
 */
static int darshan_log_get_stdio_record(darshan_fd fd, void** stdio_buf_p)
{
    struct darshan_stdio_file *file = *((struct darshan_stdio_file **)stdio_buf_p);
    int rec_len;
    int i;
    int ret;

    if(fd->mod_map[DARSHAN_STDIO_MOD].len == 0)
        return(0);

    if(fd->mod_ver[DARSHAN_STDIO_MOD] == 0 ||
        fd->mod_ver[DARSHAN_STDIO_MOD] > DARSHAN_STDIO_VER)
    {
        fprintf(stderr, "Error: Invalid STDIO module version number (got %d)\n",
            fd->mod_ver[DARSHAN_STDIO_MOD]);
        return(-1);
    }

    if(*stdio_buf_p == NULL)
    {
        file = malloc(sizeof(*file));
        if(!file)
            return(-1);
    }

    if(fd->mod_ver[DARSHAN_STDIO_MOD] == DARSHAN_STDIO_VER)
    {
        /* log format is in current version, so we don't need to do any
         * translation of counters while reading
         */
        rec_len = sizeof(struct darshan_stdio_file);
        ret = darshan_log_get_mod(fd, DARSHAN_STDIO_MOD, file, rec_len);
    }
    else
    {
        char scratch[1024] = {0};
        char *src_p, *dest_p;
        int len;

        if(fd->mod_ver[DARSHAN_STDIO_MOD] == 1)
        {
            rec_len = DARSHAN_STDIO_FILE_SIZE_1;
            ret = darshan_log_get_mod(fd, DARSHAN_STDIO_MOD, scratch, rec_len);
            if(ret != rec_len)
                goto exit;

            /* upconvert version 1 to version 2 in-place */
            dest_p = scratch + sizeof(struct darshan_base_record) +
                (2 * sizeof(int64_t));
            src_p = dest_p - sizeof(int64_t);
            len = rec_len - (src_p - scratch);
            memmove(dest_p, src_p, len);
            /* set FDOPENS to -1 */
            *((int64_t *)src_p) = -1;
        }

        memcpy(file, scratch, sizeof(struct darshan_stdio_file));
    }

exit:
    if(*stdio_buf_p == NULL)
    {
        if(ret == rec_len)
            *stdio_buf_p = file;
        else
            free(file);
    }

    if(ret < 0)
        return(-1);
    else if(ret < rec_len)
        return(0);
    else
    {
        /* if the read was successful, do any necessary byte-swapping */
        if(fd->swap_flag)
        {
            DARSHAN_BSWAP64(&file->base_rec.id);
            DARSHAN_BSWAP64(&file->base_rec.rank);
            for(i=0; i<STDIO_NUM_INDICES; i++)
            {
                /* skip counters we explicitly set since they don't
                 * need to be byte swapped
                 */
                if((fd->mod_ver[DARSHAN_STDIO_MOD] == 1) &&
                    (i == STDIO_FDOPENS))
                    continue;
                DARSHAN_BSWAP64(&file->counters[i]);
            }
            for(i=0; i<STDIO_F_NUM_INDICES; i++)
                DARSHAN_BSWAP64(&file->fcounters[i]);
        }

        return(1);
    }
}

/* write the STDIO record stored in 'stdio_buf' to log file descriptor 'fd'.
 * Return 0 on success, -1 on failure
 */
static int darshan_log_put_stdio_record(darshan_fd fd, void* stdio_buf)
{
    struct darshan_stdio_file *rec = (struct darshan_stdio_file *)stdio_buf;
    int ret;

    /* append STDIO record to darshan log file */
    ret = darshan_log_put_mod(fd, DARSHAN_STDIO_MOD, rec,
        sizeof(struct darshan_stdio_file), DARSHAN_STDIO_VER);
    if(ret < 0)
        return(-1);

    return(0);
}

/* print all I/O data record statistics for the given STDIO record */
static void darshan_log_print_stdio_record(void *file_rec, char *file_name,
    char *mnt_pt, char *fs_type)
{
    int i;
    struct darshan_stdio_file *stdio_rec =
        (struct darshan_stdio_file *)file_rec;

    /* print each of the integer and floating point counters for the STDIO module */
    for(i=0; i<STDIO_NUM_INDICES; i++)
    {
        /* macro defined in darshan-logutils.h */
        DARSHAN_D_COUNTER_PRINT(darshan_module_names[DARSHAN_STDIO_MOD],
            stdio_rec->base_rec.rank, stdio_rec->base_rec.id, stdio_counter_names[i],
            stdio_rec->counters[i], file_name, mnt_pt, fs_type);
    }

    for(i=0; i<STDIO_F_NUM_INDICES; i++)
    {
        /* macro defined in darshan-logutils.h */
        DARSHAN_F_COUNTER_PRINT(darshan_module_names[DARSHAN_STDIO_MOD],
            stdio_rec->base_rec.rank, stdio_rec->base_rec.id, stdio_f_counter_names[i],
            stdio_rec->fcounters[i], file_name, mnt_pt, fs_type);
    }

    return;
}

/* print out a description of the STDIO module record fields */
static void darshan_log_print_stdio_description(int ver)
{
    printf("\n# description of STDIO counters:\n");
    printf("#   STDIO_{OPENS|FDOPENS|WRITES|READS|SEEKS|FLUSHES} are types of operations.\n");
    printf("#   STDIO_BYTES_*: total bytes read and written.\n");
    printf("#   STDIO_MAX_BYTE_*: highest offset byte read and written.\n");
    printf("#   STDIO_*_RANK: rank of the processes that were the fastest and slowest at I/O (for shared files).\n");
    printf("#   STDIO_*_RANK_BYTES: bytes transferred by the fastest and slowest ranks (for shared files).\n");
    printf("#   STDIO_F_*_START_TIMESTAMP: timestamp of the first call to that type of function.\n");
    printf("#   STDIO_F_*_END_TIMESTAMP: timestamp of the completion of the last call to that type of function.\n");
    printf("#   STDIO_F_*_TIME: cumulative time spent in different types of functions.\n");
    printf("#   STDIO_F_*_RANK_TIME: fastest and slowest I/O time for a single rank (for shared files).\n");
    printf("#   STDIO_F_VARIANCE_RANK_*: variance of total I/O time and bytes moved for all ranks (for shared files).\n");

    if(ver == 1)
    {
        printf("\n# WARNING: STDIO module log format version 1 has the following limitations:\n");
        printf("# - No support for properly instrumenting fdopen operations (STDIO_FDOPENS)\n");
    }

    if(ver >= 2)
    {
        printf("\n# WARNING: STDIO_OPENS counter includes STDIO_FDOPENS count\n");
        printf("\n# WARNING: STDIO counters related to file offsets may be incorrect if a file is simultaneously accessed by both STDIO and POSIX (e.g., using fdopen())\n");
        printf("# \t- Affected counters include: MAX_BYTE_{READ|WRITTEN}\n");
    }

    return;
}

static void darshan_log_print_stdio_record_diff(void *file_rec1, char *file_name1,
    void *file_rec2, char *file_name2)
{
    struct darshan_stdio_file *file1 = (struct darshan_stdio_file *)file_rec1;
    struct darshan_stdio_file *file2 = (struct darshan_stdio_file *)file_rec2;
    int i;

    /* NOTE: we assume that both input records are the same module format version */

    for(i=0; i<STDIO_NUM_INDICES; i++)
    {
        if(!file2)
        {
            printf("- ");
            DARSHAN_D_COUNTER_PRINT(darshan_module_names[DARSHAN_STDIO_MOD],
                file1->base_rec.rank, file1->base_rec.id, stdio_counter_names[i],
                file1->counters[i], file_name1, "", "");

        }
        else if(!file1)
        {
            printf("+ ");
            DARSHAN_D_COUNTER_PRINT(darshan_module_names[DARSHAN_STDIO_MOD],
                file2->base_rec.rank, file2->base_rec.id, stdio_counter_names[i],
                file2->counters[i], file_name2, "", "");
        }
        else if(file1->counters[i] != file2->counters[i])
        {
            printf("- ");
            DARSHAN_D_COUNTER_PRINT(darshan_module_names[DARSHAN_STDIO_MOD],
                file1->base_rec.rank, file1->base_rec.id, stdio_counter_names[i],
                file1->counters[i], file_name1, "", "");
            printf("+ ");
            DARSHAN_D_COUNTER_PRINT(darshan_module_names[DARSHAN_STDIO_MOD],
                file2->base_rec.rank, file2->base_rec.id, stdio_counter_names[i],
                file2->counters[i], file_name2, "", "");
        }
    }

    for(i=0; i<STDIO_F_NUM_INDICES; i++)
    {
        if(!file2)
        {
            printf("- ");
            DARSHAN_F_COUNTER_PRINT(darshan_module_names[DARSHAN_STDIO_MOD],
                file1->base_rec.rank, file1->base_rec.id, stdio_f_counter_names[i],
                file1->fcounters[i], file_name1, "", "");

        }
        else if(!file1)
        {
            printf("+ ");
            DARSHAN_F_COUNTER_PRINT(darshan_module_names[DARSHAN_STDIO_MOD],
                file2->base_rec.rank, file2->base_rec.id, stdio_f_counter_names[i],
                file2->fcounters[i], file_name2, "", "");
        }
        else if(file1->fcounters[i] != file2->fcounters[i])
        {
            printf("- ");
            DARSHAN_F_COUNTER_PRINT(darshan_module_names[DARSHAN_STDIO_MOD],
                file1->base_rec.rank, file1->base_rec.id, stdio_f_counter_names[i],
                file1->fcounters[i], file_name1, "", "");
            printf("+ ");
            DARSHAN_F_COUNTER_PRINT(darshan_module_names[DARSHAN_STDIO_MOD],
                file2->base_rec.rank, file2->base_rec.id, stdio_f_counter_names[i],
                file2->fcounters[i], file_name2, "", "");
        }
    }

    return;
}

/* simple helper struct for determining time & byte variances */
struct var_t
{
    double n;
    double M;
    double S;
};

static void darshan_log_agg_stdio_records(void *rec, void *agg_rec, int init_flag)
{
    struct darshan_stdio_file *stdio_rec = (struct darshan_stdio_file *)rec;
    struct darshan_stdio_file *agg_stdio_rec = (struct darshan_stdio_file *)agg_rec;
    int i;
    double old_M;
    double stdio_time = stdio_rec->fcounters[STDIO_F_READ_TIME] +
        stdio_rec->fcounters[STDIO_F_WRITE_TIME] +
        stdio_rec->fcounters[STDIO_F_META_TIME];
    double stdio_bytes = (double)stdio_rec->counters[STDIO_BYTES_READ] +
        stdio_rec->counters[STDIO_BYTES_WRITTEN];
    struct var_t *var_time_p = (struct var_t *)
        ((char *)rec + sizeof(struct darshan_stdio_file));
    struct var_t *var_bytes_p = (struct var_t *)
        ((char *)var_time_p + sizeof(struct var_t));

    for(i = 0; i < STDIO_NUM_INDICES; i++)
    {
        switch(i)
        {
            case STDIO_OPENS:
            case STDIO_FDOPENS:
            case STDIO_READS:
            case STDIO_WRITES:
            case STDIO_SEEKS:
            case STDIO_FLUSHES:
            case STDIO_BYTES_WRITTEN:
            case STDIO_BYTES_READ:
                /* sum */
                agg_stdio_rec->counters[i] += stdio_rec->counters[i];
                break;
            case STDIO_MAX_BYTE_READ:
            case STDIO_MAX_BYTE_WRITTEN:
                /* max */
                if(stdio_rec->counters[i] > agg_stdio_rec->counters[i])
                {
                    agg_stdio_rec->counters[i] = stdio_rec->counters[i];
                }
                break;
            case STDIO_FASTEST_RANK:
            case STDIO_FASTEST_RANK_BYTES:
            case STDIO_SLOWEST_RANK:
            case STDIO_SLOWEST_RANK_BYTES:
                /* these are set with the FP counters */
                break;
            default:
                agg_stdio_rec->counters[i] = -1;
                break;
        }
    }

    for(i = 0; i < STDIO_F_NUM_INDICES; i++)
    {
        switch(i)
        {
            case STDIO_F_META_TIME:
            case STDIO_F_WRITE_TIME:
            case STDIO_F_READ_TIME:
                /* sum */
                agg_stdio_rec->fcounters[i] += stdio_rec->fcounters[i];
                break;
            case STDIO_F_OPEN_START_TIMESTAMP:
            case STDIO_F_CLOSE_START_TIMESTAMP:
            case STDIO_F_WRITE_START_TIMESTAMP:
            case STDIO_F_READ_START_TIMESTAMP:
                /* minimum non-zero */
                if((stdio_rec->fcounters[i] > 0)  &&
                    ((agg_stdio_rec->fcounters[i] == 0) ||
                    (stdio_rec->fcounters[i] < agg_stdio_rec->fcounters[i])))
                {
                    agg_stdio_rec->fcounters[i] = stdio_rec->fcounters[i];
                }
                break;
            case STDIO_F_OPEN_END_TIMESTAMP:
            case STDIO_F_CLOSE_END_TIMESTAMP:
            case STDIO_F_WRITE_END_TIMESTAMP:
            case STDIO_F_READ_END_TIMESTAMP:
                /* maximum */
                if(stdio_rec->fcounters[i] > agg_stdio_rec->fcounters[i])
                {
                    agg_stdio_rec->fcounters[i] = stdio_rec->fcounters[i];
                }
                break;
            case STDIO_F_FASTEST_RANK_TIME:
                if(init_flag)
                {
                    /* set fastest rank counters according to root rank. these counters
                     * will be determined as the aggregation progresses.
                     */
                    agg_stdio_rec->counters[STDIO_FASTEST_RANK] = stdio_rec->base_rec.rank;
                    agg_stdio_rec->counters[STDIO_FASTEST_RANK_BYTES] = stdio_bytes;
                    agg_stdio_rec->fcounters[STDIO_F_FASTEST_RANK_TIME] = stdio_time;
                }

                if(stdio_time < agg_stdio_rec->fcounters[STDIO_F_FASTEST_RANK_TIME])
                {
                    agg_stdio_rec->counters[STDIO_FASTEST_RANK] = stdio_rec->base_rec.rank;
                    agg_stdio_rec->counters[STDIO_FASTEST_RANK_BYTES] = stdio_bytes;
                    agg_stdio_rec->fcounters[STDIO_F_FASTEST_RANK_TIME] = stdio_time;
                }
                break;
            case STDIO_F_SLOWEST_RANK_TIME:
                if(init_flag)
                {
                    /* set slowest rank counters according to root rank. these counters
                     * will be determined as the aggregation progresses.
                     */
                    agg_stdio_rec->counters[STDIO_SLOWEST_RANK] = stdio_rec->base_rec.rank;
                    agg_stdio_rec->counters[STDIO_SLOWEST_RANK_BYTES] = stdio_bytes;
                    agg_stdio_rec->fcounters[STDIO_F_SLOWEST_RANK_TIME] = stdio_time;
                }

                if(stdio_time > agg_stdio_rec->fcounters[STDIO_F_SLOWEST_RANK_TIME])
                {
                    agg_stdio_rec->counters[STDIO_SLOWEST_RANK] = stdio_rec->base_rec.rank;
                    agg_stdio_rec->counters[STDIO_SLOWEST_RANK_BYTES] = stdio_bytes;
                    agg_stdio_rec->fcounters[STDIO_F_SLOWEST_RANK_TIME] = stdio_time;
                }
                break;
            case STDIO_F_VARIANCE_RANK_TIME:
                if(init_flag)
                {
                    var_time_p->n = 1;
                    var_time_p->M = stdio_time;
                    var_time_p->S = 0;
                }
                else
                {
                    old_M = var_time_p->M;

                    var_time_p->n++;
                    var_time_p->M += (stdio_time - var_time_p->M) / var_time_p->n;
                    var_time_p->S += (stdio_time - var_time_p->M) * (stdio_time - old_M);

                    agg_stdio_rec->fcounters[STDIO_F_VARIANCE_RANK_TIME] =
                        var_time_p->S / var_time_p->n;
                }
                break;
            case STDIO_F_VARIANCE_RANK_BYTES:
                if(init_flag)
                {
                    var_bytes_p->n = 1;
                    var_bytes_p->M = stdio_bytes;
                    var_bytes_p->S = 0;
                }
                else
                {
                    old_M = var_bytes_p->M;

                    var_bytes_p->n++;
                    var_bytes_p->M += (stdio_bytes - var_bytes_p->M) / var_bytes_p->n;
                    var_bytes_p->S += (stdio_bytes - var_bytes_p->M) * (stdio_bytes - old_M);

                    agg_stdio_rec->fcounters[STDIO_F_VARIANCE_RANK_BYTES] =
                        var_bytes_p->S / var_bytes_p->n;
                }
                break;
            default:
                agg_stdio_rec->fcounters[i] = -1;
                break;
        }
    }

    return;
}

/*
 * Local variables:
 *  c-indent-level: 4
 *  c-basic-offset: 4
 * End:
 *
 * vim: ts=8 sts=4 sw=4 expandtab
 */
