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

#define max(a,b) (((a) > (b)) ? (a) : (b))

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
static int darshan_log_sizeof_stdio_record(void* stdio_buf_p);
static int darshan_log_record_metrics_stdio_record(void*  stdio_buf_p,
                                                 uint64_t* rec_id,
                                                 int64_t* r_bytes,
                                                 int64_t* w_bytes,
                                                 int64_t* max_offset,
                                                 double* io_total_time,
                                                 double* md_only_time,
                                                 double* rw_only_time,
                                                 int64_t* rank,
                                                 int64_t* nprocs);

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
    .log_agg_records = &darshan_log_agg_stdio_records,
    .log_sizeof_record = &darshan_log_sizeof_stdio_record,
    .log_record_metrics = &darshan_log_record_metrics_stdio_record
};

static int darshan_log_sizeof_stdio_record(void* stdio_buf_p)
{
    /* stdio records have a fixed size */
    return(sizeof(struct darshan_stdio_file));
}

static int darshan_log_record_metrics_stdio_record(void*  stdio_buf_p,
                                                 uint64_t* rec_id,
                                                 int64_t* r_bytes,
                                                 int64_t* w_bytes,
                                                 int64_t* max_offset,
                                                 double* io_total_time,
                                                 double* md_only_time,
                                                 double* rw_only_time,
                                                 int64_t* rank,
                                                 int64_t* nprocs)
{
    struct darshan_stdio_file *stdio_rec = (struct darshan_stdio_file *)stdio_buf_p;

    *rec_id = stdio_rec->base_rec.id;
    *r_bytes = stdio_rec->counters[STDIO_BYTES_READ];
    *w_bytes = stdio_rec->counters[STDIO_BYTES_WRITTEN];

    *max_offset = max(stdio_rec->counters[STDIO_MAX_BYTE_READ], stdio_rec->counters[STDIO_MAX_BYTE_WRITTEN]);

    *rank = stdio_rec->base_rec.rank;
    /* nprocs is 1 per record, unless rank is negative, in which case we
     * report -1 as the rank value to represent "all"
     */
    if(stdio_rec->base_rec.rank < 0)
        *nprocs = -1;
    else
        *nprocs = 1;

    if(stdio_rec->base_rec.rank < 0) {
        /* shared file records populate a counter with the slowest rank time
         * (derived during reduction).  They do not have a breakdown of meta
         * and rw time, though.
         */
        *io_total_time = stdio_rec->fcounters[STDIO_F_SLOWEST_RANK_TIME];
        *md_only_time = 0;
        *rw_only_time = 0;
    }
    else {
        /* non-shared records have separate meta, read, and write values
         * that we can combine as needed
         */
        *io_total_time = stdio_rec->fcounters[STDIO_F_META_TIME] +
                         stdio_rec->fcounters[STDIO_F_READ_TIME] +
                         stdio_rec->fcounters[STDIO_F_WRITE_TIME];
        *md_only_time = stdio_rec->fcounters[STDIO_F_META_TIME];
        *rw_only_time = stdio_rec->fcounters[STDIO_F_READ_TIME] +
                        stdio_rec->fcounters[STDIO_F_WRITE_TIME];
    }

    return(0);
}

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
    int64_t stdio_fastest_rank, stdio_slowest_rank,
        stdio_fastest_bytes, stdio_slowest_bytes;
    double stdio_fastest_time, stdio_slowest_time;
    int shared_file_flag = 0;

    /* For the incoming record, we need to determine what values to use for
     * subsequent comparision against the aggregate record's fastest and
     * slowest fields. This is is complicated by the fact that shared file
     * records already have derived values, while unique file records do
     * not.  Handle both cases here so that this function can be generic.
     */
    if(stdio_rec->base_rec.rank == -1)
    {
        /* shared files should have pre-calculated fastest and slowest
         * counters */
        stdio_fastest_rank = stdio_rec->counters[STDIO_FASTEST_RANK];
        stdio_slowest_rank = stdio_rec->counters[STDIO_SLOWEST_RANK];
        stdio_fastest_bytes = stdio_rec->counters[STDIO_FASTEST_RANK_BYTES];
        stdio_slowest_bytes = stdio_rec->counters[STDIO_SLOWEST_RANK_BYTES];
        stdio_fastest_time = stdio_rec->fcounters[STDIO_F_FASTEST_RANK_TIME];
        stdio_slowest_time = stdio_rec->fcounters[STDIO_F_SLOWEST_RANK_TIME];
    }
    else
    {
        /* for non-shared files, derive bytes and time using data from this
         * rank
         */
        stdio_fastest_rank = stdio_rec->base_rec.rank;
        stdio_slowest_rank = stdio_fastest_rank;
        stdio_fastest_bytes = stdio_rec->counters[STDIO_BYTES_READ] +
            stdio_rec->counters[STDIO_BYTES_WRITTEN];
        stdio_slowest_bytes = stdio_fastest_bytes;
        stdio_fastest_time = stdio_rec->fcounters[STDIO_F_READ_TIME] +
            stdio_rec->fcounters[STDIO_F_WRITE_TIME] +
            stdio_rec->fcounters[STDIO_F_META_TIME];
        stdio_slowest_time = stdio_fastest_time;
    }

#if 0
    /* NOTE: the code commented out in this function is used for variance
     * calculation.  This metric is most helpful for shared file records,
     * but this function has now been generalized to allow for aggregation
     * of arbitrary records in a log.
     *
     * This functionality could be reinstated as an optional feature.
     * Ideally in that case the caller would also provide a buffer for
     * stateful calculations; the current logic assumes that it is safe to
     * use additional bytes off of the end of the rec argument buffer.
     */
    struct var_t *var_time_p = (struct var_t *)
        ((char *)rec + sizeof(struct darshan_stdio_file));
    struct var_t *var_bytes_p = (struct var_t *)
        ((char *)var_time_p + sizeof(struct var_t));
    double old_M;
#endif

    /* if this is our first record, store base id and rank */
    if(init_flag)
    {
        agg_stdio_rec->base_rec.rank = stdio_rec->base_rec.rank;
        agg_stdio_rec->base_rec.id = stdio_rec->base_rec.id;
    }

    /* so far do all of the records reference the same file? */
    if(agg_stdio_rec->base_rec.id == stdio_rec->base_rec.id)
        shared_file_flag = 1;
    else
        agg_stdio_rec->base_rec.id = 0;

    /* so far do all of the records reference the same rank? */
    if(agg_stdio_rec->base_rec.rank != stdio_rec->base_rec.rank)
        agg_stdio_rec->base_rec.rank = -1;

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
            /* intentionally do not include a default block; we want to
             * get a compile-time warning in this function when new
             * counters are added to the enumeration to make sure we
             * handle them all correctly.
             */
#if 0
            default:
                agg_stdio_rec->counters[i] = -1;
                break;
#endif
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
                if(!shared_file_flag)
                {
                    /* The fastest counters are only valid under these
                     * conditions when aggregating records that all refer to
                     * the same file.
                     */
                    agg_stdio_rec->counters[STDIO_FASTEST_RANK] = -1;
                    agg_stdio_rec->counters[STDIO_FASTEST_RANK_BYTES] = -1;
                    agg_stdio_rec->fcounters[STDIO_F_FASTEST_RANK_TIME] = 0.0;
                    break;
                }
                if (init_flag ||
                    stdio_fastest_time < agg_stdio_rec->fcounters[STDIO_F_FASTEST_RANK_TIME]) {
                    /* The incoming record wins if a) this is the first
                     * record we are aggregating or b) it is the fastest
                     * record we have seen so far.
                     */
                    agg_stdio_rec->counters[STDIO_FASTEST_RANK]
                        = stdio_fastest_rank;
                    agg_stdio_rec->counters[STDIO_FASTEST_RANK_BYTES]
                        = stdio_fastest_bytes;
                    agg_stdio_rec->fcounters[STDIO_F_FASTEST_RANK_TIME]
                        = stdio_fastest_time;
                }
                break;
            case STDIO_F_SLOWEST_RANK_TIME:
                if(!shared_file_flag)
                {
                    /* The slowest counters are only valid under these
                     * conditions when aggregating records that all refer to
                     * the same file.
                     */
                    agg_stdio_rec->counters[STDIO_SLOWEST_RANK] = -1;
                    agg_stdio_rec->counters[STDIO_SLOWEST_RANK_BYTES] = -1;
                    agg_stdio_rec->fcounters[STDIO_F_SLOWEST_RANK_TIME] = 0.0;
                    break;
                }
                if (init_flag ||
                    stdio_slowest_time > agg_stdio_rec->fcounters[STDIO_F_SLOWEST_RANK_TIME]) {
                    /* The incoming record wins if a) this is the first
                     * record we are aggregating or b) it is the slowest
                     * record we have seen so far.
                     */
                    agg_stdio_rec->counters[STDIO_SLOWEST_RANK]
                        = stdio_slowest_rank;
                    agg_stdio_rec->counters[STDIO_SLOWEST_RANK_BYTES]
                        = stdio_slowest_bytes;
                    agg_stdio_rec->fcounters[STDIO_F_SLOWEST_RANK_TIME]
                        = stdio_slowest_time;
                }
                break;

            case STDIO_F_VARIANCE_RANK_TIME:
#if 0
/* NOTE: see comment at the top of this function about the var_* variables */
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
#else
                agg_stdio_rec->fcounters[i] = 0;
#endif
                break;
            case STDIO_F_VARIANCE_RANK_BYTES:
#if 0
/* NOTE: see comment at the top of this function about the var_* variables */
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
#else
                agg_stdio_rec->fcounters[i] = 0;
#endif
                break;
            /* intentionally do not include a default block; we want to
             * get a compile-time warning in this function when new
             * counters are added to the enumeration to make sure we
             * handle them all correctly.
             */
#if 0
            default:
                agg_stdio_rec->fcounters[i] = -1;
                break;
#endif
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
