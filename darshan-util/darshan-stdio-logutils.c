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

/* prototypes for each of the STDIO module's logutil functions */
static int darshan_log_get_stdio_record(darshan_fd fd, void** stdio_buf_p);
static int darshan_log_put_stdio_record(darshan_fd fd, void* stdio_buf);
static void darshan_log_print_stdio_record(void *file_rec,
    char *file_name, char *mnt_pt, char *fs_type);
static void darshan_log_print_stdio_description(int ver);
static void darshan_log_print_stdio_record_diff(void *file_rec1, char *file_name1,
    void *file_rec2, char *file_name2);
static void darshan_log_agg_stdio_records(void *rec, void *agg_rec, int init_flag);
static void darshan_log_stdio_accum_perf(void *pfile, perf_data_t *pdata);
static void darshan_log_stdio_accum_file(void *pfile, hash_entry_t *hfile, int64_t nprocs);
static void darshan_log_stdio_calc_file(hash_entry_t *file_hash, file_data_t *fdata);
static void darshan_log_stdio_file_list(hash_entry_t *file_hash, struct darshan_name_record_ref *name_hash, int detail_flag);
static void darshan_log_stdio_print_total_file(void *pfile, int stdio_ver);

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
    .log_accum_file = &darshan_log_stdio_accum_file,
    .log_accum_perf = &darshan_log_stdio_accum_perf,
    .log_calc_file = &darshan_log_stdio_calc_file,
    .log_print_total_file = &darshan_log_stdio_print_total_file,
    .log_file_list = &darshan_log_stdio_file_list,
    .log_calc_perf = &darshan_calc_perf
};

/* retrieve a STDIO record from log file descriptor 'fd', storing the
 * data in the buffer address pointed to by 'stdio_buf_p'. Return 1 on
 * successful record read, 0 on no more data, and -1 on error.
 */
static int darshan_log_get_stdio_record(darshan_fd fd, void** stdio_buf_p)
{
    struct darshan_stdio_file *file = *((struct darshan_stdio_file **)stdio_buf_p);
    int i;
    int ret;

    if(fd->mod_map[DARSHAN_STDIO_MOD].len == 0)
        return(0);

    if(*stdio_buf_p == NULL)
    {
        file = malloc(sizeof(*file));
        if(!file)
            return(-1);
    }

    /* read a STDIO module record from the darshan log file */
    ret = darshan_log_get_mod(fd, DARSHAN_STDIO_MOD, file,
        sizeof(struct darshan_stdio_file));

    if(*stdio_buf_p == NULL)
    {
        if(ret == sizeof(struct darshan_stdio_file))
            *stdio_buf_p = file;
        else
            free(file);
    }

    if(ret < 0)
        return(-1);
    else if(ret < sizeof(struct darshan_stdio_file))
        return(0);
    else
    {
        /* if the read was successful, do any necessary byte-swapping */
        if(fd->swap_flag)
        {
            DARSHAN_BSWAP64(&file->base_rec.id);
            DARSHAN_BSWAP64(&file->base_rec.rank);
            for(i=0; i<STDIO_NUM_INDICES; i++)
                DARSHAN_BSWAP64(&file->counters[i]);
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
        DARSHAN_COUNTER_PRINT(darshan_module_names[DARSHAN_STDIO_MOD],
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
    printf("#   STDIO_{OPENS|WRITES|READS|SEEKS|FLUSHES} are types of operations.\n");
    printf("#   STDIO_BYTES_*: total bytes read and written.\n");
    printf("#   STDIO_MAX_BYTE_*: highest offset byte read and written.\n");
    printf("#   STDIO_*_RANK: rank of the processes that were the fastest and slowest at I/O (for shared files).\n");
    printf("#   STDIO_*_RANK_BYTES: bytes transferred by the fastest and slowest ranks (for shared files).\n");
    printf("#   STDIO_F_*_START_TIMESTAMP: timestamp of the first call to that type of function.\n");
    printf("#   STDIO_F_*_END_TIMESTAMP: timestamp of the completion of the last call to that type of function.\n");
    printf("#   STDIO_F_*_TIME: cumulative time spent in different types of functions.\n");
    printf("#   STDIO_F_*_RANK_TIME: fastest and slowest I/O time for a single rank (for shared files).\n");
    printf("#   STDIO_F_VARIANCE_RANK_*: variance of total I/O time and bytes moved for all ranks (for shared files).\n");

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
            DARSHAN_COUNTER_PRINT(darshan_module_names[DARSHAN_STDIO_MOD],
                file1->base_rec.rank, file1->base_rec.id, stdio_counter_names[i],
                file1->counters[i], file_name1, "", "");

        }
        else if(!file1)
        {
            printf("+ ");
            DARSHAN_COUNTER_PRINT(darshan_module_names[DARSHAN_STDIO_MOD],
                file2->base_rec.rank, file2->base_rec.id, stdio_counter_names[i],
                file2->counters[i], file_name2, "", "");
        }
        else if(file1->counters[i] != file2->counters[i])
        {
            printf("- ");
            DARSHAN_COUNTER_PRINT(darshan_module_names[DARSHAN_STDIO_MOD],
                file1->base_rec.rank, file1->base_rec.id, stdio_counter_names[i],
                file1->counters[i], file_name1, "", "");
            printf("+ ");
            DARSHAN_COUNTER_PRINT(darshan_module_names[DARSHAN_STDIO_MOD],
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

static void darshan_log_stdio_accum_file(void *infile,
                                         hash_entry_t *hfile,
                                         int64_t nprocs)
{
    struct darshan_stdio_file *pfile = infile;
    int i;
    struct darshan_stdio_file* tmp;

    hfile->procs += 1;

    if(pfile->base_rec.rank == -1)
    {
        hfile->slowest_time = pfile->fcounters[STDIO_F_SLOWEST_RANK_TIME];
    }
    else
    {
        hfile->slowest_time = max(hfile->slowest_time,
            (pfile->fcounters[STDIO_F_META_TIME] +
            pfile->fcounters[STDIO_F_READ_TIME] +
            pfile->fcounters[STDIO_F_WRITE_TIME]));
    }

    if(pfile->base_rec.rank == -1)
    {
        hfile->procs = nprocs;
        hfile->type |= FILETYPE_SHARED;

    }
    else if(hfile->procs > 1)
    {
        hfile->type &= (~FILETYPE_UNIQUE);
        hfile->type |= FILETYPE_PARTSHARED;
    }
    else
    {
        hfile->type |= FILETYPE_UNIQUE;
    }

    hfile->cumul_time += pfile->fcounters[STDIO_F_META_TIME] +
                         pfile->fcounters[STDIO_F_READ_TIME] +
                         pfile->fcounters[STDIO_F_WRITE_TIME];

    if(hfile->rec_dat == NULL)
    {
        hfile->rec_dat = malloc(sizeof(struct darshan_stdio_file));
        assert(hfile->rec_dat);
        memset(hfile->rec_dat, 0, sizeof(struct darshan_stdio_file));
    }
    tmp = (struct darshan_stdio_file*)hfile->rec_dat;

    for(i = 0; i < STDIO_NUM_INDICES; i++)
    {
        switch(i)
        {
        case STDIO_MAX_BYTE_READ:
        case STDIO_MAX_BYTE_WRITTEN:
            if (tmp->counters[i] < pfile->counters[i])
            {
                tmp->counters[i] = pfile->counters[i];
            }
            break;
        case POSIX_FASTEST_RANK:
        case POSIX_SLOWEST_RANK:
        case POSIX_FASTEST_RANK_BYTES:
        case POSIX_SLOWEST_RANK_BYTES:
            tmp->counters[i] = 0;
            break;
        default:
            tmp->counters[i] += pfile->counters[i];
            break;
        }
    }

    for(i = 0; i < STDIO_F_NUM_INDICES; i++)
    {
        switch(i)
        {
            case STDIO_F_OPEN_START_TIMESTAMP:
            case STDIO_F_CLOSE_START_TIMESTAMP:
            case STDIO_F_READ_START_TIMESTAMP:
            case STDIO_F_WRITE_START_TIMESTAMP:
                if(tmp->fcounters[i] == 0 ||
                    tmp->fcounters[i] > pfile->fcounters[i])
                {
                    tmp->fcounters[i] = pfile->fcounters[i];
                }
                break;
            case STDIO_F_READ_END_TIMESTAMP:
            case STDIO_F_WRITE_END_TIMESTAMP:
            case STDIO_F_OPEN_END_TIMESTAMP:
            case STDIO_F_CLOSE_END_TIMESTAMP:
                if(tmp->fcounters[i] == 0 ||
                    tmp->fcounters[i] < pfile->fcounters[i])
                {
                    tmp->fcounters[i] = pfile->fcounters[i];
                }
                break;
            case STDIO_F_FASTEST_RANK_TIME:
            case STDIO_F_SLOWEST_RANK_TIME:
            case STDIO_F_VARIANCE_RANK_TIME:
            case STDIO_F_VARIANCE_RANK_BYTES:
                tmp->fcounters[i] = 0;
                break;
            default:
                tmp->fcounters[i] += pfile->fcounters[i];
                break;
        }
    }

    return;
}

static void darshan_log_stdio_accum_perf(void *infile,
                                         perf_data_t *pdata)
{
    struct darshan_stdio_file *pfile = infile;
    pdata->total_bytes += pfile->counters[STDIO_BYTES_READ] +
                          pfile->counters[STDIO_BYTES_WRITTEN];

    /*
     * Calculation of Shared File Time
     *   Four Methods!!!!
     *     by_cumul: sum time counters and divide by nprocs
     *               (inaccurate if lots of variance between procs)
     *     by_open: difference between timestamp of open and close
     *              (inaccurate if file is left open without i/o happening)
     *     by_open_lastio: difference between timestamp of open and the
     *                     timestamp of last i/o
     *                     (similar to above but fixes case where file is left
     *                      open after io is complete)
     *     by_slowest: use slowest rank time from log data
     *                 (most accurate but requires newer log version)
     */
    if(pfile->base_rec.rank == -1)
    {
        /* by_open */
        if(pfile->fcounters[STDIO_F_CLOSE_END_TIMESTAMP] >
            pfile->fcounters[STDIO_F_OPEN_START_TIMESTAMP])
        {
            pdata->shared_time_by_open +=
                pfile->fcounters[STDIO_F_CLOSE_END_TIMESTAMP] -
                pfile->fcounters[STDIO_F_OPEN_START_TIMESTAMP];
        }

        /* by_open_lastio */
        if(pfile->fcounters[STDIO_F_READ_END_TIMESTAMP] >
            pfile->fcounters[STDIO_F_WRITE_END_TIMESTAMP])
        {
            /* be careful: file may have been opened but not read or written */
            if(pfile->fcounters[STDIO_F_READ_END_TIMESTAMP] > pfile->fcounters[STDIO_F_OPEN_START_TIMESTAMP])
            {
                pdata->shared_time_by_open_lastio +=
                    pfile->fcounters[STDIO_F_READ_END_TIMESTAMP] -
                    pfile->fcounters[STDIO_F_OPEN_START_TIMESTAMP];
            }
        }
        else
        {
            /* be careful: file may have been opened but not read or written */
            if(pfile->fcounters[STDIO_F_WRITE_END_TIMESTAMP] > pfile->fcounters[STDIO_F_OPEN_START_TIMESTAMP])
            {
                pdata->shared_time_by_open_lastio +=
                    pfile->fcounters[STDIO_F_WRITE_END_TIMESTAMP] -
                    pfile->fcounters[STDIO_F_OPEN_START_TIMESTAMP];
            }
        }

        pdata->shared_time_by_cumul +=
            pfile->fcounters[STDIO_F_META_TIME] +
            pfile->fcounters[STDIO_F_READ_TIME] +
            pfile->fcounters[STDIO_F_WRITE_TIME];
        pdata->shared_meta_time += pfile->fcounters[STDIO_F_META_TIME];

        /* by_slowest */
        pdata->shared_time_by_slowest +=
           pfile->fcounters[STDIO_F_SLOWEST_RANK_TIME];
    }

    /*
     * Calculation of Unique File Time
     *   record the data for each file and sum it 
     */
    else
    {
        pdata->rank_cumul_io_time[pfile->base_rec.rank] +=
            (pfile->fcounters[STDIO_F_META_TIME] +
            pfile->fcounters[STDIO_F_READ_TIME] +
            pfile->fcounters[STDIO_F_WRITE_TIME]);
        pdata->rank_cumul_md_time[pfile->base_rec.rank] +=
            pfile->fcounters[STDIO_F_META_TIME];
    }

    return;
}

static void darshan_log_stdio_calc_file(hash_entry_t *file_hash,
                                        file_data_t *fdata)
{
    hash_entry_t *curr = NULL;
    hash_entry_t *tmp = NULL;
    struct darshan_stdio_file *file_rec;

    memset(fdata, 0, sizeof(*fdata));
    HASH_ITER(hlink, file_hash, curr, tmp)
    {
        int64_t bytes;
        int64_t r;
        int64_t w;

        file_rec = (struct darshan_stdio_file*)curr->rec_dat;
        assert(file_rec);

        bytes = file_rec->counters[STDIO_BYTES_READ] +
                file_rec->counters[STDIO_BYTES_WRITTEN];

        r = file_rec->counters[STDIO_READS];

        w = file_rec->counters[STDIO_WRITES];

        fdata->total += 1;
        fdata->total_size += bytes;
        fdata->total_max = max(fdata->total_max, bytes);

        if (r && !w)
        {
            fdata->read_only += 1;
            fdata->read_only_size += bytes;
            fdata->read_only_max = max(fdata->read_only_max, bytes);
        }

        if (!r && w)
        {
            fdata->write_only += 1;
            fdata->write_only_size += bytes;
            fdata->write_only_max = max(fdata->write_only_max, bytes);
        }

        if (r && w)
        {
            fdata->read_write += 1;
            fdata->read_write_size += bytes;
            fdata->read_write_max = max(fdata->read_write_max, bytes);
        }

        if ((curr->type & (FILETYPE_SHARED|FILETYPE_PARTSHARED)))
        {
            fdata->shared += 1;
            fdata->shared_size += bytes;
            fdata->shared_max = max(fdata->shared_max, bytes);
        }

        if ((curr->type & (FILETYPE_UNIQUE)))
        {
            fdata->unique += 1;
            fdata->unique_size += bytes;
            fdata->unique_max = max(fdata->unique_max, bytes);
        }
    }

    return;
}

static void darshan_log_stdio_print_total_file(
    void *infile,
    int stdio_ver)
{
    struct darshan_stdio_file *pfile = infile;
    int i;

    mod_logutils[DARSHAN_STDIO_MOD]->log_print_description(stdio_ver);
    printf("\n");
    for(i = 0; i < STDIO_NUM_INDICES; i++)
    {
        printf("total_%s: %"PRId64"\n",
            stdio_counter_names[i], pfile->counters[i]);
    }
    for(i = 0; i < STDIO_F_NUM_INDICES; i++)
    {
        printf("total_%s: %lf\n",
            stdio_f_counter_names[i], pfile->fcounters[i]);
    }
    return;
}

static void darshan_log_stdio_file_list(
    hash_entry_t *file_hash,
    struct darshan_name_record_ref *name_hash,
    int detail_flag)
{
    hash_entry_t *curr = NULL;
    hash_entry_t *tmp = NULL;
    struct darshan_stdio_file *file_rec = NULL;
    struct darshan_name_record_ref *ref = NULL;
    int i;

    /* list of columns:
     *
     * normal mode
     * - file id
     * - file name
     * - nprocs
     * - slowest I/O time
     * - average cumulative I/O time
     *
     * detailed mode
     * - first open
     * - first read
     * - first write
     * - last read
     * - last write
     * - last close
     * - STDIO opens
     */

    if(detail_flag)
        printf("\n# Per-file summary of I/O activity (detailed).\n");
    else
        printf("\n# Per-file summary of I/O activity.\n");
    printf("# -----\n");

    printf("# <record_id>: darshan record id for this file\n");
    printf("# <file_name>: full file name\n");
    printf("# <nprocs>: number of processes that opened the file\n");
    printf("# <slowest>: (estimated) time in seconds consumed in IO by slowest process\n");
    printf("# <avg>: average time in seconds consumed in IO per process\n");
    if(detail_flag)
    {
        printf("# <start_{open/close/write/read}>: start timestamp of first open, close, write, or read\n");
        printf("# <end_{open/close/write/read}>: end timestamp of last open, close, write, or read\n");
        printf("# <stdio_opens>: STDIO open calls\n");
    }

    printf("\n# <record_id>\t<file_name>\t<nprocs>\t<slowest>\t<avg>");
    if(detail_flag)
    {
        printf("\t<start_open>\t<start_close>\t<start_write>\t<start_read>");
        printf("\t<end_open>\t<end_close>\t<end_write>\t<end_read>\t<stdio_opens>");
    }
    printf("\n");

    HASH_ITER(hlink, file_hash, curr, tmp)
    {
        file_rec = (struct darshan_stdio_file*)curr->rec_dat;
        assert(file_rec);

        HASH_FIND(hlink, name_hash, &(curr->rec_id), sizeof(darshan_record_id), ref);
        assert(ref);

        printf("%" PRIu64 "\t%s\t%" PRId64 "\t%f\t%f",
            curr->rec_id,
            ref->name_record->name,
            curr->procs,
            curr->slowest_time,
            curr->cumul_time/(double)curr->procs);

        if(detail_flag)
        {
            for(i=STDIO_F_OPEN_START_TIMESTAMP; i<=STDIO_F_READ_END_TIMESTAMP; i++)
            {
                printf("\t%f", file_rec->fcounters[i]);
            }
            printf("\t%" PRId64, file_rec->counters[STDIO_OPENS]);
        }
        printf("\n");
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
