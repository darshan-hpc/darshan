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

/* counter name strings for the POSIX module */
#define X(a) #a,
char *posix_counter_names[] = {
    POSIX_COUNTERS
};

char *posix_f_counter_names[] = {
    POSIX_F_COUNTERS
};
#undef X

#define DARSHAN_POSIX_FILE_SIZE_1 680

static int darshan_log_get_posix_file(darshan_fd fd, void* posix_buf);
static int darshan_log_put_posix_file(darshan_fd fd, void* posix_buf, int ver);
static void darshan_log_print_posix_file(void *file_rec,
    char *file_name, char *mnt_pt, char *fs_type, int ver);
static void darshan_log_print_posix_description(void);
static void darshan_log_print_posix_file_diff(void *file_rec1, char *file_name1,
    void *file_rec2, char *file_name2);
static void darshan_log_agg_posix_files(void *rec, void *agg_rec, int init_flag);

struct darshan_mod_logutil_funcs posix_logutils =
{
    .log_get_record = &darshan_log_get_posix_file,
    .log_put_record = &darshan_log_put_posix_file,
    .log_print_record = &darshan_log_print_posix_file,
    .log_print_description = &darshan_log_print_posix_description,
    .log_print_diff = &darshan_log_print_posix_file_diff,
    .log_agg_records = &darshan_log_agg_posix_files,
};

static int darshan_log_get_posix_file(darshan_fd fd, void* posix_buf)
{
    struct darshan_posix_file *file = (struct darshan_posix_file *)posix_buf;
    int rec_len;
    char *buffer, *p;
    int i;
    int ret = -1;

    /* read the POSIX record from file, checking the version first so we
     * can correctly up-convert to the current darshan version
     */
    if(fd->mod_ver[DARSHAN_POSIX_MOD] == 1)
    {
        buffer = malloc(DARSHAN_POSIX_FILE_SIZE_1);
        if(!buffer)
            return(-1);

        rec_len = DARSHAN_POSIX_FILE_SIZE_1;
        ret = darshan_log_get_mod(fd, DARSHAN_POSIX_MOD, buffer, rec_len);
        if(ret == rec_len)
        {
            /* copy record data directly from the temporary buffer into the 
             * corresponding locations in the output file record
             */
            p = buffer;
            memcpy(&(file->base_rec), p, sizeof(struct darshan_base_record));
            p += sizeof(struct darshan_base_record);
            memcpy(&(file->counters[0]), p, 6 * sizeof(int64_t));
            p += (6 * sizeof(int64_t));
            p += (4 * sizeof(int64_t)); /* skip old stdio counters */
            memcpy(&(file->counters[6]), p, 58 * sizeof(int64_t));
            p += (58 * sizeof(int64_t));
            memcpy(&(file->fcounters[0]), p, 15 * sizeof(double));
        }
        free(buffer);
    }
    else if(fd->mod_ver[DARSHAN_POSIX_MOD] == 2)
    {
        rec_len = sizeof(struct darshan_posix_file);
        ret = darshan_log_get_mod(fd, DARSHAN_POSIX_MOD, posix_buf, rec_len);
    }

    if(ret < 0)
        return(-1);
    else if(ret < rec_len)
        return(0);
    else
    {
        if(fd->swap_flag)
        {
            /* swap bytes if necessary */
            DARSHAN_BSWAP64(&file->base_rec.id);
            DARSHAN_BSWAP64(&file->base_rec.rank);
            for(i=0; i<POSIX_NUM_INDICES; i++)
                DARSHAN_BSWAP64(&file->counters[i]);
            for(i=0; i<POSIX_F_NUM_INDICES; i++)
                DARSHAN_BSWAP64(&file->fcounters[i]);
        }

        return(1);
    }
}

static int darshan_log_put_posix_file(darshan_fd fd, void* posix_buf, int ver)
{
    struct darshan_posix_file *file = (struct darshan_posix_file *)posix_buf;
    int ret;

    ret = darshan_log_put_mod(fd, DARSHAN_POSIX_MOD, file,
        sizeof(struct darshan_posix_file), ver);
    if(ret < 0)
        return(-1);

    return(0);
}

static void darshan_log_print_posix_file(void *file_rec, char *file_name,
    char *mnt_pt, char *fs_type, int ver)
{
    int i;
    struct darshan_posix_file *posix_file_rec =
        (struct darshan_posix_file *)file_rec;

    for(i=0; i<POSIX_NUM_INDICES; i++)
    {
        DARSHAN_COUNTER_PRINT(darshan_module_names[DARSHAN_POSIX_MOD],
            posix_file_rec->base_rec.rank, posix_file_rec->base_rec.id,
            posix_counter_names[i], posix_file_rec->counters[i],
            file_name, mnt_pt, fs_type);
    }

    for(i=0; i<POSIX_F_NUM_INDICES; i++)
    {
        DARSHAN_F_COUNTER_PRINT(darshan_module_names[DARSHAN_POSIX_MOD],
            posix_file_rec->base_rec.rank, posix_file_rec->base_rec.id,
            posix_f_counter_names[i], posix_file_rec->fcounters[i],
            file_name, mnt_pt, fs_type);
    }

    return;
}

static void darshan_log_print_posix_description()
{
    printf("\n# description of POSIX counters:\n");
    printf("#   POSIX_*: posix operation counts.\n");
    printf("#   READS,WRITES,OPENS,SEEKS,STATS, and MMAPS are types of operations.\n");
    printf("#   POSIX_MODE: mode that file was opened in.\n");
    printf("#   POSIX_BYTES_*: total bytes read and written.\n");
    printf("#   POSIX_MAX_BYTE_*: highest offset byte read and written.\n");
    printf("#   POSIX_CONSEC_*: number of exactly adjacent reads and writes.\n");
    printf("#   POSIX_SEQ_*: number of reads and writes from increasing offsets.\n");
    printf("#   POSIX_RW_SWITCHES: number of times access alternated between read and write.\n");
    printf("#   POSIX_*_ALIGNMENT: memory and file alignment.\n");
    printf("#   POSIX_*_NOT_ALIGNED: number of reads and writes that were not aligned.\n");
    printf("#   POSIX_MAX_*_TIME_SIZE: size of the slowest read and write operations.\n");
    printf("#   POSIX_SIZE_*_*: histogram of read and write access sizes.\n");
    printf("#   POSIX_STRIDE*_STRIDE: the four most common strides detected.\n");
    printf("#   POSIX_STRIDE*_COUNT: count of the four most common strides.\n");
    printf("#   POSIX_ACCESS*_ACCESS: the four most common access sizes.\n");
    printf("#   POSIX_ACCESS*_COUNT: count of the four most common access sizes.\n");
    printf("#   POSIX_*_RANK: rank of the processes that were the fastest and slowest at I/O (for shared files).\n");
    printf("#   POSIX_*_RANK_BYTES: bytes transferred by the fastest and slowest ranks (for shared files).\n");
    printf("#   POSIX_F_OPEN_TIMESTAMP: timestamp of first open.\n");
    printf("#   POSIX_F_*_START_TIMESTAMP: timestamp of first read/write.\n");
    printf("#   POSIX_F_*_END_TIMESTAMP: timestamp of last read/write.\n");
    printf("#   POSIX_F_CLOSE_TIMESTAMP: timestamp of last close.\n");
    printf("#   POSIX_F_READ/WRITE/META_TIME: cumulative time spent in read, write, or metadata operations.\n");
    printf("#   POSIX_F_MAX_*_TIME: duration of the slowest read and write operations.\n");
    printf("#   POSIX_F_*_RANK_TIME: fastest and slowest I/O time for a single rank (for shared files).\n");
    printf("#   POSIX_F_VARIANCE_RANK_*: variance of total I/O time and bytes moved for all ranks (for shared files).\n");

    DARSHAN_PRINT_HEADER();

    return;
}

static void darshan_log_print_posix_file_diff(void *file_rec1, char *file_name1,
    void *file_rec2, char *file_name2)
{
    struct darshan_posix_file *file1 = (struct darshan_posix_file *)file_rec1;
    struct darshan_posix_file *file2 = (struct darshan_posix_file *)file_rec2;
    int i;

    /* NOTE: we assume that both input records are the same module format version */

    for(i=0; i<POSIX_NUM_INDICES; i++)
    {
        if(!file2)
        {
            printf("- ");
            DARSHAN_COUNTER_PRINT(darshan_module_names[DARSHAN_POSIX_MOD],
                file1->base_rec.rank, file1->base_rec.id, posix_counter_names[i],
                file1->counters[i], file_name1, "", "");

        }
        else if(!file1)
        {
            printf("+ ");
            DARSHAN_COUNTER_PRINT(darshan_module_names[DARSHAN_POSIX_MOD],
                file2->base_rec.rank, file2->base_rec.id, posix_counter_names[i],
                file2->counters[i], file_name2, "", "");
        }
        else if(file1->counters[i] != file2->counters[i])
        {
            printf("- ");
            DARSHAN_COUNTER_PRINT(darshan_module_names[DARSHAN_POSIX_MOD],
                file1->base_rec.rank, file1->base_rec.id, posix_counter_names[i],
                file1->counters[i], file_name1, "", "");
            printf("+ ");
            DARSHAN_COUNTER_PRINT(darshan_module_names[DARSHAN_POSIX_MOD],
                file2->base_rec.rank, file2->base_rec.id, posix_counter_names[i],
                file2->counters[i], file_name2, "", "");
        }
    }

    for(i=0; i<POSIX_F_NUM_INDICES; i++)
    {
        if(!file2)
        {
            printf("- ");
            DARSHAN_F_COUNTER_PRINT(darshan_module_names[DARSHAN_POSIX_MOD],
                file1->base_rec.rank, file1->base_rec.id, posix_f_counter_names[i],
                file1->fcounters[i], file_name1, "", "");

        }
        else if(!file1)
        {
            printf("+ ");
            DARSHAN_F_COUNTER_PRINT(darshan_module_names[DARSHAN_POSIX_MOD],
                file2->base_rec.rank, file2->base_rec.id, posix_f_counter_names[i],
                file2->fcounters[i], file_name2, "", "");
        }
        else if(file1->fcounters[i] != file2->fcounters[i])
        {
            printf("- ");
            DARSHAN_F_COUNTER_PRINT(darshan_module_names[DARSHAN_POSIX_MOD],
                file1->base_rec.rank, file1->base_rec.id, posix_f_counter_names[i],
                file1->fcounters[i], file_name1, "", "");
            printf("+ ");
            DARSHAN_F_COUNTER_PRINT(darshan_module_names[DARSHAN_POSIX_MOD],
                file2->base_rec.rank, file2->base_rec.id, posix_f_counter_names[i],
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

static void darshan_log_agg_posix_files(void *rec, void *agg_rec, int init_flag)
{
    struct darshan_posix_file *psx_rec = (struct darshan_posix_file *)rec;
    struct darshan_posix_file *agg_psx_rec = (struct darshan_posix_file *)agg_rec;
    int i, j, k;
    int total_count;
    int64_t tmp_val[4];
    int64_t tmp_cnt[4];
    int tmp_ndx;
    double old_M;
    double psx_time = psx_rec->fcounters[POSIX_F_READ_TIME] +
        psx_rec->fcounters[POSIX_F_WRITE_TIME] +
        psx_rec->fcounters[POSIX_F_META_TIME];
    double psx_bytes = (double)psx_rec->counters[POSIX_BYTES_READ] +
        psx_rec->counters[POSIX_BYTES_WRITTEN];
    struct var_t *var_time_p = (struct var_t *)
        ((char *)rec + sizeof(struct darshan_posix_file));
    struct var_t *var_bytes_p = (struct var_t *)
        ((char *)var_time_p + sizeof(struct var_t));

    for(i = 0; i < POSIX_NUM_INDICES; i++)
    {
        switch(i)
        {
            case POSIX_OPENS:
            case POSIX_READS:
            case POSIX_WRITES:
            case POSIX_SEEKS:
            case POSIX_STATS:
            case POSIX_MMAPS:
            case POSIX_FSYNCS:
            case POSIX_FDSYNCS:
            case POSIX_BYTES_READ:
            case POSIX_BYTES_WRITTEN:
            case POSIX_CONSEC_READS:
            case POSIX_CONSEC_WRITES:
            case POSIX_SEQ_READS:
            case POSIX_SEQ_WRITES:
            case POSIX_RW_SWITCHES:
            case POSIX_MEM_NOT_ALIGNED:
            case POSIX_FILE_NOT_ALIGNED:
            case POSIX_SIZE_READ_0_100:
            case POSIX_SIZE_READ_100_1K:
            case POSIX_SIZE_READ_1K_10K:
            case POSIX_SIZE_READ_10K_100K:
            case POSIX_SIZE_READ_100K_1M:
            case POSIX_SIZE_READ_1M_4M:
            case POSIX_SIZE_READ_4M_10M:
            case POSIX_SIZE_READ_10M_100M:
            case POSIX_SIZE_READ_100M_1G:
            case POSIX_SIZE_READ_1G_PLUS:
            case POSIX_SIZE_WRITE_0_100:
            case POSIX_SIZE_WRITE_100_1K:
            case POSIX_SIZE_WRITE_1K_10K:
            case POSIX_SIZE_WRITE_10K_100K:
            case POSIX_SIZE_WRITE_100K_1M:
            case POSIX_SIZE_WRITE_1M_4M:
            case POSIX_SIZE_WRITE_4M_10M:
            case POSIX_SIZE_WRITE_10M_100M:
            case POSIX_SIZE_WRITE_100M_1G:
            case POSIX_SIZE_WRITE_1G_PLUS:
                /* sum */
                agg_psx_rec->counters[i] += psx_rec->counters[i];
                break;
            case POSIX_MODE:
            case POSIX_MEM_ALIGNMENT:
            case POSIX_FILE_ALIGNMENT:
                /* just set to the input value */
                agg_psx_rec->counters[i] = psx_rec->counters[i];
                break;
            case POSIX_MAX_BYTE_READ:
            case POSIX_MAX_BYTE_WRITTEN:
                /* max */
                if(psx_rec->counters[i] > agg_psx_rec->counters[i])
                {
                    agg_psx_rec->counters[i] = psx_rec->counters[i];
                }
                break;
            case POSIX_MAX_READ_TIME_SIZE:
            case POSIX_MAX_WRITE_TIME_SIZE:
            case POSIX_FASTEST_RANK:
            case POSIX_FASTEST_RANK_BYTES:
            case POSIX_SLOWEST_RANK:
            case POSIX_SLOWEST_RANK_BYTES:
                /* these are set with the FP counters */
                break;
            case POSIX_STRIDE1_STRIDE:
            case POSIX_ACCESS1_ACCESS:
                /* increment common value counters */

                /* first, collapse duplicates */
                for(j = i; j < i + 4; j++)
                {
                    for(k = 0; k < 4; k++)
                    {
                        if(agg_psx_rec->counters[i + k] == psx_rec->counters[j])
                        {
                            agg_psx_rec->counters[i + k + 4] += psx_rec->counters[j + 4];
                            psx_rec->counters[j] = psx_rec->counters[j + 4] = 0;
                        }
                    }
                }

                /* second, add new counters */
                for(j = i; j < i + 4; j++)
                {
                    tmp_ndx = 0;
                    memset(tmp_val, 0, 4 * sizeof(int64_t));
                    memset(tmp_cnt, 0, 4 * sizeof(int64_t));

                    if(psx_rec->counters[j] == 0) break;
                    for(k = 0; k < 4; k++)
                    {
                        if(agg_psx_rec->counters[i + k] == psx_rec->counters[j])
                        {
                            total_count = agg_psx_rec->counters[i + k + 4] +
                                psx_rec->counters[j + 4];
                            break;
                        }
                    }
                    if(k == 4) total_count = psx_rec->counters[j + 4];

                    for(k = 0; k < 4; k++)
                    {
                        if((agg_psx_rec->counters[i + k + 4] > total_count) ||
                           ((agg_psx_rec->counters[i + k + 4] == total_count) &&
                            (agg_psx_rec->counters[i + k] > psx_rec->counters[j])))
                        {
                            tmp_val[tmp_ndx] = agg_psx_rec->counters[i + k];
                            tmp_cnt[tmp_ndx] = agg_psx_rec->counters[i + k + 4];
                            tmp_ndx++;
                        }
                        else break;
                    }
                    if(tmp_ndx == 4) break;

                    tmp_val[tmp_ndx] = psx_rec->counters[j];
                    tmp_cnt[tmp_ndx] = psx_rec->counters[j + 4];
                    tmp_ndx++;

                    while(tmp_ndx != 4)
                    {
                        if(agg_psx_rec->counters[i + k] != psx_rec->counters[j])
                        {
                            tmp_val[tmp_ndx] = agg_psx_rec->counters[i + k];
                            tmp_cnt[tmp_ndx] = agg_psx_rec->counters[i + k + 4];
                            tmp_ndx++;
                        }
                        k++;
                    }
                    memcpy(&(agg_psx_rec->counters[i]), tmp_val, 4 * sizeof(int64_t));
                    memcpy(&(agg_psx_rec->counters[i + 4]), tmp_cnt, 4 * sizeof(int64_t));
                }
                break;
            case POSIX_STRIDE2_STRIDE:
            case POSIX_STRIDE3_STRIDE:
            case POSIX_STRIDE4_STRIDE:
            case POSIX_STRIDE1_COUNT:
            case POSIX_STRIDE2_COUNT:
            case POSIX_STRIDE3_COUNT:
            case POSIX_STRIDE4_COUNT:
            case POSIX_ACCESS2_ACCESS:
            case POSIX_ACCESS3_ACCESS:
            case POSIX_ACCESS4_ACCESS:
            case POSIX_ACCESS1_COUNT:
            case POSIX_ACCESS2_COUNT:
            case POSIX_ACCESS3_COUNT:
            case POSIX_ACCESS4_COUNT:
                /* these are set all at once with common counters above */
                break;
            default:
                agg_psx_rec->counters[i] = -1;
                break;
        }
    }

    for(i = 0; i < POSIX_F_NUM_INDICES; i++)
    {
        switch(i)
        {
            case POSIX_F_READ_TIME:
            case POSIX_F_WRITE_TIME:
            case POSIX_F_META_TIME:
                /* sum */
                agg_psx_rec->fcounters[i] += psx_rec->fcounters[i];
                break;
            case POSIX_F_OPEN_TIMESTAMP:
            case POSIX_F_READ_START_TIMESTAMP:
            case POSIX_F_WRITE_START_TIMESTAMP:
                /* minimum non-zero */
                if((psx_rec->fcounters[i] > 0)  &&
                    ((agg_psx_rec->fcounters[i] == 0) ||
                    (psx_rec->fcounters[i] < agg_psx_rec->fcounters[i])))
                {
                    agg_psx_rec->fcounters[i] = psx_rec->fcounters[i];
                }
                break;
            case POSIX_F_READ_END_TIMESTAMP:
            case POSIX_F_WRITE_END_TIMESTAMP:
            case POSIX_F_CLOSE_TIMESTAMP:
                /* maximum */
                if(psx_rec->fcounters[i] > agg_psx_rec->fcounters[i])
                {
                    agg_psx_rec->fcounters[i] = psx_rec->fcounters[i];
                }
                break;
            case POSIX_F_MAX_READ_TIME:
                if(psx_rec->fcounters[i] > agg_psx_rec->fcounters[i])
                {
                    agg_psx_rec->fcounters[i] = psx_rec->fcounters[i];
                    agg_psx_rec->counters[POSIX_MAX_READ_TIME_SIZE] =
                        psx_rec->counters[POSIX_MAX_READ_TIME_SIZE];
                }
                break;
            case POSIX_F_MAX_WRITE_TIME:
                if(psx_rec->fcounters[i] > agg_psx_rec->fcounters[i])
                {
                    agg_psx_rec->fcounters[i] = psx_rec->fcounters[i];
                    agg_psx_rec->counters[POSIX_MAX_WRITE_TIME_SIZE] =
                        psx_rec->counters[POSIX_MAX_WRITE_TIME_SIZE];
                }
                break;
            case POSIX_F_FASTEST_RANK_TIME:
                if(init_flag)
                {
                    /* set fastest rank counters according to root rank. these counters
                     * will be determined as the aggregation progresses.
                     */
                    agg_psx_rec->counters[POSIX_FASTEST_RANK] = psx_rec->base_rec.rank;
                    agg_psx_rec->counters[POSIX_FASTEST_RANK_BYTES] = psx_bytes;
                    agg_psx_rec->fcounters[POSIX_F_FASTEST_RANK_TIME] = psx_time;
                }

                if(psx_time < agg_psx_rec->fcounters[POSIX_F_FASTEST_RANK_TIME])
                {
                    agg_psx_rec->counters[POSIX_FASTEST_RANK] = psx_rec->base_rec.rank;
                    agg_psx_rec->counters[POSIX_FASTEST_RANK_BYTES] = psx_bytes;
                    agg_psx_rec->fcounters[POSIX_F_FASTEST_RANK_TIME] = psx_time;
                }
                break;
            case POSIX_F_SLOWEST_RANK_TIME:
                if(init_flag)
                {
                    /* set slowest rank counters according to root rank. these counters
                     * will be determined as the aggregation progresses.
                     */
                    agg_psx_rec->counters[POSIX_SLOWEST_RANK] = psx_rec->base_rec.rank;
                    agg_psx_rec->counters[POSIX_SLOWEST_RANK_BYTES] = psx_bytes;
                    agg_psx_rec->fcounters[POSIX_F_SLOWEST_RANK_TIME] = psx_time;
                }

                if(psx_time > agg_psx_rec->fcounters[POSIX_F_SLOWEST_RANK_TIME])
                {
                    agg_psx_rec->counters[POSIX_SLOWEST_RANK] = psx_rec->base_rec.rank;
                    agg_psx_rec->counters[POSIX_SLOWEST_RANK_BYTES] = psx_bytes;
                    agg_psx_rec->fcounters[POSIX_F_SLOWEST_RANK_TIME] = psx_time;
                }
                break;
            case POSIX_F_VARIANCE_RANK_TIME:
                if(init_flag)
                {
                    var_time_p->n = 1;
                    var_time_p->M = psx_time;
                    var_time_p->S = 0;
                }
                else
                {
                    old_M = var_time_p->M;

                    var_time_p->n++;
                    var_time_p->M += (psx_time - var_time_p->M) / var_time_p->n;
                    var_time_p->S += (psx_time - var_time_p->M) * (psx_time - old_M);

                    agg_psx_rec->fcounters[POSIX_F_VARIANCE_RANK_TIME] =
                        var_time_p->S / var_time_p->n;
                }
                break;
            case POSIX_F_VARIANCE_RANK_BYTES:
                if(init_flag)
                {
                    var_bytes_p->n = 1;
                    var_bytes_p->M = psx_bytes;
                    var_bytes_p->S = 0;
                }
                else
                {
                    old_M = var_bytes_p->M;

                    var_bytes_p->n++;
                    var_bytes_p->M += (psx_bytes - var_bytes_p->M) / var_bytes_p->n;
                    var_bytes_p->S += (psx_bytes - var_bytes_p->M) * (psx_bytes - old_M);

                    agg_psx_rec->fcounters[POSIX_F_VARIANCE_RANK_BYTES] =
                        var_bytes_p->S / var_bytes_p->n;
                }
                break;
            default:
                agg_psx_rec->fcounters[i] = -1;
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
