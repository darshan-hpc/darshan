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
#define DARSHAN_POSIX_FILE_SIZE_2 648

static int darshan_log_get_posix_file(darshan_fd fd, void** posix_buf_p);
static int darshan_log_put_posix_file(darshan_fd fd, void* posix_buf);
static void darshan_log_print_posix_file(void *file_rec,
    char *file_name, char *mnt_pt, char *fs_type);
static void darshan_log_print_posix_description(int ver);
static void darshan_log_print_posix_file_diff(void *file_rec1, char *file_name1,
    void *file_rec2, char *file_name2);
static void darshan_log_agg_posix_files(void *rec, void *agg_rec, int init_flag);
static void darshan_log_posix_accum_file(void *pfile, hash_entry_t *hfile, int64_t nprocs);
static void darshan_log_posix_accum_perf(void *pfile, perf_data_t *pdata);
static void darshan_log_posix_calc_file(hash_entry_t *file_hash, file_data_t *fdata);
static void darshan_log_posix_print_total_file(void *pfile, int posix_ver);
static void darshan_log_posix_file_list(hash_entry_t *file_hash, struct darshan_name_record_ref *name_hash, int detail_flag);

struct darshan_mod_logutil_funcs posix_logutils =
{
    .log_get_record = &darshan_log_get_posix_file,
    .log_put_record = &darshan_log_put_posix_file,
    .log_print_record = &darshan_log_print_posix_file,
    .log_print_description = &darshan_log_print_posix_description,
    .log_print_diff = &darshan_log_print_posix_file_diff,
    .log_agg_records = &darshan_log_agg_posix_files,
    .log_accum_file = &darshan_log_posix_accum_file,
    .log_accum_perf = &darshan_log_posix_accum_perf,
    .log_calc_file = &darshan_log_posix_calc_file,
    .log_print_total_file = &darshan_log_posix_print_total_file,
    .log_file_list = &darshan_log_posix_file_list,
    .log_calc_perf = &darshan_calc_perf
};

static int darshan_log_get_posix_file(darshan_fd fd, void** posix_buf_p)
{
    struct darshan_posix_file *file = *((struct darshan_posix_file **)posix_buf_p);
    int rec_len;
    int i;
    int ret = -1;

    if(fd->mod_map[DARSHAN_POSIX_MOD].len == 0)
        return(0);

    if(*posix_buf_p == NULL)
    {
        file = malloc(sizeof(*file));
        if(!file)
            return(-1);
    }

    if(fd->mod_ver[DARSHAN_POSIX_MOD] == DARSHAN_POSIX_VER) 
    {
        /* log format is in current version, so we don't need to do any
         * translation of counters while reading
         */
        rec_len = sizeof(struct darshan_posix_file);
        ret = darshan_log_get_mod(fd, DARSHAN_POSIX_MOD, file, rec_len);
    }
    else
    {
        char scratch[1024] = {0};
        char *src_p, *dest_p;
        int len;

        if(fd->mod_ver[DARSHAN_POSIX_MOD] == 1)
        {
            rec_len = DARSHAN_POSIX_FILE_SIZE_1;
            ret = darshan_log_get_mod(fd, DARSHAN_POSIX_MOD, scratch, rec_len);
            if(ret != rec_len)
                goto exit;

            /* upconvert version 1 to version 2 in-place */
            dest_p = scratch + (sizeof(struct darshan_base_record) + 
                (6 * sizeof(int64_t)));
            src_p = dest_p + (4 * sizeof(int64_t));
            len = rec_len - (src_p - scratch);
            memmove(dest_p, src_p, len);
        }
        if(fd->mod_ver[DARSHAN_POSIX_MOD] <= 2)
        {
            if(fd->mod_ver[DARSHAN_POSIX_MOD] == 2)
            {
                rec_len = DARSHAN_POSIX_FILE_SIZE_2;
                ret = darshan_log_get_mod(fd, DARSHAN_POSIX_MOD, scratch, rec_len);
                if(ret != rec_len)
                    goto exit;
            }

            /* upconvert version 2 to version 3 in-place */
            dest_p = scratch + (sizeof(struct darshan_base_record) +
                (64 * sizeof(int64_t)) + (5 * sizeof(double)));
            src_p = dest_p - (2 * sizeof(double));
            len = (12 * sizeof(double));
            memmove(dest_p, src_p, len);
            /* set F_CLOSE_START and F_OPEN_END to -1 */
            *((double *)src_p) = -1;
            *((double *)(src_p + sizeof(double))) = -1;
        }

        memcpy(file, scratch, sizeof(struct darshan_posix_file));
    }

exit:
    if(*posix_buf_p == NULL)
    {
        if(ret == rec_len)
            *posix_buf_p = file;
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
            for(i=0; i<POSIX_NUM_INDICES; i++)
                DARSHAN_BSWAP64(&file->counters[i]);
            for(i=0; i<POSIX_F_NUM_INDICES; i++)
            {
                /* skip counters we explicitly set to -1 since they don't
                 * need to be byte swapped
                 */
                if((fd->mod_ver[DARSHAN_POSIX_MOD] < 3) &&
                    ((i == POSIX_F_CLOSE_START_TIMESTAMP) ||
                     (i == POSIX_F_OPEN_END_TIMESTAMP)))
                    continue;
                DARSHAN_BSWAP64(&file->fcounters[i]);
            }
        }

        return(1);
    }
}

static int darshan_log_put_posix_file(darshan_fd fd, void* posix_buf)
{
    struct darshan_posix_file *file = (struct darshan_posix_file *)posix_buf;
    int ret;

    ret = darshan_log_put_mod(fd, DARSHAN_POSIX_MOD, file,
        sizeof(struct darshan_posix_file), DARSHAN_POSIX_VER);
    if(ret < 0)
        return(-1);

    return(0);
}

static void darshan_log_print_posix_file(void *file_rec, char *file_name,
    char *mnt_pt, char *fs_type)
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

static void darshan_log_print_posix_description(int ver)
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
    printf("#   POSIX_F_*_START_TIMESTAMP: timestamp of first open/read/write/close.\n");
    printf("#   POSIX_F_*_END_TIMESTAMP: timestamp of last open/read/write/close.\n");
    printf("#   POSIX_F_READ/WRITE/META_TIME: cumulative time spent in read, write, or metadata operations.\n");
    printf("#   POSIX_F_MAX_*_TIME: duration of the slowest read and write operations.\n");
    printf("#   POSIX_F_*_RANK_TIME: fastest and slowest I/O time for a single rank (for shared files).\n");
    printf("#   POSIX_F_VARIANCE_RANK_*: variance of total I/O time and bytes moved for all ranks (for shared files).\n");

    if(ver <= 1)
    {
        printf("\n# WARNING: POSIX module log format version 1 has the following limitations:\n");
        printf("# - Partial instrumentation of stdio stream I/O functions not parsable by Darshan versions >= 3.1.0\n");
        printf("#     * Using darshan-logutils versions < 3.1.0, this data can be found in the following POSIX counters:\n");
        printf("#         * POSIX_FOPENS, POSIX_FREADS, POSIX_FWRITES, POSIX_FSEEKS\n");
    }
    if(ver <= 2)
    {
        printf("\n# WARNING: POSIX module log format version <=2 does not support the following counters:\n");
        printf("# - POSIX_F_CLOSE_START_TIMESTAMP\n");
        printf("# - POSIX_F_OPEN_END_TIMESTAMP\n");
    }

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
            case POSIX_F_OPEN_START_TIMESTAMP:
            case POSIX_F_READ_START_TIMESTAMP:
            case POSIX_F_WRITE_START_TIMESTAMP:
            case POSIX_F_CLOSE_START_TIMESTAMP:
                /* minimum non-zero */
                if((psx_rec->fcounters[i] > 0)  &&
                    ((agg_psx_rec->fcounters[i] == 0) ||
                    (psx_rec->fcounters[i] < agg_psx_rec->fcounters[i])))
                {
                    agg_psx_rec->fcounters[i] = psx_rec->fcounters[i];
                }
                break;
            case POSIX_F_OPEN_END_TIMESTAMP:
            case POSIX_F_READ_END_TIMESTAMP:
            case POSIX_F_WRITE_END_TIMESTAMP:
            case POSIX_F_CLOSE_END_TIMESTAMP:
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

static void darshan_log_posix_accum_file(
    void *infile,
    hash_entry_t *hfile,
    int64_t nprocs)
{
    struct darshan_posix_file *pfile = infile;
    int i, j;
    int set;
    int min_ndx;
    int64_t min;
    struct darshan_posix_file* tmp;

    hfile->procs += 1;

    if(pfile->base_rec.rank == -1)
    {
        hfile->slowest_time = pfile->fcounters[POSIX_F_SLOWEST_RANK_TIME];
    }
    else
    {
        hfile->slowest_time = max(hfile->slowest_time,
            (pfile->fcounters[POSIX_F_META_TIME] +
            pfile->fcounters[POSIX_F_READ_TIME] +
            pfile->fcounters[POSIX_F_WRITE_TIME]));
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

    hfile->cumul_time += pfile->fcounters[POSIX_F_META_TIME] +
                         pfile->fcounters[POSIX_F_READ_TIME] +
                         pfile->fcounters[POSIX_F_WRITE_TIME];

    if(hfile->rec_dat == NULL)
    {
        hfile->rec_dat = malloc(sizeof(struct darshan_posix_file));
        assert(hfile->rec_dat);
        memset(hfile->rec_dat, 0, sizeof(struct darshan_posix_file));
    }
    tmp = (struct darshan_posix_file*)hfile->rec_dat;

    for(i = 0; i < POSIX_NUM_INDICES; i++)
    {
        switch(i)
        {
        case POSIX_MODE:
        case POSIX_MEM_ALIGNMENT:
        case POSIX_FILE_ALIGNMENT:
            tmp->counters[i] = pfile->counters[i];
            break;
        case POSIX_MAX_BYTE_READ:
        case POSIX_MAX_BYTE_WRITTEN:
            if (tmp->counters[i] < pfile->counters[i])
            {
                tmp->counters[i] = pfile->counters[i];
            }
            break;
        case POSIX_STRIDE1_STRIDE:
        case POSIX_STRIDE2_STRIDE:
        case POSIX_STRIDE3_STRIDE:
        case POSIX_STRIDE4_STRIDE:
        case POSIX_ACCESS1_ACCESS:
        case POSIX_ACCESS2_ACCESS:
        case POSIX_ACCESS3_ACCESS:
        case POSIX_ACCESS4_ACCESS:
           /*
            * do nothing here because these will be stored
            * when the _COUNT is accessed.
            */
           break;
        case POSIX_STRIDE1_COUNT:
        case POSIX_STRIDE2_COUNT:
        case POSIX_STRIDE3_COUNT:
        case POSIX_STRIDE4_COUNT:
            set = 0;
            min_ndx = POSIX_STRIDE1_COUNT;
            min = tmp->counters[min_ndx];
            for(j = POSIX_STRIDE1_COUNT; j <= POSIX_STRIDE4_COUNT; j++)
            {
                if(tmp->counters[j-4] == pfile->counters[i-4])
                {
                    tmp->counters[j] += pfile->counters[i];
                    set = 1;
                    break;
                }
                if(tmp->counters[j] < min)
                {
                    min_ndx = j;
                    min = tmp->counters[j];
                }
            }
            if(!set && (pfile->counters[i] > min))
            {
                tmp->counters[min_ndx] = pfile->counters[i];
                tmp->counters[min_ndx-4] = pfile->counters[i-4];
            }
            break;
        case POSIX_ACCESS1_COUNT:
        case POSIX_ACCESS2_COUNT:
        case POSIX_ACCESS3_COUNT:
        case POSIX_ACCESS4_COUNT:
            set = 0;
            min_ndx = POSIX_ACCESS1_COUNT;
            min = tmp->counters[min_ndx];
            for(j = POSIX_ACCESS1_COUNT; j <= POSIX_ACCESS4_COUNT; j++)
            {
                if(tmp->counters[j-4] == pfile->counters[i-4])
                {
                    tmp->counters[j] += pfile->counters[i];
                    set = 1;
                    break;
                }
                if(tmp->counters[j] < min)
                {
                    min_ndx = j;
                    min = tmp->counters[j];
                }
            }
            if(!set && (pfile->counters[i] > min))
            {
                tmp->counters[i] = pfile->counters[i];
                tmp->counters[i-4] = pfile->counters[i-4];
            }
            break;
        case POSIX_FASTEST_RANK:
        case POSIX_SLOWEST_RANK:
        case POSIX_FASTEST_RANK_BYTES:
        case POSIX_SLOWEST_RANK_BYTES:
            tmp->counters[i] = 0;
            break;
        case POSIX_MAX_READ_TIME_SIZE:
        case POSIX_MAX_WRITE_TIME_SIZE:
            break;
        default:
            tmp->counters[i] += pfile->counters[i];
            break;
        }
    }

    for(i = 0; i < POSIX_F_NUM_INDICES; i++)
    {
        switch(i)
        {
            case POSIX_F_OPEN_START_TIMESTAMP:
            case POSIX_F_READ_START_TIMESTAMP:
            case POSIX_F_WRITE_START_TIMESTAMP:
            case POSIX_F_CLOSE_START_TIMESTAMP:
                if(tmp->fcounters[i] == 0 ||
                    tmp->fcounters[i] > pfile->fcounters[i])
                {
                    tmp->fcounters[i] = pfile->fcounters[i];
                }
                break;
            case POSIX_F_OPEN_END_TIMESTAMP:
            case POSIX_F_READ_END_TIMESTAMP:
            case POSIX_F_WRITE_END_TIMESTAMP:
            case POSIX_F_CLOSE_END_TIMESTAMP:
                if(tmp->fcounters[i] == 0 ||
                    tmp->fcounters[i] < pfile->fcounters[i])
                {
                    tmp->fcounters[i] = pfile->fcounters[i];
                }
                break;
            case POSIX_F_FASTEST_RANK_TIME:
            case POSIX_F_SLOWEST_RANK_TIME:
            case POSIX_F_VARIANCE_RANK_TIME:
            case POSIX_F_VARIANCE_RANK_BYTES:
                tmp->fcounters[i] = 0;
                break;
            case POSIX_F_MAX_READ_TIME:
                if(tmp->fcounters[i] < pfile->fcounters[i])
                {
                    tmp->fcounters[i] = pfile->fcounters[i];
                    tmp->counters[POSIX_MAX_READ_TIME_SIZE] =
                        pfile->counters[POSIX_MAX_READ_TIME_SIZE];
                }
                break;
            case POSIX_F_MAX_WRITE_TIME:
                if(tmp->fcounters[i] < pfile->fcounters[i])
                {
                    tmp->fcounters[i] = pfile->fcounters[i];
                    tmp->counters[POSIX_MAX_WRITE_TIME_SIZE] =
                        pfile->counters[POSIX_MAX_WRITE_TIME_SIZE];
                }
                break;
            default:
                tmp->fcounters[i] += pfile->fcounters[i];
                break;
        }
    }

    return;
}

static void darshan_log_posix_accum_perf(
    void *infile,
    perf_data_t *pdata)
{
    struct darshan_posix_file *pfile = infile;
    pdata->total_bytes += pfile->counters[POSIX_BYTES_READ] +
                          pfile->counters[POSIX_BYTES_WRITTEN];

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
        if(pfile->fcounters[POSIX_F_CLOSE_END_TIMESTAMP] >
            pfile->fcounters[POSIX_F_OPEN_START_TIMESTAMP])
        {
            pdata->shared_time_by_open +=
                pfile->fcounters[POSIX_F_CLOSE_END_TIMESTAMP] -
                pfile->fcounters[POSIX_F_OPEN_START_TIMESTAMP];
        }

        /* by_open_lastio */
        if(pfile->fcounters[POSIX_F_READ_END_TIMESTAMP] >
            pfile->fcounters[POSIX_F_WRITE_END_TIMESTAMP])
        {
            /* be careful: file may have been opened but not read or written */
            if(pfile->fcounters[POSIX_F_READ_END_TIMESTAMP] > pfile->fcounters[POSIX_F_OPEN_START_TIMESTAMP])
            {
                pdata->shared_time_by_open_lastio +=
                    pfile->fcounters[POSIX_F_READ_END_TIMESTAMP] -
                    pfile->fcounters[POSIX_F_OPEN_START_TIMESTAMP];
            }
        }
        else
        {
            /* be careful: file may have been opened but not read or written */
            if(pfile->fcounters[POSIX_F_WRITE_END_TIMESTAMP] > pfile->fcounters[POSIX_F_OPEN_START_TIMESTAMP])
            {
                pdata->shared_time_by_open_lastio +=
                    pfile->fcounters[POSIX_F_WRITE_END_TIMESTAMP] -
                    pfile->fcounters[POSIX_F_OPEN_START_TIMESTAMP];
            }
        }

        pdata->shared_time_by_cumul +=
            pfile->fcounters[POSIX_F_META_TIME] +
            pfile->fcounters[POSIX_F_READ_TIME] +
            pfile->fcounters[POSIX_F_WRITE_TIME];
        pdata->shared_meta_time += pfile->fcounters[POSIX_F_META_TIME];

        /* by_slowest */
        pdata->shared_time_by_slowest +=
            pfile->fcounters[POSIX_F_SLOWEST_RANK_TIME];
    }

    /*
     * Calculation of Unique File Time
     *   record the data for each file and sum it 
     */
    else
    {
        pdata->rank_cumul_io_time[pfile->base_rec.rank] +=
            (pfile->fcounters[POSIX_F_META_TIME] +
            pfile->fcounters[POSIX_F_READ_TIME] +
            pfile->fcounters[POSIX_F_WRITE_TIME]);
        pdata->rank_cumul_md_time[pfile->base_rec.rank] +=
            pfile->fcounters[POSIX_F_META_TIME];
    }

    return;
}

static void darshan_log_posix_calc_file(
    hash_entry_t *file_hash,
    file_data_t *fdata)
{
    hash_entry_t *curr = NULL;
    hash_entry_t *tmp = NULL;
    struct darshan_posix_file *file_rec;

    memset(fdata, 0, sizeof(*fdata));
    HASH_ITER(hlink, file_hash, curr, tmp)
    {
        int64_t bytes;
        int64_t r;
        int64_t w;

        file_rec = (struct darshan_posix_file*)curr->rec_dat;
        assert(file_rec);

        bytes = file_rec->counters[POSIX_BYTES_READ] +
                file_rec->counters[POSIX_BYTES_WRITTEN];

        r = file_rec->counters[POSIX_READS];

        w = file_rec->counters[POSIX_WRITES];

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

static void darshan_log_posix_print_total_file(
    void *infile,
    int posix_ver)
{
    struct darshan_posix_file *pfile = infile;
    int i;

    mod_logutils[DARSHAN_POSIX_MOD]->log_print_description(posix_ver);
    printf("\n");
    for(i = 0; i < POSIX_NUM_INDICES; i++)
    {
        printf("total_%s: %"PRId64"\n",
            posix_counter_names[i], pfile->counters[i]);
    }
    for(i = 0; i < POSIX_F_NUM_INDICES; i++)
    {
        printf("total_%s: %lf\n",
            posix_f_counter_names[i], pfile->fcounters[i]);
    }
    return;
}

static void darshan_log_posix_file_list(
    hash_entry_t *file_hash,
    struct darshan_name_record_ref *name_hash,
    int detail_flag)
{
    hash_entry_t *curr = NULL;
    hash_entry_t *tmp = NULL;
    struct darshan_posix_file *file_rec = NULL;
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
     * - POSIX opens
     * - r histogram
     * - w histogram
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
        printf("# <start_{open/read/write/close}>: start timestamp of first open, read, write, or close\n");
        printf("# <end_{open/read/write/close}>: end timestamp of last open, read, write, or close\n");
        printf("# <posix_opens>: POSIX open calls\n");
        printf("# <POSIX_SIZE_READ_*>: POSIX read size histogram\n");
        printf("# <POSIX_SIZE_WRITE_*>: POSIX write size histogram\n");
    }

    printf("\n# <record_id>\t<file_name>\t<nprocs>\t<slowest>\t<avg>");
    if(detail_flag)
    {
        printf("\t<start_open>\t<start_read>\t<start_write>\t<start_close>");
        printf("\t<end_open>\t<end_read>\t<end_write>\t<end_close>\t<posix_opens>");
        for(i=POSIX_SIZE_READ_0_100; i<= POSIX_SIZE_WRITE_1G_PLUS; i++)
            printf("\t<%s>", posix_counter_names[i]);
    }
    printf("\n");
    HASH_ITER(hlink, file_hash, curr, tmp)
    {
        file_rec = (struct darshan_posix_file*)curr->rec_dat;
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
            for(i=POSIX_F_OPEN_START_TIMESTAMP; i<=POSIX_F_CLOSE_END_TIMESTAMP; i++)
            {
                printf("\t%f", file_rec->fcounters[i]);
            }
            printf("\t%" PRId64, file_rec->counters[POSIX_OPENS]);
            for(i=POSIX_SIZE_READ_0_100; i<= POSIX_SIZE_WRITE_1G_PLUS; i++)
                printf("\t%" PRId64, file_rec->counters[i]);
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
