/*
 * Copyright (C) 2020 University of Chicago.
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
#include <errno.h>

#include "darshan-logutils.h"

#ifdef HAVE_LIBUUID
#include <uuid/uuid.h>
#endif

/* counter name strings for the DFS module */
#define X(a) #a,
char *dfs_counter_names[] = {
    DFS_COUNTERS
};

char *dfs_f_counter_names[] = {
    DFS_F_COUNTERS
};
#undef X

static int darshan_log_get_dfs_file(darshan_fd fd, void** dfs_buf_p);
static int darshan_log_put_dfs_file(darshan_fd fd, void* dfs_buf);
static void darshan_log_print_dfs_file(void *file_rec,
    char *file_name, char *mnt_pt, char *fs_type);
static void darshan_log_print_dfs_description(int ver);
static void darshan_log_print_dfs_file_diff(void *file_rec1, char *file_name1,
    void *file_rec2, char *file_name2);
static void darshan_log_agg_dfs_files(void *rec, void *agg_rec, int init_flag);
static int darshan_log_sizeof_dfs_file(void* dfs_buf_p);
static int darshan_log_record_metrics_dfs_file(void* dfs_buf_p,
                                                 uint64_t* rec_id,
                                                 int64_t* r_bytes,
                                                 int64_t* w_bytes,
                                                 int64_t* max_offset,
                                                 double* io_total_time,
                                                 double* md_only_time,
                                                 double* rw_only_time,
                                                 int64_t* rank,
                                                 int64_t* nprocs);

struct darshan_mod_logutil_funcs dfs_logutils =
{
    .log_get_record = &darshan_log_get_dfs_file,
    .log_put_record = &darshan_log_put_dfs_file,
    .log_print_record = &darshan_log_print_dfs_file,
    .log_print_description = &darshan_log_print_dfs_description,
    .log_print_diff = &darshan_log_print_dfs_file_diff,
    .log_agg_records = &darshan_log_agg_dfs_files,
    .log_sizeof_record = &darshan_log_sizeof_dfs_file,
    .log_record_metrics = &darshan_log_record_metrics_dfs_file
};

static int darshan_log_sizeof_dfs_file(void* dfs_buf_p)
{
    /* dfs records have a fixed size */
    return(sizeof(struct darshan_dfs_file));
}

static int darshan_log_record_metrics_dfs_file(void* dfs_buf_p,
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
    struct darshan_dfs_file *dfs_rec = (struct darshan_dfs_file *)dfs_buf_p;

    *rec_id = dfs_rec->base_rec.id;
    *r_bytes = dfs_rec->counters[DFS_BYTES_READ];
    *w_bytes = dfs_rec->counters[DFS_BYTES_WRITTEN];

    /* the dfs module doesn't report this */
    *max_offset = -1;

    *rank = dfs_rec->base_rec.rank;
    /* nprocs is 1 per record, unless rank is negative, in which case we
     * report -1 as the rank value to represent "all"
     */
    if(dfs_rec->base_rec.rank < 0)
        *nprocs = -1;
    else
        *nprocs = 1;

    if(dfs_rec->base_rec.rank < 0) {
        /* shared file records populate a counter with the slowest rank time
         * (derived during reduction).  They do not have a breakdown of meta
         * and rw time, though.
         */
        *io_total_time = dfs_rec->fcounters[DFS_F_SLOWEST_RANK_TIME];
        *md_only_time = 0;
        *rw_only_time = 0;
    }
    else {
        /* non-shared records have separate meta, read, and write values
         * that we can combine as needed
         */
        *io_total_time = dfs_rec->fcounters[DFS_F_META_TIME] +
                         dfs_rec->fcounters[DFS_F_READ_TIME] +
                         dfs_rec->fcounters[DFS_F_WRITE_TIME];
        *md_only_time = dfs_rec->fcounters[DFS_F_META_TIME];
        *rw_only_time = dfs_rec->fcounters[DFS_F_READ_TIME] +
                        dfs_rec->fcounters[DFS_F_WRITE_TIME];
    }

    return(0);
}

static int darshan_log_get_dfs_file(darshan_fd fd, void** dfs_buf_p)
{
    struct darshan_dfs_file *file = *((struct darshan_dfs_file **)dfs_buf_p);
    int rec_len;
    int i;
    int ret = -1;

    if(fd->mod_map[DARSHAN_DFS_MOD].len == 0)
        return(0);

    if(fd->mod_ver[DARSHAN_DFS_MOD] == 0 ||
        fd->mod_ver[DARSHAN_DFS_MOD] > DARSHAN_DFS_VER)
    {
        fprintf(stderr, "Error: Invalid DFS module version number (got %d)\n",
            fd->mod_ver[DARSHAN_DFS_MOD]);
        return(-1);
    }

    if(*dfs_buf_p == NULL)
    {
        file = malloc(sizeof(*file));
        if(!file)
            return(-1);
    }

    if(fd->mod_ver[DARSHAN_DFS_MOD] == DARSHAN_DFS_VER)
    {
        /* log format is in current version, so we don't need to do any
         * translation of counters while reading
         */
        rec_len = sizeof(struct darshan_dfs_file);
        ret = darshan_log_get_mod(fd, DARSHAN_DFS_MOD, file, rec_len);
    }
    else
    {
        assert(0);
    }

    if(*dfs_buf_p == NULL)
    {
        if(ret == rec_len)
            *dfs_buf_p = file;
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
            for(i=0; i<DFS_NUM_INDICES; i++)
                DARSHAN_BSWAP64(&file->counters[i]);
            for(i=0; i<DFS_F_NUM_INDICES; i++)
                DARSHAN_BSWAP64(&file->fcounters[i]);
            DARSHAN_BSWAP128(&file->pool_uuid);
            DARSHAN_BSWAP128(&file->cont_uuid);
        }

        return(1);
    }
}

static int darshan_log_put_dfs_file(darshan_fd fd, void* dfs_buf)
{
    struct darshan_dfs_file *file = (struct darshan_dfs_file *)dfs_buf;
    int ret;

    ret = darshan_log_put_mod(fd, DARSHAN_DFS_MOD, file,
        sizeof(struct darshan_dfs_file), DARSHAN_DFS_VER);
    if(ret < 0)
        return(-1);

    return(0);
}

static void darshan_log_print_dfs_file(void *file_rec, char *file_name,
    char *mnt_pt, char *fs_type)
{
    int i;
    struct darshan_dfs_file *dfs_file_rec =
        (struct darshan_dfs_file *)file_rec;
    char pool_cont_uuid_str[128] = {0};

#ifdef HAVE_LIBUUID
    if(!uuid_is_null(dfs_file_rec->pool_uuid) && !uuid_is_null(dfs_file_rec->cont_uuid))
    {
        uuid_unparse(dfs_file_rec->pool_uuid, pool_cont_uuid_str);
        strcat(pool_cont_uuid_str, ":");
        uuid_unparse(dfs_file_rec->cont_uuid, pool_cont_uuid_str+strlen(pool_cont_uuid_str));
    }
    else
        strcat(pool_cont_uuid_str, "UNKNOWN");
#else
    strcat(pool_cont_uuid_str, "N/A");
#endif

    mnt_pt = pool_cont_uuid_str;
    fs_type = "N/A";

    for(i=0; i<DFS_NUM_INDICES; i++)
    {
        DARSHAN_D_COUNTER_PRINT(darshan_module_names[DARSHAN_DFS_MOD],
            dfs_file_rec->base_rec.rank, dfs_file_rec->base_rec.id,
            dfs_counter_names[i], dfs_file_rec->counters[i],
            file_name, mnt_pt, fs_type);
    }

    for(i=0; i<DFS_F_NUM_INDICES; i++)
    {
        DARSHAN_F_COUNTER_PRINT(darshan_module_names[DARSHAN_DFS_MOD],
            dfs_file_rec->base_rec.rank, dfs_file_rec->base_rec.id,
            dfs_f_counter_names[i], dfs_file_rec->fcounters[i],
            file_name, mnt_pt, fs_type);
    }

    return;
}

static void darshan_log_print_dfs_description(int ver)
{
    printf("\n# description of DFS counters:\n");
    printf("#   DFS_*: DFS operation counts.\n");
    printf("#   OPENS,GLOBAL_OPENS,LOOKUPS,DUPS,READS,READXS,WRITES,WRITEXS,GET_SIZES,PUNCHES,REMOVES,STATS are types of operations.\n");
    printf("#   DFS_BYTES_*: total bytes read and written.\n");
    printf("#   DFS_RW_SWITCHES: number of times access alternated between read and write.\n");
    printf("#   DFS_MAX_*_TIME_SIZE: size of the slowest read and write operations.\n");
    printf("#   DFS_SIZE_*_*: histogram of read and write access sizes.\n");
    printf("#   DFS_ACCESS*_ACCESS: the four most common access sizes.\n");
    printf("#   DFS_ACCESS*_COUNT: count of the four most common access sizes.\n");
    printf("#   DFS_CHUNK_SIZE: DFS file chunk size.\n");
    printf("#   DFS_*_RANK: rank of the processes that were the fastest and slowest at I/O (for shared files).\n");
    printf("#   DFS_*_RANK_BYTES: bytes transferred by the fastest and slowest ranks (for shared files).\n");
    printf("#   DFS_F_*_START_TIMESTAMP: timestamp of first open/read/write/close.\n");
    printf("#   DFS_F_*_END_TIMESTAMP: timestamp of last open/read/write/close.\n");
    printf("#   DFS_F_READ/WRITE/META_TIME: cumulative time spent in read, write, or metadata operations.\n");
    printf("#   DFS_F_MAX_*_TIME: duration of the slowest read and write operations.\n");
    printf("#   DFS_F_*_RANK_TIME: fastest and slowest I/O time for a single rank (for shared files).\n");

    return;
}

static void darshan_log_print_dfs_file_diff(void *file_rec1, char *file_name1,
    void *file_rec2, char *file_name2)
{
    struct darshan_dfs_file *file1 = (struct darshan_dfs_file *)file_rec1;
    struct darshan_dfs_file *file2 = (struct darshan_dfs_file *)file_rec2;
    int i;

    /* NOTE: we assume that both input records are the same module format version */

    for(i=0; i<DFS_NUM_INDICES; i++)
    {
        if(!file2)
        {
            printf("- ");
            DARSHAN_D_COUNTER_PRINT(darshan_module_names[DARSHAN_DFS_MOD],
                file1->base_rec.rank, file1->base_rec.id, dfs_counter_names[i],
                file1->counters[i], file_name1, "", "");

        }
        else if(!file1)
        {
            printf("+ ");
            DARSHAN_D_COUNTER_PRINT(darshan_module_names[DARSHAN_DFS_MOD],
                file2->base_rec.rank, file2->base_rec.id, dfs_counter_names[i],
                file2->counters[i], file_name2, "", "");
        }
        else if(file1->counters[i] != file2->counters[i])
        {
            printf("- ");
            DARSHAN_D_COUNTER_PRINT(darshan_module_names[DARSHAN_DFS_MOD],
                file1->base_rec.rank, file1->base_rec.id, dfs_counter_names[i],
                file1->counters[i], file_name1, "", "");
            printf("+ ");
            DARSHAN_D_COUNTER_PRINT(darshan_module_names[DARSHAN_DFS_MOD],
                file2->base_rec.rank, file2->base_rec.id, dfs_counter_names[i],
                file2->counters[i], file_name2, "", "");
        }
    }

    for(i=0; i<DFS_F_NUM_INDICES; i++)
    {
        if(!file2)
        {
            printf("- ");
            DARSHAN_F_COUNTER_PRINT(darshan_module_names[DARSHAN_DFS_MOD],
                file1->base_rec.rank, file1->base_rec.id, dfs_f_counter_names[i],
                file1->fcounters[i], file_name1, "", "");

        }
        else if(!file1)
        {
            printf("+ ");
            DARSHAN_F_COUNTER_PRINT(darshan_module_names[DARSHAN_DFS_MOD],
                file2->base_rec.rank, file2->base_rec.id, dfs_f_counter_names[i],
                file2->fcounters[i], file_name2, "", "");
        }
        else if(file1->fcounters[i] != file2->fcounters[i])
        {
            printf("- ");
            DARSHAN_F_COUNTER_PRINT(darshan_module_names[DARSHAN_DFS_MOD],
                file1->base_rec.rank, file1->base_rec.id, dfs_f_counter_names[i],
                file1->fcounters[i], file_name1, "", "");
            printf("+ ");
            DARSHAN_F_COUNTER_PRINT(darshan_module_names[DARSHAN_DFS_MOD],
                file2->base_rec.rank, file2->base_rec.id, dfs_f_counter_names[i],
                file2->fcounters[i], file_name2, "", "");
        }
    }

    return;
}

static void darshan_log_agg_dfs_files(void *rec, void *agg_rec, int init_flag)
{
    struct darshan_dfs_file *dfs_rec = (struct darshan_dfs_file *)rec;
    struct darshan_dfs_file *agg_dfs_rec = (struct darshan_dfs_file *)agg_rec;
    int i, j, k;
    int total_count;
    int64_t tmp_val[4];
    int64_t tmp_cnt[4];
    int duplicate_mask[4] = {0};
    int tmp_ndx;
    int64_t dfs_fastest_rank, dfs_slowest_rank,
        dfs_fastest_bytes, dfs_slowest_bytes;
    double dfs_fastest_time, dfs_slowest_time;
    int shared_file_flag = 0;

    /* For the incoming record, we need to determine what values to use for
     * subsequent comparision against the aggregate record's fastest and
     * slowest fields. This is is complicated by the fact that shared file
     * records already have derived values, while unique file records do
     * not.  Handle both cases here so that this function can be generic.
     */
    if(dfs_rec->base_rec.rank == -1)
    {
        /* shared files should have pre-calculated fastest and slowest
         * counters */
        dfs_fastest_rank = dfs_rec->counters[DFS_FASTEST_RANK];
        dfs_slowest_rank = dfs_rec->counters[DFS_SLOWEST_RANK];
        dfs_fastest_bytes = dfs_rec->counters[DFS_FASTEST_RANK_BYTES];
        dfs_slowest_bytes = dfs_rec->counters[DFS_SLOWEST_RANK_BYTES];
        dfs_fastest_time = dfs_rec->fcounters[DFS_F_FASTEST_RANK_TIME];
        dfs_slowest_time = dfs_rec->fcounters[DFS_F_SLOWEST_RANK_TIME];
    }
    else
    {
        /* for non-shared files, derive bytes and time using data from this
         * rank
         */
        dfs_fastest_rank = dfs_rec->base_rec.rank;
        dfs_slowest_rank = dfs_fastest_rank;
        dfs_fastest_bytes = dfs_rec->counters[DFS_BYTES_READ] +
            dfs_rec->counters[DFS_BYTES_WRITTEN];
        dfs_slowest_bytes = dfs_fastest_bytes;
        dfs_fastest_time = dfs_rec->fcounters[DFS_F_READ_TIME] +
            dfs_rec->fcounters[DFS_F_WRITE_TIME] +
            dfs_rec->fcounters[DFS_F_META_TIME];
        dfs_slowest_time = dfs_fastest_time;
    }

    /* if this is our first record, store base id and rank */
    if(init_flag)
    {
        agg_dfs_rec->base_rec.rank = dfs_rec->base_rec.rank;
        agg_dfs_rec->base_rec.id = dfs_rec->base_rec.id;
    }

    /* so far do all of the records reference the same file? */
    if(agg_dfs_rec->base_rec.id == dfs_rec->base_rec.id)
        shared_file_flag = 1;
    else
        agg_dfs_rec->base_rec.id = 0;

    /* so far do all of the records reference the same rank? */
    if(agg_dfs_rec->base_rec.rank != dfs_rec->base_rec.rank)
        agg_dfs_rec->base_rec.rank = -1;

    for(i = 0; i < DFS_NUM_INDICES; i++)
    {
        switch(i)
        {
            case DFS_OPENS:
            case DFS_GLOBAL_OPENS:
            case DFS_LOOKUPS:
            case DFS_DUPS:
            case DFS_READS:
            case DFS_READXS:
            case DFS_WRITES:
            case DFS_WRITEXS:
            case DFS_NB_READS:
            case DFS_NB_WRITES:
            case DFS_GET_SIZES:
            case DFS_PUNCHES:
            case DFS_REMOVES:
            case DFS_STATS:
            case DFS_BYTES_READ:
            case DFS_BYTES_WRITTEN:
            case DFS_RW_SWITCHES:
            case DFS_SIZE_READ_0_100:
            case DFS_SIZE_READ_100_1K:
            case DFS_SIZE_READ_1K_10K:
            case DFS_SIZE_READ_10K_100K:
            case DFS_SIZE_READ_100K_1M:
            case DFS_SIZE_READ_1M_4M:
            case DFS_SIZE_READ_4M_10M:
            case DFS_SIZE_READ_10M_100M:
            case DFS_SIZE_READ_100M_1G:
            case DFS_SIZE_READ_1G_PLUS:
            case DFS_SIZE_WRITE_0_100:
            case DFS_SIZE_WRITE_100_1K:
            case DFS_SIZE_WRITE_1K_10K:
            case DFS_SIZE_WRITE_10K_100K:
            case DFS_SIZE_WRITE_100K_1M:
            case DFS_SIZE_WRITE_1M_4M:
            case DFS_SIZE_WRITE_4M_10M:
            case DFS_SIZE_WRITE_10M_100M:
            case DFS_SIZE_WRITE_100M_1G:
            case DFS_SIZE_WRITE_1G_PLUS:
                /* sum */
                agg_dfs_rec->counters[i] += dfs_rec->counters[i];
                if(agg_dfs_rec->counters[i] < 0) /* make sure invalid counters are -1 exactly */
                    agg_dfs_rec->counters[i] = -1;
                break;
            case DFS_CHUNK_SIZE:
                /* just set to the input value */
                agg_dfs_rec->counters[i] = dfs_rec->counters[i];
                break;
            case DFS_MAX_READ_TIME_SIZE:
            case DFS_MAX_WRITE_TIME_SIZE:
            case DFS_FASTEST_RANK:
            case DFS_FASTEST_RANK_BYTES:
            case DFS_SLOWEST_RANK:
            case DFS_SLOWEST_RANK_BYTES:
                /* these are set with the FP counters */
                break;
            case DFS_ACCESS1_ACCESS:
                /* increment common value counters */

                /* first, collapse duplicates */
                for(j = i; j < i + 4; j++)
                {
                    for(k = 0; k < 4; k++)
                    {
                        if(agg_dfs_rec->counters[i + k] == dfs_rec->counters[j])
                        {
                            agg_dfs_rec->counters[i + k + 4] += dfs_rec->counters[j + 4];
                            /* flag that we should ignore this one now */
                            duplicate_mask[j-i] = 1;
                        }
                    }
                }

                /* second, add new counters */
                for(j = i; j < i + 4; j++)
                {
                    /* skip any that were handled above already */
                    if(duplicate_mask[j-i])
                        continue;
                    tmp_ndx = 0;
                    memset(tmp_val, 0, 4 * sizeof(int64_t));
                    memset(tmp_cnt, 0, 4 * sizeof(int64_t));

                    if(dfs_rec->counters[j] == 0) break;
                    for(k = 0; k < 4; k++)
                    {
                        if(agg_dfs_rec->counters[i + k] == dfs_rec->counters[j])
                        {
                            total_count = agg_dfs_rec->counters[i + k + 4] +
                                dfs_rec->counters[j + 4];
                            break;
                        }
                    }
                    if(k == 4) total_count = dfs_rec->counters[j + 4];

                    for(k = 0; k < 4; k++)
                    {
                        if((agg_dfs_rec->counters[i + k + 4] > total_count) ||
                           ((agg_dfs_rec->counters[i + k + 4] == total_count) &&
                            (agg_dfs_rec->counters[i + k] > dfs_rec->counters[j])))
                        {
                            tmp_val[tmp_ndx] = agg_dfs_rec->counters[i + k];
                            tmp_cnt[tmp_ndx] = agg_dfs_rec->counters[i + k + 4];
                            tmp_ndx++;
                        }
                        else break;
                    }
                    if(tmp_ndx == 4) break;

                    tmp_val[tmp_ndx] = dfs_rec->counters[j];
                    tmp_cnt[tmp_ndx] = dfs_rec->counters[j + 4];
                    tmp_ndx++;

                    while(tmp_ndx != 4)
                    {
                        if(agg_dfs_rec->counters[i + k] != dfs_rec->counters[j])
                        {
                            tmp_val[tmp_ndx] = agg_dfs_rec->counters[i + k];
                            tmp_cnt[tmp_ndx] = agg_dfs_rec->counters[i + k + 4];
                            tmp_ndx++;
                        }
                        k++;
                    }
                    memcpy(&(agg_dfs_rec->counters[i]), tmp_val, 4 * sizeof(int64_t));
                    memcpy(&(agg_dfs_rec->counters[i + 4]), tmp_cnt, 4 * sizeof(int64_t));
                }
                break;
            case DFS_ACCESS2_ACCESS:
            case DFS_ACCESS3_ACCESS:
            case DFS_ACCESS4_ACCESS:
            case DFS_ACCESS1_COUNT:
            case DFS_ACCESS2_COUNT:
            case DFS_ACCESS3_COUNT:
            case DFS_ACCESS4_COUNT:
                /* these are set all at once with common counters above */
                break;
        }
    }

    for(i = 0; i < DFS_F_NUM_INDICES; i++)
    {
        switch(i)
        {
            case DFS_F_READ_TIME:
            case DFS_F_WRITE_TIME:
            case DFS_F_META_TIME:
                /* sum */
                agg_dfs_rec->fcounters[i] += dfs_rec->fcounters[i];
                break;
            case DFS_F_OPEN_START_TIMESTAMP:
            case DFS_F_READ_START_TIMESTAMP:
            case DFS_F_WRITE_START_TIMESTAMP:
            case DFS_F_CLOSE_START_TIMESTAMP:
                /* minimum non-zero */
                if((dfs_rec->fcounters[i] > 0)  &&
                    ((agg_dfs_rec->fcounters[i] == 0) ||
                    (dfs_rec->fcounters[i] < agg_dfs_rec->fcounters[i])))
                {
                    agg_dfs_rec->fcounters[i] = dfs_rec->fcounters[i];
                }
                break;
            case DFS_F_OPEN_END_TIMESTAMP:
            case DFS_F_READ_END_TIMESTAMP:
            case DFS_F_WRITE_END_TIMESTAMP:
            case DFS_F_CLOSE_END_TIMESTAMP:
                /* maximum */
                if(dfs_rec->fcounters[i] > agg_dfs_rec->fcounters[i])
                {
                    agg_dfs_rec->fcounters[i] = dfs_rec->fcounters[i];
                }
                break;
            case DFS_F_MAX_READ_TIME:
                if(dfs_rec->fcounters[i] > agg_dfs_rec->fcounters[i])
                {
                    agg_dfs_rec->fcounters[i] = dfs_rec->fcounters[i];
                    agg_dfs_rec->counters[DFS_MAX_READ_TIME_SIZE] =
                        dfs_rec->counters[DFS_MAX_READ_TIME_SIZE];
                }
                break;
            case DFS_F_MAX_WRITE_TIME:
                if(dfs_rec->fcounters[i] > agg_dfs_rec->fcounters[i])
                {
                    agg_dfs_rec->fcounters[i] = dfs_rec->fcounters[i];
                    agg_dfs_rec->counters[DFS_MAX_WRITE_TIME_SIZE] =
                        dfs_rec->counters[DFS_MAX_WRITE_TIME_SIZE];
                }
                break;
            case DFS_F_FASTEST_RANK_TIME:

                if(!shared_file_flag)
                {
                    /* The fastest counters are only valid under these
                     * conditions when aggregating records that all refer to
                     * the same file.
                     */
                    agg_dfs_rec->counters[DFS_FASTEST_RANK] = -1;
                    agg_dfs_rec->counters[DFS_FASTEST_RANK_BYTES] = -1;
                    agg_dfs_rec->fcounters[DFS_F_FASTEST_RANK_TIME] = 0.0;
                    break;
                }
                if (init_flag ||
                    dfs_fastest_time < agg_dfs_rec->fcounters[DFS_F_FASTEST_RANK_TIME]) {
                    /* The incoming record wins if a) this is the first
                     * record we are aggregating or b) it is the fastest
                     * record we have seen so far.
                     */
                    agg_dfs_rec->counters[DFS_FASTEST_RANK]
                        = dfs_fastest_rank;
                    agg_dfs_rec->counters[DFS_FASTEST_RANK_BYTES]
                        = dfs_fastest_bytes;
                    agg_dfs_rec->fcounters[DFS_F_FASTEST_RANK_TIME]
                        = dfs_fastest_time;
                }
                break;
            case DFS_F_SLOWEST_RANK_TIME:
                if(!shared_file_flag)
                {
                    /* The slowest counters are only valid under these
                     * conditions when aggregating records that all refer to
                     * the same file.
                     */
                    agg_dfs_rec->counters[DFS_SLOWEST_RANK] = -1;
                    agg_dfs_rec->counters[DFS_SLOWEST_RANK_BYTES] = -1;
                    agg_dfs_rec->fcounters[DFS_F_SLOWEST_RANK_TIME] = 0.0;
                    break;
                }
                if (init_flag ||
                    dfs_slowest_time > agg_dfs_rec->fcounters[DFS_F_SLOWEST_RANK_TIME]) {
                    /* The incoming record wins if a) this is the first
                     * record we are aggregating or b) it is the slowest
                     * record we have seen so far.
                     */
                    agg_dfs_rec->counters[DFS_SLOWEST_RANK]
                        = dfs_slowest_rank;
                    agg_dfs_rec->counters[DFS_SLOWEST_RANK_BYTES]
                        = dfs_slowest_bytes;
                    agg_dfs_rec->fcounters[DFS_F_SLOWEST_RANK_TIME]
                        = dfs_slowest_time;
                }
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
