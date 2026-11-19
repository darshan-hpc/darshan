/*
 * Copyright (C) 2025 University of Chicago.
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

/* counter name strings for the GDS module */
#define X(a) #a,
char *gds_counter_names[] = {
    GDS_COUNTERS
};

char *gds_f_counter_names[] = {
    GDS_F_COUNTERS
};
#undef X

static int darshan_log_get_gds_file(darshan_fd fd, void** gds_buf_p);
static int darshan_log_put_gds_file(darshan_fd fd, void* gds_buf);
static void darshan_log_print_gds_file(void *file_rec,
    char *file_name, char *mnt_pt, char *fs_type);
static void darshan_log_print_gds_description(int ver);
static void darshan_log_print_gds_file_diff(void *file_rec1, char *file_name1,
    void *file_rec2, char *file_name2);
static void darshan_log_agg_gds_files(void *rec, void *agg_rec, int init_flag);
static int darshan_log_sizeof_gds_file(void* gds_buf_p);
static int darshan_log_record_metrics_gds_file(void* gds_buf_p,
                                                 uint64_t* rec_id,
                                                 int64_t* r_bytes,
                                                 int64_t* w_bytes,
                                                 int64_t* max_offset,
                                                 double* io_total_time,
                                                 double* md_only_time,
                                                 double* rw_only_time,
                                                 int64_t* rank,
                                                 int64_t* nprocs);

struct darshan_mod_logutil_funcs gds_logutils =
{
    .log_get_record = &darshan_log_get_gds_file,
    .log_put_record = &darshan_log_put_gds_file,
    .log_print_record = &darshan_log_print_gds_file,
    .log_print_description = &darshan_log_print_gds_description,
    .log_print_diff = &darshan_log_print_gds_file_diff,
    .log_agg_records = &darshan_log_agg_gds_files,
    .log_sizeof_record = &darshan_log_sizeof_gds_file,
    .log_record_metrics = &darshan_log_record_metrics_gds_file
};

static int darshan_log_sizeof_gds_file(void* gds_buf_p)
{
    /* gds records have a fixed size */
    return(sizeof(struct darshan_gds_file));
}

static int darshan_log_record_metrics_gds_file(void* gds_buf_p,
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
    struct darshan_gds_file *gds_rec = (struct darshan_gds_file *)gds_buf_p;

    *rec_id = gds_rec->base_rec.id;
    *r_bytes = gds_rec->counters[GDS_BYTES_READ];
    *w_bytes = gds_rec->counters[GDS_BYTES_WRITTEN];

    *max_offset = gds_rec->counters[GDS_MAX_BYTE_READ];
    if(gds_rec->counters[GDS_MAX_BYTE_WRITTEN] > *max_offset)
        *max_offset = gds_rec->counters[GDS_MAX_BYTE_WRITTEN];

    *rank = gds_rec->base_rec.rank;
    /* nprocs is 1 per record, unless rank is negative, in which case we
     * report -1 as the rank value to represent "all"
     */
    if(gds_rec->base_rec.rank < 0)
        *nprocs = -1;
    else
        *nprocs = 1;

    if(gds_rec->base_rec.rank < 0) {
        /* shared file records populate a counter with the slowest rank time
         * (derived during reduction).  They do not have a breakdown of meta
         * and rw time, though.
         */
        *io_total_time = gds_rec->fcounters[GDS_F_SLOWEST_RANK_TIME];
        *md_only_time = 0;
        *rw_only_time = 0;
    }
    else {
        /* non-shared records have separate meta, read, and write values
         * that we can combine as needed
         */
        *io_total_time = gds_rec->fcounters[GDS_F_META_TIME] +
                         gds_rec->fcounters[GDS_F_READ_TIME] +
                         gds_rec->fcounters[GDS_F_WRITE_TIME];
        *md_only_time = gds_rec->fcounters[GDS_F_META_TIME];
        *rw_only_time = gds_rec->fcounters[GDS_F_READ_TIME] +
                        gds_rec->fcounters[GDS_F_WRITE_TIME];
    }

    return(0);
}

static int darshan_log_get_gds_file(darshan_fd fd, void** gds_buf_p)
{
    struct darshan_gds_file *file = *((struct darshan_gds_file **)gds_buf_p);
    int rec_len;
    int i;
    int ret = -1;

    if(fd->mod_map[DARSHAN_GDS_MOD].len == 0)
        return(0);

    if(fd->mod_ver[DARSHAN_GDS_MOD] == 0 ||
        fd->mod_ver[DARSHAN_GDS_MOD] > DARSHAN_GDS_VER)
    {
        fprintf(stderr, "Error: Invalid GDS module version number (got %d)\n",
            fd->mod_ver[DARSHAN_GDS_MOD]);
        return(-1);
    }

    if(*gds_buf_p == NULL)
    {
        file = malloc(sizeof(*file));
        if(!file)
            return(-1);
    }

    if(fd->mod_ver[DARSHAN_GDS_MOD] == DARSHAN_GDS_VER)
    {
        /* log format is in current version, so we don't need to do any
         * translation of counters while reading
         */
        rec_len = sizeof(struct darshan_gds_file);
        ret = darshan_log_get_mod(fd, DARSHAN_GDS_MOD, file, rec_len);
    }
    else
    {
        assert(0);
    }

    if(*gds_buf_p == NULL)
    {
        if(ret == rec_len)
            *gds_buf_p = file;
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
            for(i=0; i<GDS_NUM_INDICES; i++)
                DARSHAN_BSWAP64(&file->counters[i]);
            for(i=0; i<GDS_F_NUM_INDICES; i++)
                DARSHAN_BSWAP64(&file->fcounters[i]);
        }

        return(1);
    }
}

static int darshan_log_put_gds_file(darshan_fd fd, void* gds_buf)
{
    struct darshan_gds_file *file = (struct darshan_gds_file *)gds_buf;
    int ret;

    ret = darshan_log_put_mod(fd, DARSHAN_GDS_MOD, file,
        sizeof(struct darshan_gds_file), DARSHAN_GDS_VER);
    if(ret < 0)
        return(-1);

    return(0);
}

static void darshan_log_print_gds_file(void *file_rec, char *file_name,
    char *mnt_pt, char *fs_type)
{
    int i;
    struct darshan_gds_file *gds_file_rec =
        (struct darshan_gds_file *)file_rec;

    for(i=0; i<GDS_NUM_INDICES; i++)
    {
        DARSHAN_D_COUNTER_PRINT(darshan_module_names[DARSHAN_GDS_MOD],
            gds_file_rec->base_rec.rank, gds_file_rec->base_rec.id,
            gds_counter_names[i], gds_file_rec->counters[i],
            file_name, mnt_pt, fs_type);
    }

    for(i=0; i<GDS_F_NUM_INDICES; i++)
    {
        DARSHAN_F_COUNTER_PRINT(darshan_module_names[DARSHAN_GDS_MOD],
            gds_file_rec->base_rec.rank, gds_file_rec->base_rec.id,
            gds_f_counter_names[i], gds_file_rec->fcounters[i],
            file_name, mnt_pt, fs_type);
    }

    return;
}

static void darshan_log_print_gds_description(int ver)
{
    printf("\n# description of GDS counters (NVIDIA GPUDirect Storage, cuFile API):\n");
    printf("#   GDS_HANDLE_REGISTERS/DEREGISTERS: cuFileHandleRegister/Deregister counts.\n");
    printf("#   GDS_READS/WRITES: cuFileRead/cuFileWrite counts; GDS_READ/WRITE_ERRORS count failed calls.\n");
    printf("#   GDS_BATCH_READS/WRITES: operations submitted via cuFileBatchIOSubmit.\n");
    printf("#   GDS_ASYNC_READS/WRITES: cuFileReadAsync/cuFileWriteAsync counts.\n");
    printf("#   GDS_BYTES_READ/WRITTEN: bytes moved by synchronous calls.\n");
    printf("#   GDS_BATCH_BYTES_*: bytes requested by batch operations (completion not tracked).\n");
    printf("#   GDS_ASYNC_BYTES_REQ_*: bytes requested by async operations (upper bound; completion not tracked).\n");
    printf("#   GDS_MAX_BYTE_*: highest offset read/written.\n");
    printf("#   GDS_CONSEC/SEQ_*: consecutive and sequential access counts.\n");
    printf("#   GDS_RW_SWITCHES: number of times access alternated between read and write.\n");
    printf("#   GDS_MAX_*_TIME_SIZE: size of the slowest read and write operations.\n");
    printf("#   GDS_SIZE_*_*: histogram of read and write access sizes.\n");
    printf("#   GDS_ACCESS*_ACCESS: the four most common access sizes.\n");
    printf("#   GDS_ACCESS*_COUNT: count of the four most common access sizes.\n");
    printf("#   GDS_*_RANK: rank of the processes that were the fastest and slowest at I/O (for shared files).\n");
    printf("#   GDS_*_RANK_BYTES: bytes transferred by the fastest and slowest ranks (for shared files).\n");
    printf("#   GDS_F_*_START_TIMESTAMP: timestamp of first register(open)/read/write/deregister(close).\n");
    printf("#   GDS_F_*_END_TIMESTAMP: timestamp of last register(open)/read/write/deregister(close).\n");
    printf("#   GDS_F_READ/WRITE/META_TIME: cumulative time spent in read, write, or handle register/deregister.\n");
    printf("#   GDS_F_MAX_*_TIME: duration of the slowest read and write operations.\n");
    printf("#   GDS_F_*_RANK_TIME: fastest and slowest I/O time for a single rank (for shared files).\n");
    printf("#   Note: with cuFile compat mode, POSIX module counters also include the underlying pread/pwrite.\n");

    return;
}

static void darshan_log_print_gds_file_diff(void *file_rec1, char *file_name1,
    void *file_rec2, char *file_name2)
{
    struct darshan_gds_file *file1 = (struct darshan_gds_file *)file_rec1;
    struct darshan_gds_file *file2 = (struct darshan_gds_file *)file_rec2;
    int i;

    /* NOTE: we assume that both input records are the same module format version */

    for(i=0; i<GDS_NUM_INDICES; i++)
    {
        if(!file2)
        {
            printf("- ");
            DARSHAN_D_COUNTER_PRINT(darshan_module_names[DARSHAN_GDS_MOD],
                file1->base_rec.rank, file1->base_rec.id, gds_counter_names[i],
                file1->counters[i], file_name1, "", "");

        }
        else if(!file1)
        {
            printf("+ ");
            DARSHAN_D_COUNTER_PRINT(darshan_module_names[DARSHAN_GDS_MOD],
                file2->base_rec.rank, file2->base_rec.id, gds_counter_names[i],
                file2->counters[i], file_name2, "", "");
        }
        else if(file1->counters[i] != file2->counters[i])
        {
            printf("- ");
            DARSHAN_D_COUNTER_PRINT(darshan_module_names[DARSHAN_GDS_MOD],
                file1->base_rec.rank, file1->base_rec.id, gds_counter_names[i],
                file1->counters[i], file_name1, "", "");
            printf("+ ");
            DARSHAN_D_COUNTER_PRINT(darshan_module_names[DARSHAN_GDS_MOD],
                file2->base_rec.rank, file2->base_rec.id, gds_counter_names[i],
                file2->counters[i], file_name2, "", "");
        }
    }

    for(i=0; i<GDS_F_NUM_INDICES; i++)
    {
        if(!file2)
        {
            printf("- ");
            DARSHAN_F_COUNTER_PRINT(darshan_module_names[DARSHAN_GDS_MOD],
                file1->base_rec.rank, file1->base_rec.id, gds_f_counter_names[i],
                file1->fcounters[i], file_name1, "", "");

        }
        else if(!file1)
        {
            printf("+ ");
            DARSHAN_F_COUNTER_PRINT(darshan_module_names[DARSHAN_GDS_MOD],
                file2->base_rec.rank, file2->base_rec.id, gds_f_counter_names[i],
                file2->fcounters[i], file_name2, "", "");
        }
        else if(file1->fcounters[i] != file2->fcounters[i])
        {
            printf("- ");
            DARSHAN_F_COUNTER_PRINT(darshan_module_names[DARSHAN_GDS_MOD],
                file1->base_rec.rank, file1->base_rec.id, gds_f_counter_names[i],
                file1->fcounters[i], file_name1, "", "");
            printf("+ ");
            DARSHAN_F_COUNTER_PRINT(darshan_module_names[DARSHAN_GDS_MOD],
                file2->base_rec.rank, file2->base_rec.id, gds_f_counter_names[i],
                file2->fcounters[i], file_name2, "", "");
        }
    }

    return;
}

static void darshan_log_agg_gds_files(void *rec, void *agg_rec, int init_flag)
{
    struct darshan_gds_file *gds_rec = (struct darshan_gds_file *)rec;
    struct darshan_gds_file *agg_gds_rec = (struct darshan_gds_file *)agg_rec;
    int i, j, k;
    int total_count;
    int64_t tmp_val[4];
    int64_t tmp_cnt[4];
    int duplicate_mask[4] = {0};
    int tmp_ndx;
    int64_t gds_fastest_rank, gds_slowest_rank,
        gds_fastest_bytes, gds_slowest_bytes;
    double gds_fastest_time, gds_slowest_time;
    int shared_file_flag = 0;

    /* For the incoming record, we need to determine what values to use for
     * subsequent comparision against the aggregate record's fastest and
     * slowest fields. This is is complicated by the fact that shared file
     * records already have derived values, while unique file records do
     * not.  Handle both cases here so that this function can be generic.
     */
    if(gds_rec->base_rec.rank == -1)
    {
        /* shared files should have pre-calculated fastest and slowest
         * counters */
        gds_fastest_rank = gds_rec->counters[GDS_FASTEST_RANK];
        gds_slowest_rank = gds_rec->counters[GDS_SLOWEST_RANK];
        gds_fastest_bytes = gds_rec->counters[GDS_FASTEST_RANK_BYTES];
        gds_slowest_bytes = gds_rec->counters[GDS_SLOWEST_RANK_BYTES];
        gds_fastest_time = gds_rec->fcounters[GDS_F_FASTEST_RANK_TIME];
        gds_slowest_time = gds_rec->fcounters[GDS_F_SLOWEST_RANK_TIME];
    }
    else
    {
        /* for non-shared files, derive bytes and time using data from this
         * rank
         */
        gds_fastest_rank = gds_rec->base_rec.rank;
        gds_slowest_rank = gds_fastest_rank;
        gds_fastest_bytes = gds_rec->counters[GDS_BYTES_READ] +
            gds_rec->counters[GDS_BYTES_WRITTEN];
        gds_slowest_bytes = gds_fastest_bytes;
        gds_fastest_time = gds_rec->fcounters[GDS_F_READ_TIME] +
            gds_rec->fcounters[GDS_F_WRITE_TIME] +
            gds_rec->fcounters[GDS_F_META_TIME];
        gds_slowest_time = gds_fastest_time;
    }

    /* if this is our first record, store base id and rank */
    if(init_flag)
    {
        agg_gds_rec->base_rec.rank = gds_rec->base_rec.rank;
        agg_gds_rec->base_rec.id = gds_rec->base_rec.id;
    }

    /* so far do all of the records reference the same file? */
    if(agg_gds_rec->base_rec.id == gds_rec->base_rec.id)
        shared_file_flag = 1;
    else
        agg_gds_rec->base_rec.id = 0;

    /* so far do all of the records reference the same rank? */
    if(agg_gds_rec->base_rec.rank != gds_rec->base_rec.rank)
        agg_gds_rec->base_rec.rank = -1;

    for(i = 0; i < GDS_NUM_INDICES; i++)
    {
        switch(i)
        {
            case GDS_HANDLE_REGISTERS:
            case GDS_HANDLE_DEREGISTERS:
            case GDS_READS:
            case GDS_WRITES:
            case GDS_READ_ERRORS:
            case GDS_WRITE_ERRORS:
            case GDS_BATCH_READS:
            case GDS_BATCH_WRITES:
            case GDS_ASYNC_READS:
            case GDS_ASYNC_WRITES:
            case GDS_BYTES_READ:
            case GDS_BYTES_WRITTEN:
            case GDS_BATCH_BYTES_READ:
            case GDS_BATCH_BYTES_WRITTEN:
            case GDS_ASYNC_BYTES_REQ_READ:
            case GDS_ASYNC_BYTES_REQ_WRITTEN:
            case GDS_CONSEC_READS:
            case GDS_CONSEC_WRITES:
            case GDS_SEQ_READS:
            case GDS_SEQ_WRITES:
            case GDS_RW_SWITCHES:
            case GDS_SIZE_READ_0_100:
            case GDS_SIZE_READ_100_1K:
            case GDS_SIZE_READ_1K_10K:
            case GDS_SIZE_READ_10K_100K:
            case GDS_SIZE_READ_100K_1M:
            case GDS_SIZE_READ_1M_4M:
            case GDS_SIZE_READ_4M_10M:
            case GDS_SIZE_READ_10M_100M:
            case GDS_SIZE_READ_100M_1G:
            case GDS_SIZE_READ_1G_PLUS:
            case GDS_SIZE_WRITE_0_100:
            case GDS_SIZE_WRITE_100_1K:
            case GDS_SIZE_WRITE_1K_10K:
            case GDS_SIZE_WRITE_10K_100K:
            case GDS_SIZE_WRITE_100K_1M:
            case GDS_SIZE_WRITE_1M_4M:
            case GDS_SIZE_WRITE_4M_10M:
            case GDS_SIZE_WRITE_10M_100M:
            case GDS_SIZE_WRITE_100M_1G:
            case GDS_SIZE_WRITE_1G_PLUS:
                /* sum */
                agg_gds_rec->counters[i] += gds_rec->counters[i];
                if(agg_gds_rec->counters[i] < 0) /* make sure invalid counters are -1 exactly */
                    agg_gds_rec->counters[i] = -1;
                break;
            case GDS_MAX_BYTE_READ:
            case GDS_MAX_BYTE_WRITTEN:
                /* max */
                if(gds_rec->counters[i] > agg_gds_rec->counters[i])
                    agg_gds_rec->counters[i] = gds_rec->counters[i];
                break;
            case GDS_MAX_READ_TIME_SIZE:
            case GDS_MAX_WRITE_TIME_SIZE:
            case GDS_FASTEST_RANK:
            case GDS_FASTEST_RANK_BYTES:
            case GDS_SLOWEST_RANK:
            case GDS_SLOWEST_RANK_BYTES:
                /* these are set with the FP counters */
                break;
            case GDS_ACCESS1_ACCESS:
                /* increment common value counters */

                /* first, collapse duplicates */
                for(j = i; j < i + 4; j++)
                {
                    for(k = 0; k < 4; k++)
                    {
                        if(agg_gds_rec->counters[i + k] == gds_rec->counters[j])
                        {
                            agg_gds_rec->counters[i + k + 4] += gds_rec->counters[j + 4];
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

                    if(gds_rec->counters[j] == 0) break;
                    for(k = 0; k < 4; k++)
                    {
                        if(agg_gds_rec->counters[i + k] == gds_rec->counters[j])
                        {
                            total_count = agg_gds_rec->counters[i + k + 4] +
                                gds_rec->counters[j + 4];
                            break;
                        }
                    }
                    if(k == 4) total_count = gds_rec->counters[j + 4];

                    for(k = 0; k < 4; k++)
                    {
                        if((agg_gds_rec->counters[i + k + 4] > total_count) ||
                           ((agg_gds_rec->counters[i + k + 4] == total_count) &&
                            (agg_gds_rec->counters[i + k] > gds_rec->counters[j])))
                        {
                            tmp_val[tmp_ndx] = agg_gds_rec->counters[i + k];
                            tmp_cnt[tmp_ndx] = agg_gds_rec->counters[i + k + 4];
                            tmp_ndx++;
                        }
                        else break;
                    }
                    if(tmp_ndx == 4) break;

                    tmp_val[tmp_ndx] = gds_rec->counters[j];
                    tmp_cnt[tmp_ndx] = gds_rec->counters[j + 4];
                    tmp_ndx++;

                    while(tmp_ndx != 4)
                    {
                        if(agg_gds_rec->counters[i + k] != gds_rec->counters[j])
                        {
                            tmp_val[tmp_ndx] = agg_gds_rec->counters[i + k];
                            tmp_cnt[tmp_ndx] = agg_gds_rec->counters[i + k + 4];
                            tmp_ndx++;
                        }
                        k++;
                    }
                    memcpy(&(agg_gds_rec->counters[i]), tmp_val, 4 * sizeof(int64_t));
                    memcpy(&(agg_gds_rec->counters[i + 4]), tmp_cnt, 4 * sizeof(int64_t));
                }
                break;
            case GDS_ACCESS2_ACCESS:
            case GDS_ACCESS3_ACCESS:
            case GDS_ACCESS4_ACCESS:
            case GDS_ACCESS1_COUNT:
            case GDS_ACCESS2_COUNT:
            case GDS_ACCESS3_COUNT:
            case GDS_ACCESS4_COUNT:
                /* these are set all at once with common counters above */
                break;
        }
    }

    for(i = 0; i < GDS_F_NUM_INDICES; i++)
    {
        switch(i)
        {
            case GDS_F_READ_TIME:
            case GDS_F_WRITE_TIME:
            case GDS_F_META_TIME:
                /* sum */
                agg_gds_rec->fcounters[i] += gds_rec->fcounters[i];
                break;
            case GDS_F_OPEN_START_TIMESTAMP:
            case GDS_F_READ_START_TIMESTAMP:
            case GDS_F_WRITE_START_TIMESTAMP:
            case GDS_F_CLOSE_START_TIMESTAMP:
                /* minimum non-zero */
                if((gds_rec->fcounters[i] > 0)  &&
                    ((agg_gds_rec->fcounters[i] == 0) ||
                    (gds_rec->fcounters[i] < agg_gds_rec->fcounters[i])))
                {
                    agg_gds_rec->fcounters[i] = gds_rec->fcounters[i];
                }
                break;
            case GDS_F_OPEN_END_TIMESTAMP:
            case GDS_F_READ_END_TIMESTAMP:
            case GDS_F_WRITE_END_TIMESTAMP:
            case GDS_F_CLOSE_END_TIMESTAMP:
                /* maximum */
                if(gds_rec->fcounters[i] > agg_gds_rec->fcounters[i])
                {
                    agg_gds_rec->fcounters[i] = gds_rec->fcounters[i];
                }
                break;
            case GDS_F_MAX_READ_TIME:
                if(gds_rec->fcounters[i] > agg_gds_rec->fcounters[i])
                {
                    agg_gds_rec->fcounters[i] = gds_rec->fcounters[i];
                    agg_gds_rec->counters[GDS_MAX_READ_TIME_SIZE] =
                        gds_rec->counters[GDS_MAX_READ_TIME_SIZE];
                }
                break;
            case GDS_F_MAX_WRITE_TIME:
                if(gds_rec->fcounters[i] > agg_gds_rec->fcounters[i])
                {
                    agg_gds_rec->fcounters[i] = gds_rec->fcounters[i];
                    agg_gds_rec->counters[GDS_MAX_WRITE_TIME_SIZE] =
                        gds_rec->counters[GDS_MAX_WRITE_TIME_SIZE];
                }
                break;
            case GDS_F_FASTEST_RANK_TIME:

                if(!shared_file_flag)
                {
                    /* The fastest counters are only valid under these
                     * conditions when aggregating records that all refer to
                     * the same file.
                     */
                    agg_gds_rec->counters[GDS_FASTEST_RANK] = -1;
                    agg_gds_rec->counters[GDS_FASTEST_RANK_BYTES] = -1;
                    agg_gds_rec->fcounters[GDS_F_FASTEST_RANK_TIME] = 0.0;
                    break;
                }
                if (init_flag ||
                    gds_fastest_time < agg_gds_rec->fcounters[GDS_F_FASTEST_RANK_TIME]) {
                    /* The incoming record wins if a) this is the first
                     * record we are aggregating or b) it is the fastest
                     * record we have seen so far.
                     */
                    agg_gds_rec->counters[GDS_FASTEST_RANK]
                        = gds_fastest_rank;
                    agg_gds_rec->counters[GDS_FASTEST_RANK_BYTES]
                        = gds_fastest_bytes;
                    agg_gds_rec->fcounters[GDS_F_FASTEST_RANK_TIME]
                        = gds_fastest_time;
                }
                break;
            case GDS_F_SLOWEST_RANK_TIME:
                if(!shared_file_flag)
                {
                    /* The slowest counters are only valid under these
                     * conditions when aggregating records that all refer to
                     * the same file.
                     */
                    agg_gds_rec->counters[GDS_SLOWEST_RANK] = -1;
                    agg_gds_rec->counters[GDS_SLOWEST_RANK_BYTES] = -1;
                    agg_gds_rec->fcounters[GDS_F_SLOWEST_RANK_TIME] = 0.0;
                    break;
                }
                if (init_flag ||
                    gds_slowest_time > agg_gds_rec->fcounters[GDS_F_SLOWEST_RANK_TIME]) {
                    /* The incoming record wins if a) this is the first
                     * record we are aggregating or b) it is the slowest
                     * record we have seen so far.
                     */
                    agg_gds_rec->counters[GDS_SLOWEST_RANK]
                        = gds_slowest_rank;
                    agg_gds_rec->counters[GDS_SLOWEST_RANK_BYTES]
                        = gds_slowest_bytes;
                    agg_gds_rec->fcounters[GDS_F_SLOWEST_RANK_TIME]
                        = gds_slowest_time;
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
