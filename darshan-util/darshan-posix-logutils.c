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

#include "darshan-posix-logutils.h"

/* counter name strings for the POSIX module */
#define X(a) #a,
char *posix_counter_names[] = {
    POSIX_COUNTERS
};

char *posix_f_counter_names[] = {
    POSIX_F_COUNTERS
};
#undef X

static int darshan_log_get_posix_file(darshan_fd fd, void* posix_buf);
static int darshan_log_put_posix_file(darshan_fd fd, void* posix_buf);
static void darshan_log_print_posix_file(void *file_rec,
    char *file_name, char *mnt_pt, char *fs_type);
static void darshan_log_agg_posix_files(void *rec, void *agg_rec, int init_flag);
static void darshan_log_print_posix_file_diff(void *file_rec1, char *file_name1,
    void *file_rec2, char *file_name2);

struct darshan_mod_logutil_funcs posix_logutils =
{
    .log_get_record = &darshan_log_get_posix_file,
    .log_put_record = &darshan_log_put_posix_file,
    .log_print_record = &darshan_log_print_posix_file,
    .log_agg_records = &darshan_log_agg_posix_files,
    .log_print_diff = &darshan_log_print_posix_file_diff
};

static int darshan_log_get_posix_file(darshan_fd fd, void* posix_buf)
{
    struct darshan_posix_file *file;
    int i;
    int ret;

    ret = darshan_log_getmod(fd, DARSHAN_POSIX_MOD, posix_buf,
        sizeof(struct darshan_posix_file));
    if(ret < 0)
        return(-1);
    else if(ret < sizeof(struct darshan_posix_file))
        return(0);
    else
    {
        file = (struct darshan_posix_file *)posix_buf;
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

static int darshan_log_put_posix_file(darshan_fd fd, void* posix_buf)
{
    struct darshan_posix_file *file = (struct darshan_posix_file *)posix_buf;
    int ret;

    ret = darshan_log_putmod(fd, DARSHAN_POSIX_MOD, file,
        sizeof(struct darshan_posix_file));
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

static void darshan_log_agg_posix_files(void *rec, void *agg_rec, int init_flag)
{
    struct darshan_posix_file *psx_rec = (struct darshan_posix_file *)rec;
    struct darshan_posix_file *agg_psx_rec = (struct darshan_posix_file *)agg_rec;
    int i;
    double psx_time = psx_rec->fcounters[POSIX_F_READ_TIME] +
        psx_rec->fcounters[POSIX_F_WRITE_TIME] +
        psx_rec->fcounters[POSIX_F_META_TIME];

    /* special case initialization of shared record for
     * first call of this function
     */
    if(init_flag)
    {
        /* set fastest/slowest rank counters according to root rank.
         * these counters will be determined as the aggregation progresses.
         */
        agg_psx_rec->counters[POSIX_FASTEST_RANK] = psx_rec->base_rec.rank;
        agg_psx_rec->counters[POSIX_FASTEST_RANK_BYTES] =
            psx_rec->counters[POSIX_BYTES_READ] +
            psx_rec->counters[POSIX_BYTES_WRITTEN];
        agg_psx_rec->fcounters[POSIX_F_FASTEST_RANK_TIME] = psx_time;

        agg_psx_rec->counters[POSIX_SLOWEST_RANK] =
            agg_psx_rec->counters[POSIX_FASTEST_RANK];
        agg_psx_rec->counters[POSIX_SLOWEST_RANK_BYTES] =
            agg_psx_rec->counters[POSIX_FASTEST_RANK_BYTES];
        agg_psx_rec->fcounters[POSIX_F_SLOWEST_RANK_TIME] =
            agg_psx_rec->fcounters[POSIX_F_FASTEST_RANK_TIME];
    }

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
            case POSIX_FOPENS:
            case POSIX_FREADS:
            case POSIX_FWRITES:
            case POSIX_FSEEKS:
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
            default:
                /* TODO: common access counters and strides */
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
                if(psx_time < agg_psx_rec->fcounters[POSIX_F_FASTEST_RANK_TIME])
                {
                    agg_psx_rec->counters[POSIX_FASTEST_RANK] = psx_rec->base_rec.rank;
                    agg_psx_rec->counters[POSIX_FASTEST_RANK_BYTES] =
                        psx_rec->counters[POSIX_BYTES_READ] +
                        psx_rec->counters[POSIX_BYTES_WRITTEN];
                    agg_psx_rec->fcounters[POSIX_F_FASTEST_RANK_TIME] = psx_time;
                }
                break;
            case POSIX_F_SLOWEST_RANK_TIME:
                if(psx_time > agg_psx_rec->fcounters[POSIX_F_SLOWEST_RANK_TIME])
                {
                    agg_psx_rec->counters[POSIX_SLOWEST_RANK] = psx_rec->base_rec.rank;
                    agg_psx_rec->counters[POSIX_SLOWEST_RANK_BYTES] =
                        psx_rec->counters[POSIX_BYTES_READ] +
                        psx_rec->counters[POSIX_BYTES_WRITTEN];
                    agg_psx_rec->fcounters[POSIX_F_SLOWEST_RANK_TIME] = psx_time;
                }
                break;
            default:
                /* TODO: variance */
                agg_psx_rec->fcounters[i] = -1;
                break;
        }
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

/*
 * Local variables:
 *  c-indent-level: 4
 *  c-basic-offset: 4
 * End:
 *
 * vim: ts=8 sts=4 sw=4 expandtab
 */
