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

/* counter name strings for the MPI-IO module */
#define X(a) #a,
char *mpiio_counter_names[] = {
    MPIIO_COUNTERS
};

char *mpiio_f_counter_names[] = {
    MPIIO_F_COUNTERS
};
#undef X

static int darshan_log_get_mpiio_file(darshan_fd fd, void* mpiio_buf);
static int darshan_log_put_mpiio_file(darshan_fd fd, void* mpiio_buf, int ver);
static void darshan_log_print_mpiio_file(void *file_rec,
    char *file_name, char *mnt_pt, char *fs_type, int ver);
static void darshan_log_print_mpiio_description(void);
static void darshan_log_print_mpiio_file_diff(void *file_rec1, char *file_name1,
    void *file_rec2, char *file_name2);
static void darshan_log_agg_mpiio_files(void *rec, void *agg_rec, int init_flag);

struct darshan_mod_logutil_funcs mpiio_logutils =
{
    .log_get_record = &darshan_log_get_mpiio_file,
    .log_put_record = &darshan_log_put_mpiio_file,
    .log_print_record = &darshan_log_print_mpiio_file,
    .log_print_description = &darshan_log_print_mpiio_description,
    .log_print_diff = &darshan_log_print_mpiio_file_diff,
    .log_agg_records = &darshan_log_agg_mpiio_files
};

static int darshan_log_get_mpiio_file(darshan_fd fd, void* mpiio_buf)
{
    struct darshan_mpiio_file *file;
    int i;
    int ret;

    ret = darshan_log_getmod(fd, DARSHAN_MPIIO_MOD, mpiio_buf,
        sizeof(struct darshan_mpiio_file));
    if(ret < 0)
        return(-1);
    else if(ret < sizeof(struct darshan_mpiio_file))
        return(0);
    else
    {
        file = (struct darshan_mpiio_file *)mpiio_buf;
        if(fd->swap_flag)
        {
            /* swap bytes if necessary */
            DARSHAN_BSWAP64(&(file->base_rec.id));
            DARSHAN_BSWAP64(&(file->base_rec.rank));
            for(i=0; i<MPIIO_NUM_INDICES; i++)
                DARSHAN_BSWAP64(&file->counters[i]);
            for(i=0; i<MPIIO_F_NUM_INDICES; i++)
                DARSHAN_BSWAP64(&file->fcounters[i]);
        }

        return(1);
    }
}

static int darshan_log_put_mpiio_file(darshan_fd fd, void* mpiio_buf, int ver)
{
    struct darshan_mpiio_file *file = (struct darshan_mpiio_file *)mpiio_buf;
    int ret;

    ret = darshan_log_putmod(fd, DARSHAN_MPIIO_MOD, file,
        sizeof(struct darshan_mpiio_file), ver);
    if(ret < 0)
        return(-1);

    return(0);
}

static void darshan_log_print_mpiio_file(void *file_rec, char *file_name,
    char *mnt_pt, char *fs_type, int ver)
{
    int i;
    struct darshan_mpiio_file *mpiio_file_rec =
        (struct darshan_mpiio_file *)file_rec;

    for(i=0; i<MPIIO_NUM_INDICES; i++)
    {
        DARSHAN_COUNTER_PRINT(darshan_module_names[DARSHAN_MPIIO_MOD],
            mpiio_file_rec->base_rec.rank, mpiio_file_rec->base_rec.id,
            mpiio_counter_names[i], mpiio_file_rec->counters[i],
            file_name, mnt_pt, fs_type);
    }

    for(i=0; i<MPIIO_F_NUM_INDICES; i++)
    {
        DARSHAN_F_COUNTER_PRINT(darshan_module_names[DARSHAN_MPIIO_MOD],
            mpiio_file_rec->base_rec.rank, mpiio_file_rec->base_rec.id,
            mpiio_f_counter_names[i], mpiio_file_rec->fcounters[i],
            file_name, mnt_pt, fs_type);
    }

    return;
}

static void darshan_log_print_mpiio_description()
{
    printf("\n# description of MPIIO counters:\n");
    printf("#   MPIIO_INDEP_*: MPI independent operation counts.\n");
    printf("#   MPIIO_COLL_*: MPI collective operation counts.\n");
    printf("#   MPIIO_SPLIT_*: MPI split collective operation counts.\n");
    printf("#   MPIIO_NB_*: MPI non blocking operation counts.\n");
    printf("#   READS,WRITES,and OPENS are types of operations.\n");
    printf("#   MPIIO_SYNCS: MPI file sync operation counts.\n");
    printf("#   MPIIO_HINTS: number of times MPI hints were used.\n");
    printf("#   MPIIO_VIEWS: number of times MPI file views were used.\n");
    printf("#   MPIIO_MODE: MPI-IO access mode that file was opened with.\n");
    printf("#   MPIIO_BYTES_*: total bytes read and written at MPI-IO layer.\n");
    printf("#   MPIIO_RW_SWITCHES: number of times access alternated between read and write.\n");
    printf("#   MPIIO_MAX_*_TIME_SIZE: size of the slowest read and write operations.\n");
    printf("#   MPIIO_SIZE_*_AGG_*: histogram of MPI datatype total sizes for read and write operations.\n");
    printf("#   MPIIO_ACCESS*_ACCESS: the four most common total access sizes.\n");
    printf("#   MPIIO_ACCESS*_COUNT: count of the four most common total access sizes.\n");
    printf("#   MPIIO_*_RANK: rank of the processes that were the fastest and slowest at I/O (for shared files).\n");
    printf("#   MPIIO_*_RANK_BYTES: total bytes transferred at MPI-IO layer by the fastest and slowest ranks (for shared files).\n");
    printf("#   MPIIO_F_OPEN_TIMESTAMP: timestamp of first open.\n");
    printf("#   MPIIO_F_*_START_TIMESTAMP: timestamp of first MPI-IO read/write.\n");
    printf("#   MPIIO_F_*_END_TIMESTAMP: timestamp of last MPI-IO read/write.\n");
    printf("#   MPIIO_F_CLOSE_TIMESTAMP: timestamp of last close.\n");
    printf("#   MPIIO_F_READ/WRITE/META_TIME: cumulative time spent in MPI-IO read, write, or metadata operations.\n");
    printf("#   MPIIO_F_MAX_*_TIME: duration of the slowest MPI-IO read and write operations.\n");
    printf("#   MPIIO_F_*_RANK_TIME: fastest and slowest I/O time for a single rank (for shared files).\n");
    printf("#   MPIIO_F_VARIANCE_RANK_*: variance of total I/O time and bytes moved for all ranks (for shared files).\n");

    DARSHAN_PRINT_HEADER();

    return;
}

static void darshan_log_print_mpiio_file_diff(void *file_rec1, char *file_name1,
    void *file_rec2, char *file_name2)
{
    struct darshan_mpiio_file *file1 = (struct darshan_mpiio_file *)file_rec1;
    struct darshan_mpiio_file *file2 = (struct darshan_mpiio_file *)file_rec2;
    int i;

    /* NOTE: we assume that both input records are the same module format version */

    for(i=0; i<MPIIO_NUM_INDICES; i++)
    {
        if(!file2)
        {
            printf("- ");
            DARSHAN_COUNTER_PRINT(darshan_module_names[DARSHAN_MPIIO_MOD],
                file1->base_rec.rank, file1->base_rec.id, mpiio_counter_names[i],
                file1->counters[i], file_name1, "", "");

        }
        else if(!file1)
        {
            printf("+ ");
            DARSHAN_COUNTER_PRINT(darshan_module_names[DARSHAN_MPIIO_MOD],
                file2->base_rec.rank, file2->base_rec.id, mpiio_counter_names[i],
                file2->counters[i], file_name2, "", "");
        }
        else if(file1->counters[i] != file2->counters[i])
        {
            printf("- ");
            DARSHAN_COUNTER_PRINT(darshan_module_names[DARSHAN_MPIIO_MOD],
                file1->base_rec.rank, file1->base_rec.id, mpiio_counter_names[i],
                file1->counters[i], file_name1, "", "");
            printf("+ ");
            DARSHAN_COUNTER_PRINT(darshan_module_names[DARSHAN_MPIIO_MOD],
                file2->base_rec.rank, file2->base_rec.id, mpiio_counter_names[i],
                file2->counters[i], file_name2, "", "");
        }
    }

    for(i=0; i<MPIIO_F_NUM_INDICES; i++)
    {
        if(!file2)
        {
            printf("- ");
            DARSHAN_F_COUNTER_PRINT(darshan_module_names[DARSHAN_MPIIO_MOD],
                file1->base_rec.rank, file1->base_rec.id, mpiio_f_counter_names[i],
                file1->fcounters[i], file_name1, "", "");

        }
        else if(!file1)
        {
            printf("+ ");
            DARSHAN_F_COUNTER_PRINT(darshan_module_names[DARSHAN_MPIIO_MOD],
                file2->base_rec.rank, file2->base_rec.id, mpiio_f_counter_names[i],
                file2->fcounters[i], file_name2, "", "");
        }
        else if(file1->fcounters[i] != file2->fcounters[i])
        {
            printf("- ");
            DARSHAN_F_COUNTER_PRINT(darshan_module_names[DARSHAN_MPIIO_MOD],
                file1->base_rec.rank, file1->base_rec.id, mpiio_f_counter_names[i],
                file1->fcounters[i], file_name1, "", "");
            printf("+ ");
            DARSHAN_F_COUNTER_PRINT(darshan_module_names[DARSHAN_MPIIO_MOD],
                file2->base_rec.rank, file2->base_rec.id, mpiio_f_counter_names[i],
                file2->fcounters[i], file_name2, "", "");
        }
    }

    return;
}

static void darshan_log_agg_mpiio_files(void *rec, void *agg_rec, int init_flag)
{
    struct darshan_mpiio_file *mpi_rec = (struct darshan_mpiio_file *)rec;
    struct darshan_mpiio_file *agg_mpi_rec = (struct darshan_mpiio_file *)agg_rec;
    int i, j, k;
    int set;
    int min_ndx;
    int64_t min;
    double mpi_time = mpi_rec->fcounters[MPIIO_F_READ_TIME] +
        mpi_rec->fcounters[MPIIO_F_WRITE_TIME] +
        mpi_rec->fcounters[MPIIO_F_META_TIME];

    /* special case initialization of shared record for
     * first call of this function
     */
    if(init_flag)
    {
        /* set fastest/slowest rank counters according to root rank.
         * these counters will be determined as the aggregation progresses.
         */
        agg_mpi_rec->counters[MPIIO_FASTEST_RANK] = mpi_rec->base_rec.rank;
        agg_mpi_rec->counters[MPIIO_FASTEST_RANK_BYTES] =
            mpi_rec->counters[MPIIO_BYTES_READ] +
            mpi_rec->counters[MPIIO_BYTES_WRITTEN];
        agg_mpi_rec->fcounters[MPIIO_F_FASTEST_RANK_TIME] = mpi_time;

        agg_mpi_rec->counters[MPIIO_SLOWEST_RANK] =
            agg_mpi_rec->counters[MPIIO_FASTEST_RANK];
        agg_mpi_rec->counters[MPIIO_SLOWEST_RANK_BYTES] =
            agg_mpi_rec->counters[MPIIO_FASTEST_RANK_BYTES];
        agg_mpi_rec->fcounters[MPIIO_F_SLOWEST_RANK_TIME] =
            agg_mpi_rec->fcounters[MPIIO_F_FASTEST_RANK_TIME];
    }

    for(i = 0; i < MPIIO_NUM_INDICES; i++)
    {
        switch(i)
        {
            case MPIIO_INDEP_OPENS:
            case MPIIO_COLL_OPENS:
            case MPIIO_INDEP_READS:
            case MPIIO_INDEP_WRITES:
            case MPIIO_COLL_READS:
            case MPIIO_COLL_WRITES:
            case MPIIO_SPLIT_READS:
            case MPIIO_SPLIT_WRITES:
            case MPIIO_NB_READS:
            case MPIIO_NB_WRITES:
            case MPIIO_SYNCS:
            case MPIIO_HINTS:
            case MPIIO_VIEWS:
            case MPIIO_BYTES_READ:
            case MPIIO_BYTES_WRITTEN:
            case MPIIO_RW_SWITCHES:
            case MPIIO_SIZE_READ_AGG_0_100:
            case MPIIO_SIZE_READ_AGG_100_1K:
            case MPIIO_SIZE_READ_AGG_1K_10K:
            case MPIIO_SIZE_READ_AGG_10K_100K:
            case MPIIO_SIZE_READ_AGG_100K_1M:
            case MPIIO_SIZE_READ_AGG_1M_4M:
            case MPIIO_SIZE_READ_AGG_4M_10M:
            case MPIIO_SIZE_READ_AGG_10M_100M:
            case MPIIO_SIZE_READ_AGG_100M_1G:
            case MPIIO_SIZE_READ_AGG_1G_PLUS:
            case MPIIO_SIZE_WRITE_AGG_0_100:
            case MPIIO_SIZE_WRITE_AGG_100_1K:
            case MPIIO_SIZE_WRITE_AGG_1K_10K:
            case MPIIO_SIZE_WRITE_AGG_10K_100K:
            case MPIIO_SIZE_WRITE_AGG_100K_1M:
            case MPIIO_SIZE_WRITE_AGG_1M_4M:
            case MPIIO_SIZE_WRITE_AGG_4M_10M:
            case MPIIO_SIZE_WRITE_AGG_10M_100M:
            case MPIIO_SIZE_WRITE_AGG_100M_1G:
            case MPIIO_SIZE_WRITE_AGG_1G_PLUS:
                /* sum */
                agg_mpi_rec->counters[i] += mpi_rec->counters[i];
                break;
            case MPIIO_MODE:
                /* just set to the input value */
                agg_mpi_rec->counters[i] = mpi_rec->counters[i];
                break;
            case MPIIO_MAX_READ_TIME_SIZE:
            case MPIIO_MAX_WRITE_TIME_SIZE:
            case MPIIO_FASTEST_RANK:
            case MPIIO_FASTEST_RANK_BYTES:
            case MPIIO_SLOWEST_RANK:
            case MPIIO_SLOWEST_RANK_BYTES:
                /* these are set with the FP counters */
                break;
            case MPIIO_ACCESS1_ACCESS:
                /* increment common value counters */
                for(j = i; j < i + 4; j++)
                {
                    min = agg_mpi_rec->counters[i + 4];
                    min_ndx = 0;
                    set = 0;
                    for(k = 0; k < 4; k++)
                    {
                        if(agg_mpi_rec->counters[i + k] == mpi_rec->counters[j])
                        {
                            agg_mpi_rec->counters[i + k + 4] += mpi_rec->counters[j + 4];
                            set = 1;
                            break;
                        }
                        else if(agg_mpi_rec->counters[i + k + 4] < min)
                        {
                            min = agg_mpi_rec->counters[i + k + 4];
                            min_ndx = k;
                        }
                    }
                    if(!set && (mpi_rec->counters[j + 4] > min))
                    {
                        agg_mpi_rec->counters[i + min_ndx] = mpi_rec->counters[j];
                        agg_mpi_rec->counters[i + min_ndx + 4] = mpi_rec->counters[j + 4];
                    }
                }
                break;
            case MPIIO_ACCESS2_ACCESS:
            case MPIIO_ACCESS3_ACCESS:
            case MPIIO_ACCESS4_ACCESS:
            case MPIIO_ACCESS1_COUNT:
            case MPIIO_ACCESS2_COUNT:
            case MPIIO_ACCESS3_COUNT:
            case MPIIO_ACCESS4_COUNT:
                /* these are set all at once with common counters above */
                break;
            default:
                agg_mpi_rec->counters[i] = -1;
                break;
        }
    }

    for(i = 0; i < MPIIO_F_NUM_INDICES; i++)
    {
        switch(i)
        {
            case MPIIO_F_READ_TIME:
            case MPIIO_F_WRITE_TIME:
            case MPIIO_F_META_TIME:
                /* sum */
                agg_mpi_rec->fcounters[i] += mpi_rec->fcounters[i];
                break;
            case MPIIO_F_OPEN_TIMESTAMP:
            case MPIIO_F_READ_START_TIMESTAMP:
            case MPIIO_F_WRITE_START_TIMESTAMP:
                /* minimum non-zero */
                if((mpi_rec->fcounters[i] > 0)  &&
                    ((agg_mpi_rec->fcounters[i] == 0) ||
                    (mpi_rec->fcounters[i] < agg_mpi_rec->fcounters[i])))
                {
                    agg_mpi_rec->fcounters[i] = mpi_rec->fcounters[i];
                }
                break;
            case MPIIO_F_READ_END_TIMESTAMP:
            case MPIIO_F_WRITE_END_TIMESTAMP:
            case MPIIO_F_CLOSE_TIMESTAMP:
                /* maximum */
                if(mpi_rec->fcounters[i] > agg_mpi_rec->fcounters[i])
                {
                    agg_mpi_rec->fcounters[i] = mpi_rec->fcounters[i];
                }
                break;
            case MPIIO_F_MAX_READ_TIME:
                if(mpi_rec->fcounters[i] > agg_mpi_rec->fcounters[i])
                {
                    agg_mpi_rec->fcounters[i] = mpi_rec->fcounters[i];
                    agg_mpi_rec->counters[MPIIO_MAX_READ_TIME_SIZE] =
                        mpi_rec->counters[MPIIO_MAX_READ_TIME_SIZE];
                }
                break;
            case MPIIO_F_MAX_WRITE_TIME:
                if(mpi_rec->fcounters[i] > agg_mpi_rec->fcounters[i])
                {
                    agg_mpi_rec->fcounters[i] = mpi_rec->fcounters[i];
                    agg_mpi_rec->counters[MPIIO_MAX_WRITE_TIME_SIZE] =
                        mpi_rec->counters[MPIIO_MAX_WRITE_TIME_SIZE];
                }
                break;
            case MPIIO_F_FASTEST_RANK_TIME:
                if(mpi_time < agg_mpi_rec->fcounters[MPIIO_F_FASTEST_RANK_TIME])
                {
                    agg_mpi_rec->counters[MPIIO_FASTEST_RANK] = mpi_rec->base_rec.rank;
                    agg_mpi_rec->counters[MPIIO_FASTEST_RANK_BYTES] =
                        mpi_rec->counters[MPIIO_BYTES_READ] +
                        mpi_rec->counters[MPIIO_BYTES_WRITTEN];
                    agg_mpi_rec->fcounters[MPIIO_F_FASTEST_RANK_TIME] = mpi_time;
                }
                break;
            case MPIIO_F_SLOWEST_RANK_TIME:
                if(mpi_time > agg_mpi_rec->fcounters[MPIIO_F_SLOWEST_RANK_TIME])
                {
                    agg_mpi_rec->counters[MPIIO_SLOWEST_RANK] = mpi_rec->base_rec.rank;
                    agg_mpi_rec->counters[MPIIO_SLOWEST_RANK_BYTES] =
                        mpi_rec->counters[MPIIO_BYTES_READ] +
                        mpi_rec->counters[MPIIO_BYTES_WRITTEN];
                    agg_mpi_rec->fcounters[MPIIO_F_SLOWEST_RANK_TIME] = mpi_time;
                }
                break;
            default:
                /* TODO: variance */
                agg_mpi_rec->fcounters[i] = -1;
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
