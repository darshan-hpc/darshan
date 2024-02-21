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

struct darshan_mod_logutil_funcs dfs_logutils =
{
    .log_get_record = &darshan_log_get_dfs_file,
    .log_put_record = &darshan_log_put_dfs_file,
    .log_print_record = &darshan_log_print_dfs_file,
    .log_print_description = &darshan_log_print_dfs_description,
    .log_print_diff = &darshan_log_print_dfs_file_diff,
    .log_agg_records = &darshan_log_agg_dfs_files,
};

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
    char pool_cont_uuid_str[128];

    uuid_unparse(dfs_file_rec->pool_uuid, pool_cont_uuid_str);
    strcat(pool_cont_uuid_str, ":");
    uuid_unparse(dfs_file_rec->cont_uuid, pool_cont_uuid_str+strlen(pool_cont_uuid_str));

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
    printf("#   OPENS,LOOKUPS,DUPS,READS,READXS,NB_READS,WRITES,WRITEXS,NB_WRITES,GET_SIZES,PUNCHES,MOVES,EXCHANGES,STATS are types of operations.\n");
    printf("#   DFS_F_*_START_TIMESTAMP: timestamp of first open/read/write/close.\n");
    printf("#   DFS_F_*_END_TIMESTAMP: timestamp of last open/read/write/close.\n");
    printf("#   DFS_F_READ/WRITE/META_TIME: cumulative time spent in read, write, or metadata operations.\n");
    printf("#   DFS_BYTES_*: total bytes read and written.\n");
    printf("#   DFS_RW_SWITCHES: number of times access alternated between read and write.\n");
    printf("#   DFS_CHUNK_SIZE: DFS file chunk size.\n");
    printf("#   DFS_USE_DTX: whether DFS distributed transactions are used.\n");
    printf("#   DFS_MAX_*_TIME_SIZE: size of the slowest read and write operations.\n");
    printf("#   DFS_SIZE_*_*: histogram of read and write access sizes.\n");
    printf("#   DFS_ACCESS*_ACCESS: the four most common access sizes.\n");
    printf("#   DFS_ACCESS*_COUNT: count of the four most common access sizes.\n");
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
    /* XXX */
    return;
}

static void darshan_log_agg_dfs_files(void *rec, void *agg_rec, int init_flag)
{
    /* XXX */
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
