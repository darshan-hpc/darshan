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

/* counter name strings for the BGQ module */
#define X(a) #a,
char *bgq_counter_names[] = {
    BGQ_COUNTERS
};

char *bgq_f_counter_names[] = {
    BGQ_F_COUNTERS
};
#undef X

/* NOTE:
 */
#define DARSHAN_BGQ_FILE_SIZE_1 (112 + 8)

static int darshan_log_get_bgq_rec(darshan_fd fd, void** bgq_buf_p);
static int darshan_log_put_bgq_rec(darshan_fd fd, void* bgq_buf);
static void darshan_log_print_bgq_rec(void *file_rec,
    char *file_name, char *mnt_pt, char *fs_type);
static void darshan_log_print_bgq_description(int ver);
static void darshan_log_print_bgq_rec_diff(void *file_rec1, char *file_name1,
    void *file_rec2, char *file_name2);
static void darshan_log_agg_bgq_recs(void *rec, void *agg_rec, int init_flag);

struct darshan_mod_logutil_funcs bgq_logutils =
{
    .log_get_record = &darshan_log_get_bgq_rec,
    .log_put_record = &darshan_log_put_bgq_rec,
    .log_print_record = &darshan_log_print_bgq_rec,
    .log_print_description = &darshan_log_print_bgq_description,
    .log_print_diff = &darshan_log_print_bgq_rec_diff,
    .log_agg_records = &darshan_log_agg_bgq_recs
};

static int darshan_log_get_bgq_rec(darshan_fd fd, void** bgq_buf_p)
{
    struct darshan_bgq_record *rec = *((struct darshan_bgq_record **)bgq_buf_p);
    int rec_len;
    char *buffer, *p;
    int i;
    int ret = -1;

    if(fd->mod_map[DARSHAN_BGQ_MOD].len == 0)
        return(0);

    if(*bgq_buf_p == NULL)
    {
        rec = malloc(sizeof(*rec));
        if(!rec)
            return(-1);
    }

    /* read the BGQ record from file, checking the version first so we
     * can read it correctly
     */
    if(fd->mod_ver[DARSHAN_BGQ_MOD] == 1)
    {
        buffer = malloc(DARSHAN_BGQ_FILE_SIZE_1);
        if(!buffer)
        {
            if(*bgq_buf_p == NULL)
                free(rec);
            return(-1);
        }

        rec_len = DARSHAN_BGQ_FILE_SIZE_1;
        ret = darshan_log_get_mod(fd, DARSHAN_BGQ_MOD, buffer, rec_len);
        if(ret > 0)
        {
            /* up-convert old BGQ format to new format */
            p = buffer;
            memcpy(&(rec->base_rec), p, sizeof(struct darshan_base_record));
            /* skip however long int+padding is */
            p += (rec_len - (BGQ_NUM_INDICES * sizeof(int64_t)) -
                (BGQ_F_NUM_INDICES * sizeof(double)));
            memcpy(&(rec->counters[0]), p, BGQ_NUM_INDICES * sizeof(int64_t));
            p += (BGQ_NUM_INDICES * sizeof(int64_t));
            memcpy(&(rec->fcounters[0]), p, BGQ_F_NUM_INDICES * sizeof(double));
            ret = rec_len;
        }
        free(buffer);
    }
    else if(fd->mod_ver[DARSHAN_BGQ_MOD] == 2)
    {
        rec_len = sizeof(struct darshan_bgq_record);
        ret = darshan_log_get_mod(fd, DARSHAN_BGQ_MOD, rec, rec_len);
    }

    if(*bgq_buf_p == NULL)
    {
        if(ret == rec_len)
            *bgq_buf_p = rec;
        else
            free(rec);
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
            DARSHAN_BSWAP64(&(rec->base_rec.id));
            DARSHAN_BSWAP64(&(rec->base_rec.rank));
            for(i=0; i<BGQ_NUM_INDICES; i++)
                DARSHAN_BSWAP64(&rec->counters[i]);
            for(i=0; i<BGQ_F_NUM_INDICES; i++)
                DARSHAN_BSWAP64(&rec->fcounters[i]);
        }

        return(1);
    }
}

static int darshan_log_put_bgq_rec(darshan_fd fd, void* bgq_buf)
{
    struct darshan_bgq_record *rec = (struct darshan_bgq_record *)bgq_buf;
    int ret;

    ret = darshan_log_put_mod(fd, DARSHAN_BGQ_MOD, rec,
        sizeof(struct darshan_bgq_record), DARSHAN_BGQ_VER);
    if(ret < 0)
        return(-1);

    return(0);
}

static void darshan_log_print_bgq_rec(void *file_rec, char *file_name,
    char *mnt_pt, char *fs_type)
{
    int i;
    struct darshan_bgq_record *bgq_file_rec =
        (struct darshan_bgq_record *)file_rec;

    for(i=0; i<BGQ_NUM_INDICES; i++)
    {
        DARSHAN_D_COUNTER_PRINT(darshan_module_names[DARSHAN_BGQ_MOD],
            bgq_file_rec->base_rec.rank, bgq_file_rec->base_rec.id,
            bgq_counter_names[i], bgq_file_rec->counters[i],
            file_name, mnt_pt, fs_type);
    }

    for(i=0; i<BGQ_F_NUM_INDICES; i++)
    {
        DARSHAN_F_COUNTER_PRINT(darshan_module_names[DARSHAN_BGQ_MOD],
            bgq_file_rec->base_rec.rank, bgq_file_rec->base_rec.id,
            bgq_f_counter_names[i], bgq_file_rec->fcounters[i],
            file_name, mnt_pt, fs_type);
    }

    return;
}

static void darshan_log_print_bgq_description(int ver)
{
    printf("\n# description of BGQ counters:\n");
    printf("#   BGQ_CSJOBID: BGQ control system job ID.\n");
    printf("#   BGQ_NNODES: number of BGQ compute nodes for this job.\n");
    printf("#   BGQ_RANKSPERNODE: number of MPI ranks per compute node.\n");
    printf("#   BGQ_DDRPERNODE: size in MB of DDR3 per compute node.\n");
    printf("#   BGQ_INODES: number of BGQ I/O nodes for this job.\n");
    printf("#   BGQ_*NODES: dimension of A, B, C, D, & E dimensions of torus.\n");
    printf("#   BGQ_TORUSENABLED: which dimensions of the torus are enabled.\n");
    printf("#   BGQ_F_TIMESTAMP: timestamp when the BGQ data was collected.\n");

    return;
}

static void darshan_log_print_bgq_rec_diff(void *file_rec1, char *file_name1,
    void *file_rec2, char *file_name2)
{
    struct darshan_bgq_record *file1 = (struct darshan_bgq_record *)file_rec1;
    struct darshan_bgq_record *file2 = (struct darshan_bgq_record *)file_rec2;
    int i;

    /* NOTE: we assume that both input records are the same module format version */

    for(i=0; i<BGQ_NUM_INDICES; i++)
    {
        if(!file2)
        {
            printf("- ");
            DARSHAN_D_COUNTER_PRINT(darshan_module_names[DARSHAN_BGQ_MOD],
                file1->base_rec.rank, file1->base_rec.id, bgq_counter_names[i],
                file1->counters[i], file_name1, "", "");

        }
        else if(!file1)
        {
            printf("+ ");
            DARSHAN_D_COUNTER_PRINT(darshan_module_names[DARSHAN_BGQ_MOD],
                file2->base_rec.rank, file2->base_rec.id, bgq_counter_names[i],
                file2->counters[i], file_name2, "", "");
        }
        else if(file1->counters[i] != file2->counters[i])
        {
            printf("- ");
            DARSHAN_D_COUNTER_PRINT(darshan_module_names[DARSHAN_BGQ_MOD],
                file1->base_rec.rank, file1->base_rec.id, bgq_counter_names[i],
                file1->counters[i], file_name1, "", "");
            printf("+ ");
            DARSHAN_D_COUNTER_PRINT(darshan_module_names[DARSHAN_BGQ_MOD],
                file2->base_rec.rank, file2->base_rec.id, bgq_counter_names[i],
                file2->counters[i], file_name2, "", "");
        }
    }

    for(i=0; i<BGQ_F_NUM_INDICES; i++)
    {
        if(!file2)
        {
            printf("- ");
            DARSHAN_F_COUNTER_PRINT(darshan_module_names[DARSHAN_BGQ_MOD],
                file1->base_rec.rank, file1->base_rec.id, bgq_f_counter_names[i],
                file1->fcounters[i], file_name1, "", "");

        }
        else if(!file1)
        {
            printf("+ ");
            DARSHAN_F_COUNTER_PRINT(darshan_module_names[DARSHAN_BGQ_MOD],
                file2->base_rec.rank, file2->base_rec.id, bgq_f_counter_names[i],
                file2->fcounters[i], file_name2, "", "");
        }
        else if(file1->fcounters[i] != file2->fcounters[i])
        {
            printf("- ");
            DARSHAN_F_COUNTER_PRINT(darshan_module_names[DARSHAN_BGQ_MOD],
                file1->base_rec.rank, file1->base_rec.id, bgq_f_counter_names[i],
                file1->fcounters[i], file_name1, "", "");
            printf("+ ");
            DARSHAN_F_COUNTER_PRINT(darshan_module_names[DARSHAN_BGQ_MOD],
                file2->base_rec.rank, file2->base_rec.id, bgq_f_counter_names[i],
                file2->fcounters[i], file_name2, "", "");
        }
    }

    return;
}


static void darshan_log_agg_bgq_recs(void *rec, void *agg_rec, int init_flag)
{
    struct darshan_bgq_record *bgq_rec = (struct darshan_bgq_record *)rec;
    struct darshan_bgq_record *agg_bgq_rec = (struct darshan_bgq_record *)agg_rec;
    int i;

    if(init_flag)
    {
        /* when initializing, just copy over the first record */
        memcpy(agg_bgq_rec, bgq_rec, sizeof(struct darshan_bgq_record));

        /* TODO: each record stores the ID of the corresponding rank's BG/Q
         * inode. Currently, this log aggregation interface assumes we can
         * aggregate logs one at a time, without having to know the value of
         * a counter on all processes. What we need here is a way to determine
         * the inode IDs used for every process, filter out duplicates, then
         * count the total number to set this counter. Will have to think
         * more about how we can calculate this value using this interface
         */
        agg_bgq_rec->counters[BGQ_INODES] = -1;
    }
    else
    {
        /* for remaining records, just sanity check the records are identical */
        for(i = 0; i < BGQ_NUM_INDICES; i++)
        {
            /* TODO: ignore BGQ_INODES counter since it might be different in
             * each record (more details in note above)
             */
            if(i == BGQ_INODES)
                continue;
            assert(bgq_rec->counters[i] == agg_bgq_rec->counters[i]);
        }

        /* NOTE: ignore BGQ_F_TIMESTAMP counter -- just use the value from the
         * record that we initialized with
         */
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
