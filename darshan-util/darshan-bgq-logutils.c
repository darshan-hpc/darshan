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

/* counter name strings for the BGQ module */
#define X(a) #a,
char *bgq_counter_names[] = {
    BGQ_COUNTERS
};

char *bgq_f_counter_names[] = {
    BGQ_F_COUNTERS
};
#undef X

static int darshan_log_get_bgq_rec(darshan_fd fd, void* bgq_buf);
static int darshan_log_put_bgq_rec(darshan_fd fd, void* bgq_buf, int ver);
static void darshan_log_print_bgq_rec(void *file_rec,
    char *file_name, char *mnt_pt, char *fs_type, int ver);
static void darshan_log_print_bgq_description(void);
static void darshan_log_print_bgq_rec_diff(void *file_rec1, char *file_name1,
    void *file_rec2, char *file_name2);

struct darshan_mod_logutil_funcs bgq_logutils =
{
    .log_get_record = &darshan_log_get_bgq_rec,
    .log_put_record = &darshan_log_put_bgq_rec,
    .log_print_record = &darshan_log_print_bgq_rec,
    .log_print_description = &darshan_log_print_bgq_description,
    .log_print_diff = &darshan_log_print_bgq_rec_diff,
    .log_agg_records = NULL /* TODO: how would aggregation work for the BG/Q module ? */
};

static int darshan_log_get_bgq_rec(darshan_fd fd, void* bgq_buf)
{
    struct darshan_bgq_record *rec;
    int i;
    int ret;

    ret = darshan_log_getmod(fd, DARSHAN_BGQ_MOD, bgq_buf,
        sizeof(struct darshan_bgq_record));
    if(ret < 0)
        return(-1);
    else if(ret < sizeof(struct darshan_bgq_record))
        return(0);
    else
    {
        rec = (struct darshan_bgq_record *)bgq_buf;
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

static int darshan_log_put_bgq_rec(darshan_fd fd, void* bgq_buf, int ver)
{
    struct darshan_bgq_record *rec = (struct darshan_bgq_record *)bgq_buf;
    int ret;

    ret = darshan_log_putmod(fd, DARSHAN_BGQ_MOD, rec,
        sizeof(struct darshan_bgq_record), ver);
    if(ret < 0)
        return(-1);

    return(0);
}

static void darshan_log_print_bgq_rec(void *file_rec, char *file_name,
    char *mnt_pt, char *fs_type, int ver)
{
    int i;
    struct darshan_bgq_record *bgq_file_rec =
        (struct darshan_bgq_record *)file_rec;

    for(i=0; i<BGQ_NUM_INDICES; i++)
    {
        DARSHAN_COUNTER_PRINT(darshan_module_names[DARSHAN_BGQ_MOD],
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

static void darshan_log_print_bgq_description()
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

    DARSHAN_PRINT_HEADER();

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
            DARSHAN_COUNTER_PRINT(darshan_module_names[DARSHAN_BGQ_MOD],
                file1->rank, file1->f_id, bgq_counter_names[i],
                file1->counters[i], file_name1, "", "");

        }
        else if(!file1)
        {
            printf("+ ");
            DARSHAN_COUNTER_PRINT(darshan_module_names[DARSHAN_BGQ_MOD],
                file2->rank, file2->f_id, bgq_counter_names[i],
                file2->counters[i], file_name2, "", "");
        }
        else if(file1->counters[i] != file2->counters[i])
        {
            printf("- ");
            DARSHAN_COUNTER_PRINT(darshan_module_names[DARSHAN_BGQ_MOD],
                file1->rank, file1->f_id, bgq_counter_names[i],
                file1->counters[i], file_name1, "", "");
            printf("+ ");
            DARSHAN_COUNTER_PRINT(darshan_module_names[DARSHAN_BGQ_MOD],
                file2->rank, file2->f_id, bgq_counter_names[i],
                file2->counters[i], file_name2, "", "");
        }
    }

    for(i=0; i<BGQ_F_NUM_INDICES; i++)
    {
        if(!file2)
        {
            printf("- ");
            DARSHAN_F_COUNTER_PRINT(darshan_module_names[DARSHAN_BGQ_MOD],
                file1->rank, file1->f_id, bgq_f_counter_names[i],
                file1->fcounters[i], file_name1, "", "");

        }
        else if(!file1)
        {
            printf("+ ");
            DARSHAN_F_COUNTER_PRINT(darshan_module_names[DARSHAN_BGQ_MOD],
                file2->rank, file2->f_id, bgq_f_counter_names[i],
                file2->fcounters[i], file_name2, "", "");
        }
        else if(file1->fcounters[i] != file2->fcounters[i])
        {
            printf("- ");
            DARSHAN_F_COUNTER_PRINT(darshan_module_names[DARSHAN_BGQ_MOD],
                file1->rank, file1->f_id, bgq_f_counter_names[i],
                file1->fcounters[i], file_name1, "", "");
            printf("+ ");
            DARSHAN_F_COUNTER_PRINT(darshan_module_names[DARSHAN_BGQ_MOD],
                file2->rank, file2->f_id, bgq_f_counter_names[i],
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
