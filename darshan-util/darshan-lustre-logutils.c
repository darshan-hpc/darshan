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

#include "darshan-logutils.h"

/* counter name strings for the LUSTRE module */
#define X(a) #a,
char *lustre_counter_names[] = {
    LUSTRE_COUNTERS
};
#undef X

static int darshan_log_get_lustre_record(darshan_fd fd, void* lustre_buf,
    darshan_record_id* rec_id);
static int darshan_log_put_lustre_record(darshan_fd fd, void* lustre_buf, int ver);
static void darshan_log_print_lustre_record(void *file_rec,
    char *file_name, char *mnt_pt, char *fs_type, int ver);
static void darshan_log_print_lustre_description(void);
static void darshan_log_print_lustre_record_diff(void *rec1, char *file_name1,
    void *rec2, char *file_name2);

struct darshan_mod_logutil_funcs lustre_logutils =
{
    .log_get_record = &darshan_log_get_lustre_record,
    .log_put_record = &darshan_log_put_lustre_record,
    .log_print_record = &darshan_log_print_lustre_record,
    .log_print_description = &darshan_log_print_lustre_description,
    .log_print_diff = &darshan_log_print_lustre_record_diff
};

static int darshan_log_get_lustre_record(darshan_fd fd, void* lustre_buf,
    darshan_record_id* rec_id)
{
    struct darshan_lustre_record *rec;
    int i;
    int ret;

    ret = darshan_log_getmod(fd, DARSHAN_LUSTRE_MOD, lustre_buf,
        sizeof(struct darshan_lustre_record));
    if(ret < 0)
        return(-1);
    else if(ret < sizeof(struct darshan_lustre_record))
        return(0);
    else
    {
        rec = (struct darshan_lustre_record *)lustre_buf;
        if(fd->swap_flag)
        {
            /* swap bytes if necessary */
            DARSHAN_BSWAP64(&rec->rec_id);
            DARSHAN_BSWAP64(&rec->rank);
            for(i=0; i<LUSTRE_NUM_INDICES; i++)
                DARSHAN_BSWAP64(&rec->counters[i]);
        }

        *rec_id = rec->rec_id;
        return(1);
    }
}

static int darshan_log_put_lustre_record(darshan_fd fd, void* lustre_buf, int ver)
{
    struct darshan_lustre_record *rec = (struct darshan_lustre_record *)lustre_buf;
    int ret;

    ret = darshan_log_putmod(fd, DARSHAN_LUSTRE_MOD, rec,
        sizeof(struct darshan_lustre_record), ver);
    if(ret < 0)
        return(-1);

    return(0);
}

static void darshan_log_print_lustre_record(void *rec, char *file_name,
    char *mnt_pt, char *fs_type, int ver)
{
    int i;
    struct darshan_lustre_record *lustre_rec =
        (struct darshan_lustre_record *)rec;

    for(i=0; i<LUSTRE_NUM_INDICES; i++)
    {
        DARSHAN_COUNTER_PRINT(darshan_module_names[DARSHAN_LUSTRE_MOD],
            lustre_rec->rank, lustre_rec->rec_id, lustre_counter_names[i],
            lustre_rec->counters[i], file_name, mnt_pt, fs_type);
    }

    return;
}

static void darshan_log_print_lustre_description()
{
    /* TODO: add actual counter descriptions here */
    printf("\n# description of LUSTRE counters:\n");
    printf("#   LUSTRE_OSTS: number of OSTs across the entire file system.\n");
    printf("#   LUSTRE_MDTS: number of MDTs across the entire file system.\n");
    printf("#   LUSTRE_STRIPE_SIZE: stripe size for file in bytes.\n");
    printf("#   LUSTRE_STRIPE_WIDTH: number of OSTs over which file is striped.\n");
    printf("#   LUSTRE_STRIPE_OFFSET: OBD index of the file's first stripe.\n");

    DARSHAN_PRINT_HEADER();

    return;
}

static void darshan_log_print_lustre_record_diff(void *rec1, char *file_name1,
    void *rec2, char *file_name2)
{
    struct darshan_lustre_record *lustre_rec1 = (struct darshan_lustre_record *)rec1;
    struct darshan_lustre_record *lustre_rec2 = (struct darshan_lustre_record *)rec2;
    int i;

    /* NOTE: we assume that both input records are the same module format version */

    for(i=0; i<LUSTRE_NUM_INDICES; i++)
    {
        if(!lustre_rec2)
        {
            printf("- ");
            DARSHAN_COUNTER_PRINT(darshan_module_names[DARSHAN_LUSTRE_MOD],
                lustre_rec1->rank, lustre_rec1->rec_id, lustre_counter_names[i],
                lustre_rec1->counters[i], file_name1, "", "");

        }
        else if(!lustre_rec1)
        {
            printf("+ ");
            DARSHAN_COUNTER_PRINT(darshan_module_names[DARSHAN_LUSTRE_MOD],
                lustre_rec2->rank, lustre_rec2->rec_id, lustre_counter_names[i],
                lustre_rec2->counters[i], file_name2, "", "");
        }
        else if(lustre_rec1->counters[i] != lustre_rec2->counters[i])
        {
            printf("- ");
            DARSHAN_COUNTER_PRINT(darshan_module_names[DARSHAN_LUSTRE_MOD],
                lustre_rec1->rank, lustre_rec1->rec_id, lustre_counter_names[i],
                lustre_rec1->counters[i], file_name1, "", "");
            printf("+ ");
            DARSHAN_COUNTER_PRINT(darshan_module_names[DARSHAN_LUSTRE_MOD],
                lustre_rec2->rank, lustre_rec2->rec_id, lustre_counter_names[i],
                lustre_rec2->counters[i], file_name2, "", "");
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
