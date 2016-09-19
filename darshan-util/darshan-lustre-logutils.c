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

static int darshan_log_get_lustre_record(darshan_fd fd, void** lustre_buf_p);
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

static int darshan_log_get_lustre_record(darshan_fd fd, void** lustre_buf_p)
{
    struct darshan_lustre_record *rec = *((struct darshan_lustre_record **)lustre_buf_p);
    struct darshan_lustre_record tmp_rec;
    int i;
    int ret;

    if(fd->mod_map[DARSHAN_LUSTRE_MOD].len == 0)
        return(0);

    /* retrieve the fixed-size portion of the record */
    ret = darshan_log_get_mod(fd, DARSHAN_LUSTRE_MOD, &tmp_rec,
        sizeof(struct darshan_lustre_record));
    if(ret < 0)
        return(-1);
    else if(ret < sizeof(struct darshan_lustre_record))
        return(0);

    if(*lustre_buf_p == NULL)
    {
        rec = malloc(LUSTRE_RECORD_SIZE(tmp_rec.counters[LUSTRE_STRIPE_WIDTH]));
        if(!rec)
            return(-1);
    }
    memcpy(rec, &tmp_rec, sizeof(struct darshan_lustre_record));

    /* swap bytes if necessary */
    if(fd->swap_flag)
    {
        DARSHAN_BSWAP64(&rec->base_rec.id);
        DARSHAN_BSWAP64(&rec->base_rec.rank);
        for(i=0; i<LUSTRE_NUM_INDICES; i++)
            DARSHAN_BSWAP64(&rec->counters[i]);
        DARSHAN_BSWAP64(&(rec->ost_ids[0]));
    }

    /* now read the rest of the record */
    if ( rec->counters[LUSTRE_STRIPE_WIDTH] > 1 ) {
        ret = darshan_log_get_mod(
            fd,
            DARSHAN_LUSTRE_MOD,
            (void*)(&(rec->ost_ids[1])),
            (rec->counters[LUSTRE_STRIPE_WIDTH] - 1)*sizeof(OST_ID)
        );
        if(ret < 0)
            ret = -1;
        else if(ret < (rec->counters[LUSTRE_STRIPE_WIDTH] - 1)*sizeof(OST_ID))
            ret = 0;
        else
            ret = 1;
        /* swap bytes if necessary */
        if ( fd->swap_flag )
            for (i = 1; i < rec->counters[LUSTRE_STRIPE_WIDTH]; i++ )
                DARSHAN_BSWAP64(&(rec->ost_ids[i]));
    }
    else
    {
        ret = 1;
    }

    if(*lustre_buf_p == NULL)
    {
        if(ret == 1)
            *lustre_buf_p = rec;
        else
            free(rec);
    }

    return(ret);
}

static int darshan_log_put_lustre_record(darshan_fd fd, void* lustre_buf, int ver)
{
    struct darshan_lustre_record *rec = (struct darshan_lustre_record *)lustre_buf;
    int ret;

    ret = darshan_log_put_mod(fd, DARSHAN_LUSTRE_MOD, rec,
        LUSTRE_RECORD_SIZE(rec->counters[LUSTRE_STRIPE_WIDTH]), DARSHAN_LUSTRE_VER);
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
            lustre_rec->base_rec.rank, lustre_rec->base_rec.id, lustre_counter_names[i],
            lustre_rec->counters[i], file_name, mnt_pt, fs_type);
    }

    for (i = 0; i < lustre_rec->counters[LUSTRE_STRIPE_WIDTH]; i++ )
    {
        char strbuf[25];
        snprintf( strbuf, 25, "LUSTRE_OST_ID_%d", i );
        DARSHAN_COUNTER_PRINT(darshan_module_names[DARSHAN_LUSTRE_MOD],
            lustre_rec->base_rec.rank,
            lustre_rec->base_rec.id,
            strbuf,
            lustre_rec->ost_ids[i],
            file_name,
            mnt_pt,
            fs_type);
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
    printf("#   LUSTRE_STRIPE_OFFSET: OST ID offset specified when the file was created.\n");
    printf("#   LUSTRE_OST_ID_*: indices of OSTs over which the file is striped.\n");

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
                lustre_rec1->base_rec.rank, lustre_rec1->base_rec.id,
                lustre_counter_names[i], lustre_rec1->counters[i], file_name1, "", "");

        }
        else if(!lustre_rec1)
        {
            printf("+ ");
            DARSHAN_COUNTER_PRINT(darshan_module_names[DARSHAN_LUSTRE_MOD],
                lustre_rec2->base_rec.rank, lustre_rec2->base_rec.id,
                lustre_counter_names[i], lustre_rec2->counters[i], file_name2, "", "");
        }
        else if(lustre_rec1->counters[i] != lustre_rec2->counters[i])
        {
            printf("- ");
            DARSHAN_COUNTER_PRINT(darshan_module_names[DARSHAN_LUSTRE_MOD],
                lustre_rec1->base_rec.rank, lustre_rec1->base_rec.id,
                lustre_counter_names[i], lustre_rec1->counters[i], file_name1, "", "");
            printf("+ ");
            DARSHAN_COUNTER_PRINT(darshan_module_names[DARSHAN_LUSTRE_MOD],
                lustre_rec2->base_rec.rank, lustre_rec2->base_rec.id,
                lustre_counter_names[i], lustre_rec2->counters[i], file_name2, "", "");
        }
    }

    /* would it be more or less useful to sort the OST IDs before comparing? */
    if ( lustre_rec1->counters[LUSTRE_STRIPE_WIDTH] == lustre_rec2->counters[LUSTRE_STRIPE_WIDTH] ) {
        for (i = 0; i < lustre_rec1->counters[LUSTRE_STRIPE_WIDTH]; i++ )
        {
            if (lustre_rec1->ost_ids[i] != lustre_rec2->ost_ids[i])
            {
                char strbuf[25];
                snprintf( strbuf, 25, "LUSTRE_OST_ID_%d", i );
                printf("- ");
                DARSHAN_COUNTER_PRINT(darshan_module_names[DARSHAN_LUSTRE_MOD],
                    lustre_rec1->base_rec.rank,
                    lustre_rec1->base_rec.id,
                    strbuf,
                    lustre_rec1->ost_ids[i],
                    file_name1,
                    "",
                    "");
                printf("+ ");
                DARSHAN_COUNTER_PRINT(darshan_module_names[DARSHAN_LUSTRE_MOD],
                    lustre_rec2->base_rec.rank,
                    lustre_rec2->base_rec.id,
                    strbuf,
                    lustre_rec2->ost_ids[i],
                    file_name2,
                    "",
                    "");
            }
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
