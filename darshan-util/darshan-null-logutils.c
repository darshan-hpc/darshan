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

/* integer counter name strings for the NULL module */
#define X(a) #a,
char *null_counter_names[] = {
    NULL_COUNTERS
};

/* floating point counter name strings for the NULL module */
char *null_f_counter_names[] = {
    NULL_F_COUNTERS
};
#undef X

/* prototypes for each of the NULL module's logutil functions */
static int darshan_log_get_null_record(darshan_fd fd, void** null_buf_p);
static int darshan_log_put_null_record(darshan_fd fd, void* null_buf);
static void darshan_log_print_null_record(void *file_rec,
    char *file_name, char *mnt_pt, char *fs_type);
static void darshan_log_print_null_description(int ver);
static void darshan_log_print_null_record_diff(void *file_rec1, char *file_name1,
    void *file_rec2, char *file_name2);
static void darshan_log_agg_null_records(void *rec, void *agg_rec, int init_flag);

/* structure storing each function needed for implementing the darshan
 * logutil interface. these functions are used for reading, writing, and
 * printing module data in a consistent manner.
 */
struct darshan_mod_logutil_funcs null_logutils =
{
    .log_get_record = &darshan_log_get_null_record,
    .log_put_record = &darshan_log_put_null_record,
    .log_print_record = &darshan_log_print_null_record,
    .log_print_description = &darshan_log_print_null_description,
    .log_print_diff = &darshan_log_print_null_record_diff,
    .log_agg_records = &darshan_log_agg_null_records
};

/* retrieve a NULL record from log file descriptor 'fd', storing the
 * data in the buffer address pointed to by 'null_buf_p'. Return 1 on
 * successful record read, 0 on no more data, and -1 on error.
 */
static int darshan_log_get_null_record(darshan_fd fd, void** null_buf_p)
{
    struct darshan_null_record *rec = *((struct darshan_null_record **)null_buf_p);
    int i;
    int ret;

    if(fd->mod_map[DARSHAN_NULL_MOD].len == 0)
        return(0);

    if(fd->mod_ver[DARSHAN_NULL_MOD] == 0 ||
        fd->mod_ver[DARSHAN_NULL_MOD] > DARSHAN_NULL_VER)
    {
        fprintf(stderr, "Error: Invalid NHLL module version number (got %d)\n",
            fd->mod_ver[DARSHAN_NULL_MOD]);
        return(-1);
    }

    if(*null_buf_p == NULL)
    {
        rec = malloc(sizeof(*rec));
        if(!rec)
            return(-1);
    }

    /* read a NULL module record from the darshan log file */
    ret = darshan_log_get_mod(fd, DARSHAN_NULL_MOD, rec,
        sizeof(struct darshan_null_record));

    if(*null_buf_p == NULL)
    {
        if(ret == sizeof(struct darshan_null_record))
            *null_buf_p = rec;
        else
            free(rec);
    }

    if(ret < 0)
        return(-1);
    else if(ret < sizeof(struct darshan_null_record))
        return(0);
    else
    {
        /* if the read was successful, do any necessary byte-swapping */
        if(fd->swap_flag)
        {
            DARSHAN_BSWAP64(&(rec->base_rec.id));
            DARSHAN_BSWAP64(&(rec->base_rec.rank));
            for(i=0; i<NULL_NUM_INDICES; i++)
                DARSHAN_BSWAP64(&rec->counters[i]);
            for(i=0; i<NULL_F_NUM_INDICES; i++)
                DARSHAN_BSWAP64(&rec->fcounters[i]);
        }

        return(1);
    }
}

/* write the NULL record stored in 'null_buf' to log file descriptor 'fd'.
 * Return 0 on success, -1 on failure
 */
static int darshan_log_put_null_record(darshan_fd fd, void* null_buf)
{
    struct darshan_null_record *rec = (struct darshan_null_record *)null_buf;
    int ret;

    /* append NULL record to darshan log file */
    ret = darshan_log_put_mod(fd, DARSHAN_NULL_MOD, rec,
        sizeof(struct darshan_null_record), DARSHAN_NULL_VER);
    if(ret < 0)
        return(-1);

    return(0);
}

/* print all I/O data record statistics for the given NULL record */
static void darshan_log_print_null_record(void *file_rec, char *file_name,
    char *mnt_pt, char *fs_type)
{
    int i;
    struct darshan_null_record *null_rec =
        (struct darshan_null_record *)file_rec;

    /* print each of the integer and floating point counters for the NULL module */
    for(i=0; i<NULL_NUM_INDICES; i++)
    {
        /* macro defined in darshan-logutils.h */
        DARSHAN_D_COUNTER_PRINT(darshan_module_names[DARSHAN_NULL_MOD],
            null_rec->base_rec.rank, null_rec->base_rec.id,
            null_counter_names[i], null_rec->counters[i],
            file_name, mnt_pt, fs_type);
    }

    for(i=0; i<NULL_F_NUM_INDICES; i++)
    {
        /* macro defined in darshan-logutils.h */
        DARSHAN_F_COUNTER_PRINT(darshan_module_names[DARSHAN_NULL_MOD],
            null_rec->base_rec.rank, null_rec->base_rec.id,
            null_f_counter_names[i], null_rec->fcounters[i],
            file_name, mnt_pt, fs_type);
    }

    return;
}

/* print out a description of the NULL module record fields */
static void darshan_log_print_null_description(int ver)
{
    printf("\n# description of NULL counters:\n");
    printf("#   NULL_FOOS: number of 'foo' function calls.\n");
    printf("#   NULL_FOO_MAX_DAT: maximum data value set by calls to 'foo'.\n");
    printf("#   NULL_F_FOO_TIMESTAMP: timestamp of the first call to function 'foo'.\n");
    printf("#   NULL_F_FOO_MAX_DURATION: timer indicating duration of call to 'foo' with max NULL_FOO_MAX_DAT value.\n");

    return;
}

/* print a diff of two NULL records (with the same record id) */
static void darshan_log_print_null_record_diff(void *file_rec1, char *file_name1,
    void *file_rec2, char *file_name2)
{
    struct darshan_null_record *file1 = (struct darshan_null_record *)file_rec1;
    struct darshan_null_record *file2 = (struct darshan_null_record *)file_rec2;
    int i;

    /* NOTE: we assume that both input records are the same module format version */

    for(i=0; i<NULL_NUM_INDICES; i++)
    {
        if(!file2)
        {
            printf("- ");
            DARSHAN_D_COUNTER_PRINT(darshan_module_names[DARSHAN_NULL_MOD],
                file1->base_rec.rank, file1->base_rec.id, null_counter_names[i],
                file1->counters[i], file_name1, "", "");

        }
        else if(!file1)
        {
            printf("+ ");
            DARSHAN_D_COUNTER_PRINT(darshan_module_names[DARSHAN_NULL_MOD],
                file2->base_rec.rank, file2->base_rec.id, null_counter_names[i],
                file2->counters[i], file_name2, "", "");
        }
        else if(file1->counters[i] != file2->counters[i])
        {
            printf("- ");
            DARSHAN_D_COUNTER_PRINT(darshan_module_names[DARSHAN_NULL_MOD],
                file1->base_rec.rank, file1->base_rec.id, null_counter_names[i],
                file1->counters[i], file_name1, "", "");
            printf("+ ");
            DARSHAN_D_COUNTER_PRINT(darshan_module_names[DARSHAN_NULL_MOD],
                file2->base_rec.rank, file2->base_rec.id, null_counter_names[i],
                file2->counters[i], file_name2, "", "");
        }
    }

    for(i=0; i<NULL_F_NUM_INDICES; i++)
    {
        if(!file2)
        {
            printf("- ");
            DARSHAN_F_COUNTER_PRINT(darshan_module_names[DARSHAN_NULL_MOD],
                file1->base_rec.rank, file1->base_rec.id, null_f_counter_names[i],
                file1->fcounters[i], file_name1, "", "");

        }
        else if(!file1)
        {
            printf("+ ");
            DARSHAN_F_COUNTER_PRINT(darshan_module_names[DARSHAN_NULL_MOD],
                file2->base_rec.rank, file2->base_rec.id, null_f_counter_names[i],
                file2->fcounters[i], file_name2, "", "");
        }
        else if(file1->fcounters[i] != file2->fcounters[i])
        {
            printf("- ");
            DARSHAN_F_COUNTER_PRINT(darshan_module_names[DARSHAN_NULL_MOD],
                file1->base_rec.rank, file1->base_rec.id, null_f_counter_names[i],
                file1->fcounters[i], file_name1, "", "");
            printf("+ ");
            DARSHAN_F_COUNTER_PRINT(darshan_module_names[DARSHAN_NULL_MOD],
                file2->base_rec.rank, file2->base_rec.id, null_f_counter_names[i],
                file2->fcounters[i], file_name2, "", "");
        }
    }

    return;
}

/* aggregate the input NULL record 'rec'  into the output record 'agg_rec' */
static void darshan_log_agg_null_records(void *rec, void *agg_rec, int init_flag)
{
    struct darshan_null_record *null_rec = (struct darshan_null_record *)rec;
    struct darshan_null_record *agg_null_rec = (struct darshan_null_record *)agg_rec;
    int i;

    for(i = 0; i < NULL_NUM_INDICES; i++)
    {
        switch(i)
        {
            case NULL_FOOS:
                /* sum */
                agg_null_rec->counters[i] += null_rec->counters[i];
                break;
            case NULL_FOO_MAX_DAT:
                /* max */
                if(null_rec->counters[i] > agg_null_rec->counters[i])
                {
                    agg_null_rec->counters[i] = null_rec->counters[i];
                    agg_null_rec->fcounters[NULL_F_FOO_MAX_DURATION] =
                        null_rec->fcounters[NULL_F_FOO_MAX_DURATION];
                }
                break;
            default:
                /* if we don't know how to aggregate this counter, just set to -1 */
                agg_null_rec->counters[i] = -1;
                break;
        }
    }

    for(i = 0; i < NULL_F_NUM_INDICES; i++)
    {
        switch(i)
        {
            case NULL_F_FOO_TIMESTAMP:
                /* min non-zero */
                if((null_rec->fcounters[i] > 0)  &&
                    ((agg_null_rec->fcounters[i] == 0) ||
                    (null_rec->fcounters[i] < agg_null_rec->fcounters[i])))
                {
                    agg_null_rec->fcounters[i] = null_rec->fcounters[i];
                }
                break;
            default:
                /* if we don't know how to aggregate this counter, just set to -1 */
                agg_null_rec->fcounters[i] = -1;
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
