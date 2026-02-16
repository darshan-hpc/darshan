/*
 * Copyright (C) 2018 University of Chicago.
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
#include "darshan-apmpi-log-format.h"

/* counter name strings for the MPI module */
#define Y(a) #a, 
#define Z(a) #a
#define X I
#define V J
char *apmpi_counter_names[] = {
    APMPI_MPIOP_COUNTERS
};
#undef X
#undef V
#define X F_TIME
#define V F_TIME
char *apmpi_f_mpiop_totaltime_counter_names[] = {
    APMPI_F_MPIOP_TOTALTIME_COUNTERS
};
#undef X
#undef V
#define X F_SYNC
#define V F_SYNC
char *apmpi_f_mpiop_synctime_counter_names[] = {
    APMPI_F_MPIOP_SYNCTIME_COUNTERS
};
#undef X
#undef V
char *apmpi_f_mpi_global_counter_names[] = {
    APMPI_F_MPI_GLOBAL_COUNTERS
};
#undef Y
#undef Z

static int darshan_log_get_apmpi_rec(darshan_fd fd, void** buf_p);
static int darshan_log_put_apmpi_rec(darshan_fd fd, void* buf);
static void darshan_log_print_apmpi_rec(void *file_rec,
    char *file_name, char *mnt_pt, char *fs_type);
static void darshan_log_print_apmpi_description(int ver);
static void darshan_log_print_apmpi_rec_diff(void *file_rec1, char *file_name1,
    void *file_rec2, char *file_name2);

struct darshan_mod_logutil_funcs apmpi_logutils =
{
    .log_get_record = &darshan_log_get_apmpi_rec,
    .log_put_record = &darshan_log_put_apmpi_rec,
    .log_print_record = &darshan_log_print_apmpi_rec,
    .log_print_description = &darshan_log_print_apmpi_description,
    .log_print_diff = &darshan_log_print_apmpi_rec_diff,
    .log_agg_records = NULL
};


static int darshan_log_get_apmpi_rec(darshan_fd fd, void** buf_p)
{
    struct darshan_apmpi_header_record *hdr_rec;
    struct darshan_apmpi_perf_record *prf_rec;
    int rec_len;
    char *buffer;
    int i;
    int ret = -1;
    static int first_rec = 1;

    if(fd->mod_map[DARSHAN_APMPI_MOD].len == 0)
        return(0);

    if(fd->mod_ver[DARSHAN_APMPI_MOD] == 0 ||
        fd->mod_ver[DARSHAN_APMPI_MOD] > APMPI_VER)
    {
        fprintf(stderr, "Error: Invalid APMPI module version number (got %d)\n",
            fd->mod_ver[DARSHAN_APMPI_MOD]);
        return(-1);
    }

    if (!*buf_p)
    {
        /* assume this is the largest possible record size */
        buffer = malloc(sizeof(struct darshan_apmpi_perf_record));
        if (!buffer)
        {
            return(-1);
        }
    }
    else
    {
        buffer = *buf_p;
    }

    if (fd->mod_ver[DARSHAN_APMPI_MOD] < APMPI_VER)
    {
        /* perform conversion as needed */
    }

    /* v1, current version */
    if (fd->mod_ver[DARSHAN_APMPI_MOD] == APMPI_VER)
    {
        if (first_rec)
        {
            rec_len = sizeof(struct darshan_apmpi_header_record);
            first_rec = 0;
        }
        else
            rec_len = sizeof(struct darshan_apmpi_perf_record);

        ret = darshan_log_get_mod(fd, DARSHAN_APMPI_MOD, buffer, rec_len);
    }

    if (ret == rec_len)
    {
        if(fd->swap_flag)
        {
            if (rec_len == sizeof(struct darshan_apmpi_header_record))
            {
                hdr_rec = (struct darshan_apmpi_header_record*)buffer;
                /* swap bytes if necessary */
                DARSHAN_BSWAP64(&(hdr_rec->base_rec.id));
                DARSHAN_BSWAP64(&(hdr_rec->base_rec.rank));
                DARSHAN_BSWAP64(&(hdr_rec->magic));
                DARSHAN_BSWAP32(&(hdr_rec->sync_flag));
                DARSHAN_BSWAP64(&(hdr_rec->apmpi_f_variance_total_mpitime));
                DARSHAN_BSWAP64(&(hdr_rec->apmpi_f_variance_total_mpisynctime));
            }
            else
            {
                prf_rec = (struct darshan_apmpi_perf_record*)buffer;
                DARSHAN_BSWAP64(&(prf_rec->base_rec.id));
                DARSHAN_BSWAP64(&(prf_rec->base_rec.rank));
                for (i = 0; i < APMPI_NUM_INDICES; i++)
                {
                    DARSHAN_BSWAP64(&prf_rec->counters[i]);
                }
                for (i = 0; i < APMPI_F_MPIOP_TOTALTIME_NUM_INDICES; i++)
                {
                    DARSHAN_BSWAP64(&prf_rec->fcounters[i]);
                }
                for (i = 0; i < APMPI_F_MPIOP_SYNCTIME_NUM_INDICES; i++)
                {
                    DARSHAN_BSWAP64(&prf_rec->fsynccounters[i]);
                }
                for (i = 0; i < APMPI_F_MPI_GLOBAL_NUM_INDICES; i++)
                {
                    DARSHAN_BSWAP64(&prf_rec->fglobalcounters[i]);
                }
            }
        }
        *buf_p = buffer;
        return(1);
    }
    else if (ret < 0)
    {
 //       *buf_p = NULL;
        if (!*buf_p) free(buffer);
        return(-1);
    }
    else
    {
  //      *buf_p = NULL;
        if (!*buf_p) free(buffer);
        return(0);
    }
}

static int darshan_log_put_apmpi_rec(darshan_fd fd, void* buf)
{
    int ret;
    int rec_len;
    static int first_rec = 1;

    if (first_rec)
    {
        rec_len = sizeof(struct darshan_apmpi_header_record);
        first_rec = 0;
    }
    else
        rec_len = sizeof(struct darshan_apmpi_perf_record);
    
    ret = darshan_log_put_mod(fd, DARSHAN_APMPI_MOD, buf,
                              rec_len, APMPI_VER);
    if(ret < 0)
        return(-1);

    return(0);
}

static void darshan_log_print_apmpi_rec(void *rec, char *file_name,
    char *mnt_pt, char *fs_type)
{
    int i;
    static int first_rec = 1;
    static int sync_flag;
    struct darshan_apmpi_header_record *hdr_rec;
    struct darshan_apmpi_perf_record *prf_rec;
    
    if (first_rec)
    {
        hdr_rec = rec;
        DARSHAN_F_COUNTER_PRINT(darshan_module_names[DARSHAN_APMPI_MOD],
            hdr_rec->base_rec.rank, hdr_rec->base_rec.id,
            "MPI_TOTAL_COMM_TIME_VARIANCE", hdr_rec->apmpi_f_variance_total_mpitime,
            "", "", "");
        if(hdr_rec->sync_flag)
        DARSHAN_F_COUNTER_PRINT(darshan_module_names[DARSHAN_APMPI_MOD],
            hdr_rec->base_rec.rank, hdr_rec->base_rec.id,
            "MPI_TOTAL_COMM_SYNC_TIME_VARIANCE", hdr_rec->apmpi_f_variance_total_mpisynctime,
            "", "", "");
        first_rec = 0;
        sync_flag = hdr_rec->sync_flag;
    }
    else
    {
        prf_rec = rec;

        DARSHAN_S_COUNTER_PRINT(darshan_module_names[DARSHAN_APMPI_MOD],
                  prf_rec->base_rec.rank, prf_rec->base_rec.id,
                  "MPI_PROCESSOR_NAME", prf_rec->node_name,
                  "", "", "");
   
        for(i = 0; i < APMPI_NUM_INDICES; i++)
        {
            DARSHAN_U_COUNTER_PRINT(darshan_module_names[DARSHAN_APMPI_MOD],
                prf_rec->base_rec.rank, prf_rec->base_rec.id,
                apmpi_counter_names[i], prf_rec->counters[i],
                "", "", "");
        }
        for(i = 0; i < APMPI_F_MPIOP_TOTALTIME_NUM_INDICES; i++)
        {
            DARSHAN_F_COUNTER_PRINT(darshan_module_names[DARSHAN_APMPI_MOD],
                prf_rec->base_rec.rank, prf_rec->base_rec.id,
                apmpi_f_mpiop_totaltime_counter_names[i], prf_rec->fcounters[i],
                "", "", "");
        }
        if(sync_flag)
        {
            for(i = 0; i < APMPI_F_MPIOP_SYNCTIME_NUM_INDICES; i++)
            {
                DARSHAN_F_COUNTER_PRINT(darshan_module_names[DARSHAN_APMPI_MOD],
                    prf_rec->base_rec.rank, prf_rec->base_rec.id,
                    apmpi_f_mpiop_synctime_counter_names[i], prf_rec->fsynccounters[i],
                    "", "", "");
            }
        }
        DARSHAN_F_COUNTER_PRINT(darshan_module_names[DARSHAN_APMPI_MOD],
            prf_rec->base_rec.rank, prf_rec->base_rec.id,
            apmpi_f_mpi_global_counter_names[0], prf_rec->fglobalcounters[0],
                "", "", "");
        if(sync_flag)
            DARSHAN_F_COUNTER_PRINT(darshan_module_names[DARSHAN_APMPI_MOD],
                prf_rec->base_rec.rank, prf_rec->base_rec.id,
                apmpi_f_mpi_global_counter_names[1], prf_rec->fglobalcounters[1],
                "", "", "");
    }

    return;
}

static void darshan_log_print_apmpi_description(int ver)
{
    printf("\n# description of APMPI counters:\n");
    printf("#   global summary stats showing the variance across all MPI processes:\n");
    printf("#     MPI_TOTAL_COMM_TIME_VARIANCE: variance in total communication time across all the processes.\n");
    printf("#     MPI_TOTAL_COMM_SYNC_TIME_VARIANCE: variance in total sync time across all the processes, if enabled.\n");
    printf("#   per-process detailed stats on the usage of various MPI routines:\n");
    printf("#     MPI_PROCESSOR_NAME: name of the processor used by the MPI process.\n");
    printf("#     MPI_*_CALL_COUNT: total call count for an MPI op.\n");
    printf("#     MPI_*_TOTAL_BYTES: total bytes (i.e., cumulative across all calls) moved with an MPI op.\n");
    printf("#     MPI_*_MSG_SIZE_AGG_*: histogram of total bytes moved for all the calls of an MPI op.\n");
    printf("#     MPI_*_TOTAL_TIME: total time (i.e, cumulative across all calls) of an MPI op.\n");
    printf("#     MPI_*_MIN_TIME: minimum time across all calls of an MPI op.\n");
    printf("#     MPI_*_MAX_TIME: maximum time across all calls of an MPI op.\n");
    printf("#     MPI_*_TOTAL_SYNC_TIME: total sync time (cumulative across all calls of an op) of an MPI op, if enabled.\n");
    printf("#     MPI_TOTAL_COMM_TIME: total communication (MPI) time of a process across all the MPI ops.\n");
    printf("#     MPI_TOTAL_COMM_SYNC_TIME: total sync time of a process across all the MPI ops, if enabled.\n");
    return;
}

static void darshan_log_print_apmpi_rec_diff(void *file_rec1, char *file_name1,
    void *file_rec2, char *file_name2)
{
    struct darshan_apmpi_header_record *hdr_rec1;
    struct darshan_apmpi_header_record *hdr_rec2;
    struct darshan_apmpi_perf_record   *prf_rec1;
    struct darshan_apmpi_perf_record   *prf_rec2;

    hdr_rec1 = (struct darshan_apmpi_header_record*) file_rec1;
    hdr_rec2 = (struct darshan_apmpi_header_record*) file_rec2;
    prf_rec1 = (struct darshan_apmpi_perf_record*) file_rec1;
    prf_rec2 = (struct darshan_apmpi_perf_record*) file_rec2;
    static int sync_flag;
    sync_flag = hdr_rec1->sync_flag && hdr_rec2->sync_flag;

    if (hdr_rec1->magic == APMPI_MAGIC)
    {
        /* this is the header record */   
        if (!hdr_rec2) 
        {
            printf("- ");   
            DARSHAN_F_COUNTER_PRINT(darshan_module_names[DARSHAN_APMPI_MOD],
                hdr_rec1->base_rec.rank, hdr_rec1->base_rec.id,
                "MPI_TOTAL_COMM_TIME_VARIANCE", hdr_rec1->apmpi_f_variance_total_mpitime,
                "", "", "");
            if(sync_flag)
            DARSHAN_F_COUNTER_PRINT(darshan_module_names[DARSHAN_APMPI_MOD],
                hdr_rec1->base_rec.rank, hdr_rec1->base_rec.id,
                "MPI_TOTAL_COMM_SYNC_TIME_VARIANCE", hdr_rec1->apmpi_f_variance_total_mpisynctime,
                "", "", "");
        }
        else if (!hdr_rec1)
        {
            printf("+ ");
            DARSHAN_F_COUNTER_PRINT(darshan_module_names[DARSHAN_APMPI_MOD],
                hdr_rec2->base_rec.rank, hdr_rec2->base_rec.id,
                "MPI_TOTAL_COMM_TIME_VARIANCE", hdr_rec2->apmpi_f_variance_total_mpitime,
                "", "", "");
            if(sync_flag)
            DARSHAN_F_COUNTER_PRINT(darshan_module_names[DARSHAN_APMPI_MOD],
                hdr_rec2->base_rec.rank, hdr_rec2->base_rec.id,
                "MPI_TOTAL_COMM_SYNC_TIME_VARIANCE", hdr_rec2->apmpi_f_variance_total_mpisynctime,
                "", "", "");
        }
        else
        {   
            if (hdr_rec1->apmpi_f_variance_total_mpitime != hdr_rec2->apmpi_f_variance_total_mpitime)
            {
                printf("- ");
                DARSHAN_F_COUNTER_PRINT(darshan_module_names[DARSHAN_APMPI_MOD],
                    hdr_rec1->base_rec.rank, hdr_rec1->base_rec.id,
                    "MPI_TOTAL_COMM_TIME_VARIANCE", hdr_rec1->apmpi_f_variance_total_mpitime,
                    "", "", "");
                printf("+ ");
                DARSHAN_F_COUNTER_PRINT(darshan_module_names[DARSHAN_APMPI_MOD],
                    hdr_rec2->base_rec.rank, hdr_rec2->base_rec.id,
                    "MPI_TOTAL_COMM_TIME_VARIANCE", hdr_rec2->apmpi_f_variance_total_mpitime,
                    "", "", "");
            }
            if(sync_flag)
            {
            if (hdr_rec1->apmpi_f_variance_total_mpisynctime != hdr_rec2->apmpi_f_variance_total_mpisynctime)
            {
                printf("- ");
                DARSHAN_F_COUNTER_PRINT(darshan_module_names[DARSHAN_APMPI_MOD],
                    hdr_rec1->base_rec.rank, hdr_rec1->base_rec.id,
                    "MPI_TOTAL_COMM_SYNC_TIME_VARIANCE", hdr_rec1->apmpi_f_variance_total_mpisynctime,
                    "", "", "");
                printf("+ ");
                DARSHAN_F_COUNTER_PRINT(darshan_module_names[DARSHAN_APMPI_MOD],
                    hdr_rec2->base_rec.rank, hdr_rec2->base_rec.id,
                    "MPI_TOTAL_COMM_SYNC_TIME_VARIANCE", hdr_rec2->apmpi_f_variance_total_mpisynctime,
                    "", "", "");
            }
            }
        }
    }
    else
    {   
        if(!prf_rec2)
        {
            printf("- ");
            DARSHAN_S_COUNTER_PRINT(darshan_module_names[DARSHAN_APMPI_MOD],
                  prf_rec1->base_rec.rank, prf_rec1->base_rec.id,
                  "MPI_PROCESSOR_NAME", prf_rec1->node_name,
                  "", "", "");
        }
        else if (!prf_rec1)
        {
            printf("+ ");
            DARSHAN_S_COUNTER_PRINT(darshan_module_names[DARSHAN_APMPI_MOD],
                  prf_rec2->base_rec.rank, prf_rec2->base_rec.id,
                  "MPI_PROCESSOR_NAME", prf_rec2->node_name,
                  "", "", "");
        }
        else if (prf_rec1->node_name != prf_rec2->node_name)
        {
            printf("- ");
            DARSHAN_S_COUNTER_PRINT(darshan_module_names[DARSHAN_APMPI_MOD],
                  prf_rec1->base_rec.rank, prf_rec1->base_rec.id,
                  "MPI_PROCESSOR_NAME", prf_rec1->node_name,
                  "", "", "");
            printf("+ ");
            DARSHAN_S_COUNTER_PRINT(darshan_module_names[DARSHAN_APMPI_MOD],
                  prf_rec2->base_rec.rank, prf_rec2->base_rec.id,
                  "MPI_PROCESSOR_NAME", prf_rec2->node_name,
                  "", "", "");
        }
        int i;
        for(i = 0; i < APMPI_NUM_INDICES; i++)
        {
            if (!prf_rec2)
            {   
                printf("- ");
                DARSHAN_U_COUNTER_PRINT(darshan_module_names[DARSHAN_APMPI_MOD],
                    prf_rec1->base_rec.rank, prf_rec1->base_rec.id,
                    apmpi_counter_names[i], prf_rec1->counters[i],
                    "", "", "");
            }
            else if (!prf_rec1)
            {
                printf("+ ");
                DARSHAN_U_COUNTER_PRINT(darshan_module_names[DARSHAN_APMPI_MOD],
                    prf_rec2->base_rec.rank, prf_rec2->base_rec.id,
                    apmpi_counter_names[i], prf_rec2->counters[i],
                    "", "", "");
            }
            else if (prf_rec1->counters[i] != prf_rec2->counters[i])
            {
                printf("- ");
                DARSHAN_U_COUNTER_PRINT(darshan_module_names[DARSHAN_APMPI_MOD],
                    prf_rec1->base_rec.rank, prf_rec1->base_rec.id,
                    apmpi_counter_names[i], prf_rec1->counters[i],
                    "", "", "");
                printf("+ ");
                DARSHAN_U_COUNTER_PRINT(darshan_module_names[DARSHAN_APMPI_MOD],
                    prf_rec2->base_rec.rank, prf_rec2->base_rec.id,
                    apmpi_counter_names[i], prf_rec2->counters[i],
                    "", "", "");
            }
        }
        
        for(i = 0; i < APMPI_F_MPIOP_TOTALTIME_NUM_INDICES; i++)
        {   
            if (!prf_rec2)
            {   
                printf("- ");
                DARSHAN_F_COUNTER_PRINT(darshan_module_names[DARSHAN_APMPI_MOD],
                    prf_rec1->base_rec.rank, prf_rec1->base_rec.id,
                    apmpi_f_mpiop_totaltime_counter_names[i], prf_rec1->fcounters[i],
                    "", "", "");
            }
            else if (!prf_rec1)
            {
                printf("+ ");
                DARSHAN_F_COUNTER_PRINT(darshan_module_names[DARSHAN_APMPI_MOD],
                    prf_rec2->base_rec.rank, prf_rec2->base_rec.id,
                    apmpi_f_mpiop_totaltime_counter_names[i], prf_rec2->fcounters[i],
                    "", "", "");
            }
            else if (prf_rec1->counters[i] != prf_rec2->counters[i])
            {
                printf("- ");
                DARSHAN_F_COUNTER_PRINT(darshan_module_names[DARSHAN_APMPI_MOD],
                    prf_rec1->base_rec.rank, prf_rec1->base_rec.id,
                    apmpi_f_mpiop_totaltime_counter_names[i], prf_rec1->fcounters[i],
                    "", "", "");
                printf("+ ");
                DARSHAN_F_COUNTER_PRINT(darshan_module_names[DARSHAN_APMPI_MOD],
                    prf_rec2->base_rec.rank, prf_rec2->base_rec.id,
                    apmpi_f_mpiop_totaltime_counter_names[i], prf_rec2->fcounters[i],
                    "", "", "");
            }
        }
        if(sync_flag)
        {
        for(i = 0; i < APMPI_F_MPIOP_SYNCTIME_NUM_INDICES; i++)
        {
            if (!prf_rec2)
            {
                printf("- ");
                DARSHAN_F_COUNTER_PRINT(darshan_module_names[DARSHAN_APMPI_MOD],
                    prf_rec1->base_rec.rank, prf_rec1->base_rec.id,
                    apmpi_f_mpiop_synctime_counter_names[i], prf_rec1->fsynccounters[i],
                    "", "", "");

            }
            else if (!prf_rec1)
            {
                printf("+ ");
                DARSHAN_F_COUNTER_PRINT(darshan_module_names[DARSHAN_APMPI_MOD],
                    prf_rec2->base_rec.rank, prf_rec2->base_rec.id,
                    apmpi_f_mpiop_synctime_counter_names[i], prf_rec2->fsynccounters[i],
                    "", "", "");
            }
            else if (prf_rec1->fsynccounters[i] != prf_rec2->fsynccounters[i])
            {
                printf("- ");
                DARSHAN_F_COUNTER_PRINT(darshan_module_names[DARSHAN_APMPI_MOD],
                    prf_rec1->base_rec.rank, prf_rec1->base_rec.id,
                    apmpi_f_mpiop_synctime_counter_names[i], prf_rec1->fsynccounters[i],
                    "", "", "");
                printf("+ ");
                DARSHAN_F_COUNTER_PRINT(darshan_module_names[DARSHAN_APMPI_MOD],
                    prf_rec2->base_rec.rank, prf_rec2->base_rec.id,
                    apmpi_f_mpiop_synctime_counter_names[i], prf_rec2->fsynccounters[i],
                    "", "", "");
            }
        }
        }
        //for(i = 0; i < APMPI_F_MPI_GLOBAL_NUM_INDICES; i++)
        {
            if (!prf_rec2)
            {
                printf("- ");
                DARSHAN_F_COUNTER_PRINT(darshan_module_names[DARSHAN_APMPI_MOD],
                    prf_rec1->base_rec.rank, prf_rec1->base_rec.id,
                    apmpi_f_mpi_global_counter_names[0], prf_rec1->fglobalcounters[0],
                    "", "", "");
            }
            else if (!prf_rec1)
            {
                printf("+ ");
                DARSHAN_F_COUNTER_PRINT(darshan_module_names[DARSHAN_APMPI_MOD],
                    prf_rec2->base_rec.rank, prf_rec2->base_rec.id,
                    apmpi_f_mpi_global_counter_names[0], prf_rec2->fglobalcounters[0],
                    "", "", "");
            }
            else if (prf_rec1->fglobalcounters[0] != prf_rec2->fglobalcounters[0])
            {
                printf("- ");
                DARSHAN_F_COUNTER_PRINT(darshan_module_names[DARSHAN_APMPI_MOD],
                    prf_rec1->base_rec.rank, prf_rec1->base_rec.id,
                    apmpi_f_mpi_global_counter_names[0], prf_rec1->fglobalcounters[0],
                    "", "", "");
                printf("+ ");
                DARSHAN_F_COUNTER_PRINT(darshan_module_names[DARSHAN_APMPI_MOD],
                    prf_rec2->base_rec.rank, prf_rec2->base_rec.id,
                    apmpi_f_mpi_global_counter_names[0], prf_rec2->fglobalcounters[0],
                    "", "", "");
            }
            if(sync_flag)
            {
            if (!prf_rec2)
            {
                printf("- ");
                DARSHAN_F_COUNTER_PRINT(darshan_module_names[DARSHAN_APMPI_MOD],
                    prf_rec1->base_rec.rank, prf_rec1->base_rec.id,
                    apmpi_f_mpi_global_counter_names[1], prf_rec1->fglobalcounters[1],
                    "", "", "");
            }
            else if (!prf_rec1)
            {
                printf("+ ");
                DARSHAN_F_COUNTER_PRINT(darshan_module_names[DARSHAN_APMPI_MOD],
                    prf_rec2->base_rec.rank, prf_rec2->base_rec.id,
                    apmpi_f_mpi_global_counter_names[1], prf_rec2->fglobalcounters[1],
                    "", "", "");
            }
            else if (prf_rec1->fglobalcounters[1] != prf_rec2->fglobalcounters[1])
            {
                printf("- ");
                DARSHAN_F_COUNTER_PRINT(darshan_module_names[DARSHAN_APMPI_MOD],
                    prf_rec1->base_rec.rank, prf_rec1->base_rec.id,
                    apmpi_f_mpi_global_counter_names[1], prf_rec1->fglobalcounters[1],
                    "", "", "");
                printf("+ ");
                DARSHAN_F_COUNTER_PRINT(darshan_module_names[DARSHAN_APMPI_MOD],
                    prf_rec2->base_rec.rank, prf_rec2->base_rec.id,
                    apmpi_f_mpi_global_counter_names[1], prf_rec2->fglobalcounters[1],
                    "", "", "");
            }
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
