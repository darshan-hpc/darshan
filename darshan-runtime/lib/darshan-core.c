/*
 *  (C) 2009 by Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
 */

#include "darshan-runtime-config.h"

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#ifdef HAVE_MNTENT_H
#include <mntent.h>
#endif
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/vfs.h>
#include <pthread.h>

#include <mpi.h>

#include "darshan-core.h"

extern char* __progname_full;

static void darshan_core_initialize(int *argc, char ***argv);
static void darshan_core_shutdown(void);

/* internal variables */
static struct darshan_core_job_runtime *darshan_global_job = NULL;
static pthread_mutex_t darshan_mutex = PTHREAD_MUTEX_INITIALIZER;

#define DARSHAN_LOCK() pthread_mutex_lock(&darshan_mutex)
#define DARSHAN_UNLOCK() pthread_mutex_unlock(&darshan_mutex)

/* intercept MPI initialize and finalize to initialize darshan */
int MPI_Init(int *argc, char ***argv)
{
    int ret;

    ret = DARSHAN_MPI_CALL(PMPI_Init)(argc, argv);
    if(ret != MPI_SUCCESS)
    {
        return(ret);
    }

    darshan_core_initialize(argc, argv);

    return(ret);
}

int MPI_Init_thread(int *argc, char ***argv, int required, int *provided)
{
    int ret;

    ret = DARSHAN_MPI_CALL(PMPI_Init_thread)(argc, argv, required, provided);
    if (ret != MPI_SUCCESS)
    {
        return(ret);
    }

    darshan_core_initialize(argc, argv);

    return(ret);
}

int MPI_Finalize(void)
{
    int ret;

    darshan_core_shutdown();

    ret = DARSHAN_MPI_CALL(PMPI_Finalize)();
    return(ret);
}

static void darshan_core_initialize(int *argc, char ***argv)
{
    int i;
    int nprocs;
    int rank;
    int internal_timing_flag = 0;
    double init_start, init_time, init_max;
    char* truncate_string = "<TRUNCATED>";
    int truncate_offset;
    int chars_left = 0;

    DARSHAN_MPI_CALL(PMPI_Comm_size)(MPI_COMM_WORLD, &nprocs);
    DARSHAN_MPI_CALL(PMPI_Comm_rank)(MPI_COMM_WORLD, &rank);

    if(getenv("DARSHAN_INTERNAL_TIMING"))
        internal_timing_flag = 1;

    if (internal_timing_flag)
        init_start = DARSHAN_MPI_CALL(PMPI_Wtime)();

    /* setup darshan runtime if darshan is enabled and hasn't been initialized already */
    if (!getenv("DARSHAN_DISABLE") && !darshan_global_job)
    {
        /* allocate structure to track darshan_global_job information */
        darshan_global_job = malloc(sizeof(*darshan_global_job));
        if (darshan_global_job)
        {
            memset(darshan_global_job, 0, sizeof(*darshan_global_job));

            if (getenv("DARSHAN_DISABLE_TIMING"))
            {
                darshan_global_job->flags |= CP_FLAG_NOTIMING;
            }

            strcpy(darshan_global_job->log_job.version_string, CP_VERSION);
            darshan_global_job->log_job.magic_nr = CP_MAGIC_NR;
            darshan_global_job->log_job.uid = getuid();
            darshan_global_job->log_job.start_time = time(NULL);
            darshan_global_job->log_job.nprocs = nprocs;
            darshan_global_job->wtime_offset = DARSHAN_MPI_CALL(PMPI_Wtime)();
            darshan_global_job->mod_list_head = NULL;

            /* record exe and arguments */
            for(i=0; i<(*argc); i++)
            {
                chars_left = CP_EXE_LEN-strlen(darshan_global_job->exe);
                strncat(darshan_global_job->exe, *(argv[i]), chars_left);
                if(i < ((*argc)-1))
                {
                    chars_left = CP_EXE_LEN-strlen(darshan_global_job->exe);
                    strncat(darshan_global_job->exe, " ", chars_left);
                }
            }

            /* if we don't see any arguments, then use glibc symbol to get
             * program name at least (this happens in fortran)
             */
            if(argc == 0)
            {
                chars_left = CP_EXE_LEN-strlen(darshan_global_job->exe);
                strncat(darshan_global_job->exe, __progname_full, chars_left);
                chars_left = CP_EXE_LEN-strlen(darshan_global_job->exe);
                strncat(darshan_global_job->exe, " <unknown args>", chars_left);
            }

            if(chars_left == 0)
            {
                /* we ran out of room; mark that string was truncated */
                truncate_offset = CP_EXE_LEN - strlen(truncate_string);
                sprintf(&darshan_global_job->exe[truncate_offset], "%s",
                    truncate_string);
            }
        }
    }

    if(internal_timing_flag)
    {
        init_time = DARSHAN_MPI_CALL(PMPI_Wtime)() - init_start;
        DARSHAN_MPI_CALL(PMPI_Reduce)(&init_time, &init_max, 1,
            MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
        if(rank == 0)
        {
            printf("#darshan:<op>\t<nprocs>\t<time>\n");
            printf("darshan:init\t%d\t%f\n", nprocs, init_max);
        }
    }

    return;
}

static void darshan_core_shutdown()
{
    int internal_timing_flag = 0;

    if(getenv("DARSHAN_INTERNAL_TIMING"))
        internal_timing_flag = 1;

    printf("darshan SHUTDOWN\n");

    return;
}

/* ********************************************************* */

void darshan_core_register_module(
    char *name,
    struct darshan_module_funcs *funcs,
    int *runtime_mem_limit)
{
    struct darshan_core_module *tmp;
    struct darshan_core_module *new_mod;

    *runtime_mem_limit = 0;
    if (!darshan_global_job)
        return;

    DARSHAN_LOCK();
    tmp = darshan_global_job->mod_list_head;
    while(tmp)
    {
        /* silently return if this module is already registered */
        if (strcmp(tmp->name, name) == 0)
        {
            DARSHAN_UNLOCK();
            return;
        }
        tmp = tmp->next_mod;
    }

    /* allocate new module and add to the head of the linked list of darshan modules */
    new_mod = malloc(sizeof(*new_mod));
    if (!new_mod)
    {
        DARSHAN_UNLOCK();
        return;
    }

    memset(new_mod, 0, sizeof(*new_mod));
    strncpy(new_mod->name, name, DARSHAN_MOD_NAME_LEN);
    new_mod->mod_funcs = *funcs;
    new_mod->next_mod = darshan_global_job->mod_list_head;
    darshan_global_job->mod_list_head = new_mod;
    DARSHAN_UNLOCK();

    /* TODO: something smarter than just 2 MiB per module */
    *runtime_mem_limit = 2 * 1024 * 1024;

    return;
}

void darshan_core_lookup_id(
    void *name,
    int len,
    int printable_flag,
    darshan_file_id *id)
{
    darshan_file_id tmp_id;

    if (!darshan_global_job)
        return;

    /* TODO: what do you do with printable flag? */

    /* hash the input name to get a unique id for this record */
    tmp_id = darshan_hash(name, len, 0);
    
    /* TODO: how to store the filename to hash mapping? */

    *id = tmp_id;
    return;
}

double darshan_core_wtime()
{
    if(!darshan_global_job || darshan_global_job->flags & CP_FLAG_NOTIMING)
    {
        return(0);
    }

    return DARSHAN_MPI_CALL(PMPI_Wtime)();
}

/*
 * Local variables:
 *  c-indent-level: 4
 *  c-basic-offset: 4
 * End:
 *
 * vim: ts=8 sts=4 sw=4 expandtab
 */
