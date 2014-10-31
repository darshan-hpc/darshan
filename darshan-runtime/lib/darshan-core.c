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
#include "utlist.h"

extern char* __progname_full;

static void darshan_core_initialize(int *argc, char ***argv);
static void darshan_core_shutdown(void);

/* internal variables */
static struct darshan_core_job_runtime *darshan_core_job = NULL;
static pthread_mutex_t darshan_mutex = PTHREAD_MUTEX_INITIALIZER;

#define DARSHAN_LOCK() pthread_mutex_lock(&darshan_mutex)
#define DARSHAN_UNLOCK() pthread_mutex_unlock(&darshan_mutex)

#define DARSHAN_MOD_REGISTER(__mod) \
    LL_PREPEND(darshan_core_job->mod_list_head, __mod)
#define DARSHAN_MOD_SEARCH(__mod, __tmp) \
    LL_SEARCH(darshan_core_job->mod_list_head, __mod, __tmp, mod_cmp)
#define DARSHAN_MOD_ITER(__mod, __tmp) \
    LL_FOREACH_SAFE(darshan_core_job->mod_list_head, __mod, __tmp)
#define DARSHAN_MOD_DELETE(__mod) \
   LL_DELETE(darshan_core_job->mod_list_head, __mod)

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
    if(ret != MPI_SUCCESS)
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

/* *********************************** */

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

    if(internal_timing_flag)
        init_start = DARSHAN_MPI_CALL(PMPI_Wtime)();

    /* setup darshan runtime if darshan is enabled and hasn't been initialized already */
    if(!getenv("DARSHAN_DISABLE") && !darshan_core_job)
    {
        /* allocate structure to track darshan_core_job information */
        darshan_core_job = malloc(sizeof(*darshan_core_job));
        if(darshan_core_job)
        {
            memset(darshan_core_job, 0, sizeof(*darshan_core_job));

            if(getenv("DARSHAN_DISABLE_TIMING"))
            {
                darshan_core_job->flags |= CP_FLAG_NOTIMING;
            }

            strcpy(darshan_core_job->log_job.version_string, CP_VERSION);
            darshan_core_job->log_job.magic_nr = CP_MAGIC_NR;
            darshan_core_job->log_job.uid = getuid();
            darshan_core_job->log_job.start_time = time(NULL);
            darshan_core_job->log_job.nprocs = nprocs;
            darshan_core_job->wtime_offset = DARSHAN_MPI_CALL(PMPI_Wtime)();

            /* record exe and arguments */
            for(i=0; i<(*argc); i++)
            {
                chars_left = CP_EXE_LEN-strlen(darshan_core_job->exe);
                strncat(darshan_core_job->exe, *(argv[i]), chars_left);
                if(i < ((*argc)-1))
                {
                    chars_left = CP_EXE_LEN-strlen(darshan_core_job->exe);
                    strncat(darshan_core_job->exe, " ", chars_left);
                }
            }

            /* if we don't see any arguments, then use glibc symbol to get
             * program name at least (this happens in fortran)
             */
            if(argc == 0)
            {
                chars_left = CP_EXE_LEN-strlen(darshan_core_job->exe);
                strncat(darshan_core_job->exe, __progname_full, chars_left);
                chars_left = CP_EXE_LEN-strlen(darshan_core_job->exe);
                strncat(darshan_core_job->exe, " <unknown args>", chars_left);
            }

            if(chars_left == 0)
            {
                /* we ran out of room; mark that string was truncated */
                truncate_offset = CP_EXE_LEN - strlen(truncate_string);
                sprintf(&darshan_core_job->exe[truncate_offset], "%s",
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
    struct darshan_core_module *mod, *tmp;
    int internal_timing_flag = 0;

    if(getenv("DARSHAN_INTERNAL_TIMING"))
        internal_timing_flag = 1;

    /* TODO: coordinate shutdown accross all registered modules */
    DARSHAN_MOD_ITER(mod, tmp)
    {
        printf("Shutting down %s module\n", mod->name);

        DARSHAN_MOD_DELETE(mod);
        free(mod);
    };

    free(darshan_core_job);

    return;
}

static int mod_cmp(struct darshan_core_module* a, struct darshan_core_module* b)
{
    return strcmp(a->name, b->name);
}

/* ********************************************************* */

void darshan_core_register_module(
    char *name,
    struct darshan_module_funcs *funcs,
    int *runtime_mem_limit)
{
    struct darshan_core_module tmp;
    struct darshan_core_module* mod;

    *runtime_mem_limit = 0;
    if(!darshan_core_job)
        return;

    DARSHAN_LOCK();

    /* see if this module is already registered */
    strncpy(tmp.name, name, DARSHAN_MOD_NAME_LEN);
    DARSHAN_MOD_SEARCH(mod, &tmp);
    if(mod)
    {
        /* if module is already registered, update module_funcs and return */
        /* NOTE: we do not recalculate memory limit here, just set to 0 */
        mod->mod_funcs = *funcs;

        DARSHAN_UNLOCK();
        return;
    }

    /* this module has not been registered yet, allocate and register it */
    mod = malloc(sizeof(*mod));
    if(!mod)
    {
        DARSHAN_UNLOCK();
        return;
    }
    memset(mod, 0, sizeof(*mod));

    strncpy(mod->name, name, DARSHAN_MOD_NAME_LEN);
    mod->mod_funcs = *funcs;
    DARSHAN_MOD_REGISTER(mod);

    /* TODO: something smarter than just 2 MiB per module */
    *runtime_mem_limit = 2 * 1024 * 1024;

    DARSHAN_UNLOCK();

    return;
}

void darshan_core_lookup_id(
    void *name,
    int len,
    int printable_flag,
    darshan_file_id *id)
{
    darshan_file_id tmp_id;

    if(!darshan_core_job)
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
    if(!darshan_core_job || darshan_core_job->flags & CP_FLAG_NOTIMING)
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
