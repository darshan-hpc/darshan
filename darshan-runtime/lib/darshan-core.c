/*
 *  (C) 2009 by Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
 */

#define _XOPEN_SOURCE 500

#include "darshan-runtime-config.h"

#include <stdio.h>
#ifdef HAVE_MNTENT_H
#include <mntent.h>
#endif
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <limits.h>
#include <pthread.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/vfs.h>

#include <mpi.h>

#include "darshan-core.h"
#include "utlist.h"

extern char* __progname_full;

static void darshan_core_initialize(int *argc, char ***argv);
static void darshan_core_shutdown(void);
static void darshan_core_cleanup(struct darshan_core_job_runtime* job);

/* internal variables */
static struct darshan_core_job_runtime *darshan_core_job = NULL;
static pthread_mutex_t darshan_mutex = PTHREAD_MUTEX_INITIALIZER;

#define DARSHAN_LOCK() pthread_mutex_lock(&darshan_mutex)
#define DARSHAN_UNLOCK() pthread_mutex_unlock(&darshan_mutex)

#define DARSHAN_MOD_REGISTER(__mod, __job) \
    LL_PREPEND(__job->mod_list_head, __mod)
#define DARSHAN_MOD_SEARCH(__mod, __tmp, __job) \
    LL_SEARCH(__job->mod_list_head, __mod, __tmp, mod_cmp)
#define DARSHAN_MOD_ITER(__mod, __tmp, __job) \
    LL_FOREACH_SAFE(__job->mod_list_head, __mod, __tmp)
#define DARSHAN_MOD_DELETE(__mod, __job) \
   LL_DELETE(__job->mod_list_head, __mod)

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
    int rank;
    char *logfile_name;
    struct darshan_core_job_runtime* final_job;
    struct darshan_core_module *mod, *tmp;
    int internal_timing_flag = 0;
    int jobid;
    char* jobid_str;
    char* envjobid;
    char* logpath;
    int ret;
    uint64_t hlevel;
    char hname[HOST_NAME_MAX];
    uint64_t logmod;
    char* logpath_override = NULL;
#ifdef __CP_LOG_ENV
    char env_check[256];
    char* env_tok;
#endif

    if(getenv("DARSHAN_INTERNAL_TIMING"))
        internal_timing_flag = 1;

    DARSHAN_LOCK();
    if(!darshan_core_job)
    {
        DARSHAN_UNLOCK();
        return;
    }
    /* disable further tracing while hanging onto the data so that we can
     * write it out
     */
    final_job = darshan_core_job;
    darshan_core_job = NULL;
    DARSHAN_UNLOCK();

    logfile_name = malloc(PATH_MAX);
    if(!logfile_name)
    {
        darshan_core_cleanup(final_job);
        return;
    }

    DARSHAN_MPI_CALL(PMPI_Comm_rank)(MPI_COMM_WORLD, &rank);

    /* construct log file name */
    if(rank == 0)
    {
        char cuser[L_cuserid] = {0};
        struct tm* my_tm;
        time_t start_time_tmp;

        /* Use CP_JOBID_OVERRIDE for the env var or CP_JOBID */
        envjobid = getenv(CP_JOBID_OVERRIDE);
        if (!envjobid)
        {
            envjobid = CP_JOBID;
        }

        /* Use CP_LOG_PATH_OVERRIDE for the value or __CP_LOG_PATH */
        logpath = getenv(CP_LOG_PATH_OVERRIDE);
        if (!logpath)
        {
#ifdef __CP_LOG_PATH
            logpath = __CP_LOG_PATH;
#endif
        }

        /* find a job id */
        jobid_str = getenv(envjobid);
        if(jobid_str)
        {
            /* in cobalt we can find it in env var */
            ret = sscanf(jobid_str, "%d", &jobid);
        }
        if(!jobid_str || ret != 1)
        {
            /* use pid as fall back */
            jobid = getpid();
        }

        /* break out time into something human readable */
        start_time_tmp = final_job->log_job.start_time;
        my_tm = localtime(&start_time_tmp);

        /* get the username for this job.  In order we will try each of the
         * following until one of them succeeds:
         *
         * - cuserid()
         * - getenv("LOGNAME")
         * - snprintf(..., geteuid());
         *
         * Note that we do not use getpwuid() because it generally will not
         * work in statically compiled binaries.
         */

#ifndef DARSHAN_DISABLE_CUSERID
        cuserid(cuser);
#endif

        /* if cuserid() didn't work, then check the environment */
        if (strcmp(cuser, "") == 0)
        {
            char* logname_string;
            logname_string = getenv("LOGNAME");
            if(logname_string)
            {
                strncpy(cuser, logname_string, (L_cuserid-1));
            }

        }

        /* if cuserid() and environment both fail, then fall back to uid */
        if (strcmp(cuser, "") == 0)
        {
            uid_t uid = geteuid();
            snprintf(cuser, sizeof(cuser), "%u", uid);
        }

        /* generate a random number to help differentiate the log */
        hlevel=DARSHAN_MPI_CALL(PMPI_Wtime)() * 1000000;
        (void) gethostname(hname, sizeof(hname));
        logmod = darshan_hash((void*)hname,strlen(hname),hlevel);

        /* see if darshan was configured using the --with-logpath-by-env
         * argument, which allows the user to specify an absolute path to
         * place logs via an env variable.
         */
#ifdef __CP_LOG_ENV
        /* just silently skip if the environment variable list is too big */
        if(strlen(__CP_LOG_ENV) < 256)
        {
            /* copy env variable list to a temporary buffer */
            strcpy(env_check, __CP_LOG_ENV);
            /* tokenize the comma-separated list */
            env_tok = strtok(env_check, ",");
            if(env_tok)
            {
                do
                {
                    /* check each env variable in order */
                    logpath_override = getenv(env_tok);
                    if(logpath_override)
                    {
                        /* stop as soon as we find a match */
                        break;
                    }
                }while((env_tok = strtok(NULL, ",")));
            }
        }
#endif

        if(logpath_override)
        {
            ret = snprintf(logfile_name, PATH_MAX,
                "%s/%s_%s_id%d_%d-%d-%d-%" PRIu64 ".darshan_partial",
                logpath_override,
                cuser, __progname, jobid,
                (my_tm->tm_mon+1),
                my_tm->tm_mday,
                (my_tm->tm_hour*60*60 + my_tm->tm_min*60 + my_tm->tm_sec),
                logmod);
            if(ret == (PATH_MAX-1))
            {
                /* file name was too big; squish it down */
                snprintf(logfile_name, PATH_MAX,
                    "%s/id%d.darshan_partial",
                    logpath_override, jobid);
            }
        }
        else if(logpath)
        {
            ret = snprintf(logfile_name, PATH_MAX,
                "%s/%d/%d/%d/%s_%s_id%d_%d-%d-%d-%" PRIu64 ".darshan_partial",
                logpath, (my_tm->tm_year+1900),
                (my_tm->tm_mon+1), my_tm->tm_mday,
                cuser, __progname, jobid,
                (my_tm->tm_mon+1),
                my_tm->tm_mday,
                (my_tm->tm_hour*60*60 + my_tm->tm_min*60 + my_tm->tm_sec),
                logmod);
            if(ret == (PATH_MAX-1))
            {
                /* file name was too big; squish it down */
                snprintf(logfile_name, PATH_MAX,
                    "%s/id%d.darshan_partial",
                    logpath, jobid);
            }
        }
        else
        {
            logfile_name[0] = '\0';
        }

        /* add jobid */
        final_job->log_job.jobid = (int64_t)jobid;
    }

    /* broadcast log file name */
    DARSHAN_MPI_CALL(PMPI_Bcast)(logfile_name, PATH_MAX, MPI_CHAR, 0,
        MPI_COMM_WORLD);

    if(strlen(logfile_name) == 0)
    {
        /* failed to generate log file name */
        darshan_core_cleanup(final_job);
        return;
    }

    final_job->log_job.end_time = time(NULL);

    /* TODO: coordinate shutdown accross all registered modules */



    free(logfile_name);
    darshan_core_cleanup(final_job);

    if (internal_timing_flag)
    {
        /* TODO: what do we want to time in new darshan version? */
    }
    
    return;
}

static void darshan_core_cleanup(struct darshan_core_job_runtime* job)
{
    struct darshan_core_module *mod, *tmp;

    DARSHAN_MOD_ITER(mod, tmp, job)
    {
        DARSHAN_MOD_DELETE(mod, job);
        free(mod);
    };

    free(job);

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

    DARSHAN_LOCK();

    *runtime_mem_limit = 0;
    if(!darshan_core_job)
    {
        DARSHAN_UNLOCK();
        return;
    }

    /* see if this module is already registered */
    strncpy(tmp.name, name, DARSHAN_MOD_NAME_LEN);
    DARSHAN_MOD_SEARCH(mod, &tmp, darshan_core_job);
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
    DARSHAN_MOD_REGISTER(mod, darshan_core_job);

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
