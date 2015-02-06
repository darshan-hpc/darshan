/*
 *  (C) 2009 by Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
 */

#define _GNU_SOURCE

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
#include <assert.h>

#include "uthash.h"
#include "darshan-core.h"

/* TODO is __progname_full needed here */
extern char* __progname;

/* internal variable delcarations */
static struct darshan_core_runtime *darshan_core_job = NULL;
static pthread_mutex_t darshan_core_mutex = PTHREAD_MUTEX_INITIALIZER;
static int my_rank = -1;
static int nprocs = -1;

/* prototypes for internal helper functions */
static void darshan_core_initialize(
    int *argc, char ***argv);
static void darshan_core_shutdown(
    void);
static void darshan_core_cleanup(
    struct darshan_core_runtime* job);
static void darshan_get_logfile_name(
    char* logfile_name, int jobid, struct tm* start_tm);
static void darshan_log_record_hints_and_ver(
    struct darshan_core_runtime* job);
static int darshan_get_shared_record_ids(
    struct darshan_core_runtime *job, darshan_record_id *shared_recs);
static int darshan_log_write_header(
    MPI_File log_fh, int mod_count, int64_t rec_off, int64_t psx_off);
static int darshan_log_write_record_map(
    MPI_File log_fh, struct darshan_core_runtime *job,
    darshan_record_id *shared_recs, MPI_Offset *off);
static int darshan_log_coll_append(MPI_File log_fh, MPI_Offset *off,
    void *buf, int count);

#define DARSHAN_CORE_LOCK() pthread_mutex_lock(&darshan_core_mutex)
#define DARSHAN_CORE_UNLOCK() pthread_mutex_unlock(&darshan_core_mutex)

/* intercept MPI initialize and finalize to manage darshan core runtime */
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
    int internal_timing_flag = 0;
    double init_start, init_time, init_max;
    char* truncate_string = "<TRUNCATED>";
    int truncate_offset;
    int chars_left = 0;

    DARSHAN_MPI_CALL(PMPI_Comm_size)(MPI_COMM_WORLD, &nprocs);
    DARSHAN_MPI_CALL(PMPI_Comm_rank)(MPI_COMM_WORLD, &my_rank);

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
                strncat(darshan_core_job->exe, __progname, chars_left);
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
        if(my_rank == 0)
        {
            printf("#darshan:<op>\t<nprocs>\t<time>\n");
            printf("darshan:init\t%d\t%f\n", nprocs, init_max);
        }
    }

    return;
}

static void darshan_core_shutdown()
{
    int i;
    char *logfile_name;
    struct darshan_core_runtime *final_job;
    struct darshan_core_module *mod, *tmp;
    int internal_timing_flag = 0;
    char *envjobid;
    char *jobid_str;
    int jobid;
    struct tm *start_tm;
    time_t start_time_tmp;
    int ret = 0;
    int all_ret = 0;
    int64_t first_start_time;
    int64_t last_end_time;
    int local_mod_use[DARSHAN_MAX_MODS] = {0};
    int global_mod_use_count[DARSHAN_MAX_MODS] = {0};
    darshan_record_id shared_recs[DARSHAN_CORE_MAX_RECORDS] = {0};
    char *key;
    char *value;
    char *hints;
    char *tok_str;
    char *orig_tok_str;
    char *saveptr = NULL;
    char *mod_index;
    char *new_logfile_name;
    double start_log_time;
    double end_log_time;
    long offset;
    MPI_File log_fh;
    MPI_Offset my_off = 0;
    MPI_Info info;
    MPI_Status status;

    if(getenv("DARSHAN_INTERNAL_TIMING"))
        internal_timing_flag = 1;

    DARSHAN_CORE_LOCK();
    if(!darshan_core_job)
    {
        DARSHAN_CORE_UNLOCK();
        return;
    }
    /* disable further tracing while hanging onto the data so that we can
     * write it out
     */
    final_job = darshan_core_job;
    darshan_core_job = NULL;
    DARSHAN_CORE_UNLOCK();

    start_log_time = DARSHAN_MPI_CALL(PMPI_Wtime)();

    logfile_name = malloc(PATH_MAX);
    if(!logfile_name)
    {
        darshan_core_cleanup(final_job);
        return;
    }

    /* set darshan job id/metadata and constuct log file name on rank 0 */
    if(my_rank == 0)
    {
        /* Use CP_JOBID_OVERRIDE for the env var or CP_JOBID */
        envjobid = getenv(CP_JOBID_OVERRIDE);
        if(!envjobid)
        {
            envjobid = CP_JOBID;
        }

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

        final_job->log_job.jobid = (int64_t)jobid;

        /* if we are using any hints to write the log file, then record those
         * hints in the log file header
         */
        darshan_log_record_hints_and_ver(final_job);

        /* use human readable start time format in log filename */
        start_time_tmp = final_job->log_job.start_time;
        start_tm = localtime(&start_time_tmp);

        /* construct log file name */
        darshan_get_logfile_name(logfile_name, jobid, start_tm);
    }

    /* broadcast log file name */
    DARSHAN_MPI_CALL(PMPI_Bcast)(logfile_name, PATH_MAX, MPI_CHAR, 0,
        MPI_COMM_WORLD);

    if(strlen(logfile_name) == 0)
    {
        /* failed to generate log file name */
        free(logfile_name);
        darshan_core_cleanup(final_job);
        return;
    }

    final_job->log_job.end_time = time(NULL);

    /* reduce to report first start time and last end time across all ranks
     * at rank 0
     */
    DARSHAN_MPI_CALL(PMPI_Reduce)(&final_job->log_job.start_time, &first_start_time, 1, MPI_LONG_LONG, MPI_MIN, 0, MPI_COMM_WORLD);
    DARSHAN_MPI_CALL(PMPI_Reduce)(&final_job->log_job.end_time, &last_end_time, 1, MPI_LONG_LONG, MPI_MAX, 0, MPI_COMM_WORLD);
    if(my_rank == 0)
    {
        final_job->log_job.start_time = first_start_time;
        final_job->log_job.end_time = last_end_time;
    }

    /* set which local modules were actually used */
    for(i = 0; i < DARSHAN_MAX_MODS; i++)
    {
        if(final_job->mod_array[i])
            local_mod_use[i] = 1;
    }

    /* reduce the number of times a module was opened globally and bcast to everyone */   
    DARSHAN_MPI_CALL(PMPI_Allreduce)(local_mod_use, global_mod_use_count, DARSHAN_MAX_MODS, MPI_INT, MPI_SUM, MPI_COMM_WORLD);

    /* check environment variable to see if the default MPI file hints have
     * been overridden
     */
    MPI_Info_create(&info);

    hints = getenv(CP_LOG_HINTS_OVERRIDE);
    if(!hints)
    {
        hints = __CP_LOG_HINTS;
    }

    if(hints && strlen(hints) > 0)
    {
        tok_str = strdup(hints);
        if(tok_str)
        {
            orig_tok_str = tok_str;
            do
            {
                /* split string on semicolon */
                key = strtok_r(tok_str, ";", &saveptr);
                if(key)
                {
                    tok_str = NULL;
                    /* look for = sign splitting key/value pairs */
                    value = index(key, '=');
                    if(value)
                    {
                        /* break key and value into separate null terminated strings */
                        value[0] = '\0';
                        value++;
                        if(strlen(key) > 0)
                            MPI_Info_set(info, key, value);
                    }
                }
            }while(key != NULL);
            free(orig_tok_str);
        }
    }

    /* TODO: ensuing error checking...does MPI ensure collective I/O functions return the same error
     * globally, or do I always need to allreduce????? */

    /* get a list of records which are shared across all processes */
    ret = darshan_get_shared_record_ids(final_job, shared_recs);

    /* error out if unable to determine shared file records */
    DARSHAN_MPI_CALL(PMPI_Allreduce)(&ret, &all_ret, 1, MPI_INT,
        MPI_LOR, MPI_COMM_WORLD);
    if(all_ret != 0)
    {
        if(my_rank == 0)
        {
            fprintf(stderr, "darshan library warning: unable to determine shared file records\n");
        }
        free(logfile_name);
        darshan_core_cleanup(final_job);
        return;

    }

    /* open the darshan log file for writing */
    ret = DARSHAN_MPI_CALL(PMPI_File_open)(MPI_COMM_WORLD, logfile_name,
        MPI_MODE_CREATE | MPI_MODE_WRONLY | MPI_MODE_EXCL, info, &log_fh);
    MPI_Info_free(&info);

    /* error out if unable to open log file */
    DARSHAN_MPI_CALL(PMPI_Allreduce)(&ret, &all_ret, 1, MPI_INT,
        MPI_LOR, MPI_COMM_WORLD);
    if(all_ret != 0)
    {
        if(my_rank == 0)
        {
            int msg_len;
            char msg[MPI_MAX_ERROR_STRING] = {0};

            MPI_Error_string(ret, msg, &msg_len);
            fprintf(stderr, "darshan library warning: unable to open log file %s: %s\n",
                logfile_name, msg);
            unlink(logfile_name);
        }
        free(logfile_name);
        darshan_core_cleanup(final_job);
        return;
    }


    if(my_rank == 0)
    {
        my_off = sizeof(struct darshan_header);
        my_off += 2 * sizeof(int64_t); /* FIXME account for changes to index map */

        ret = DARSHAN_MPI_CALL(PMPI_File_write_at)(log_fh, my_off, &final_job->log_job,
                sizeof(struct darshan_job), MPI_BYTE, &status);
        if(ret != MPI_SUCCESS)
        {
            int msg_len;
            char msg[MPI_MAX_ERROR_STRING] = {0};

            MPI_Error_string(ret, msg, &msg_len);
            fprintf(stderr, "darshan library warning: unable to write job data to log file %s: %s\n",
                    logfile_name, msg);
        }

        my_off += sizeof(struct darshan_job);
    }

    int64_t rec_off = my_off; /* TODO: get rid of this hack */
    /* write the record name->id map to the log file */
    ret = darshan_log_write_record_map(log_fh, final_job, shared_recs, &my_off);

    DARSHAN_MPI_CALL(PMPI_Allreduce)(&ret, &all_ret, 1, MPI_INT,
        MPI_LOR, MPI_COMM_WORLD);
    if(all_ret != 0)
    {
        if(my_rank == 0)
        {
            fprintf(stderr, "darshan library warning: unable to write record map to log file %s\n",
                logfile_name);
        }
        free(logfile_name);
        darshan_core_cleanup(final_job);
        return;
    }
    int64_t psx_off = my_off; /* TODO: get rid of this hack */

    /* loop over globally used darshan modules and:
     *      - get final output buffer
     *      - compress (zlib) provided output buffer
     *      - append compressed buffer to log file
     *      - shutdown the module
     */
    for(i = 0; i < DARSHAN_MAX_MODS; i++)
    {
        struct darshan_core_module* this_mod = final_job->mod_array[i];
        MPI_Comm mod_comm;
        void* mod_buf = NULL;
        int mod_buf_size = 0;

        if(!global_mod_use_count[i])
            continue;

        /* create a communicator to use for shutting down the module */
        if(global_mod_use_count[i] == nprocs)
        {
            MPI_Comm_dup(MPI_COMM_WORLD, &mod_comm);
        }
        else
        {
            MPI_Comm_split(MPI_COMM_WORLD, local_mod_use[i], 0, &mod_comm);
        }

        /* if module is registered locally, get the corresponding output buffer */
        if(local_mod_use[i])
        {
            /* get output buffer from module */
            this_mod->mod_funcs.get_output_data(mod_comm, &mod_buf, &mod_buf_size);
        }

        /* append module data buffer to the darshan log file */
        ret = darshan_log_coll_append(log_fh, &my_off, mod_buf, mod_buf_size);
        if(ret < 0)
        {
            /* TODO: */
        }

        /* shutdown module if registered locally */
        if(local_mod_use[i])
        {
            this_mod->mod_funcs.shutdown();
            this_mod = NULL;
        }

        MPI_Comm_free(&mod_comm);
    }

    /* rank 0 is responsible for writing the log header and index map */
    if(my_rank == 0)
    {
        ret = darshan_log_write_header(log_fh, 1, rec_off, psx_off);
    }

    DARSHAN_MPI_CALL(PMPI_File_close)(&log_fh);

    /* if we got this far, there are no errors, so rename from *.darshan_partial
     * to *-<logwritetime>.darshan.gz, which indicates that this log file is
     * complete and ready for analysis
     */
    new_logfile_name = malloc(PATH_MAX);
    if(new_logfile_name)
    {
        new_logfile_name[0] = '\0';
        end_log_time = DARSHAN_MPI_CALL(PMPI_Wtime)();
        strcat(new_logfile_name, logfile_name);
        mod_index = strstr(new_logfile_name, ".darshan_partial");
        sprintf(mod_index, "_%d.darshan.gz", (int)(end_log_time-start_log_time+1));
        rename(logfile_name, new_logfile_name);
        /* set permissions on log file */
#ifdef __CP_GROUP_READABLE_LOGS
        chmod(new_logfile_name, (S_IRUSR|S_IRGRP));
#else
        chmod(new_logfile_name, (S_IRUSR));
#endif
        free(new_logfile_name);
    }

    darshan_core_cleanup(final_job);
    free(logfile_name);

    if(internal_timing_flag)
    {
        /* TODO: what do we want to time in new darshan version? */
    }
    
    return;
}

/* free darshan core data structures to shutdown */
static void darshan_core_cleanup(struct darshan_core_runtime* job)
{
    int i;

    for(i = 0; i < DARSHAN_MAX_MODS; i++)
    {
        if(job->mod_array[i])
        {        
            free(job->mod_array[i]);
            job->mod_array[i] = NULL;
        }
    }

    free(job);

    return;
}

/* construct the darshan log file name */
static void darshan_get_logfile_name(char* logfile_name, int jobid, struct tm* start_tm)
{
    char* logpath;
    char* logname_string;
    char* logpath_override = NULL;
#ifdef __CP_LOG_ENV
    char env_check[256];
    char* env_tok;
#endif
    uint64_t hlevel;
    char hname[HOST_NAME_MAX];
    uint64_t logmod;
    char cuser[L_cuserid] = {0};
    int ret;

    /* Use CP_LOG_PATH_OVERRIDE for the value or __CP_LOG_PATH */
    logpath = getenv(CP_LOG_PATH_OVERRIDE);
    if(!logpath)
    {
#ifdef __CP_LOG_PATH
        logpath = __CP_LOG_PATH;
#endif
    }

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
    if(strcmp(cuser, "") == 0)
    {
        logname_string = getenv("LOGNAME");
        if(logname_string)
        {
            strncpy(cuser, logname_string, (L_cuserid-1));
        }

    }

    /* if cuserid() and environment both fail, then fall back to uid */
    if(strcmp(cuser, "") == 0)
    {
        uid_t uid = geteuid();
        snprintf(cuser, sizeof(cuser), "%u", uid);
    }

    /* generate a random number to help differentiate the log */
    hlevel=DARSHAN_MPI_CALL(PMPI_Wtime)() * 1000000;
    (void)gethostname(hname, sizeof(hname));
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
            (start_tm->tm_mon+1),
            start_tm->tm_mday,
            (start_tm->tm_hour*60*60 + start_tm->tm_min*60 + start_tm->tm_sec),
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
            logpath, (start_tm->tm_year+1900),
            (start_tm->tm_mon+1), start_tm->tm_mday,
            cuser, __progname, jobid,
            (start_tm->tm_mon+1),
            start_tm->tm_mday,
            (start_tm->tm_hour*60*60 + start_tm->tm_min*60 + start_tm->tm_sec),
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

    return;
}

/* record any hints used to write the darshan log in the log header */
static void darshan_log_record_hints_and_ver(struct darshan_core_runtime* job)
{
    char* hints;
    char* header_hints;
    int meta_remain = 0;
    char* m;

    /* check environment variable to see if the default MPI file hints have
     * been overridden
     */
    hints = getenv(CP_LOG_HINTS_OVERRIDE);
    if(!hints)
    {
        hints = __CP_LOG_HINTS;
    }

    if(!hints || strlen(hints) < 1)
        return;

    header_hints = strdup(hints);
    if(!header_hints)
        return;

    meta_remain = DARSHAN_JOB_METADATA_LEN -
        strlen(job->log_job.metadata) - 1;
    if(meta_remain >= (strlen(PACKAGE_VERSION) + 9))
    {
        sprintf(job->log_job.metadata, "lib_ver=%s\n", PACKAGE_VERSION);
        meta_remain -= (strlen(PACKAGE_VERSION) + 9);
    }
    if(meta_remain >= (3 + strlen(header_hints)))
    {
        m = job->log_job.metadata + strlen(job->log_job.metadata);
        /* We have room to store the hints in the metadata portion of
         * the job header.  We just prepend an h= to the hints list.  The
         * metadata parser will ignore = characters that appear in the value
         * portion of the metadata key/value pair.
         */
        sprintf(m, "h=%s\n", header_hints);
    }
    free(header_hints);

    return;
}

static int darshan_get_shared_record_ids(struct darshan_core_runtime *job,
    darshan_record_id *shared_recs)
{
    int i;
    int ndx;
    int ret;
    struct darshan_core_record_ref *ref, *tmp;
    darshan_record_id id_array[DARSHAN_CORE_MAX_RECORDS] = {0};
    darshan_record_id mask_array[DARSHAN_CORE_MAX_RECORDS] = {0};
    darshan_record_id all_mask_array[DARSHAN_CORE_MAX_RECORDS] = {0};

    /* first, determine list of records root process has opened */
    if(my_rank == 0)
    {
        ndx = 0;
        HASH_ITER(hlink, job->rec_hash, ref, tmp)
        {
            id_array[ndx++] = ref->rec.id;           
        }
    }

    /* broadcast root's list of records to all other processes */
    ret = DARSHAN_MPI_CALL(PMPI_Bcast)(id_array,
        (DARSHAN_CORE_MAX_RECORDS * sizeof(darshan_record_id)),
        MPI_BYTE, 0, MPI_COMM_WORLD);
    if(ret != 0)
    {
        return(-1);
    }

    /* everyone looks to see if they opened the same records as root */
    for(i=0; (i<DARSHAN_CORE_MAX_RECORDS && id_array[i] != 0); i++)
    {
        HASH_ITER(hlink, job->rec_hash, ref, tmp)
        {
            if(id_array[i] == ref->rec.id)
            {
                /* we opened that record too */
                mask_array[i] = 1;
                break;
            }
        }
    }

    /* now allreduce so everyone agrees which files are shared */
    ret = DARSHAN_MPI_CALL(PMPI_Allreduce)(mask_array, all_mask_array,
        DARSHAN_CORE_MAX_RECORDS, MPI_INT, MPI_LAND, MPI_COMM_WORLD);
    if(ret != 0)
    {
        return(-1);
    }

    ndx = 0;
    for(i=0; (i<DARSHAN_CORE_MAX_RECORDS && id_array[i] != 0); i++)
    {
        if(all_mask_array[i] != 0)
        {
            shared_recs[ndx++] = id_array[i];
        }
    }

    return(0);
}

static int darshan_log_write_header(MPI_File log_fh, int mod_count,
    int64_t rec_off, int64_t psx_off)
{
    struct darshan_header base_hdr;
    unsigned char *hdr_buf;
    unsigned char *tmp_p;
    int hdr_size;
    MPI_Status status;
    int i;
    int ret;

    /* set the fields of the darshan header */
    strcpy(base_hdr.version_string, CP_VERSION);
    base_hdr.magic_nr = CP_MAGIC_NR;
    base_hdr.comp_type = DARSHAN_GZ_COMP;
    base_hdr.mod_count = mod_count;

    hdr_size = sizeof(struct darshan_header) + (2 * sizeof(int64_t)); /* TODO: */
    hdr_buf = malloc(hdr_size);
    if(!hdr_buf)
    {
        return(-1);
    }

    /* pack the header in buffer for writing */
    tmp_p = hdr_buf;
    *((struct darshan_header *)tmp_p) = base_hdr;
    tmp_p += sizeof(struct darshan_header);

    /* TODO: we need to have a way of identifying different modules in index map */
    *((int64_t *)tmp_p) = rec_off;
    tmp_p += sizeof(int64_t);
    *((int64_t *)tmp_p) = psx_off;

    ret = DARSHAN_MPI_CALL(PMPI_File_write_at)(log_fh, 0, hdr_buf, hdr_size,
        MPI_BYTE, &status);
    if(ret < 0)
    {
        return(-1);
    }

    return(0);
}

/* NOTE: the map written to file may contain duplicate id->name entries if a
 *       record is opened by multiple ranks, but not all ranks
 */
static int darshan_log_write_record_map(MPI_File log_fh, struct darshan_core_runtime *job,
    darshan_record_id *shared_recs, MPI_Offset *off)
{
    int i;
    int ret;
    struct darshan_core_record_ref *ref, *tmp;
    uint32_t name_len;
    size_t record_sz;
    size_t map_buf_sz = 0;
    unsigned char *map_buf;
    unsigned char *map_buf_off;
    MPI_Status status;

    /* non-root ranks (rank 0) remove shared records from their map --
     * these records will be written by rank 0
     */
    if(my_rank > 0)
    {
        for(i=0; (i<DARSHAN_CORE_MAX_RECORDS && shared_recs[i]); i++)
        {
            HASH_FIND(hlink, job->rec_hash, &shared_recs[i], sizeof(darshan_record_id), ref);
            assert(ref); /* this id had better be in the hash ... */
            HASH_DELETE(hlink, job->rec_hash, ref);
            if(ref->rec.name) free(ref->rec.name);
            free(ref);
        }
    }

    /* allocate a buffer to store at most 64 bytes for each of a max number of records */
    /* NOTE: this buffer may be reallocated if estimate is too small */
    map_buf_sz = DARSHAN_CORE_MAX_RECORDS * 64;
    map_buf = malloc(map_buf_sz);
    if(!map_buf)
    {
        return(-1);
    }

    /* serialize the record map into a buffer for writing */
    map_buf_off = map_buf;
    HASH_ITER(hlink, job->rec_hash, ref, tmp)
    {
        name_len = strlen(ref->rec.name);
        record_sz = sizeof(darshan_record_id) + sizeof(int) + name_len;
        /* make sure there is room in the buffer for this record */
        if((map_buf_off + record_sz) > (map_buf + map_buf_sz))
        {
            unsigned char *tmp_buf;
            size_t old_buf_sz;

            /* if no room, reallocate the map buffer at twice the current size */
            old_buf_sz = map_buf_off - map_buf;
            map_buf_sz *= 2;
            tmp_buf = malloc(map_buf_sz);
            if(!tmp_buf)
            {
                free(map_buf);
                return(-1);
            }

            memcpy(tmp_buf, map_buf, old_buf_sz);
            free(map_buf);
            map_buf = tmp_buf;
            map_buf_off = map_buf + old_buf_sz;
        }

        /* now serialize the record into the map buffer.
         * NOTE: darshan record map serialization method: 
         *          ... darshan_record_id | (uint32_t) path_len | path ...
         */
        *((darshan_record_id *)map_buf_off) = ref->rec.id;
        map_buf_off += sizeof(darshan_record_id);
        *((uint32_t *)map_buf_off) = name_len;
        map_buf_off += sizeof(uint32_t);
        memcpy(map_buf_off, ref->rec.name, name_len);
        map_buf_off += name_len;
    }

    /* collectively write out the record map to the darshan log */
    if(map_buf_off > map_buf)
    {
        /* we have records to contribute to the collective write of the record map */
        ret = darshan_log_coll_append(log_fh, off, map_buf, (map_buf_off-map_buf));
    }
    else
    {
        /* we have no data to write, but participate in the collective anyway */
        ret = darshan_log_coll_append(log_fh, off, NULL, 0);
    }

    free(map_buf);

    if(ret < 0)
        return(-1);

    return(0);
}

/* NOTE: This function assumes that rank 0 passes in the log file offset
 *       to start appending at, so the caller must make sure rank 0
 *       has this info. After returning from this function, it is
 *       guaranteed all ranks will have the ending log file offset.
 */
static int darshan_log_coll_append(MPI_File log_fh, MPI_Offset *off,
    void *buf, int count)
{
    MPI_Offset send_off, my_off;
    MPI_Status status;
    int ret;

    /* figure out where everyone is writing */
    send_off = count;
    if(my_rank == 0)
        send_off += *off; /* rank 0 knows the beginning offset */

    DARSHAN_MPI_CALL(PMPI_Scan)(&send_off, &my_off, 1, MPI_OFFSET,
        MPI_SUM, MPI_COMM_WORLD);
    /* scan in inclusive; subtract local size back out */
    my_off -= count;

    /* perform the collective write */
    ret = DARSHAN_MPI_CALL(PMPI_File_write_at_all)(log_fh, my_off, buf,
        count, MPI_BYTE, &status);
    if(ret < 0)
        return(-1);

    *off = my_off + count;
    /* broadcast the final offset so everyone knows */
    DARSHAN_MPI_CALL(PMPI_Bcast)(off, 1, MPI_OFFSET, (nprocs-1), MPI_COMM_WORLD);

    return(0);
}

/* ********************************************************* */

void darshan_core_register_module(
    darshan_module_id id,
    char *name,
    struct darshan_module_funcs *funcs,
    int *runtime_mem_limit)
{
    struct darshan_core_module* mod;

    DARSHAN_CORE_LOCK();

    *runtime_mem_limit = 0;
    if(!darshan_core_job || (id >= DARSHAN_MAX_MODS))
    {
        DARSHAN_CORE_UNLOCK();
        return;
    }

    /* see if this module is already registered */
    if(darshan_core_job->mod_array[id])
    {
        /* if module is already registered just return */
        /* NOTE: we do not recalculate memory limit here, just set to 0 */
        DARSHAN_CORE_UNLOCK();
        return;
    }

    /* this module has not been registered yet, allocate and initialize it */
    mod = malloc(sizeof(*mod));
    if(!mod)
    {
        DARSHAN_CORE_UNLOCK();
        return;
    }
    memset(mod, 0, sizeof(*mod));

    mod->id = id;
    strncpy(mod->name, name, DARSHAN_MOD_NAME_LEN);
    mod->mod_funcs = *funcs;

    /* register module with darshan */
    darshan_core_job->mod_array[id] = mod;

    /* TODO: something smarter than just 2 MiB per module */
    *runtime_mem_limit = 2 * 1024 * 1024;

    DARSHAN_CORE_UNLOCK();

    return;
}

void darshan_core_lookup_record_id(
    void *name,
    int len,
    int printable_flag,
    darshan_record_id *id)
{
    darshan_record_id tmp_id;
    struct darshan_core_record_ref* ref;

    if(!darshan_core_job || !name)
        return;

    /* TODO: what do you do with printable flag? */

    /* hash the input name to get a unique id for this record */
    tmp_id = darshan_hash(name, len, 0);
 
    DARSHAN_CORE_LOCK();

    /* check to see if we've already stored the id->name mapping for this record */
    HASH_FIND(hlink, darshan_core_job->rec_hash, &tmp_id, sizeof(darshan_record_id), ref);
    if(!ref)
    {
        /* if not, add this record to the hash */
        ref = malloc(sizeof(struct darshan_core_record_ref));
        if(ref)
        {
            ref->rec.id = tmp_id;
            ref->rec.name = malloc(strlen(name) + 1);
            if(ref->rec.name)
                strcpy(ref->rec.name, name);

            HASH_ADD(hlink, darshan_core_job->rec_hash, rec.id, sizeof(darshan_record_id), ref);
        }
    }   

    DARSHAN_CORE_UNLOCK();

    printf("New Darshan record: %s (%"PRIu64")\n", (char *)name, tmp_id);

    *id = tmp_id;
    return;
}

double darshan_core_wtime()
{
    if(!darshan_core_job)
    {
        return(0);
    }

    return(DARSHAN_MPI_CALL(PMPI_Wtime)() - darshan_core_job->wtime_offset);
}

/*
 * Local variables:
 *  c-indent-level: 4
 *  c-basic-offset: 4
 * End:
 *
 * vim: ts=8 sts=4 sw=4 expandtab
 */
