/*
 * Copyright (C) 2015 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

#ifdef HAVE_CONFIG_H
# include <darshan-runtime-config.h>
#endif

#define _XOPEN_SOURCE 500
#define _GNU_SOURCE

#include <stdio.h>
#ifdef HAVE_MNTENT_H
#include <mntent.h>
#endif
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <limits.h>
#include <pthread.h>
#include <fcntl.h>
#include <stdarg.h>
#include <dirent.h>
#include <sys/ioctl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <sys/time.h>
#include <sys/vfs.h>
#include <zlib.h>
#include <assert.h>

#ifdef HAVE_MPI
#include <mpi.h>
#endif

#include "uthash.h"
#include "darshan.h"
#include "darshan-core.h"
#include "darshan-dynamic.h"
#include "darshan-dxt.h"

#ifdef DARSHAN_LUSTRE
#include <lustre/lustre_user.h>
#endif

extern char* __progname;
extern char* __progname_full;

/* internal variable delcarations */
static struct darshan_core_runtime *darshan_core = NULL;
static pthread_mutex_t darshan_core_mutex = PTHREAD_RECURSIVE_MUTEX_INITIALIZER_NP;
static int using_mpi = 0;
static int my_rank = 0;
static int nprocs = 1;
static int darshan_mem_alignment = 1;
static size_t darshan_mod_mem_quota = DARSHAN_MOD_MEM_MAX;
static int orig_parent_pid = 0;
static int parent_pid;

static struct darshan_core_mnt_data mnt_data_array[DARSHAN_MAX_MNTS];
static int mnt_data_count = 0;

/* paths prefixed with the following directories are not tracked by darshan */
char* darshan_path_exclusions[] = {
    "/etc/",
    "/dev/",
    "/usr/",
    "/bin/",
    "/boot/",
    "/lib/",
    "/opt/",
    "/sbin/",
    "/sys/",
    "/proc/",
    "/var/",
    NULL
};
/* paths prefixed with the following directories are tracked by darshan even if
 * they share a root with a path listed in darshan_path_exclusions
 */
char* darshan_path_inclusions[] = {
    "/var/opt/cray/dws/mounts/",
    NULL
};

/* allow users to override the path exclusions */
char** user_darshan_path_exclusions = NULL;

#ifdef DARSHAN_BGQ
extern void bgq_runtime_initialize();
#endif

#ifdef DARSHAN_USE_APXC
extern void apxc_runtime_initialize();
#endif

/* array of init functions for modules which need to be statically
 * initialized by darshan at startup time
 */
void (*mod_static_init_fns[])(void) =
{
#ifdef DARSHAN_BGQ
    &bgq_runtime_initialize,
#endif
#ifdef DARSHAN_USE_APXC
    &apxc_runtime_initialize,
#endif
    NULL
};

#ifdef DARSHAN_LUSTRE
/* XXX need to use extern to get Lustre module's instrumentation function
 * since modules have no way of providing this to darshan-core
 */
extern void darshan_instrument_lustre_file(const char *filepath, int fd);
#endif

/* prototypes for internal helper functions */
#ifdef __DARSHAN_ENABLE_MMAP_LOGS
static void *darshan_init_mmap_log(
    struct darshan_core_runtime* core, int jobid);
#endif
static void darshan_log_record_hints_and_ver(
    struct darshan_core_runtime* core);
static void darshan_get_exe_and_mounts(
    struct darshan_core_runtime *core, int argc, char **argv);
static void darshan_fs_info_from_path(
    const char *path, struct darshan_fs_info *fs_info);
static int darshan_add_name_record_ref(
    struct darshan_core_runtime *core, darshan_record_id rec_id,
    const char *name, darshan_module_id mod_id);
static void darshan_get_user_name(
    char *user);
#ifdef HAVE_MPI
static void darshan_get_shared_records(
    struct darshan_core_runtime *core, darshan_record_id **shared_recs,
    int *shared_rec_cnt);
#endif
static void darshan_get_logfile_name(
    char* logfile_name, struct darshan_core_runtime* core);
static int darshan_log_open(
    char *logfile_name, struct darshan_core_runtime *core,
    darshan_core_log_fh *log_fh);
static int darshan_log_write_job_record(
    darshan_core_log_fh log_fh, struct darshan_core_runtime *core,
    uint64_t *inout_off);
static int darshan_log_write_name_record_hash(
    darshan_core_log_fh log_fh, struct darshan_core_runtime *core,
    uint64_t *inout_off);
static int darshan_log_write_header(
    darshan_core_log_fh log_fh, struct darshan_core_runtime *core);
static int darshan_log_append(
    darshan_core_log_fh log_fh, struct darshan_core_runtime *core,
    void *buf, int count, uint64_t *inout_off);
void darshan_log_close(
    darshan_core_log_fh log_fh);
void darshan_log_finalize(
    char *logfile_name, double start_log_time);
static int darshan_deflate_buffer(
    void **pointers, int *lengths, int count, char *comp_buf,
    int *comp_buf_length);
static void darshan_core_cleanup(
    struct darshan_core_runtime* core);
static double darshan_core_wtime_absolute(void);
static void darshan_core_fork_child_cb(void);

#define DARSHAN_CORE_LOCK() pthread_mutex_lock(&darshan_core_mutex)
#define DARSHAN_CORE_UNLOCK() pthread_mutex_unlock(&darshan_core_mutex)

#define DARSHAN_WARN(__err_str, ...) do { \
    darshan_core_fprintf(stderr, "darshan_library_warning: " \
        __err_str ".\n", ## __VA_ARGS__); \
} while(0)

#ifdef HAVE_MPI

/* MPI variant of darshan logging helpers */
#define DARSHAN_CHECK_ERR(__ret, __err_str, ...) do { \
    if(using_mpi) \
        PMPI_Allreduce(MPI_IN_PLACE, &__ret, 1, MPI_INT, MPI_LOR, final_core->mpi_comm); \
    if(__ret != 0) { \
        if(my_rank == 0) { \
            DARSHAN_WARN(__err_str, ## __VA_ARGS__); \
            if(log_created) \
                unlink(logfile_name); \
        } \
        goto cleanup; \
    } \
} while(0)

#else

/* Non-MPI variant of darshan logging helpers */
#define DARSHAN_CHECK_ERR(__ret, __err_str, ...) do { \
    if(__ret != 0) { \
        DARSHAN_WARN(__err_str, ## __VA_ARGS__); \
        if(log_created) \
            unlink(logfile_name); \
        goto cleanup; \
    } \
} while(0)

#endif

/* *********************************** */

void darshan_core_initialize(int argc, char **argv)
{
    struct darshan_core_runtime *init_core = NULL;
    int internal_timing_flag = 0;
    double init_start, init_time;
    char *envstr;
    char *jobid_str;
    int jobid;
    int init_pid = getpid();
    int ret;
    int i;
    int tmpval;
    double tmpfloat;

    /* setup darshan runtime if darshan is enabled and hasn't been initialized already */
    if (darshan_core != NULL || getenv("DARSHAN_DISABLE"))
        return;

    if(getenv("DARSHAN_INTERNAL_TIMING"))
    {
        internal_timing_flag = 1;
        init_start = darshan_core_wtime();
    }

    #if (__DARSHAN_MEM_ALIGNMENT < 1)
        #error Darshan must be configured with a positive value for --with-mem-align
    #endif
    envstr = getenv(DARSHAN_MEM_ALIGNMENT_OVERRIDE);
    if(envstr)
    {
        ret = sscanf(envstr, "%d", &tmpval);
        /* silently ignore if the env variable is set poorly */
        if(ret == 1 && tmpval > 0)
        {
            darshan_mem_alignment = tmpval;
        }
    }
    else
    {
        darshan_mem_alignment = __DARSHAN_MEM_ALIGNMENT;
    }

    /* avoid floating point errors on faulty input */
    if(darshan_mem_alignment < 1)
    {
        darshan_mem_alignment = 1;
    }

    /* Use DARSHAN_JOBID_OVERRIDE for the env var for __DARSHAN_JOBID */
    envstr = getenv(DARSHAN_JOBID_OVERRIDE);
    if(!envstr)
    {
        envstr = __DARSHAN_JOBID;
    }

    /* find a job id */
    jobid_str = getenv(envstr);
    if(jobid_str)
    {
        /* in cobalt we can find it in env var */
        ret = sscanf(jobid_str, "%d", &jobid);
    }
    if(!jobid_str || ret != 1)
    {
        /* use pid as fall back */
        if(!orig_parent_pid)
            jobid = init_pid;
        else
            jobid = orig_parent_pid;
    }

    /* set the memory quota for darshan modules' records */
    envstr = getenv(DARSHAN_MOD_MEM_OVERRIDE);
    if(envstr)
    {
        ret = sscanf(envstr, "%lf", &tmpfloat);
        /* silently ignore if the env variable is set poorly */
        if(ret == 1 && tmpfloat > 0)
        {
            darshan_mod_mem_quota = tmpfloat * 1024 * 1024; /* convert from MiB */
        }
    }

    /* allocate structure to track darshan core runtime information */
    init_core = malloc(sizeof(*init_core));
    if(init_core)
    {
        memset(init_core, 0, sizeof(*init_core));

#ifdef HAVE_MPI
        PMPI_Initialized(&using_mpi);
        if(using_mpi)
        {
            PMPI_Comm_dup(MPI_COMM_WORLD, &init_core->mpi_comm);
            PMPI_Comm_size(init_core->mpi_comm, &nprocs);
            PMPI_Comm_rank(init_core->mpi_comm, &my_rank);
        }
#endif

        /* setup fork handlers if not using MPI */
        if(!using_mpi && !orig_parent_pid)
        {
            pthread_atfork(NULL, NULL, &darshan_core_fork_child_cb);
        }

        /* record absolute start time at startup so that we can later
         * generate relative times with this as a reference point.
         */
        init_core->wtime_offset = darshan_core_wtime_absolute();

        /* set PID that initialized Darshan runtime */
        init_core->pid = init_pid;

        /* TODO: do we alloc new memory as we go or just do everything up front? */

#ifndef __DARSHAN_ENABLE_MMAP_LOGS
        /* just allocate memory for each log file region */
        init_core->log_hdr_p = malloc(sizeof(struct darshan_header));
        init_core->log_job_p = malloc(sizeof(struct darshan_job));
        init_core->log_exemnt_p = malloc(DARSHAN_EXE_LEN+1);
        init_core->log_name_p = malloc(DARSHAN_NAME_RECORD_BUF_SIZE);
        init_core->log_mod_p = malloc(darshan_mod_mem_quota);

        if(!(init_core->log_hdr_p) || !(init_core->log_job_p) ||
           !(init_core->log_exemnt_p) || !(init_core->log_name_p) ||
           !(init_core->log_mod_p))
        {
            free(init_core);
            return;
        }
        /* if allocation succeeds, zero fill memory regions */
        memset(init_core->log_hdr_p, 0, sizeof(struct darshan_header));
        memset(init_core->log_job_p, 0, sizeof(struct darshan_job));
        memset(init_core->log_exemnt_p, 0, DARSHAN_EXE_LEN+1);
        memset(init_core->log_name_p, 0, DARSHAN_NAME_RECORD_BUF_SIZE);
        memset(init_core->log_mod_p, 0, darshan_mod_mem_quota);
#else
        /* if mmap logs are enabled, we need to initialize the mmap region
         * before setting the corresponding log file region pointers
         */
        void *mmap_p = darshan_init_mmap_log(init_core, jobid);
        if(!mmap_p)
        {
            free(init_core);
            return;
        }

        /* set the memory pointers for each log file region */
        init_core->log_hdr_p = (struct darshan_header *)mmap_p;
        init_core->log_job_p = (struct darshan_job *)
            ((char *)init_core->log_hdr_p + sizeof(struct darshan_header));
        init_core->log_exemnt_p = (char *)
            ((char *)init_core->log_job_p + sizeof(struct darshan_job));
        init_core->log_name_p = (void *)
            ((char *)init_core->log_exemnt_p + DARSHAN_EXE_LEN + 1);
        init_core->log_mod_p = (void *)
            ((char *)init_core->log_name_p + DARSHAN_NAME_RECORD_BUF_SIZE);

        /* set header fields needed for the mmap log mechanism */
        init_core->log_hdr_p->comp_type = DARSHAN_NO_COMP;
        init_core->log_hdr_p->name_map.off =
            ((char *)init_core->log_name_p - (char *)init_core->log_hdr_p);
#endif

        /* set known header fields for the log file */
        strcpy(init_core->log_hdr_p->version_string, DARSHAN_LOG_VERSION);
        init_core->log_hdr_p->magic_nr = DARSHAN_MAGIC_NR;

        /* set known job-level metadata fields for the log file */
        init_core->log_job_p->uid = getuid();
        init_core->log_job_p->start_time = time(NULL);
        init_core->log_job_p->nprocs = nprocs;
        init_core->log_job_p->jobid = (int64_t)jobid;

        /* if we are using any hints to write the log file, then record those
         * hints with the darshan job information
         */
        darshan_log_record_hints_and_ver(init_core);

        /* collect information about command line and mounted file systems */
        darshan_get_exe_and_mounts(init_core, argc, argv);

        /* determine if/when DXT should be enabled by looking for triggers */
        char *trigger_conf = getenv("DXT_TRIGGER_CONF_PATH");
        if(trigger_conf)
        {
            dxt_load_trigger_conf(trigger_conf);
        }

        /* if darshan was successfully initialized, set the global pointer
         * and bootstrap any modules with static initialization routines
         */
        DARSHAN_CORE_LOCK();
        darshan_core = init_core;
        DARSHAN_CORE_UNLOCK();

        i = 0;
        while(mod_static_init_fns[i])
        {
            (*mod_static_init_fns[i])();
            i++;
        }
    }

    if(internal_timing_flag)
    {
        init_time = darshan_core_wtime() - init_start;
#ifdef HAVE_MPI
        if(using_mpi)
        {
            if(my_rank == 0)
            {
                PMPI_Reduce(MPI_IN_PLACE, &init_time, 1,
                    MPI_DOUBLE, MPI_MAX, 0, darshan_core->mpi_comm);
            }
            else
            {
                PMPI_Reduce(&init_time, &init_time, 1,
                    MPI_DOUBLE, MPI_MAX, 0, darshan_core->mpi_comm);
                return; /* return early so every rank doesn't print */
            }
        }
#endif

        darshan_core_fprintf(stderr, "#darshan:<op>\t<nprocs>\t<time>\n");
        darshan_core_fprintf(stderr, "darshan:init\t%d\t%f\n", nprocs, init_time);
    }

    return;
}

void darshan_core_shutdown(int write_log)
{
    struct darshan_core_runtime *final_core;
    double start_log_time;
    int internal_timing_flag = 0;
    double open1 = 0, open2 = 0;
    double job1 = 0, job2 = 0;
    double rec1 = 0, rec2 = 0;
    double mod1[DARSHAN_MAX_MODS] = {0};
    double mod2[DARSHAN_MAX_MODS] = {0};
    double header1 = 0, header2 = 0;
    double tm_end;
    int active_mods[DARSHAN_MAX_MODS] = {0};
    uint64_t gz_fp = 0;
    char *logfile_name = NULL;
    darshan_core_log_fh log_fh;
    int log_created = 0;
    int meta_remain = 0;
    char *m;
    int i;
    int ret;
#ifdef HAVE_MPI
    darshan_record_id *shared_recs = NULL;
    darshan_record_id *mod_shared_recs = NULL;
    int shared_rec_cnt = 0;
#endif

    /* disable darhan-core while we shutdown */
    DARSHAN_CORE_LOCK();
    if(!darshan_core)
    {
        DARSHAN_CORE_UNLOCK();
        return;
    }
    final_core = darshan_core;
    darshan_core = NULL;
    DARSHAN_CORE_UNLOCK();

    /* skip to cleanup if not writing a log */
    if(!write_log)
        goto cleanup;

    /* NOTE: from this point on, this function must use
     * darshan_core_wtime_absolute() rather than darshan_core_wtime() to
     * collect timestamps for internal timing calculations.  The former no
     * longer works because it relies on runtime state to calculate
     * timestamps relative to job start.
     */

    /* grab some initial timing information */
#ifdef HAVE_MPI
    /* if using mpi, sync across procs first */
    if(using_mpi)
        PMPI_Barrier(final_core->mpi_comm);
#endif
    start_log_time = darshan_core_wtime_absolute();
    final_core->log_job_p->end_time = time(NULL);

    if(getenv("DARSHAN_INTERNAL_TIMING"))
        internal_timing_flag = 1;

#ifdef __DARSHAN_ENABLE_MMAP_LOGS
    /* remove the temporary mmap log files */
    /* NOTE: this unlink is not immediate as it must wait for the mapping
     * to no longer be referenced, which in our case happens when the
     * executable exits. If the application terminates mid-shutdown, then
     * there will be no mmap files and no final log file.
     */
    unlink(final_core->mmap_log_name);
#endif

    final_core->comp_buf = malloc(darshan_mod_mem_quota);
    logfile_name = malloc(PATH_MAX);
    if(!final_core->comp_buf || !logfile_name)
        goto cleanup;

    /* set which modules were used locally */
    for(i = 0; i < DARSHAN_MAX_MODS; i++)
    {
        if(final_core->mod_array[i])
            active_mods[i] = 1;
    }

#ifdef HAVE_MPI
    if(using_mpi)
    {
        /* allreduce locally active mods to determine globally active mods */
        PMPI_Allreduce(MPI_IN_PLACE, active_mods, DARSHAN_MAX_MODS, MPI_INT,
            MPI_SUM, final_core->mpi_comm);

        /* reduce to report first start and last end time across all ranks at rank 0 */
        if(my_rank == 0)
        {
            PMPI_Reduce(MPI_IN_PLACE, &final_core->log_job_p->start_time,
                1, MPI_INT64_T, MPI_MIN, 0, final_core->mpi_comm);
            PMPI_Reduce(MPI_IN_PLACE, &final_core->log_job_p->end_time,
                1, MPI_INT64_T, MPI_MAX, 0, final_core->mpi_comm);
        }
        else
        {
            PMPI_Reduce(&final_core->log_job_p->start_time,
                &final_core->log_job_p->start_time,
                1, MPI_INT64_T, MPI_MIN, 0, final_core->mpi_comm);
            PMPI_Reduce(&final_core->log_job_p->end_time,
                &final_core->log_job_p->end_time,
                1, MPI_INT64_T, MPI_MAX, 0, final_core->mpi_comm);
        }

        /* get a list of records which are shared across all processes */
        darshan_get_shared_records(final_core, &shared_recs, &shared_rec_cnt);

        mod_shared_recs = malloc(shared_rec_cnt * sizeof(darshan_record_id));
        assert(mod_shared_recs);
    }
#endif

    /* detect whether we forked, saving the parent pid in the log metadata if so */
    /* NOTE: this should only be triggered in non-MPI cases, since MPI mode still
     * bootstraps the shutdown procedure on MPI_Finalize, which forked processes
     * will not call
     */
    if(orig_parent_pid)
    {
        /* set fork metadata */
        meta_remain = DARSHAN_JOB_METADATA_LEN -
            strlen(final_core->log_job_p->metadata) - 1;
        if(meta_remain >= 18) // 18 bytes enough for meta string + max PID (5 chars)
        {
            m = final_core->log_job_p->metadata +
                strlen(final_core->log_job_p->metadata);
            sprintf(m, "fork_parent=%d\n", parent_pid);
        }
    }

    /* get the log file name */
    darshan_get_logfile_name(logfile_name, final_core);
    if(strlen(logfile_name) == 0)
    {
        /* failed to generate log file name */
        goto cleanup;
    }

    if(internal_timing_flag)
        open1 = darshan_core_wtime_absolute();
    /* open the darshan log file */
    ret = darshan_log_open(logfile_name, final_core, &log_fh);
    if(internal_timing_flag)
        open2 = darshan_core_wtime_absolute();
    /* error out if unable to open log file */
    DARSHAN_CHECK_ERR(ret, "unable to create log file %s", logfile_name);
    log_created = 1;

    if(internal_timing_flag)
        job1 = darshan_core_wtime_absolute();
    /* write the the compressed darshan job information */
    ret = darshan_log_write_job_record(log_fh, final_core, &gz_fp);
    if(internal_timing_flag)
        job2 = darshan_core_wtime_absolute();
    /* error out if unable to write job information */
    DARSHAN_CHECK_ERR(ret, "unable to write job record to file %s", logfile_name);

    if(internal_timing_flag)
        rec1 = darshan_core_wtime_absolute();
    /* write the record name->id hash to the log file */
    final_core->log_hdr_p->name_map.off = gz_fp;
    ret = darshan_log_write_name_record_hash(log_fh, final_core, &gz_fp);
    if(internal_timing_flag)
        rec2 = darshan_core_wtime_absolute();
    final_core->log_hdr_p->name_map.len = gz_fp - final_core->log_hdr_p->name_map.off;
    /* error out if unable to write name records */
    DARSHAN_CHECK_ERR(ret, "unable to write name records to log file %s", logfile_name);

    /* loop over globally used darshan modules and:
     *      - get final output buffer
     *      - compress (zlib) provided output buffer
     *      - append compressed buffer to log file
     *      - add module map info (file offset/length) to log header
     *      - shutdown the module
     */
    for(i = 0; i < DARSHAN_MAX_MODS; i++)
    {
        struct darshan_core_module* this_mod = final_core->mod_array[i];
        void* mod_buf = NULL;
        int mod_buf_sz = 0;

        if(!active_mods[i])
        {
            final_core->log_hdr_p->mod_map[i].off = 0;
            final_core->log_hdr_p->mod_map[i].len = 0;
            continue;
        }

        if(internal_timing_flag)
            mod1[i] = darshan_core_wtime_absolute();

        /* if module is registered locally, perform module shutdown operations */
        if(this_mod)
        {
            mod_buf = final_core->mod_array[i]->rec_buf_start;
            mod_buf_sz = final_core->mod_array[i]->rec_buf_p - mod_buf;

#ifdef HAVE_MPI
            if(using_mpi)
            {
                struct darshan_core_name_record_ref *ref = NULL;
                int mod_shared_rec_cnt = 0;
                int j;

                /* set the shared record list for this module */
                for(j = 0; j < shared_rec_cnt; j++)
                {
                    HASH_FIND(hlink, final_core->name_hash, &shared_recs[j],
                        sizeof(darshan_record_id), ref);
                    assert(ref);

                    if(DARSHAN_MOD_FLAG_ISSET(ref->global_mod_flags, i))
                    {
                        mod_shared_recs[mod_shared_rec_cnt++] = shared_recs[j];
                    }
                }

                /* allow the module an opportunity to reduce shared files */
                if(this_mod->mod_funcs.mod_redux_func && (mod_shared_rec_cnt > 0) &&
                   (!getenv("DARSHAN_DISABLE_SHARED_REDUCTION")))
                {
                    this_mod->mod_funcs.mod_redux_func(mod_buf, final_core->mpi_comm,
                        mod_shared_recs, mod_shared_rec_cnt);
                }
            }
#endif

            /* get the final output buffer */
            this_mod->mod_funcs.mod_output_func(&mod_buf, &mod_buf_sz);
        }

        /* append this module's data to the darshan log */
        final_core->log_hdr_p->mod_map[i].off = gz_fp;
        ret = darshan_log_append(log_fh, final_core, mod_buf, mod_buf_sz, &gz_fp);
        final_core->log_hdr_p->mod_map[i].len =
            gz_fp - final_core->log_hdr_p->mod_map[i].off;

        if(internal_timing_flag)
            mod2[i] = darshan_core_wtime_absolute();

        /* error out if unable to write module data */
        DARSHAN_CHECK_ERR(ret, "unable to write %s module data to log file %s",
            darshan_module_names[i], logfile_name);
    }

    if(internal_timing_flag)
        header1 = darshan_core_wtime_absolute();
    ret = darshan_log_write_header(log_fh, final_core);
    if(internal_timing_flag)
        header2 = darshan_core_wtime_absolute();
    DARSHAN_CHECK_ERR(ret, "unable to write header to file %s", logfile_name);

    /* done writing data, close the log file */
    darshan_log_close(log_fh);

    /* finalize log file name and permissions */
    darshan_log_finalize(logfile_name, start_log_time);

    if(internal_timing_flag)
    {
        double open_tm;
        double header_tm;
        double job_tm;
        double rec_tm;
        double mod_tm[DARSHAN_MAX_MODS];
        double all_tm;

        tm_end = darshan_core_wtime_absolute();

        open_tm = open2 - open1;
        header_tm = header2 - header1;
        job_tm = job2 - job1;
        rec_tm = rec2 - rec1;
        all_tm = tm_end - start_log_time;
        for(i = 0; i < DARSHAN_MAX_MODS; i++)
        {
            mod_tm[i] = mod2[i] - mod1[i];
        }

#ifdef HAVE_MPI
        if(using_mpi)
        {
            if(my_rank == 0)
            {
                PMPI_Reduce(MPI_IN_PLACE, &open_tm, 1,
                    MPI_DOUBLE, MPI_MAX, 0, final_core->mpi_comm);
                PMPI_Reduce(MPI_IN_PLACE, &header_tm, 1,
                    MPI_DOUBLE, MPI_MAX, 0, final_core->mpi_comm);
                PMPI_Reduce(MPI_IN_PLACE, &job_tm, 1,
                    MPI_DOUBLE, MPI_MAX, 0, final_core->mpi_comm);
                PMPI_Reduce(MPI_IN_PLACE, &rec_tm, 1,
                    MPI_DOUBLE, MPI_MAX, 0, final_core->mpi_comm);
                PMPI_Reduce(MPI_IN_PLACE, &all_tm, 1,
                    MPI_DOUBLE, MPI_MAX, 0, final_core->mpi_comm);
                PMPI_Reduce(MPI_IN_PLACE, mod_tm, DARSHAN_MAX_MODS,
                    MPI_DOUBLE, MPI_MAX, 0, final_core->mpi_comm);
            }
            else
            {
                PMPI_Reduce(&open_tm, &open_tm, 1,
                    MPI_DOUBLE, MPI_MAX, 0, final_core->mpi_comm);
                PMPI_Reduce(&header_tm, &header_tm, 1,
                    MPI_DOUBLE, MPI_MAX, 0, final_core->mpi_comm);
                PMPI_Reduce(&job_tm, &job_tm, 1,
                    MPI_DOUBLE, MPI_MAX, 0, final_core->mpi_comm);
                PMPI_Reduce(&rec_tm, &rec_tm, 1,
                    MPI_DOUBLE, MPI_MAX, 0, final_core->mpi_comm);
                PMPI_Reduce(&all_tm, &all_tm, 1,
                    MPI_DOUBLE, MPI_MAX, 0, final_core->mpi_comm);
                PMPI_Reduce(mod_tm, mod_tm, DARSHAN_MAX_MODS,
                    MPI_DOUBLE, MPI_MAX, 0, final_core->mpi_comm);

                /* let rank 0 report the timing info */
                goto cleanup;
            }
        }
#endif

        darshan_core_fprintf(stderr, "#darshan:<op>\t<nprocs>\t<time>\n");
        darshan_core_fprintf(stderr, "darshan:log_open\t%d\t%f\n", nprocs, open_tm);
        darshan_core_fprintf(stderr, "darshan:job_write\t%d\t%f\n", nprocs, job_tm);
        darshan_core_fprintf(stderr, "darshan:hash_write\t%d\t%f\n", nprocs, rec_tm);
        darshan_core_fprintf(stderr, "darshan:header_write\t%d\t%f\n", nprocs, header_tm);
        for(i = 0; i < DARSHAN_MAX_MODS; i++)
        {
            if(active_mods[i])
                darshan_core_fprintf(stderr, "darshan:%s_shutdown\t%d\t%f\n",
                    darshan_module_names[i], nprocs, mod_tm[i]);
        }
        darshan_core_fprintf(stderr, "darshan:core_shutdown\t%d\t%f\n", nprocs, all_tm);
    }

cleanup:
    for(i = 0; i < DARSHAN_MAX_MODS; i++)
        if(final_core->mod_array[i])
            final_core->mod_array[i]->mod_funcs.mod_cleanup_func();
    darshan_core_cleanup(final_core);
#ifdef HAVE_MPI
    if(using_mpi)
    {
        free(shared_recs);
        free(mod_shared_recs);
    }
#endif
    free(logfile_name);

    return;
}

/* *********************************** */

#ifdef __DARSHAN_ENABLE_MMAP_LOGS
static void *darshan_init_mmap_log(struct darshan_core_runtime* core, int jobid)
{
    int ret;
    int mmap_fd;
    size_t mmap_size;
    int sys_page_size;
    char cuser[L_cuserid] = {0};
    uint64_t hlevel;
    char hname[HOST_NAME_MAX];
    uint64_t logmod;
    char *envstr;
    char *mmap_log_path;
    void *mmap_p;

    sys_page_size = sysconf(_SC_PAGESIZE);
    assert(sys_page_size > 0);

    mmap_size = sizeof(struct darshan_header) + DARSHAN_JOB_RECORD_SIZE +
        + DARSHAN_NAME_RECORD_BUF_SIZE + darshan_mod_mem_quota;
    if(mmap_size % sys_page_size)
        mmap_size = ((mmap_size / sys_page_size) + 1) * sys_page_size;

    envstr = getenv(DARSHAN_MMAP_LOG_PATH_OVERRIDE);
    if(envstr)
        mmap_log_path = envstr;
    else
        mmap_log_path = DARSHAN_DEF_MMAP_LOG_PATH;

    darshan_get_user_name(cuser);

    /* generate a random number to help differentiate the temporary log */
    /* NOTE: job id is not sufficient for constructing a unique log file name,
     * since a job could be composed of multiple application runs, so we also
     * add a random number component to the log name
     */
    if(my_rank == 0)
    {
        hlevel = darshan_core_wtime_absolute() * 1000000;
        (void)gethostname(hname, sizeof(hname));
        logmod = darshan_hash((void*)hname,strlen(hname),hlevel);
    }
#ifdef HAVE_MPI
    if(using_mpi)
        PMPI_Bcast(&logmod, 1, MPI_UINT64_T, 0, core->mpi_comm);
#endif

    /* construct a unique temporary log file name for this process
     * to write mmap log data to
     */
    snprintf(core->mmap_log_name, PATH_MAX,
        "/%s/%s_%s_id%d_mmap-log-%" PRIu64 "-%d.darshan",
        mmap_log_path, cuser, __progname, jobid, logmod, my_rank);

    /* create the temporary mmapped darshan log */
    mmap_fd = open(core->mmap_log_name, O_CREAT|O_RDWR|O_EXCL , 0644);
    if(mmap_fd < 0)
    {
        darshan_core_fprintf(stderr, "darshan library warning: "
            "unable to create darshan log file %s\n", core->mmap_log_name);
        return(NULL);
    }

    /* TODO: ftruncate or just zero fill? */
    /* allocate the necessary space in the log file */
    ret = ftruncate(mmap_fd, mmap_size);
    if(ret < 0)
    {
        darshan_core_fprintf(stderr, "darshan library warning: "
            "unable to allocate darshan log file %s\n", core->mmap_log_name);
        close(mmap_fd);
        unlink(core->mmap_log_name);
        return(NULL);
    }

    /* create the memory map for darshan's data structures so they are
     * persisted to file as the application executes
     */
    mmap_p = mmap(NULL, mmap_size, PROT_WRITE, MAP_SHARED, mmap_fd, 0);
    if(mmap_p == MAP_FAILED)
    {
        darshan_core_fprintf(stderr, "darshan library warning: "
            "unable to mmap darshan log file %s\n", core->mmap_log_name);
        close(mmap_fd);
        unlink(core->mmap_log_name);
        return(NULL);
    }

    /* close darshan log file (this does *not* unmap the log file) */
    close(mmap_fd);

    return(mmap_p);
}
#endif

/* record any hints used to write the darshan log in the job data */
static void darshan_log_record_hints_and_ver(struct darshan_core_runtime* core)
{
    char* hints;
    char* job_hints;
    int meta_remain = 0;
    char* m;

    /* check environment variable to see if the default MPI file hints have
     * been overridden
     */
    hints = getenv(DARSHAN_LOG_HINTS_OVERRIDE);
    if(!hints)
    {
        hints = __DARSHAN_LOG_HINTS;
    }

    if(!hints || strlen(hints) < 1)
        return;

    job_hints = strdup(hints);
    if(!job_hints)
        return;

    meta_remain = DARSHAN_JOB_METADATA_LEN -
        strlen(core->log_job_p->metadata) - 1;
    if(meta_remain >= (strlen(PACKAGE_VERSION) + 9))
    {
        sprintf(core->log_job_p->metadata, "lib_ver=%s\n", PACKAGE_VERSION);
        meta_remain -= (strlen(PACKAGE_VERSION) + 9);
    }
    if(meta_remain >= (3 + strlen(job_hints)))
    {
        m = core->log_job_p->metadata + strlen(core->log_job_p->metadata);
        /* We have room to store the hints in the metadata portion of
         * the job structure.  We just prepend an h= to the hints list.  The
         * metadata parser will ignore = characters that appear in the value
         * portion of the metadata key/value pair.
         */
        sprintf(m, "h=%s\n", job_hints);
    }
    free(job_hints);

    return;
}

static int mnt_data_cmp(const void* a, const void* b)
{
    const struct darshan_core_mnt_data *d_a = (const struct darshan_core_mnt_data*)a;
    const struct darshan_core_mnt_data *d_b = (const struct darshan_core_mnt_data*)b;

    if(strlen(d_a->path) > strlen(d_b->path))
        return(-1);
    else if(strlen(d_a->path) < strlen(d_b->path))
        return(1);
    else
        return(0);
}

/* adds an entry to table of mounted file systems */
static void add_entry(char* buf, int* space_left, struct mntent* entry)
{
    int i;
    int ret;
    char tmp_mnt[256];
    struct statfs statfsbuf;

    /* avoid adding the same mount points multiple times -- to limit
     * storage space and potential statfs, ioctl, etc calls
     */
    for(i = 0; i < mnt_data_count; i++)
    {
        if((strncmp(mnt_data_array[i].path, entry->mnt_dir, DARSHAN_MAX_MNT_PATH) == 0) &&
           (strncmp(mnt_data_array[i].type, entry->mnt_type, DARSHAN_MAX_MNT_PATH) == 0))
            return;
    }

    strncpy(mnt_data_array[mnt_data_count].path, entry->mnt_dir,
        DARSHAN_MAX_MNT_PATH-1);
    strncpy(mnt_data_array[mnt_data_count].type, entry->mnt_type,
        DARSHAN_MAX_MNT_TYPE-1);
    /* NOTE: we now try to detect the preferred block size for each file
     * system using fstatfs().  On Lustre we assume a size of 1 MiB
     * because fstatfs() reports 4 KiB.
     */
#ifndef LL_SUPER_MAGIC
#define LL_SUPER_MAGIC 0x0BD00BD0
#endif
    ret = statfs(entry->mnt_dir, &statfsbuf);
    mnt_data_array[mnt_data_count].fs_info.fs_type = statfsbuf.f_type;
    if(ret == 0 && statfsbuf.f_type != LL_SUPER_MAGIC)
        mnt_data_array[mnt_data_count].fs_info.block_size = statfsbuf.f_bsize;
    else if(ret == 0 && statfsbuf.f_type == LL_SUPER_MAGIC)
        mnt_data_array[mnt_data_count].fs_info.block_size = 1024*1024;
    else
        mnt_data_array[mnt_data_count].fs_info.block_size = 4096;

#ifdef DARSHAN_LUSTRE
    /* attempt to retrieve OST and MDS counts from Lustre */
    mnt_data_array[mnt_data_count].fs_info.ost_count = -1;
    mnt_data_array[mnt_data_count].fs_info.mdt_count = -1;
    if ( statfsbuf.f_type == LL_SUPER_MAGIC )
    {
        int n_ost, n_mdt;
        int ret_ost, ret_mdt;
        DIR *mount_dir;

        mount_dir = opendir( entry->mnt_dir );
        if ( mount_dir  )
        {
            /* n_ost and n_mdt are used for both input and output to ioctl */
            n_ost = 0;
            n_mdt = 1;

            ret_ost = ioctl( dirfd(mount_dir), LL_IOC_GETOBDCOUNT, &n_ost );
            ret_mdt = ioctl( dirfd(mount_dir), LL_IOC_GETOBDCOUNT, &n_mdt );

            if ( !(ret_ost < 0 || ret_mdt < 0) )
            {
                mnt_data_array[mnt_data_count].fs_info.ost_count = n_ost;
                mnt_data_array[mnt_data_count].fs_info.mdt_count = n_mdt;
            }
            closedir( mount_dir );
        }
    }
#endif

    /* store mount information with the job-level metadata in darshan log */
    ret = snprintf(tmp_mnt, 256, "\n%s\t%s",
        entry->mnt_type, entry->mnt_dir);
    if(ret < 256 && strlen(tmp_mnt) <= (*space_left))
    {
        strcat(buf, tmp_mnt);
        (*space_left) -= strlen(tmp_mnt);
    }

    mnt_data_count++;
    return;
}

/* darshan_get_exe_and_mounts()
 *
 * collects command line and list of mounted file systems into a string that
 * will be stored with the job-level metadata
 */
static void darshan_get_exe_and_mounts(struct darshan_core_runtime *core,
    int argc, char **argv)
{
    FILE* tab;
    struct mntent *entry;
    char* exclude;
    char* truncate_string = "<TRUNCATED>";
    int truncate_offset;
    int space_left = DARSHAN_EXE_LEN;
    FILE *fh;
    int i, ii;
    char cmdl[DARSHAN_EXE_LEN];
    int tmp_index = 0;
    int skip = 0;
    char* env_exclusions;
    char* string;
    char* token;

    /* skip these fs types */
    static char* fs_exclusions[] = {
        "tmpfs",
        "proc",
        "sysfs",
        "devpts",
        "binfmt_misc",
        "fusectl",
        "debugfs",
        "securityfs",
        "nfsd",
        "none",
        "rpc_pipefs",
        "hugetlbfs",
        "cgroup",
        NULL
    };

    /* Check if user has set the env variable DARSHAN_EXCLUDE_DIRS */
    env_exclusions = getenv("DARSHAN_EXCLUDE_DIRS");
    if(env_exclusions)
    {
        fs_exclusions[0]=NULL;
        /* if DARSHAN_EXCLUDE_DIRS=none, do not exclude any dir */
        if(strncmp(env_exclusions,"none",strlen(env_exclusions))>=0)
        {
            if (my_rank == 0)
                darshan_core_fprintf(stderr, "Darshan info: no system dirs will be excluded\n");
            darshan_path_exclusions[0]=NULL;
        }
        else
        {
            if (my_rank == 0)
                darshan_core_fprintf(stderr, "Darshan info: the following system dirs will be excluded: %s\n",
                    env_exclusions);
            string = strdup(env_exclusions);
            i = 0;
            /* get the comma separated number of directories */
            token = strtok(string, ",");
            while (token != NULL)
            {
                i++;
                token = strtok(NULL, ",");
            }
            user_darshan_path_exclusions=(char **)malloc((i+1)*sizeof(char *));
            assert(user_darshan_path_exclusions);

            i = 0;
            strcpy(string, env_exclusions);
            token = strtok(string, ",");
            while (token != NULL)
            {
                user_darshan_path_exclusions[i]=(char *)malloc(strlen(token)+1);
                assert(user_darshan_path_exclusions[i]);
                strcpy(user_darshan_path_exclusions[i],token);
                i++;
                token = strtok(NULL, ",");
            }
            user_darshan_path_exclusions[i]=NULL;
            free(string);
        }
    }

    /* record exe and arguments */
    for(i=0; i<argc; i++)
    {
        strncat(core->log_exemnt_p, argv[i], space_left);
        space_left = DARSHAN_EXE_LEN-strlen(core->log_exemnt_p);
        if(i < (argc-1))
        {
            strncat(core->log_exemnt_p, " ", space_left);
            space_left = DARSHAN_EXE_LEN-strlen(core->log_exemnt_p);
        }
    }

    /* if we don't see any arguments, then use glibc symbol to get
     * program name at least (this happens in fortran)
     */
    if(argc == 0)
    {
        /* get the name of the executable and the arguments from
           /proc/self/cmdline */

        cmdl[0] = '\0';
        char* s;
        fh = fopen("/proc/self/cmdline","r");
        if(fh) {
            ii = 0;
            s = fgets(cmdl,DARSHAN_EXE_LEN,fh);
            if(!s)
                sprintf(cmdl, "%s <unknown args>", __progname_full);
            else {
                for(i=1;i<DARSHAN_EXE_LEN;i++)  {
                    if(cmdl[i]==0 && ii == 0) {
                      cmdl[i]=' '; ii = 1;
                    } else if(cmdl[i]==0 && ii == 1) {
                      break;
                    } else {
                      ii = 0;
                    }
                }
            }
            fclose(fh);
        } else {
           sprintf(cmdl, "%s <unknown args>", __progname_full);
        }
        strncat(core->log_exemnt_p, cmdl, space_left);
        space_left = DARSHAN_EXE_LEN-strlen(core->log_exemnt_p);
    }

    if(space_left == 0)
    {
        /* we ran out of room; mark that string was truncated */
        truncate_offset = DARSHAN_EXE_LEN - strlen(truncate_string);
        sprintf(&(core->log_exemnt_p[truncate_offset]), "%s",
            truncate_string);
    }

    /* we make two passes through mounted file systems; in the first pass we
     * grab any non-nfs mount points, then on the second pass we grab nfs
     * mount points
     */
    mnt_data_count = 0;

    tab = setmntent("/etc/mtab", "r");
    if(!tab)
        return;
    /* loop through list of mounted file systems */
    while(mnt_data_count<DARSHAN_MAX_MNTS && (entry = getmntent(tab)) != NULL)
    {
        /* filter out excluded fs types */
        tmp_index = 0;
        skip = 0;
        while((exclude = fs_exclusions[tmp_index]))
        {
            if(!(strcmp(exclude, entry->mnt_type)))
            {
                skip =1;
                break;
            }
            tmp_index++;
        }

        if(skip || (strcmp(entry->mnt_type, "nfs") == 0))
            continue;

        add_entry(core->log_exemnt_p, &space_left, entry);
    }
    endmntent(tab);

    tab = setmntent("/etc/mtab", "r");
    if(!tab)
        return;
    /* loop through list of mounted file systems */
    while(mnt_data_count<DARSHAN_MAX_MNTS && (entry = getmntent(tab)) != NULL)
    {
        if(strcmp(entry->mnt_type, "nfs") != 0)
            continue;

        add_entry(core->log_exemnt_p, &space_left, entry);
    }
    endmntent(tab);

    /* sort mount points in order of longest path to shortest path.  This is
     * necessary so that if we try to match file paths to mount points later
     * we don't match on "/" every time.
     */
    qsort(mnt_data_array, mnt_data_count, sizeof(mnt_data_array[0]), mnt_data_cmp);
    return;
}

static void darshan_fs_info_from_path(const char *path, struct darshan_fs_info *fs_info)
{
    int i;
    fs_info->fs_type = -1;
    fs_info->block_size = -1;

    for(i=0; i<mnt_data_count; i++)
    {
        if(!(strncmp(mnt_data_array[i].path, path, strlen(mnt_data_array[i].path))))
        {
            *fs_info = mnt_data_array[i].fs_info;
            return;
        }
    }

    return;
}

static int darshan_add_name_record_ref(struct darshan_core_runtime *core,
    darshan_record_id rec_id, const char *name, darshan_module_id mod_id)
{
    struct darshan_core_name_record_ref *ref;
    int record_size = sizeof(darshan_record_id) + strlen(name) + 1;

    if((record_size + core->name_mem_used) > DARSHAN_NAME_RECORD_BUF_SIZE)
        return(0);

    ref = malloc(sizeof(*ref));
    if(!ref)
        return(0);
    memset(ref, 0, sizeof(*ref));

    /* initialize the name record */
    ref->name_record = (struct darshan_name_record *)
        ((char *)core->log_name_p + core->name_mem_used);
    memset(ref->name_record, 0, record_size);
    ref->name_record->id = rec_id;
    strcpy(ref->name_record->name, name);
    DARSHAN_MOD_FLAG_SET(ref->mod_flags, mod_id);

    /* add the record to the hash table */
    HASH_ADD(hlink, core->name_hash, name_record->id,
        sizeof(darshan_record_id), ref);
    core->name_mem_used += record_size;
#ifdef __DARSHAN_ENABLE_MMAP_LOGS
    core->log_hdr_p->name_map.len += record_size;
#endif

    return(1);
}

static void darshan_get_user_name(char *cuser)
{
    char* logname_string;

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

#ifndef __DARSHAN_DISABLE_CUSERID
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
        snprintf(cuser, L_cuserid, "%u", uid);
    }

    return;
}

#ifdef HAVE_MPI
static void darshan_get_shared_records(struct darshan_core_runtime *core,
    darshan_record_id **shared_recs, int *shared_rec_cnt)
{
    int i, j;
    int tmp_cnt = HASH_CNT(hlink, core->name_hash);
    struct darshan_core_name_record_ref *tmp, *ref;
    darshan_record_id *id_array;
    uint64_t *mod_flags;
    uint64_t *global_mod_flags;

    /* broadcast root's number of records to all other processes */
    PMPI_Bcast(&tmp_cnt, 1, MPI_INT, 0, core->mpi_comm);

    /* use root record count to allocate data structures */
    id_array = malloc(tmp_cnt * sizeof(darshan_record_id));
    mod_flags = malloc(tmp_cnt * sizeof(uint64_t));
    global_mod_flags = malloc(tmp_cnt * sizeof(uint64_t));
    *shared_recs = malloc(tmp_cnt * sizeof(darshan_record_id));
    assert(id_array && mod_flags && global_mod_flags && *shared_recs);

    memset(mod_flags, 0, tmp_cnt * sizeof(uint64_t));
    memset(global_mod_flags, 0, tmp_cnt * sizeof(uint64_t));
    memset(*shared_recs, 0, tmp_cnt * sizeof(darshan_record_id));

    /* first, determine list of records root process has opened */
    if(my_rank == 0)
    {
        i = 0;
        HASH_ITER(hlink, core->name_hash, ref, tmp)
        {
            id_array[i++] = ref->name_record->id;
        }
    }

    /* broadcast root's list of records to all other processes */
    PMPI_Bcast(id_array, (tmp_cnt * sizeof(darshan_record_id)),
        MPI_BYTE, 0, core->mpi_comm);

    /* everyone looks to see if they opened the same records as root */
    for(i=0; i<tmp_cnt; i++)
    {
        HASH_FIND(hlink, core->name_hash, &id_array[i], sizeof(darshan_record_id), ref);
        if(ref)
        {
            /* we opened that record too, save the mod_flags */
            mod_flags[i] = ref->mod_flags;
        }
    }

    /* now allreduce so everyone agrees which records are shared and
     * which modules accessed them collectively
     */
    PMPI_Allreduce(mod_flags, global_mod_flags, tmp_cnt,
        MPI_UINT64_T, MPI_BAND, core->mpi_comm);

    j = 0;
    for(i=0; i<tmp_cnt; i++)
    {
        if(global_mod_flags[i] != 0)
        {
            (*shared_recs)[j++] = id_array[i];

            /* set global_mod_flags so we know which modules collectively
             * accessed this module. we need this info to support shared
             * record reductions
             */
            HASH_FIND(hlink, core->name_hash, &id_array[i], sizeof(darshan_record_id), ref);
            assert(ref);
            ref->global_mod_flags = global_mod_flags[i];
        }
    }
    *shared_rec_cnt = j;

    free(id_array);
    free(mod_flags);
    free(global_mod_flags);
    return;
}
#endif

/* construct the darshan log file name */
static void darshan_get_logfile_name(
    char* logfile_name, struct darshan_core_runtime* core)
{
    char* user_logfile_name;
    char* logpath;
    char* logpath_override = NULL;
#ifdef __DARSHAN_LOG_ENV
    char env_check[256];
    char* env_tok;
#endif
    uint64_t hlevel;
    char hname[HOST_NAME_MAX];
    uint64_t logmod;
    char cuser[L_cuserid] = {0};
    struct tm *start_tm;
    int jobid;
    int pid;
    time_t start_time;
    int ret;

#ifdef HAVE_MPI
    if (using_mpi && my_rank > 0)
        goto bcast;
#endif

    jobid = core->log_job_p->jobid;
    pid = core->pid;
    start_time = core->log_job_p->start_time;

    /* first, check if user specifies a complete logpath to use */
    user_logfile_name = getenv("DARSHAN_LOGFILE");
    if(user_logfile_name)
    {
        if(strlen(user_logfile_name) >= (PATH_MAX-1))
        {
            DARSHAN_WARN("user log file name too long");
            logfile_name[0] = '\0';
        }
        else
        {
            strcpy(logfile_name, user_logfile_name);
        }
    }
    else
    {
        /* otherwise, generate the log path automatically */

        /* Use DARSHAN_LOG_PATH_OVERRIDE for the value or __DARSHAN_LOG_PATH */
        logpath = getenv(DARSHAN_LOG_PATH_OVERRIDE);
        if(!logpath)
        {
#ifdef __DARSHAN_LOG_PATH
            logpath = __DARSHAN_LOG_PATH;
#endif
        }

        darshan_get_user_name(cuser);

        /* generate a random number to help differentiate the log */
        hlevel = darshan_core_wtime() * 1000000;
        (void)gethostname(hname, sizeof(hname));
        logmod = darshan_hash((void*)hname,strlen(hname),hlevel);

        /* use human readable start time format in log filename */
        start_tm = localtime(&start_time);

        /* see if darshan was configured using the --with-logpath-by-env
         * argument, which allows the user to specify an absolute path to
         * place logs via an env variable.
         */
#ifdef __DARSHAN_LOG_ENV
        /* just silently skip if the environment variable list is too big */
        if(strlen(__DARSHAN_LOG_ENV) < 256)
        {
            /* copy env variable list to a temporary buffer */
            strcpy(env_check, __DARSHAN_LOG_ENV);
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
                "%s/%s_%s_id%d-%d_%d-%d-%d-%" PRIu64 ".darshan_partial",
                logpath_override,
                cuser, __progname, jobid, pid,
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
                "%s/%d/%d/%d/%s_%s_id%d-%d_%d-%d-%d-%" PRIu64 ".darshan_partial",
                logpath, (start_tm->tm_year+1900),
                (start_tm->tm_mon+1), start_tm->tm_mday,
                cuser, __progname, jobid, pid,
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
    }

#ifdef HAVE_MPI
bcast:
    if(using_mpi)
    {
        PMPI_Bcast(logfile_name, PATH_MAX, MPI_CHAR, 0, core->mpi_comm);
        if(my_rank > 0)
            return;
    }
#endif

    if(strlen(logfile_name) == 0)
        DARSHAN_WARN("unable to determine log file path");

    return;
}

static int darshan_log_open(char *logfile_name, struct darshan_core_runtime *core,
    darshan_core_log_fh *log_fh)
{
#ifdef HAVE_MPI
    int ret;
    char *hints;
    char *tok_str;
    char *orig_tok_str;
    char *key;
    char *value;
    char *saveptr = NULL;
    MPI_Info info;

    if(using_mpi)
    {
        /* check environment variable to see if the default MPI file hints have
         * been overridden
         */
        MPI_Info_create(&info);

        hints = getenv(DARSHAN_LOG_HINTS_OVERRIDE);
        if(!hints)
        {
            hints = __DARSHAN_LOG_HINTS;
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

        /* open the darshan log file for writing using MPI */
        ret = MPI_File_open(core->mpi_comm, logfile_name,
            MPI_MODE_CREATE | MPI_MODE_WRONLY | MPI_MODE_EXCL, info, &log_fh->mpi_fh);
        MPI_Info_free(&info);
        if(ret != MPI_SUCCESS)
            return(-1);
        return(0);
    }
#endif

    /* open the darshan log file for writing */
    log_fh->nompi_fd = open(logfile_name, O_CREAT | O_WRONLY | O_EXCL, S_IRUSR);
    if(log_fh->nompi_fd < 0)
        return(-1);
    return(0);
}

static int darshan_log_write_job_record(darshan_core_log_fh log_fh,
    struct darshan_core_runtime *core, uint64_t *inout_off)
{
    void *pointers[2] = {core->log_job_p, core->log_exemnt_p};
    int lengths[2] = {sizeof(struct darshan_job), strlen(core->log_exemnt_p)};
    int comp_buf_sz = 0;
    int ret;

#ifdef HAVE_MPI
    /* only rank 0 writes the job record */
    if (using_mpi && my_rank > 0)
        return(0);
#endif

    /* compress the job info and the trailing mount/exe data */
    ret = darshan_deflate_buffer(pointers, lengths, 2,
        core->comp_buf, &comp_buf_sz);
    if(ret)
    {
        DARSHAN_WARN("error compressing job record");
    }
    else
    {
        /* write the job information, preallocing space for the log header */
        *inout_off += sizeof(struct darshan_header);

#ifdef HAVE_MPI
        MPI_Status status;
        if(using_mpi)
        {
            ret = PMPI_File_write_at(log_fh.mpi_fh, *inout_off, core->comp_buf,
                comp_buf_sz, MPI_BYTE, &status);
            if(ret != MPI_SUCCESS)
            {
                DARSHAN_WARN("error writing job record");
                ret = -1;
            }
            else
            {
                *inout_off += comp_buf_sz;
                ret = 0;
            }

            return(ret);
        }
#endif

        ret = pwrite(log_fh.nompi_fd, core->comp_buf, comp_buf_sz, *inout_off);
        if(ret != comp_buf_sz)
        {
            DARSHAN_WARN("error writing job record");
            ret = -1;
        }
        else
        {
            *inout_off += comp_buf_sz;
            ret = 0;
        }
    }

    return(ret);
}

static int darshan_log_write_name_record_hash(darshan_core_log_fh log_fh,
    struct darshan_core_runtime *core, uint64_t *inout_off)
{
    int name_rec_buf_len;
    int ret;

    name_rec_buf_len = core->name_mem_used;
#ifdef HAVE_MPI
    if(using_mpi && (my_rank > 0))
    {
        struct darshan_core_name_record_ref *ref;
        struct darshan_name_record *name_rec;
        char *my_buf, *shared_buf;
        char *tmp_p;
        int rec_len;
        int shared_buf_len;

        /* remove globally shared name records from non-zero ranks */

        name_rec = core->log_name_p;
        my_buf = core->log_name_p;
        shared_buf = core->comp_buf;
        shared_buf_len = 0;
        while(name_rec_buf_len > 0)
        {
            HASH_FIND(hlink, core->name_hash, &(name_rec->id),
                sizeof(darshan_record_id), ref);
            assert(ref);
            rec_len = sizeof(darshan_record_id) + strlen(name_rec->name) + 1;

            if(ref->global_mod_flags)
            {
                /* this record is shared globally, move to the temporary
                 * shared record buffer and update hash references
                 */
                HASH_DELETE(hlink, core->name_hash, ref);
                memcpy(shared_buf, name_rec, rec_len);
                ref->name_record = (struct darshan_name_record *)shared_buf;
                HASH_ADD(hlink, core->name_hash, name_record->id,
                    sizeof(darshan_record_id), ref);

                shared_buf += rec_len;
                shared_buf_len += rec_len;
            }
            else
            {
                /* this record is not shared, but we still may need to
                 * move it forward in our buffer and update hash references
                 */
                if(my_buf != (char *)name_rec)
                {
                    HASH_DELETE(hlink, core->name_hash, ref);
                    memcpy(my_buf, name_rec, rec_len);
                    ref->name_record =(struct darshan_name_record *)my_buf;
                    HASH_ADD(hlink, core->name_hash, name_record->id,
                        sizeof(darshan_record_id), ref);
                }
                my_buf += rec_len;
            }

            tmp_p = (char *)name_rec + rec_len;
            name_rec = (struct darshan_name_record *)tmp_p;
            name_rec_buf_len -= rec_len;
        }
        name_rec_buf_len = core->name_mem_used - shared_buf_len;

        /* append the shared records back to the end of the name record
         * buffer and update hash table references so we can still
         * reference these records as modules shutdown
         */
        name_rec = (struct darshan_name_record *)core->comp_buf;
        while(shared_buf_len > 0)
        {
            HASH_FIND(hlink, core->name_hash, &(name_rec->id),
                sizeof(darshan_record_id), ref);
            assert(ref);
            rec_len = sizeof(darshan_record_id) + strlen(name_rec->name) + 1;

            HASH_DELETE(hlink, core->name_hash, ref);
            memcpy(my_buf, name_rec, rec_len);
            ref->name_record = (struct darshan_name_record *)my_buf;
            HASH_ADD(hlink, core->name_hash, name_record->id,
                sizeof(darshan_record_id), ref);

            tmp_p = (char *)name_rec + rec_len;
            name_rec = (struct darshan_name_record *)tmp_p;
            my_buf += rec_len;
            shared_buf_len -= rec_len;
        }
    }
#endif

    /* collectively write out the record hash to the darshan log */
    ret = darshan_log_append(log_fh, core, core->log_name_p,
        name_rec_buf_len, inout_off);
    return(ret);
}

static int darshan_log_write_header(darshan_core_log_fh log_fh,
    struct darshan_core_runtime *core)
{
    int ret;

    core->log_hdr_p->comp_type = DARSHAN_ZLIB_COMP;

#ifdef HAVE_MPI
    MPI_Status status;
    if(using_mpi)
    {
        /* write out log header, after running 2 reductions on header variables:
         *  1) reduce 'partial_flag' variable to determine which modules ran out
         *     of memory for storing data
         *  2) reduce 'mod_ver' array to determine which log format version each
         *     module used for this output log
         */
        if(my_rank == 0)
        {
            PMPI_Reduce(
                MPI_IN_PLACE, &(core->log_hdr_p->partial_flag),
                1, MPI_UINT32_T, MPI_BOR, 0, core->mpi_comm);
            PMPI_Reduce(
                MPI_IN_PLACE, &(core->log_hdr_p->mod_ver),
                DARSHAN_MAX_MODS, MPI_UINT32_T, MPI_MAX, 0, core->mpi_comm);
        }
        else
        {
            PMPI_Reduce(
                &(core->log_hdr_p->partial_flag), &(core->log_hdr_p->partial_flag),
                1, MPI_UINT32_T, MPI_BOR, 0, core->mpi_comm);
            PMPI_Reduce(
                &(core->log_hdr_p->mod_ver), &(core->log_hdr_p->mod_ver),
                DARSHAN_MAX_MODS, MPI_UINT32_T, MPI_MAX, 0, core->mpi_comm);
            return(0); /* only rank 0 writes the header */
        }

        /* write the header using MPI */
        ret = PMPI_File_write_at(log_fh.mpi_fh, 0, core->log_hdr_p,
            sizeof(struct darshan_header), MPI_BYTE, &status);
        if(ret != MPI_SUCCESS)
        {
            DARSHAN_WARN("error writing darshan log header");
            ret = -1;
        }
        else
        {
            ret = 0;
        }
        return(ret);
    }
#endif

    /* write log header */
    ret = pwrite(log_fh.nompi_fd, core->log_hdr_p, sizeof(struct darshan_header), 0);
    if(ret != sizeof(struct darshan_header))
    {
        DARSHAN_WARN("error writing darshan log header");
        ret = -1;
    }
    else
    {
        ret = 0;
    }

    return(ret);
}

/* NOTE: inout_off contains the starting offset of this append at the beginning
 *       of the call, and contains the ending offset at the end of the call.
 *       This variable is only valid on the root rank (rank 0).
 */
static int darshan_log_append(darshan_core_log_fh log_fh, struct darshan_core_runtime *core,
    void *buf, int count, uint64_t *inout_off)
{
    int comp_buf_sz = 0;
    int ret;

    /* compress the input buffer */
    ret = darshan_deflate_buffer((void **)&buf, &count, 1,
        core->comp_buf, &comp_buf_sz);
    if(ret < 0)
        comp_buf_sz = 0;

#ifdef HAVE_MPI
    MPI_Offset send_off, my_off;
    MPI_Status status;

    if(using_mpi)
    {
        /* figure out where everyone is writing using scan */
        send_off = comp_buf_sz;
        if(my_rank == 0)
        {
            send_off += *inout_off; /* rank 0 knows the beginning offset */
        }

        PMPI_Scan(&send_off, &my_off, 1, MPI_OFFSET, MPI_SUM, core->mpi_comm);
        /* scan is inclusive; subtract local size back out */
        my_off -= comp_buf_sz;

        if(ret == 0)
        {
            /* no compression errors, proceed with the collective write */
            ret = PMPI_File_write_at_all(log_fh.mpi_fh, my_off,
                core->comp_buf, comp_buf_sz, MPI_BYTE, &status);
            if(ret != MPI_SUCCESS)
                ret = -1;
        }
        else
        {
            /* error during compression. preserve and return error to caller,
             * but participate in collective write to avoid deadlock.
             */
            (void)PMPI_File_write_at_all(log_fh.mpi_fh, my_off,
                core->comp_buf, comp_buf_sz, MPI_BYTE, &status);
        }

        if(nprocs > 1)
        {
            /* send the ending offset from rank (n-1) to rank 0 */
            if(my_rank == (nprocs-1))
            {
                my_off += comp_buf_sz;
                PMPI_Send(&my_off, 1, MPI_OFFSET, 0, 0, core->mpi_comm);
            }
            if(my_rank == 0)
            {
                PMPI_Recv(&my_off, 1, MPI_OFFSET, (nprocs-1), 0, core->mpi_comm, &status);

                *inout_off = my_off;
            }
        }
        else
        {
            *inout_off = my_off + comp_buf_sz;
        }

        return(ret);
    }
#endif

    ret = pwrite(log_fh.nompi_fd, core->comp_buf, comp_buf_sz, *inout_off);
    if(ret != comp_buf_sz)
        return(-1);
    *inout_off += comp_buf_sz;
    return(0);
}

void darshan_log_close(darshan_core_log_fh log_fh)
{
#ifdef HAVE_MPI
    if(using_mpi)
    {
        PMPI_File_close(&log_fh.mpi_fh);
        return;
    }
#endif

    close(log_fh.nompi_fd);
    return;
}

void darshan_log_finalize(char *logfile_name, double start_log_time)
{
#ifdef HAVE_MPI
    if(using_mpi && (my_rank > 0))
        return;
#endif

    /* finalize the darshan log file by renaming from *.darshan_partial
     * to *-<logwritetime>.darshan, indicating that this log file is complete
     * and ready for analysis
     */
        mode_t chmod_mode = S_IRUSR;
#ifdef __DARSHAN_GROUP_READABLE_LOGS
        chmod_mode |= S_IRGRP;
#endif

    if(getenv("DARSHAN_LOGFILE"))
    {
        chmod(logfile_name, chmod_mode);
    }
    else
    {
        char* tmp_index;
        double end_log_time;
        char* new_logfile_name;

        new_logfile_name = malloc(PATH_MAX);
        if(new_logfile_name)
        {
            new_logfile_name[0] = '\0';
            end_log_time = darshan_core_wtime_absolute();
            strcat(new_logfile_name, logfile_name);
            tmp_index = strstr(new_logfile_name, ".darshan_partial");
            sprintf(tmp_index, "_%d.darshan", (int)(end_log_time-start_log_time+1));
            rename(logfile_name, new_logfile_name);
            /* set permissions on log file */
            chmod(new_logfile_name, chmod_mode);
            free(new_logfile_name);
        }
    }

    return;
}

static int darshan_deflate_buffer(void **pointers, int *lengths, int count,
    char *comp_buf, int *comp_buf_length)
{
    int ret = 0;
    int i;
    int total_target = 0;
    z_stream tmp_stream;

    /* just return if there is no data */
    for(i = 0; i < count; i++)
    {
        total_target += lengths[i];
    }
    if(total_target)
    {
        total_target = 0;
    }
    else
    {
        *comp_buf_length = 0;
        return(0);
    }

    memset(&tmp_stream, 0, sizeof(tmp_stream));
    tmp_stream.zalloc = Z_NULL;
    tmp_stream.zfree = Z_NULL;
    tmp_stream.opaque = Z_NULL;

    /* initialize the zlib compression parameters */
    /* TODO: check these parameters? */
//    ret = deflateInit2(&tmp_stream, Z_DEFAULT_COMPRESSION, Z_DEFLATED,
//        15 + 16, 8, Z_DEFAULT_STRATEGY);
    ret = deflateInit(&tmp_stream, Z_DEFAULT_COMPRESSION);
    if(ret != Z_OK)
    {
        return(-1);
    }

    tmp_stream.next_out = (unsigned char *)comp_buf;
    tmp_stream.avail_out = darshan_mod_mem_quota;

    /* loop over the input pointers */
    for(i = 0; i < count; i++)
    {
        total_target += lengths[i];
        tmp_stream.next_in = pointers[i];
        tmp_stream.avail_in = lengths[i];
        /* while we have not finished consuming all of the data available to
         * this point in the loop
         */
        while(tmp_stream.total_in < total_target)
        {
            if(tmp_stream.avail_out == 0)
            {
                /* We ran out of buffer space for compression.  In theory,
                 * we could start using some of the file_array buffer space
                 * without having to malloc again.  In practice, this case 
                 * is going to be practically impossible to hit.
                 */
                deflateEnd(&tmp_stream);
                return(-1);
            }

            /* compress data */
            ret = deflate(&tmp_stream, Z_NO_FLUSH);
            if(ret != Z_OK)
            {
                deflateEnd(&tmp_stream);
                return(-1);
            }
        }
    }

    /* flush compression and end */
    ret = deflate(&tmp_stream, Z_FINISH);
    if(ret != Z_STREAM_END)
    {
        deflateEnd(&tmp_stream);
        return(-1);
    }
    deflateEnd(&tmp_stream);

    *comp_buf_length = tmp_stream.total_out;
    return(0);
}

/* free darshan core data structures to shutdown */
static void darshan_core_cleanup(struct darshan_core_runtime* core)
{
    struct darshan_core_name_record_ref *tmp, *ref;
    int i;

    HASH_ITER(hlink, core->name_hash, ref, tmp)
    {
        HASH_DELETE(hlink, core->name_hash, ref);
        free(ref);
    }

    for(i = 0; i < DARSHAN_MAX_MODS; i++)
    {
        if(core->mod_array[i])
        {
            free(core->mod_array[i]);
            core->mod_array[i] = NULL;
        }
    }

#ifndef __DARSHAN_ENABLE_MMAP_LOGS
    free(core->log_hdr_p);
    free(core->log_job_p);
    free(core->log_exemnt_p);
    free(core->log_name_p);
    free(core->log_mod_p);
#endif

#ifdef HAVE_MPI
    if(using_mpi)
        PMPI_Comm_free(&core->mpi_comm);
#endif

    if(core->comp_buf)
        free(core->comp_buf);
    free(core);

    return;
}

static void darshan_core_fork_child_cb(void)
{
    /* hold onto the original parent PID, which we will use as jobid if the user didn't
     * provide a jobid env variable
     */
    parent_pid = darshan_core->pid;
    if(!orig_parent_pid)
        orig_parent_pid = parent_pid;

    /* shutdown and re-init darshan, making sure to not write out a log file */
    darshan_core_shutdown(0);
    darshan_core_initialize(0, NULL);

    return;
}

/* crude benchmarking hook into darshan-core to benchmark Darshan
 * shutdown overhead using a variety of application I/O workloads
 */
extern void darshan_posix_shutdown_bench_setup();
extern void darshan_mpiio_shutdown_bench_setup();
#ifdef HAVE_MPI
void darshan_shutdown_bench(int argc, char **argv)
{
    /* clear out existing core runtime structure */
    if(darshan_core)
    {
        darshan_core_cleanup(darshan_core);
        darshan_core = NULL;
    }

    /***********************************************************/
    /* restart darshan */
    darshan_core_initialize(argc, argv);

    darshan_posix_shutdown_bench_setup(1);
    darshan_mpiio_shutdown_bench_setup(1);

    if(my_rank == 0)
        fprintf(stderr, "# 1 unique file per proc\n");
    PMPI_Barrier(MPI_COMM_WORLD);
    darshan_core_shutdown(1);
    darshan_core = NULL;

    sleep(1);

    /***********************************************************/
    /* restart darshan */
    darshan_core_initialize(argc, argv);

    darshan_posix_shutdown_bench_setup(2);
    darshan_mpiio_shutdown_bench_setup(2);

    if(my_rank == 0)
        fprintf(stderr, "# 1 shared file per proc\n");
    PMPI_Barrier(MPI_COMM_WORLD);
    darshan_core_shutdown(1);
    darshan_core = NULL;

    sleep(1);

    /***********************************************************/
    /* restart darshan */
    darshan_core_initialize(argc, argv);

    darshan_posix_shutdown_bench_setup(3);
    darshan_mpiio_shutdown_bench_setup(3);

    if(my_rank == 0)
        fprintf(stderr, "# 1024 unique files per proc\n");
    PMPI_Barrier(MPI_COMM_WORLD);
    darshan_core_shutdown(1);
    darshan_core = NULL;

    sleep(1);

    /***********************************************************/
    /* restart darshan */
    darshan_core_initialize(argc, argv);

    darshan_posix_shutdown_bench_setup(4);
    darshan_mpiio_shutdown_bench_setup(4);

    if(my_rank == 0)
        fprintf(stderr, "# 1024 shared files per proc\n");
    PMPI_Barrier(MPI_COMM_WORLD);
    darshan_core_shutdown(1);
    darshan_core = NULL;

    sleep(1);

    /***********************************************************/

    return;
}
#else
void darshan_shutdown_bench(int argc, char **argv)
{
    fprintf(stderr, "Error: darshan_shutdown_bench() not implemented for non-mpi builds.\n");
    assert(0);
    return;
}
#endif

/* ********************************************************* */

void darshan_core_register_module(
    darshan_module_id mod_id,
    darshan_module_funcs mod_funcs,
    size_t *inout_mod_buf_size,
    int *rank,
    int *sys_mem_alignment)
{
    struct darshan_core_module* mod;
    size_t mod_mem_req = *inout_mod_buf_size;
    size_t mod_mem_avail;

    *inout_mod_buf_size = 0;

    DARSHAN_CORE_LOCK();
    if((darshan_core == NULL) ||
       (mod_id >= DARSHAN_MAX_MODS) ||
       (darshan_core->mod_array[mod_id] != NULL))
    {
        /* just return if darshan not initialized, the module id
         * is invalid, or if the module is already registered
         */
        DARSHAN_CORE_UNLOCK();
        return;
    }

    mod = malloc(sizeof(*mod));
    if(!mod)
    {
        DARSHAN_CORE_UNLOCK();
        return;
    }
    memset(mod, 0, sizeof(*mod));

    /* set module's record buffer and max memory usage */
    mod_mem_avail = darshan_mod_mem_quota - darshan_core->mod_mem_used;
    if(mod_mem_avail >= mod_mem_req)
        mod->rec_mem_avail = mod_mem_req;
    else
        mod->rec_mem_avail = mod_mem_avail;
    mod->rec_buf_start = darshan_core->log_mod_p + darshan_core->mod_mem_used;
    mod->rec_buf_p = mod->rec_buf_start;
    mod->mod_funcs = mod_funcs;

    /* register module with darshan */
    darshan_core->mod_array[mod_id] = mod;
    darshan_core->mod_mem_used += mod->rec_mem_avail;
    darshan_core->log_hdr_p->mod_ver[mod_id] = darshan_module_versions[mod_id];
#ifdef __DARSHAN_ENABLE_MMAP_LOGS
    darshan_core->log_hdr_p->mod_map[mod_id].off =
        ((char *)mod->rec_buf_start - (char *)darshan_core->log_hdr_p);
#endif

    *inout_mod_buf_size = mod->rec_mem_avail;
    DARSHAN_CORE_UNLOCK();

    /* set the memory alignment and calling process's rank, if desired */
    if(sys_mem_alignment)
        *sys_mem_alignment = darshan_mem_alignment;
    if(rank)
        *rank = my_rank;

    return;
}

/* NOTE: we currently don't really have a simple way of returning the
 * memory allocated to this module back to darshan to hand out to
 * other modules, so all we do is disable the module so darshan does
 * not attempt to call into it at shutdown time
 */
void darshan_core_unregister_module(
    darshan_module_id mod_id)
{
    DARSHAN_CORE_LOCK();
    if(!darshan_core)
    {
        DARSHAN_CORE_UNLOCK();
        return;
    }

    /* update darshan internal structures and header */
    free(darshan_core->mod_array[mod_id]);
    darshan_core->mod_array[mod_id] = NULL;
    darshan_core->log_hdr_p->mod_ver[mod_id] = 0;
#ifdef __DARSHAN_ENABLE_MMAP_LOGS
    darshan_core->log_hdr_p->mod_map[mod_id].off =
        darshan_core->log_hdr_p->mod_map[mod_id].len = 0;
#endif

    DARSHAN_CORE_UNLOCK();
    return;
}

darshan_record_id darshan_core_gen_record_id(
    const char *name)
{
    /* hash the input name to get a unique id for this record */
    return darshan_hash((unsigned char *)name, strlen(name), 0);
}

void *darshan_core_register_record(
    darshan_record_id rec_id,
    const char *name,
    darshan_module_id mod_id,
    int rec_len,
    struct darshan_fs_info *fs_info)
{
    struct darshan_core_name_record_ref *ref;
    void *rec_buf;
    int ret;

    DARSHAN_CORE_LOCK();
    if(!darshan_core)
    {
        DARSHAN_CORE_UNLOCK();
        return(NULL);
    }

    /* check to see if this module has enough space to store a new record */
    if(darshan_core->mod_array[mod_id]->rec_mem_avail < rec_len)
    {
        DARSHAN_MOD_FLAG_SET(darshan_core->log_hdr_p->partial_flag, mod_id);
        DARSHAN_CORE_UNLOCK();
        return(NULL);
    }

    /* register a name record if a name is given for this record */
    if(name)
    {
        /* check to see if we've already stored the id->name mapping for
         * this record, and add a new name record if not
         */
        HASH_FIND(hlink, darshan_core->name_hash, &rec_id,
            sizeof(darshan_record_id), ref);
        if(!ref)
        {
            ret = darshan_add_name_record_ref(darshan_core, rec_id, name, mod_id);
            if(ret == 0)
            {
                DARSHAN_MOD_FLAG_SET(darshan_core->log_hdr_p->partial_flag, mod_id);
                DARSHAN_CORE_UNLOCK();
                return(NULL);
            }
        }
        else
        {
            DARSHAN_MOD_FLAG_SET(ref->mod_flags, mod_id);
        }
    }

    rec_buf = darshan_core->mod_array[mod_id]->rec_buf_p;
    darshan_core->mod_array[mod_id]->rec_buf_p += rec_len;
    darshan_core->mod_array[mod_id]->rec_mem_avail -= rec_len;
#ifdef __DARSHAN_ENABLE_MMAP_LOGS
    darshan_core->log_hdr_p->mod_map[mod_id].len += rec_len;
#endif
    DARSHAN_CORE_UNLOCK();

    if(fs_info)
        darshan_fs_info_from_path(name, fs_info);

    return(rec_buf);;
}

char *darshan_core_lookup_record_name(darshan_record_id rec_id)
{
    struct darshan_core_name_record_ref *ref;
    char *name = NULL;

    DARSHAN_CORE_LOCK();
    HASH_FIND(hlink, darshan_core->name_hash, &rec_id,
        sizeof(darshan_record_id), ref);
    if(ref)
        name = ref->name_record->name;
    DARSHAN_CORE_UNLOCK();

    return(name);
}

void darshan_instrument_fs_data(int fs_type, const char *path, int fd)
{
#ifdef DARSHAN_LUSTRE
    /* allow Lustre to generate a record if we configured with Lustre support */
    /* XXX: Note that we short-circuit this Lustre file system check and try to
     * query Lustre striping stats for *all* files instrumented by Darshan (i.e.,
     * Lustre files or not). We do this so that symlinks to Lustre files are
     * properly instrumented, since these symlinks might live on other non-Lustre
     * file systems. We have instrumented this Lustre file system check on a number
     * of file systems and believe it is low overhead enough to not be noticable by
     * users.
     */
    if(1 || fs_type == LL_SUPER_MAGIC)
    {
        darshan_instrument_lustre_file(path, fd);
        return;
    }
#endif
    return;
}

/* retrieve the wtime relative to execution start time */
double darshan_core_wtime()
{
    double wtime_offset;

    DARSHAN_CORE_LOCK();
    if(!darshan_core)
    {
        DARSHAN_CORE_UNLOCK();
        return(0);
    }
    else
    {
        wtime_offset = darshan_core->wtime_offset;
    }
    DARSHAN_CORE_UNLOCK();

    return(darshan_core_wtime_absolute() - wtime_offset);
}

/* retrieve absolute wtime */
static double darshan_core_wtime_absolute(void)
{
#ifdef HAVE_MPI
    if(using_mpi)
        return(PMPI_Wtime());
#endif

    struct timeval tval;
    gettimeofday(&tval, NULL);
    return(tval.tv_sec + (tval.tv_usec / 1000000.0));
}

#ifdef DARSHAN_PRELOAD
extern int (*__real_vfprintf)(FILE *stream, const char *format, va_list);
#else
extern int __real_vfprintf(FILE *stream, const char *format, va_list);
#endif
void darshan_core_fprintf(
    FILE *stream, const char *format, ...)
{
    va_list ap;

    MAP_OR_FAIL(vfprintf);

    va_start(ap, format);
    __real_vfprintf(stream, format, ap);
    va_end(ap);

    return;
}

int darshan_core_excluded_path(const char *path)
{
    char *exclude, *include;
    int tmp_index = 0;
    int tmp_jndex;

    /* if user has set DARSHAN_EXCLUDE_DIRS, override the default ones */
    if (user_darshan_path_exclusions != NULL) {
        while((exclude = user_darshan_path_exclusions[tmp_index++])) {
            if(!(strncmp(exclude, path, strlen(exclude))))
                return(1);
        }
    }
    else
    {
        /* scan blacklist for paths to exclude */
        while((exclude = darshan_path_exclusions[tmp_index++])) {
            if(!(strncmp(exclude, path, strlen(exclude)))) {
                /* before excluding path, ensure it's not in whitelist */
                tmp_jndex = 0;
                while((include = darshan_path_inclusions[tmp_jndex++])) {
                    if(!(strncmp(include, path, strlen(include))))
                        return(0); /* whitelist hits are always tracked */
                }
                return(1); /* if not in whitelist, then blacklist it */
            }
        }
    }

    /* if not in blacklist, no problem */
    return(0);
}

int darshan_core_disabled_instrumentation()
{
    int ret;

    DARSHAN_CORE_LOCK();
    if(darshan_core)
        ret = 0;
    else
        ret = 1;
    DARSHAN_CORE_UNLOCK();

    return(ret);
}

/*
 * Local variables:
 *  c-indent-level: 4
 *  c-basic-offset: 4
 * End:
 *
 * vim: ts=8 sts=4 sw=4 expandtab
 */
