/*
 * Copyright (C) 2015 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

#define _XOPEN_SOURCE 500
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
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <sys/vfs.h>
#include <zlib.h>
#include <mpi.h>
#include <assert.h>

#include "uthash.h"
#include "darshan.h"
#include "darshan-core.h"
#include "darshan-dynamic.h"

extern char* __progname;
extern char* __progname_full;

/* internal variable delcarations */
static struct darshan_core_runtime *darshan_core = NULL;
static pthread_mutex_t darshan_core_mutex = PTHREAD_RECURSIVE_MUTEX_INITIALIZER_NP;
static int my_rank = -1;
static int nprocs = -1;
static int darshan_mem_alignment = 1;

/* paths prefixed with the following directories are not traced by darshan */
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
NULL
};

#ifdef DARSHAN_BGQ
extern void bgq_runtime_initialize();
#endif

/* array of init functions for modules which need to be statically
 * initialized by darshan at startup time
 */
void (*mod_static_init_fns[])(void) =
{
#ifdef DARSHAN_BGQ
    &bgq_runtime_initialize,
#endif
    NULL
};

#define DARSHAN_CORE_LOCK() pthread_mutex_lock(&darshan_core_mutex)
#define DARSHAN_CORE_UNLOCK() pthread_mutex_unlock(&darshan_core_mutex)

/* FS mount information */
#define DARSHAN_MAX_MNTS 64
#define DARSHAN_MAX_MNT_PATH 256
#define DARSHAN_MAX_MNT_TYPE 32
struct mnt_data
{
    int block_size;
    char path[DARSHAN_MAX_MNT_PATH];
    char type[DARSHAN_MAX_MNT_TYPE];
};
static struct mnt_data mnt_data_array[DARSHAN_MAX_MNTS];
static int mnt_data_count = 0;

/* prototypes for internal helper functions */
static void darshan_get_logfile_name(
    char* logfile_name, int jobid, struct tm* start_tm);
static void darshan_log_record_hints_and_ver(
    struct darshan_core_runtime* core);
static void darshan_get_exe_and_mounts_root(
    struct darshan_core_runtime *core, int argc, char **argv);
static void darshan_get_exe_and_mounts(
    struct darshan_core_runtime *core, int argc, char **argv);
static void darshan_block_size_from_path(
    const char *path, int *block_size);
static void darshan_get_shared_records(
    struct darshan_core_runtime *core, darshan_record_id **shared_recs,
    int *shared_rec_cnt);
static int darshan_log_open_all(
    char *logfile_name, MPI_File *log_fh);
static int darshan_deflate_buffer(
    void **pointers, int *lengths, int count, char *comp_buf,
    int *comp_buf_length);
static int darshan_log_write_record_hash(
    MPI_File log_fh, struct darshan_core_runtime *core,
    uint64_t *inout_off);
static int darshan_log_append_all(
    MPI_File log_fh, struct darshan_core_runtime *core, void *buf,
    int count, uint64_t *inout_off);
static void darshan_core_cleanup(
    struct darshan_core_runtime* core);

/* *********************************** */

void darshan_core_initialize(int argc, char **argv)
{
    struct darshan_core_runtime *init_core = NULL;
    int internal_timing_flag = 0;
    double init_start, init_time, init_max;
    char mmap_log_name[PATH_MAX];
    int mmap_fd;
    int mmap_size;
    int sys_page_size;
    char *envstr;
    char *jobid_str;
    int jobid;
    int ret;
    int tmpval;
    int i;

    DARSHAN_MPI_CALL(PMPI_Comm_size)(MPI_COMM_WORLD, &nprocs);
    DARSHAN_MPI_CALL(PMPI_Comm_rank)(MPI_COMM_WORLD, &my_rank);

    if(getenv("DARSHAN_INTERNAL_TIMING"))
        internal_timing_flag = 1;

    if(internal_timing_flag)
        init_start = DARSHAN_MPI_CALL(PMPI_Wtime)();

    /* setup darshan runtime if darshan is enabled and hasn't been initialized already */
    if(!getenv("DARSHAN_DISABLE") && !darshan_core)
    {
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
        if (darshan_mem_alignment < 1)
        {
            darshan_mem_alignment = 1;
        }

        /* allocate structure to track darshan core runtime information */
        init_core = malloc(sizeof(*init_core));
        if(init_core)
        {
            memset(init_core, 0, sizeof(*init_core));
            init_core->wtime_offset = DARSHAN_MPI_CALL(PMPI_Wtime)();

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
                jobid = getpid();
            }

            sys_page_size = sysconf(_SC_PAGESIZE);
            assert(sys_page_size > 0);

            /* set the size of the mmap, making sure to round up to the
             * nearest page size. One mmap chunk is used for the job-level
             * metadata, and the rest are statically assigned to modules
             */
            mmap_size = (1 + DARSHAN_MAX_MODS) * DARSHAN_MMAP_CHUNK_SIZE;
            if(mmap_size % sys_page_size)
                mmap_size = ((mmap_size / sys_page_size) + 1) * sys_page_size;

            /* construct a unique temporary log file name for this process
             * to write mmap log data to
             */
            snprintf(mmap_log_name, PATH_MAX, "/tmp/darshan_job%d.%d",
                jobid, my_rank);

            /* create the temporary mmapped darshan log */
            mmap_fd = open(mmap_log_name, O_CREAT|O_RDWR|O_EXCL , 0644);
            if(mmap_fd < 0)
            {
                fprintf(stderr, "darshan library warning: "
                    "unable to create darshan log file %s\n", mmap_log_name);
                free(init_core);
                return;
            }

            /* allocate the necessary space in the log file */
            ret = ftruncate(mmap_fd, mmap_size);
            if(ret < 0)
            {
                fprintf(stderr, "darshan library warning: "
                    "unable to allocate darshan log file %s\n", mmap_log_name);
                free(init_core);
                close(mmap_fd);
                unlink(mmap_log_name);
                return;
            }

            /* memory map buffers for getting at least some summary i/o data
             * into a log file if darshan does not shut down properly
             */
            init_core->mmap_p = mmap(NULL, mmap_size, PROT_WRITE, MAP_SHARED,
                mmap_fd, 0);
            if(init_core->mmap_p == MAP_FAILED)
            {
                fprintf(stderr, "darshan library warning: "
                    "unable to mmap darshan log file %s\n", mmap_log_name);
                free(init_core);
                close(mmap_fd);
                unlink(mmap_log_name);
                return;
            }

            /* close darshan log file (this does *not* unmap the log file) */
            close(mmap_fd);

            /* set the pointers for each log file region */
            init_core->mmap_job_p = (struct darshan_job *)(init_core->mmap_p);
            init_core->mmap_exe_mnt_p =
                (char *)(((char *)init_core->mmap_p) + sizeof(struct darshan_job));
            init_core->mmap_mod_p =
                (void *)(((char *)init_core->mmap_p) + DARSHAN_MMAP_CHUNK_SIZE);

            /* set known job-level metadata files for the log file */
            init_core->mmap_job_p->uid = getuid();
            init_core->mmap_job_p->start_time = time(NULL);
            init_core->mmap_job_p->nprocs = nprocs;
            init_core->mmap_job_p->jobid = (int64_t)jobid;

            /* if we are using any hints to write the log file, then record those
             * hints with the darshan job information
             */
            darshan_log_record_hints_and_ver(init_core);

            /* collect information about command line and mounted file systems */
            darshan_get_exe_and_mounts(init_core, argc, argv);

            /* bootstrap any modules with static initialization routines */
            i = 0;
            while(mod_static_init_fns[i])
            {
                (*mod_static_init_fns[i])();
                i++;
            }

            darshan_core = init_core;
        }
    }

    if(internal_timing_flag)
    {
        init_time = DARSHAN_MPI_CALL(PMPI_Wtime)() - init_start;
        DARSHAN_MPI_CALL(PMPI_Reduce)(&init_time, &init_max, 1,
            MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
        if(my_rank == 0)
        {
            fprintf(stderr, "#darshan:<op>\t<nprocs>\t<time>\n");
            fprintf(stderr, "darshan:init\t%d\t%f\n", nprocs, init_max);
        }
    }

    return;
}

void darshan_core_shutdown()
{
    int i;
    struct darshan_core_runtime *final_core;
    int internal_timing_flag = 0;
    double start_log_time;
    double tm_end;

    if(getenv("DARSHAN_INTERNAL_TIMING"))
        internal_timing_flag = 1;

    if(internal_timing_flag)
        start_log_time = DARSHAN_MPI_CALL(PMPI_Wtime)();

    /* disable darhan-core while we shutdown */
    DARSHAN_CORE_LOCK();
    if(!darshan_core)
    {
        DARSHAN_CORE_UNLOCK();
        return;
    }
    final_core = darshan_core;
    darshan_core = NULL;

    for(i = 0; i < DARSHAN_MAX_MODS; i++)
    {
        if(final_core->mod_array[i])
        {
            final_core->mod_array[i]->mod_funcs.begin_shutdown();
        }
    }
    DARSHAN_CORE_UNLOCK();

    final_core->mmap_job_p->end_time = time(NULL);

    darshan_core_cleanup(final_core);

    if(internal_timing_flag)
    {
        double all_tm, all_slowest;

        tm_end = DARSHAN_MPI_CALL(PMPI_Wtime)();

        all_tm = tm_end - start_log_time;

        DARSHAN_MPI_CALL(PMPI_Reduce)(&all_tm, &all_slowest, 1,
            MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);

        if(my_rank == 0)
        {
            fprintf(stderr, "#darshan:<op>\t<nprocs>\t<time>\n");
            fprintf(stderr, "darshan:core_shutdown\t%d\t%f\n", nprocs, all_slowest);
        }
    }

    return;
}

/* *********************************** */

/* construct the darshan log file name */
static void darshan_get_logfile_name(char* logfile_name, int jobid, struct tm* start_tm)
{
    char* user_logfile_name;
    char* logpath;
    char* logname_string;
    char* logpath_override = NULL;
#ifdef __DARSHAN_LOG_ENV
    char env_check[256];
    char* env_tok;
#endif
    uint64_t hlevel;
    char hname[HOST_NAME_MAX];
    uint64_t logmod;
    char cuser[L_cuserid] = {0};
    int ret;

    /* first, check if user specifies a complete logpath to use */
    user_logfile_name = getenv("DARSHAN_LOGFILE");
    if(user_logfile_name)
    {
        if(strlen(user_logfile_name) >= (PATH_MAX-1))
        {
            fprintf(stderr, "darshan library warning: user log file name too long.\n");
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
    }

    return;
}

/* record any hints used to write the darshan log in the log header */
static void darshan_log_record_hints_and_ver(struct darshan_core_runtime* core)
{
    char* hints;
    char* header_hints;
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

    header_hints = strdup(hints);
    if(!header_hints)
        return;

    meta_remain = DARSHAN_JOB_METADATA_LEN -
        strlen(core->mmap_job_p->metadata) - 1;
    if(meta_remain >= (strlen(PACKAGE_VERSION) + 9))
    {
        sprintf(core->mmap_job_p->metadata, "lib_ver=%s\n", PACKAGE_VERSION);
        meta_remain -= (strlen(PACKAGE_VERSION) + 9);
    }
    if(meta_remain >= (3 + strlen(header_hints)))
    {
        m = core->mmap_job_p->metadata + strlen(core->mmap_job_p->metadata);
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

static int mnt_data_cmp(const void* a, const void* b)
{
    const struct mnt_data *d_a = (const struct mnt_data*)a;
    const struct mnt_data *d_b = (const struct mnt_data*)b;

    if(strlen(d_a->path) > strlen(d_b->path))
        return(-1);
    else if(strlen(d_a->path) < strlen(d_b->path))
        return(1);
    else
        return(0);
}

/* adds an entry to table of mounted file systems */
static void add_entry(char* buf, int* space_left, struct mntent *entry)
{
    int ret;
    char tmp_mnt[256];
    struct statfs statfsbuf;

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
    if(ret == 0 && statfsbuf.f_type != LL_SUPER_MAGIC)
        mnt_data_array[mnt_data_count].block_size = statfsbuf.f_bsize;
    else if(ret == 0 && statfsbuf.f_type == LL_SUPER_MAGIC)
        mnt_data_array[mnt_data_count].block_size = 1024*1024;
    else
        mnt_data_array[mnt_data_count].block_size = 4096;

    /* store mount information for use in header of darshan log */
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

/* darshan_get_exe_and_mounts_root()
 *
 * collects command line and list of mounted file systems into a string that
 * will be stored with the job header
 */
static void darshan_get_exe_and_mounts_root(struct darshan_core_runtime *core,
    int argc, char **argv)
{
    FILE* tab;
    struct mntent *entry;
    char* exclude;
    char* truncate_string = "<TRUNCATED>";
    int truncate_offset;
    int space_left = DARSHAN_EXE_LEN;
    int i;
    int tmp_index = 0;
    int skip = 0;

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

    /* record exe and arguments */
    for(i=0; i<argc; i++)
    {
        strncat(core->mmap_exe_mnt_p, argv[i], space_left);
        space_left = DARSHAN_EXE_LEN-strlen(core->mmap_exe_mnt_p);
        if(i < (argc-1))
        {
            strncat(core->mmap_exe_mnt_p, " ", space_left);
            space_left = DARSHAN_EXE_LEN-strlen(core->mmap_exe_mnt_p);
        }
    }

    /* if we don't see any arguments, then use glibc symbol to get
     * program name at least (this happens in fortran)
     */
    if(argc == 0)
    {
        strncat(core->mmap_exe_mnt_p, __progname_full, space_left);
        space_left = DARSHAN_EXE_LEN-strlen(core->mmap_exe_mnt_p);
        strncat(core->mmap_exe_mnt_p, " <unknown args>", space_left);
        space_left = DARSHAN_EXE_LEN-strlen(core->mmap_exe_mnt_p);
    }

    if(space_left == 0)
    {
        /* we ran out of room; mark that string was truncated */
        truncate_offset = DARSHAN_EXE_LEN - strlen(truncate_string);
        sprintf(&core->mmap_exe_mnt_p[truncate_offset], "%s",
            truncate_string);
    }

    /* we make two passes through mounted file systems; in the first pass we
     * grab any non-nfs mount points, then on the second pass we grab nfs
     * mount points
     */

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

        add_entry(core->mmap_exe_mnt_p, &space_left, entry);
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

        add_entry(core->mmap_exe_mnt_p, &space_left, entry);
    }
    endmntent(tab);

    /* Sort mount points in order of longest path to shortest path.  This is
     * necessary so that if we try to match file paths to mount points later
     * we don't match on "/" every time.
     */
    qsort(mnt_data_array, mnt_data_count, sizeof(mnt_data_array[0]), mnt_data_cmp);
    return;
}

/* darshan_get_exe_and_mounts()
 *
 * collects command line and list of mounted file systems into a string that
 * will be stored with the job header
 */
static void darshan_get_exe_and_mounts(struct darshan_core_runtime *core,
    int argc, char **argv)
{
    if(my_rank == 0)
    {
        darshan_get_exe_and_mounts_root(core, argc, argv);
    }

    /* broadcast mount count to all nodes */
    DARSHAN_MPI_CALL(PMPI_Bcast)(&mnt_data_count, 1, MPI_INT, 0,
        MPI_COMM_WORLD);
    /* broadcast mount data to all nodes */
    DARSHAN_MPI_CALL(PMPI_Bcast)(mnt_data_array,
        mnt_data_count*sizeof(mnt_data_array[0]), MPI_BYTE, 0, MPI_COMM_WORLD);

    return;
}

static void darshan_block_size_from_path(const char *path, int *block_size)
{
    int i;
    *block_size = -1;

    for(i=0; i<mnt_data_count; i++)
    {
        if(!(strncmp(mnt_data_array[i].path, path, strlen(mnt_data_array[i].path))))
        {
            *block_size = mnt_data_array[i].block_size;
            return;
        }
    }

    return;
}

static void darshan_get_shared_records(struct darshan_core_runtime *core,
    darshan_record_id **shared_recs, int *shared_rec_cnt)
{
    int i, j;
    int tmp_cnt = core->rec_count;
    struct darshan_core_record_ref *tmp, *ref;
    darshan_record_id *id_array;
    uint64_t *mod_flags;
    uint64_t *global_mod_flags;

    /* broadcast root's number of records to all other processes */
    DARSHAN_MPI_CALL(PMPI_Bcast)(&tmp_cnt, 1, MPI_INT, 0, MPI_COMM_WORLD);

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
        HASH_ITER(hlink, core->rec_hash, ref, tmp)
        {
            id_array[i++] = ref->rec.id;           
        }
    }

    /* broadcast root's list of records to all other processes */
    DARSHAN_MPI_CALL(PMPI_Bcast)(id_array, (tmp_cnt * sizeof(darshan_record_id)),
        MPI_BYTE, 0, MPI_COMM_WORLD);

    /* everyone looks to see if they opened the same records as root */
    for(i=0; i<tmp_cnt; i++)
    {
        HASH_FIND(hlink, core->rec_hash, &id_array[i], sizeof(darshan_record_id), ref);
        if(ref)
        {
            /* we opened that record too, save the mod_flags */
            mod_flags[i] = ref->mod_flags;
        }
    }

    /* now allreduce so everyone agrees which files are shared and
     * which modules accessed them collectively
     */
    DARSHAN_MPI_CALL(PMPI_Allreduce)(mod_flags, global_mod_flags, tmp_cnt,
        MPI_UINT64_T, MPI_BAND, MPI_COMM_WORLD);

    j = 0;
    for(i=0; i<tmp_cnt; i++)
    {
        if(global_mod_flags[i] != 0)
        {
            (*shared_recs)[j++] = id_array[i];

            /* set global_mod_flags so we know which modules collectively
             * accessed this module. we need this info to support shared
             * file reductions
             */
            HASH_FIND(hlink, core->rec_hash, &id_array[i], sizeof(darshan_record_id), ref);
            assert(ref);
            ref->global_mod_flags = global_mod_flags[i];
        }
    }
    *shared_rec_cnt = j;

    return;
}

static int darshan_log_open_all(char *logfile_name, MPI_File *log_fh)
{
    char *hints;
    char *tok_str;
    char *orig_tok_str;
    char *key;
    char *value;
    char *saveptr = NULL;
    int ret;
    MPI_Info info;

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

    /* open the darshan log file for writing */
    ret = DARSHAN_MPI_CALL(PMPI_File_open)(MPI_COMM_WORLD, logfile_name,
        MPI_MODE_CREATE | MPI_MODE_WRONLY | MPI_MODE_EXCL, info, log_fh);
    if(ret != MPI_SUCCESS)
        return(-1);

    MPI_Info_free(&info);
    return(0);
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
    tmp_stream.avail_out = DARSHAN_COMP_BUF_SIZE;

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

/* NOTE: the map written to file may contain duplicate id->name entries if a
 *       record is opened by multiple ranks, but not all ranks
 */
static int darshan_log_write_record_hash(MPI_File log_fh, struct darshan_core_runtime *core,
    uint64_t *inout_off)
{
    int ret;
    struct darshan_core_record_ref *ref, *tmp;
    uint32_t name_len;
    size_t record_sz;
    size_t hash_buf_sz = 0;
    char *hash_buf;
    char *hash_buf_off;

    /* allocate a buffer to store at most 64 bytes for each registered record */
    /* NOTE: this buffer may be reallocated if estimate is too small */
    hash_buf_sz = core->rec_count * 64;
    hash_buf = malloc(hash_buf_sz);
    if(!hash_buf)
    {
        return(-1);
    }

    /* serialize the record hash into a buffer for writing */
    hash_buf_off = hash_buf;
    HASH_ITER(hlink, core->rec_hash, ref, tmp)
    {
        /* to avoid duplicate records, only rank 0 will write shared records */
        if(my_rank > 0 && ref->global_mod_flags)
            continue;

        name_len = strlen(ref->rec.name);
        record_sz = sizeof(darshan_record_id) + sizeof(uint32_t) + name_len;
        /* make sure there is room in the buffer for this record */
        if((hash_buf_off + record_sz) > (hash_buf + hash_buf_sz))
        {
            char *tmp_buf;
            size_t old_buf_sz;

            /* if no room, reallocate the hash buffer at twice the current size */
            old_buf_sz = hash_buf_off - hash_buf;
            hash_buf_sz *= 2;
            tmp_buf = malloc(hash_buf_sz);
            if(!tmp_buf)
            {
                free(hash_buf);
                return(-1);
            }

            memcpy(tmp_buf, hash_buf, old_buf_sz);
            free(hash_buf);
            hash_buf = tmp_buf;
            hash_buf_off = hash_buf + old_buf_sz;
        }

        /* now serialize the record into the hash buffer.
         * NOTE: darshan record hash serialization method: 
         *          ... darshan_record_id | (uint32_t) path_len | path ...
         */
        *((darshan_record_id *)hash_buf_off) = ref->rec.id;
        hash_buf_off += sizeof(darshan_record_id);
        *((uint32_t *)hash_buf_off) = name_len;
        hash_buf_off += sizeof(uint32_t);
        memcpy(hash_buf_off, ref->rec.name, name_len);
        hash_buf_off += name_len;
    }
    hash_buf_sz = hash_buf_off - hash_buf;

    /* collectively write out the record hash to the darshan log */
    ret = darshan_log_append_all(log_fh, core, hash_buf, hash_buf_sz, inout_off);

    free(hash_buf);

    return(ret);
}

/* NOTE: inout_off contains the starting offset of this append at the beginning
 *       of the call, and contains the ending offset at the end of the call.
 *       This variable is only valid on the root rank (rank 0).
 */
static int darshan_log_append_all(MPI_File log_fh, struct darshan_core_runtime *core,
    void *buf, int count, uint64_t *inout_off)
{
    MPI_Offset send_off, my_off;
    MPI_Status status;
    int comp_buf_sz = 0;
    int ret;

    /* compress the input buffer */
    ret = darshan_deflate_buffer((void **)&buf, &count, 1,
        core->comp_buf, &comp_buf_sz);
    if(ret < 0)
        comp_buf_sz = 0;

    /* figure out where everyone is writing using scan */
    send_off = comp_buf_sz;
    if(my_rank == 0)
    {
        send_off += *inout_off; /* rank 0 knows the beginning offset */
    }

    DARSHAN_MPI_CALL(PMPI_Scan)(&send_off, &my_off, 1, MPI_OFFSET,
        MPI_SUM, MPI_COMM_WORLD);
    /* scan in inclusive; subtract local size back out */
    my_off -= comp_buf_sz;

    if(ret == 0)
    {
        /* no compression errors, proceed with the collective write */
        ret = DARSHAN_MPI_CALL(PMPI_File_write_at_all)(log_fh, my_off,
            core->comp_buf, comp_buf_sz, MPI_BYTE, &status);
    }
    else
    {
        /* error during compression. preserve and return error to caller,
         * but participate in collective write to avoid deadlock.
         */
        (void)DARSHAN_MPI_CALL(PMPI_File_write_at_all)(log_fh, my_off,
            core->comp_buf, comp_buf_sz, MPI_BYTE, &status);
    }

    if(nprocs > 1)
    {
        /* send the ending offset from rank (n-1) to rank 0 */
        if(my_rank == (nprocs-1))
        {
            my_off += comp_buf_sz;
            DARSHAN_MPI_CALL(PMPI_Send)(&my_off, 1, MPI_OFFSET, 0, 0,
                MPI_COMM_WORLD);
        }
        if(my_rank == 0)
        {
            DARSHAN_MPI_CALL(PMPI_Recv)(&my_off, 1, MPI_OFFSET, (nprocs-1), 0,
                MPI_COMM_WORLD, &status);

            *inout_off = my_off;
        }
    }
    else
    {
        *inout_off = my_off + comp_buf_sz;
    }

    if(ret != 0)
        return(-1);
    return(0);
}

/* free darshan core data structures to shutdown */
static void darshan_core_cleanup(struct darshan_core_runtime* core)
{
    struct darshan_core_record_ref *tmp, *ref;
    int i;

    HASH_ITER(hlink, core->rec_hash, ref, tmp)
    {
        HASH_DELETE(hlink, core->rec_hash, ref);
        free(ref->rec.name);
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

    free(core);

    return;
}

/* ********************************************************* */

void darshan_core_register_module(
    darshan_module_id mod_id,
    struct darshan_module_funcs *funcs,
    int *my_rank,
    int *mod_mem_limit,
    void **mmap_buf,
    int *mmap_buf_size,
    int *sys_mem_alignment)
{
    int ret;
    int tmpval;
    struct darshan_core_module* mod;
    char *mod_mem_str = NULL;
    *mod_mem_limit = 0;

    if(!darshan_core || (mod_id >= DARSHAN_MAX_MODS))
        return;

    if(sys_mem_alignment)
        *sys_mem_alignment = darshan_mem_alignment;

    /* get the calling process's rank */
    DARSHAN_MPI_CALL(PMPI_Comm_rank)(MPI_COMM_WORLD, my_rank);

    /* pass back the mmap buffer this module can use to persist
     * some module data (mmap_buf_size at max) even in the case
     * where darshan is not finalized
     */
    *mmap_buf =
        (void *)(((char *)darshan_core->mmap_mod_p) + (mod_id * DARSHAN_MMAP_CHUNK_SIZE));
    *mmap_buf_size = DARSHAN_MMAP_CHUNK_SIZE;

    /* see if this module is already registered */
    DARSHAN_CORE_LOCK();
    if(darshan_core->mod_array[mod_id])
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
    mod->id = mod_id;
    mod->mod_funcs = *funcs;

    /* register module with darshan */
    darshan_core->mod_array[mod_id] = mod;

    /* get the calling process's rank */
    DARSHAN_MPI_CALL(PMPI_Comm_rank)(MPI_COMM_WORLD, my_rank);

    /* set the maximum amount of memory this module can use */
    mod_mem_str = getenv(DARSHAN_MOD_MEM_OVERRIDE);
    if(mod_mem_str)
    {
        ret = sscanf(mod_mem_str, "%d", &tmpval);
        /* silently ignore if the env variable is set poorly */
        if(ret == 1 && tmpval > 0)
            *mod_mem_limit = (tmpval * 1024 * 1024); /* convert to MiB */
        else
            *mod_mem_limit = DARSHAN_MOD_MEM_MAX;
    }
    else
    {
        *mod_mem_limit = DARSHAN_MOD_MEM_MAX;
    }
    DARSHAN_CORE_UNLOCK();

    return;
}

/* TODO: test */
void darshan_core_unregister_module(
    darshan_module_id mod_id)
{
    struct darshan_core_record_ref *ref, *tmp;

    if(!darshan_core)
        return;

    DARSHAN_CORE_LOCK();

    /* iterate all records and disassociate this module from them */
    HASH_ITER(hlink, darshan_core->rec_hash, ref, tmp)
    {
        darshan_core_unregister_record(ref->rec.id, mod_id);
    }

    free(darshan_core->mod_array[mod_id]);
    darshan_core->mod_array[mod_id] = NULL;

    DARSHAN_CORE_UNLOCK();

    return;
}

void darshan_core_register_record(
    void *name,
    int len,
    darshan_module_id mod_id,
    int printable_flag,
    int mod_limit_flag,
    darshan_record_id *rec_id,
    int *file_alignment)
{
    darshan_record_id tmp_rec_id;
    struct darshan_core_record_ref *ref;

    *rec_id = 0;

    if(!darshan_core)
        return;

    /* TODO: what do you do with printable flag? */

    /* hash the input name to get a unique id for this record */
    tmp_rec_id = darshan_hash(name, len, 0);

    /* check to see if we've already stored the id->name mapping for this record */
    DARSHAN_CORE_LOCK();
    HASH_FIND(hlink, darshan_core->rec_hash, &tmp_rec_id, sizeof(darshan_record_id), ref);
    if(!ref)
    {
        /* record not found -- add it to the hash if this module has not already used
         * all of its memory
         */
  
        if(mod_limit_flag)
        {
            /* if this module is OOM, set a flag in the header to indicate this */
            DARSHAN_MOD_FLAG_SET(darshan_core->log_header.partial_flag, mod_id);
            DARSHAN_CORE_UNLOCK();
            return;
        }

        ref = malloc(sizeof(struct darshan_core_record_ref));
        if(ref)
        {
            ref->mod_flags = ref->global_mod_flags = 0;
            ref->rec.id = tmp_rec_id;
            ref->rec.name = malloc(strlen(name) + 1);
            if(ref->rec.name)
                strcpy(ref->rec.name, name);

            HASH_ADD(hlink, darshan_core->rec_hash, rec.id, sizeof(darshan_record_id), ref);
            darshan_core->rec_count++;
        }
    }
    DARSHAN_MOD_FLAG_SET(ref->mod_flags, mod_id);
    DARSHAN_CORE_UNLOCK();

    if(file_alignment)
        darshan_block_size_from_path(name, file_alignment);

    *rec_id = tmp_rec_id;
    return;
}

/* TODO: test */
void darshan_core_unregister_record(
    darshan_record_id rec_id,
    darshan_module_id mod_id)
{
    struct darshan_core_record_ref *ref;

    if(!darshan_core)
        return;

    DARSHAN_CORE_LOCK();
    HASH_FIND(hlink, darshan_core->rec_hash, &rec_id, sizeof(darshan_record_id), ref);
    assert(ref); 

    /* disassociate this module from the given record id */
    DARSHAN_MOD_FLAG_UNSET(ref->mod_flags, mod_id);
    if(!(ref->mod_flags))
    {
        /* if no other modules are associated with this rec, delete it */
        HASH_DELETE(hlink, darshan_core->rec_hash, ref);
    }
    DARSHAN_CORE_UNLOCK();

    return;
}

double darshan_core_wtime()
{
    if(!darshan_core)
    {
        return(0);
    }

    return(DARSHAN_MPI_CALL(PMPI_Wtime)() - darshan_core->wtime_offset);
}

int darshan_core_excluded_path(const char *path)
{
    char *exclude;
    int tmp_index = 0;

    while((exclude = darshan_path_exclusions[tmp_index])) {
        if(!(strncmp(exclude, path, strlen(exclude))))
            return(1);
        tmp_index++;
    }

    return(0);
}

/*
 * Local variables:
 *  c-indent-level: 4
 *  c-basic-offset: 4
 * End:
 *
 * vim: ts=8 sts=4 sw=4 expandtab
 */
