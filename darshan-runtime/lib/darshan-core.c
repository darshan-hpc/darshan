/*
 *  (C) 2009 by Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
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
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/vfs.h>
#include <zlib.h>
#include <mpi.h>
#include <assert.h>

#include "uthash.h"
#include "darshan-core.h"

/* TODO is __progname_full needed here */
extern char* __progname;

/* internal variable delcarations */
static struct darshan_core_runtime *darshan_core = NULL;
static pthread_mutex_t darshan_core_mutex = PTHREAD_RECURSIVE_MUTEX_INITIALIZER_NP;
static int my_rank = -1;
static int nprocs = -1;

#define DARSHAN_CORE_LOCK() pthread_mutex_lock(&darshan_core_mutex)
#define DARSHAN_CORE_UNLOCK() pthread_mutex_unlock(&darshan_core_mutex)

/* FS mount information */
#define DARSHAN_MAX_MNTS 64
#define DARSHAN_MAX_MNT_PATH 256
#define DARSHAN_MAX_MNT_TYPE 32
struct mnt_data
{
    int64_t block_size;
    char path[DARSHAN_MAX_MNT_PATH];
    char type[DARSHAN_MAX_MNT_TYPE];
};
static struct mnt_data mnt_data_array[DARSHAN_MAX_MNTS];
static int mnt_data_count = 0;

/* prototypes for internal helper functions */
static void darshan_core_initialize(
    int *argc, char ***argv);
static void darshan_core_shutdown(
    void);
static void darshan_core_cleanup(
    struct darshan_core_runtime* core);
static void darshan_get_logfile_name(
    char* logfile_name, int jobid, struct tm* start_tm);
static void darshan_log_record_hints_and_ver(
    struct darshan_core_runtime* core);
static void darshan_get_exe_and_mounts_root(
    struct darshan_core_runtime *core, char* trailing_data,
    int space_left);
static char* darshan_get_exe_and_mounts(
    struct darshan_core_runtime *core);
static void darshan_get_shared_records(
    struct darshan_core_runtime *core, darshan_record_id *shared_recs);
static int darshan_log_open_all(
    char *logfile_name, MPI_File *log_fh);
static int darshan_deflate_buffer(
    void **pointers, int *lengths, int count, int nocomp_flag,
    char *comp_buf, int *comp_length);
static int darshan_log_write_record_hash(
    MPI_File log_fh, struct darshan_core_runtime *core,
    uint64_t *inout_off);
static int darshan_log_append_all(
    MPI_File log_fh, struct darshan_core_runtime *core, void *buf,
    int count, uint64_t *inout_off, uint64_t *agg_uncomp_sz);

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
    if(!getenv("DARSHAN_DISABLE") && !darshan_core)
    {
        /* TODO: darshan mem alignment code? */

        /* allocate structure to track darshan_core_runtime information */
        darshan_core = malloc(sizeof(*darshan_core));
        if(darshan_core)
        {
            memset(darshan_core, 0, sizeof(*darshan_core));

            darshan_core->log_job.uid = getuid();
            darshan_core->log_job.start_time = time(NULL);
            darshan_core->log_job.nprocs = nprocs;
            darshan_core->wtime_offset = DARSHAN_MPI_CALL(PMPI_Wtime)();

            /* record exe and arguments */
            for(i=0; i<(*argc); i++)
            {
                chars_left = DARSHAN_EXE_LEN-strlen(darshan_core->exe);
                strncat(darshan_core->exe, (*argv)[i], chars_left);
                if(i < ((*argc)-1))
                {
                    chars_left = DARSHAN_EXE_LEN-strlen(darshan_core->exe);
                    strncat(darshan_core->exe, " ", chars_left);
                }
            }

            /* if we don't see any arguments, then use glibc symbol to get
             * program name at least (this happens in fortran)
             */
            if(argc == 0)
            {
                chars_left = DARSHAN_EXE_LEN-strlen(darshan_core->exe);
                strncat(darshan_core->exe, __progname, chars_left);
                chars_left = DARSHAN_EXE_LEN-strlen(darshan_core->exe);
                strncat(darshan_core->exe, " <unknown args>", chars_left);
            }

            if(chars_left == 0)
            {
                /* we ran out of room; mark that string was truncated */
                truncate_offset = DARSHAN_EXE_LEN - strlen(truncate_string);
                sprintf(&darshan_core->exe[truncate_offset], "%s",
                    truncate_string);
            }

            /* collect information about command line and mounted file systems */
            darshan_core->trailing_data = darshan_get_exe_and_mounts(darshan_core);
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
    struct darshan_core_runtime *final_core;
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
    double start_log_time;
    double open1, open2;
    double job1, job2;
    double rec1, rec2;
    double mod1[DARSHAN_MAX_MODS] = {0};
    double mod2[DARSHAN_MAX_MODS] = {0};
    double header1, header2;
    double tm_end;
    long offset;
    uint64_t gz_fp = 0;
    uint64_t tmp_off = 0;
    MPI_File log_fh;
    MPI_Status status;

    if(getenv("DARSHAN_INTERNAL_TIMING"))
        internal_timing_flag = 1;

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

    /* we also need to set which modules were registered on this process and
     * disable tracing within those modules while we shutdown
     */
    for(i = 0; i < DARSHAN_MAX_MODS; i++)
    {
        if(final_core->mod_array[i])
        {
            local_mod_use[i] = 1;
            final_core->mod_array[i]->mod_funcs.disable_instrumentation();
        }
    }
    DARSHAN_CORE_UNLOCK();

    logfile_name = malloc(PATH_MAX);
    if(!logfile_name)
    {
        darshan_core_cleanup(final_core);
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

        final_core->log_job.jobid = (int64_t)jobid;

        /* if we are using any hints to write the log file, then record those
         * hints with the darshan job information
         */
        darshan_log_record_hints_and_ver(final_core);

        /* use human readable start time format in log filename */
        start_time_tmp = final_core->log_job.start_time;
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
        darshan_core_cleanup(final_core);
        return;
    }

    final_core->log_job.end_time = time(NULL);

    /* reduce to report first start time and last end time across all ranks
     * at rank 0
     */
    DARSHAN_MPI_CALL(PMPI_Reduce)(&final_core->log_job.start_time, &first_start_time, 1, MPI_LONG_LONG, MPI_MIN, 0, MPI_COMM_WORLD);
    DARSHAN_MPI_CALL(PMPI_Reduce)(&final_core->log_job.end_time, &last_end_time, 1, MPI_LONG_LONG, MPI_MAX, 0, MPI_COMM_WORLD);
    if(my_rank == 0)
    {
        final_core->log_job.start_time = first_start_time;
        final_core->log_job.end_time = last_end_time;
    }

    /* reduce the number of times a module was opened globally and bcast to everyone */   
    DARSHAN_MPI_CALL(PMPI_Allreduce)(local_mod_use, global_mod_use_count, DARSHAN_MAX_MODS, MPI_INT, MPI_SUM, MPI_COMM_WORLD);

    /* get a list of records which are shared across all processes */
    darshan_get_shared_records(final_core, shared_recs);

    if(internal_timing_flag)
        open1 = DARSHAN_MPI_CALL(PMPI_Wtime)();
    /* collectively open the darshan log file */
    ret = darshan_log_open_all(logfile_name, &log_fh);
    if(internal_timing_flag)
        open2 = DARSHAN_MPI_CALL(PMPI_Wtime)();

    /* error out if unable to open log file */
    DARSHAN_MPI_CALL(PMPI_Allreduce)(&ret, &all_ret, 1, MPI_INT,
        MPI_LOR, MPI_COMM_WORLD);
    if(all_ret != 0)
    {
        if(my_rank == 0)
        {
            fprintf(stderr, "darshan library warning: unable to open log file %s\n",
                logfile_name);
            unlink(logfile_name);
        }
        free(logfile_name);
        darshan_core_cleanup(final_core);
        return;
    }

    if(internal_timing_flag)
        job1 = DARSHAN_MPI_CALL(PMPI_Wtime)();
    /* rank 0 is responsible for writing the compressed darshan job information */
    if(my_rank == 0)
    {
        void *pointers[2] = {&final_core->log_job, final_core->trailing_data};
        int lengths[2] = {sizeof(struct darshan_job), DARSHAN_EXE_LEN+1};
        int comp_buf_sz = 0;

        /* compress the job info and the trailing mount/exe data */
        all_ret = darshan_deflate_buffer(pointers, lengths, 2, 0,
            final_core->comp_buf, &comp_buf_sz);
        if(all_ret)
        {
            fprintf(stderr, "darshan library warning: unable to compress job data\n");
            unlink(logfile_name);
        }
        else
        {
            /* write the job information, preallocing space for the log header */
            gz_fp += sizeof(struct darshan_header) + 23; /* gzip headers/trailers ... */
            all_ret = DARSHAN_MPI_CALL(PMPI_File_write_at)(log_fh, gz_fp,
                final_core->comp_buf, comp_buf_sz, MPI_BYTE, &status);
            if(all_ret != MPI_SUCCESS)
            {
                fprintf(stderr, "darshan library warning: unable to write job data to log file %s\n",
                        logfile_name);
                unlink(logfile_name);
                
            }

            /* set the beginning offset of record hash, which follows job info just written */
            gz_fp += comp_buf_sz;
        }
    }

    /* error out if unable to write job information */
    DARSHAN_MPI_CALL(PMPI_Bcast)(&all_ret, 1, MPI_INT, 0, MPI_COMM_WORLD);
    if(all_ret != 0)
    {
        free(logfile_name);
        darshan_core_cleanup(final_core);
        return;
    }
    if(internal_timing_flag)
        job2 = DARSHAN_MPI_CALL(PMPI_Wtime)();

    if(internal_timing_flag)
        rec1 = DARSHAN_MPI_CALL(PMPI_Wtime)();
    /* write the record name->id hash to the log file */
    ret = darshan_log_write_record_hash(log_fh, final_core, &gz_fp);
    tmp_off = final_core->log_header.rec_map.off + final_core->log_header.rec_map.len;

    /* error out if unable to write record hash */
    DARSHAN_MPI_CALL(PMPI_Allreduce)(&ret, &all_ret, 1, MPI_INT,
        MPI_LOR, MPI_COMM_WORLD);
    if(all_ret != 0)
    {
        if(my_rank == 0)
        {
            fprintf(stderr, "darshan library warning: unable to write record hash to log file %s\n",
                logfile_name);
            unlink(logfile_name);
        }
        free(logfile_name);
        darshan_core_cleanup(final_core);
        return;
    }
    if(internal_timing_flag)
        rec2 = DARSHAN_MPI_CALL(PMPI_Wtime)();

    /* loop over globally used darshan modules and:
     *      - perform shared file reductions, if possible
     *      - get final output buffer
     *      - compress (zlib) provided output buffer
     *      - append compressed buffer to log file
     *      - add module index info (file offset/length) to log header
     *      - shutdown the module
     */
    for(i = 0; i < DARSHAN_MAX_MODS; i++)
    {
        struct darshan_core_module* this_mod = final_core->mod_array[i];
        darshan_record_id mod_shared_recs[DARSHAN_CORE_MAX_RECORDS];
        struct darshan_core_record_ref *ref = NULL;
        int shared_rec_cnt = 0;
        void* mod_buf = NULL;
        int mod_buf_sz = 0;
        int comp_buf_sz = 0;
        int j;

        if(global_mod_use_count[i] == 0)
        {
            if(my_rank == 0)
            {
                final_core->log_header.mod_map[i].off = 0;
                final_core->log_header.mod_map[i].len = 0;
            }
            continue;
        }
 
        if(internal_timing_flag)
            mod1[i] = DARSHAN_MPI_CALL(PMPI_Wtime)();   
        /* if all processes used this module, prepare to do a shared file reduction */
        if(global_mod_use_count[j] == nprocs)
        {
            int shared_rec_count = 0;
            int rec_sz = 0;
            void *red_send_buf = NULL, *red_recv_buf = NULL;
            MPI_Datatype red_type;
            MPI_Op red_op;

            /* set the shared file list for this module */
            memset(mod_shared_recs, 0, DARSHAN_CORE_MAX_RECORDS * sizeof(darshan_record_id));
            for(j = 0; j < DARSHAN_CORE_MAX_RECORDS && shared_recs[j] != 0; j++)
            {
                HASH_FIND(hlink, final_core->rec_hash, &shared_recs[j],
                    sizeof(darshan_record_id), ref);
                assert(ref);
                if(DARSHAN_CORE_MOD_ISSET(ref->global_mod_flags, i))
                {
                    mod_shared_recs[shared_rec_count++] = shared_recs[j];
                }
            }

            /* if there are globally shared files, do a shared file reduction */
            if(shared_rec_count)
            {
                this_mod->mod_funcs.prepare_for_reduction(mod_shared_recs, &shared_rec_count,
                    &red_send_buf, &red_recv_buf, &rec_sz);

                if(shared_rec_count)
                {
                    /* construct a datatype for a file record.  This is serving no purpose
                     * except to make sure we can do a reduction on proper boundaries
                     */
                    DARSHAN_MPI_CALL(PMPI_Type_contiguous)(rec_sz, MPI_BYTE, &red_type);
                    DARSHAN_MPI_CALL(PMPI_Type_commit)(&red_type);

                    /* register a reduction operator for this module */
                    DARSHAN_MPI_CALL(PMPI_Op_create)(this_mod->mod_funcs.reduce_records,
                        1, &red_op);

                    /* reduce shared file records for this module */
                    DARSHAN_MPI_CALL(PMPI_Reduce)(red_send_buf, red_recv_buf,
                        shared_rec_count, red_type, red_op, 0, MPI_COMM_WORLD);

                    DARSHAN_MPI_CALL(PMPI_Type_free)(&red_type);
                    DARSHAN_MPI_CALL(PMPI_Op_free)(&red_op);
                }
            }
        }

        /* if module is registered locally, get the corresponding output buffer */
        if(this_mod)
        {
            /* get output buffer from module */
            this_mod->mod_funcs.get_output_data(&mod_buf, &mod_buf_sz);
        }

        final_core->log_header.mod_map[i].off = tmp_off;

        /* append this module's data to the darshan log */
        ret = darshan_log_append_all(log_fh, final_core, mod_buf, mod_buf_sz,
            &gz_fp, &(final_core->log_header.mod_map[i].len));
        tmp_off += final_core->log_header.mod_map[i].len;

        /* error out if the log append failed */
        DARSHAN_MPI_CALL(PMPI_Allreduce)(&ret, &all_ret, 1, MPI_INT,
            MPI_LOR, MPI_COMM_WORLD);
        if(all_ret != 0)
        {
            if(my_rank == 0)
            {
                fprintf(stderr,
                    "darshan library warning: unable to write %s module data to log file %s\n",
                    darshan_module_names[i], logfile_name);
                unlink(logfile_name);
            }
            free(logfile_name);
            darshan_core_cleanup(final_core);
            return;
        }

        /* shutdown module if registered locally */
        if(this_mod)
        {
            this_mod->mod_funcs.shutdown();
        }
        if(internal_timing_flag)
            mod2[i] = DARSHAN_MPI_CALL(PMPI_Wtime)();
    }

    if(internal_timing_flag)
        header1 = DARSHAN_MPI_CALL(PMPI_Wtime)();
    /* rank 0 is responsible for writing the log header */
    if(my_rank == 0)
    {
        void *header_buf = &(final_core->log_header);
        int header_buf_sz = sizeof(struct darshan_header);
        int comp_buf_sz = 0;

        /* initialize the remaining header fields */
        strcpy(final_core->log_header.version_string, DARSHAN_LOG_VERSION);
        final_core->log_header.magic_nr = DARSHAN_MAGIC_NR;

        /* deflate the header */
        /* NOTE: the header is not actually compressed because space for it must
         *       be preallocated before writing. i.e., the "compressed" header
         *       must be constant sized, sizeof(struct darshan_header) + 23.
         *       it is still necessary to deflate the header or the resulting log
         *       file will not be a valid gzip file.
         */
        all_ret = darshan_deflate_buffer((void **)&header_buf, &header_buf_sz, 1, 1,
            final_core->comp_buf, &comp_buf_sz);
        if(all_ret)
        {
            fprintf(stderr, "darshan library warning: unable to compress header\n");
            unlink(logfile_name);
        }
        else
        {
            all_ret = DARSHAN_MPI_CALL(PMPI_File_write_at)(log_fh, 0, final_core->comp_buf,
                comp_buf_sz, MPI_BYTE, &status);
            if(all_ret != MPI_SUCCESS)
            {
                fprintf(stderr, "darshan library warning: unable to write header to log file %s\n",
                        logfile_name);
                unlink(logfile_name);
            }
        }
    }

    /* error out if unable to write log header */
    DARSHAN_MPI_CALL(PMPI_Bcast)(&all_ret, 1, MPI_INT, 0, MPI_COMM_WORLD);
    if(all_ret != 0)
    {
        free(logfile_name);
        darshan_core_cleanup(final_core);
        return;
    }
    if(internal_timing_flag)
        header2 = DARSHAN_MPI_CALL(PMPI_Wtime)();

    DARSHAN_MPI_CALL(PMPI_File_close)(&log_fh);

    /* if we got this far, there are no errors, so rename from *.darshan_partial
     * to *-<logwritetime>.darshan.gz, which indicates that this log file is
     * complete and ready for analysis
     */
    if(my_rank == 0)
    {
        if(getenv("DARSHAN_LOGFILE"))
        {
#ifdef __CP_GROUP_READABLE_LOGS
            chmod(logfile_name, (S_IRUSR|S_IRGRP));
#else
            chmod(logfile_name, (S_IRUSR));
#endif
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
                end_log_time = DARSHAN_MPI_CALL(PMPI_Wtime)();
                strcat(new_logfile_name, logfile_name);
                tmp_index = strstr(new_logfile_name, ".darshan_partial");
                sprintf(tmp_index, "_%d.darshan.gz", (int)(end_log_time-start_log_time+1));
                rename(logfile_name, new_logfile_name);
                /* set permissions on log file */
#ifdef __CP_GROUP_READABLE_LOGS
                chmod(new_logfile_name, (S_IRUSR|S_IRGRP));
#else
                chmod(new_logfile_name, (S_IRUSR));
#endif
                free(new_logfile_name);
            }
        }
    }

    free(logfile_name);
    darshan_core_cleanup(final_core);

    if(internal_timing_flag)
    {
        double open_tm, open_slowest;
        double header_tm, header_slowest;
        double job_tm, job_slowest;
        double rec_tm, rec_slowest;
        double mod_tm[DARSHAN_MAX_MODS], mod_slowest[DARSHAN_MAX_MODS];
        double all_tm, all_slowest;

        tm_end = DARSHAN_MPI_CALL(PMPI_Wtime)();

        open_tm = open2 - open1;
        header_tm = header2 - header1;
        job_tm = job2 - job1;
        rec_tm = rec2 - rec1;
        all_tm = tm_end - start_log_time;
        for(i = 0;i < DARSHAN_MAX_MODS; i++)
        {
            mod_tm[i] = mod2[i] - mod1[i];
        }

        DARSHAN_MPI_CALL(PMPI_Reduce)(&open_tm, &open_slowest, 1,
            MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
        DARSHAN_MPI_CALL(PMPI_Reduce)(&header_tm, &header_slowest, 1,
            MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
        DARSHAN_MPI_CALL(PMPI_Reduce)(&job_tm, &job_slowest, 1,
            MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
        DARSHAN_MPI_CALL(PMPI_Reduce)(&rec_tm, &rec_slowest, 1,
            MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
        DARSHAN_MPI_CALL(PMPI_Reduce)(&all_tm, &all_slowest, 1,
            MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
        DARSHAN_MPI_CALL(PMPI_Reduce)(mod_tm, mod_slowest, DARSHAN_MAX_MODS,
            MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);

        if(my_rank == 0)
        {
            printf("#darshan:<op>\t<nprocs>\t<time>\n");
            printf("darshan:log_open\t%d\t%f\n", nprocs, open_slowest);
            printf("darshan:job_write\t%d\t%f\n", nprocs, job_slowest);
            printf("darshan:hash_write\t%d\t%f\n", nprocs, rec_slowest);
            printf("darshan:header_write\t%d\t%f\n", nprocs, header_slowest);
            for(i = 0; i < DARSHAN_MAX_MODS; i++)
            {
                if(global_mod_use_count[i])
                    printf("darshan:%s_shutdown\t%d\t%f\n", darshan_module_names[i],
                        nprocs, mod_slowest[i]);
            }
            printf("darshan:core_shutdown\t%d\t%f\n", nprocs, all_slowest);
        }
    }
    
    return;
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

    free(core->trailing_data);
    free(core);

    return;
}

/* construct the darshan log file name */
static void darshan_get_logfile_name(char* logfile_name, int jobid, struct tm* start_tm)
{
    char* user_logfile_name;
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
        strlen(core->log_job.metadata) - 1;
    if(meta_remain >= (strlen(PACKAGE_VERSION) + 9))
    {
        sprintf(core->log_job.metadata, "lib_ver=%s\n", PACKAGE_VERSION);
        meta_remain -= (strlen(PACKAGE_VERSION) + 9);
    }
    if(meta_remain >= (3 + strlen(header_hints)))
    {
        m = core->log_job.metadata + strlen(core->log_job.metadata);
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
static void add_entry(char* trailing_data, int* space_left, struct mntent *entry)
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
        strcat(trailing_data, tmp_mnt);
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
    char* trailing_data, int space_left)
{
    FILE* tab;
    struct mntent *entry;
    char* exclude;
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

    /* length of exe has already been safety checked in darshan-posix.c */
    strcat(trailing_data, core->exe);
    space_left = DARSHAN_EXE_LEN - strlen(trailing_data);

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

        add_entry(trailing_data, &space_left, entry);
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

        add_entry(trailing_data, &space_left, entry);
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
static char* darshan_get_exe_and_mounts(struct darshan_core_runtime *core)
{
    char* trailing_data;
    int space_left;

    space_left = DARSHAN_EXE_LEN + 1;
    trailing_data = malloc(space_left);
    if(!trailing_data)
    {
        return(NULL);
    }
    memset(trailing_data, 0, space_left);

    if(my_rank == 0)
    {
        darshan_get_exe_and_mounts_root(core, trailing_data, space_left);
    }

    /* broadcast trailing data to all nodes */
    DARSHAN_MPI_CALL(PMPI_Bcast)(trailing_data, space_left, MPI_CHAR, 0,
        MPI_COMM_WORLD);
    /* broadcast mount count to all nodes */
    DARSHAN_MPI_CALL(PMPI_Bcast)(&mnt_data_count, 1, MPI_INT, 0,
        MPI_COMM_WORLD);
    /* broadcast mount data to all nodes */
    DARSHAN_MPI_CALL(PMPI_Bcast)(mnt_data_array,
        mnt_data_count*sizeof(mnt_data_array[0]), MPI_BYTE, 0, MPI_COMM_WORLD);

    return(trailing_data);
}

static void darshan_get_shared_records(struct darshan_core_runtime *core,
    darshan_record_id *shared_recs)
{
    int i;
    int ndx;
    struct darshan_core_record_ref *tmp, *ref;
    darshan_record_id id_array[DARSHAN_CORE_MAX_RECORDS] = {0};
    uint64_t mod_flags[DARSHAN_CORE_MAX_RECORDS] = {0};
    uint64_t global_mod_flags[DARSHAN_CORE_MAX_RECORDS] = {0};

    /* first, determine list of records root process has opened */
    if(my_rank == 0)
    {
        ndx = 0;
        HASH_ITER(hlink, core->rec_hash, ref, tmp)
        {
            id_array[ndx++] = ref->rec.id;           
        }
    }

    /* broadcast root's list of records to all other processes */
    DARSHAN_MPI_CALL(PMPI_Bcast)(id_array,
        (DARSHAN_CORE_MAX_RECORDS * sizeof(darshan_record_id)),
        MPI_BYTE, 0, MPI_COMM_WORLD);

    /* everyone looks to see if they opened the same records as root */
    for(i=0; (i<DARSHAN_CORE_MAX_RECORDS && id_array[i] != 0); i++)
    {
        HASH_FIND(hlink, core->rec_hash, &id_array[i], sizeof(darshan_record_id), ref);
        if(ref)
        {
            /* we opened that record too, save the mod_flags */
            mod_flags[i] = ref->mod_flags;
            break;
        }
    }

    /* now allreduce so everyone agrees which files are shared and
     * which modules accessed them collectively
     */
    DARSHAN_MPI_CALL(PMPI_Allreduce)(mod_flags, global_mod_flags,
        DARSHAN_CORE_MAX_RECORDS, MPI_UINT64_T, MPI_LAND, MPI_COMM_WORLD);

    ndx = 0;
    for(i=0; (i<DARSHAN_CORE_MAX_RECORDS && id_array[i] != 0); i++)
    {
        if(global_mod_flags[i] != 0)
        {
            shared_recs[ndx++] = id_array[i];

            /* set global_mod_flags so we know which modules collectively
             * accessed this module. we need this info to support shared
             * file reductions
             */
            HASH_FIND(hlink, core->rec_hash, &id_array[i], sizeof(darshan_record_id), ref);
            assert(ref);
            ref->global_mod_flags = global_mod_flags[i];
        }
    }

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

    /* open the darshan log file for writing */
    ret = DARSHAN_MPI_CALL(PMPI_File_open)(MPI_COMM_WORLD, logfile_name,
        MPI_MODE_CREATE | MPI_MODE_WRONLY | MPI_MODE_EXCL, info, log_fh);
    if(ret != MPI_SUCCESS)
        return(-1);

    MPI_Info_free(&info);
    return(0);
}

static int darshan_deflate_buffer(void **pointers, int *lengths, int count,
    int nocomp_flag, char *comp_buf, int *comp_length)
{
    int ret = 0;
    int i;
    int total_target = 0;
    int z_comp_level;
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
        *comp_length = 0;
        return(0);
    }

    memset(&tmp_stream, 0, sizeof(tmp_stream));
    tmp_stream.zalloc = Z_NULL;
    tmp_stream.zfree = Z_NULL;
    tmp_stream.opaque = Z_NULL;

    /* initialize the zlib compression parameters */
    /* TODO: check these parameters? */
    z_comp_level = nocomp_flag ? Z_NO_COMPRESSION : Z_DEFAULT_COMPRESSION;
    ret = deflateInit2(&tmp_stream, z_comp_level, Z_DEFLATED,
        15 + 16, 8, Z_DEFAULT_STRATEGY);
//    ret = deflateInit(&tmp_stream, z_comp_level);
    if(ret != Z_OK)
    {
        return(-1);
    }

    tmp_stream.next_out = comp_buf;
    tmp_stream.avail_out = DARSHAN_CORE_COMP_BUF_SIZE;

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

    *comp_length = tmp_stream.total_out;
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
    MPI_Status status;

    /* allocate a buffer to store at most 64 bytes for each of a max number of records */
    /* NOTE: this buffer may be reallocated if estimate is too small */
    hash_buf_sz = DARSHAN_CORE_MAX_RECORDS * 64;
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

    /* store uncompressed offset of the record hash */
    core->log_header.rec_map.off = sizeof(struct darshan_header) +
        DARSHAN_JOB_RECORD_SIZE;

    /* collectively write out the record hash to the darshan log */
    hash_buf_sz = hash_buf_off - hash_buf;
    ret = darshan_log_append_all(log_fh, core, hash_buf, hash_buf_sz,
        inout_off, &(core->log_header.rec_map.len));

    free(hash_buf);

    return(ret);
}

/* NOTE: inout_off contains the starting offset of this append at the beginning
 *       of the call, and contains the ending offset at the end of the call.
 *       total_uncomp_sz will store the collective size of the uncompressed
 *       buffer being written. This data is necessary to properly index data
 *       when reading a darshan log. Also, these variables should only be valid
 *       on the root rank (rank 0).
 */
static int darshan_log_append_all(MPI_File log_fh, struct darshan_core_runtime *core,
    void *buf, int count, uint64_t *inout_off, uint64_t *agg_uncomp_sz)
{
    MPI_Offset send_off, my_off;
    MPI_Status status;
    uint64_t uncomp_buf_sz = count;
    int comp_buf_sz = 0;
    int ret;

    /* compress the input buffer */
    ret = darshan_deflate_buffer((void **)&buf, &count, 1, 0,
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

    /* send the ending offset from rank (n-1) to rank 0 */
    if(nprocs > 1)
    {
        if(my_rank == (nprocs-1))
        {
            my_off += comp_buf_sz;
            DARSHAN_MPI_CALL(PMPI_Send)(&my_off, 1, MPI_OFFSET, 0, 0,
                MPI_COMM_WORLD);
        }
        else if(my_rank == 0)
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

    /* pass back the aggregate uncompressed size of this blob */
    DARSHAN_MPI_CALL(PMPI_Reduce)(&uncomp_buf_sz, agg_uncomp_sz, 1,
        MPI_UINT64_T, MPI_SUM, 0, MPI_COMM_WORLD);

    if(ret != 0)
        return(-1);
    return(0);
}

/* ********************************************************* */

void darshan_core_register_module(
    darshan_module_id mod_id,
    struct darshan_module_funcs *funcs,
    int *runtime_mem_limit)
{
    struct darshan_core_module* mod;
    *runtime_mem_limit = 0;

    if(!darshan_core || (mod_id >= DARSHAN_MAX_MODS))
        return;

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

    /* TODO: something smarter than just 2 MiB per module */
    *runtime_mem_limit = 2 * 1024 * 1024;

    DARSHAN_CORE_UNLOCK();

    return;
}

/* TODO: implement & test*/
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
    int printable_flag,
    darshan_module_id mod_id,
    darshan_record_id *rec_id)
{
    darshan_record_id tmp_rec_id;
    struct darshan_core_record_ref *ref;

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
        /* record not found -- add it to the hash if we aren't already tracking the
         * maximum number of records
         */               
        if(darshan_core->rec_count >= DARSHAN_CORE_MAX_RECORDS)
        {
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
    ref->mod_flags = DARSHAN_CORE_MOD_SET(ref->mod_flags, mod_id);
    DARSHAN_CORE_UNLOCK();

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
    ref->mod_flags = DARSHAN_CORE_MOD_UNSET(ref->mod_flags, mod_id);
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

/*
 * Local variables:
 *  c-indent-level: 4
 *  c-basic-offset: 4
 * End:
 *
 * vim: ts=8 sts=4 sw=4 expandtab
 */
