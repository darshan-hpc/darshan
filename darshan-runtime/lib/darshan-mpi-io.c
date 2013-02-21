/*
 *  (C) 2009 by Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
 */

#define _XOPEN_SOURCE 500
#define _GNU_SOURCE /* for tdestroy() */

#include "darshan-runtime-config.h"

#include <stdio.h>
#ifdef HAVE_MNTENT_H
#include <mntent.h>
#endif
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <limits.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/vfs.h>
#include <zlib.h>
#include <assert.h>
#include <search.h>

#include "mpi.h"
#include "darshan.h"
#include "darshan-dynamic.h"

extern char* __progname;

/* maximum number of memory segments each process will write to the log */
#define CP_MAX_MEM_SEGMENTS 8

/* Some old versions of MPI don't provide all of these COMBINER definitions.  
 * If any are missing then we define them to an arbitrary value just to 
 * prevent compile errors in DATATYPE_INC().
 */
#ifndef MPI_COMBINER_NAMED
    #define MPI_COMBINER_NAMED CP_COMBINER_NAMED
#endif
#ifndef MPI_COMBINER_DUP
    #define MPI_COMBINER_DUP CP_COMBINER_DUP
#endif
#ifndef MPI_COMBINER_CONTIGUOUS
    #define MPI_COMBINER_CONTIGUOUS CP_COMBINER_CONTIGUOUS
#endif
#ifndef MPI_COMBINER_VECTOR
    #define MPI_COMBINER_VECTOR CP_COMBINER_VECTOR
#endif
#ifndef MPI_COMBINER_HVECTOR_INTEGER
    #define MPI_COMBINER_HVECTOR_INTEGER CP_COMBINER_HVECTOR_INTEGER
#endif
#ifndef MPI_COMBINER_HVECTOR
    #define MPI_COMBINER_HVECTOR CP_COMBINER_HVECTOR
#endif
#ifndef MPI_COMBINER_INDEXED
    #define MPI_COMBINER_INDEXED CP_COMBINER_INDEXED
#endif
#ifndef MPI_COMBINER_HINDEXED_INTEGER
    #define MPI_COMBINER_HINDEXED_INTEGER CP_COMBINER_HINDEXED_INTEGER
#endif
#ifndef MPI_COMBINER_HINDEXED
    #define MPI_COMBINER_HINDEXED CP_COMBINER_HINDEXED
#endif
#ifndef MPI_COMBINER_INDEXED_BLOCK
    #define MPI_COMBINER_INDEXED_BLOCK CP_COMBINER_INDEXED_BLOCK
#endif
#ifndef MPI_COMBINER_STRUCT_INTEGER
    #define MPI_COMBINER_STRUCT_INTEGER CP_COMBINER_STRUCT_INTEGER
#endif
#ifndef MPI_COMBINER_STRUCT
    #define MPI_COMBINER_STRUCT CP_COMBINER_STRUCT
#endif
#ifndef MPI_COMBINER_SUBARRAY
    #define MPI_COMBINER_SUBARRAY CP_COMBINER_SUBARRAY
#endif
#ifndef MPI_COMBINER_DARRAY
    #define MPI_COMBINER_DARRAY CP_COMBINER_DARRAY
#endif
#ifndef MPI_COMBINER_F90_REAL
    #define MPI_COMBINER_F90_REAL CP_COMBINER_F90_REAL
#endif
#ifndef MPI_COMBINER_F90_COMPLEX
    #define MPI_COMBINER_F90_COMPLEX CP_COMBINER_F90_COMPLEX
#endif
#ifndef MPI_COMBINER_F90_INTEGER
    #define MPI_COMBINER_F90_INTEGER CP_COMBINER_F90_INTEGER
#endif
#ifndef MPI_COMBINER_RESIZED
    #define MPI_COMBINER_RESIZED CP_COMBINER_RESIZED
#endif

#define CP_DATATYPE_INC(__file, __datatype) do {\
    int num_integers, num_addresses, num_datatypes, combiner, ret; \
    ret = DARSHAN_MPI_CALL(PMPI_Type_get_envelope)(__datatype, &num_integers, \
        &num_addresses, &num_datatypes, &combiner); \
    if(ret == MPI_SUCCESS) { \
        switch(combiner) { \
            case MPI_COMBINER_NAMED:\
                CP_INC(__file,CP_COMBINER_NAMED,1); break; \
            case MPI_COMBINER_DUP:\
                CP_INC(__file,CP_COMBINER_DUP,1); break; \
            case MPI_COMBINER_CONTIGUOUS:\
                CP_INC(__file,CP_COMBINER_CONTIGUOUS,1); break; \
            case MPI_COMBINER_VECTOR:\
                CP_INC(__file,CP_COMBINER_VECTOR,1); break; \
            case MPI_COMBINER_HVECTOR_INTEGER:\
                CP_INC(__file,CP_COMBINER_HVECTOR_INTEGER,1); break; \
            case MPI_COMBINER_HVECTOR:\
                CP_INC(__file,CP_COMBINER_HVECTOR,1); break; \
            case MPI_COMBINER_INDEXED:\
                CP_INC(__file,CP_COMBINER_INDEXED,1); break; \
            case MPI_COMBINER_HINDEXED_INTEGER:\
                CP_INC(__file,CP_COMBINER_HINDEXED_INTEGER,1); break; \
            case MPI_COMBINER_HINDEXED:\
                CP_INC(__file,CP_COMBINER_HINDEXED,1); break; \
            case MPI_COMBINER_INDEXED_BLOCK:\
                CP_INC(__file,CP_COMBINER_INDEXED_BLOCK,1); break; \
            case MPI_COMBINER_STRUCT_INTEGER:\
                CP_INC(__file,CP_COMBINER_STRUCT_INTEGER,1); break; \
            case MPI_COMBINER_STRUCT:\
                CP_INC(__file,CP_COMBINER_STRUCT,1); break; \
            case MPI_COMBINER_SUBARRAY:\
                CP_INC(__file,CP_COMBINER_SUBARRAY,1); break; \
            case MPI_COMBINER_DARRAY:\
                CP_INC(__file,CP_COMBINER_DARRAY,1); break; \
            case MPI_COMBINER_F90_REAL:\
                CP_INC(__file,CP_COMBINER_F90_REAL,1); break; \
            case MPI_COMBINER_F90_COMPLEX:\
                CP_INC(__file,CP_COMBINER_F90_COMPLEX,1); break; \
            case MPI_COMBINER_F90_INTEGER:\
                CP_INC(__file,CP_COMBINER_F90_INTEGER,1); break; \
            case MPI_COMBINER_RESIZED:\
                CP_INC(__file,CP_COMBINER_RESIZED,1); break; \
        } \
    } \
} while(0)

#define CP_RECORD_MPI_WRITE(__ret, __fh, __count, __datatype, __counter, __tm1, __tm2) do { \
    struct darshan_file_runtime* file; \
    int size = 0; \
    MPI_Aint extent = 0; \
    if(__ret != MPI_SUCCESS) break; \
    file = darshan_file_by_fh(__fh); \
    if(!file) break; \
    DARSHAN_MPI_CALL(PMPI_Type_size)(__datatype, &size);  \
    size = size * __count; \
    DARSHAN_MPI_CALL(PMPI_Type_extent)(__datatype, &extent); \
    CP_BUCKET_INC(file, CP_SIZE_WRITE_AGG_0_100, size); \
    CP_BUCKET_INC(file, CP_EXTENT_WRITE_0_100, extent); \
    CP_INC(file, __counter, 1); \
    CP_DATATYPE_INC(file, __datatype); \
    CP_F_INC_NO_OVERLAP(file, __tm1, __tm2, file->last_mpi_write_end, CP_F_MPI_WRITE_TIME); \
    if(CP_F_VALUE(file, CP_F_WRITE_START_TIMESTAMP) == 0) \
        CP_F_SET(file, CP_F_WRITE_START_TIMESTAMP, __tm1); \
    CP_F_SET(file, CP_F_WRITE_END_TIMESTAMP, __tm2); \
} while(0)

#define CP_RECORD_MPI_READ(__ret, __fh, __count, __datatype, __counter, __tm1, __tm2) do { \
    struct darshan_file_runtime* file; \
    int size = 0; \
    MPI_Aint extent = 0; \
    if(__ret != MPI_SUCCESS) break; \
    file = darshan_file_by_fh(__fh); \
    if(!file) break; \
    DARSHAN_MPI_CALL(PMPI_Type_size)(__datatype, &size);  \
    size = size * __count; \
    DARSHAN_MPI_CALL(PMPI_Type_extent)(__datatype, &extent); \
    CP_BUCKET_INC(file, CP_SIZE_READ_AGG_0_100, size); \
    CP_BUCKET_INC(file, CP_EXTENT_READ_0_100, extent); \
    CP_INC(file, __counter, 1); \
    CP_DATATYPE_INC(file, __datatype); \
    CP_F_INC_NO_OVERLAP(file, __tm1, __tm2, file->last_mpi_read_end, CP_F_MPI_READ_TIME); \
    if(CP_F_VALUE(file, CP_F_READ_START_TIMESTAMP) == 0) \
        CP_F_SET(file, CP_F_READ_START_TIMESTAMP, __tm1); \
    CP_F_SET(file, CP_F_READ_END_TIMESTAMP, __tm2); \
} while(0)

static void cp_log_construct_indices(struct darshan_job_runtime* final_job,
    int rank, int* inout_count, int* lengths, void** pointers, char*
    trailing_data);
static int cp_log_write(struct darshan_job_runtime* final_job, int rank, 
    char* logfile_name, int count, int* lengths, void** pointers, double start_log_time);
static void cp_log_record_hints(struct darshan_job_runtime* final_job, int rank);
static int cp_log_reduction(struct darshan_job_runtime* final_job, int rank, 
    char* logfile_name, MPI_Offset* next_offset);
static void darshan_file_reduce(void* infile_v, 
    void* inoutfile_v, int *len, 
    MPI_Datatype *datatype);
static int cp_log_compress(struct darshan_job_runtime* final_job,
    int rank, int* inout_count, int* lengths, void** pointers);
static int file_compare(const void* a, const void* b);
       void darshan_mpi_initialize(int *argc, char ***argv);
static int darshan_file_variance(
    struct darshan_file *infile_array,
    struct darshan_file *outfile_array,
    int count, int rank);
static void pairwise_variance_reduce (
    void *invec, void *inoutvec, int *len, MPI_Datatype *dt);
static void debug_mounts(const char* mtab_file, const char* out_file);

static struct darshan_file_runtime* darshan_file_by_fh(MPI_File fh);
static void darshan_file_close_fh(MPI_File fh);
static struct darshan_file_runtime* darshan_file_by_name_setfh(const char* name, MPI_File fh);

#define CP_MAX_MNTS 32
static uint64_t mnt_hash_array[CP_MAX_MNTS] = {0};
static int64_t mnt_id_array[CP_MAX_MNTS] = {0};
static int64_t mnt_block_size_array[CP_MAX_MNTS] = {0};
static char* mnt_path_array[CP_MAX_MNTS] = {0};

static uint64_t mnt_hash_array_root[CP_MAX_MNTS] = {0};
static int64_t mnt_id_array_root[CP_MAX_MNTS] = {0};
struct
{
    int64_t mnt_id_local;
    int64_t mnt_id_root;
} mnt_mapping[CP_MAX_MNTS];

struct variance_dt
{
    double n;
    double T;
    double S;
};

void darshan_mpi_initialize(int *argc, char ***argv)
{
    int nprocs;
    int rank;

    DARSHAN_MPI_CALL(PMPI_Comm_size)(MPI_COMM_WORLD, &nprocs);
    DARSHAN_MPI_CALL(PMPI_Comm_rank)(MPI_COMM_WORLD, &rank);

    if(argc && argv)
    {
        darshan_initialize(*argc, *argv, nprocs, rank);
    }
    else
    {
        /* we don't see argc and argv here in fortran */
        darshan_initialize(0, NULL, nprocs, rank);
    }

    return;
}

void darshan_shutdown(int timing_flag)
{
    int rank;
    char* logfile_name;
    struct darshan_job_runtime* final_job;
    double start_log_time = 0;
    int all_ret = 0;
    int local_ret = 0;
    MPI_Offset next_offset = 0;
    char* jobid_str;
    char* envjobid;
    char* logpath;
    int jobid;
    int index_count = 0;
    int lengths[CP_MAX_MEM_SEGMENTS];
    void* pointers[CP_MAX_MEM_SEGMENTS];
    int ret;
    double red1=0, red2=0, gz1=0, gz2=0, write1=0, write2=0, tm_end=0;
    double bcst1=0, bcst2=0, bcst3=0;
    int nprocs;
    int i, j;
    int map_index = 0;
    time_t start_time_tmp = 0;
    uint64_t logmod;
    char hname[HOST_NAME_MAX];
    char* logpath_override = NULL;
#ifdef __CP_LOG_ENV
    char env_check[256];
    char* env_tok;
#endif
    uint64_t hlevel;

    CP_LOCK();
    if(!darshan_global_job)
    {
        CP_UNLOCK();
        return;
    }
    /* disable further tracing while hanging onto the data so that we can
     * write it out
     */
    final_job = darshan_global_job;
    darshan_global_job = NULL;
    CP_UNLOCK();

    start_log_time = DARSHAN_MPI_CALL(PMPI_Wtime)();

    /* figure out which access sizes to log */
    darshan_walk_file_accesses(final_job);

    /* if the records have been condensed, then zero out fields that are no
     * longer valid for safety 
     */
    if(final_job->flags & CP_FLAG_CONDENSED && final_job->file_count)
    {
        CP_SET(&final_job->file_runtime_array[0], CP_MODE, 0);
        CP_SET(&final_job->file_runtime_array[0], CP_CONSEC_READS, 0);
        CP_SET(&final_job->file_runtime_array[0], CP_CONSEC_WRITES, 0);
        CP_SET(&final_job->file_runtime_array[0], CP_SEQ_READS, 0);
        CP_SET(&final_job->file_runtime_array[0], CP_SEQ_WRITES, 0);
        CP_SET(&final_job->file_runtime_array[0], CP_STRIDE1_STRIDE, 0);
        CP_SET(&final_job->file_runtime_array[0], CP_STRIDE2_STRIDE, 0);
        CP_SET(&final_job->file_runtime_array[0], CP_STRIDE3_STRIDE, 0);
        CP_SET(&final_job->file_runtime_array[0], CP_STRIDE4_STRIDE, 0);
        CP_SET(&final_job->file_runtime_array[0], CP_STRIDE1_COUNT, 0);
        CP_SET(&final_job->file_runtime_array[0], CP_STRIDE2_COUNT, 0);
        CP_SET(&final_job->file_runtime_array[0], CP_STRIDE3_COUNT, 0);
        CP_SET(&final_job->file_runtime_array[0], CP_STRIDE4_COUNT, 0);
        CP_SET(&final_job->file_runtime_array[0], CP_ACCESS1_ACCESS, 0);
        CP_SET(&final_job->file_runtime_array[0], CP_ACCESS2_ACCESS, 0);
        CP_SET(&final_job->file_runtime_array[0], CP_ACCESS3_ACCESS, 0);
        CP_SET(&final_job->file_runtime_array[0], CP_ACCESS4_ACCESS, 0);
        CP_SET(&final_job->file_runtime_array[0], CP_ACCESS1_COUNT, 0);
        CP_SET(&final_job->file_runtime_array[0], CP_ACCESS2_COUNT, 0);
        CP_SET(&final_job->file_runtime_array[0], CP_ACCESS3_COUNT, 0);
        CP_SET(&final_job->file_runtime_array[0], CP_ACCESS4_COUNT, 0);
        
        CP_F_SET(&final_job->file_runtime_array[0], CP_F_OPEN_TIMESTAMP, 0);
        CP_F_SET(&final_job->file_runtime_array[0], CP_F_CLOSE_TIMESTAMP, 0);
        CP_F_SET(&final_job->file_runtime_array[0], CP_F_READ_START_TIMESTAMP, 0);
        CP_F_SET(&final_job->file_runtime_array[0], CP_F_READ_END_TIMESTAMP, 0);
        CP_F_SET(&final_job->file_runtime_array[0], CP_F_WRITE_START_TIMESTAMP, 0);
        CP_F_SET(&final_job->file_runtime_array[0], CP_F_WRITE_END_TIMESTAMP, 0);
    }

    logfile_name = malloc(PATH_MAX);
    if(!logfile_name)
    {
        darshan_finalize(final_job);
        return;
    }

    DARSHAN_MPI_CALL(PMPI_Comm_rank)(MPI_COMM_WORLD, &rank);

    /* broadcast mount point information from root */
    if(rank == 0)
    {
        memcpy(mnt_hash_array_root, mnt_hash_array,
            CP_MAX_MNTS*sizeof(uint64_t));
        memcpy(mnt_id_array_root, mnt_id_array,
            CP_MAX_MNTS*sizeof(int64_t));
    }

    bcst1=DARSHAN_MPI_CALL(PMPI_Wtime)();
    hlevel = bcst1*1000000;

    DARSHAN_MPI_CALL(PMPI_Bcast)(mnt_id_array_root,
        CP_MAX_MNTS*sizeof(int64_t), MPI_BYTE, 0, MPI_COMM_WORLD);
    DARSHAN_MPI_CALL(PMPI_Bcast)(mnt_hash_array_root,
        CP_MAX_MNTS*sizeof(uint64_t), MPI_BYTE, 0, MPI_COMM_WORLD);
    bcst2=DARSHAN_MPI_CALL(PMPI_Wtime)();

    /* identify any common mount points that have different device ids on
     * non-root processes
     */
    for(i=0; (i<CP_MAX_MNTS && mnt_hash_array_root[i] != 0); i++)
    {
        for(j=0; (j<CP_MAX_MNTS && mnt_hash_array[j] != 0); j++)
        {
            if(mnt_hash_array_root[i] == mnt_hash_array[j])
            {
                /* found a shared mount point */
                if(mnt_id_array_root[i] != mnt_id_array[j])
                {
                    /* mismatching ids; record correct mapping */
                    mnt_mapping[map_index].mnt_id_local =
                        mnt_id_array[j];
                    mnt_mapping[map_index].mnt_id_root = 
                        mnt_id_array_root[i];
                    map_index++;
                }
                break;
            }
        }
    }
 
    /* adjust affected file records */
    for(i=0; (i<final_job->file_count && map_index > 0); i++)
    {
        for(j=0; j<map_index; j++)
        {
            if(final_job->file_array[i].counters[CP_DEVICE] ==
                mnt_mapping[j].mnt_id_local)
            {
                final_job->file_array[i].counters[CP_DEVICE] =  
                    mnt_mapping[j].mnt_id_root;
                break;
            }
        }
    }
   
    /* construct log file name */
    if(rank == 0)
    {
        char cuser[L_cuserid] = {0};
        struct tm* my_tm;

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
        start_time_tmp += final_job->log_job.start_time;
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
    bcst3=DARSHAN_MPI_CALL(PMPI_Wtime)();
    DARSHAN_MPI_CALL(PMPI_Bcast)(logfile_name, PATH_MAX, MPI_CHAR, 0,
        MPI_COMM_WORLD);

    if(strlen(logfile_name) == 0)
    {
        /* failed to generate log file name */
        darshan_finalize(final_job);
	return;
    }

    final_job->log_job.end_time = time(NULL);

    /* reduce records for shared files */
    if(timing_flag)
        red1 = DARSHAN_MPI_CALL(PMPI_Wtime)();
    local_ret = cp_log_reduction(final_job, rank, logfile_name, 
        &next_offset);
    if(timing_flag)
        red2 = DARSHAN_MPI_CALL(PMPI_Wtime)();
    DARSHAN_MPI_CALL(PMPI_Allreduce)(&local_ret, &all_ret, 1, MPI_INT, MPI_LOR, 
        MPI_COMM_WORLD);

    /* if we are using any hints to write the log file, then record those
     * hints in the log file header
     */
    cp_log_record_hints(final_job, rank);

    if(all_ret == 0)
    {
        /* collect data to write from local process */
        cp_log_construct_indices(final_job, rank, &index_count, lengths, 
            pointers, final_job->trailing_data);
    }

    if(all_ret == 0)
    {
        /* compress data */
        if(timing_flag)
            gz1 = DARSHAN_MPI_CALL(PMPI_Wtime)();
        local_ret = cp_log_compress(final_job, rank, &index_count, 
            lengths, pointers);
        if(timing_flag)
            gz2 = DARSHAN_MPI_CALL(PMPI_Wtime)();
        DARSHAN_MPI_CALL(PMPI_Allreduce)(&local_ret, &all_ret, 1,
            MPI_INT, MPI_LOR, MPI_COMM_WORLD);
    }

    if(all_ret == 0)
    {
        /* actually write out log file */
        if(timing_flag)
            write1 = DARSHAN_MPI_CALL(PMPI_Wtime)();
        local_ret = cp_log_write(final_job, rank, logfile_name, 
            index_count, lengths, pointers, start_log_time);
        if(timing_flag)
            write2 = DARSHAN_MPI_CALL(PMPI_Wtime)();
        DARSHAN_MPI_CALL(PMPI_Allreduce)(&local_ret, &all_ret, 1,
            MPI_INT, MPI_LOR, MPI_COMM_WORLD);
    }

    if(rank == 0)
    {
        if(all_ret != 0)
        {
            fprintf(stderr, "darshan library warning: unable to write log file %s\n", logfile_name);
            /* if any process failed to write log, then delete the whole 
             * file so we don't leave corrupted results
             */
            unlink(logfile_name);
        }
        else
        {
            /* rename from *.darshan_partial to *-<logwritetime>.darshan.gz,
             * which indicates that this log file is complete and ready for
             * analysis
             */ 
            char* mod_index;
            double end_log_time;
            char* new_logfile_name;

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
                chmod(new_logfile_name, (S_IRUSR)); 
                free(new_logfile_name);
            }
        }
    }

    if(final_job->trailing_data)
        free(final_job->trailing_data);
    for(i=0; i<CP_MAX_MNTS; i++)
    {
        if(mnt_path_array[i])
        {
            free(mnt_path_array[i]);
            mnt_path_array[i] = NULL;
        }
    }
    free(logfile_name);
    darshan_finalize(final_job);
    
    if(timing_flag)
    {
        double red_tm, red_slowest;
        double gz_tm, gz_slowest;
        double write_tm, write_slowest;
        double all_tm, all_slowest;
        double bcst_tm, bcst_slowest;
        
        tm_end = DARSHAN_MPI_CALL(PMPI_Wtime)();

        bcst_tm=(bcst2-bcst1)+(red1-bcst3);
        red_tm = red2-red1;
        gz_tm = gz2-gz1;
        write_tm = write2-write1;
        all_tm = tm_end-start_log_time;

        DARSHAN_MPI_CALL(PMPI_Allreduce)(&red_tm, &red_slowest, 1,
            MPI_DOUBLE, MPI_MAX, MPI_COMM_WORLD);
        DARSHAN_MPI_CALL(PMPI_Allreduce)(&gz_tm, &gz_slowest, 1,
            MPI_DOUBLE, MPI_MAX, MPI_COMM_WORLD);
        DARSHAN_MPI_CALL(PMPI_Allreduce)(&write_tm, &write_slowest, 1,
            MPI_DOUBLE, MPI_MAX, MPI_COMM_WORLD);
        DARSHAN_MPI_CALL(PMPI_Allreduce)(&all_tm, &all_slowest, 1,
            MPI_DOUBLE, MPI_MAX, MPI_COMM_WORLD);
        DARSHAN_MPI_CALL(PMPI_Allreduce)(&bcst_tm, &bcst_slowest, 1,
            MPI_DOUBLE, MPI_MAX, MPI_COMM_WORLD);

        if(rank == 0)
        {
            DARSHAN_MPI_CALL(PMPI_Comm_size)(MPI_COMM_WORLD, &nprocs);
            printf("#<op>\t<nprocs>\t<time>\n");
            printf("bcst\t%d\t%f\n", nprocs, bcst_slowest);
            printf("reduce\t%d\t%f\n", nprocs, red_slowest);
            printf("gzip\t%d\t%f\n", nprocs, gz_slowest);
            printf("write\t%d\t%f\n", nprocs, write_slowest);
            printf("all\t%d\t%f\n", nprocs, all_slowest);
        }
    }

    return;
}

int MPI_File_open(MPI_Comm comm, char *filename, int amode, MPI_Info info, MPI_File *fh) 
{
    int ret;
    struct darshan_file_runtime* file;
    char* tmp;
    int comm_size;
    double tm1, tm2;

    tm1 = darshan_wtime();
    ret = DARSHAN_MPI_CALL(PMPI_File_open)(comm, filename, amode, info, fh);
    tm2 = darshan_wtime();

    if(ret == MPI_SUCCESS)
    {
        CP_LOCK();

        /* use ROMIO approach to strip prefix if present */
        /* strip off prefix if there is one, but only skip prefixes
         * if they are greater than length one to allow for windows
         * drive specifications (e.g. c:\...) 
         */
        tmp = strchr(filename, ':');
        if (tmp > filename + 1) {
            filename = tmp + 1;
        }

        file = darshan_file_by_name_setfh(filename, (*fh));
        if(file)
        {
            CP_SET(file, CP_MODE, amode);
            CP_F_INC_NO_OVERLAP(file, tm1, tm2, file->last_mpi_meta_end, CP_F_MPI_META_TIME);
            if(CP_F_VALUE(file, CP_F_OPEN_TIMESTAMP) == 0)
                CP_F_SET(file, CP_F_OPEN_TIMESTAMP,
                DARSHAN_MPI_CALL(PMPI_Wtime)());
            DARSHAN_MPI_CALL(PMPI_Comm_size)(comm, &comm_size);
            if(comm_size == 1)
            {
                CP_INC(file, CP_INDEP_OPENS, 1);
            }
            else
            {
                CP_INC(file, CP_COLL_OPENS, 1);
            }
            if(info != MPI_INFO_NULL)
            {
                CP_INC(file, CP_HINTS, 1);
            }
        }

        CP_UNLOCK();
    }

    return(ret);
}

int MPI_File_close(MPI_File *fh) 
{
    struct darshan_file_runtime* file;
    MPI_File tmp_fh = *fh;
    double tm1, tm2;
    int ret;
    
    tm1 = darshan_wtime();
    ret = DARSHAN_MPI_CALL(PMPI_File_close)(fh);
    tm2 = darshan_wtime();

    CP_LOCK();
    file = darshan_file_by_fh(tmp_fh);
    if(file)
    {
        CP_F_SET(file, CP_F_CLOSE_TIMESTAMP, DARSHAN_MPI_CALL(PMPI_Wtime)());
        CP_F_INC_NO_OVERLAP(file, tm1, tm2, file->last_mpi_meta_end, CP_F_MPI_META_TIME);
        darshan_file_close_fh(tmp_fh);
    }
    CP_UNLOCK();

    return(ret);
}

int MPI_File_sync(MPI_File fh)
{
    int ret;
    struct darshan_file_runtime* file;
    double tm1, tm2;

    tm1 = darshan_wtime();
    ret = DARSHAN_MPI_CALL(PMPI_File_sync)(fh);
    tm2 = darshan_wtime();
    if(ret == MPI_SUCCESS)
    {
        CP_LOCK();
        file = darshan_file_by_fh(fh);
        if(file)
        {
            CP_F_INC_NO_OVERLAP(file, tm1, tm2, file->last_mpi_write_end, CP_F_MPI_WRITE_TIME);
            CP_INC(file, CP_SYNCS, 1);
        }
        CP_UNLOCK();
    }

    return(ret);
}


int MPI_File_set_view(MPI_File fh, MPI_Offset disp, MPI_Datatype etype, 
    MPI_Datatype filetype, char *datarep, MPI_Info info)
{
    int ret;
    struct darshan_file_runtime* file;
    double tm1, tm2;

    tm1 = darshan_wtime();
    ret = DARSHAN_MPI_CALL(PMPI_File_set_view)(fh, disp, etype,
        filetype, datarep, info);
    tm2 = darshan_wtime();
    if(ret == MPI_SUCCESS)
    {
        CP_LOCK();
        file = darshan_file_by_fh(fh);
        if(file)
        {
            CP_INC(file, CP_VIEWS, 1);
            if(info != MPI_INFO_NULL)
            {
                CP_F_INC_NO_OVERLAP(file, tm1, tm2, file->last_mpi_meta_end, CP_F_MPI_META_TIME);
                CP_INC(file, CP_HINTS, 1);
            }
            CP_DATATYPE_INC(file, filetype);
        }
        CP_UNLOCK();
    }

    return(ret);
}

int MPI_File_read(MPI_File fh, void *buf, int count, 
    MPI_Datatype datatype, MPI_Status *status)
{
    int ret;
    double tm1, tm2;

    tm1 = darshan_wtime();
    ret = DARSHAN_MPI_CALL(PMPI_File_read)(fh, buf, count, datatype, status);
    tm2 = darshan_wtime();
    CP_LOCK();
    CP_RECORD_MPI_READ(ret, fh, count, datatype, CP_INDEP_READS, tm1, tm2);
    CP_UNLOCK();
    return(ret);
}

int MPI_File_read_at(MPI_File fh, MPI_Offset offset, void *buf,
    int count, MPI_Datatype datatype, MPI_Status *status)
{
    int ret;
    double tm1, tm2;

    tm1 = darshan_wtime();
    ret = DARSHAN_MPI_CALL(PMPI_File_read_at)(fh, offset, buf,
        count, datatype, status);
    tm2 = darshan_wtime();
    CP_LOCK();
    CP_RECORD_MPI_READ(ret, fh, count, datatype, CP_INDEP_READS, tm1, tm2);
    CP_UNLOCK();
    return(ret);
}

int MPI_File_read_at_all(MPI_File fh, MPI_Offset offset, void * buf,
    int count, MPI_Datatype datatype, MPI_Status * status)
{
    int ret;
    double tm1, tm2;

    tm1 = darshan_wtime();
    ret = DARSHAN_MPI_CALL(PMPI_File_read_at_all)(fh, offset, buf,
        count, datatype, status);
    tm2 = darshan_wtime();
    CP_LOCK();
    CP_RECORD_MPI_READ(ret, fh, count, datatype, CP_COLL_READS, tm1, tm2);
    CP_UNLOCK();
    return(ret);
}

int MPI_File_read_all(MPI_File fh, void * buf, int count, MPI_Datatype datatype, MPI_Status *status)
{
    int ret;
    double tm1, tm2;

    tm1 = darshan_wtime();
    ret = DARSHAN_MPI_CALL(PMPI_File_read_all)(fh, buf, count,
        datatype, status);
    tm2 = darshan_wtime();
    CP_LOCK();
    CP_RECORD_MPI_READ(ret, fh, count, datatype, CP_COLL_READS, tm1, tm2);
    CP_UNLOCK();
    return(ret);
}

int MPI_File_read_shared(MPI_File fh, void * buf, int count, MPI_Datatype datatype, MPI_Status *status)
{
    int ret;
    double tm1, tm2;

    tm1 = darshan_wtime();
    ret = DARSHAN_MPI_CALL(PMPI_File_read_shared)(fh, buf, count,
        datatype, status);
    tm2 = darshan_wtime();
    CP_LOCK();
    CP_RECORD_MPI_READ(ret, fh, count, datatype, CP_INDEP_READS, tm1, tm2);
    CP_UNLOCK();
    return(ret);
}

int MPI_File_read_ordered(MPI_File fh, void * buf, int count, 
    MPI_Datatype datatype, MPI_Status * status)
{
    int ret;
    double tm1, tm2;

    tm1 = darshan_wtime();
    ret = DARSHAN_MPI_CALL(PMPI_File_read_ordered)(fh, buf, count,
        datatype, status);
    tm2 = darshan_wtime();
    CP_LOCK();
    CP_RECORD_MPI_READ(ret, fh, count, datatype, CP_COLL_READS, tm1, tm2);
    CP_UNLOCK();
    return(ret);
}

int MPI_File_read_at_all_begin(MPI_File fh, MPI_Offset offset, void * buf,
    int count, MPI_Datatype datatype)
{
    int ret;
    double tm1, tm2;

    tm1 = darshan_wtime();
    ret = DARSHAN_MPI_CALL(PMPI_File_read_at_all_begin)(fh, offset, buf,
        count, datatype);
    tm2 = darshan_wtime();
    CP_LOCK();
    CP_RECORD_MPI_READ(ret, fh, count, datatype, CP_SPLIT_READS, tm1, tm2);
    CP_UNLOCK();
    return(ret);
}

int MPI_File_read_all_begin(MPI_File fh, void * buf, int count, MPI_Datatype datatype)
{
    int ret;
    double tm1, tm2;

    tm1 = darshan_wtime();
    ret = DARSHAN_MPI_CALL(PMPI_File_read_all_begin)(fh, buf, count, datatype);
    tm2 = darshan_wtime();
    CP_LOCK();
    CP_RECORD_MPI_READ(ret, fh, count, datatype, CP_SPLIT_READS, tm1, tm2);
    CP_UNLOCK();
    return(ret);
}

int MPI_File_read_ordered_begin(MPI_File fh, void * buf, int count, MPI_Datatype datatype)
{
    int ret;
    double tm1, tm2;

    tm1 = darshan_wtime();
    ret = DARSHAN_MPI_CALL(PMPI_File_read_ordered_begin)(fh, buf, count,
        datatype);
    tm2 = darshan_wtime();
    CP_LOCK();
    CP_RECORD_MPI_READ(ret, fh, count, datatype, CP_SPLIT_READS, tm1, tm2);
    CP_UNLOCK();
    return(ret);
}

int MPI_File_iread_at(MPI_File fh, MPI_Offset offset, void * buf,
    int count, MPI_Datatype datatype, __D_MPI_REQUEST *request)
{
    int ret;
    double tm1, tm2;

    tm1 = darshan_wtime();
    ret = DARSHAN_MPI_CALL(PMPI_File_iread_at)(fh, offset, buf, count,
        datatype, request);
    tm2 = darshan_wtime();
    CP_LOCK();
    CP_RECORD_MPI_READ(ret, fh, count, datatype, CP_NB_READS, tm1, tm2);
    CP_UNLOCK();
    return(ret);
}

int MPI_File_iread(MPI_File fh, void * buf, int count, MPI_Datatype datatype, __D_MPI_REQUEST * request)
{
    int ret;
    double tm1, tm2;

    tm1 = darshan_wtime();
    ret = DARSHAN_MPI_CALL(PMPI_File_iread)(fh, buf, count, datatype, request);
    tm2 = darshan_wtime();
    CP_LOCK();
    CP_RECORD_MPI_READ(ret, fh, count, datatype, CP_NB_READS, tm1, tm2);
    CP_UNLOCK();
    return(ret);
}

int MPI_File_iread_shared(MPI_File fh, void * buf, int count,
    MPI_Datatype datatype, __D_MPI_REQUEST * request)
{
    int ret;
    double tm1, tm2;

    tm1 = darshan_wtime();
    ret = DARSHAN_MPI_CALL(PMPI_File_iread_shared)(fh, buf, count,
        datatype, request);
    tm2 = darshan_wtime();
    CP_LOCK();
    CP_RECORD_MPI_READ(ret, fh, count, datatype, CP_NB_READS, tm1, tm2);
    CP_UNLOCK();
    return(ret);
}


int MPI_File_write(MPI_File fh, void *buf, int count, 
    MPI_Datatype datatype, MPI_Status *status)
{
    int ret;
    double tm1, tm2;

    tm1 = darshan_wtime();
    ret = DARSHAN_MPI_CALL(PMPI_File_write)(fh, buf, count, datatype, status);
    tm2 = darshan_wtime();
    CP_LOCK();
    CP_RECORD_MPI_WRITE(ret, fh, count, datatype, CP_INDEP_WRITES, tm1, tm2);
    CP_UNLOCK();
    return(ret);
}

int MPI_File_write_at(MPI_File fh, MPI_Offset offset, void *buf,
    int count, MPI_Datatype datatype, MPI_Status *status)
{
    int ret;
    double tm1, tm2;

    tm1 = darshan_wtime();
    ret = DARSHAN_MPI_CALL(PMPI_File_write_at)(fh, offset, buf,
        count, datatype, status);
    tm2 = darshan_wtime();
    CP_LOCK();
    CP_RECORD_MPI_WRITE(ret, fh, count, datatype, CP_INDEP_WRITES, tm1, tm2);
    CP_UNLOCK();
    return(ret);
}

int MPI_File_write_at_all(MPI_File fh, MPI_Offset offset, void * buf,
    int count, MPI_Datatype datatype, MPI_Status * status)
{
    int ret;
    double tm1, tm2;

    tm1 = darshan_wtime();
    ret = DARSHAN_MPI_CALL(PMPI_File_write_at_all)(fh, offset, buf,
        count, datatype, status);
    tm2 = darshan_wtime();
    CP_LOCK();
    CP_RECORD_MPI_WRITE(ret, fh, count, datatype, CP_COLL_WRITES, tm1, tm2);
    CP_UNLOCK();
    return(ret);
}

int MPI_File_write_all(MPI_File fh, void * buf, int count, MPI_Datatype datatype, MPI_Status *status)
{
    int ret;
    double tm1, tm2;

    tm1 = darshan_wtime();
    ret = DARSHAN_MPI_CALL(PMPI_File_write_all)(fh, buf, count,
        datatype, status);
    tm2 = darshan_wtime();
    CP_LOCK();
    CP_RECORD_MPI_WRITE(ret, fh, count, datatype, CP_COLL_WRITES, tm1, tm2);
    CP_UNLOCK();
    return(ret);
}

int MPI_File_write_shared(MPI_File fh, void * buf, int count, MPI_Datatype datatype, MPI_Status *status)
{
    int ret;
    double tm1, tm2;

    tm1 = darshan_wtime();
    ret = DARSHAN_MPI_CALL(PMPI_File_write_shared)(fh, buf, count,
        datatype, status);
    tm2 = darshan_wtime();
    CP_LOCK();
    CP_RECORD_MPI_WRITE(ret, fh, count, datatype, CP_INDEP_WRITES, tm1, tm2);
    CP_UNLOCK();
    return(ret);
}

int MPI_File_write_ordered(MPI_File fh, void * buf, int count, 
    MPI_Datatype datatype, MPI_Status * status)
{
    int ret;
    double tm1, tm2;

    tm1 = darshan_wtime();
    ret = DARSHAN_MPI_CALL(PMPI_File_write_ordered)(fh, buf, count,
         datatype, status);
    tm2 = darshan_wtime();
    CP_LOCK();
    CP_RECORD_MPI_WRITE(ret, fh, count, datatype, CP_COLL_WRITES, tm1, tm2);
    CP_UNLOCK();
    return(ret);
}

int MPI_File_write_at_all_begin(MPI_File fh, MPI_Offset offset, void * buf,
    int count, MPI_Datatype datatype)
{
    int ret;
    double tm1, tm2;

    tm1 = darshan_wtime();
    ret = DARSHAN_MPI_CALL(PMPI_File_write_at_all_begin)(fh, offset,
        buf, count, datatype);
    tm2 = darshan_wtime();
    CP_LOCK();
    CP_RECORD_MPI_WRITE(ret, fh, count, datatype, CP_SPLIT_WRITES, tm1, tm2);
    CP_UNLOCK();
    return(ret);
}

int MPI_File_write_all_begin(MPI_File fh, void * buf, int count, MPI_Datatype datatype)
{
    int ret;
    double tm1, tm2;

    tm1 = darshan_wtime();
    ret = DARSHAN_MPI_CALL(PMPI_File_write_all_begin)(fh, buf, count, datatype);
    tm2 = darshan_wtime();
    CP_LOCK();
    CP_RECORD_MPI_WRITE(ret, fh, count, datatype, CP_SPLIT_WRITES, tm1, tm2);
    CP_UNLOCK();
    return(ret);
}

int MPI_File_write_ordered_begin(MPI_File fh, void * buf, int count, MPI_Datatype datatype)
{
    int ret;
    double tm1, tm2;

    tm1 = darshan_wtime();
    ret = DARSHAN_MPI_CALL(PMPI_File_write_ordered_begin)(fh, buf, count,
        datatype);
    tm2 = darshan_wtime();
    CP_LOCK();
    CP_RECORD_MPI_WRITE(ret, fh, count, datatype, CP_SPLIT_WRITES, tm1, tm2);
    CP_UNLOCK();
    return(ret);
}

int MPI_File_iwrite_at(MPI_File fh, MPI_Offset offset, void * buf,
    int count, MPI_Datatype datatype, __D_MPI_REQUEST *request)
{
    int ret;
    double tm1, tm2;

    tm1 = darshan_wtime();
    ret = DARSHAN_MPI_CALL(PMPI_File_iwrite_at)(fh, offset, buf,
        count, datatype, request);
    tm2 = darshan_wtime();
    CP_LOCK();
    CP_RECORD_MPI_WRITE(ret, fh, count, datatype, CP_NB_WRITES, tm1, tm2);
    CP_UNLOCK();
    return(ret);
}

int MPI_File_iwrite(MPI_File fh, void * buf, int count, MPI_Datatype datatype, __D_MPI_REQUEST * request)
{
    int ret;
    double tm1, tm2;

    tm1 = darshan_wtime();
    ret = DARSHAN_MPI_CALL(PMPI_File_iwrite)(fh, buf, count, datatype, request);
    tm2 = darshan_wtime();
    CP_LOCK();
    CP_RECORD_MPI_WRITE(ret, fh, count, datatype, CP_NB_WRITES, tm1, tm2);
    CP_UNLOCK();
    return(ret);
}

int MPI_File_iwrite_shared(MPI_File fh, void * buf, int count,
    MPI_Datatype datatype, __D_MPI_REQUEST * request)
{
    int ret;
    double tm1, tm2;

    tm1 = darshan_wtime();
    ret = DARSHAN_MPI_CALL(PMPI_File_iwrite_shared)(fh, buf, count,
        datatype, request);
    tm2 = darshan_wtime();
    CP_LOCK();
    CP_RECORD_MPI_WRITE(ret, fh, count, datatype, CP_NB_WRITES, tm1, tm2);
    CP_UNLOCK();
    return(ret);
}

/* cp_log_reduction()
 *
 * Identify shared files and reduce them to one log entry
 *
 * returns 0 on success, -1 on failure
 */
static int cp_log_reduction(struct darshan_job_runtime* final_job, int rank, 
    char* logfile_name, MPI_Offset* next_offset)
{
    /* TODO: these need to be allocated differently now, too big */
    uint64_t hash_array[CP_MAX_FILES] = {0};
    int mask_array[CP_MAX_FILES] = {0};
    int all_mask_array[CP_MAX_FILES] = {0};
    int ret;
    int i;
    int j;
    MPI_Op reduce_op;
    MPI_Datatype rtype;
    struct darshan_file* tmp_array = NULL;
    int shared_count = 0;

    /* register a reduction operation */
    ret = DARSHAN_MPI_CALL(PMPI_Op_create)(darshan_file_reduce, 1, &reduce_op); 
    if(ret != 0)
    {
        return(-1);
    }

    /* construct a datatype for a file record.  This is serving no purpose
     * except to make sure we can do a reduction on proper boundaries
     */
    DARSHAN_MPI_CALL(PMPI_Type_contiguous)(sizeof(struct darshan_file),
        MPI_BYTE, &rtype); 
    DARSHAN_MPI_CALL(PMPI_Type_commit)(&rtype); 

    /* gather list of files that root process has opened */
    if(rank == 0)
    {
        for(i=0; i<final_job->file_count; i++)
        {
            hash_array[i] = final_job->file_array[i].hash;
        }
    }

    /* broadcast list of files to all other processes */
    ret = DARSHAN_MPI_CALL(PMPI_Bcast)(hash_array,
        (CP_MAX_FILES * sizeof(uint64_t)), 
        MPI_BYTE, 0, MPI_COMM_WORLD);
    if(ret != 0)
    {
        DARSHAN_MPI_CALL(PMPI_Op_free)(&reduce_op);
        DARSHAN_MPI_CALL(PMPI_Type_free)(&rtype);
        return(-1);
    }

    /* everyone looks to see if they have also opened that same file */
    for(i=0; (i<CP_MAX_FILES && hash_array[i] != 0); i++)
    {
        for(j=0; j<final_job->file_count; j++)
        {
            if(hash_array[i] && final_job->file_array[j].hash == hash_array[i])
            {
                /* we opened that file too */
                mask_array[i] = 1;
                break;
            }
        }
    }

    /* now allreduce so that everyone agrees on which files are shared */
    ret = DARSHAN_MPI_CALL(PMPI_Allreduce)(mask_array, all_mask_array,
        CP_MAX_FILES, MPI_INT, MPI_LAND, MPI_COMM_WORLD);
    if(ret != 0)
    {
        DARSHAN_MPI_CALL(PMPI_Op_free)(&reduce_op);
        DARSHAN_MPI_CALL(PMPI_Type_free)(&rtype);
        return(-1);
    }

    /* walk through mask array counting entries and marking corresponding
     * files with a rank of -1
     */
    for(i=0; i<CP_MAX_FILES; i++)
    {
        if(all_mask_array[i])
        {
            shared_count++;
            for(j=0; j<final_job->file_count; j++)
            {
                if(final_job->file_array[j].hash == hash_array[i])
                {

                    /*
                     * Initialize fastest/slowest info prior
                     * to the reduction.
                     */
                    final_job->file_array[j].counters[CP_FASTEST_RANK] =
                      final_job->file_array[j].rank;
                    final_job->file_array[j].counters[CP_FASTEST_RANK_BYTES] =
                      final_job->file_array[j].counters[CP_BYTES_READ] +
                      final_job->file_array[j].counters[CP_BYTES_WRITTEN];
                    final_job->file_array[j].fcounters[CP_F_FASTEST_RANK_TIME] =
                      final_job->file_array[j].fcounters[CP_F_POSIX_META_TIME] +
                      final_job->file_array[j].fcounters[CP_F_POSIX_READ_TIME] +
                      final_job->file_array[j].fcounters[CP_F_POSIX_WRITE_TIME];

                    final_job->file_array[j].counters[CP_SLOWEST_RANK] =
                      final_job->file_array[j].rank;
                    final_job->file_array[j].counters[CP_SLOWEST_RANK_BYTES] =
                      final_job->file_array[j].counters[CP_BYTES_READ] +
                      final_job->file_array[j].counters[CP_BYTES_WRITTEN];
                    final_job->file_array[j].fcounters[CP_F_SLOWEST_RANK_TIME] =
                      final_job->file_array[j].fcounters[CP_F_POSIX_META_TIME] +
                      final_job->file_array[j].fcounters[CP_F_POSIX_READ_TIME] +
                      final_job->file_array[j].fcounters[CP_F_POSIX_WRITE_TIME];

                    final_job->file_array[j].rank = -1;
                    break;
                }
            }
        }
    }

    if(shared_count)
    {
        if(rank == 0)
        {
            /* root proc needs to allocate memory to store reduction */
            tmp_array = malloc(shared_count*sizeof(struct darshan_file));
            if(!tmp_array)
            {
                /* TODO: think more about how to handle errors like this */
                DARSHAN_MPI_CALL(PMPI_Op_free)(&reduce_op);
                DARSHAN_MPI_CALL(PMPI_Type_free)(&rtype);
                return(-1);
            }
        }

        /* sort the array of files descending by rank so that we get all of the 
         * shared files (marked by rank -1) in a contiguous portion at end 
         * of the array
         */
        qsort(final_job->file_array, final_job->file_count, 
            sizeof(struct darshan_file), file_compare);

        ret = DARSHAN_MPI_CALL(PMPI_Reduce)(
            &final_job->file_array[final_job->file_count-shared_count], 
            tmp_array, shared_count, rtype, reduce_op, 0, MPI_COMM_WORLD);
        if(ret != 0)
        {
            DARSHAN_MPI_CALL(PMPI_Op_free)(&reduce_op);
            DARSHAN_MPI_CALL(PMPI_Type_free)(&rtype);
            return(-1);
        }

        ret = darshan_file_variance(
            &final_job->file_array[final_job->file_count-shared_count],
            tmp_array, shared_count, rank);
        if (ret)
        {
            DARSHAN_MPI_CALL(PMPI_Op_free)(&reduce_op);
            DARSHAN_MPI_CALL(PMPI_Type_free)(&rtype);
            return(-1);
        }

        if(rank == 0)
        {
            /* root replaces local files with shared ones */
            memcpy(&final_job->file_array[final_job->file_count-shared_count],
                tmp_array, shared_count*sizeof(struct darshan_file));
            free(tmp_array);
            tmp_array = NULL;
        }
        else
        {
            /* everyone else simply discards those file records */
            final_job->file_count -= shared_count;
        }
    }
    
    DARSHAN_MPI_CALL(PMPI_Op_free)(&reduce_op);
    DARSHAN_MPI_CALL(PMPI_Type_free)(&rtype);

    return(0);
}

/* TODO: should we use more of the CP macros here? */
static void darshan_file_reduce(void* infile_v, 
    void* inoutfile_v, int *len, 
    MPI_Datatype *datatype)
{
    struct darshan_file tmp_file;
    struct darshan_file* infile = infile_v;
    struct darshan_file* inoutfile = inoutfile_v;
    struct darshan_file_runtime tmp_runtime;
    int i;
    int j;
    int k;

    for(i=0; i<*len; i++)
    {
        memset(&tmp_file, 0, sizeof(tmp_file));

        tmp_file.hash = infile->hash;
        tmp_file.rank = -1; /* indicates shared across all procs */

        /* sum */
        for(j=CP_INDEP_OPENS; j<=CP_VIEWS; j++)
        {
            tmp_file.counters[j] = infile->counters[j] + 
                inoutfile->counters[j];
        }

        /* pick one, favoring complete records if available */
        if(CP_FILE_PARTIAL(infile))
            tmp_file.counters[CP_MODE] = inoutfile->counters[CP_MODE];
        else
            tmp_file.counters[CP_MODE] = infile->counters[CP_MODE];


        /* sum */
        for(j=CP_BYTES_READ; j<=CP_BYTES_WRITTEN; j++)
        {
            tmp_file.counters[j] = infile->counters[j] + 
                inoutfile->counters[j];
        }

        /* max */
        for(j=CP_MAX_BYTE_READ; j<=CP_MAX_BYTE_WRITTEN; j++)
        {
            tmp_file.counters[j] = (
                (infile->counters[j] > inoutfile->counters[j]) ? 
                infile->counters[j] :
                inoutfile->counters[j]);
        }

        /* sum */
        for(j=CP_CONSEC_READS; j<=CP_MEM_NOT_ALIGNED; j++)
        {
            tmp_file.counters[j] = infile->counters[j] + 
                inoutfile->counters[j];
        }

        /* pick one, favoring complete records if available */
        if(CP_FILE_PARTIAL(infile))
            tmp_file.counters[CP_MEM_ALIGNMENT] = inoutfile->counters[CP_MEM_ALIGNMENT];
        else
            tmp_file.counters[CP_MEM_ALIGNMENT] = infile->counters[CP_MEM_ALIGNMENT];

        /* sum */
        for(j=CP_FILE_NOT_ALIGNED; j<=CP_FILE_NOT_ALIGNED; j++)
        {
            tmp_file.counters[j] = infile->counters[j] + 
                inoutfile->counters[j];
        }

        /* pick one, favoring complete records if available */
        if(CP_FILE_PARTIAL(infile))
            tmp_file.counters[CP_FILE_ALIGNMENT] = inoutfile->counters[CP_FILE_ALIGNMENT];
        else
            tmp_file.counters[CP_FILE_ALIGNMENT] = infile->counters[CP_FILE_ALIGNMENT];
        
        /* skip CP_MAX_*_TIME_SIZE; handled in floating point section */

        /* sum */
        for(j=CP_SIZE_READ_0_100; j<=CP_EXTENT_WRITE_1G_PLUS; j++)
        {
            tmp_file.counters[j] = infile->counters[j] + 
                inoutfile->counters[j];
        }

        /* pick the 4 most common strides out of the 8 we have to chose from */

        /* first collapse any duplicates */
        for(j=CP_STRIDE1_STRIDE; j<=CP_STRIDE4_STRIDE; j++)
        {
            for(k=CP_STRIDE1_STRIDE; k<=CP_STRIDE4_STRIDE; k++)
            {
                if(infile->counters[j] == inoutfile->counters[k])
                {
                    infile->counters[j+4] += inoutfile->counters[k+4];
                    inoutfile->counters[k] = 0;
                    inoutfile->counters[k+4] = 0;
                }
            }
        }

        /* placeholder so we can re-use macros */
        tmp_runtime.log_file = &tmp_file;
        /* first set */
        for(j=CP_STRIDE1_STRIDE; j<=CP_STRIDE4_STRIDE; j++)
        {
            CP_COUNTER_INC(&tmp_runtime, infile->counters[j],
                infile->counters[j+4], 1, CP_STRIDE1_STRIDE, CP_STRIDE1_COUNT);
        }
        /* second set */
        for(j=CP_STRIDE1_STRIDE; j<=CP_STRIDE4_STRIDE; j++)
        {
            CP_COUNTER_INC(&tmp_runtime, inoutfile->counters[j],
                inoutfile->counters[j+4], 1, CP_STRIDE1_STRIDE, CP_STRIDE1_COUNT);
        }

        /* TODO: subroutine so we don't duplicate so much */
        /* same for access counts */

        /* first collapse any duplicates */
        for(j=CP_ACCESS1_ACCESS; j<=CP_ACCESS4_ACCESS; j++)
        {
            for(k=CP_ACCESS1_ACCESS; k<=CP_ACCESS4_ACCESS; k++)
            {
                if(infile->counters[j] == inoutfile->counters[k])
                {
                    infile->counters[j+4] += inoutfile->counters[k+4];
                    inoutfile->counters[k] = 0;
                    inoutfile->counters[k+4] = 0;
                }
            }
        }

        /* placeholder so we can re-use macros */
        tmp_runtime.log_file = &tmp_file;
        /* first set */
        for(j=CP_ACCESS1_ACCESS; j<=CP_ACCESS4_ACCESS; j++)
        {
            CP_COUNTER_INC(&tmp_runtime, infile->counters[j],
                infile->counters[j+4], 1, CP_ACCESS1_ACCESS, CP_ACCESS1_COUNT);
        }
        /* second set */
        for(j=CP_ACCESS1_ACCESS; j<=CP_ACCESS4_ACCESS; j++)
        {
            CP_COUNTER_INC(&tmp_runtime, inoutfile->counters[j],
                inoutfile->counters[j+4], 1, CP_ACCESS1_ACCESS, CP_ACCESS1_COUNT);
        }

        /* min non-zero (if available) value */
        for(j=CP_F_OPEN_TIMESTAMP; j<=CP_F_WRITE_START_TIMESTAMP; j++)
        {
            if(infile->fcounters[j] > inoutfile->fcounters[j] && inoutfile->fcounters[j] > 0)
                tmp_file.fcounters[j] = inoutfile->fcounters[j];
            else
                tmp_file.fcounters[j] = infile->fcounters[j];
        }

        /* max */
        for(j=CP_F_CLOSE_TIMESTAMP; j<=CP_F_WRITE_END_TIMESTAMP; j++)
        {
            if(infile->fcounters[j] > inoutfile->fcounters[j])
                tmp_file.fcounters[j] = infile->fcounters[j];
            else
                tmp_file.fcounters[j] = inoutfile->fcounters[j];
        }

        /* sum */
        for(j=CP_F_POSIX_READ_TIME; j<=CP_F_MPI_WRITE_TIME; j++)
        {
            tmp_file.fcounters[j] = infile->fcounters[j] + 
                inoutfile->fcounters[j];
        }

        /* max (special case) */
        if(infile->fcounters[CP_F_MAX_WRITE_TIME] > 
            inoutfile->fcounters[CP_F_MAX_WRITE_TIME])
        {
            tmp_file.fcounters[CP_F_MAX_WRITE_TIME] = 
                infile->fcounters[CP_F_MAX_WRITE_TIME];
            tmp_file.counters[CP_MAX_WRITE_TIME_SIZE] = 
                infile->counters[CP_MAX_WRITE_TIME_SIZE];
        }
        else
        {
            tmp_file.fcounters[CP_F_MAX_WRITE_TIME] = 
                inoutfile->fcounters[CP_F_MAX_WRITE_TIME];
            tmp_file.counters[CP_MAX_WRITE_TIME_SIZE] = 
                inoutfile->counters[CP_MAX_WRITE_TIME_SIZE];
        }

        if(infile->fcounters[CP_F_MAX_READ_TIME] > 
            inoutfile->fcounters[CP_F_MAX_READ_TIME])
        {
            tmp_file.fcounters[CP_F_MAX_READ_TIME] = 
                infile->fcounters[CP_F_MAX_READ_TIME];
            tmp_file.counters[CP_MAX_READ_TIME_SIZE] = 
                infile->counters[CP_MAX_READ_TIME_SIZE];
        }
        else
        {
            tmp_file.fcounters[CP_F_MAX_READ_TIME] = 
                inoutfile->fcounters[CP_F_MAX_READ_TIME];
            tmp_file.counters[CP_MAX_READ_TIME_SIZE] = 
                inoutfile->counters[CP_MAX_READ_TIME_SIZE];
        }

        /* min (zeroes are ok here; some procs don't do I/O) */
        if(infile->fcounters[CP_F_FASTEST_RANK_TIME] <
           inoutfile->fcounters[CP_F_FASTEST_RANK_TIME])
        {
            tmp_file.counters[CP_FASTEST_RANK] =
                infile->counters[CP_FASTEST_RANK];
            tmp_file.counters[CP_FASTEST_RANK_BYTES] = 
                infile->counters[CP_FASTEST_RANK_BYTES];
            tmp_file.fcounters[CP_F_FASTEST_RANK_TIME] =
                infile->fcounters[CP_F_FASTEST_RANK_TIME];
        }
        else
        {
            tmp_file.counters[CP_FASTEST_RANK] =
                inoutfile->counters[CP_FASTEST_RANK];
            tmp_file.counters[CP_FASTEST_RANK_BYTES] =
                inoutfile->counters[CP_FASTEST_RANK_BYTES];
            tmp_file.fcounters[CP_F_FASTEST_RANK_TIME] = 
                inoutfile->fcounters[CP_F_FASTEST_RANK_TIME];
        }

        /* max */
        if(infile->fcounters[CP_F_SLOWEST_RANK_TIME] >
           inoutfile->fcounters[CP_F_SLOWEST_RANK_TIME])
        {
            tmp_file.counters[CP_SLOWEST_RANK] =
                infile->counters[CP_SLOWEST_RANK];
            tmp_file.counters[CP_SLOWEST_RANK_BYTES] =
                infile->counters[CP_SLOWEST_RANK_BYTES];
            tmp_file.fcounters[CP_F_SLOWEST_RANK_TIME] = 
                infile->fcounters[CP_F_SLOWEST_RANK_TIME];
        }
        else
        {
            tmp_file.counters[CP_SLOWEST_RANK] = 
                inoutfile->counters[CP_SLOWEST_RANK];
            tmp_file.counters[CP_SLOWEST_RANK_BYTES] = 
                inoutfile->counters[CP_SLOWEST_RANK_BYTES];
            tmp_file.fcounters[CP_F_SLOWEST_RANK_TIME] = 
                inoutfile->fcounters[CP_F_SLOWEST_RANK_TIME];
        }

        /* pick one device id and file size, favoring complete records if
         * available
         */
        if(CP_FILE_PARTIAL(infile))
        {
            tmp_file.counters[CP_DEVICE] = inoutfile->counters[CP_DEVICE];
            tmp_file.counters[CP_SIZE_AT_OPEN] = inoutfile->counters[CP_SIZE_AT_OPEN];
        }
        else
        {
            tmp_file.counters[CP_DEVICE] = infile->counters[CP_DEVICE];
            tmp_file.counters[CP_SIZE_AT_OPEN] = infile->counters[CP_SIZE_AT_OPEN];
        }

        /* pick one name suffix (every file record should have this, whether
         * it is a partial record or not
         */
        strcpy(tmp_file.name_suffix, infile->name_suffix);

        *inoutfile = tmp_file;
        inoutfile++;
        infile++;
    }
    
    return;
}
/* cp_log_construct_indices()
 *
 * create memory datatypes to describe the log data to write out
 */
static void cp_log_construct_indices(struct darshan_job_runtime* final_job, 
    int rank, int* inout_count, int* lengths, void** pointers, char*
    trailing_data)
{
    *inout_count = 0;

    if(rank == 0)
    {
        /* root process is responsible for writing header */
        lengths[*inout_count] = sizeof(final_job->log_job);
        pointers[*inout_count] = &final_job->log_job;
        (*inout_count)++;

        /* also string containing exe command line */
        lengths[*inout_count] = CP_EXE_LEN + 1; 
        pointers[*inout_count] = trailing_data;
        (*inout_count)++;
    }

    /* everyone adds their own file records, if present */
    if(final_job->file_count > 0)
    {
        lengths[*inout_count] = final_job->file_count*CP_FILE_RECORD_SIZE;
        pointers[*inout_count] = final_job->file_array;
        (*inout_count)++;
    }
    
    return;
}

/* cp_log_write()
 *
 * actually write log information to disk
 */
static int cp_log_write(struct darshan_job_runtime* final_job, int rank, 
    char* logfile_name, int count, int* lengths, void** pointers, double start_log_time)
{
    int ret;
    MPI_File fh;
    MPI_Status status;
    MPI_Datatype mtype;
    int my_total = 0;
    long my_total_long;
    long offset;
    int i;
    MPI_Aint displacements[CP_MAX_MEM_SEGMENTS];
    void* buf;
    int failed_write = 0;
    char* hints;
    char* key;
    char* value;
    char* tok_str;
    char* saveptr = NULL;
    MPI_Info info;

    /* skip building a datatype if we don't have anything to write */
    if(count > 0)
    {
        /* construct data type to describe everything we are writing */
        /* NOTE: there may be a bug in MPI-IO when using MPI_BOTTOM with an
         * hindexed data type.  We will instead use the first pointer as a base
         * and adjust the displacements relative to it.
         */
        buf = pointers[0];
        for(i=0; i<count; i++)
        {
            /* use this transform to be compiler safe */
            uintptr_t ptr  = (uintptr_t) pointers[i];
            uintptr_t base = (uintptr_t) buf;
            displacements[i] = (MPI_Aint)(ptr - base);
        }
        DARSHAN_MPI_CALL(PMPI_Type_hindexed)(count, lengths, displacements,
            MPI_BYTE, &mtype);
        DARSHAN_MPI_CALL(PMPI_Type_commit)(&mtype); 
    }
    
    MPI_Info_create(&info);

    /* check environment variable to see if the default MPI file hints have
     * been overridden
     */
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
            free(tok_str);
        }
    }
    
    ret = DARSHAN_MPI_CALL(PMPI_File_open)(MPI_COMM_WORLD, logfile_name,
        MPI_MODE_CREATE | MPI_MODE_WRONLY | MPI_MODE_EXCL, MPI_INFO_NULL, &fh);
    MPI_Info_free(&info);
    if(ret != MPI_SUCCESS)
    {
        if(rank == 0)
        {
            int msg_len;
            char msg[MPI_MAX_ERROR_STRING] = {0};
            
            MPI_Error_string(ret, msg, &msg_len);
            fprintf(stderr, "darshan library warning: unable to open log file %s: %s\n", logfile_name, msg);
        }
        if(count > 0)
            DARSHAN_MPI_CALL(PMPI_Type_free)(&mtype);
        return(-1);
    }
  
    /* figure out where everyone is writing */
    if(count > 0)
        DARSHAN_MPI_CALL(PMPI_Type_size)(mtype, &my_total);
    else
        my_total = 0;
    my_total_long = my_total;
    DARSHAN_MPI_CALL(PMPI_Scan)(&my_total_long, &offset, 1,
        MPI_LONG, MPI_SUM, MPI_COMM_WORLD); 
    /* scan is inclusive; subtract local size back out */
    offset -= my_total_long;

    if(count > 0)
    {
        /* collectively write out file records from all processes */
        ret = DARSHAN_MPI_CALL(PMPI_File_write_at_all)(fh, offset, buf, 
            1, mtype, &status);
        if(ret != MPI_SUCCESS)
        {
            failed_write = 1;
        }
    }
    else
    {
        /* nothing to write, but we need to participate in the 
         * collectivee anyway 
         */
        ret = DARSHAN_MPI_CALL(PMPI_File_write_at_all)(fh, offset, NULL, 
            0, MPI_BYTE, &status);
        if(ret != MPI_SUCCESS)
        {
            failed_write = 1;
        }
    }

    DARSHAN_MPI_CALL(PMPI_File_close)(&fh);

    if(count > 0)
        DARSHAN_MPI_CALL(PMPI_Type_free)(&mtype);

    if(failed_write)
    {
        return(-1);
    }
    return(0);
}

/* cp_log_compress()
 *
 * gzip memory buffers to write to log file.  Modifies the count, lengths,
 * and pointers to reference new buffer (or buffers)
 *
 * returns 0 on success, -1 on error
 */
/* TODO: pick settings for compression (memory, level, etc.) */
static int cp_log_compress(struct darshan_job_runtime* final_job,
    int rank, int* inout_count, int* lengths, void** pointers)
{
    int ret = 0;
    z_stream tmp_stream;
    int total_target = 0;
    int i;
    int no_data_flag = 1;

    /* do we actually have anything to write? */
    for(i=0; i<*inout_count; i++)
    {
        if(lengths[i])
        {
            no_data_flag = 0;
            break;
        }
    }

    if(no_data_flag)
    {
        /* nothing to compress */
        *inout_count = 0;
        return(0);
    }

    memset(&tmp_stream, 0, sizeof(tmp_stream));
    tmp_stream.zalloc = Z_NULL;
    tmp_stream.zfree = Z_NULL;
    tmp_stream.opaque = Z_NULL;

    ret = deflateInit2(&tmp_stream, Z_DEFAULT_COMPRESSION, Z_DEFLATED,
        31, 8, Z_DEFAULT_STRATEGY);
    if(ret != Z_OK)
    {
        return(-1);
    }

    tmp_stream.next_out = (void*)final_job->comp_buf;
    tmp_stream.avail_out = CP_COMP_BUF_SIZE;

    /* loop through all pointers to be compressed */
    for(i=0; i<*inout_count; i++)
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

    /* substitute our new buffer */
    pointers[0] = final_job->comp_buf;
    lengths[0] = tmp_stream.total_out;
    *inout_count = 1;

    return(0);
}

static struct darshan_file_runtime* walker_file = NULL;
static int walker_validx;
static int walker_cntidx;

static void cp_access_walker(const void* nodep, const VISIT which, const int depth)
{
    struct cp_access_counter* counter;

    switch (which)
    {
        case postorder:
        case leaf:
            counter = *(struct cp_access_counter**)nodep;
#if 0
            printf("   type %d size: %" PRId64 ", freq: %d\n", walker_validx, counter->size, counter->freq);
#endif
            CP_COUNTER_INC(walker_file, counter->size, counter->freq, 1, walker_validx, walker_cntidx);
        default:
            break;
    }

    return;
};

/* darshan_walk_file_accesses()
 *
 * goes through runtime collections of accesses sizes and chooses the 4 most
 * common for logging
 */
void darshan_walk_file_accesses(struct darshan_job_runtime* final_job)
{
    int i;

    for(i=0; i<final_job->file_count; i++)
    {
        #if 0
        printf("file: %d\n", i);
        #endif
        
        /* walk trees for both access sizes and stride sizes to pick 4 most
         * common of each
         */

        /* NOTE: setting global variables here for cp_access_walker() */
        walker_file = &final_job->file_runtime_array[i];
        walker_validx = CP_ACCESS1_ACCESS;
        walker_cntidx = CP_ACCESS1_COUNT;
        twalk(walker_file->access_root,
            cp_access_walker);
        tdestroy(walker_file->access_root, free);

        walker_validx = CP_STRIDE1_STRIDE;
        walker_cntidx = CP_STRIDE1_COUNT;
        twalk(walker_file->stride_root,
            cp_access_walker);
        tdestroy(walker_file->stride_root, free);
    }

    return;
}

static int file_compare(const void* a, const void* b)
{
    const struct darshan_file* f_a = a;
    const struct darshan_file* f_b = b;
    
    if(f_a->rank < f_b->rank)
        return 1;
    if(f_a->rank > f_b->rank)
        return -1;
    
    return 0;
}

static path_len_cmp(const void* a, const void* b)
{
    const char** str_a  = (const char**)a;
    const char** str_b  = (const char**)b;

    if(strlen(*str_a) > strlen(*str_b))
        return(-1);
    else if(strlen(*str_a) < strlen(*str_b))
        return(1);
    else
        return(0);
}

/* darshan_get_exe_and_mounts()
 *
 * collects command line and list of mounted file systems into a string that
 * will be stored with the job header
 */
char* darshan_get_exe_and_mounts(struct darshan_job_runtime* final_job)
{
    FILE* tab;
    struct mntent *entry;
    char* exclude;
    int tmp_index = 0;
    int ret;
    struct stat statbuf;
    struct statfs statfsbuf;
    int skip = 0;
    char* trailing_data;
    int space_left;
    char tmp_mnt[256];
    int mnt_array_index = 0;
    int i;

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

    space_left = CP_EXE_LEN + 1;
    trailing_data = malloc(space_left);
    if(!trailing_data)
    {
        return(NULL);
    }
    memset(trailing_data, 0, space_left);

    /* length of exe has already been safety checked in darshan-posix.c */
    strcat(trailing_data, final_job->exe);
    space_left = CP_EXE_LEN - strlen(trailing_data);

    tab = setmntent("/etc/mtab", "r");
    if(!tab)
        return(trailing_data);

    /* loop through list of mounted file systems */
    while((entry = getmntent(tab)) != NULL)
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

        if(skip)
            continue;

        ret = stat(entry->mnt_dir, &statbuf);
        if(ret == 0)
        {
            int64_t tmp_st_dev = statbuf.st_dev;

            mnt_path_array[mnt_array_index] = strdup(entry->mnt_dir);
            mnt_array_index++;
            
            /* store mount information for use in header of darshan log */
            ret = snprintf(tmp_mnt, 256, "\n%" PRId64 "\t%s\t%s", tmp_st_dev, 
                    entry->mnt_type, entry->mnt_dir);
            if(ret >= 256)
            {
                /* didn't fit; skip this entry */
                continue;
            }
            if(strlen(tmp_mnt) <= space_left)
            {
                strcat(trailing_data, tmp_mnt);
                space_left -= strlen(tmp_mnt);
            }
#if 0
            printf("dev: %" PRId64 ", mnt_pt: %s, type: %s\n",  
                tmp_st_dev, entry->mnt_dir, entry->mnt_type);
#endif
        }
    }

    /* Sort mount points in order of longest path to shortest path.  This is
     * necessary so that if we try to match file paths to mount points later
     * we don't match on "/" every time.
     */
    qsort(mnt_path_array, mnt_array_index, sizeof(char*), path_len_cmp);

    /* collect stat and hash information for each mnt */
    for(i=0; (i<mnt_array_index && i<CP_MAX_MNTS); i++)
    {
        ret = stat(mnt_path_array[i], &statbuf);
        if(ret == 0)
        {
            int64_t tmp_st_dev = statbuf.st_dev;

            /* record hash of mount point and id */
            mnt_hash_array[i] =
                darshan_hash((void*)mnt_path_array[i], 
                strlen(mnt_path_array[i]), 0);
            mnt_id_array[i] = tmp_st_dev;
        }
/* NOTE: we now try to detect default block size for each file system, but we
 * skip this step on Lustre, which appears to always report a 4K block size
 * through statfs().
 */
#ifndef LL_SUPER_MAGIC
#define LL_SUPER_MAGIC 0x0BD00BD0
#endif
        ret = statfs(mnt_path_array[i], &statfsbuf);
        if(ret == 0 && statfsbuf.f_type != LL_SUPER_MAGIC)
            mnt_block_size_array[i] = statfsbuf.f_bsize;
        else
            mnt_block_size_array[i] = -1;

    }
    return(trailing_data);
}

/*
 * Computes population variance of bytes moved and total time
 * for each rank on a shared file.
 */
static int darshan_file_variance(
    struct darshan_file *infile_array,
    struct darshan_file *outfile_array,
    int count, int rank)
{
    MPI_Op pw_var_op = MPI_OP_NULL;
    MPI_Datatype var_dt = MPI_BYTE;
    int ret;
    int i;
    struct variance_dt* var_array = NULL;
    struct variance_dt* varres_array = NULL;

    ret = DARSHAN_MPI_CALL(PMPI_Op_create)(pairwise_variance_reduce, 1,
        &pw_var_op);
    if (ret != MPI_SUCCESS)
    {
        goto error_handler;
    }

    ret = DARSHAN_MPI_CALL(PMPI_Type_contiguous)(sizeof(struct variance_dt),
        MPI_BYTE, &var_dt);
    if (ret != MPI_SUCCESS)
    {
        goto error_handler;
    }

    ret = DARSHAN_MPI_CALL(PMPI_Type_commit)(&var_dt);
    if (ret != MPI_SUCCESS)
    {
        goto error_handler;
    }

    var_array = malloc(count*sizeof(struct variance_dt));
    if(!var_array)
    {
        goto error_handler;
    }       

    if (rank == 0)
    {
        varres_array = malloc(count*sizeof(struct variance_dt));
        if(!varres_array)
        {
            goto error_handler;
        }
    }
 
    /*
     * total time
     */

    for(i=0; i<count; i++)
    {
        var_array[i].n = 1;
        var_array[i].S = 0;
        var_array[i].T = infile_array[i].fcounters[CP_F_POSIX_META_TIME] +
                         infile_array[i].fcounters[CP_F_POSIX_READ_TIME] +
                         infile_array[i].fcounters[CP_F_POSIX_WRITE_TIME];
    } 

    ret = DARSHAN_MPI_CALL(PMPI_Reduce)(
             var_array, varres_array, count, var_dt, pw_var_op,
             0, MPI_COMM_WORLD);
    if(ret != MPI_SUCCESS)
    {
        goto error_handler;
    }

    if (rank == 0)
    {
        for(i=0; i<count; i++)
        {
            outfile_array[i].fcounters[CP_F_VARIANCE_RANK_TIME] =
                (varres_array[i].S / varres_array[i].n);
        }
    }

    /*
     * total bytes
     */
    for(i=0; i<count; i++)
    {
        var_array[i].n = 1;
        var_array[i].S = 0;
        var_array[i].T = (double)
                         infile_array[i].counters[CP_BYTES_READ] +
                         infile_array[i].counters[CP_BYTES_WRITTEN];
    } 

    ret = DARSHAN_MPI_CALL(PMPI_Reduce)(
             var_array, varres_array, count, var_dt, pw_var_op,
             0, MPI_COMM_WORLD);
    if(ret != MPI_SUCCESS)
    {
        goto error_handler;
    }

    if (rank == 0)
    {
        for(i=0; i<count; i++)
        {
            outfile_array[i].fcounters[CP_F_VARIANCE_RANK_BYTES] =
                (varres_array[i].S / varres_array[i].n);
        }
    }

    ret = 0;

error_handler:
    if (var_dt != MPI_BYTE) DARSHAN_MPI_CALL(PMPI_Type_free)(&var_dt);
    if (pw_var_op != MPI_OP_NULL) DARSHAN_MPI_CALL(PMPI_Op_free)(&pw_var_op);
    if (var_array) free(var_array);
    if (varres_array) free(varres_array);

    return ret;
}

static void pairwise_variance_reduce (
    void *invec, void *inoutvec, int *len, MPI_Datatype *dt)
{
    int i;
    struct variance_dt *X = invec;
    struct variance_dt *Y = inoutvec;
    struct variance_dt  Z;

    for (i=0; i<*len; i++,X++,Y++)
    {
        Z.n = X->n + Y->n;
        Z.T = X->T + Y->T;
        Z.S = X->S + Y->S + (X->n/(Y->n*Z.n)) *
           ((Y->n/X->n)*X->T - Y->T) * ((Y->n/X->n)*X->T - Y->T);

        *Y = Z;
    }

    return;
}

/* record any hints used to write the darshan log in the log header */
static void cp_log_record_hints(struct darshan_job_runtime* final_job, int rank)
{
    char* hints;
    char* header_hints;
    int meta_remain = 0;

    /* only need to do this on first process */
    if(rank > 0)
        return;

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
        strlen(final_job->log_job.metadata) - 1;
    if(meta_remain >= (3 + strlen(header_hints)))
    {
        /* We have room to store the hints in the metadata portion of
         * the job header.  We just prepend an h= to the hints list.  The
         * metadata parser will ignore = characters that appear in the value
         * portion of the metadata key/value pair.
         */
        strcat(final_job->log_job.metadata, "h=");
        strcat(final_job->log_job.metadata, header_hints);
    }
    free(header_hints);

    return;
}

static void debug_mounts(const char* mtab_file, const char* out_file)
{
    FILE* tab;
    struct mntent *entry;
    int ret;
    struct stat statbuf;
    FILE* out;

    out = fopen(out_file, "w");
    if(!out)
    {
        perror("darshan: fopen");
        return;
    }

    tab = setmntent(mtab_file, "r");
    if(!tab)
    {
        perror("darshan: setmnt");
        return;
    }

    while((entry = getmntent(tab)) != NULL)
    {
        ret = stat(entry->mnt_dir, &statbuf);
        if(ret == 0)
        {
            int64_t tmp_st_dev = statbuf.st_dev;

            fprintf(out, "%" PRId64 "\t%s\t%s\n", tmp_st_dev, 
                entry->mnt_type, entry->mnt_dir);
        }
        else
        {
            perror("darshan: stat");
        }
    }
    return;
}

static struct darshan_file_runtime* darshan_file_by_name_setfh(const char* name, MPI_File fh)
{
    struct darshan_file_runtime* tmp_file;

    tmp_file = darshan_file_by_name_sethandle(name, &fh, sizeof(fh), DARSHAN_FH);
    return(tmp_file);
}

static void darshan_file_close_fh(MPI_File fh)
{
    darshan_file_closehandle(&fh, sizeof(fh), DARSHAN_FH);
    return;
}

static struct darshan_file_runtime* darshan_file_by_fh(MPI_File fh)
{
    struct darshan_file_runtime* tmp_file;

    tmp_file = darshan_file_by_handle(&fh, sizeof(fh), DARSHAN_FH);
    
    return(tmp_file);
}

/* find the device id and block size for the specified file, based on 
 * data from the mount entries.
 */
void darshan_mnt_id_from_path(const char* path, int64_t* device_id, int64_t* block_size)
{
    int i;
    *device_id = -1;
    *block_size = -1;

    for(i=0; (i<CP_MAX_MNTS && mnt_hash_array[i] != 0); i++)
    {
        if(!(strncmp(mnt_path_array[i], path, strlen(mnt_path_array[i]))))
        {
            *device_id = mnt_id_array[i];
            *block_size = mnt_block_size_array[i];
            return;
        }
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
