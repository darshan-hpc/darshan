/*
 *  (C) 2009 by Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
 */

#define _XOPEN_SOURCE 500
#define _GNU_SOURCE /* for tdestroy() */

#include <stdio.h>
#include <mntent.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <limits.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <zlib.h>
#include <assert.h>
#include <search.h>

#include "mpi.h"
#include "darshan.h"
#include "darshan-config.h"

extern char* __progname;

/* maximum number of memory segments each process will write to the log */
#define CP_MAX_MEM_SEGMENTS 8

#define CP_DATATYPE_INC(__file, __datatype) do {\
    int num_integers, num_addresses, num_datatypes, combiner, ret; \
    ret = MPI_Type_get_envelope(__datatype, &num_integers, &num_addresses, \
        &num_datatypes, &combiner); \
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
    MPI_Type_size(__datatype, &size);  \
    size = size * __count; \
    MPI_Type_extent(__datatype, &extent); \
    CP_BUCKET_INC(file, CP_SIZE_WRITE_AGG_0_100, size); \
    CP_BUCKET_INC(file, CP_EXTENT_WRITE_0_100, extent); \
    CP_INC(file, __counter, 1); \
    CP_DATATYPE_INC(file, __datatype); \
    CP_F_INC(file, CP_F_MPI_WRITE_TIME, (__tm2-__tm1)); \
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
    MPI_Type_size(__datatype, &size);  \
    size = size * __count; \
    MPI_Type_extent(__datatype, &extent); \
    CP_BUCKET_INC(file, CP_SIZE_READ_AGG_0_100, size); \
    CP_BUCKET_INC(file, CP_EXTENT_READ_0_100, extent); \
    CP_INC(file, __counter, 1); \
    CP_DATATYPE_INC(file, __datatype); \
    CP_F_INC(file, CP_F_MPI_READ_TIME, (__tm2-__tm1)); \
    if(CP_F_VALUE(file, CP_F_READ_START_TIMESTAMP) == 0) \
        CP_F_SET(file, CP_F_READ_START_TIMESTAMP, __tm1); \
    CP_F_SET(file, CP_F_READ_END_TIMESTAMP, __tm2); \
} while(0)

static struct darshan_file_runtime* darshan_file_by_fh(MPI_File fh);
static void cp_log_construct_indices(struct darshan_job_runtime* final_job,
    int rank, int* inout_count, int* lengths, void** pointers, char*
    trailing_data);
static int cp_log_write(struct darshan_job_runtime* final_job, int rank, 
    char* logfile_name, int count, int* lengths, void** pointers, double start_log_time);
static int cp_log_reduction(struct darshan_job_runtime* final_job, int rank, 
    char* logfile_name, MPI_Offset* next_offset);
static void darshan_file_reduce(void* infile_v, 
    void* inoutfile_v, int *len, 
    MPI_Datatype *datatype);
static int cp_log_compress(struct darshan_job_runtime* final_job,
    int rank, int* inout_count, int* lengths, void** pointers);
static int file_compare(const void* a, const void* b);
static void darshan_mpi_initialize(int *argc, char ***argv);
static char*  darshan_get_exe_and_mounts(struct darshan_job_runtime* final_job);

#define CP_MAX_MNTS 32
uint64_t mnt_hash_array[CP_MAX_MNTS] = {0};
int64_t mnt_id_array[CP_MAX_MNTS] = {0};
uint64_t mnt_hash_array_root[CP_MAX_MNTS] = {0};
int64_t mnt_id_array_root[CP_MAX_MNTS] = {0};
struct
{
    int64_t mnt_id_local;
    int64_t mnt_id_root;
} mnt_mapping[CP_MAX_MNTS];

int MPI_Init(int *argc, char ***argv)
{
    int ret;

    ret = PMPI_Init(argc, argv);
    if(ret != MPI_SUCCESS)
    {
        return(ret);
    }

    darshan_mpi_initialize(argc, argv);

    return(ret);
}

int MPI_Init_thread (int *argc, char ***argv, int required, int *provided)
{
    int ret;

    ret = PMPI_Init_thread(argc, argv, required, provided);
    if (ret != MPI_SUCCESS)
    {
        return(ret);
    }

    darshan_mpi_initialize(argc, argv);

    return(ret);
}

static void darshan_mpi_initialize(int *argc, char ***argv)
{
    int nprocs;
    int rank;

    MPI_Comm_size(MPI_COMM_WORLD, &nprocs);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    CP_LOCK();
    if(argc && argv)
    {
        darshan_initialize(*argc, *argv, nprocs, rank);
    }
    else
    {
        /* we don't see argc and argv here in fortran */
        darshan_initialize(0, NULL, nprocs, rank);
    }

    CP_UNLOCK();

    return;
}

void darshan_shutdown(int timing_flag)
{
    int rank;
    char* logfile_name;
    struct darshan_job_runtime* final_job;
    double start_log_time = 0;
    int flags;
    int all_ret = 0;
    int local_ret = 0;
    MPI_Offset next_offset = 0;
    char* jobid_str;
    int jobid;
    int index_count = 0;
    int lengths[CP_MAX_MEM_SEGMENTS];
    void* pointers[CP_MAX_MEM_SEGMENTS];
    int ret;
    double red1=0, red2=0, gz1=0, gz2=0, write1=0, write2=0, tm_end=0;
    int nprocs;
    char* trailing_data = NULL;
    int i, j;
    int map_index = 0;
    time_t start_time_tmp = 0;

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
    flags = final_job->flags;
    CP_UNLOCK();

    start_log_time = MPI_Wtime();

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

    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    /* collect information about command line and 
     * mounted file systems 
     */
    trailing_data = darshan_get_exe_and_mounts(final_job);

    /* broadcast mount point information from root */
    if(rank == 0)
    {
        memcpy(mnt_hash_array_root, mnt_hash_array,
            CP_MAX_MNTS*sizeof(uint64_t));
        memcpy(mnt_id_array_root, mnt_id_array,
            CP_MAX_MNTS*sizeof(int64_t));
    }
    MPI_Bcast(mnt_id_array_root, CP_MAX_MNTS*sizeof(int64_t), MPI_BYTE, 0,
        MPI_COMM_WORLD);
    MPI_Bcast(mnt_hash_array_root, CP_MAX_MNTS*sizeof(uint64_t), MPI_BYTE, 0,
        MPI_COMM_WORLD);
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

        /* find a job id */
        jobid_str = getenv(CP_JOBID);
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

        /* note: getpwuid() causes link errors for static binaries */
        cuserid(cuser);

        ret = snprintf(logfile_name, PATH_MAX, 
            "%s/%d/%d/%d/%s_%s_id%d_%d-%d-%d.darshan_partial",
            __CP_LOG_PATH, (my_tm->tm_year+1900), 
            (my_tm->tm_mon+1), my_tm->tm_mday, 
            cuser, __progname, jobid,
            (my_tm->tm_mon+1), 
            my_tm->tm_mday, 
            (my_tm->tm_hour*60*60 + my_tm->tm_min*60 + my_tm->tm_sec));
        if(ret == (PATH_MAX-1))
        {
            /* file name was too big; squish it down */
            snprintf(logfile_name, PATH_MAX,
                "%s/id%d.darshan_partial",
                __CP_LOG_PATH, jobid);
        }

        /* add jobid */
        final_job->log_job.jobid = (int64_t)jobid;
    }

    /* broadcast log file name */
    MPI_Bcast(logfile_name, PATH_MAX, MPI_CHAR, 0, MPI_COMM_WORLD);

    final_job->log_job.end_time = time(NULL);

    /* reduce records for shared files */
    if(timing_flag)
        red1 = MPI_Wtime();
    local_ret = cp_log_reduction(final_job, rank, logfile_name, 
        &next_offset);
    if(timing_flag)
        red2 = MPI_Wtime();
    MPI_Allreduce(&local_ret, &all_ret, 1, MPI_INT, MPI_LOR, 
        MPI_COMM_WORLD);

    if(all_ret == 0)
    {
        /* collect data to write from local process */
        cp_log_construct_indices(final_job, rank, &index_count, lengths, 
            pointers, trailing_data);
    }

    if(all_ret == 0)
    {
        /* compress data */
        if(timing_flag)
            gz1 = MPI_Wtime();
        local_ret = cp_log_compress(final_job, rank, &index_count, 
            lengths, pointers);
        if(timing_flag)
            gz2 = MPI_Wtime();
        MPI_Allreduce(&local_ret, &all_ret, 1, MPI_INT, MPI_LOR, 
            MPI_COMM_WORLD);
    }

    if(all_ret == 0)
    {
        /* actually write out log file */
        if(timing_flag)
            write1 = MPI_Wtime();
        local_ret = cp_log_write(final_job, rank, logfile_name, 
            index_count, lengths, pointers, start_log_time);
        if(timing_flag)
            write2 = MPI_Wtime();
        MPI_Allreduce(&local_ret, &all_ret, 1, MPI_INT, MPI_LOR, 
            MPI_COMM_WORLD);
    }

    /* if any process failed to write log, then delete the whole file so we
     * don't leave corrupted results
     */
    if(all_ret != 0 && rank == 0)
    {
        unlink(logfile_name);
    }

    if(trailing_data)
        free(trailing_data);
    free(logfile_name);
    darshan_finalize(final_job);
    
    if(timing_flag)
    {
        double red_tm, red_slowest;
        double gz_tm, gz_slowest;
        double write_tm, write_slowest;
        double all_tm, all_slowest;
        
        tm_end = MPI_Wtime();

        red_tm = red2-red1;
        gz_tm = gz2-gz1;
        write_tm = write2-write1;
        all_tm = tm_end-start_log_time;

        MPI_Allreduce(&red_tm, &red_slowest, 1,
            MPI_DOUBLE, MPI_MAX, MPI_COMM_WORLD);
        MPI_Allreduce(&gz_tm, &gz_slowest, 1,
            MPI_DOUBLE, MPI_MAX, MPI_COMM_WORLD);
        MPI_Allreduce(&write_tm, &write_slowest, 1,
            MPI_DOUBLE, MPI_MAX, MPI_COMM_WORLD);
        MPI_Allreduce(&all_tm, &all_slowest, 1,
            MPI_DOUBLE, MPI_MAX, MPI_COMM_WORLD);

        if(rank == 0)
        {
            MPI_Comm_size(MPI_COMM_WORLD, &nprocs);
            printf("#<op>\t<nprocs>\t<time>\n");
            printf("reduce\t%d\t%f\n", nprocs, red_slowest);
            printf("gzip\t%d\t%f\n", nprocs, gz_slowest);
            printf("write\t%d\t%f\n", nprocs, write_slowest);
            printf("all\t%d\t%f\n", nprocs, all_slowest);
        }
    }

    return;
}

int MPI_Finalize(void)
{
    int ret;

    darshan_shutdown(0);

    ret = PMPI_Finalize();
    return(ret);
}

int MPI_File_open(MPI_Comm comm, char *filename, int amode, MPI_Info info, MPI_File *fh) 
{
    int ret;
    struct darshan_file_runtime* file;
    char* tmp;
    int comm_size;
    int hash_index;
    uint64_t tmp_hash;
    double tm1, tm2;

    tm1 = darshan_wtime();
    ret = PMPI_File_open(comm, filename, amode, info, fh);
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

        file = darshan_file_by_name(filename);
        /* TODO: handle the case of multiple concurrent opens */
        if(file && (file->fh == MPI_FILE_NULL))
        {
            file->fh = *fh;
            CP_SET(file, CP_MODE, amode);
            CP_F_INC(file, CP_F_MPI_META_TIME, (tm2-tm1));
            if(CP_F_VALUE(file, CP_F_OPEN_TIMESTAMP) == 0)
                CP_F_SET(file, CP_F_OPEN_TIMESTAMP, MPI_Wtime());
            MPI_Comm_size(comm, &comm_size);
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
            tmp_hash = hash((void*)fh, sizeof(*fh), 0);
            hash_index = tmp_hash & CP_HASH_MASK;
            file->fh_prev = NULL;
            file->fh_next = darshan_global_job->fh_table[hash_index];
            if(file->fh_next)
                file->fh_next->fh_prev = file;
            darshan_global_job->fh_table[hash_index] = file;
        }
        CP_UNLOCK();
    }

    return(ret);
}

int MPI_File_close(MPI_File *fh) 
{
    int hash_index;
    uint64_t tmp_hash;
    struct darshan_file_runtime* file;
    MPI_File tmp_fh = *fh;
    double tm1, tm2;
    int ret;
    
    tm1 = darshan_wtime();
    ret = PMPI_File_close(fh);
    tm2 = darshan_wtime();

    CP_LOCK();
    file = darshan_file_by_fh(tmp_fh);
    if(file)
    {
        file->fh = MPI_FILE_NULL;
        CP_F_SET(file, CP_F_CLOSE_TIMESTAMP, MPI_Wtime());
        CP_F_INC(file, CP_F_MPI_META_TIME, (tm2-tm1));
        if(file->fh_prev == NULL)
        {
            /* head of fh hash table list */
            tmp_hash = hash((void*)&tmp_fh, sizeof(tmp_fh), 0);
            hash_index = tmp_hash & CP_HASH_MASK;
            darshan_global_job->fh_table[hash_index] = file->fh_next;
            if(file->fh_next)
                file->fh_next->fh_prev = NULL;
        }
        else
        {
            if(file->fh_prev)
                file->fh_prev->fh_next = file->fh_next;
            if(file->fh_next)
                file->fh_next->fh_prev = file->fh_prev;
        }
        file->fh_prev = NULL;
        file->fh_next = NULL;
        darshan_global_job->darshan_mru_file = file; /* in case we open it again, or hit posix calls */
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
    ret = PMPI_File_sync(fh);
    tm2 = darshan_wtime();
    if(ret == MPI_SUCCESS)
    {
        CP_LOCK();
        file = darshan_file_by_fh(fh);
        if(file)
        {
            CP_F_INC(file, CP_F_MPI_WRITE_TIME, (tm2-tm1));
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
    ret = PMPI_File_set_view(fh, disp, etype, filetype, datarep, info);
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
                CP_F_INC(file, CP_F_MPI_META_TIME, (tm2-tm1));
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
    ret = PMPI_File_read(fh, buf, count, datatype, status);
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
    ret = PMPI_File_read_at(fh, offset, buf, count, datatype, status);
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
    ret = PMPI_File_read_at_all(fh, offset, buf, count, datatype, status);
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
    ret = PMPI_File_read_all(fh, buf, count, datatype, status);
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
    ret = PMPI_File_read_shared(fh, buf, count, datatype, status);
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
    ret = PMPI_File_read_ordered(fh, buf, count, datatype, status);
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
    ret = PMPI_File_read_at_all_begin(fh, offset, buf, count, datatype);
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
    ret = PMPI_File_read_all_begin(fh, buf, count, datatype);
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
    ret = PMPI_File_read_ordered_begin(fh, buf, count, datatype);
    tm2 = darshan_wtime();
    CP_LOCK();
    CP_RECORD_MPI_READ(ret, fh, count, datatype, CP_SPLIT_READS, tm1, tm2);
    CP_UNLOCK();
    return(ret);
}

int MPI_File_iread_at(MPI_File fh, MPI_Offset offset, void * buf,
    int count, MPI_Datatype datatype, MPIO_Request *request)
{
    int ret;
    double tm1, tm2;

    tm1 = darshan_wtime();
    ret = PMPI_File_iread_at(fh, offset, buf, count, datatype, request);
    tm2 = darshan_wtime();
    CP_LOCK();
    CP_RECORD_MPI_READ(ret, fh, count, datatype, CP_NB_READS, tm1, tm2);
    CP_UNLOCK();
    return(ret);
}

int MPI_File_iread(MPI_File fh, void * buf, int count, MPI_Datatype datatype, MPIO_Request * request)
{
    int ret;
    double tm1, tm2;

    tm1 = darshan_wtime();
    ret = PMPI_File_iread(fh, buf, count, datatype, request);
    tm2 = darshan_wtime();
    CP_LOCK();
    CP_RECORD_MPI_READ(ret, fh, count, datatype, CP_NB_READS, tm1, tm2);
    CP_UNLOCK();
    return(ret);
}

int MPI_File_iread_shared(MPI_File fh, void * buf, int count,
    MPI_Datatype datatype, MPIO_Request * request)
{
    int ret;
    double tm1, tm2;

    tm1 = darshan_wtime();
    ret = PMPI_File_iread_shared(fh, buf, count, datatype, request);
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
    ret = PMPI_File_write(fh, buf, count, datatype, status);
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
    ret = PMPI_File_write_at(fh, offset, buf, count, datatype, status);
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
    ret = PMPI_File_write_at_all(fh, offset, buf, count, datatype, status);
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
    ret = PMPI_File_write_all(fh, buf, count, datatype, status);
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
    ret = PMPI_File_write_shared(fh, buf, count, datatype, status);
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
    ret = PMPI_File_write_ordered(fh, buf, count, datatype, status);
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
    ret = PMPI_File_write_at_all_begin(fh, offset, buf, count, datatype);
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
    ret = PMPI_File_write_all_begin(fh, buf, count, datatype);
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
    ret = PMPI_File_write_ordered_begin(fh, buf, count, datatype);
    tm2 = darshan_wtime();
    CP_LOCK();
    CP_RECORD_MPI_WRITE(ret, fh, count, datatype, CP_SPLIT_WRITES, tm1, tm2);
    CP_UNLOCK();
    return(ret);
}

int MPI_File_iwrite_at(MPI_File fh, MPI_Offset offset, void * buf,
    int count, MPI_Datatype datatype, MPIO_Request *request)
{
    int ret;
    double tm1, tm2;

    tm1 = darshan_wtime();
    ret = PMPI_File_iwrite_at(fh, offset, buf, count, datatype, request);
    tm2 = darshan_wtime();
    CP_LOCK();
    CP_RECORD_MPI_WRITE(ret, fh, count, datatype, CP_NB_WRITES, tm1, tm2);
    CP_UNLOCK();
    return(ret);
}

int MPI_File_iwrite(MPI_File fh, void * buf, int count, MPI_Datatype datatype, MPIO_Request * request)
{
    int ret;
    double tm1, tm2;

    tm1 = darshan_wtime();
    ret = PMPI_File_iwrite(fh, buf, count, datatype, request);
    tm2 = darshan_wtime();
    CP_LOCK();
    CP_RECORD_MPI_WRITE(ret, fh, count, datatype, CP_NB_WRITES, tm1, tm2);
    CP_UNLOCK();
    return(ret);
}

int MPI_File_iwrite_shared(MPI_File fh, void * buf, int count,
    MPI_Datatype datatype, MPIO_Request * request)
{
    int ret;
    double tm1, tm2;

    tm1 = darshan_wtime();
    ret = PMPI_File_iwrite_shared(fh, buf, count, datatype, request);
    tm2 = darshan_wtime();
    CP_LOCK();
    CP_RECORD_MPI_WRITE(ret, fh, count, datatype, CP_NB_WRITES, tm1, tm2);
    CP_UNLOCK();
    return(ret);
}

static struct darshan_file_runtime* darshan_file_by_fh(MPI_File fh)
{
    struct darshan_file_runtime* tmp_file;
    uint64_t tmp_hash = 0;
    int hash_index;

    if(!darshan_global_job)
        return(NULL);

    /* if we have already condensed the data, then just hand the first file
     * back
     */
    if(darshan_global_job->flags & CP_FLAG_CONDENSED)
    {
        return(&darshan_global_job->file_runtime_array[0]);
    }

    /* check most recently used */
    if(darshan_global_job->darshan_mru_file && darshan_global_job->darshan_mru_file->fh == fh)
    {
        return(darshan_global_job->darshan_mru_file);
    }

    tmp_hash = hash((void*)(&fh), sizeof(fh), 0);

    /* search hash table */
    hash_index = tmp_hash & CP_HASH_MASK;
    tmp_file = darshan_global_job->fh_table[hash_index];
    while(tmp_file)
    {
        if(tmp_file->fh == fh)
        {
            darshan_global_job->darshan_mru_file = tmp_file;
            return(tmp_file);
        }
        tmp_file = tmp_file->fh_next;
    }

    return(NULL);
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
    ret = MPI_Op_create(darshan_file_reduce, 1, &reduce_op); 
    if(ret != 0)
    {
        return(-1);
    }

    /* construct a datatype for a file record.  This is serving no purpose
     * except to make sure we can do a reduction on proper boundaries
     */
    MPI_Type_contiguous(sizeof(struct darshan_file), MPI_BYTE, &rtype); 
    MPI_Type_commit(&rtype); 

    /* gather list of files that root process has opened */
    if(rank == 0)
    {
        for(i=0; i<final_job->file_count; i++)
        {
            hash_array[i] = final_job->file_array[i].hash;
        }
    }

    /* broadcast list of files to all other processes */
    ret = MPI_Bcast(hash_array, (CP_MAX_FILES * sizeof(uint64_t)), 
        MPI_BYTE, 0, MPI_COMM_WORLD);
    if(ret != 0)
    {
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
    ret = MPI_Allreduce(mask_array, all_mask_array, CP_MAX_FILES, MPI_INT, 
        MPI_LAND, MPI_COMM_WORLD);
    if(ret != 0)
    {
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
                return(-1);
            }
        }

        /* sort the array of files descending by rank so that we get all of the 
         * shared files (marked by rank -1) in a contiguous portion at end 
         * of the array
         */
        qsort(final_job->file_array, final_job->file_count, 
            sizeof(struct darshan_file), file_compare);

        ret = MPI_Reduce(
            &final_job->file_array[final_job->file_count-shared_count], 
            tmp_array, shared_count, rtype, reduce_op, 0, MPI_COMM_WORLD);
        if(ret != 0)
        {
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

        /* pick one */
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

        /* pick one */
        tmp_file.counters[CP_MEM_ALIGNMENT] = infile->counters[CP_MEM_ALIGNMENT];
        /* sum */
        for(j=CP_FILE_NOT_ALIGNED; j<=CP_FILE_NOT_ALIGNED; j++)
        {
            tmp_file.counters[j] = infile->counters[j] + 
                inoutfile->counters[j];
        }

        /* pick one */
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

        /* min */
        for(j=CP_F_OPEN_TIMESTAMP; j<=CP_F_WRITE_START_TIMESTAMP; j++)
        {
            if(infile->fcounters[j] > inoutfile->fcounters[j])
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
        for(j=CP_F_POSIX_READ_TIME; j<=CP_F_NC_WRITE_TIME; j++)
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

        /* pick one device id and file size */
        tmp_file.counters[CP_DEVICE] = infile->counters[CP_DEVICE];
        tmp_file.counters[CP_SIZE_AT_OPEN] = infile->counters[CP_SIZE_AT_OPEN];

        /* pick one name suffix */
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

    /* construct data type to describe everything we are writing */
    /* NOTE: there may be a bug in MPI-IO when using MPI_BOTTOM with an
     * hindexed data type.  We will instead use the first pointer as a base
     * and adjust the displacements relative to it.
     */
    buf = pointers[0];
    for(i=0; i<count; i++)
    {
        displacements[i] = (MPI_Aint)(pointers[i] - buf);
    }
    MPI_Type_hindexed(count, lengths, displacements, MPI_BYTE, &mtype);
    MPI_Type_commit(&mtype); 

    ret = PMPI_File_open(MPI_COMM_WORLD, logfile_name, MPI_MODE_CREATE |
        MPI_MODE_WRONLY | MPI_MODE_EXCL, MPI_INFO_NULL, &fh);
    if(ret != MPI_SUCCESS)
    {
        /* TODO: keep this print or not? */
        fprintf(stderr, "darshan library warning: unable to open log file %s\n", logfile_name);
        MPI_Type_free(&mtype);
        return(-1);
    }
   
    PMPI_File_set_size(fh, 0);

    /* figure out where everyone is writing */
    MPI_Type_size(mtype, &my_total);
    my_total_long = my_total;
    MPI_Scan(&my_total_long, &offset, 1, MPI_LONG, MPI_SUM, MPI_COMM_WORLD); 
    /* scan is inclusive; subtract local size back out */
    offset -= my_total_long;

    /* collectively write out file records from all processes */
    ret = PMPI_File_write_at_all(fh, offset, buf, 
        1, mtype, &status);
    if(ret != MPI_SUCCESS)
    {
        failed_write = 1;
    }

    PMPI_File_close(&fh);

    /* rename from *.darshan_partial to *-<logwritetime>.darshan.gz */
    if(rank == 0)
    {
        char* mod_index;
        double end_log_time;
        char* new_logfile_name;

        new_logfile_name = malloc(PATH_MAX);
        if(new_logfile_name)
        {
            new_logfile_name[0] = '\0';
            end_log_time = MPI_Wtime();
            strcat(new_logfile_name, logfile_name);
            mod_index = strstr(new_logfile_name, ".darshan_partial");
            sprintf(mod_index, "_%d.darshan.gz", (int)(end_log_time-start_log_time+1));
            rename(logfile_name, new_logfile_name);
            /* set permissions on log file */
            chmod(new_logfile_name, (S_IRUSR)); 
            free(new_logfile_name);
        }
    }

    MPI_Type_free(&mtype);

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
            //printf("   type %d size: %lld, freq: %d\n", walker_validx, counter->size, counter->freq);
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
        //printf("file: %d\n", i);
        
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

/* darshan_get_exe_and_mounts()
 *
 * collects command line and list of mounted file systems into a string that
 * will be stored with the job header
 */
static char* darshan_get_exe_and_mounts(struct darshan_job_runtime* final_job)
{
    FILE* tab;
    struct mntent *entry;
    char* exclude;
    int tmp_index = 0;
    int ret;
    struct stat statbuf;
    int skip = 0;
    char* trailing_data;
    int space_left;
    char tmp_mnt[256];
    int mnt_array_index = 0;

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
        NULL
    };

    /* extra byte for \0 already accounted for */
    space_left = CP_EXE_LEN;
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

    while((entry = getmntent(tab)) != NULL)
    {
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

            /* record hash of mount point and id */
            if(mnt_array_index < CP_MAX_MNTS)
            {
                mnt_hash_array[mnt_array_index] =
                    hash((void*)entry->mnt_dir, strlen(entry->mnt_dir), 0);
                mnt_id_array[mnt_array_index] = tmp_st_dev;
                mnt_array_index++;
            }

            ret = snprintf(tmp_mnt, 256, "\n%lld\t%s\t%s", lld(tmp_st_dev), 
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
            printf("dev: %lld, mnt_pt: %s, type: %s\n",  
                lld(tmp_st_dev), entry->mnt_dir, entry->mnt_type);
#endif
        }
    }
    return(trailing_data);
}

/*
 * Local variables:
 *  c-indent-level: 4
 *  c-basic-offset: 4
 * End:
 *
 * vim: ts=8 sts=4 sw=4 expandtab
 */
