/*
 *  (C) 2009 by Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
 */

#ifndef __DARSHAN_H
#define __DARSHAN_H

#include <unistd.h>
#include <sys/types.h>
#include <stdint.h>
#include <mpi.h>
#include "darshan-log-format.h"

/* Environment variable to override CP_JOBID */
#define CP_JOBID_OVERRIDE "DARSHAN_JOBID"

/* Environment variable to override __CP_LOG_PATH */
#define CP_LOG_PATH_OVERRIDE "DARSHAN_LOGPATH"

/* Environment variable to override __CP_LOG_PATH */
#define CP_LOG_HINTS_OVERRIDE "DARSHAN_LOGHINTS"

/* Environment variable to override __CP_MEM_ALIGNMENT */
#define CP_MEM_ALIGNMENT_OVERRIDE "DARSHAN_MEMALIGN"

/* maximum number of files per process we will track */
#define CP_MAX_FILES 1024

/* TODO: figure out how to pick good value here */
/* hash table size parameters */
#define CP_HASH_BITS 8
#define CP_HASH_SIZE (1 << CP_HASH_BITS)
#define CP_HASH_MASK (CP_HASH_SIZE - 1)

/* maximum number of access sizes and stride sizes that darshan will track
 * per file at runtime; at log time they will be reduced into the 4 most
 * frequently occurring ones 
 */
#define CP_MAX_ACCESS_COUNT_RUNTIME 32

/* flags to indicate properties of file records */
#define CP_FLAG_CONDENSED 1<<0
#define CP_FLAG_NOTIMING 1<<1

/* calculation of compression buffer size (defaults to 50% of the maximum
 * memory that Darshan is allowed to consume on a process) 
 */
#define CP_COMP_BUF_SIZE ((CP_MAX_FILES * sizeof(struct darshan_file))/2)

enum cp_io_type
{
    CP_READ = 1,
    CP_WRITE = 2,
};

struct cp_access_counter
{
    int64_t size;
    int freq;
};

struct darshan_io_tracker;

/* in memory structure to keep up with file level data */
struct darshan_file_runtime
{
    struct darshan_file* log_file;
    struct darshan_file_runtime* name_next;
    struct darshan_file_runtime* name_prev;
    void* access_root;
    int access_count;
    void* stride_root;
    int stride_count;
    int64_t last_byte_read;
    int64_t last_byte_written;
    int64_t offset;
    enum cp_io_type last_io_type;
    double last_posix_write_end;
    double last_mpi_write_end;
    double last_posix_read_end;
    double last_mpi_read_end;
    double last_posix_meta_end;
    double last_mpi_meta_end;
    struct darshan_aio_tracker* aio_list_head;
    struct darshan_aio_tracker* aio_list_tail;
};

/* handles used by various APIs to refer to files */
enum darshan_handle_type 
{
    DARSHAN_FD = 1,
    DARSHAN_FH,
    DARSHAN_NCID,
    DARSHAN_HID
};

#define DARSHAN_FILE_HANDLE_MAX (sizeof(MPI_File))
/* This struct is used to track a reference to a file by file 
 * descriptor, MPI file handle, ncdf id, etc.
 */
struct darshan_file_ref
{
    struct darshan_file_runtime* file;
    char handle[DARSHAN_FILE_HANDLE_MAX];
    int handle_sz;
    enum darshan_handle_type handle_type;
    struct darshan_file_ref* next;
    struct darshan_file_ref* prev;
};

/* in memory structure to keep up with job level data */
struct darshan_job_runtime
{
    struct darshan_job log_job;
    char exe[CP_EXE_LEN+1];
    struct darshan_file file_array[CP_MAX_FILES];
    struct darshan_file_runtime file_runtime_array[CP_MAX_FILES];
    char comp_buf[CP_COMP_BUF_SIZE];
    int flags;
    int file_count;
    struct darshan_file_runtime* name_table[CP_HASH_SIZE];
    struct darshan_file_ref* handle_table[CP_HASH_SIZE];
    double wtime_offset;
    char* trailing_data;
};

extern pthread_mutex_t cp_mutex;
#define CP_LOCK() pthread_mutex_lock(&cp_mutex)
#define CP_UNLOCK() pthread_mutex_unlock(&cp_mutex)

#define CP_SET(__file, __counter, __value) do {\
    (__file)->log_file->counters[__counter] = __value; \
} while(0)

#define CP_F_SET(__file, __counter, __value) do {\
    (__file)->log_file->fcounters[__counter] = __value; \
} while(0)

#define CP_INC(__file, __counter, __value) do {\
    (__file)->log_file->counters[__counter] += __value; \
} while(0)

#define CP_F_INC(__file, __counter, __value) do {\
    (__file)->log_file->fcounters[__counter] += __value; \
} while(0)

#define CP_F_INC_NO_OVERLAP(__file, __tm1, __tm2, __last, __counter) do { \
    if(__tm1 > __last) \
        CP_F_INC(__file, __counter, (__tm2-__tm1)); \
    else \
        CP_F_INC(__file, __counter, (__tm2 - __last)); \
    if(__tm2 > __last) \
        __last = __tm2; \
} while(0)

#define CP_VALUE(__file, __counter) \
    ((__file)->log_file->counters[__counter])

#define CP_F_VALUE(__file, __counter) \
    ((__file)->log_file->fcounters[__counter])

#define CP_MAX(__file, __counter, __value) do {\
    if((__file)->log_file->counters[__counter] < __value) \
    { \
        (__file)->log_file->counters[__counter] = __value; \
    } \
} while(0)

#define CP_COUNTER_INC(__file, __value, __count, __maxflag, __validx, __cntidx) do {\
    int i; \
    int set = 0; \
    int64_t min = CP_VALUE(__file, __cntidx); \
    int min_index = 0; \
    if(__value == 0) break; \
    for(i=0; i<4; i++) { \
        /* increment bucket if already exists */ \
        if(CP_VALUE(__file, __validx + i) == __value) { \
            CP_INC(__file, __cntidx + i, (__count)); \
            set = 1; \
            break; \
        } \
        /* otherwise find the least frequently used bucket */ \
        else if(CP_VALUE(__file, __cntidx + i) < min) { \
            min = CP_VALUE(__file, __cntidx + i); \
            min_index = i; \
        } \
    } \
    if((!set && !__maxflag) || (!set && __maxflag && (__count) > min)) { \
        CP_INC(__file, __cntidx+min_index, (__count)); \
        CP_SET(__file, __validx+min_index, __value); \
    } \
} while(0)

#define CP_BUCKET_INC(__file, __counter_base, __value) do {\
    if(__value < 101) \
        (__file)->log_file->counters[__counter_base] += 1; \
    else if(__value < 1025) \
        (__file)->log_file->counters[__counter_base+1] += 1; \
    else if(__value < 10241) \
        (__file)->log_file->counters[__counter_base+2] += 1; \
    else if(__value < 102401) \
        (__file)->log_file->counters[__counter_base+3] += 1; \
    else if(__value < 1048577) \
        (__file)->log_file->counters[__counter_base+4] += 1; \
    else if(__value < 4194305) \
        (__file)->log_file->counters[__counter_base+5] += 1; \
    else if(__value < 10485761) \
        (__file)->log_file->counters[__counter_base+6] += 1; \
    else if(__value < 104857601) \
        (__file)->log_file->counters[__counter_base+7] += 1; \
    else if(__value < 1073741825) \
        (__file)->log_file->counters[__counter_base+8] += 1; \
    else \
        (__file)->log_file->counters[__counter_base+9] += 1; \
} while(0)

enum cp_counter_type
{
    CP_COUNTER_ACCESS,
    CP_COUNTER_STRIDE
};

/* checking alignment according to this document:
 * http://publib.boulder.ibm.com/infocenter/compbgpl/v9v111/index.jsp?topic=/com.ibm.bg9111.doc/bgusing/data_alignment.htm
 */

extern struct darshan_job_runtime* darshan_global_job;

void darshan_initialize(int argc, char** argv, int nprocs, int rank);
void darshan_finalize(struct darshan_job_runtime* job);
void darshan_condense(void);
void darshan_shutdown(int timing_flag);
void darshan_walk_file_accesses(struct darshan_job_runtime* final_job);
double darshan_wtime(void);
void darshan_mnt_id_from_path(const char* path, int64_t* device_id, int64_t* block_size);
char* darshan_get_exe_and_mounts(struct darshan_job_runtime* final_job);
void darshan_mpi_initialize(int *argc, char ***argv);

uint32_t darshan_hashlittle(const void *key, size_t length, uint32_t initval);
uint64_t darshan_hash(const register unsigned char *k, register uint64_t length, register uint64_t level);

struct darshan_file_runtime* darshan_file_by_name(const char* name);

struct darshan_file_runtime* darshan_file_by_name_sethandle(
    const char* name,
    const void* handle,
    int handle_sz,
    enum darshan_handle_type handle_type);

struct darshan_file_runtime* darshan_file_by_handle(
    const void* handle,
    int handle_sz,
    enum darshan_handle_type handle_type);

void darshan_file_closehandle(
    const void* handle,
    int handle_sz,
    enum darshan_handle_type handle_type);

#endif /* __DARSHAN_H */
