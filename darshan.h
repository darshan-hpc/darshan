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

/* ratio of extra memory allocated to use as a compression buffer if needed */
/* example: 0.5 means that we allocate 50% extra memory to compress the logs
 * if all CP_MAX_FILES entries are used.
 */
#define CP_COMPRESSION_ALLOWANCE 0.5

/* flags to indicate properties of file records */
#define CP_FLAG_CONDENSED 1<<0
#define CP_FLAG_NOTIMING 1<<1

/* calculation of compression buffer size */
#define CP_COMP_BUF_SIZE ((int)((double)CP_MAX_FILES *\
    CP_COMPRESSION_ALLOWANCE * (double)sizeof(struct darshan_file)))

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

/* in memory structure to keep up with file level data */
struct darshan_file_runtime
{
    struct darshan_file* log_file;
    MPI_File fh;
    int fd;
    int ncid;
    int hid;
    struct darshan_file_runtime* name_next;
    struct darshan_file_runtime* name_prev;
    struct darshan_file_runtime* fd_next;
    struct darshan_file_runtime* fd_prev;
    struct darshan_file_runtime* ncid_next;
    struct darshan_file_runtime* ncid_prev;
    struct darshan_file_runtime* hid_next;
    struct darshan_file_runtime* hid_prev;
    struct darshan_file_runtime* fh_next;
    struct darshan_file_runtime* fh_prev;
    void* access_root;
    int access_count;
    void* stride_root;
    int stride_count;
    int64_t last_byte_read;
    int64_t last_byte_written;
    int64_t offset;
    enum cp_io_type last_io_type;
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
    struct darshan_file_runtime* fd_table[CP_HASH_SIZE];
    struct darshan_file_runtime* ncid_table[CP_HASH_SIZE];
    struct darshan_file_runtime* hid_table[CP_HASH_SIZE];
    struct darshan_file_runtime* fh_table[CP_HASH_SIZE];
    struct darshan_file_runtime* darshan_mru_file;
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

struct darshan_file_runtime* darshan_file_by_name(const char* name);
struct darshan_file_runtime* darshan_file_by_fd(int fd);
void darshan_initialize(int argc, char** argv, int nprocs, int rank);
void darshan_finalize(struct darshan_job_runtime* job);
void darshan_condense(void);
void darshan_search_bench(int argc, char** argv, int iters);
void darshan_shutdown(int timing_flag);
void darshan_shutdown_bench(int argc, char** argv, int rank, int nprocs);
void darshan_walk_file_accesses(struct darshan_job_runtime* final_job);
double darshan_wtime(void);

uint32_t darshan_hashlittle(const void *key, size_t length, uint32_t initval);
uint64_t darshan_hash(register unsigned char *k, register uint64_t length, register uint64_t level);

#endif /* __DARSHAN_H */
