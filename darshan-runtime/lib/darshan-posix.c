/*
 *  (C) 2009 by Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
 */

#include "darshan-runtime-config.h"
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdarg.h>
#include <string.h>
#include <time.h>
#include <stdlib.h>
#include <errno.h>
#include <sys/uio.h>
#include <sys/mman.h>
#include <search.h>
#include <assert.h>
#include <libgen.h>
#include <limits.h>
#include <aio.h>
#define __USE_GNU
#include <pthread.h>

#include "darshan.h"

#ifndef HAVE_OFF64_T
typedef int64_t off64_t;
#endif
#ifndef HAVE_AIOCB64
#define aiocb64 aiocb
#endif

/* TODO these go where ? */

#define DARSHAN_FORWARD_DECL(name,ret,args) \
  extern ret __real_ ## name args;

#define DARSHAN_DECL(__name) __wrap_ ## __name

#define MAP_OR_FAIL(func)

#define POSIX_HASH_BITS 8
#define POSIX_HASH_SIZE (1 << POSIX_HASH_BITS)
#define POSIX_HASH_MASK (POSIX_HASH_SIZE - 1)

/* TODO: where do these file record structs go? (some needed for darshan-util) */
/* TODO: DARSHAN_* OR CP_* */

#define POSIX_MOD_NAME "POSIX"

enum darshan_posix_indices
{
    CP_POSIX_READS,              /* count of posix reads */
    CP_POSIX_WRITES,             /* count of posix writes */
    CP_POSIX_OPENS,              /* count of posix opens */
    CP_POSIX_SEEKS,              /* count of posix seeks */
    CP_POSIX_STATS,              /* count of posix stat/lstat/fstats */
    CP_POSIX_MMAPS,              /* count of posix mmaps */
    CP_POSIX_FREADS,
    CP_POSIX_FWRITES,
    CP_POSIX_FOPENS,
    CP_POSIX_FSEEKS,
    CP_POSIX_FSYNCS,
    CP_POSIX_FDSYNCS,
    CP_MODE,                      /* mode of file */
    CP_BYTES_READ,                /* total bytes read */
    CP_BYTES_WRITTEN,             /* total bytes written */
    CP_MAX_BYTE_READ,             /* highest offset byte read */
    CP_MAX_BYTE_WRITTEN,          /* highest offset byte written */
    CP_CONSEC_READS,              /* count of consecutive reads */
    CP_CONSEC_WRITES,             /* count of consecutive writes */
    CP_SEQ_READS,                 /* count of sequential reads */
    CP_SEQ_WRITES,                /* count of sequential writes */
    CP_RW_SWITCHES,               /* number of times switched between read and write */
    CP_MEM_NOT_ALIGNED,           /* count of accesses not mem aligned */
    CP_MEM_ALIGNMENT,             /* mem alignment in bytes */
    CP_FILE_NOT_ALIGNED,          /* count of accesses not file aligned */
    CP_FILE_ALIGNMENT,            /* file alignment in bytes */
    CP_MAX_READ_TIME_SIZE,
    CP_MAX_WRITE_TIME_SIZE,
    /* buckets */
    CP_SIZE_READ_0_100,           /* count of posix read size ranges */
    CP_SIZE_READ_100_1K,
    CP_SIZE_READ_1K_10K,
    CP_SIZE_READ_10K_100K,
    CP_SIZE_READ_100K_1M,
    CP_SIZE_READ_1M_4M,
    CP_SIZE_READ_4M_10M,
    CP_SIZE_READ_10M_100M,
    CP_SIZE_READ_100M_1G,
    CP_SIZE_READ_1G_PLUS,
    /* buckets */
    CP_SIZE_WRITE_0_100,          /* count of posix write size ranges */
    CP_SIZE_WRITE_100_1K,
    CP_SIZE_WRITE_1K_10K,
    CP_SIZE_WRITE_10K_100K,
    CP_SIZE_WRITE_100K_1M,
    CP_SIZE_WRITE_1M_4M,
    CP_SIZE_WRITE_4M_10M,
    CP_SIZE_WRITE_10M_100M,
    CP_SIZE_WRITE_100M_1G,
    CP_SIZE_WRITE_1G_PLUS,
    /* counters */
    CP_STRIDE1_STRIDE,             /* the four most frequently appearing strides */
    CP_STRIDE2_STRIDE,
    CP_STRIDE3_STRIDE,
    CP_STRIDE4_STRIDE,
    CP_STRIDE1_COUNT,              /* count of each of the most frequent strides */
    CP_STRIDE2_COUNT,
    CP_STRIDE3_COUNT,
    CP_STRIDE4_COUNT,
    CP_ACCESS1_ACCESS,             /* the four most frequently appearing access sizes */
    CP_ACCESS2_ACCESS,
    CP_ACCESS3_ACCESS,
    CP_ACCESS4_ACCESS,
    CP_ACCESS1_COUNT,              /* count of each of the most frequent access sizes */
    CP_ACCESS2_COUNT,
    CP_ACCESS3_COUNT,
    CP_ACCESS4_COUNT,
    CP_DEVICE,                     /* device id reported by stat */
    CP_SIZE_AT_OPEN,
    CP_FASTEST_RANK,
    CP_FASTEST_RANK_BYTES,
    CP_SLOWEST_RANK,
    CP_SLOWEST_RANK_BYTES,

    CP_NUM_INDICES,
};

/* floating point statistics */
enum f_darshan_posix_indices
{
    /* NOTE: adjust cp_normalize_timestamps() function if any TIMESTAMPS are
     * added or modified in this list
     */
    CP_F_OPEN_TIMESTAMP = 0,    /* timestamp of first open */
    CP_F_READ_START_TIMESTAMP,  /* timestamp of first read */
    CP_F_WRITE_START_TIMESTAMP, /* timestamp of first write */
    CP_F_CLOSE_TIMESTAMP,       /* timestamp of last close */
    CP_F_READ_END_TIMESTAMP,    /* timestamp of last read */
    CP_F_WRITE_END_TIMESTAMP,   /* timestamp of last write */
    CP_F_POSIX_READ_TIME,       /* cumulative posix read time */
    CP_F_POSIX_WRITE_TIME,      /* cumulative posix write time */
    CP_F_POSIX_META_TIME,       /* cumulative posix meta time */
    CP_F_MAX_READ_TIME,
    CP_F_MAX_WRITE_TIME,
    /* Total I/O and meta time consumed by fastest and slowest ranks, 
     * reported in either MPI or POSIX time depending on how the file 
     * was accessed.
     */
    CP_F_FASTEST_RANK_TIME,     
    CP_F_SLOWEST_RANK_TIME,
    CP_F_VARIANCE_RANK_TIME,
    CP_F_VARIANCE_RANK_BYTES,

    CP_F_NUM_INDICES,
};

struct darshan_posix_file
{
    darshan_file_id f_id;
    int64_t counters[CP_NUM_INDICES];
    double fcounters[CP_F_NUM_INDICES];
};

struct darshan_posix_runtime_file
{
    struct darshan_posix_file file_record;
    struct darshan_posix_runtime_file *next_file;
};

struct darshan_posix_runtime
{
    struct darshan_posix_runtime_file* file_array;
    int file_array_size;
    int file_count;
    struct darshan_posix_runtime_file* file_table[POSIX_HASH_SIZE];
};

static pthread_mutex_t posix_runtime_mutex = PTHREAD_RECURSIVE_MUTEX_INITIALIZER_NP;
static struct darshan_posix_runtime *posix_runtime = NULL;
static int my_rank = -1;
static int darshan_mem_alignment = 1;

/* these are paths that we will not trace */
static char* exclusions[] = {
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


DARSHAN_FORWARD_DECL(open, int, (const char *path, int flags, ...));
//DARSHAN_FORWARD_DECL(close, int, (int fd));

static void posix_runtime_initialize(void);
static void posix_runtime_finalize(void);

static void posix_prepare_for_shutdown(void);
static void posix_get_output_data(void **buffer, int size);

#define POSIX_LOCK() pthread_mutex_lock(&posix_runtime_mutex)
#define POSIX_UNLOCK() pthread_mutex_unlock(&posix_runtime_mutex)

#define POSIX_RECORD_OPEN(__ret, __path, __mode, __stream_flag, __tm1, __tm2) do { \
    struct darshan_posix_runtime_file* file; \
    char* exclude; \
    int tmp_index = 0; \
    if(__ret < 0) break; \
    while((exclude = exclusions[tmp_index])) { \
        if(!(strncmp(exclude, __path, strlen(exclude)))) \
            break; \
        tmp_index++; \
    } \
    if(exclude) break; \
    file = darshan_file_by_name(__path, __ret); \
    if(!file) break; \
    file->log_file->rank = my_rank; \
    if(__mode) \
        DARSHAN_SET(file, DARSHAN_MODE, __mode); \
    file->offset = 0; \
    file->last_byte_written = 0; \
    file->last_byte_read = 0; \
    if(__stream_flag)\
        DARSHAN_INC(file, DARSHAN_POSIX_FOPENS, 1); \
    else \
        DARSHAN_INC(file, DARSHAN_POSIX_OPENS, 1); \
    if(DARSHAN_F_VALUE(file, DARSHAN_F_OPEN_TIMESTAMP) == 0) \
        DARSHAN_F_SET(file, DARSHAN_F_OPEN_TIMESTAMP, __tm1); \
    DARSHAN_F_INC_NO_OVERLAP(file, __tm1, __tm2, file->last_posix_meta_end, DARSHAN_F_POSIX_META_TIME); \
} while (0)

int DARSHAN_DECL(open)(const char *path, int flags, ...)
{
    int mode = 0;
    int ret;
    double tm1, tm2;

    MAP_OR_FAIL(open);

    if(flags & O_CREAT) 
    {
        va_list arg;
        va_start(arg, flags);
        mode = va_arg(arg, int);
        va_end(arg);

        tm1 = darshan_core_wtime();
        ret = __real_open(path, flags, mode);
        tm2 = darshan_core_wtime();
    }
    else
    {
        tm1 = darshan_core_wtime();
        ret = __real_open(path, flags);
        tm2 = darshan_core_wtime();
    }

    POSIX_LOCK();
    posix_runtime_initialize();

    //POSIX_RECORD_OPEN(ret, path, mode, 0, tm1, tm2);

    POSIX_UNLOCK();

    return(ret);
}

#if 0
int DARSHAN_DECL(close)(int fd)
{
    struct darshan_file_runtime* file;
    int tmp_fd = fd;
    double tm1, tm2;
    int ret;

    MAP_OR_FAIL(close);

    tm1 = darshan_core_wtime();
    ret = __real_close(fd);
    tm2 = darshan_core_wtime();

    POSIX_LOCK();
    posix_runtime_initialize();

    file = darshan_file_by_fd(tmp_fd);
    if(file)
    {
        file->last_byte_written = 0;
        file->last_byte_read = 0;
        DARSHAN_F_SET(file, DARSHAN_F_CLOSE_TIMESTAMP, posix_wtime());
        DARSHAN_F_INC_NO_OVERLAP(file, tm1, tm2, file->last_posix_meta_end, DARSHAN_F_POSIX_META_TIME);
        darshan_file_close_fd(tmp_fd);
    }

    POSIX_UNLOCK();    

    return(ret);
}
#endif

/* ***************************************************** */

static void posix_runtime_initialize()
{
    char *alignstr;
    int tmpval;
    int ret;
    int mem_limit;
    struct darshan_module_funcs posix_mod_fns =
    {
        .prepare_for_shutdown = &posix_prepare_for_shutdown,
        .get_output_data = &posix_get_output_data,
    };

    if(posix_runtime)
        return;

#if 0
    /* set the memory alignment according to config or environment variables */
    #if (__CP_MEM_ALIGNMENT < 1)
        #error Darshan must be configured with a positive value for --with-mem-align
    #endif
    alignstr = getenv("DARSHAN_MEMALIGN");
    if(alignstr)
    {
        ret = sscanf(alignstr, "%d", &tmpval);
        /* silently ignore if the env variable is set poorly */
        if(ret == 1 && tmpval > 0)
        {
            darshan_mem_alignment = tmpval;
        }
    }
    else
    {
        darshan_mem_alignment = __CP_MEM_ALIGNMENT;
    }

    /* avoid floating point errors on faulty input */
    if(darshan_mem_alignment < 1)
    {
        darshan_mem_alignment = 1;
    }
#endif

    posix_runtime = malloc(sizeof(*posix_runtime));
    if(!posix_runtime)
        return;
    memset(posix_runtime, 0, sizeof(*posix_runtime));

    /* register the posix module with darshan core */
    darshan_core_register_module(
        POSIX_MOD_NAME,
        &posix_mod_fns,
        &mem_limit);

    /* initialize posix runtime variables */
    posix_runtime->file_array_size = mem_limit / sizeof(struct darshan_posix_runtime_file);
    posix_runtime->file_count = 0;

    /* allocate array of runtime file records no larger than the returned mem_limit */
    posix_runtime->file_array = malloc(sizeof(struct darshan_posix_runtime_file) *
                                       posix_runtime->file_array_size);
    if(!posix_runtime->file_array)
    {
        posix_runtime->file_array_size = 0;
        return;
    }
    memset(posix_runtime->file_array, 0, sizeof(struct darshan_posix_runtime_file) *
           posix_runtime->file_array_size);

    return;
}

static struct darshan_posix_runtime_file* posix_file_by_name(const char *name)
{
    struct darshan_posix_runtime_file *tmp_file;
    char *newname = NULL;
    darshan_file_id tmp_id;
    int hash_index;

    if(!posix_runtime)
        return(NULL);

    newname = darshan_clean_file_path(name);
    if(!newname)
        newname = (char*)name;

    /* get a unique id for this file from darshan core */
    darshan_core_lookup_id(
        (void*)newname,
        strlen(newname),
        1,
        &tmp_id);

    /* search the hash table for this file record */
    hash_index = tmp_id & POSIX_HASH_MASK;
    tmp_file = posix_runtime->file_table[hash_index];
    while(tmp_file)
    {
        if(tmp_file->file_record.f_id == tmp_id)
        {
            if(newname != name)
                free(newname);
            return(tmp_file);
        }
        tmp_file = tmp_file->next_file;
    }

    /* no existing record, assign a new file record from the global array */
    tmp_file = &posix_runtime->file_array[posix_runtime->file_count];
    tmp_file->file_record.f_id = tmp_id;

    posix_runtime->file_count++;

    /* put record into hash table, head of list at that index */
    tmp_file->next_file = posix_runtime->file_table[hash_index];
    posix_runtime->file_table[hash_index] = tmp_file;

    if(newname != name)
        free(newname);
    return(tmp_file);
}

/* ***************************************************** */

static void posix_prepare_for_shutdown()
{

    return;
}

static void posix_get_output_data(void **buffer, int size)
{

    return;
}

/*
 * Local variables:
 *  c-indent-level: 4
 *  c-basic-offset: 4
 * End:
 *
 * vim: ts=8 sts=4 sw=4 expandtab
 */
