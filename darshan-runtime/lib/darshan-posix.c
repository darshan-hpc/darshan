/*
 *  (C) 2009 by Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
 */

#define _GNU_SOURCE

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
#include <aio.h>
#include <pthread.h>

#include "darshan.h"
#include "uthash.h"

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
enum darshan_f_posix_indices
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
    int64_t rank;
    int64_t counters[CP_NUM_INDICES];
    double fcounters[CP_F_NUM_INDICES];
};

struct posix_runtime_file
{
    struct darshan_posix_file* file_record;
    UT_hash_handle hlink;
};

struct posix_runtime_file_ref
{
    struct posix_runtime_file* file;
    int fd;
    UT_hash_handle hlink;
};

struct posix_runtime
{
    struct posix_runtime_file* file_runtime_array;
    struct darshan_posix_file* file_record_array;
    int file_array_size;
    int file_array_ndx;
    struct posix_runtime_file* file_hash;
    struct posix_runtime_file_ref* fd_hash;
};

static struct posix_runtime *posix_runtime = NULL;
static pthread_mutex_t posix_runtime_mutex = PTHREAD_RECURSIVE_MUTEX_INITIALIZER_NP;
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
DARSHAN_FORWARD_DECL(close, int, (int fd));

static void posix_runtime_initialize(void);

static struct posix_runtime_file* posix_file_by_name(const char *name);
static struct posix_runtime_file* posix_file_by_name_setfd(const char* name, int fd);
static void posix_file_close_fd(int fd);

static void posix_get_output_data(MPI_Comm comm, void **buffer, int *size);
static void posix_shutdown(void);

#define POSIX_LOCK() pthread_mutex_lock(&posix_runtime_mutex)
#define POSIX_UNLOCK() pthread_mutex_unlock(&posix_runtime_mutex)

#define POSIX_SET(__file, __counter, __value) do {\
    (__file)->file_record->counters[__counter] = __value; \
} while(0)

#define POSIX_F_SET(__file, __counter, __value) do {\
    (__file)->file_record->fcounters[__counter] = __value; \
} while(0)

#define POSIX_INC(__file, __counter, __value) do {\
    (__file)->file_record->counters[__counter] += __value; \
} while(0)

#define POSIX_F_INC(__file, __counter, __value) do {\
    (__file)->file_record->fcounters[__counter] += __value; \
} while(0)

#define POSIX_F_INC_NO_OVERLAP(__file, __tm1, __tm2, __last, __counter) do { \
    if(__tm1 > __last) \
        POSIX_F_INC(__file, __counter, (__tm2-__tm1)); \
    else \
        POSIX_F_INC(__file, __counter, (__tm2 - __last)); \
    if(__tm2 > __last) \
        __last = __tm2; \
} while(0)

#define POSIX_VALUE(__file, __counter) \
    ((__file)->file_record->counters[__counter])

#define POSIX_F_VALUE(__file, __counter) \
    ((__file)->file_record->fcounters[__counter])

#define POSIX_MAX(__file, __counter, __value) do {\
    if((__file)->file_record->counters[__counter] < __value) \
    { \
        (__file)->file_record->counters[__counter] = __value; \
    } \
} while(0)

#define POSIX_RECORD_OPEN(__ret, __path, __mode, __stream_flag, __tm1, __tm2) do { \
    struct posix_runtime_file* file; \
    char* exclude; \
    int tmp_index = 0; \
    if(__ret < 0) break; \
    while((exclude = exclusions[tmp_index])) { \
        if(!(strncmp(exclude, __path, strlen(exclude)))) \
            break; \
        tmp_index++; \
    } \
    if(exclude) break; \
    file = posix_file_by_name_setfd(__path, __ret); \
    if(!file) break; \
    file->file_record->rank = my_rank; \
    if(__mode) \
        POSIX_SET(file, CP_MODE, __mode); \
    if(__stream_flag)\
        POSIX_INC(file, CP_POSIX_FOPENS, 1); \
    else \
        POSIX_INC(file, CP_POSIX_OPENS, 1); \
    if(POSIX_F_VALUE(file, CP_F_OPEN_TIMESTAMP) == 0) \
        POSIX_F_SET(file, CP_F_OPEN_TIMESTAMP, __tm1); \
} while(0)

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

    POSIX_RECORD_OPEN(ret, path, mode, 0, tm1, tm2);

    POSIX_UNLOCK();

    return(ret);
}

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

    posix_file_close_fd(tmp_fd);

    POSIX_UNLOCK();    

    return(ret);
}

/* ***************************************************** */

static void posix_runtime_initialize()
{
    char *alignstr;
    int tmpval;
    int ret;
    int mem_limit;
    struct darshan_module_funcs posix_mod_fns =
    {
        .get_output_data = &posix_get_output_data,
        .shutdown = &posix_shutdown
    };

    if(posix_runtime)
        return;

    /* register the posix module with darshan core */
    darshan_core_register_module(
        DARSHAN_POSIX_MOD,
        POSIX_MOD_NAME,
        &posix_mod_fns,
        &mem_limit);

    /* return if no memory assigned by darshan core */
    if(mem_limit == 0)
        return;

    posix_runtime = malloc(sizeof(*posix_runtime));
    if(!posix_runtime)
        return;
    memset(posix_runtime, 0, sizeof(*posix_runtime));

    /* set maximum number of file records according to max memory limit */
    /* NOTE: maximum number of records is based on the size of a posix file record */
    posix_runtime->file_array_size = mem_limit / sizeof(struct darshan_posix_file);
    posix_runtime->file_array_ndx = 0;

    /* allocate array of runtime file records */
    posix_runtime->file_runtime_array = malloc(posix_runtime->file_array_size *
                                               sizeof(struct posix_runtime_file));
    posix_runtime->file_record_array = malloc(posix_runtime->file_array_size *
                                              sizeof(struct darshan_posix_file));
    if(!posix_runtime->file_runtime_array || !posix_runtime->file_record_array)
    {
        posix_runtime->file_array_size = 0;
        return;
    }
    memset(posix_runtime->file_runtime_array, 0, posix_runtime->file_array_size *
           sizeof(struct posix_runtime_file));
    memset(posix_runtime->file_record_array, 0, posix_runtime->file_array_size *
           sizeof(struct darshan_posix_file));

    DARSHAN_MPI_CALL(PMPI_Comm_rank)(MPI_COMM_WORLD, &my_rank);

#if 0
    /* set the memory alignment according to config or environment variables */
    #if (__CP_MEM_ALIGNMENT < 1)
        #error Darshan must be configured with a positive value for --with-mem-align
    #endif
    alignstr = getenv(CP_MEM_ALIGNMENT_OVERRIDE);
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

    return;
}

static struct posix_runtime_file* posix_file_by_name(const char *name)
{
    struct posix_runtime_file *file = NULL;
    char *newname = NULL;
    darshan_file_id file_id;

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
        &file_id);

    /* search the hash table for this file record, and return if found */
    HASH_FIND(hlink, posix_runtime->file_hash, &file_id, sizeof(darshan_file_id), file);
    if(file)
    {
        if(newname != name)
            free(newname);
        return(file);
    }

    if(posix_runtime->file_array_ndx < posix_runtime->file_array_size);
    {
        /* no existing record, assign a new file record from the global array */
        file = &(posix_runtime->file_runtime_array[posix_runtime->file_array_ndx]);
        file->file_record = &(posix_runtime->file_record_array[posix_runtime->file_array_ndx]);
        file->file_record->f_id = file_id;

        /* add new record to file hash table */
        HASH_ADD(hlink, posix_runtime->file_hash, file_record->f_id, sizeof(darshan_file_id), file);

        posix_runtime->file_array_ndx++;
    }

    if(newname != name)
        free(newname);
    return(file);
}

static struct posix_runtime_file* posix_file_by_name_setfd(const char* name, int fd)
{
    struct posix_runtime_file* file;
    struct posix_runtime_file_ref* ref;

    if(!posix_runtime)
        return(NULL);

    /* find file record by name first */
    file = posix_file_by_name(name);

    if(!file)
        return(NULL);

    /* search hash table for existing file ref for this fd */
    HASH_FIND(hlink, posix_runtime->fd_hash, &fd, sizeof(int), ref);
    if(ref)
    {
        /* we have a reference.  Make sure it points to the correct file
         * and return it
         */
        ref->file = file;
        return(file);
    }

    /* if we hit this point, then we don't have a reference for this fd
     * in the table yet.  Add it.
     */
    ref = malloc(sizeof(*ref));
    if(!ref)
        return(NULL);
    memset(ref, 0, sizeof(*ref));
    ref->file = file;
    ref->fd = fd;    

    HASH_ADD(hlink, posix_runtime->fd_hash, fd, sizeof(int), ref);

    return(file);
}

static void posix_file_close_fd(int fd)
{
    struct posix_runtime_file_ref *ref;

    if(!posix_runtime)
        return;

    /* search hash table for this fd */
    HASH_FIND(hlink, posix_runtime->fd_hash, &fd, sizeof(int), ref);
    if(ref)
    {
        /* we have a reference, delete it */
        HASH_DELETE(hlink, posix_runtime->fd_hash, ref);
        free(ref);
    }

    return;
}

/* ***************************************************** */

static void posix_get_output_data(MPI_Comm comm, void **buffer, int *size)
{
    int comm_cmp;
    
    MPI_Comm_compare(MPI_COMM_WORLD, comm, &comm_cmp);

    /* only do shared file reductions if this communicator includes _everyone_ */
    if((comm_cmp == MPI_IDENT) || (comm_cmp == MPI_CONGRUENT))
    {
        /* don't reduce shared files if that feature is disabled, either */
        if(!getenv("DARSHAN_DISABLE_SHARED_REDUCTION"))
        {
            /* TODO reduction code */
        }
    }

    *buffer = (void *)(posix_runtime->file_record_array);
    *size = posix_runtime->file_array_ndx * sizeof(struct darshan_posix_file);

    return;
}

static void posix_shutdown()
{
    /* TODO destroy hash tables ?? */

    free(posix_runtime->file_runtime_array);
    free(posix_runtime->file_record_array);
    free(posix_runtime);
    posix_runtime = NULL;

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
