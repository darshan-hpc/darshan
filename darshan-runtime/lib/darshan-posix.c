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

#include "uthash.h"
#include "darshan.h"
#include "darshan-posix-log-format.h"

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
static int instrumentation_disabled = 0;
static int my_rank = -1;

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
DARSHAN_FORWARD_DECL(open64, int, (const char *path, int flags, ...));
DARSHAN_FORWARD_DECL(close, int, (int fd));

static void posix_runtime_initialize(void);

static struct posix_runtime_file* posix_file_by_name(const char *name);
static struct posix_runtime_file* posix_file_by_name_setfd(const char* name, int fd);
static struct posix_runtime_file* posix_file_by_fd(int fd);
static void posix_file_close_fd(int fd);

static void posix_disable_instrumentation(void);
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

int DARSHAN_DECL(open64)(const char *path, int flags, ...)
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
        ret = __real_open64(path, flags, mode);
        tm2 = darshan_core_wtime();
    }
    else
    {
        tm1 = darshan_core_wtime();
        ret = __real_open64(path, flags);
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
    struct posix_runtime_file* file;
    double tm1, tm2;
    int ret;

    MAP_OR_FAIL(close);

    tm1 = darshan_core_wtime();
    ret = __real_close(fd);
    tm2 = darshan_core_wtime();

    POSIX_LOCK();
    posix_runtime_initialize();

    file = posix_file_by_fd(fd);
    if(file)
    {
        POSIX_F_SET(file, CP_F_CLOSE_TIMESTAMP, darshan_core_wtime());
        posix_file_close_fd(fd);
    }
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
        .disable_instrumentation = &posix_disable_instrumentation,
        .get_output_data = &posix_get_output_data,
        .shutdown = &posix_shutdown
    };

    /* don't do anything if already initialized or instrumenation is disabled */
    if(posix_runtime || instrumentation_disabled)
        return;

    /* register the posix module with darshan core */
    darshan_core_register_module(
        DARSHAN_POSIX_MOD,
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

    DARSHAN_MPI_CALL(PMPI_Comm_rank)(MPI_COMM_WORLD, &my_rank); /* TODO: can we move this out of here? */

    return;
}

static struct posix_runtime_file* posix_file_by_name(const char *name)
{
    struct posix_runtime_file *file = NULL;
    char *newname = NULL;
    darshan_record_id file_id;

    if(!posix_runtime || instrumentation_disabled)
        return(NULL);

    newname = darshan_clean_file_path(name);
    if(!newname)
        newname = (char*)name;

    /* get a unique id for this file from darshan core */
    darshan_core_lookup_record_id(
        (void*)newname,
        strlen(newname),
        1,
        &file_id);

    /* search the hash table for this file record, and return if found */
    HASH_FIND(hlink, posix_runtime->file_hash, &file_id, sizeof(darshan_record_id), file);
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
        HASH_ADD(hlink, posix_runtime->file_hash, file_record->f_id, sizeof(darshan_record_id), file);

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

    if(!posix_runtime || instrumentation_disabled)
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

static struct posix_runtime_file* posix_file_by_fd(int fd)
{
    struct posix_runtime_file_ref* ref;

    if(!posix_runtime || instrumentation_disabled)
        return(NULL);

    /* search hash table for existing file ref for this fd */
    HASH_FIND(hlink, posix_runtime->fd_hash, &fd, sizeof(int), ref);
    if(ref)
        return(ref->file);

    return(NULL);
}

static void posix_file_close_fd(int fd)
{
    struct posix_runtime_file_ref* ref;

    if(!posix_runtime || instrumentation_disabled)
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

static void posix_disable_instrumentation()
{
    POSIX_LOCK();
    instrumentation_disabled = 1;
    POSIX_UNLOCK();

    return;
}

static void posix_get_output_data(MPI_Comm comm, void **buffer, int *size)
{
    /* TODO: shared file reduction */

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
