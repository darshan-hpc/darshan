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
#include <aio.h>
#define __USE_GNU
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

#ifdef DARSHAN_PRELOAD
#define __USE_GNU
#include <dlfcn.h>
#include <stdlib.h>

#define DARSHAN_FORWARD_DECL(name,ret,args) \
  ret (*__real_ ## name)args = NULL;

#define DARSHAN_DECL(__name) __name

#define DARSHAN_MPI_CALL(func) __real_ ## func

#define MAP_OR_FAIL(func) \
    if (!(__real_ ## func)) \
    { \
        __real_ ## func = dlsym(RTLD_NEXT, #func); \
        if(!(__real_ ## func)) { \
           fprintf(stderr, "Darshan failed to map symbol: %s\n", #func); \
           exit(1); \
       } \
    }

#else

#define DARSHAN_FORWARD_DECL(name,ret,args) \
  extern ret __real_ ## name args;

#define DARSHAN_DECL(__name) __wrap_ ## __name

#define DARSHAN_MPI_CALL(func) func

#define MAP_OR_FAIL(func)

#endif

/* TODO: more libc, fgetc, etc etc etc. */

DARSHAN_FORWARD_DECL(open, int, (const char *path, int flags, ...));
DARSHAN_FORWARD_DECL(open64, int, (const char *path, int flags, ...));
DARSHAN_FORWARD_DECL(creat, int, (const char* path, mode_t mode));
DARSHAN_FORWARD_DECL(creat64, int, (const char* path, mode_t mode));
DARSHAN_FORWARD_DECL(read, ssize_t, (int fd, void *buf, size_t count));
DARSHAN_FORWARD_DECL(write, ssize_t, (int fd, const void *buf, size_t count));
DARSHAN_FORWARD_DECL(pread, ssize_t, (int fd, void *buf, size_t count, off_t offset));
DARSHAN_FORWARD_DECL(pwrite, ssize_t, (int fd, const void *buf, size_t count, off_t offset));
DARSHAN_FORWARD_DECL(pread64, ssize_t, (int fd, void *buf, size_t count, off64_t offset));
DARSHAN_FORWARD_DECL(pwrite64, ssize_t, (int fd, const void *buf, size_t count, off64_t offset));
DARSHAN_FORWARD_DECL(readv, ssize_t, (int fd, const struct iovec *iov, int iovcnt));
DARSHAN_FORWARD_DECL(writev, ssize_t, (int fd, const struct iovec *iov, int iovcnt));
DARSHAN_FORWARD_DECL(close, int, (int fd));

struct posix_file_runtime
{
    struct darshan_posix_file* file_record;
    double last_meta_end;
    double last_read_end;
    double last_write_end;
    UT_hash_handle hlink;
};

struct posix_file_runtime_ref
{
    struct posix_file_runtime* file;
    int fd;
    UT_hash_handle hlink;
};

struct posix_runtime
{
    struct posix_file_runtime* file_runtime_array;
    struct darshan_posix_file* file_record_array;
    int file_array_size;
    int file_array_ndx;
    struct posix_file_runtime* file_hash;
    struct posix_file_runtime_ref* fd_hash;
    void *red_buf;
    int shared_rec_count;
};

static struct posix_runtime *posix_runtime = NULL;
static pthread_mutex_t posix_runtime_mutex = PTHREAD_RECURSIVE_MUTEX_INITIALIZER_NP;
static int instrumentation_disabled = 0;
static int my_rank = -1;

/* TODO: I'm sure these should be applied on all modules */
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

static void posix_runtime_initialize(void);
static struct posix_file_runtime* posix_file_by_name(const char *name);
static struct posix_file_runtime* posix_file_by_name_setfd(const char* name, int fd);
static struct posix_file_runtime* posix_file_by_fd(int fd);
static void posix_file_close_fd(int fd);

static void posix_disable_instrumentation(void);
static void posix_prepare_for_reduction(darshan_record_id *shared_recs,
    int *shared_rec_count, void **send_buf, void **recv_buf, int *rec_size);
static void posix_reduce_records(void* infile_v, void* inoutfile_v,
    int *len, MPI_Datatype *datatype);
static void posix_get_output_data(void **buffer, int *size);
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
    struct posix_file_runtime* file; \
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
    /* CP_STAT_FILE(file, __path, __ret); */ \
    file->file_record->rank = my_rank; \
    if(__mode) \
        POSIX_SET(file, POSIX_MODE, __mode); \
    /* file->offset = 0; */ \
    /* file->last_byte_written = 0; */ \
    /* file->last_byte_read = 0; */ \
    if(__stream_flag)\
        POSIX_INC(file, POSIX_FOPENS, 1); \
    else \
        POSIX_INC(file, POSIX_OPENS, 1); \
    if(POSIX_F_VALUE(file, POSIX_F_OPEN_TIMESTAMP) == 0) \
        POSIX_F_SET(file, POSIX_F_OPEN_TIMESTAMP, __tm1); \
    POSIX_F_INC_NO_OVERLAP(file, __tm1, __tm2, file->last_meta_end, POSIX_F_META_TIME); \
} while(0)

#define POSIX_RECORD_READ(__ret, __fd, __pread_flag, __pread_offset, __aligned, __stream_flag, __tm1, __tm2) do{ \
    /* size_t stride; \
    int64_t this_offset; */ \
    struct posix_file_runtime* file; \
    /* int64_t file_alignment; */ \
    double __elapsed = __tm2-__tm1; \
    if(__ret < 0) break; \
    file = posix_file_by_fd(__fd); \
    if(!file) break; \
    /* if(__pread_flag) \
        this_offset = __pread_offset; \
    else \
        this_offset = file->offset; \
    file_alignment = CP_VALUE(file, CP_FILE_ALIGNMENT); \
    if(this_offset > file->last_byte_read) \
        CP_INC(file, CP_SEQ_READS, 1); \
    if(this_offset == (file->last_byte_read + 1)) \
        CP_INC(file, CP_CONSEC_READS, 1); \
    if(this_offset > 0 && this_offset > file->last_byte_read \
        && file->last_byte_read != 0) \
        stride = this_offset - file->last_byte_read - 1; \
    else \
        stride = 0; \
    file->last_byte_read = this_offset + __ret - 1; \
    file->offset = this_offset + __ret; \
    CP_MAX(file, CP_MAX_BYTE_READ, (this_offset + __ret -1)); \
    CP_INC(file, CP_BYTES_READ, __ret); */\
    if(__stream_flag)\
        POSIX_INC(file, POSIX_FREADS, 1); \
    else\
        POSIX_INC(file, POSIX_READS, 1); \
    /* CP_BUCKET_INC(file, CP_SIZE_READ_0_100, __ret); \
    cp_access_counter(file, stride, CP_COUNTER_STRIDE); \
    if(!__aligned) \
        CP_INC(file, CP_MEM_NOT_ALIGNED, 1); \
    if(file_alignment > 0 && (this_offset % file_alignment) != 0) \
        CP_INC(file, CP_FILE_NOT_ALIGNED, 1); \
    cp_access_counter(file, __ret, CP_COUNTER_ACCESS); \
    if(file->last_io_type == CP_WRITE) \
        CP_INC(file, CP_RW_SWITCHES, 1); \
    file->last_io_type = CP_READ; */ \
    POSIX_F_INC_NO_OVERLAP(file, __tm1, __tm2, file->last_read_end, POSIX_F_READ_TIME);  \
    if(POSIX_F_VALUE(file, POSIX_F_READ_START_TIMESTAMP) == 0) \
        POSIX_F_SET(file, POSIX_F_READ_START_TIMESTAMP, __tm1); \
    POSIX_F_SET(file, POSIX_F_READ_END_TIMESTAMP, __tm2); \
    if(POSIX_F_VALUE(file, POSIX_F_MAX_READ_TIME) < __elapsed){ \
        POSIX_F_SET(file, POSIX_F_MAX_READ_TIME, __elapsed); \
        POSIX_SET(file, POSIX_MAX_READ_TIME_SIZE, __ret); } \
} while(0)

#define POSIX_RECORD_WRITE(__ret, __fd, __pwrite_flag, __pwrite_offset, __aligned, __stream_flag, __tm1, __tm2) do{ \
    /* size_t stride; \
    int64_t this_offset; */ \
    struct posix_file_runtime* file; \
    /* int64_t file_alignment; */ \
    double __elapsed = __tm2-__tm1; \
    if(__ret < 0) break; \
    file = posix_file_by_fd(__fd); \
    if(!file) break; \
    /* if(__pwrite_flag) \
        this_offset = __pwrite_offset; \
    else \
        this_offset = file->offset; \
    file_alignment = CP_VALUE(file, CP_FILE_ALIGNMENT); \
    if(this_offset > file->last_byte_written) \
        CP_INC(file, CP_SEQ_WRITES, 1); \
    if(this_offset == (file->last_byte_written + 1)) \
        CP_INC(file, CP_CONSEC_WRITES, 1); \
    if(this_offset > 0 && this_offset > file->last_byte_written \
        && file->last_byte_written != 0) \
        stride = this_offset - file->last_byte_written - 1; \
    else \
        stride = 0; \
    file->last_byte_written = this_offset + __ret - 1; \
    file->offset = this_offset + __ret; \
    CP_MAX(file, CP_MAX_BYTE_WRITTEN, (this_offset + __ret -1)); \
    CP_INC(file, CP_BYTES_WRITTEN, __ret); */ \
    if(__stream_flag) \
        POSIX_INC(file, POSIX_FWRITES, 1); \
    else \
        POSIX_INC(file, POSIX_WRITES, 1); \
    /* CP_BUCKET_INC(file, CP_SIZE_WRITE_0_100, __ret); \
    cp_access_counter(file, stride, CP_COUNTER_STRIDE); \
    if(!__aligned) \
        CP_INC(file, CP_MEM_NOT_ALIGNED, 1); \
    if(file_alignment > 0 && (this_offset % file_alignment) != 0) \
        CP_INC(file, CP_FILE_NOT_ALIGNED, 1); \
    cp_access_counter(file, __ret, CP_COUNTER_ACCESS); \
    if(file->last_io_type == CP_READ) \
        CP_INC(file, CP_RW_SWITCHES, 1); \
    file->last_io_type = CP_WRITE; */ \
    POSIX_F_INC_NO_OVERLAP(file, __tm1, __tm2, file->last_write_end, POSIX_F_WRITE_TIME); \
    if(POSIX_F_VALUE(file, POSIX_F_WRITE_START_TIMESTAMP) == 0) \
        POSIX_F_SET(file, POSIX_F_WRITE_START_TIMESTAMP, __tm1); \
    POSIX_F_SET(file, POSIX_F_WRITE_END_TIMESTAMP, __tm2); \
    if(POSIX_F_VALUE(file, POSIX_F_MAX_WRITE_TIME) < __elapsed){ \
        POSIX_F_SET(file, POSIX_F_MAX_WRITE_TIME, __elapsed); \
        POSIX_SET(file, POSIX_MAX_WRITE_TIME_SIZE, __ret); } \
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

    MAP_OR_FAIL(open64);

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

int DARSHAN_DECL(creat)(const char* path, mode_t mode)
{
    int ret;
    double tm1, tm2;

    MAP_OR_FAIL(creat);

    tm1 = darshan_core_wtime();
    ret = __real_creat(path, mode);
    tm2 = darshan_core_wtime();

    POSIX_LOCK();
    posix_runtime_initialize();
    POSIX_RECORD_OPEN(ret, path, mode, 0, tm1, tm2);
    POSIX_UNLOCK();

    return(ret);
}

int DARSHAN_DECL(creat64)(const char* path, mode_t mode)
{
    int ret;
    double tm1, tm2;

    MAP_OR_FAIL(creat64);

    tm1 = darshan_core_wtime();
    ret = __real_creat64(path, mode);
    tm2 = darshan_core_wtime();

    POSIX_LOCK();
    posix_runtime_initialize();
    POSIX_RECORD_OPEN(ret, path, mode, 0, tm1, tm2);
    POSIX_UNLOCK();

    return(ret);
}

ssize_t DARSHAN_DECL(read)(int fd, void *buf, size_t count)
{
    ssize_t ret;
    int aligned_flag = 0;
    double tm1, tm2;

    MAP_OR_FAIL(write);

    /* if((unsigned long)buf % darshan_mem_alignment == 0) aligned_flag = 1; */

    tm1 = darshan_core_wtime();
    ret = __real_read(fd, buf, count);
    tm2 = darshan_core_wtime();

    POSIX_LOCK();
    posix_runtime_initialize();
    POSIX_RECORD_READ(ret, fd, 0, 0, aligned_flag, 0, tm1, tm2);
    POSIX_UNLOCK();

    return(ret);
}

ssize_t DARSHAN_DECL(write)(int fd, const void *buf, size_t count)
{
    ssize_t ret;
    int aligned_flag = 0;
    double tm1, tm2;

    MAP_OR_FAIL(write);

    /* if((unsigned long)buf % darshan_mem_alignment == 0) aligned_flag = 1; */

    tm1 = darshan_core_wtime();
    ret = __real_write(fd, buf, count);
    tm2 = darshan_core_wtime();

    POSIX_LOCK();
    posix_runtime_initialize();
    POSIX_RECORD_WRITE(ret, fd, 0, 0, aligned_flag, 0, tm1, tm2);
    POSIX_UNLOCK();

    return(ret);
}

ssize_t DARSHAN_DECL(pread)(int fd, void *buf, size_t count, off_t offset)
{
    ssize_t ret;
    int aligned_flag = 0;
    double tm1, tm2;

    MAP_OR_FAIL(pread);

    /* if((unsigned long)buf % darshan_mem_alignment == 0) aligned_flag = 1; */

    tm1 = darshan_core_wtime();
    ret = __real_pread(fd, buf, count, offset);
    tm2 = darshan_core_wtime();

    POSIX_LOCK();
    posix_runtime_initialize();
    POSIX_RECORD_READ(ret, fd, 1, offset, aligned_flag, 0, tm1, tm2);
    POSIX_UNLOCK();

    return(ret);
}

ssize_t DARSHAN_DECL(pwrite)(int fd, const void *buf, size_t count, off_t offset)
{
    ssize_t ret;
    int aligned_flag = 0;
    double tm1, tm2;

    MAP_OR_FAIL(pwrite);

    /* if((unsigned long)buf % darshan_mem_alignment == 0) aligned_flag = 1; */

    tm1 = darshan_core_wtime();
    ret = __real_pwrite(fd, buf, count, offset);
    tm2 = darshan_core_wtime();

    POSIX_LOCK();
    posix_runtime_initialize();
    POSIX_RECORD_WRITE(ret, fd, 1, offset, aligned_flag, 0, tm1, tm2);
    POSIX_UNLOCK();

    return(ret);
}

ssize_t DARSHAN_DECL(pread64)(int fd, void *buf, size_t count, off64_t offset)
{
    ssize_t ret;
    int aligned_flag = 0;
    double tm1, tm2;

    MAP_OR_FAIL(pread64);

    /* if((unsigned long)buf % darshan_mem_alignment == 0) aligned_flag = 1; */

    tm1 = darshan_core_wtime();
    ret = __real_pread64(fd, buf, count, offset);
    tm2 = darshan_core_wtime();

    POSIX_LOCK();
    posix_runtime_initialize();
    POSIX_RECORD_READ(ret, fd, 1, offset, aligned_flag, 0, tm1, tm2);
    POSIX_UNLOCK();

    return(ret);
}

ssize_t DARSHAN_DECL(pwrite64)(int fd, const void *buf, size_t count, off64_t offset)
{
    ssize_t ret;
    int aligned_flag = 0;
    double tm1, tm2;

    MAP_OR_FAIL(pwrite64);

    /* if((unsigned long)buf % darshan_mem_alignment == 0) aligned_flag = 1; */

    tm1 = darshan_core_wtime();
    ret = __real_pwrite64(fd, buf, count, offset);
    tm2 = darshan_core_wtime();

    POSIX_LOCK();
    posix_runtime_initialize();
    POSIX_RECORD_WRITE(ret, fd, 1, offset, aligned_flag, 0, tm1, tm2);
    POSIX_UNLOCK();

    return(ret);
}

ssize_t DARSHAN_DECL(readv)(int fd, const struct iovec *iov, int iovcnt)
{
    ssize_t ret;
    int aligned_flag = 0;
    /* int aligned_flag = 1; */
    int i;
    double tm1, tm2;

    MAP_OR_FAIL(readv);
/*
    for(i=0; i<iovcnt; i++)
    {
        if(((unsigned long)iov[i].iov_base % darshan_mem_alignment) != 0)
            aligned_flag = 0;
    }*/

    tm1 = darshan_core_wtime();
    ret = __real_readv(fd, iov, iovcnt);
    tm2 = darshan_core_wtime();

    POSIX_LOCK();
    POSIX_RECORD_READ(ret, fd, 0, 0, aligned_flag, 0, tm1, tm2);
    POSIX_UNLOCK();

    return(ret);
}

ssize_t DARSHAN_DECL(writev)(int fd, const struct iovec *iov, int iovcnt)
{
    ssize_t ret;
    int aligned_flag = 0;
    /* int aligned_flag = 1; */
    int i;
    double tm1, tm2;

    MAP_OR_FAIL(writev);
/*
    for(i=0; i<iovcnt; i++)
    {
        if(((unsigned long)iov[i].iov_base % darshan_mem_alignment) != 0)
            aligned_flag = 0;
    }*/

    tm1 = darshan_core_wtime();
    ret = __real_writev(fd, iov, iovcnt);
    tm2 = darshan_core_wtime();

    POSIX_LOCK();
    POSIX_RECORD_WRITE(ret, fd, 0, 0, aligned_flag, 0, tm1, tm2);
    POSIX_UNLOCK();

    return(ret);
}

int DARSHAN_DECL(close)(int fd)
{
    struct posix_file_runtime* file;
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
        /* file->last_byte_written = 0; */
        /* file->last_byte_read = 0; */
        POSIX_F_SET(file, POSIX_F_CLOSE_TIMESTAMP, darshan_core_wtime());
        POSIX_F_INC_NO_OVERLAP(file, tm1, tm2, file->last_meta_end, POSIX_F_META_TIME);
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
        .prepare_for_reduction = &posix_prepare_for_reduction,
        .reduce_records = &posix_reduce_records,
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
    /* TODO: should we base memory usage off file record or total runtime structure sizes? */
    posix_runtime->file_array_size = mem_limit / sizeof(struct darshan_posix_file);
    posix_runtime->file_array_ndx = 0;

    /* allocate array of runtime file records */
    posix_runtime->file_runtime_array = malloc(posix_runtime->file_array_size *
                                               sizeof(struct posix_file_runtime));
    posix_runtime->file_record_array = malloc(posix_runtime->file_array_size *
                                              sizeof(struct darshan_posix_file));
    if(!posix_runtime->file_runtime_array || !posix_runtime->file_record_array)
    {
        posix_runtime->file_array_size = 0;
        return;
    }
    memset(posix_runtime->file_runtime_array, 0, posix_runtime->file_array_size *
           sizeof(struct posix_file_runtime));
    memset(posix_runtime->file_record_array, 0, posix_runtime->file_array_size *
           sizeof(struct darshan_posix_file));

    /* TODO: can we move this out of here? perhaps register_module returns rank? */
    DARSHAN_MPI_CALL(PMPI_Comm_rank)(MPI_COMM_WORLD, &my_rank);

    return;
}

static struct posix_file_runtime* posix_file_by_name(const char *name)
{
    struct posix_file_runtime *file = NULL;
    char *newname = NULL;
    darshan_record_id file_id;

    if(!posix_runtime || instrumentation_disabled)
        return(NULL);

    newname = darshan_clean_file_path(name);
    if(!newname)
        newname = (char*)name;

    /* get a unique id for this file from darshan core */
    darshan_core_register_record(
        (void*)newname,
        strlen(newname),
        1,
        DARSHAN_POSIX_MOD,
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

static struct posix_file_runtime* posix_file_by_name_setfd(const char* name, int fd)
{
    struct posix_file_runtime* file;
    struct posix_file_runtime_ref* ref;

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

static struct posix_file_runtime* posix_file_by_fd(int fd)
{
    struct posix_file_runtime_ref* ref;

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
    struct posix_file_runtime_ref* ref;

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

static int posix_file_compare(const void* a, const void* b)
{
    const struct darshan_posix_file* f_a = a;
    const struct darshan_posix_file* f_b = b;

    if(f_a->rank < f_b->rank)
        return 1;
    if(f_a->rank > f_b->rank)
        return -1;

    return 0;
}

/* ***************************************************** */

static void posix_disable_instrumentation()
{
    assert(posix_runtime);

    POSIX_LOCK();
    instrumentation_disabled = 1;
    POSIX_UNLOCK();

    return;
}

static void posix_prepare_for_reduction(
    darshan_record_id *shared_recs,
    int *shared_rec_count,
    void **send_buf,
    void **recv_buf,
    int *rec_size)
{
    struct posix_file_runtime *file;
    struct darshan_posix_file *tmp_array;
    int i;

    assert(posix_runtime);

    /* necessary initialization of shared records (e.g., change rank to -1) */
    for(i = 0; i < *shared_rec_count; i++)
    {
        HASH_FIND(hlink, posix_runtime->file_hash, &shared_recs[i],
            sizeof(darshan_record_id), file);
        assert(file);

        /* TODO: any initialization before reduction */
        file->file_record->rank = -1;
    }

    /* sort the array of files descending by rank so that we get all of the 
     * shared files (marked by rank -1) in a contiguous portion at end 
     * of the array
     */
    qsort(posix_runtime->file_record_array, posix_runtime->file_array_ndx,
        sizeof(struct darshan_posix_file), posix_file_compare);

    /* make *send_buf point to the shared files at the end of sorted array */
    *send_buf =
        &(posix_runtime->file_record_array[posix_runtime->file_array_ndx-(*shared_rec_count)]);

    /* allocate memory for the reduction output on rank 0 */
    if(my_rank == 0)
    {
        *recv_buf = malloc(*shared_rec_count * sizeof(struct darshan_posix_file));
        if(!(*recv_buf))
            return;
    }

    *rec_size = sizeof(struct darshan_posix_file);

    /* TODO: cleaner way to do this? */
    if(my_rank == 0)
        posix_runtime->red_buf = *recv_buf;
    posix_runtime->shared_rec_count = *shared_rec_count;

    return;
}

static void posix_reduce_records(
    void* infile_v,
    void* inoutfile_v,
    int *len,
    MPI_Datatype *datatype)
{
    struct darshan_posix_file tmp_file;
    struct darshan_posix_file *infile = infile_v;
    struct darshan_posix_file *inoutfile = inoutfile_v;
    int i;

    assert(posix_runtime);

    for(i = 0; i < *len; i++)
    {
        memset(&tmp_file, 0, sizeof(struct darshan_posix_file));

        tmp_file.f_id = infile->f_id;
        tmp_file.rank = -1;

        tmp_file.counters[POSIX_OPENS] = infile->counters[POSIX_OPENS] +
            inoutfile->counters[POSIX_OPENS];

        if((infile->fcounters[POSIX_F_OPEN_TIMESTAMP] > inoutfile->fcounters[POSIX_F_OPEN_TIMESTAMP]) &&
            (inoutfile->fcounters[POSIX_F_OPEN_TIMESTAMP] > 0))
            tmp_file.fcounters[POSIX_F_OPEN_TIMESTAMP] = inoutfile->fcounters[POSIX_F_OPEN_TIMESTAMP];
        else
            tmp_file.fcounters[POSIX_F_OPEN_TIMESTAMP] = infile->fcounters[POSIX_F_OPEN_TIMESTAMP];

        if(infile->fcounters[POSIX_F_CLOSE_TIMESTAMP] > inoutfile->fcounters[POSIX_F_CLOSE_TIMESTAMP])
            tmp_file.fcounters[POSIX_F_CLOSE_TIMESTAMP] = infile->fcounters[POSIX_F_CLOSE_TIMESTAMP];
        else
            tmp_file.fcounters[POSIX_F_CLOSE_TIMESTAMP] = inoutfile->fcounters[POSIX_F_CLOSE_TIMESTAMP];

        /* update pointers */
        *inoutfile = tmp_file;
        inoutfile++;
        infile++;
    }

    return;
}

static void posix_get_output_data(
    void **buffer,
    int *size)
{
    assert(posix_runtime);

    /* TODO: cleaner way to do this? */
    /* clean up reduction state */
    if(my_rank == 0)
    {
        int tmp_ndx = posix_runtime->file_array_ndx - posix_runtime->shared_rec_count;
        memcpy(&(posix_runtime->file_record_array[tmp_ndx]), posix_runtime->red_buf,
            posix_runtime->shared_rec_count * sizeof(struct darshan_posix_file));
        free(posix_runtime->red_buf);
    }
    else
    {
        posix_runtime->file_array_ndx -= posix_runtime->shared_rec_count;
    }

    *buffer = (void *)(posix_runtime->file_record_array);
    *size = posix_runtime->file_array_ndx * sizeof(struct darshan_posix_file);

    return;
}

static void posix_shutdown()
{
    struct posix_file_runtime_ref *ref, *tmp;

    HASH_ITER(hlink, posix_runtime->fd_hash, ref, tmp)
    {
        HASH_DELETE(hlink, posix_runtime->fd_hash, ref);
        free(ref);
    }

    HASH_CLEAR(hlink, posix_runtime->file_hash); /* these entries are freed all at once below */

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
