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

extern char* __progname_full;

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


extern double (*__real_PMPI_Wtime)(void);

#else

#define DARSHAN_FORWARD_DECL(name,ret,args) \
  extern ret __real_ ## name args;

#define DARSHAN_DECL(__name) __wrap_ ## __name

#define MAP_OR_FAIL(func)

#define DARSHAN_MPI_CALL(func) func

#endif

DARSHAN_FORWARD_DECL(creat, int, (const char* path, mode_t mode));
DARSHAN_FORWARD_DECL(creat64, int, (const char* path, mode_t mode));
DARSHAN_FORWARD_DECL(open, int, (const char *path, int flags, ...));
DARSHAN_FORWARD_DECL(open64, int, (const char *path, int flags, ...));
DARSHAN_FORWARD_DECL(close, int, (int fd));
DARSHAN_FORWARD_DECL(write, ssize_t, (int fd, const void *buf, size_t count));
DARSHAN_FORWARD_DECL(read, ssize_t, (int fd, void *buf, size_t count));
DARSHAN_FORWARD_DECL(lseek, off_t, (int fd, off_t offset, int whence));
DARSHAN_FORWARD_DECL(lseek64, off64_t, (int fd, off64_t offset, int whence));
DARSHAN_FORWARD_DECL(pread, ssize_t, (int fd, void *buf, size_t count, off_t offset));
DARSHAN_FORWARD_DECL(pread64, ssize_t, (int fd, void *buf, size_t count, off64_t offset));
DARSHAN_FORWARD_DECL(pwrite, ssize_t, (int fd, const void *buf, size_t count, off_t offset));
DARSHAN_FORWARD_DECL(pwrite64, ssize_t, (int fd, const void *buf, size_t count, off64_t offset
));
DARSHAN_FORWARD_DECL(readv, ssize_t, (int fd, const struct iovec *iov, int iovcnt));
DARSHAN_FORWARD_DECL(writev, ssize_t, (int fd, const struct iovec *iov, int iovcnt));
DARSHAN_FORWARD_DECL(__fxstat, int, (int vers, int fd, struct stat *buf));
DARSHAN_FORWARD_DECL(__fxstat64, int, (int vers, int fd, struct stat64 *buf));
DARSHAN_FORWARD_DECL(__lxstat, int, (int vers, const char* path, struct stat *buf));
DARSHAN_FORWARD_DECL(__lxstat64, int, (int vers, const char* path, struct stat64 *buf));
DARSHAN_FORWARD_DECL(__xstat, int, (int vers, const char* path, struct stat *buf));
DARSHAN_FORWARD_DECL(__xstat64, int, (int vers, const char* path, struct stat64 *buf));
DARSHAN_FORWARD_DECL(mmap, void*, (void *addr, size_t length, int prot, int flags, int fd, off_t offset));
DARSHAN_FORWARD_DECL(mmap64, void*, (void *addr, size_t length, int prot, int flags, int fd, off64_t offset));
DARSHAN_FORWARD_DECL(fopen, FILE*, (const char *path, const char *mode));
DARSHAN_FORWARD_DECL(fopen64, FILE*, (const char *path, const char *mode));
DARSHAN_FORWARD_DECL(fclose, int, (FILE *fp));
DARSHAN_FORWARD_DECL(fread, size_t, (void *ptr, size_t size, size_t nmemb, FILE *stream));
DARSHAN_FORWARD_DECL(fwrite, size_t, (const void *ptr, size_t size, size_t nmemb, FILE *stream));
DARSHAN_FORWARD_DECL(fseek, int, (FILE *stream, long offset, int whence));
DARSHAN_FORWARD_DECL(fsync, int, (int fd));
DARSHAN_FORWARD_DECL(fdatasync, int, (int fd));
DARSHAN_FORWARD_DECL(aio_read, int, (struct aiocb *aiocbp));
DARSHAN_FORWARD_DECL(aio_read64, int, (struct aiocb64 *aiocbp));
DARSHAN_FORWARD_DECL(aio_write, int, (struct aiocb *aiocbp));
DARSHAN_FORWARD_DECL(aio_write64, int, (struct aiocb64 *aiocbp));
DARSHAN_FORWARD_DECL(lio_listio, int, (int mode, struct aiocb *const aiocb_list[], int nitems, struct sigevent *sevp));
DARSHAN_FORWARD_DECL(lio_listio64, int, (int mode, struct aiocb64 *const aiocb_list[], int nitems, struct sigevent *sevp));
DARSHAN_FORWARD_DECL(aio_return, ssize_t, (struct aiocb *aiocbp));
DARSHAN_FORWARD_DECL(aio_return64, ssize_t, (struct aiocb64 *aiocbp));

pthread_mutex_t cp_mutex = PTHREAD_RECURSIVE_MUTEX_INITIALIZER_NP;
struct darshan_job_runtime* darshan_global_job = NULL;
static int my_rank = -1;
static struct stat64 cp_stat_buf;
static int darshan_mem_alignment = 1;

/* struct to track information about aio operations in flight */
struct darshan_aio_tracker
{
    double tm1;
    void *aiocbp;
    struct darshan_aio_tracker* next;
};

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

static double posix_wtime(void);

static void cp_access_counter(struct darshan_file_runtime* file, ssize_t size,     enum cp_counter_type type);

static struct darshan_file_ref* ref_by_handle(
    const void* handle,
    int handle_sz,
    enum darshan_handle_type handle_type);

static struct darshan_file_runtime* darshan_file_by_fd(int fd);
static void darshan_file_close_fd(int fd);
static struct darshan_file_runtime* darshan_file_by_name_setfd(const char* name, int fd);
static char* clean_path(const char* path);
static void darshan_aio_tracker_add(int fd, void *aiocbp);
static struct darshan_aio_tracker* darshan_aio_tracker_del(int fd, void *aiocbp);

#define CP_RECORD_WRITE(__ret, __fd, __count, __pwrite_flag, __pwrite_offset, __aligned, __stream_flag, __tm1, __tm2) do{ \
    size_t stride; \
    int64_t this_offset; \
    int64_t file_alignment; \
    struct darshan_file_runtime* file; \
    double __elapsed = __tm2-__tm1; \
    if(__ret < 0) break; \
    file = darshan_file_by_fd(__fd); \
    if(!file) break; \
    if(__pwrite_flag) \
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
    CP_INC(file, CP_BYTES_WRITTEN, __ret); \
    if(__stream_flag) \
        CP_INC(file, CP_POSIX_FWRITES, 1); \
    else \
        CP_INC(file, CP_POSIX_WRITES, 1); \
    CP_BUCKET_INC(file, CP_SIZE_WRITE_0_100, __ret); \
    cp_access_counter(file, stride, CP_COUNTER_STRIDE); \
    if(!__aligned) \
        CP_INC(file, CP_MEM_NOT_ALIGNED, 1); \
    if(file_alignment > 0 && (this_offset % file_alignment) != 0) \
        CP_INC(file, CP_FILE_NOT_ALIGNED, 1); \
    cp_access_counter(file, __ret, CP_COUNTER_ACCESS); \
    if(file->last_io_type == CP_READ) \
        CP_INC(file, CP_RW_SWITCHES, 1); \
    file->last_io_type = CP_WRITE; \
    CP_F_INC_NO_OVERLAP(file, __tm1, __tm2, file->last_posix_write_end, CP_F_POSIX_WRITE_TIME); \
    if(CP_F_VALUE(file, CP_F_WRITE_START_TIMESTAMP) == 0) \
        CP_F_SET(file, CP_F_WRITE_START_TIMESTAMP, __tm1); \
    CP_F_SET(file, CP_F_WRITE_END_TIMESTAMP, __tm2); \
    if(CP_F_VALUE(file, CP_F_MAX_WRITE_TIME) < __elapsed){ \
        CP_F_SET(file, CP_F_MAX_WRITE_TIME, __elapsed); \
        CP_SET(file, CP_MAX_WRITE_TIME_SIZE, __ret); } \
} while(0)

#define CP_RECORD_READ(__ret, __fd, __count, __pread_flag, __pread_offset, __aligned, __stream_flag, __tm1, __tm2) do{ \
    size_t stride; \
    int64_t this_offset; \
    struct darshan_file_runtime* file; \
    int64_t file_alignment; \
    double __elapsed = __tm2-__tm1; \
    if(__ret < 0) break; \
    file = darshan_file_by_fd(__fd); \
    if(!file) break; \
    if(__pread_flag)\
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
    CP_MAX(file, CP_MAX_BYTE_READ, (this_offset + __ret -1)); \
    file->offset = this_offset + __ret; \
    CP_INC(file, CP_BYTES_READ, __ret); \
    if(__stream_flag)\
        CP_INC(file, CP_POSIX_FREADS, 1); \
    else\
        CP_INC(file, CP_POSIX_READS, 1); \
    CP_BUCKET_INC(file, CP_SIZE_READ_0_100, __ret); \
    cp_access_counter(file, stride, CP_COUNTER_STRIDE); \
    if(!__aligned) \
        CP_INC(file, CP_MEM_NOT_ALIGNED, 1); \
    if(file_alignment > 0 && (this_offset % file_alignment) != 0) \
        CP_INC(file, CP_FILE_NOT_ALIGNED, 1); \
    cp_access_counter(file, __ret, CP_COUNTER_ACCESS); \
    if(file->last_io_type == CP_WRITE) \
        CP_INC(file, CP_RW_SWITCHES, 1); \
    file->last_io_type = CP_READ; \
    CP_F_INC_NO_OVERLAP(file, __tm1, __tm2, file->last_posix_read_end, CP_F_POSIX_READ_TIME); \
    if(CP_F_VALUE(file, CP_F_READ_START_TIMESTAMP) == 0) \
        CP_F_SET(file, CP_F_READ_START_TIMESTAMP, __tm1); \
    CP_F_SET(file, CP_F_READ_END_TIMESTAMP, __tm2); \
    if(CP_F_VALUE(file, CP_F_MAX_READ_TIME) < __elapsed){ \
        CP_F_SET(file, CP_F_MAX_READ_TIME, __elapsed); \
        CP_SET(file, CP_MAX_READ_TIME_SIZE, __ret); } \
} while(0)

#define CP_LOOKUP_RECORD_STAT(__path, __statbuf, __tm1, __tm2) do { \
    char* exclude; \
    int tmp_index = 0; \
    struct darshan_file_runtime* file; \
    while((exclude = exclusions[tmp_index])) { \
        if(!(strncmp(exclude, __path, strlen(exclude)))) \
            break; \
        tmp_index++; \
    } \
    if(exclude) break; \
    file = darshan_file_by_name(__path); \
    if (file) \
    { \
        CP_RECORD_STAT(file, __statbuf, __tm1, __tm2); \
    } \
} while(0)

    
#define CP_RECORD_STAT(__file, __statbuf, __tm1, __tm2) do { \
    if(!CP_VALUE((__file), CP_POSIX_STATS) && !CP_VALUE((__file), CP_POSIX_OPENS)){ \
        CP_SET((__file), CP_FILE_ALIGNMENT, (__statbuf)->st_blksize); \
        CP_SET((__file), CP_SIZE_AT_OPEN, (__statbuf)->st_size); \
    }\
    (__file)->log_file->rank = my_rank; \
    CP_F_INC_NO_OVERLAP(file, __tm1, __tm2, file->last_posix_meta_end, CP_F_POSIX_META_TIME); \
    CP_INC(__file, CP_POSIX_STATS, 1); \
} while(0)

#ifdef __CP_STAT_AT_OPEN
#define CP_STAT_FILE(_f, _p, _r) do { \
    if(!CP_VALUE((_f), CP_POSIX_STATS) && !CP_VALUE((_f), CP_POSIX_OPENS)){ \
        if(fstat64(_r, &cp_stat_buf) == 0) { \
            CP_SET(_f, CP_FILE_ALIGNMENT, cp_stat_buf.st_blksize); \
            CP_SET(_f, CP_SIZE_AT_OPEN, cp_stat_buf.st_size); \
        }\
    }\
}while(0)
#else
#define CP_STAT_FILE(_f, _p, _r) do { }while(0)
#endif

#define CP_RECORD_OPEN(__ret, __path, __mode, __stream_flag, __tm1, __tm2) do { \
    struct darshan_file_runtime* file; \
    char* exclude; \
    int tmp_index = 0; \
    if(__ret < 0) break; \
    while((exclude = exclusions[tmp_index])) { \
        if(!(strncmp(exclude, __path, strlen(exclude)))) \
            break; \
        tmp_index++; \
    } \
    if(exclude) break; \
    file = darshan_file_by_name_setfd(__path, __ret); \
    if(!file) break; \
    CP_STAT_FILE(file, __path, __ret); \
    file->log_file->rank = my_rank; \
    if(__mode) \
        CP_SET(file, CP_MODE, __mode); \
    file->offset = 0; \
    file->last_byte_written = 0; \
    file->last_byte_read = 0; \
    if(__stream_flag)\
        CP_INC(file, CP_POSIX_FOPENS, 1); \
    else \
        CP_INC(file, CP_POSIX_OPENS, 1); \
    if(CP_F_VALUE(file, CP_F_OPEN_TIMESTAMP) == 0) \
        CP_F_SET(file, CP_F_OPEN_TIMESTAMP, posix_wtime()); \
    CP_F_INC_NO_OVERLAP(file, __tm1, __tm2, file->last_posix_meta_end, CP_F_POSIX_META_TIME); \
} while (0)

int DARSHAN_DECL(close)(int fd)
{
    struct darshan_file_runtime* file;
    int tmp_fd = fd;
    double tm1, tm2;
    int ret;

    MAP_OR_FAIL(close);

    tm1 = darshan_wtime();
    ret = __real_close(fd);
    tm2 = darshan_wtime();

    CP_LOCK();
    file = darshan_file_by_fd(tmp_fd);
    if(file)
    {
        file->last_byte_written = 0;
        file->last_byte_read = 0;
        CP_F_SET(file, CP_F_CLOSE_TIMESTAMP, posix_wtime());
        CP_F_INC_NO_OVERLAP(file, tm1, tm2, file->last_posix_meta_end, CP_F_POSIX_META_TIME);
        darshan_file_close_fd(tmp_fd);
    }
    CP_UNLOCK();

    return(ret);
}

int DARSHAN_DECL(fclose)(FILE *fp)
{
    struct darshan_file_runtime* file;
    int tmp_fd = fileno(fp);
    double tm1, tm2;
    int ret;

    MAP_OR_FAIL(fclose);

    tm1 = darshan_wtime();
    ret = __real_fclose(fp);
    tm2 = darshan_wtime();
    
    CP_LOCK();
    file = darshan_file_by_fd(tmp_fd);
    if(file)
    {
        file->last_byte_written = 0;
        file->last_byte_read = 0;
        CP_F_SET(file, CP_F_CLOSE_TIMESTAMP, posix_wtime());
        CP_F_INC_NO_OVERLAP(file, tm1, tm2, file->last_posix_meta_end, CP_F_POSIX_META_TIME);
        darshan_file_close_fd(tmp_fd);
    }
    CP_UNLOCK();

    return(ret);
}


int DARSHAN_DECL(fsync)(int fd)
{
    int ret;
    struct darshan_file_runtime* file;
    double tm1, tm2;

    MAP_OR_FAIL(fsync);

    tm1 = darshan_wtime();
    ret = __real_fsync(fd);
    tm2 = darshan_wtime();

    if(ret < 0)
        return(ret);

    CP_LOCK();
    file = darshan_file_by_fd(fd);
    if(file)
    {
        CP_F_INC_NO_OVERLAP(file, tm1, tm2, file->last_posix_write_end, CP_F_POSIX_WRITE_TIME); \
        CP_INC(file, CP_POSIX_FSYNCS, 1);
    }
    CP_UNLOCK();

    return(ret);
}

int DARSHAN_DECL(fdatasync)(int fd)
{
    int ret;
    struct darshan_file_runtime* file;
    double tm1, tm2;

    MAP_OR_FAIL(fdatasync);

    tm1 = darshan_wtime();
    ret = __real_fdatasync(fd);
    tm2 = darshan_wtime();
    if(ret < 0)
        return(ret);

    CP_LOCK();
    file = darshan_file_by_fd(fd);
    if(file)
    {
        CP_F_INC_NO_OVERLAP(file, tm1, tm2, file->last_posix_write_end, CP_F_POSIX_WRITE_TIME); \
        CP_INC(file, CP_POSIX_FDSYNCS, 1);
    }
    CP_UNLOCK();

    return(ret);
}


void* DARSHAN_DECL(mmap64)(void *addr, size_t length, int prot, int flags,
    int fd, off64_t offset)
{
    void* ret;
    struct darshan_file_runtime* file;

    MAP_OR_FAIL(mmap64);

    ret = __real_mmap64(addr, length, prot, flags, fd, offset);
    if(ret == MAP_FAILED)
        return(ret);

    CP_LOCK();
    file = darshan_file_by_fd(fd);
    if(file)
    {
        CP_INC(file, CP_POSIX_MMAPS, 1);
    }
    CP_UNLOCK();

    return(ret);
}


void* DARSHAN_DECL(mmap)(void *addr, size_t length, int prot, int flags,
    int fd, off_t offset)
{
    void* ret;
    struct darshan_file_runtime* file;

    MAP_OR_FAIL(mmap);

    ret = __real_mmap(addr, length, prot, flags, fd, offset);
    if(ret == MAP_FAILED)
        return(ret);

    CP_LOCK();
    file = darshan_file_by_fd(fd);
    if(file)
    {
        CP_INC(file, CP_POSIX_MMAPS, 1);
    }
    CP_UNLOCK();

    return(ret);
}

int DARSHAN_DECL(creat)(const char* path, mode_t mode)
{
    int ret;
    double tm1, tm2;

    MAP_OR_FAIL(creat);

    tm1 = darshan_wtime();
    ret = __real_creat(path, mode);
    tm2 = darshan_wtime();

    CP_LOCK();
    CP_RECORD_OPEN(ret, path, mode, 0, tm1, tm2);
    CP_UNLOCK();

    return(ret);
}

int DARSHAN_DECL(creat64)(const char* path, mode_t mode)
{
    int ret;
    double tm1, tm2;

    MAP_OR_FAIL(creat64);

    tm1 = darshan_wtime();
    ret = __real_creat64(path, mode);
    tm2 = darshan_wtime();

    CP_LOCK();
    CP_RECORD_OPEN(ret, path, mode, 0, tm1, tm2);
    CP_UNLOCK();

    return(ret);
}

int DARSHAN_DECL(open64)(const char* path, int flags, ...)
{
    int mode = 0;
    int ret;
    double tm1, tm2;

    MAP_OR_FAIL(open64);

    if (flags & O_CREAT) 
    {
        va_list arg;
        va_start(arg, flags);
        mode = va_arg(arg, int);
        va_end(arg);

        tm1 = darshan_wtime();
        ret = __real_open64(path, flags, mode);
        tm2 = darshan_wtime();
    }
    else
    {
        tm1 = darshan_wtime();
        ret = __real_open64(path, flags);
        tm2 = darshan_wtime();
    }

    CP_LOCK();
    CP_RECORD_OPEN(ret, path, mode, 0, tm1, tm2);
    CP_UNLOCK();

    return(ret);
}

int DARSHAN_DECL(open)(const char *path, int flags, ...)
{
    int mode = 0;
    int ret;
    double tm1, tm2;

    MAP_OR_FAIL(open);

    if (flags & O_CREAT) 
    {
        va_list arg;
        va_start(arg, flags);
        mode = va_arg(arg, int);
        va_end(arg);

        tm1 = darshan_wtime();
        ret = __real_open(path, flags, mode);
        tm2 = darshan_wtime();
    }
    else
    {
        tm1 = darshan_wtime();
        ret = __real_open(path, flags);
        tm2 = darshan_wtime();
    }

    CP_LOCK();
    CP_RECORD_OPEN(ret, path, mode, 0, tm1, tm2);
    CP_UNLOCK();

    return(ret);
}

FILE* DARSHAN_DECL(fopen64)(const char *path, const char *mode)
{
    FILE* ret;
    int fd;
    double tm1, tm2;

    MAP_OR_FAIL(fopen64);

    tm1 = darshan_wtime();
    ret = __real_fopen64(path, mode);
    tm2 = darshan_wtime();
    if(ret == 0)
        fd = -1;
    else
        fd = fileno(ret);

    CP_LOCK();
    CP_RECORD_OPEN(fd, path, 0, 1, tm1, tm2);
    CP_UNLOCK();

    return(ret);
}

FILE* DARSHAN_DECL(fopen)(const char *path, const char *mode)
{
    FILE* ret;
    int fd;
    double tm1, tm2;

    MAP_OR_FAIL(fopen);

    tm1 = darshan_wtime();
    ret = __real_fopen(path, mode);
    tm2 = darshan_wtime();
    if(ret == 0)
        fd = -1;
    else
        fd = fileno(ret);

    CP_LOCK();
    CP_RECORD_OPEN(fd, path, 0, 1, tm1, tm2);
    CP_UNLOCK();

    return(ret);
}

int DARSHAN_DECL(__xstat64)(int vers, const char *path, struct stat64 *buf)
{
    int ret;
    double tm1, tm2;

    MAP_OR_FAIL(__xstat64);

    tm1 = darshan_wtime();
    ret = __real___xstat64(vers, path, buf);
    tm2 = darshan_wtime();
    if(ret < 0 || !S_ISREG(buf->st_mode))
        return(ret);

    CP_LOCK();
    CP_LOOKUP_RECORD_STAT(path, buf, tm1, tm2);
    CP_UNLOCK();

    return(ret);
}

int DARSHAN_DECL(__lxstat64)(int vers, const char *path, struct stat64 *buf)
{
    int ret;
    double tm1, tm2;

    MAP_OR_FAIL(__lxstat64);

    tm1 = darshan_wtime();
    ret = __real___lxstat64(vers, path, buf);
    tm2 = darshan_wtime();
    if(ret < 0 || !S_ISREG(buf->st_mode))
        return(ret);

    CP_LOCK();
    CP_LOOKUP_RECORD_STAT(path, buf, tm1, tm2);
    CP_UNLOCK();

    return(ret);
}

int DARSHAN_DECL(__fxstat64)(int vers, int fd, struct stat64 *buf)
{
    int ret;
    struct darshan_file_runtime* file;
    double tm1, tm2;

    MAP_OR_FAIL(__fxstat64);

    tm1 = darshan_wtime();
    ret = __real___fxstat64(vers, fd, buf);
    tm2 = darshan_wtime();
    if(ret < 0 || !S_ISREG(buf->st_mode))
        return(ret);

    /* skip logging if this was triggered internally */
    if(buf == &cp_stat_buf)
        return(ret);

    CP_LOCK();
    file = darshan_file_by_fd(fd);
    if(file)
    {
        CP_RECORD_STAT(file, buf, tm1, tm2);
    }
    CP_UNLOCK();

    return(ret);
}


int DARSHAN_DECL(__xstat)(int vers, const char *path, struct stat *buf)
{
    int ret;
    double tm1, tm2;

    MAP_OR_FAIL(__xstat);

    tm1 = darshan_wtime();
    ret = __real___xstat(vers, path, buf);
    tm2 = darshan_wtime();
    if(ret < 0 || !S_ISREG(buf->st_mode))
        return(ret);

    CP_LOCK();
    CP_LOOKUP_RECORD_STAT(path, buf, tm1, tm2);
    CP_UNLOCK();

    return(ret);
}

int DARSHAN_DECL(__lxstat)(int vers, const char *path, struct stat *buf)
{
    int ret;
    double tm1, tm2;

    MAP_OR_FAIL(__lxstat);

    tm1 = darshan_wtime();
    ret = __real___lxstat(vers, path, buf);
    tm2 = darshan_wtime();
    if(ret < 0 || !S_ISREG(buf->st_mode))
        return(ret);

    CP_LOCK();
    CP_LOOKUP_RECORD_STAT(path, buf, tm1, tm2);
    CP_UNLOCK();

    return(ret);
}

int DARSHAN_DECL(__fxstat)(int vers, int fd, struct stat *buf)
{
    int ret;
    struct darshan_file_runtime* file;
    double tm1, tm2;

    MAP_OR_FAIL(__fxstat);

    tm1 = darshan_wtime();
    ret = __real___fxstat(vers, fd, buf);
    tm2 = darshan_wtime();
    if(ret < 0 || !S_ISREG(buf->st_mode))
        return(ret);

    /* skip logging if this was triggered internally */
    if((void*)buf == (void*)&cp_stat_buf)
        return(ret);

    CP_LOCK();
    file = darshan_file_by_fd(fd);
    if(file)
    {
        CP_RECORD_STAT(file, buf, tm1, tm2);
    }
    CP_UNLOCK();

    return(ret);
}

ssize_t DARSHAN_DECL(pread64)(int fd, void *buf, size_t count, off64_t offset)
{
    ssize_t ret;
    int aligned_flag = 0;
    double tm1, tm2;

    MAP_OR_FAIL(pread64);

    if((unsigned long)buf % darshan_mem_alignment == 0)
        aligned_flag = 1;

    tm1 = darshan_wtime();
    ret = __real_pread64(fd, buf, count, offset);
    tm2 = darshan_wtime();
    CP_LOCK();
    CP_RECORD_READ(ret, fd, count, 1, offset, aligned_flag, 0, tm1, tm2);
    CP_UNLOCK();
    return(ret);
}

ssize_t DARSHAN_DECL(pread)(int fd, void *buf, size_t count, off_t offset)
{
    ssize_t ret;
    int aligned_flag = 0;
    double tm1, tm2;

    MAP_OR_FAIL(pread);

    if((unsigned long)buf % darshan_mem_alignment == 0)
        aligned_flag = 1;

    tm1 = darshan_wtime();
    ret = __real_pread(fd, buf, count, offset);
    tm2 = darshan_wtime();
    CP_LOCK();
    CP_RECORD_READ(ret, fd, count, 1, offset, aligned_flag, 0, tm1, tm2);
    CP_UNLOCK();
    return(ret);
}


ssize_t DARSHAN_DECL(pwrite)(int fd, const void *buf, size_t count, off_t offset)
{
    ssize_t ret;
    int aligned_flag = 0;
    double tm1, tm2;

    MAP_OR_FAIL(pwrite);

    if((unsigned long)buf % darshan_mem_alignment == 0)
        aligned_flag = 1;

    tm1 = darshan_wtime();
    ret = __real_pwrite(fd, buf, count, offset);
    tm2 = darshan_wtime();
    CP_LOCK();
    CP_RECORD_WRITE(ret, fd, count, 1, offset, aligned_flag, 0, tm1, tm2);
    CP_UNLOCK();
    return(ret);
}

ssize_t DARSHAN_DECL(pwrite64)(int fd, const void *buf, size_t count, off64_t offset)
{
    ssize_t ret;
    int aligned_flag = 0;
    double tm1, tm2;

    MAP_OR_FAIL(pwrite64);

    if((unsigned long)buf % darshan_mem_alignment == 0)
        aligned_flag = 1;

    tm1 = darshan_wtime();
    ret = __real_pwrite64(fd, buf, count, offset);
    tm2 = darshan_wtime();
    CP_LOCK();
    CP_RECORD_WRITE(ret, fd, count, 1, offset, aligned_flag, 0, tm1, tm2);
    CP_UNLOCK();
    return(ret);
}

ssize_t DARSHAN_DECL(readv)(int fd, const struct iovec *iov, int iovcnt)
{
    ssize_t ret;
    int aligned_flag = 1;
    int i;
    double tm1, tm2;

    MAP_OR_FAIL(readv);

    for(i=0; i<iovcnt; i++)
    {
        if(((unsigned long)iov[i].iov_base % darshan_mem_alignment) != 0)
            aligned_flag = 0;
    }

    tm1 = darshan_wtime();
    ret = __real_readv(fd, iov, iovcnt);
    tm2 = darshan_wtime();
    CP_LOCK();
    CP_RECORD_READ(ret, fd, count, 0, 0, aligned_flag, 0, tm1, tm2);
    CP_UNLOCK();
    return(ret);
}

ssize_t DARSHAN_DECL(writev)(int fd, const struct iovec *iov, int iovcnt)
{
    ssize_t ret;
    int aligned_flag = 1;
    int i;
    double tm1, tm2;

    MAP_OR_FAIL(writev);

    for(i=0; i<iovcnt; i++)
    {
        if(!((unsigned long)iov[i].iov_base % darshan_mem_alignment == 0))
            aligned_flag = 0;
    }

    tm1 = darshan_wtime();
    ret = __real_writev(fd, iov, iovcnt);
    tm2 = darshan_wtime();
    CP_LOCK();
    CP_RECORD_WRITE(ret, fd, count, 0, 0, aligned_flag, 0, tm1, tm2);
    CP_UNLOCK();
    return(ret);
}

size_t DARSHAN_DECL(fread)(void *ptr, size_t size, size_t nmemb, FILE *stream)
{
    size_t ret;
    int aligned_flag = 0;
    double tm1, tm2;

    MAP_OR_FAIL(fread);

    if((unsigned long)ptr % darshan_mem_alignment == 0)
        aligned_flag = 1;

    tm1 = darshan_wtime();
    ret = __real_fread(ptr, size, nmemb, stream);
    tm2 = darshan_wtime();
    CP_LOCK();
    if(ret > 0)
        CP_RECORD_READ(size*ret, fileno(stream), (size*nmemb), 0, 0, aligned_flag, 1, tm1, tm2);
    else
        CP_RECORD_READ(ret, fileno(stream), (size*nmemb), 0, 0, aligned_flag, 1, tm1, tm2);
    CP_UNLOCK();
    return(ret);
}

ssize_t DARSHAN_DECL(read)(int fd, void *buf, size_t count)
{
    ssize_t ret;
    int aligned_flag = 0;
    double tm1, tm2;

    MAP_OR_FAIL(read);

    if((unsigned long)buf % darshan_mem_alignment == 0)
        aligned_flag = 1;

    tm1 = darshan_wtime();
    ret = __real_read(fd, buf, count);
    tm2 = darshan_wtime();
    CP_LOCK();
    CP_RECORD_READ(ret, fd, count, 0, 0, aligned_flag, 0, tm1, tm2);
    CP_UNLOCK();
    return(ret);
}

ssize_t DARSHAN_DECL(write)(int fd, const void *buf, size_t count)
{
    ssize_t ret;
    int aligned_flag = 0;
    double tm1, tm2;

    MAP_OR_FAIL(write);

    if((unsigned long)buf % darshan_mem_alignment == 0)
        aligned_flag = 1;

    tm1 = darshan_wtime();
    ret = __real_write(fd, buf, count);
    tm2 = darshan_wtime();
    CP_LOCK();
    CP_RECORD_WRITE(ret, fd, count, 0, 0, aligned_flag, 0, tm1, tm2);
    CP_UNLOCK();
    return(ret);
}

size_t DARSHAN_DECL(fwrite)(const void *ptr, size_t size, size_t nmemb, FILE *stream)
{
    size_t ret;
    int aligned_flag = 0;
    double tm1, tm2;

    MAP_OR_FAIL(fwrite);

    if((unsigned long)ptr % darshan_mem_alignment == 0)
        aligned_flag = 1;

    tm1 = darshan_wtime();
    ret = __real_fwrite(ptr, size, nmemb, stream);
    tm2 = darshan_wtime();
    CP_LOCK();
    if(ret > 0)
        CP_RECORD_WRITE(size*ret, fileno(stream), (size*nmemb), 0, 0, aligned_flag, 1, tm1, tm2);
    else
        CP_RECORD_WRITE(ret, fileno(stream), 0, 0, 0, aligned_flag, 1, tm1, tm2);
    CP_UNLOCK();
    return(ret);
}

off64_t DARSHAN_DECL(lseek64)(int fd, off64_t offset, int whence)
{
    off64_t ret;
    struct darshan_file_runtime* file;
    double tm1, tm2;

    MAP_OR_FAIL(lseek64);

    tm1 = darshan_wtime();
    ret = __real_lseek64(fd, offset, whence);
    tm2 = darshan_wtime();
    if(ret >= 0)
    {
        CP_LOCK();
        file = darshan_file_by_fd(fd);
        if(file)
        {
            file->offset = ret;
            CP_F_INC_NO_OVERLAP(file, tm1, tm2, file->last_posix_meta_end, CP_F_POSIX_META_TIME);
            CP_INC(file, CP_POSIX_SEEKS, 1);
        }
        CP_UNLOCK();
    }
    return(ret);
}

off_t DARSHAN_DECL(lseek)(int fd, off_t offset, int whence)
{
    off_t ret;
    struct darshan_file_runtime* file;
    double tm1, tm2;

    MAP_OR_FAIL(lseek);

    tm1 = darshan_wtime();
    ret = __real_lseek(fd, offset, whence);
    tm2 = darshan_wtime();
    if(ret >= 0)
    {
        CP_LOCK();
        file = darshan_file_by_fd(fd);
        if(file)
        {
            file->offset = ret;
            CP_F_INC_NO_OVERLAP(file, tm1, tm2, file->last_posix_meta_end, CP_F_POSIX_META_TIME);
            CP_INC(file, CP_POSIX_SEEKS, 1);
        }
        CP_UNLOCK();
    }
    return(ret);
}

ssize_t DARSHAN_DECL(aio_return64)(struct aiocb64 *aiocbp)
{
    int ret;
    double tm2;
    struct darshan_aio_tracker *tmp;
    int aligned_flag = 0;

    MAP_OR_FAIL(aio_return64);

    ret = __real_aio_return64(aiocbp);
    tm2 = darshan_wtime();
    tmp = darshan_aio_tracker_del(aiocbp->aio_fildes, aiocbp);

    if(tmp)
    {
        if((unsigned long)aiocbp->aio_buf % darshan_mem_alignment == 0)
            aligned_flag = 1;
        CP_LOCK();
        if(aiocbp->aio_lio_opcode == LIO_WRITE)
        {
            CP_RECORD_WRITE(ret, aiocbp->aio_fildes, aiocbp->aio_nbytes,
                1, aiocbp->aio_offset, aligned_flag, 0, tmp->tm1, tm2);
        }
        if(aiocbp->aio_lio_opcode == LIO_READ)
        {
            CP_RECORD_READ(ret, aiocbp->aio_fildes, aiocbp->aio_nbytes,
                1, aiocbp->aio_offset, aligned_flag, 0, tmp->tm1, tm2);
        }
        CP_UNLOCK();
        free(tmp);
    }

    return(ret);
}

ssize_t DARSHAN_DECL(aio_return)(struct aiocb *aiocbp)
{
    int ret;
    double tm2;
    struct darshan_aio_tracker *tmp;
    int aligned_flag = 0;

    MAP_OR_FAIL(aio_return);

    ret = __real_aio_return(aiocbp);
    tm2 = darshan_wtime();
    tmp = darshan_aio_tracker_del(aiocbp->aio_fildes, aiocbp);

    if(tmp)
    {
        if((unsigned long)aiocbp->aio_buf % darshan_mem_alignment == 0)
            aligned_flag = 1;
        CP_LOCK();
        CP_RECORD_WRITE(ret, aiocbp->aio_fildes, aiocbp->aio_nbytes,
            1, aiocbp->aio_offset, aligned_flag, 0, tmp->tm1, tm2);
        CP_UNLOCK();
        free(tmp);
    }

    return(ret);
}

int DARSHAN_DECL(lio_listio)(int mode, struct aiocb *const aiocb_list[],
    int nitems, struct sigevent *sevp)
{
    int ret;
    int i;

    MAP_OR_FAIL(lio_listio);

    ret = __real_lio_listio(mode, aiocb_list, nitems, sevp);
    if(ret == 0)
    {
        for(i=0; i<nitems; i++)
        {
            darshan_aio_tracker_add(aiocb_list[i]->aio_fildes, aiocb_list[i]);        
        }
    }

    return(ret);
}

int DARSHAN_DECL(lio_listio64)(int mode, struct aiocb64 *const aiocb_list[],
    int nitems, struct sigevent *sevp)
{
    int ret;
    int i;

    MAP_OR_FAIL(lio_listio);

    ret = __real_lio_listio64(mode, aiocb_list, nitems, sevp);
    if(ret == 0)
    {
        for(i=0; i<nitems; i++)
        {
            darshan_aio_tracker_add(aiocb_list[i]->aio_fildes, aiocb_list[i]);        
        }
    }

    return(ret);
}

int DARSHAN_DECL(aio_write64)(struct aiocb64 *aiocbp)
{
    int ret;

    MAP_OR_FAIL(aio_write64);

    ret = __real_aio_write64(aiocbp);
    if(ret == 0)
        darshan_aio_tracker_add(aiocbp->aio_fildes, aiocbp);

    return(ret);
}

int DARSHAN_DECL(aio_write)(struct aiocb *aiocbp)
{
    int ret;

    MAP_OR_FAIL(aio_write);

    ret = __real_aio_write(aiocbp);
    if(ret == 0)
        darshan_aio_tracker_add(aiocbp->aio_fildes, aiocbp);

    return(ret);
}

int DARSHAN_DECL(aio_read64)(struct aiocb64 *aiocbp)
{
    int ret;

    MAP_OR_FAIL(aio_read64);

    ret = __real_aio_read64(aiocbp);
    if(ret == 0)
        darshan_aio_tracker_add(aiocbp->aio_fildes, aiocbp);

    return(ret);
}

int DARSHAN_DECL(aio_read)(struct aiocb *aiocbp)
{
    int ret;

    MAP_OR_FAIL(aio_read);

    ret = __real_aio_read(aiocbp);
    if(ret == 0)
        darshan_aio_tracker_add(aiocbp->aio_fildes, aiocbp);

    return(ret);
}

int DARSHAN_DECL(fseek)(FILE *stream, long offset, int whence)
{
    int ret;
    struct darshan_file_runtime* file;
    double tm1, tm2;

    MAP_OR_FAIL(fseek);

    tm1 = darshan_wtime();
    ret = __real_fseek(stream, offset, whence);
    tm2 = darshan_wtime();
    if(ret >= 0)
    {
        CP_LOCK();
        file = darshan_file_by_fd(fileno(stream));
        if(file)
        {
            file->offset = ret;
            CP_F_INC_NO_OVERLAP(file, tm1, tm2, file->last_posix_meta_end, CP_F_POSIX_META_TIME);
            CP_INC(file, CP_POSIX_FSEEKS, 1);
        }
        CP_UNLOCK();
    }
    return(ret);
}

void darshan_finalize(struct darshan_job_runtime* job)
{
    if(!job)
    {
        return;
    }

    free(job);
}

void darshan_initialize(int argc, char** argv,  int nprocs, int rank)
{
    int i;
    char* disable;
    char* disable_timing;
    char* envstr;
    char* truncate_string = "<TRUNCATED>";
    int truncate_offset;
    int chars_left = 0;
    int ret;
    int tmpval;

    disable = getenv("DARSHAN_DISABLE");
    if(disable)
    {
        /* turn off tracing */
        return;
    }

    disable_timing = getenv("DARSHAN_DISABLE_TIMING");

    if(darshan_global_job != NULL)
    {
        return;
    }

    #if (__CP_MEM_ALIGNMENT < 1)
        #error Darshan must be configured with a positive value for --with-mem-align
    #endif
    envstr = getenv("DARSHAN_MEMALIGN");
    if (envstr)
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
        darshan_mem_alignment = __CP_MEM_ALIGNMENT;
    }

    /* avoid floating point errors on faulty input */
    if (darshan_mem_alignment < 1)
    {
        darshan_mem_alignment = 1;
    }

    /* allocate structure to track darshan_global_job information */
    darshan_global_job = malloc(sizeof(*darshan_global_job));
    if(!darshan_global_job)
    {
        return;
    }
    memset(darshan_global_job, 0, sizeof(*darshan_global_job));

    if(disable_timing)
    {
        darshan_global_job->flags |= CP_FLAG_NOTIMING;
    }

    /* set up file records */
    for(i=0; i<CP_MAX_FILES; i++)
    {
        darshan_global_job->file_runtime_array[i].log_file = 
            &darshan_global_job->file_array[i];
    }

    strcpy(darshan_global_job->log_job.version_string, CP_VERSION);
    darshan_global_job->log_job.magic_nr = CP_MAGIC_NR;
    darshan_global_job->log_job.uid = getuid();
    darshan_global_job->log_job.start_time = time(NULL);
    darshan_global_job->log_job.nprocs = nprocs;
    darshan_global_job->wtime_offset = posix_wtime();
    my_rank = rank;

    /* record exe and arguments */
    for(i=0; i<argc; i++)
    {
        chars_left = CP_EXE_LEN-strlen(darshan_global_job->exe);
        strncat(darshan_global_job->exe, argv[i], chars_left);
        if(i < (argc-1))
        {
            chars_left = CP_EXE_LEN-strlen(darshan_global_job->exe);
            strncat(darshan_global_job->exe, " ", chars_left);
        }
    }

    /* if we don't see any arguments, then use glibc symbol to get
     * program name at least (this happens in fortran)
     */
    if(argc == 0)
    {
        chars_left = CP_EXE_LEN-strlen(darshan_global_job->exe);
        strncat(darshan_global_job->exe, __progname_full, chars_left);
        chars_left = CP_EXE_LEN-strlen(darshan_global_job->exe);
        strncat(darshan_global_job->exe, " <unknown args>", chars_left);
    }

    if(chars_left == 0)
    {
        /* we ran out of room; mark that string was truncated */
        truncate_offset = CP_EXE_LEN - strlen(truncate_string);
        sprintf(&darshan_global_job->exe[truncate_offset], "%s", 
            truncate_string);
    }

    /* collect information about command line and 
     * mounted file systems 
     */
    darshan_global_job->trailing_data = 
        darshan_get_exe_and_mounts(darshan_global_job);

    return;
}

/* darshan_condense()
 *
 * collapses all file statistics into a single unified set of counters; used
 * when we have opened too many files to track independently
 */
void darshan_condense(void)
{
    struct darshan_file_runtime* base_file;
    struct darshan_file_runtime* iter_file;
    int i;
    int j;

    if(!darshan_global_job)
        return;

    base_file = &darshan_global_job->file_runtime_array[0];

    /* iterate through files */
    for(j=1; j<darshan_global_job->file_count; j++)
    {
        iter_file = &darshan_global_job->file_runtime_array[j];

        /* iterate through records */
        for(i=0; i<CP_NUM_INDICES; i++)
        {
            switch(i)
            {
                /* NOTE: several fields cease to make sense if the records
                 * have been condensed.  Just let them get summed anyway.
                 */
                /* TODO: double check this */

                /* keep up with global maxes in case they are helpful */
                case CP_MAX_BYTE_READ:
                case CP_MAX_BYTE_WRITTEN:
                    CP_MAX(base_file, i, CP_VALUE(iter_file, i));
                    break;

                /* do nothing with these; they are handled in the floating
                 * point loop 
                 */
                case CP_MAX_WRITE_TIME_SIZE:
                case CP_MAX_READ_TIME_SIZE:
                    break;

                /* pick one */
                case CP_DEVICE:
                case CP_SIZE_AT_OPEN:
                    CP_SET(base_file, i, CP_VALUE(iter_file, i));
                    break;

                /* most records can simply be added */
                default:
                    CP_INC(base_file, i, CP_VALUE(iter_file, i));
                    break;
            }
        }
        for(i=0; i<CP_F_NUM_INDICES; i++)
        {
            switch(i)
            {
                case CP_F_MAX_WRITE_TIME:
                    if(CP_F_VALUE(iter_file, i) > CP_F_VALUE(base_file, i))
                    {
                        CP_F_SET(base_file, i, CP_F_VALUE(iter_file, i));
                        CP_SET(base_file, CP_MAX_WRITE_TIME_SIZE, 
                            CP_VALUE(iter_file, CP_MAX_WRITE_TIME_SIZE));
                    }
                    break;
                case CP_F_MAX_READ_TIME:
                    if(CP_F_VALUE(iter_file, i) > CP_F_VALUE(base_file, i))
                    {
                        CP_F_SET(base_file, i, CP_F_VALUE(iter_file, i));
                        CP_SET(base_file, CP_MAX_READ_TIME_SIZE, 
                            CP_VALUE(iter_file, CP_MAX_READ_TIME_SIZE));
                    }
                    break;
                default:
                    CP_F_SET(base_file, i, CP_F_VALUE(iter_file, i) + CP_F_VALUE(base_file, i));
                    break;
            }
        }

        if(base_file->aio_list_tail)
        {
            /* base has an aio list already; add on to it */
            assert(base_file->aio_list_head);
            base_file->aio_list_tail->next = iter_file->aio_list_head;
            if(iter_file->aio_list_tail)
                base_file->aio_list_tail = iter_file->aio_list_tail;
        }
        else
        {
            /* take on list from iter */
            base_file->aio_list_head = iter_file->aio_list_head;
            base_file->aio_list_tail = iter_file->aio_list_tail;
        }
    }
    
    base_file->log_file->hash = 0;
    
    darshan_global_job->flags |= CP_FLAG_CONDENSED;
    darshan_global_job->file_count = 1;

    /* clear hash tables for safety */
    memset(darshan_global_job->name_table, 0, CP_HASH_SIZE*sizeof(struct darshan_file_runtime*));
    memset(darshan_global_job->handle_table, 0, CP_HASH_SIZE*sizeof(*darshan_global_job->handle_table));
    
    return;
}

static struct darshan_file_runtime* darshan_file_by_name_setfd(const char* name, int fd)
{
    struct darshan_file_runtime* tmp_file;

    tmp_file = darshan_file_by_name_sethandle(name, &fd, sizeof(fd), DARSHAN_FD);
    return(tmp_file);
}

static void darshan_file_close_fd(int fd)
{
    darshan_file_closehandle(&fd, sizeof(fd), DARSHAN_FD);
    return;
}

static struct darshan_file_runtime* darshan_file_by_fd(int fd)
{
    struct darshan_file_runtime* tmp_file;

    tmp_file = darshan_file_by_handle(&fd, sizeof(fd), DARSHAN_FD);
    
    return(tmp_file);
}

static int access_comparison(const void* a_p, const void* b_p)
{
    const struct cp_access_counter* a = a_p;
    const struct cp_access_counter* b = b_p;

    if(a->size < b->size)
        return(-1);
    if(a->size > b->size)
        return(1);
    return(0);
}

/* cp_access_counter()
 *
 * records the occurance of a particular access size for a file,
 * current implementation uses glibc red black tree
 */
static void cp_access_counter(struct darshan_file_runtime* file, ssize_t size, enum cp_counter_type type)
{
    struct cp_access_counter* counter;
    struct cp_access_counter* found;
    void* tmp;
    void** root;
    int* count;
    struct cp_access_counter tmp_counter;

    /* don't count sizes or strides of 0 */
    if(size == 0)
        return;
    
    switch(type)
    {
        case CP_COUNTER_ACCESS:
            root = &file->access_root;
            count = &file->access_count;
            break;
        case CP_COUNTER_STRIDE:
            root = &file->stride_root;
            count = &file->stride_count;
            break;
        default:
            return;
    }

    /* check to see if this size is already recorded */
    tmp_counter.size = size;
    tmp_counter.freq = 1;
    tmp = tfind(&tmp_counter, root, access_comparison);
    if(tmp)
    {
        found = *(struct cp_access_counter**)tmp;
        found->freq++;
        return;
    }

    /* we can add a new one as long as we haven't hit the limit */
    if(*count < CP_MAX_ACCESS_COUNT_RUNTIME)
    {
        counter = malloc(sizeof(*counter));
        if(!counter)
        {
            return;
        }

        counter->size = size;
        counter->freq = 1;

        tmp = tsearch(counter, root, access_comparison);
        found = *(struct cp_access_counter**)tmp;
        /* if we get a new answer out here we are in trouble; this was
         * already checked with the tfind()
         */
        assert(found == counter);

        (*count)++;
    }

    return;
}

/* NOTE: we disable internal benchmarking routines when building shared
 * libraries so that when Darshan is loaded with LD_PRELOAD it does not
 * depend on MPI routines.
 */
#ifndef DARSHAN_PRELOAD
void darshan_shutdown_bench(int argc, char** argv, int rank, int nprocs)
{
    int* fd_array;
    int64_t* size_array;
    int i;
    int nfiles;
    char path[256];
    int iters;
    
    /* combinations to build:
     * - 1 unique file per proc
     * - 1 shared file per proc
     * - 1024 unique file per proc
     * - 1024 shared per proc
     */

    srand(rank);
    fd_array = malloc(sizeof(int)*CP_MAX_FILES);
    size_array = malloc(sizeof(int64_t)*CP_MAX_ACCESS_COUNT_RUNTIME);

    assert(fd_array&&size_array);

    for(i=0; i<CP_MAX_FILES; i++)
        fd_array[i] = i;
    for(i=0; i<CP_MAX_ACCESS_COUNT_RUNTIME; i++)
        size_array[i] = rand();

    /* clear out existing stuff */
    darshan_walk_file_accesses(darshan_global_job);
    darshan_finalize(darshan_global_job);
    darshan_global_job = NULL;

    /***********************************************************/
    /* reset darshan to start clean */
    darshan_initialize(argc, argv, nprocs, rank);

    /* populate one unique file per proc */
    nfiles = 1;
    iters = 1;
    for(i=0; i<nfiles; i++)
    {
        sprintf(path, "%d-%d", i, rank);
        CP_RECORD_OPEN(fd_array[i], path, 777, 0, 0, 0);
    }

    for(i=0; i<iters; i++)
    {
        CP_RECORD_WRITE(size_array[(i/nfiles)%CP_MAX_ACCESS_COUNT_RUNTIME], fd_array[i%nfiles], size_array[(i/nfiles)%CP_MAX_ACCESS_COUNT_RUNTIME], 0, 0, 1, 0, 1, 2);
    }

    if(rank == 0)
        printf("# 1 unique file per proc\n");
    DARSHAN_MPI_CALL(PMPI_Barrier)(MPI_COMM_WORLD);
    darshan_shutdown(1);
    darshan_global_job = NULL;

    /***********************************************************/
    /* reset darshan to start clean */
    sleep(1);
    darshan_initialize(argc, argv, nprocs, rank);

    /* populate one shared file per proc */
    nfiles = 1;
    iters = 1;
    for(i=0; i<nfiles; i++)
    {
        sprintf(path, "%d", i);
        CP_RECORD_OPEN(fd_array[i], path, 777, 0, 0, 0);
    }

    for(i=0; i<iters; i++)
    {
        CP_RECORD_WRITE(size_array[(i/nfiles)%CP_MAX_ACCESS_COUNT_RUNTIME], fd_array[i%nfiles], size_array[(i/nfiles)%CP_MAX_ACCESS_COUNT_RUNTIME], 0, 0, 1, 0, 1, 2);
    }

    if(rank == 0)
        printf("# 1 shared file across procs\n");
    DARSHAN_MPI_CALL(PMPI_Barrier)(MPI_COMM_WORLD);
    darshan_shutdown(1);
    darshan_global_job = NULL;

    /***********************************************************/
    /* reset darshan to start clean */
    sleep(1);
    darshan_initialize(argc, argv, nprocs, rank);

    /* populate 1024 unique file per proc */
    nfiles = 1024;
    iters = 1024;
    for(i=0; i<nfiles; i++)
    {
        sprintf(path, "%d-%d", i, rank);
        CP_RECORD_OPEN(fd_array[i], path, 777, 0, 0, 0);
    }

    for(i=0; i<iters; i++)
    {
        CP_RECORD_WRITE(size_array[(i/nfiles)%CP_MAX_ACCESS_COUNT_RUNTIME], fd_array[i%nfiles], size_array[(i/nfiles)%CP_MAX_ACCESS_COUNT_RUNTIME], 0, 0, 1, 0, 1, 2);
    }

    if(rank == 0)
        printf("# 1024 unique files per proc\n");
    DARSHAN_MPI_CALL(PMPI_Barrier)(MPI_COMM_WORLD);
    darshan_shutdown(1);
    darshan_global_job = NULL;

    /***********************************************************/
    /* reset darshan to start clean */
    sleep(1);
    darshan_initialize(argc, argv, nprocs, rank);

    /* populate 1024 shared file per proc */
    nfiles = 1024;
    iters = 1024;
    for(i=0; i<nfiles; i++)
    {
        sprintf(path, "%d", i);
        CP_RECORD_OPEN(fd_array[i], path, 777, 0, 0, 0);
    }

    for(i=0; i<iters; i++)
    {
        CP_RECORD_WRITE(size_array[(i/nfiles)%CP_MAX_ACCESS_COUNT_RUNTIME], fd_array[i%nfiles], size_array[(i/nfiles)%CP_MAX_ACCESS_COUNT_RUNTIME], 0, 0, 1, 0, 1, 2);
    }

    if(rank == 0)
        printf("# 1024 shared files across procs\n");
    DARSHAN_MPI_CALL(PMPI_Barrier)(MPI_COMM_WORLD);
    darshan_shutdown(1);
    darshan_global_job = NULL;

    darshan_initialize(argc, argv, nprocs, rank);

    free(fd_array);
    free(size_array);

    return;
}
#endif

void darshan_search_bench(int argc, char** argv, int iters)
{
    int* fd_array;
    int64_t* size_array;
    int i,j;
    int skip = 32;
    int nfiles;
    char path[256];
    double tm1, tm2;
    
    fd_array = malloc(sizeof(int)*CP_MAX_FILES);
    size_array = malloc(sizeof(int64_t)*CP_MAX_ACCESS_COUNT_RUNTIME);

    assert(fd_array&&size_array);

    for(i=0; i<CP_MAX_FILES; i++)
        fd_array[i] = i;
    for(i=0; i<CP_MAX_ACCESS_COUNT_RUNTIME; i++)
        size_array[i] = rand();

    printf("#<iters>\t<numfiles>\t<numsizes>\t<total time>\t<per iter>\n");

    for(j=0; j<2; j++)
    {
        /* warm up */
        /* reset darshan to start clean */
        darshan_walk_file_accesses(darshan_global_job);
        darshan_finalize(darshan_global_job);
        darshan_global_job = NULL;
        darshan_initialize(argc, argv, 1, 0);

        nfiles = 1;
        /* populate entries for each file */
        for(i=0; i<nfiles; i++)
        {
            sprintf(path, "%d", i);
            CP_RECORD_OPEN(fd_array[i], path, 777, 0, 0, 0);
        }

        for(i=0; i<iters; i++)
        {
            if(j==0)
            {
                CP_RECORD_WRITE(size_array[(i/nfiles)%CP_MAX_ACCESS_COUNT_RUNTIME], fd_array[i%nfiles], size_array[(i/nfiles)%CP_MAX_ACCESS_COUNT_RUNTIME], 0, 0, 1, 0, 1, 2);
            }
            else
            {
                CP_RECORD_WRITE(size_array[0], fd_array[i%nfiles], size_array[0], 0, 0, 1, 0, 1, 2);
            }
        }

        /* real timing */
        for(nfiles=0; nfiles<=CP_MAX_FILES; nfiles += skip)
        {
            if(nfiles == 0)
                nfiles = 1;

            /* reset darshan to start clean */
            darshan_walk_file_accesses(darshan_global_job);
            darshan_finalize(darshan_global_job);
            darshan_global_job = NULL;
            darshan_initialize(argc, argv, 1, 0);

            /* populate entries for each file */
            for(i=0; i<nfiles; i++)
            {
                sprintf(path, "%d", i);
                CP_RECORD_OPEN(fd_array[i], path, 777, 0, 0, 0);
            }

            tm1 = darshan_wtime();
            for(i=0; i<iters; i++)
            {
                if(j==0)
                {
                    CP_RECORD_WRITE(size_array[(i/nfiles)%CP_MAX_ACCESS_COUNT_RUNTIME], fd_array[i%nfiles], size_array[(i/nfiles)%CP_MAX_ACCESS_COUNT_RUNTIME], 0, 0, 1, 0, 1, 2);
                }
                else
                {
                    CP_RECORD_WRITE(size_array[0], fd_array[i%nfiles], size_array[0], 0, 0, 1, 0, 1, 2);
                }
            }
            tm2 = darshan_wtime();

            /* printf("#<iters>\t<numfiles>\t<numsizes>\t<total time>\t<per iter>\n"); */
            printf("%d\t%d\t%d\t%f\t%.12f\n", iters, nfiles, (j==0?CP_MAX_ACCESS_COUNT_RUNTIME:1), tm2-tm1, (tm2-tm1)/iters);

            if(nfiles == 1)
                nfiles = 0;
        }
    }

    free(fd_array);
    free(size_array);
}

static double posix_wtime(void)
{
    return DARSHAN_MPI_CALL(PMPI_Wtime)();
}

double darshan_wtime(void)
{
    if(!darshan_global_job || darshan_global_job->flags & CP_FLAG_NOTIMING)
    {
        return(0);
    }
    
    return(posix_wtime());
}

struct darshan_file_runtime* darshan_file_by_name(const char* name)
{
    struct darshan_file_runtime* tmp_file;
    uint64_t tmp_hash = 0;
    char* suffix_pointer;
    int hash_index;
    char* newname = NULL;
    int64_t device_id;
    int64_t block_size;

    if(!darshan_global_job)
        return(NULL);

    /* if we have already condensed the data, then just hand the first file
     * back
     */
    if(darshan_global_job->flags & CP_FLAG_CONDENSED)
    {
        return(&darshan_global_job->file_runtime_array[0]);
    }

    /* try to construct absolute path */
    newname = clean_path(name);
    if(!newname)
        newname = (char*)name;

    tmp_hash = darshan_hash((void*)newname, strlen(newname), 0);

    /* search hash table */
    hash_index = tmp_hash & CP_HASH_MASK;
    tmp_file = darshan_global_job->name_table[hash_index];
    while(tmp_file)
    {
        if(tmp_file->log_file->hash == tmp_hash)
        {
            if(newname != name)
                free(newname);
            return(tmp_file);
        }
        tmp_file = tmp_file->name_next;
    }

    /* see if we need to condense */
    if(darshan_global_job->file_count >= CP_MAX_FILES)
    {
        darshan_condense();
        if(newname != name)
            free(newname);
        return(&darshan_global_job->file_runtime_array[0]);
    }

    /* new, unique file */
    tmp_file = &darshan_global_job->file_runtime_array[darshan_global_job->file_count];

    darshan_mnt_id_from_path(newname, &device_id, &block_size);
    CP_SET(tmp_file, CP_DEVICE, device_id);
    CP_SET(tmp_file, CP_FILE_ALIGNMENT, block_size);
    CP_SET(tmp_file, CP_MEM_ALIGNMENT, darshan_mem_alignment);
    tmp_file->log_file->hash = tmp_hash;

    /* record last N characters of file name too */
    suffix_pointer = (char*)newname;
    if(strlen(newname) > CP_NAME_SUFFIX_LEN)
    {
        suffix_pointer += (strlen(newname) - CP_NAME_SUFFIX_LEN);
    }
    strcpy(tmp_file->log_file->name_suffix, suffix_pointer); 

    /* if the "stat at open" functionality is disabled, then go ahead and
     * mark certain counters with invalid values to make sure that they are
     * not mis-interpretted.
     */
#ifndef __CP_STAT_AT_OPEN
    CP_SET(tmp_file, CP_SIZE_AT_OPEN, -1);
    if(CP_VALUE(tmp_file, CP_FILE_ALIGNMENT) == -1)
        CP_SET(tmp_file, CP_FILE_NOT_ALIGNED, -1);
#endif

    darshan_global_job->file_count++;

    /* put into hash table, head of list at that index */
    tmp_file->name_prev = NULL;
    tmp_file->name_next = darshan_global_job->name_table[hash_index];
    if(tmp_file->name_next)
        tmp_file->name_next->name_prev = tmp_file;
    darshan_global_job->name_table[hash_index] = tmp_file;

    if(newname != name)
        free(newname);
    return(tmp_file);
}


struct darshan_file_runtime* darshan_file_by_name_sethandle(
    const char* name,
    const void* handle,
    int handle_sz,
    enum darshan_handle_type handle_type)
{
    struct darshan_file_runtime* file;
    uint64_t tmp_hash;
    int hash_index;
    struct darshan_file_ref* tmp_ref;

    if(!darshan_global_job)
    {
        return(NULL);
    }

    /* find file record by name first */
    file = darshan_file_by_name(name);

    if(!file)
        return(NULL);

    /* search hash table */
    tmp_ref = ref_by_handle(handle, handle_sz, handle_type);
    if(tmp_ref)
    {
        /* we have a reference.  Make sure it points to the correct file
         * and return it
         */
        tmp_ref->file = file;
        return(file);
    }

    /* if we hit this point, then we don't have a reference for this handle
     * in the table yet.  Add it.
     */
    tmp_hash = darshan_hash(handle, handle_sz, 0);
    hash_index = tmp_hash & CP_HASH_MASK;
    tmp_ref = malloc(sizeof(*tmp_ref));
    if(!tmp_ref)
        return(NULL);

    memset(tmp_ref, 0, sizeof(*tmp_ref));
    tmp_ref->file = file;
    memcpy(tmp_ref->handle, handle, handle_sz);
    tmp_ref->handle_sz = handle_sz;
    tmp_ref->handle_type = handle_type;
    tmp_ref->prev = NULL;
    tmp_ref->next = darshan_global_job->handle_table[hash_index];
    if(tmp_ref->next)
        tmp_ref->next->prev = tmp_ref;
    darshan_global_job->handle_table[hash_index] = tmp_ref;

    return(file);
}

struct darshan_file_runtime* darshan_file_by_handle(
    const void* handle,
    int handle_sz,
    enum darshan_handle_type handle_type)
{   
    struct darshan_file_ref* tmp_ref;

    if(!darshan_global_job)
    {
        return(NULL);
    }

    tmp_ref = ref_by_handle(handle, handle_sz, handle_type);
    if(tmp_ref)
        return(tmp_ref->file);
    else
        return(NULL);

    return(NULL);
}

void darshan_file_closehandle(
    const void* handle,
    int handle_sz,
    enum darshan_handle_type handle_type)
{
    struct darshan_file_ref* tmp_ref;
    uint64_t tmp_hash;
    int hash_index;
    
    if(!darshan_global_job)
    {
        return;
    }

    /* search hash table */
    tmp_hash = darshan_hash(handle, handle_sz, 0);
    hash_index = tmp_hash & CP_HASH_MASK;
    tmp_ref = darshan_global_job->handle_table[hash_index];
    while(tmp_ref)
    {
        if(tmp_ref->handle_sz == handle_sz &&
            tmp_ref->handle_type == handle_type &&
            memcmp(tmp_ref->handle, handle, handle_sz) == 0)
        {
            /* we have a reference. */ 
            if(!tmp_ref->prev)
            {
                /* head of list */
                darshan_global_job->handle_table[hash_index] = tmp_ref->next;
                if(tmp_ref->next)
                    tmp_ref->next->prev = NULL;
            }
            else
            {
                /* not head of list */
                if(tmp_ref->prev)
                    tmp_ref->prev->next = tmp_ref->next;
                if(tmp_ref->next)
                    tmp_ref->next->prev = tmp_ref->prev;
            }
            free(tmp_ref);
            return;
        }
        tmp_ref = tmp_ref->next;
    }

    return;
}

static struct darshan_file_ref* ref_by_handle(
    const void* handle,
    int handle_sz,
    enum darshan_handle_type handle_type)
{   
    uint64_t tmp_hash;
    int hash_index;
    struct darshan_file_ref* tmp_ref;

    if(!darshan_global_job)
    {
        return(NULL);
    }

    /* search hash table */
    tmp_hash = darshan_hash(handle, handle_sz, 0);
    hash_index = tmp_hash & CP_HASH_MASK;
    tmp_ref = darshan_global_job->handle_table[hash_index];
    while(tmp_ref)
    {
        if(tmp_ref->handle_sz == handle_sz &&
            tmp_ref->handle_type == handle_type &&
            memcmp(tmp_ref->handle, handle, handle_sz) == 0)
        {
            /* we have a reference. */ 
            return(tmp_ref);
        }
        tmp_ref = tmp_ref->next;
    }

    return(NULL);
}

/* Allocate a new string that contains a cleaned-up version of the path
 * passed in as an argument.  Converts relative paths to absolute paths and
 * filters out some potential noise in the path string.
 */
static char* clean_path(const char* path)
{
    char* newpath = NULL;
    char* cwd = NULL;
    char* filter = NULL;

    if(!path || strlen(path) < 1)
        return(NULL);

    if(path[0] == '/')
    {
        /* it is already an absolute path */
        newpath = malloc(strlen(path)+1);
        if(newpath)
        {
            strcpy(newpath, path);
        }
    }
    else
    {
        /* handle relative path */
        cwd = malloc(PATH_MAX);
        if(cwd)
        {
            if(getcwd(cwd, PATH_MAX))
            {
                newpath = malloc(strlen(path) + strlen(cwd) + 2);
                if(newpath)
                {
                    sprintf(newpath, "%s/%s", cwd, path);
                }
            }
            free(cwd);
        }
    }

    if(!newpath)
        return(NULL);

    /* filter out any double slashes */
    while((filter = strstr(newpath, "//")))
    {
        /* shift down one character */
        memmove(filter, &filter[1], (strlen(&filter[1]) + 1));
    }

    /* filter out any /./ instances */
    while((filter = strstr(newpath, "/./")))
    {
        /* shift down two characters */
        memmove(filter, &filter[2], (strlen(&filter[2]) + 1));
    }

    /* return result */
    return(newpath);
}

/* adds a tracker for the given aio operation */
static void darshan_aio_tracker_add(int fd, void *aiocbp)
{
    struct darshan_aio_tracker* tracker;
    struct darshan_file_runtime* file;

    CP_LOCK();
    file = darshan_file_by_fd(fd);
    if(file)
    {
        tracker = malloc(sizeof(*tracker));
        if(tracker)
        {
            tracker->tm1 = darshan_wtime();
            tracker->aiocbp = aiocbp;
            tracker->next = NULL;
            if(file->aio_list_tail)
            {
                file->aio_list_tail->next = tracker;
                file->aio_list_tail = tracker;
            }
            else
            {
                file->aio_list_head = file->aio_list_tail = tracker;
            }
        }
    }
    CP_UNLOCK();

    return;
}

/* finds the tracker structure for a given aio operation, removes it from
 * the linked list for the darshan_file structure, and returns a pointer.  
 *
 * returns NULL if aio operation not found
 */
static struct darshan_aio_tracker* darshan_aio_tracker_del(int fd, void *aiocbp)
{
    struct darshan_aio_tracker *tmp, *prev;
    struct darshan_file_runtime* file;

    CP_LOCK();
    file = darshan_file_by_fd(fd);
    if(file)
    {
        /* is there a tracker struct for this operation? */
        tmp = file->aio_list_head; 
        prev = NULL;
        while(tmp)
        {
            if(tmp->aiocbp == aiocbp)
            {
                if(prev)
                    prev->next = tmp->next;
                else
                    file->aio_list_head = tmp->next;

                if(tmp == file->aio_list_tail)
                    file->aio_list_tail = prev;

                break;
            }
            else
            {
                prev = tmp;
                tmp = tmp->next;
            }
        }
    }

    CP_UNLOCK();

    return(tmp);
}

/*
 * Local variables:
 *  c-indent-level: 4
 *  c-basic-offset: 4
 * End:
 *
 * vim: ts=8 sts=4 sw=4 expandtab
 */
