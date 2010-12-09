/*
 *  (C) 2009 by Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
 */

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
#include <pthread.h>
#include <sys/mman.h>
#include <search.h>
#include <assert.h>

#include "darshan.h"
#include "darshan-config.h"

#ifndef HAVE_OFF64_T
typedef int64_t off64_t;
#endif

extern char* __progname_full;

extern int __real_creat(const char* path, mode_t mode);
extern int __real_creat64(const char* path, mode_t mode);
extern int __real_open(const char *path, int flags, ...);
extern int __real_open64(const char *path, int flags, ...);
extern int __real_close(int fd);
extern ssize_t __real_write(int fd, const void *buf, size_t count);
extern ssize_t __real_read(int fd, void *buf, size_t count);
extern off_t __real_lseek(int fd, off_t offset, int whence);
extern off64_t __real_lseek64(int fd, off64_t offset, int whence);
extern ssize_t __real_pread(int fd, void *buf, size_t count, off_t offset);
extern ssize_t __real_pread64(int fd, void *buf, size_t count, off64_t offset);
extern ssize_t __real_pwrite(int fd, const void *buf, size_t count, off_t offset);
extern ssize_t __real_pwrite64(int fd, const void *buf, size_t count, off64_t offset);
extern ssize_t __real_readv(int fd, const struct iovec *iov, int iovcnt);
extern ssize_t __real_writev(int fd, const struct iovec *iov, int iovcnt);
extern int __real___fxstat(int vers, int fd, struct stat *buf);
extern int __real___lxstat(int vers, const char* path, struct stat *buf);
extern int __real___xstat(int vers, const char* path, struct stat *buf);
extern int __real___fxstat64(int vers, int fd, struct stat64 *buf);
extern int __real___lxstat64(int vers, const char* path, struct stat64 *buf);
extern int __real___xstat64(int vers, const char* path, struct stat64 *buf);
extern void* __real_mmap(void *addr, size_t length, int prot, int flags,
    int fd, off_t offset);
extern void* __real_mmap64(void *addr, size_t length, int prot, int flags,
    int fd, off64_t offset);
extern FILE* __real_fopen(const char *path, const char *mode);
extern FILE* __real_fopen64(const char *path, const char *mode);
extern int __real_fclose(FILE *fp);
extern size_t __real_fread(void *ptr, size_t size, size_t nmemb, FILE *stream);
extern size_t __real_fwrite(const void *ptr, size_t size, size_t nmemb, FILE *stream);
extern int __real_fseek(FILE *stream, long offset, int whence);
extern int __real_fsync(int fd);
extern int __real_fdatasync(int fd);


pthread_mutex_t cp_mutex = PTHREAD_MUTEX_INITIALIZER;
struct darshan_job_runtime* darshan_global_job = NULL;
static int my_rank = -1;
static struct stat64 cp_stat_buf;
static int darshan_mem_alignment;

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

static void cp_access_counter(struct darshan_file_runtime* file, ssize_t size,     enum cp_counter_type type);

#define CP_RECORD_WRITE(__ret, __fd, __count, __update_offset, __aligned, __stream_flag, __tm1, __tm2) do{ \
    size_t stride; \
    off_t old_offset; \
    int64_t file_alignment; \
    struct darshan_file_runtime* file; \
    double __elapsed = __tm2-__tm1; \
    if(__ret < 0) break; \
    file = darshan_file_by_fd(__fd); \
    if(!file) break; \
    old_offset = file->offset; \
    file_alignment = CP_VALUE(file, CP_FILE_ALIGNMENT); \
    if(old_offset > file->last_byte_written) \
        CP_INC(file, CP_SEQ_WRITES, 1); \
    if(old_offset == (file->last_byte_written + 1)) \
        CP_INC(file, CP_CONSEC_WRITES, 1); \
    if(old_offset > 0 && old_offset > file->last_byte_written \
        && file->last_byte_written != 0) \
        stride = old_offset - file->last_byte_written - 1; \
    else \
        stride = 0; \
    file->last_byte_written = old_offset + __ret - 1; \
    if(__update_offset) \
        file->offset = old_offset + __ret; \
    CP_MAX(file, CP_MAX_BYTE_WRITTEN, (old_offset + __ret -1)); \
    CP_INC(file, CP_BYTES_WRITTEN, __ret); \
    if(__stream_flag) \
        CP_INC(file, CP_POSIX_FWRITES, 1); \
    else \
        CP_INC(file, CP_POSIX_WRITES, 1); \
    CP_BUCKET_INC(file, CP_SIZE_WRITE_0_100, __ret); \
    cp_access_counter(file, stride, CP_COUNTER_STRIDE); \
    if(!__aligned) \
        CP_INC(file, CP_MEM_NOT_ALIGNED, 1); \
    if(file_alignment && (old_offset % file_alignment) != 0) \
        CP_INC(file, CP_FILE_NOT_ALIGNED, 1); \
    cp_access_counter(file, __ret, CP_COUNTER_ACCESS); \
    if(file->last_io_type == CP_READ) \
        CP_INC(file, CP_RW_SWITCHES, 1); \
    file->last_io_type = CP_WRITE; \
    CP_F_INC(file, CP_F_POSIX_WRITE_TIME, (__elapsed)); \
    if(CP_F_VALUE(file, CP_F_WRITE_START_TIMESTAMP) == 0) \
        CP_F_SET(file, CP_F_WRITE_START_TIMESTAMP, __tm1); \
    CP_F_SET(file, CP_F_WRITE_END_TIMESTAMP, __tm2); \
    if(CP_F_VALUE(file, CP_F_MAX_WRITE_TIME) < __elapsed){ \
        CP_F_SET(file, CP_F_MAX_WRITE_TIME, __elapsed); \
        CP_SET(file, CP_MAX_WRITE_TIME_SIZE, __ret); } \
} while(0)

#define CP_RECORD_READ(__ret, __fd, __count, __update_offset, __aligned, __stream_flag, __tm1, __tm2) do{ \
    size_t stride; \
    off_t old_offset; \
    struct darshan_file_runtime* file; \
    int64_t file_alignment; \
    double __elapsed = __tm2-__tm1; \
    if(__ret < 0) break; \
    file = darshan_file_by_fd(__fd); \
    if(!file) break; \
    old_offset = file->offset; \
    file_alignment = CP_VALUE(file, CP_FILE_ALIGNMENT); \
    if(old_offset > file->last_byte_read) \
        CP_INC(file, CP_SEQ_READS, 1); \
    if(old_offset == (file->last_byte_read + 1)) \
        CP_INC(file, CP_CONSEC_READS, 1); \
    if(old_offset > 0 && old_offset > file->last_byte_read \
        && file->last_byte_read != 0) \
        stride = old_offset - file->last_byte_read - 1; \
    else \
        stride = 0; \
    file->last_byte_read = old_offset + __ret - 1; \
    CP_MAX(file, CP_MAX_BYTE_READ, (old_offset + __ret -1)); \
    if(__update_offset) \
        file->offset = old_offset + __ret; \
    CP_INC(file, CP_BYTES_READ, __ret); \
    if(__stream_flag)\
        CP_INC(file, CP_POSIX_FREADS, 1); \
    else\
        CP_INC(file, CP_POSIX_READS, 1); \
    CP_BUCKET_INC(file, CP_SIZE_READ_0_100, __ret); \
    cp_access_counter(file, stride, CP_COUNTER_STRIDE); \
    if(!__aligned) \
        CP_INC(file, CP_MEM_NOT_ALIGNED, 1); \
    if(file_alignment && (old_offset % file_alignment) != 0) \
        CP_INC(file, CP_FILE_NOT_ALIGNED, 1); \
    cp_access_counter(file, __ret, CP_COUNTER_ACCESS); \
    if(file->last_io_type == CP_WRITE) \
        CP_INC(file, CP_RW_SWITCHES, 1); \
    file->last_io_type = CP_READ; \
    CP_F_INC(file, CP_F_POSIX_READ_TIME, (__elapsed)); \
    if(CP_F_VALUE(file, CP_F_READ_START_TIMESTAMP) == 0) \
        CP_F_SET(file, CP_F_READ_START_TIMESTAMP, __tm1); \
    CP_F_SET(file, CP_F_READ_END_TIMESTAMP, __tm2); \
    if(CP_F_VALUE(file, CP_F_MAX_READ_TIME) < __elapsed){ \
        CP_F_SET(file, CP_F_MAX_READ_TIME, __elapsed); \
        CP_SET(file, CP_MAX_READ_TIME_SIZE, __ret); } \
} while(0)

#define CP_RECORD_STAT(__file, __statbuf, __tm1, __tm2) do { \
    if(!CP_VALUE((__file), CP_FILE_ALIGNMENT)){ \
        CP_SET((__file), CP_DEVICE, (__statbuf)->st_dev); \
        CP_SET((__file), CP_FILE_ALIGNMENT, (__statbuf)->st_blksize); \
        CP_SET((__file), CP_SIZE_AT_OPEN, (__statbuf)->st_size); \
    }\
    (__file)->log_file->rank = my_rank; \
    CP_F_INC(__file, CP_F_POSIX_META_TIME, (__tm2-__tm1)); \
    CP_INC(__file, CP_POSIX_STATS, 1); \
} while(0)

#define CP_RECORD_OPEN(__ret, __path, __mode, __stream_flag, __tm1, __tm2) do { \
    struct darshan_file_runtime* file; \
    char* exclude; \
    int tmp_index = 0; \
    int hash_index; \
    if(__ret < 0) break; \
    while((exclude = exclusions[tmp_index])) { \
        if(!(strncmp(exclude, __path, strlen(exclude)))) \
            break; \
        tmp_index++; \
    } \
    if(exclude) break; \
    file = darshan_file_by_name(__path); \
    if(!file) break; \
    if(file->fd != -1) break; /* TODO: handle multiple concurrent opens */ \
    file->fd = __ret; \
    if(!CP_VALUE(file, CP_FILE_ALIGNMENT)){ \
        if(fstat64(file->fd, &cp_stat_buf) == 0) {\
            CP_SET(file, CP_DEVICE, cp_stat_buf.st_dev); \
            CP_SET(file, CP_FILE_ALIGNMENT, cp_stat_buf.st_blksize); \
            CP_SET(file, CP_SIZE_AT_OPEN, cp_stat_buf.st_size); \
        }\
    }\
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
        CP_F_SET(file, CP_F_OPEN_TIMESTAMP, MPI_Wtime()); \
    CP_F_INC(file, CP_F_POSIX_META_TIME, (__tm2-__tm1)); \
    hash_index = file->fd & CP_HASH_MASK; \
    file->fd_prev = NULL; \
    file->fd_next = darshan_global_job->fd_table[hash_index]; \
    if(file->fd_next) \
        file->fd_next->fd_prev = file; \
    darshan_global_job->fd_table[hash_index] = file; \
} while (0)

int __wrap_close(int fd)
{
    struct darshan_file_runtime* file;
    int hash_index;
    int tmp_fd = fd;
    double tm1, tm2;
    int ret;

    tm1 = darshan_wtime();
    ret = __real_close(fd);
    tm2 = darshan_wtime();

    CP_LOCK();
    file = darshan_file_by_fd(tmp_fd);
    if(file)
    {
        file->fd = -1;
        file->last_byte_written = 0;
        file->last_byte_read = 0;
        CP_F_SET(file, CP_F_CLOSE_TIMESTAMP, MPI_Wtime());
        CP_F_INC(file, CP_F_POSIX_META_TIME, (tm2-tm1));
        if(file->fd_prev == NULL)
        {
            /* head of fd hash table list */
            hash_index = tmp_fd & CP_HASH_MASK;
            darshan_global_job->fd_table[hash_index] = file->fd_next;
            if(file->fd_next)
                file->fd_next->fd_prev = NULL;
        }
        else
        {
            if(file->fd_prev)
                file->fd_prev->fd_next = file->fd_next;
            if(file->fd_next)
                file->fd_next->fd_prev = file->fd_prev;
        }
        file->fd_prev = NULL;
        file->fd_next = NULL;
        darshan_global_job->darshan_mru_file = file; /* in case we open it again */
    }
    CP_UNLOCK();

    return(ret);
}

int __wrap_fclose(FILE *fp)
{
    struct darshan_file_runtime* file;
    int hash_index;
    int tmp_fd = fileno(fp);
    double tm1, tm2;
    int ret;

    tm1 = darshan_wtime();
    ret = __real_fclose(fp);
    tm2 = darshan_wtime();
    
    CP_LOCK();
    file = darshan_file_by_fd(tmp_fd);
    if(file)
    {
        file->fd = -1;
        file->last_byte_written = 0;
        file->last_byte_read = 0;
        CP_F_SET(file, CP_F_CLOSE_TIMESTAMP, MPI_Wtime());
        CP_F_INC(file, CP_F_POSIX_META_TIME, (tm2-tm1));
        if(file->fd_prev == NULL)
        {
            /* head of fd hash table list */
            hash_index = tmp_fd & CP_HASH_MASK;
            darshan_global_job->fd_table[hash_index] = file->fd_next;
            if(file->fd_next)
                file->fd_next->fd_prev = NULL;
        }
        else
        {
            if(file->fd_prev)
                file->fd_prev->fd_next = file->fd_next;
            if(file->fd_next)
                file->fd_next->fd_prev = file->fd_prev;
        }
        file->fd_prev = NULL;
        file->fd_next = NULL;
        darshan_global_job->darshan_mru_file = file; /* in case we open it again */
    }
    CP_UNLOCK();

    return(ret);
}


int __wrap_fsync(int fd)
{
    int ret;
    struct darshan_file_runtime* file;
    double tm1, tm2;

    tm1 = darshan_wtime();
    ret = __real_fsync(fd);
    tm2 = darshan_wtime();

    if(ret < 0)
        return(ret);

    CP_LOCK();
    file = darshan_file_by_fd(fd);
    if(file)
    {
        CP_F_INC(file, CP_F_POSIX_WRITE_TIME, (tm2-tm1));
        CP_INC(file, CP_POSIX_FSYNCS, 1);
    }
    CP_UNLOCK();

    return(ret);
}

int __wrap_fdatasync(int fd)
{
    int ret;
    struct darshan_file_runtime* file;
    double tm1, tm2;

    tm1 = darshan_wtime();
    ret = __real_fdatasync(fd);
    tm2 = darshan_wtime();
    if(ret < 0)
        return(ret);

    CP_LOCK();
    file = darshan_file_by_fd(fd);
    if(file)
    {
        CP_F_INC(file, CP_F_POSIX_WRITE_TIME, (tm2-tm1));
        CP_INC(file, CP_POSIX_FDSYNCS, 1);
    }
    CP_UNLOCK();

    return(ret);
}


void* __wrap_mmap64(void *addr, size_t length, int prot, int flags,
    int fd, off64_t offset)
{
    void* ret;
    struct darshan_file_runtime* file;

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


void* __wrap_mmap(void *addr, size_t length, int prot, int flags,
    int fd, off_t offset)
{
    void* ret;
    struct darshan_file_runtime* file;

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

int __wrap_creat(const char* path, mode_t mode)
{
    int ret;
    double tm1, tm2;

    tm1 = darshan_wtime();
    ret = __real_creat(path, mode);
    tm2 = darshan_wtime();

    CP_LOCK();
    CP_RECORD_OPEN(ret, path, mode, 0, tm1, tm2);
    CP_UNLOCK();

    return(ret);
}

int __wrap_creat64(const char* path, mode_t mode)
{
    int ret;
    double tm1, tm2;

    tm1 = darshan_wtime();
    ret = __real_creat64(path, mode);
    tm2 = darshan_wtime();

    CP_LOCK();
    CP_RECORD_OPEN(ret, path, mode, 0, tm1, tm2);
    CP_UNLOCK();

    return(ret);
}

int __wrap_open64(const char* path, int flags, ...)
{
    int mode = 0;
    int ret;
    double tm1, tm2;

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

int __wrap_open(const char *path, int flags, ...)
{
    int mode = 0;
    int ret;
    double tm1, tm2;

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

FILE* __wrap_fopen64(const char *path, const char *mode)
{
    FILE* ret;
    int fd;
    double tm1, tm2;

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

FILE* __wrap_fopen(const char *path, const char *mode)
{
    FILE* ret;
    int fd;
    double tm1, tm2;

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

int __wrap___xstat64(int vers, const char *path, struct stat64 *buf)
{
    int ret;
    struct darshan_file_runtime* file;
    double tm1, tm2;

    tm1 = darshan_wtime();
    ret = __real___xstat64(vers, path, buf);
    tm2 = darshan_wtime();
    if(ret < 0 || !S_ISREG(buf->st_mode))
        return(ret);

    CP_LOCK();
    file = darshan_file_by_name(path);
    if(file)
    {
        CP_RECORD_STAT(file, buf, tm1, tm2);
    }
    CP_UNLOCK();

    return(ret);
}

int __wrap___lxstat64(int vers, const char *path, struct stat64 *buf)
{
    int ret;
    struct darshan_file_runtime* file;
    double tm1, tm2;

    tm1 = darshan_wtime();
    ret = __real___lxstat64(vers, path, buf);
    tm2 = darshan_wtime();
    if(ret < 0 || !S_ISREG(buf->st_mode))
        return(ret);

    CP_LOCK();
    file = darshan_file_by_name(path);
    if(file)
    {
        CP_RECORD_STAT(file, buf, tm1, tm2);
    }
    CP_UNLOCK();

    return(ret);
}

int __wrap___fxstat64(int vers, int fd, struct stat64 *buf)
{
    int ret;
    struct darshan_file_runtime* file;
    double tm1, tm2;

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


int __wrap___xstat(int vers, const char *path, struct stat *buf)
{
    int ret;
    struct darshan_file_runtime* file;
    double tm1, tm2;

    tm1 = darshan_wtime();
    ret = __real___xstat(vers, path, buf);
    tm2 = darshan_wtime();
    if(ret < 0 || !S_ISREG(buf->st_mode))
        return(ret);

    CP_LOCK();
    file = darshan_file_by_name(path);
    if(file)
    {
        CP_RECORD_STAT(file, buf, tm1, tm2);
    }
    CP_UNLOCK();

    return(ret);
}

int __wrap___lxstat(int vers, const char *path, struct stat *buf)
{
    int ret;
    struct darshan_file_runtime* file;
    double tm1, tm2;

    tm1 = darshan_wtime();
    ret = __real___lxstat(vers, path, buf);
    tm2 = darshan_wtime();
    if(ret < 0 || !S_ISREG(buf->st_mode))
        return(ret);

    CP_LOCK();
    file = darshan_file_by_name(path);
    if(file)
    {
        CP_RECORD_STAT(file, buf, tm1, tm2);
    }
    CP_UNLOCK();

    return(ret);
}

int __wrap___fxstat(int vers, int fd, struct stat *buf)
{
    int ret;
    struct darshan_file_runtime* file;
    double tm1, tm2;

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

ssize_t __wrap_pread64(int fd, void *buf, size_t count, off64_t offset)
{
    ssize_t ret;
    int aligned_flag = 0;
    double tm1, tm2;

    if((unsigned long)buf % darshan_mem_alignment == 0)
        aligned_flag = 1;

    tm1 = darshan_wtime();
    ret = __real_pread64(fd, buf, count, offset);
    tm2 = darshan_wtime();
    CP_LOCK();
    CP_RECORD_READ(ret, fd, count, 0, aligned_flag, 0, tm1, tm2);
    CP_UNLOCK();
    return(ret);
}

ssize_t __wrap_pread(int fd, void *buf, size_t count, off_t offset)
{
    ssize_t ret;
    int aligned_flag = 0;
    double tm1, tm2;

    if((unsigned long)buf % darshan_mem_alignment == 0)
        aligned_flag = 1;

    tm1 = darshan_wtime();
    ret = __real_pread(fd, buf, count, offset);
    tm2 = darshan_wtime();
    CP_LOCK();
    CP_RECORD_READ(ret, fd, count, 0, aligned_flag, 0, tm1, tm2);
    CP_UNLOCK();
    return(ret);
}


ssize_t __wrap_pwrite(int fd, const void *buf, size_t count, off_t offset)
{
    ssize_t ret;
    int aligned_flag = 0;
    double tm1, tm2;

    if((unsigned long)buf % darshan_mem_alignment == 0)
        aligned_flag = 1;

    tm1 = darshan_wtime();
    ret = __real_pwrite(fd, buf, count, offset);
    tm2 = darshan_wtime();
    CP_LOCK();
    CP_RECORD_WRITE(ret, fd, count, 0, aligned_flag, 0, tm1, tm2);
    CP_UNLOCK();
    return(ret);
}

ssize_t __wrap_pwrite64(int fd, const void *buf, size_t count, off64_t offset)
{
    ssize_t ret;
    int aligned_flag = 0;
    double tm1, tm2;

    if((unsigned long)buf % darshan_mem_alignment == 0)
        aligned_flag = 1;

    tm1 = darshan_wtime();
    ret = __real_pwrite64(fd, buf, count, offset);
    tm2 = darshan_wtime();
    CP_LOCK();
    CP_RECORD_WRITE(ret, fd, count, 0, aligned_flag, 0, tm1, tm2);
    CP_UNLOCK();
    return(ret);
}

ssize_t __wrap_readv(int fd, const struct iovec *iov, int iovcnt)
{
    ssize_t ret;
    int aligned_flag = 1;
    int i;
    double tm1, tm2;

    for(i=0; i<iovcnt; i++)
    {
        if(((unsigned long)iov[i].iov_base % darshan_mem_alignment) != 0)
            aligned_flag = 0;
    }

    tm1 = darshan_wtime();
    ret = __real_readv(fd, iov, iovcnt);
    tm2 = darshan_wtime();
    CP_LOCK();
    CP_RECORD_READ(ret, fd, count, 1, aligned_flag, 0, tm1, tm2);
    CP_UNLOCK();
    return(ret);
}

ssize_t __wrap_writev(int fd, const struct iovec *iov, int iovcnt)
{
    ssize_t ret;
    int aligned_flag = 1;
    int i;
    double tm1, tm2;

    for(i=0; i<iovcnt; i++)
    {
        if(!((unsigned long)iov[i].iov_base % darshan_mem_alignment == 0))
            aligned_flag = 0;
    }

    tm1 = darshan_wtime();
    ret = __real_writev(fd, iov, iovcnt);
    tm2 = darshan_wtime();
    CP_LOCK();
    CP_RECORD_WRITE(ret, fd, count, 1, aligned_flag, 0, tm1, tm2);
    CP_UNLOCK();
    return(ret);
}

size_t __wrap_fread(void *ptr, size_t size, size_t nmemb, FILE *stream)
{
    size_t ret;
    int aligned_flag = 0;
    double tm1, tm2;

    if((unsigned long)ptr % darshan_mem_alignment == 0)
        aligned_flag = 1;

    tm1 = darshan_wtime();
    ret = __real_fread(ptr, size, nmemb, stream);
    tm2 = darshan_wtime();
    CP_LOCK();
    if(ret > 0)
        CP_RECORD_READ(size*ret, fileno(stream), (size*nmemb), 1, aligned_flag, 1, tm1, tm2);
    else
        CP_RECORD_READ(ret, fileno(stream), (size*nmemb), 1, aligned_flag, 1, tm1, tm2);
    CP_UNLOCK();
    return(ret);
}

ssize_t __wrap_read(int fd, void *buf, size_t count)
{
    ssize_t ret;
    int aligned_flag = 0;
    double tm1, tm2;

    if((unsigned long)buf % darshan_mem_alignment == 0)
        aligned_flag = 1;

    tm1 = darshan_wtime();
    ret = __real_read(fd, buf, count);
    tm2 = darshan_wtime();
    CP_LOCK();
    CP_RECORD_READ(ret, fd, count, 1, aligned_flag, 0, tm1, tm2);
    CP_UNLOCK();
    return(ret);
}

ssize_t __wrap_write(int fd, const void *buf, size_t count)
{
    ssize_t ret;
    int aligned_flag = 0;
    double tm1, tm2;

    if((unsigned long)buf % darshan_mem_alignment == 0)
        aligned_flag = 1;

    tm1 = darshan_wtime();
    ret = __real_write(fd, buf, count);
    tm2 = darshan_wtime();
    CP_LOCK();
    CP_RECORD_WRITE(ret, fd, count, 1, aligned_flag, 0, tm1, tm2);
    CP_UNLOCK();
    return(ret);
}

size_t __wrap_fwrite(const void *ptr, size_t size, size_t nmemb, FILE *stream)
{
    size_t ret;
    int aligned_flag = 0;
    double tm1, tm2;

    if((unsigned long)ptr % darshan_mem_alignment == 0)
        aligned_flag = 1;

    tm1 = darshan_wtime();
    ret = __real_fwrite(ptr, size, nmemb, stream);
    tm2 = darshan_wtime();
    CP_LOCK();
    if(ret > 0)
        CP_RECORD_WRITE(size*ret, fileno(stream), (size*nmemb), 1, aligned_flag, 1, tm1, tm2);
    else
        CP_RECORD_WRITE(ret, fileno(stream), 0, 1, aligned_flag, 1, tm1, tm2);
    CP_UNLOCK();
    return(ret);
}

off64_t __wrap_lseek64(int fd, off64_t offset, int whence)
{
    off64_t ret;
    struct darshan_file_runtime* file;
    double tm1, tm2;

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
            CP_F_INC(file, CP_F_POSIX_META_TIME, (tm2-tm1));
            CP_INC(file, CP_POSIX_SEEKS, 1);
        }
        CP_UNLOCK();
    }
    return(ret);
}

off_t __wrap_lseek(int fd, off_t offset, int whence)
{
    off_t ret;
    struct darshan_file_runtime* file;
    double tm1, tm2;

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
            CP_F_INC(file, CP_F_POSIX_META_TIME, (tm2-tm1));
            CP_INC(file, CP_POSIX_SEEKS, 1);
        }
        CP_UNLOCK();
    }
    return(ret);
}

int __wrap_fseek(FILE *stream, long offset, int whence)
{
    int ret;
    struct darshan_file_runtime* file;
    double tm1, tm2;

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
            CP_F_INC(file, CP_F_POSIX_META_TIME, (tm2-tm1));
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

    envstr = getenv("DARSHAN_MEMALIGN");
    if (envstr)
    {
        sscanf(envstr, "%d", &darshan_mem_alignment);
    }
    else
    {
        darshan_mem_alignment = __CP_MEM_ALIGNMENT;
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
        darshan_global_job->file_runtime_array[i].fd = -1;
        darshan_global_job->file_runtime_array[i].ncid = -1;
        darshan_global_job->file_runtime_array[i].hid = -1;
        darshan_global_job->file_runtime_array[i].fh = MPI_FILE_NULL;
    }

    strcpy(darshan_global_job->log_job.version_string, CP_VERSION);
    darshan_global_job->log_job.magic_nr = CP_MAGIC_NR;
    darshan_global_job->log_job.uid = getuid();
    darshan_global_job->log_job.start_time = time(NULL);
    darshan_global_job->log_job.nprocs = nprocs;
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
    }
    
    base_file->log_file->hash = 0;
    
    darshan_global_job->flags |= CP_FLAG_CONDENSED;
    darshan_global_job->file_count = 1;

    /* clear hash tables for safety */
    memset(darshan_global_job->name_table, 0, CP_HASH_SIZE*sizeof(struct darshan_file_runtime*));
    memset(darshan_global_job->fd_table, 0, CP_HASH_SIZE*sizeof(struct darshan_file_runtime*));
    memset(darshan_global_job->fh_table, 0, CP_HASH_SIZE*sizeof(struct darshan_file_runtime*));
    
    return;
}

struct darshan_file_runtime* darshan_file_by_name(const char* name)
{
    struct darshan_file_runtime* tmp_file;
    uint64_t tmp_hash = 0;
    char* suffix_pointer;
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

    tmp_hash = darshan_hash((void*)name, strlen(name), 0);

    /* check most recently used */
    if(darshan_global_job->darshan_mru_file && darshan_global_job->darshan_mru_file->log_file->hash == tmp_hash)
    {
        return(darshan_global_job->darshan_mru_file);
    }

    /* search hash table */
    hash_index = tmp_hash & CP_HASH_MASK;
    tmp_file = darshan_global_job->name_table[hash_index];
    while(tmp_file)
    {
        if(tmp_file->log_file->hash == tmp_hash)
        {
            darshan_global_job->darshan_mru_file = tmp_file;
            return(tmp_file);
        }
        tmp_file = tmp_file->name_next;
    }

    /* see if we need to condense */
    if(darshan_global_job->file_count >= CP_MAX_FILES)
    {
        darshan_condense();
        return(&darshan_global_job->file_runtime_array[0]);
    }

    /* new, unique file */
    tmp_file = &darshan_global_job->file_runtime_array[darshan_global_job->file_count];

    CP_SET(tmp_file, CP_MEM_ALIGNMENT, darshan_mem_alignment);
    tmp_file->log_file->hash = tmp_hash;

    /* record last N characters of file name too */
    suffix_pointer = (char*)name;
    if(strlen(name) > CP_NAME_SUFFIX_LEN)
    {
        suffix_pointer += (strlen(name) - CP_NAME_SUFFIX_LEN);
    }
    strcpy(tmp_file->log_file->name_suffix, suffix_pointer); 

    darshan_global_job->file_count++;

    /* put into hash table, head of list at that index */
    tmp_file->name_prev = NULL;
    tmp_file->name_next = darshan_global_job->name_table[hash_index];
    if(tmp_file->name_next)
        tmp_file->name_next->name_prev = tmp_file;
    darshan_global_job->name_table[hash_index] = tmp_file;

    darshan_global_job->darshan_mru_file = tmp_file;
    return(tmp_file);
}

struct darshan_file_runtime* darshan_file_by_fd(int fd)
{
    int hash_index;
    struct darshan_file_runtime* tmp_file;

    if(!darshan_global_job)
    {
        return(NULL);
    }

    /* if we have already condensed the data, then just hand the first file
     * back
     */
    if(darshan_global_job->flags & CP_FLAG_CONDENSED)
    {
        return(&darshan_global_job->file_runtime_array[0]);
    }

    /* try mru first */
    if(darshan_global_job->darshan_mru_file && darshan_global_job->darshan_mru_file->fd == fd)
    {
        return(darshan_global_job->darshan_mru_file);
    }

    /* search hash table */
    /* TODO: confirm that fd as hash index is reasonable */
    hash_index = fd & CP_HASH_MASK;
    tmp_file = darshan_global_job->fd_table[hash_index];
    while(tmp_file)
    {
        if(tmp_file->fd == fd)
        {
            darshan_global_job->darshan_mru_file = tmp_file;
            return(tmp_file);
        }
        tmp_file = tmp_file->fd_next;
    }

    return(NULL);
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

#if 0
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
        CP_RECORD_WRITE(size_array[(i/nfiles)%CP_MAX_ACCESS_COUNT_RUNTIME], fd_array[i%nfiles], size_array[(i/nfiles)%CP_MAX_ACCESS_COUNT_RUNTIME], 0, 1, 0, 1, 2);
    }

    if(rank == 0)
        printf("# 1 unique file per proc\n");
    darshan_shutdown(1);
    darshan_global_job = NULL;

    /***********************************************************/
    /* reset darshan to start clean */
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
        CP_RECORD_WRITE(size_array[(i/nfiles)%CP_MAX_ACCESS_COUNT_RUNTIME], fd_array[i%nfiles], size_array[(i/nfiles)%CP_MAX_ACCESS_COUNT_RUNTIME], 0, 1, 0, 1, 2);
    }

    if(rank == 0)
        printf("# 1 shared file across procs\n");
    darshan_shutdown(1);
    darshan_global_job = NULL;

    /***********************************************************/
    /* reset darshan to start clean */
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
        CP_RECORD_WRITE(size_array[(i/nfiles)%CP_MAX_ACCESS_COUNT_RUNTIME], fd_array[i%nfiles], size_array[(i/nfiles)%CP_MAX_ACCESS_COUNT_RUNTIME], 0, 1, 0, 1, 2);
    }

    if(rank == 0)
        printf("# 1024 unique files per proc\n");
    darshan_shutdown(1);
    darshan_global_job = NULL;

    /***********************************************************/
    /* reset darshan to start clean */
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
        CP_RECORD_WRITE(size_array[(i/nfiles)%CP_MAX_ACCESS_COUNT_RUNTIME], fd_array[i%nfiles], size_array[(i/nfiles)%CP_MAX_ACCESS_COUNT_RUNTIME], 0, 1, 0, 1, 2);
    }

    if(rank == 0)
        printf("# 1024 shared files across procs\n");
    darshan_shutdown(1);
    darshan_global_job = NULL;

    darshan_initialize(argc, argv, nprocs, rank);

    free(fd_array);
    free(size_array);

    return;
}

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
                CP_RECORD_WRITE(size_array[(i/nfiles)%CP_MAX_ACCESS_COUNT_RUNTIME], fd_array[i%nfiles], size_array[(i/nfiles)%CP_MAX_ACCESS_COUNT_RUNTIME], 0, 1, 0, 1, 2);
            }
            else
            {
                CP_RECORD_WRITE(size_array[0], fd_array[i%nfiles], size_array[0], 0, 1, 0, 1, 2);
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
                    CP_RECORD_WRITE(size_array[(i/nfiles)%CP_MAX_ACCESS_COUNT_RUNTIME], fd_array[i%nfiles], size_array[(i/nfiles)%CP_MAX_ACCESS_COUNT_RUNTIME], 0, 1, 0, 1, 2);
                }
                else
                {
                    CP_RECORD_WRITE(size_array[0], fd_array[i%nfiles], size_array[0], 0, 1, 0, 1, 2);
                }
            }
            tm2 = darshan_wtime();

            // printf("#<iters>\t<numfiles>\t<numsizes>\t<total time>\t<per iter>\n");
            printf("%d\t%d\t%d\t%f\t%.12f\n", iters, nfiles, (j==0?CP_MAX_ACCESS_COUNT_RUNTIME:1), tm2-tm1, (tm2-tm1)/iters);

            if(nfiles == 1)
                nfiles = 0;
        }
    }

    free(fd_array);
    free(size_array);
}
#endif

double darshan_wtime(void)
{
    if(!darshan_global_job || darshan_global_job->flags & CP_FLAG_NOTIMING)
    {
        return(0);
    }
    
    return(MPI_Wtime());
}

/*
 * Local variables:
 *  c-indent-level: 4
 *  c-basic-offset: 4
 * End:
 *
 * vim: ts=8 sts=4 sw=4 expandtab
 */
