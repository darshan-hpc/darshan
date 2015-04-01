/*
 * Copyright (C) 2015 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
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

/* TODO: more libc, fgetc, etc etc etc. */

DARSHAN_FORWARD_DECL(open, int, (const char *path, int flags, ...));
DARSHAN_FORWARD_DECL(open64, int, (const char *path, int flags, ...));
DARSHAN_FORWARD_DECL(creat, int, (const char* path, mode_t mode));
DARSHAN_FORWARD_DECL(creat64, int, (const char* path, mode_t mode));
DARSHAN_FORWARD_DECL(fopen, FILE*, (const char *path, const char *mode));
DARSHAN_FORWARD_DECL(fopen64, FILE*, (const char *path, const char *mode));
DARSHAN_FORWARD_DECL(mkstemp, int, (char *template));
DARSHAN_FORWARD_DECL(mkostemp, int, (char *template, int flags));
DARSHAN_FORWARD_DECL(mkstemps, int, (char *template, int suffixlen));
DARSHAN_FORWARD_DECL(mkostemps, int, (char *template, int suffixlen, int flags));
DARSHAN_FORWARD_DECL(read, ssize_t, (int fd, void *buf, size_t count));
DARSHAN_FORWARD_DECL(write, ssize_t, (int fd, const void *buf, size_t count));
DARSHAN_FORWARD_DECL(pread, ssize_t, (int fd, void *buf, size_t count, off_t offset));
DARSHAN_FORWARD_DECL(pwrite, ssize_t, (int fd, const void *buf, size_t count, off_t offset));
DARSHAN_FORWARD_DECL(pread64, ssize_t, (int fd, void *buf, size_t count, off64_t offset));
DARSHAN_FORWARD_DECL(pwrite64, ssize_t, (int fd, const void *buf, size_t count, off64_t offset));
DARSHAN_FORWARD_DECL(readv, ssize_t, (int fd, const struct iovec *iov, int iovcnt));
DARSHAN_FORWARD_DECL(writev, ssize_t, (int fd, const struct iovec *iov, int iovcnt));
DARSHAN_FORWARD_DECL(fread, size_t, (void *ptr, size_t size, size_t nmemb, FILE *stream));
DARSHAN_FORWARD_DECL(fwrite, size_t, (const void *ptr, size_t size, size_t nmemb, FILE *stream));
DARSHAN_FORWARD_DECL(lseek, off_t, (int fd, off_t offset, int whence));
DARSHAN_FORWARD_DECL(lseek64, off64_t, (int fd, off64_t offset, int whence));
DARSHAN_FORWARD_DECL(fseek, int, (FILE *stream, long offset, int whence));
DARSHAN_FORWARD_DECL(__xstat, int, (int vers, const char* path, struct stat *buf));
DARSHAN_FORWARD_DECL(__xstat64, int, (int vers, const char* path, struct stat64 *buf));
DARSHAN_FORWARD_DECL(__lxstat, int, (int vers, const char* path, struct stat *buf));
DARSHAN_FORWARD_DECL(__lxstat64, int, (int vers, const char* path, struct stat64 *buf));
DARSHAN_FORWARD_DECL(__fxstat, int, (int vers, int fd, struct stat *buf));
DARSHAN_FORWARD_DECL(__fxstat64, int, (int vers, int fd, struct stat64 *buf));
DARSHAN_FORWARD_DECL(mmap, void*, (void *addr, size_t length, int prot, int flags, int fd, off_t offset));
DARSHAN_FORWARD_DECL(mmap64, void*, (void *addr, size_t length, int prot, int flags, int fd, off64_t offset));
DARSHAN_FORWARD_DECL(fsync, int, (int fd));
DARSHAN_FORWARD_DECL(fdatasync, int, (int fd));
DARSHAN_FORWARD_DECL(close, int, (int fd));
DARSHAN_FORWARD_DECL(fclose, int, (FILE *fp));
/* TODO aio */
/* TODO listio */

static void posix_runtime_initialize(void);
static struct posix_file_runtime* posix_file_by_name(const char *name);
static struct posix_file_runtime* posix_file_by_name_setfd(const char* name, int fd);
static struct posix_file_runtime* posix_file_by_fd(int fd);
static void posix_file_close_fd(int fd);

static void posix_disable_instrumentation(void);
static void posix_prepare_for_reduction(darshan_record_id *shared_recs,
    int *shared_rec_count, void **send_buf, void **recv_buf, int *rec_size);
static void posix_record_reduction_op(void* infile_v, void* inoutfile_v,
    int *len, MPI_Datatype *datatype);
static void posix_get_output_data(void **buffer, int *size);
static void posix_shutdown(void);

enum posix_io_type
{
    POSIX_READ = 1,
    POSIX_WRITE = 2,
};

/* The posix_file_runtime structure maintains necessary runtime metadata
 * for the POSIX file record (darshan_posix_file structure, defined in
 * darshan-posix-log-format.h) pointed to by 'file_record'. This metadata
 * assists with the instrumenting of specific statistics in the file record.
 * 'hlink' is a hash table link structure used to add/remove this record
 * from the hash table of POSIX file records for this process. 
 *
 * RATIONALE: the POSIX module needs to track some stateful, volatile 
 * information about each open file (like the current file offset, most recent 
 * access time, etc.) to aid in instrumentation, but this information can't be
 * stored in the darshan_posix_file struct because we don't want it to appear in
 * the final darshan log file.  We therefore associate a posix_file_runtime
 * struct with each darshan_posix_file struct in order to track this information.
  *
 * NOTE: There is a one-to-one mapping of posix_file_runtime structs to
 * darshan_posix_file structs.
 *
 * NOTE: The posix_file_runtime struct contains a pointer to a darshan_posix_file
 * struct (see the *file_record member) rather than simply embedding an entire
 * darshan_posix_file struct.  This is done so that all of the darshan_posix_file
 * structs can be kept contiguous in memory as a single array to simplify
 * reduction, compression, and storage.
 */
struct posix_file_runtime
{
    struct darshan_posix_file* file_record;
    int64_t offset;
    int64_t last_byte_read;
    int64_t last_byte_written;
    enum posix_io_type last_io_type;
    double last_meta_end;
    double last_read_end;
    double last_write_end;
    UT_hash_handle hlink;
};

/* The posix_file_runtime_ref structure is used to associate a POSIX
 * file descriptor with an already existing POSIX file record. This is
 * necessary as many POSIX I/O functions take only an input file descriptor,
 * but POSIX file records are indexed by their full file paths (i.e., darshan
 * record identifiers for POSIX files are created by hashing the file path).
 * In other words, this structure is necessary as it allows us to look up a
 * file record either by a pathname (posix_file_runtime) or by POSIX file
 * descriptor (posix_file_runtime_ref), depending on which parameters are
 * available. This structure includes another hash table link, since separate
 * hashes are maintained for posix_file_runtime structures and posix_file_runtime_ref
 * structures.
 *
 * RATIONALE: In theory the fd information could be included in the
 * posix_file_runtime struct rather than in a separate structure here.  The
 * reason we don't do that is because the same file could be opened multiple
 * times by a given process with different file descriptors and thus
 * simulataneously referenced using different file descriptors.  This practice is
 * not common, but we must support it.
 *
 * NOTE: there are potentially multiple posix_file_runtime_ref structures
 * referring to a single posix_file_runtime structure.  Most of the time there is
 * only one, however.
 */
struct posix_file_runtime_ref
{
    struct posix_file_runtime* file;
    int fd;
    UT_hash_handle hlink;
};

/* The posix_runtime structure maintains necessary state for storing
 * POSIX file records and for coordinating with darshan-core at 
 * shutdown time.
 */
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
static int darshan_mem_alignment = 1;

#define POSIX_LOCK() pthread_mutex_lock(&posix_runtime_mutex)
#define POSIX_UNLOCK() pthread_mutex_unlock(&posix_runtime_mutex)

#define POSIX_RECORD_OPEN(__ret, __path, __mode, __stream_flag, __tm1, __tm2) do { \
    struct posix_file_runtime* file; \
    char* exclude; \
    int tmp_index = 0; \
    if(__ret < 0) break; \
    while((exclude = darshan_path_exclusions[tmp_index])) { \
        if(!(strncmp(exclude, __path, strlen(exclude)))) \
            break; \
        tmp_index++; \
    } \
    if(exclude) break; \
    file = posix_file_by_name_setfd(__path, __ret); \
    if(!file) break; \
    file->file_record->rank = my_rank; \
    if(__mode) \
        DARSHAN_COUNTER_SET(file->file_record, POSIX_MODE, __mode); \
    file->offset = 0; \
    file->last_byte_written = 0; \
    file->last_byte_read = 0; \
    if(__stream_flag)\
        DARSHAN_COUNTER_INC(file->file_record, POSIX_FOPENS, 1); \
    else \
        DARSHAN_COUNTER_INC(file->file_record, POSIX_OPENS, 1); \
    if(DARSHAN_COUNTER_F_VALUE(file->file_record, POSIX_F_OPEN_TIMESTAMP) == 0) \
        DARSHAN_COUNTER_F_SET(file->file_record, POSIX_F_OPEN_TIMESTAMP, __tm1); \
    DARSHAN_COUNTER_F_INC_NO_OVERLAP(file->file_record, __tm1, __tm2, file->last_meta_end, POSIX_F_META_TIME); \
} while(0)

#define POSIX_RECORD_READ(__ret, __fd, __pread_flag, __pread_offset, __aligned, __stream_flag, __tm1, __tm2) do{ \
    /* size_t stride; */\
    int64_t this_offset; \
    struct posix_file_runtime* file; \
    /* int64_t file_alignment; */ \
    double __elapsed = __tm2-__tm1; \
    if(__ret < 0) break; \
    file = posix_file_by_fd(__fd); \
    if(!file) break; \
    if(__pread_flag) \
        this_offset = __pread_offset; \
    else \
        this_offset = file->offset; \
    /* file_alignment = CP_VALUE(file, CP_FILE_ALIGNMENT); */\
    if(this_offset > file->last_byte_read) \
        DARSHAN_COUNTER_INC(file->file_record, POSIX_SEQ_READS, 1); \
    if(this_offset == (file->last_byte_read + 1)) \
        DARSHAN_COUNTER_INC(file->file_record, POSIX_CONSEC_READS, 1); \
    /* if(this_offset > 0 && this_offset > file->last_byte_read \
        && file->last_byte_read != 0) \
        stride = this_offset - file->last_byte_read - 1; \
    else \
        stride = 0; */\
    file->last_byte_read = this_offset + __ret - 1; \
    file->offset = this_offset + __ret; \
    DARSHAN_COUNTER_MAX(file->file_record, POSIX_MAX_BYTE_READ, (this_offset + __ret - 1)); \
    DARSHAN_COUNTER_INC(file->file_record, POSIX_BYTES_READ, __ret); \
    if(__stream_flag)\
        DARSHAN_COUNTER_INC(file->file_record, POSIX_FREADS, 1); \
    else\
        DARSHAN_COUNTER_INC(file->file_record, POSIX_READS, 1); \
    /* CP_BUCKET_INC(file, CP_SIZE_READ_0_100, __ret); \
    cp_access_counter(file, stride, CP_COUNTER_STRIDE); \
    if(!__aligned) \
        CP_INC(file, CP_MEM_NOT_ALIGNED, 1); \
    if(file_alignment > 0 && (this_offset % file_alignment) != 0) \
        CP_INC(file, CP_FILE_NOT_ALIGNED, 1); \
    cp_access_counter(file, __ret, CP_COUNTER_ACCESS); */\
    if(file->last_io_type == POSIX_WRITE) \
        DARSHAN_COUNTER_INC(file->file_record, POSIX_RW_SWITCHES, 1); \
    file->last_io_type = POSIX_READ; \
    DARSHAN_COUNTER_F_INC_NO_OVERLAP(file->file_record, __tm1, __tm2, file->last_read_end, POSIX_F_READ_TIME); \
    if(DARSHAN_COUNTER_F_VALUE(file->file_record, POSIX_F_READ_START_TIMESTAMP) == 0) \
        DARSHAN_COUNTER_F_SET(file->file_record, POSIX_F_READ_START_TIMESTAMP, __tm1); \
    DARSHAN_COUNTER_F_SET(file->file_record, POSIX_F_READ_END_TIMESTAMP, __tm2); \
    if(DARSHAN_COUNTER_F_VALUE(file->file_record, POSIX_F_MAX_READ_TIME) < __elapsed){ \
        DARSHAN_COUNTER_F_SET(file->file_record, POSIX_F_MAX_READ_TIME, __elapsed); \
        DARSHAN_COUNTER_SET(file->file_record, POSIX_MAX_READ_TIME_SIZE, __ret); } \
} while(0)

#define POSIX_RECORD_WRITE(__ret, __fd, __pwrite_flag, __pwrite_offset, __aligned, __stream_flag, __tm1, __tm2) do{ \
    /* size_t stride; */\
    int64_t this_offset; \
    struct posix_file_runtime* file; \
    /* int64_t file_alignment; */ \
    double __elapsed = __tm2-__tm1; \
    if(__ret < 0) break; \
    file = posix_file_by_fd(__fd); \
    if(!file) break; \
    if(__pwrite_flag) \
        this_offset = __pwrite_offset; \
    else \
        this_offset = file->offset; \
    /* file_alignment = CP_VALUE(file, CP_FILE_ALIGNMENT); */\
    if(this_offset > file->last_byte_written) \
        DARSHAN_COUNTER_INC(file->file_record, POSIX_SEQ_WRITES, 1); \
    if(this_offset == (file->last_byte_written + 1)) \
        DARSHAN_COUNTER_INC(file->file_record, POSIX_CONSEC_WRITES, 1); \
    /* if(this_offset > 0 && this_offset > file->last_byte_written \
        && file->last_byte_written != 0) \
        stride = this_offset - file->last_byte_written - 1; \
    else \
        stride = 0; */\
    file->last_byte_written = this_offset + __ret - 1; \
    file->offset = this_offset + __ret; \
    DARSHAN_COUNTER_MAX(file->file_record, POSIX_MAX_BYTE_WRITTEN, (this_offset + __ret - 1)); \
    DARSHAN_COUNTER_INC(file->file_record, POSIX_BYTES_WRITTEN, __ret); \
    if(__stream_flag) \
        DARSHAN_COUNTER_INC(file->file_record, POSIX_FWRITES, 1); \
    else \
        DARSHAN_COUNTER_INC(file->file_record, POSIX_WRITES, 1); \
    /* CP_BUCKET_INC(file, CP_SIZE_WRITE_0_100, __ret); \
    cp_access_counter(file, stride, CP_COUNTER_STRIDE); \
    if(!__aligned) \
        CP_INC(file, CP_MEM_NOT_ALIGNED, 1); \
    if(file_alignment > 0 && (this_offset % file_alignment) != 0) \
        CP_INC(file, CP_FILE_NOT_ALIGNED, 1); \
    cp_access_counter(file, __ret, CP_COUNTER_ACCESS); */\
    if(file->last_io_type == POSIX_READ) \
        DARSHAN_COUNTER_INC(file->file_record, POSIX_RW_SWITCHES, 1); \
    file->last_io_type = POSIX_WRITE; \
    DARSHAN_COUNTER_F_INC_NO_OVERLAP(file->file_record, __tm1, __tm2, file->last_write_end, POSIX_F_WRITE_TIME); \
    if(DARSHAN_COUNTER_F_VALUE(file->file_record, POSIX_F_WRITE_START_TIMESTAMP) == 0) \
        DARSHAN_COUNTER_F_SET(file->file_record, POSIX_F_WRITE_START_TIMESTAMP, __tm1); \
    DARSHAN_COUNTER_F_SET(file->file_record, POSIX_F_WRITE_END_TIMESTAMP, __tm2); \
    if(DARSHAN_COUNTER_F_VALUE(file->file_record, POSIX_F_MAX_WRITE_TIME) < __elapsed){ \
        DARSHAN_COUNTER_F_SET(file->file_record, POSIX_F_MAX_WRITE_TIME, __elapsed); \
        DARSHAN_COUNTER_SET(file->file_record, POSIX_MAX_WRITE_TIME_SIZE, __ret); } \
} while(0)

#define POSIX_LOOKUP_RECORD_STAT(__path, __statbuf, __tm1, __tm2) do { \
    char* exclude; \
    int tmp_index = 0; \
    struct posix_file_runtime* file; \
    while((exclude = darshan_path_exclusions[tmp_index])) { \
        if(!(strncmp(exclude, __path, strlen(exclude)))) \
            break; \
        tmp_index++; \
    } \
    if(exclude) break; \
    file = posix_file_by_name(__path); \
    if(file) \
    { \
        POSIX_RECORD_STAT(file, __statbuf, __tm1, __tm2); \
    } \
} while(0)

#define POSIX_RECORD_STAT(__file, __statbuf, __tm1, __tm2) do { \
    (__file)->file_record->rank = my_rank; \
    DARSHAN_COUNTER_F_INC_NO_OVERLAP((__file)->file_record, __tm1, __tm2, (__file)->last_meta_end, POSIX_F_META_TIME); \
    DARSHAN_COUNTER_INC((__file)->file_record, POSIX_STATS, 1); \
} while(0)

/**********************************************************
 *      Wrappers for POSIX I/O functions of interest      * 
 **********************************************************/

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

FILE* DARSHAN_DECL(fopen)(const char *path, const char *mode)
{
    FILE* ret;
    int fd;
    double tm1, tm2;

    MAP_OR_FAIL(fopen);

    tm1 = darshan_core_wtime();
    ret = __real_fopen(path, mode);
    tm2 = darshan_core_wtime();

    if(ret == NULL)
        fd = -1;
    else
        fd = fileno(ret);

    POSIX_LOCK();
    posix_runtime_initialize();
    POSIX_RECORD_OPEN(fd, path, 0, 1, tm1, tm2);
    POSIX_UNLOCK();

    return(ret);
}

FILE* DARSHAN_DECL(fopen64)(const char *path, const char *mode)
{
    FILE* ret;
    int fd;
    double tm1, tm2;

    MAP_OR_FAIL(fopen64);

    tm1 = darshan_core_wtime();
    ret = __real_fopen64(path, mode);
    tm2 = darshan_core_wtime();

    if(ret == NULL)
        fd = -1;
    else
        fd = fileno(ret);

    POSIX_LOCK();
    posix_runtime_initialize();
    POSIX_RECORD_OPEN(fd, path, 0, 1, tm1, tm2);
    POSIX_UNLOCK();

    return(ret);
}

int DARSHAN_DECL(mkstemp)(char* template)
{
    int ret;
    double tm1, tm2;

    MAP_OR_FAIL(mkstemp);

    tm1 = darshan_core_wtime();
    ret = __real_mkstemp(template);
    tm2 = darshan_core_wtime();

    POSIX_LOCK();
    posix_runtime_initialize();
    POSIX_RECORD_OPEN(ret, template, 0, 0, tm1, tm2);
    POSIX_UNLOCK();

    return(ret);
}

int DARSHAN_DECL(mkostemp)(char* template, int flags)
{
    int ret;
    double tm1, tm2;

    MAP_OR_FAIL(mkostemp);

    tm1 = darshan_core_wtime();
    ret = __real_mkostemp(template, flags);
    tm2 = darshan_core_wtime();

    POSIX_LOCK();
    posix_runtime_initialize();
    POSIX_RECORD_OPEN(ret, template, 0, 0, tm1, tm2);
    POSIX_UNLOCK();

    return(ret);
}

int DARSHAN_DECL(mkstemps)(char* template, int suffixlen)
{
    int ret;
    double tm1, tm2;

    MAP_OR_FAIL(mkstemps);

    tm1 = darshan_core_wtime();
    ret = __real_mkstemps(template, suffixlen);
    tm2 = darshan_core_wtime();

    POSIX_LOCK();
    posix_runtime_initialize();
    POSIX_RECORD_OPEN(ret, template, 0, 0, tm1, tm2);
    POSIX_UNLOCK();

    return(ret);
}

int DARSHAN_DECL(mkostemps)(char* template, int suffixlen, int flags)
{
    int ret;
    double tm1, tm2;

    MAP_OR_FAIL(mkostemps);

    tm1 = darshan_core_wtime();
    ret = __real_mkostemps(template, suffixlen, flags);
    tm2 = darshan_core_wtime();

    POSIX_LOCK();
    posix_runtime_initialize();
    POSIX_RECORD_OPEN(ret, template, 0, 0, tm1, tm2);
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
    posix_runtime_initialize();
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
    posix_runtime_initialize();
    POSIX_RECORD_WRITE(ret, fd, 0, 0, aligned_flag, 0, tm1, tm2);
    POSIX_UNLOCK();

    return(ret);
}

size_t DARSHAN_DECL(fread)(void *ptr, size_t size, size_t nmemb, FILE *stream)
{
    size_t ret;
    int aligned_flag = 0;
    double tm1, tm2;

    MAP_OR_FAIL(fread);

    /* if((unsigned long)ptr % darshan_mem_alignment == 0) aligned_flag = 1; */

    tm1 = darshan_core_wtime();
    ret = __real_fread(ptr, size, nmemb, stream);
    tm2 = darshan_core_wtime();

    POSIX_LOCK();
    posix_runtime_initialize();
    if(ret > 0)
    {
        POSIX_RECORD_READ(size*ret, fileno(stream), 0, 0,
            aligned_flag, 1, tm1, tm2);
    }
    else
    {
        POSIX_RECORD_READ(ret, fileno(stream), 0, 0,
            aligned_flag, 1, tm1, tm2);
    }
    POSIX_UNLOCK();

    return(ret);
}

size_t DARSHAN_DECL(fwrite)(const void *ptr, size_t size, size_t nmemb, FILE *stream)
{
    size_t ret;
    int aligned_flag = 0;
    double tm1, tm2;

    MAP_OR_FAIL(fwrite);

    /* if((unsigned long)ptr % darshan_mem_alignment == 0) aligned_flag = 1; */

    tm1 = darshan_core_wtime();
    ret = __real_fwrite(ptr, size, nmemb, stream);
    tm2 = darshan_core_wtime();

    POSIX_LOCK();
    posix_runtime_initialize();
    if(ret > 0)
    {
        POSIX_RECORD_WRITE(size*ret, fileno(stream), 0, 0,
            aligned_flag, 1, tm1, tm2);
    }
    else
    {
        POSIX_RECORD_WRITE(ret, fileno(stream), 0, 0,
            aligned_flag, 1, tm1, tm2);
    }
    POSIX_UNLOCK();

    return(ret);
}

off_t DARSHAN_DECL(lseek)(int fd, off_t offset, int whence)
{
    off_t ret;
    struct posix_file_runtime* file;
    double tm1, tm2;

    MAP_OR_FAIL(lseek);

    tm1 = darshan_core_wtime();
    ret = __real_lseek(fd, offset, whence);
    tm2 = darshan_core_wtime();

    if(ret >= 0)
    {
        POSIX_LOCK();
        posix_runtime_initialize();
        file = posix_file_by_fd(fd);
        if(file)
        {
            file->offset = ret;
            DARSHAN_COUNTER_F_INC_NO_OVERLAP(file->file_record,
                tm1, tm2, file->last_meta_end, POSIX_F_META_TIME);
            DARSHAN_COUNTER_INC(file->file_record, POSIX_SEEKS, 1);
        }
        POSIX_UNLOCK();
    }

    return(ret);
}

off_t DARSHAN_DECL(lseek64)(int fd, off_t offset, int whence)
{
    off_t ret;
    struct posix_file_runtime* file;
    double tm1, tm2;

    MAP_OR_FAIL(lseek64);

    tm1 = darshan_core_wtime();
    ret = __real_lseek64(fd, offset, whence);
    tm2 = darshan_core_wtime();

    if(ret >= 0)
    {
        POSIX_LOCK();
        posix_runtime_initialize();
        file = posix_file_by_fd(fd);
        if(file)
        {
            file->offset = ret;
            DARSHAN_COUNTER_F_INC_NO_OVERLAP(file->file_record,
                tm1, tm2, file->last_meta_end, POSIX_F_META_TIME);
            DARSHAN_COUNTER_INC(file->file_record, POSIX_SEEKS, 1);
        }
        POSIX_UNLOCK();
    }

    return(ret);
}

int DARSHAN_DECL(fseek)(FILE *stream, long offset, int whence)
{
    int ret;
    struct posix_file_runtime* file;
    double tm1, tm2;

    MAP_OR_FAIL(fseek);

    tm1 = darshan_core_wtime();
    ret = __real_fseek(stream, offset, whence);
    tm2 = darshan_core_wtime();

    if(ret >= 0)
    {
        POSIX_LOCK();
        posix_runtime_initialize();
        file = posix_file_by_fd(fileno(stream));
        if(file)
        {
            file->offset = ftell(stream);
            DARSHAN_COUNTER_F_INC_NO_OVERLAP(file->file_record,
                tm1, tm2, file->last_meta_end, POSIX_F_META_TIME);
            DARSHAN_COUNTER_INC(file->file_record, POSIX_FSEEKS, 1);
        }
        POSIX_UNLOCK();
    }

    return(ret);
}

int DARSHAN_DECL(__xstat)(int vers, const char *path, struct stat *buf)
{
    int ret;
    double tm1, tm2;

    MAP_OR_FAIL(__xstat);

    tm1 = darshan_core_wtime();
    ret = __real___xstat(vers, path, buf);
    tm2 = darshan_core_wtime();

    if(ret < 0 || !S_ISREG(buf->st_mode))
        return(ret);

    POSIX_LOCK();
    posix_runtime_initialize();
    POSIX_LOOKUP_RECORD_STAT(path, buf, tm1, tm2);
    POSIX_UNLOCK();

    return(ret);
}

int DARSHAN_DECL(__xstat64)(int vers, const char *path, struct stat64 *buf)
{
    int ret;
    double tm1, tm2;

    MAP_OR_FAIL(__xstat64);

    tm1 = darshan_core_wtime();
    ret = __real___xstat64(vers, path, buf);
    tm2 = darshan_core_wtime();

    if(ret < 0 || !S_ISREG(buf->st_mode))
        return(ret);

    POSIX_LOCK();
    posix_runtime_initialize();
    POSIX_LOOKUP_RECORD_STAT(path, buf, tm1, tm2);
    POSIX_UNLOCK();

    return(ret);
}

int DARSHAN_DECL(__lxstat)(int vers, const char *path, struct stat *buf)
{
    int ret;
    double tm1, tm2;

    MAP_OR_FAIL(__lxstat);

    tm1 = darshan_core_wtime();
    ret = __real___lxstat(vers, path, buf);
    tm2 = darshan_core_wtime();

    if(ret < 0 || !S_ISREG(buf->st_mode))
        return(ret);

    POSIX_LOCK();
    posix_runtime_initialize();
    POSIX_LOOKUP_RECORD_STAT(path, buf, tm1, tm2);
    POSIX_UNLOCK();

    return(ret);
}

int DARSHAN_DECL(__lxstat64)(int vers, const char *path, struct stat64 *buf)
{
    int ret;
    double tm1, tm2;

    MAP_OR_FAIL(__lxstat64);

    tm1 = darshan_core_wtime();
    ret = __real___lxstat64(vers, path, buf);
    tm2 = darshan_core_wtime();

    if(ret < 0 || !S_ISREG(buf->st_mode))
        return(ret);

    POSIX_LOCK();
    posix_runtime_initialize();
    POSIX_LOOKUP_RECORD_STAT(path, buf, tm1, tm2);
    POSIX_UNLOCK();

    return(ret);
}

int DARSHAN_DECL(__fxstat)(int vers, int fd, struct stat *buf)
{
    int ret;
    struct posix_file_runtime* file;
    double tm1, tm2;

    MAP_OR_FAIL(__fxstat);

    tm1 = darshan_core_wtime();
    ret = __real___fxstat(vers, fd, buf);
    tm2 = darshan_core_wtime();

    if(ret < 0 || !S_ISREG(buf->st_mode))
        return(ret);

    POSIX_LOCK();
    posix_runtime_initialize();
    file = posix_file_by_fd(fd);
    if(file)
    {
        POSIX_RECORD_STAT(file, buf, tm1, tm2);
    }
    POSIX_UNLOCK();

    return(ret);
}

int DARSHAN_DECL(__fxstat64)(int vers, int fd, struct stat64 *buf)
{
    int ret;
    struct posix_file_runtime* file;
    double tm1, tm2;

    MAP_OR_FAIL(__fxstat64);

    tm1 = darshan_core_wtime();
    ret = __real___fxstat64(vers, fd, buf);
    tm2 = darshan_core_wtime();

    if(ret < 0 || !S_ISREG(buf->st_mode))
        return(ret);

    POSIX_LOCK();
    posix_runtime_initialize();
    file = posix_file_by_fd(fd);
    if(file)
    {
        POSIX_RECORD_STAT(file, buf, tm1, tm2);
    }
    POSIX_UNLOCK();

    return(ret);
}

void* DARSHAN_DECL(mmap)(void *addr, size_t length, int prot, int flags,
    int fd, off_t offset)
{
    void* ret;
    struct posix_file_runtime* file;

    MAP_OR_FAIL(mmap);

    ret = __real_mmap(addr, length, prot, flags, fd, offset);
    if(ret == MAP_FAILED)
        return(ret);

    POSIX_LOCK();
    posix_runtime_initialize();
    file = posix_file_by_fd(fd);
    if(file)
    {
        DARSHAN_COUNTER_INC(file->file_record, POSIX_MMAPS, 1);
    }
    POSIX_UNLOCK();

    return(ret);
}

void* DARSHAN_DECL(mmap64)(void *addr, size_t length, int prot, int flags,
    int fd, off64_t offset)
{
    void* ret;
    struct posix_file_runtime* file;

    MAP_OR_FAIL(mmap64);

    ret = __real_mmap64(addr, length, prot, flags, fd, offset);
    if(ret == MAP_FAILED)
        return(ret);

    POSIX_LOCK();
    posix_runtime_initialize();
    file = posix_file_by_fd(fd);
    if(file)
    {
        DARSHAN_COUNTER_INC(file->file_record, POSIX_MMAPS, 1);
    }
    POSIX_UNLOCK();

    return(ret);
}

int DARSHAN_DECL(fsync)(int fd)
{
    int ret;
    struct posix_file_runtime* file;
    double tm1, tm2;

    MAP_OR_FAIL(fsync);

    tm1 = darshan_core_wtime();
    ret = __real_fsync(fd);
    tm2 = darshan_core_wtime();

    if(ret < 0)
        return(ret);

    POSIX_LOCK();
    posix_runtime_initialize();
    file = posix_file_by_fd(fd);
    if(file)
    {
        DARSHAN_COUNTER_F_INC_NO_OVERLAP(file->file_record,
            tm1, tm2, file->last_write_end, POSIX_F_WRITE_TIME);
        DARSHAN_COUNTER_INC(file->file_record, POSIX_FSYNCS, 1);
    }
    POSIX_UNLOCK();

    return(ret);
}

int DARSHAN_DECL(fdatasync)(int fd)
{
    int ret;
    struct posix_file_runtime* file;
    double tm1, tm2;

    MAP_OR_FAIL(fdatasync);

    tm1 = darshan_core_wtime();
    ret = __real_fdatasync(fd);
    tm2 = darshan_core_wtime();

    if(ret < 0)
        return(ret);

    POSIX_LOCK();
    posix_runtime_initialize();
    file = posix_file_by_fd(fd);
    if(file)
    {
        DARSHAN_COUNTER_F_INC_NO_OVERLAP(file->file_record,
            tm1, tm2, file->last_write_end, POSIX_F_WRITE_TIME); 
        DARSHAN_COUNTER_INC(file->file_record, POSIX_FDSYNCS, 1);
    }
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
        file->last_byte_written = 0;
        file->last_byte_read = 0;
        DARSHAN_COUNTER_F_SET(file->file_record,
            POSIX_F_CLOSE_TIMESTAMP, darshan_core_wtime());
        DARSHAN_COUNTER_F_INC_NO_OVERLAP(file->file_record,
             tm1, tm2, file->last_meta_end, POSIX_F_META_TIME);
        posix_file_close_fd(fd);
    }
    POSIX_UNLOCK();    

    return(ret);
}

int DARSHAN_DECL(fclose)(FILE *fp)
{
    struct posix_file_runtime* file;
    int fd = fileno(fp);
    double tm1, tm2;
    int ret;

    MAP_OR_FAIL(fclose);

    tm1 = darshan_core_wtime();
    ret = __real_fclose(fp);
    tm2 = darshan_core_wtime();

    POSIX_LOCK();
    posix_runtime_initialize();
    file = posix_file_by_fd(fd);
    if(file)
    {
        file->last_byte_written = 0;
        file->last_byte_read = 0;
        DARSHAN_COUNTER_F_SET(file->file_record,
            POSIX_F_CLOSE_TIMESTAMP, darshan_core_wtime());
        DARSHAN_COUNTER_F_INC_NO_OVERLAP(file->file_record,
            tm1, tm2, file->last_meta_end, POSIX_F_META_TIME);
        posix_file_close_fd(fd);
    }
    POSIX_UNLOCK();

    return(ret);
}

/**********************************************************
 * Internal functions for manipulating POSIX module state *
 **********************************************************/

/* initialize internal POSIX module data structures and register with darshan-core */
static void posix_runtime_initialize()
{
    int mem_limit;
    struct darshan_module_funcs posix_mod_fns =
    {
        .disable_instrumentation = &posix_disable_instrumentation,
        .prepare_for_reduction = &posix_prepare_for_reduction,
        .record_reduction_op = &posix_record_reduction_op,
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
        &mem_limit,
        &darshan_mem_alignment);

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

/* get a POSIX file record for the given file path */
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

/* get a POSIX file record for the given file path, and also create a
 * reference structure using the returned file descriptor
 */
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

/* get a POSIX file record for the given file descriptor */
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

/* free up reference data structures for the given file descriptor */
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

/* compare function for sorting file records by descending rank */
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

/************************************************************************
 * Functions exported by this module for coordinating with darshan-core *
 ************************************************************************/

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
    int i;
    double posix_time;

    assert(posix_runtime);

    /* necessary initialization of shared records (e.g., change rank to -1) */
    for(i = 0; i < *shared_rec_count; i++)
    {
        HASH_FIND(hlink, posix_runtime->file_hash, &shared_recs[i],
            sizeof(darshan_record_id), file);
        assert(file);

        posix_time =
            file->file_record->fcounters[POSIX_F_READ_TIME] +
            file->file_record->fcounters[POSIX_F_WRITE_TIME] +
            file->file_record->fcounters[POSIX_F_META_TIME];

        /* initialize fastest/slowest info prior to the reduction */
        file->file_record->counters[POSIX_FASTEST_RANK] =
            file->file_record->rank;
        file->file_record->counters[POSIX_FASTEST_RANK_BYTES] = 
            file->file_record->counters[POSIX_BYTES_READ] +
            file->file_record->counters[POSIX_BYTES_WRITTEN];
        file->file_record->fcounters[POSIX_F_FASTEST_RANK_TIME] = 
            posix_time;

        /* until reduction occurs, we assume that this rank is both
         * the fastest and slowest. It is up to the reduction operator
         * to find the true min and max.
         */
        file->file_record->counters[POSIX_SLOWEST_RANK] =
            file->file_record->counters[POSIX_FASTEST_RANK];
        file->file_record->counters[POSIX_SLOWEST_RANK_BYTES] =
            file->file_record->counters[POSIX_FASTEST_RANK_BYTES];
        file->file_record->fcounters[POSIX_F_SLOWEST_RANK_TIME] =
            file->file_record->fcounters[POSIX_F_FASTEST_RANK_TIME];

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

static void posix_record_reduction_op(
    void* infile_v,
    void* inoutfile_v,
    int *len,
    MPI_Datatype *datatype)
{
    struct darshan_posix_file tmp_file;
    struct darshan_posix_file *infile = infile_v;
    struct darshan_posix_file *inoutfile = inoutfile_v;
    int i;
    int j;

    assert(posix_runtime);

    for(i=0; i<*len; i++)
    {
        memset(&tmp_file, 0, sizeof(struct darshan_posix_file));

        tmp_file.f_id = infile->f_id;
        tmp_file.rank = -1;

        /* sum */
        for(j=POSIX_OPENS; j<=POSIX_FDSYNCS; j++)
        {
            tmp_file.counters[j] = infile->counters[j] + inoutfile->counters[j];
        }

        tmp_file.counters[POSIX_MODE] = infile->counters[POSIX_MODE];

        /* sum */
        for(j=POSIX_BYTES_READ; j<=POSIX_BYTES_WRITTEN; j++)
        {
            tmp_file.counters[j] = infile->counters[j] + inoutfile->counters[j];
        }

        /* max */
        for(j=POSIX_MAX_BYTE_READ; j<=POSIX_MAX_BYTE_WRITTEN; j++)
        {
            tmp_file.counters[j] = (
                (infile->counters[j] > inoutfile->counters[j]) ?
                infile->counters[j] :
                inoutfile->counters[j]);
        }

        /* sum */
        for(j=POSIX_CONSEC_READS; j<=POSIX_RW_SWITCHES; j++)
        {
            tmp_file.counters[j] = infile->counters[j] +
                inoutfile->counters[j];
        }

        /* min non-zero (if available) value */
        for(j=POSIX_F_OPEN_TIMESTAMP; j<=POSIX_F_WRITE_START_TIMESTAMP; j++)
        {
            if(infile->fcounters[j] > inoutfile->fcounters[j] && inoutfile->fcounters[j] > 0)
                tmp_file.fcounters[j] = inoutfile->fcounters[j];
            else
                tmp_file.fcounters[j] = infile->fcounters[j];
        }

        /* max */
        for(j=POSIX_F_READ_END_TIMESTAMP; j<=POSIX_F_CLOSE_TIMESTAMP; j++)
        {
            if(infile->fcounters[j] > inoutfile->fcounters[j])
                tmp_file.fcounters[j] = infile->fcounters[j];
            else
                tmp_file.fcounters[j] = inoutfile->fcounters[j];
        }

        /* sum */
        for(j=POSIX_F_READ_TIME; j<=POSIX_F_META_TIME; j++)
        {
            tmp_file.fcounters[j] = infile->fcounters[j] + inoutfile->fcounters[j];
        }

        /* max (special case) */
        if(infile->fcounters[POSIX_F_MAX_READ_TIME] >
            inoutfile->fcounters[POSIX_F_MAX_READ_TIME])
        {
            tmp_file.fcounters[POSIX_F_MAX_READ_TIME] =
                infile->fcounters[POSIX_F_MAX_READ_TIME];
            tmp_file.counters[POSIX_MAX_READ_TIME_SIZE] =
                infile->counters[POSIX_MAX_READ_TIME_SIZE];
        }
        else
        {
            tmp_file.fcounters[POSIX_F_MAX_READ_TIME] =
                inoutfile->fcounters[POSIX_F_MAX_READ_TIME];
            tmp_file.counters[POSIX_MAX_READ_TIME_SIZE] =
                inoutfile->counters[POSIX_MAX_READ_TIME_SIZE];
        }

        if(infile->fcounters[POSIX_F_MAX_WRITE_TIME] >
            inoutfile->fcounters[POSIX_F_MAX_WRITE_TIME])
        {
            tmp_file.fcounters[POSIX_F_MAX_WRITE_TIME] =
                infile->fcounters[POSIX_F_MAX_WRITE_TIME];
            tmp_file.counters[POSIX_MAX_WRITE_TIME_SIZE] =
                infile->counters[POSIX_MAX_WRITE_TIME_SIZE];
        }
        else
        {
            tmp_file.fcounters[POSIX_F_MAX_WRITE_TIME] =
                inoutfile->fcounters[POSIX_F_MAX_WRITE_TIME];
            tmp_file.counters[POSIX_MAX_WRITE_TIME_SIZE] =
                inoutfile->counters[POSIX_MAX_WRITE_TIME_SIZE];
        }

        /* min (zeroes are ok here; some procs don't do I/O) */
        if(infile->fcounters[POSIX_F_FASTEST_RANK_TIME] <
           inoutfile->fcounters[POSIX_F_FASTEST_RANK_TIME])
        {
            tmp_file.counters[POSIX_FASTEST_RANK] =
                infile->counters[POSIX_FASTEST_RANK];
            tmp_file.counters[POSIX_FASTEST_RANK_BYTES] =
                infile->counters[POSIX_FASTEST_RANK_BYTES];
            tmp_file.fcounters[POSIX_F_FASTEST_RANK_TIME] =
                infile->fcounters[POSIX_F_FASTEST_RANK_TIME];
        }
        else
        {
            tmp_file.counters[POSIX_FASTEST_RANK] =
                inoutfile->counters[POSIX_FASTEST_RANK];
            tmp_file.counters[POSIX_FASTEST_RANK_BYTES] =
                inoutfile->counters[POSIX_FASTEST_RANK_BYTES];
            tmp_file.fcounters[POSIX_F_FASTEST_RANK_TIME] =
                inoutfile->fcounters[POSIX_F_FASTEST_RANK_TIME];
        }

        /* max */
        if(infile->fcounters[POSIX_F_SLOWEST_RANK_TIME] >
           inoutfile->fcounters[POSIX_F_SLOWEST_RANK_TIME])
        {
            tmp_file.counters[POSIX_SLOWEST_RANK] =
                infile->counters[POSIX_SLOWEST_RANK];
            tmp_file.counters[POSIX_SLOWEST_RANK_BYTES] =
                infile->counters[POSIX_SLOWEST_RANK_BYTES];
            tmp_file.fcounters[POSIX_F_SLOWEST_RANK_TIME] =
                infile->fcounters[POSIX_F_SLOWEST_RANK_TIME];
        }
        else
        {
            tmp_file.counters[POSIX_SLOWEST_RANK] =
                inoutfile->counters[POSIX_SLOWEST_RANK];
            tmp_file.counters[POSIX_SLOWEST_RANK_BYTES] =
                inoutfile->counters[POSIX_SLOWEST_RANK_BYTES];
            tmp_file.fcounters[POSIX_F_SLOWEST_RANK_TIME] =
                inoutfile->fcounters[POSIX_F_SLOWEST_RANK_TIME];
        }

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
