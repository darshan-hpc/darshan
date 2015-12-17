/*
 * Copyright (C) 2015 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

#define _XOPEN_SOURCE 500
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
#include "utlist.h"

#include "darshan.h"
#include "darshan-dynamic.h"

#ifndef HAVE_OFF64_T
typedef int64_t off64_t;
#endif
#ifndef HAVE_AIOCB64
#define aiocb64 aiocb
#endif

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
DARSHAN_FORWARD_DECL(aio_read, int, (struct aiocb *aiocbp));
DARSHAN_FORWARD_DECL(aio_write, int, (struct aiocb *aiocbp));
DARSHAN_FORWARD_DECL(aio_read64, int, (struct aiocb64 *aiocbp));
DARSHAN_FORWARD_DECL(aio_write64, int, (struct aiocb64 *aiocbp));
DARSHAN_FORWARD_DECL(aio_return, ssize_t, (struct aiocb *aiocbp));
DARSHAN_FORWARD_DECL(aio_return64, ssize_t, (struct aiocb64 *aiocbp));
DARSHAN_FORWARD_DECL(lio_listio, int, (int mode, struct aiocb *const aiocb_list[], int nitems, struct sigevent *sevp));
DARSHAN_FORWARD_DECL(lio_listio64, int, (int mode, struct aiocb64 *const aiocb_list[], int nitems, struct sigevent *sevp));

/* struct to track information about aio operations in flight */
struct posix_aio_tracker
{
    double tm1;
    void *aiocbp;
    struct posix_aio_tracker* next;
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
    enum darshan_io_type last_io_type;
    double last_meta_end;
    double last_read_end;
    double last_write_end;
    void* access_root;
    int access_count;
    void* stride_root;
    int stride_count;
    struct posix_aio_tracker* aio_list;
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
};

static struct posix_runtime *posix_runtime = NULL;
static pthread_mutex_t posix_runtime_mutex = PTHREAD_RECURSIVE_MUTEX_INITIALIZER_NP;
static int instrumentation_disabled = 0;
static int my_rank = -1;
static int darshan_mem_alignment = 1;

static void posix_runtime_initialize(void);
static struct posix_file_runtime* posix_file_by_name(const char *name);
static struct posix_file_runtime* posix_file_by_name_setfd(const char* name, int fd);
static struct posix_file_runtime* posix_file_by_fd(int fd);
static void posix_file_close_fd(int fd);
static void posix_aio_tracker_add(int fd, void *aiocbp);
static struct posix_aio_tracker* posix_aio_tracker_del(int fd, void *aiocbp);
static int posix_record_compare(const void* a, const void* b);
static void posix_record_reduction_op(void* infile_v, void* inoutfile_v,
    int *len, MPI_Datatype *datatype);
static void posix_shared_record_variance(MPI_Comm mod_comm,
    struct darshan_posix_file *inrec_array, struct darshan_posix_file *outrec_array,
    int shared_rec_count);

static void posix_begin_shutdown(void);
static void posix_get_output_data(MPI_Comm mod_comm, darshan_record_id *shared_recs,
    int shared_rec_count, void **posix_buf, int *posix_buf_sz);
static void posix_shutdown(void);

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
    if(__mode) \
        file->file_record->counters[POSIX_MODE] = __mode; \
    file->offset = 0; \
    file->last_byte_written = 0; \
    file->last_byte_read = 0; \
    if(__stream_flag)\
        file->file_record->counters[POSIX_FOPENS] += 1; \
    else \
        file->file_record->counters[POSIX_OPENS] += 1; \
    if(file->file_record->fcounters[POSIX_F_OPEN_TIMESTAMP] == 0) \
        file->file_record->fcounters[POSIX_F_OPEN_TIMESTAMP] = __tm1; \
    DARSHAN_TIMER_INC_NO_OVERLAP(file->file_record->fcounters[POSIX_F_META_TIME], __tm1, __tm2, file->last_meta_end); \
} while(0)

#define POSIX_RECORD_READ(__ret, __fd, __pread_flag, __pread_offset, __aligned, __stream_flag, __tm1, __tm2) do{ \
    size_t stride; \
    int64_t this_offset; \
    struct posix_file_runtime* file; \
    int64_t file_alignment; \
    double __elapsed = __tm2-__tm1; \
    if(__ret < 0) break; \
    file = posix_file_by_fd(__fd); \
    if(!file) break; \
    if(__pread_flag) \
        this_offset = __pread_offset; \
    else \
        this_offset = file->offset; \
    if(this_offset > file->last_byte_read) \
        file->file_record->counters[POSIX_SEQ_READS] += 1;  \
    if(this_offset == (file->last_byte_read + 1)) \
        file->file_record->counters[POSIX_CONSEC_READS] += 1;  \
    if(this_offset > 0 && this_offset > file->last_byte_read \
        && file->last_byte_read != 0) \
        stride = this_offset - file->last_byte_read - 1; \
    else \
        stride = 0; \
    file->last_byte_read = this_offset + __ret - 1; \
    file->offset = this_offset + __ret; \
    if(file->file_record->counters[POSIX_MAX_BYTE_READ] < (this_offset + __ret - 1)) \
        file->file_record->counters[POSIX_MAX_BYTE_READ] = (this_offset + __ret - 1); \
    file->file_record->counters[POSIX_BYTES_READ] += __ret; \
    if(__stream_flag) \
        file->file_record->counters[POSIX_FREADS] += 1; \
    else \
        file->file_record->counters[POSIX_READS] += 1; \
    DARSHAN_BUCKET_INC(&(file->file_record->counters[POSIX_SIZE_READ_0_100]), __ret); \
    darshan_common_val_counter(&file->access_root, &file->access_count, __ret); \
    darshan_common_val_counter(&file->stride_root, &file->stride_count, stride); \
    if(!__aligned) \
        file->file_record->counters[POSIX_MEM_NOT_ALIGNED] += 1; \
    file_alignment = file->file_record->counters[POSIX_FILE_ALIGNMENT]; \
    if(file_alignment > 0 && (this_offset % file_alignment) != 0) \
        file->file_record->counters[POSIX_FILE_NOT_ALIGNED] += 1; \
    if(file->last_io_type == DARSHAN_IO_WRITE) \
        file->file_record->counters[POSIX_RW_SWITCHES] += 1; \
    file->last_io_type = DARSHAN_IO_READ; \
    if(file->file_record->fcounters[POSIX_F_READ_START_TIMESTAMP] == 0) \
        file->file_record->fcounters[POSIX_F_READ_START_TIMESTAMP] = __tm1; \
    file->file_record->fcounters[POSIX_F_READ_END_TIMESTAMP] = __tm2; \
    if(file->file_record->fcounters[POSIX_F_MAX_READ_TIME] < __elapsed) { \
        file->file_record->fcounters[POSIX_F_MAX_READ_TIME] = __elapsed; \
        file->file_record->counters[POSIX_MAX_READ_TIME_SIZE] = __ret; } \
    DARSHAN_TIMER_INC_NO_OVERLAP(file->file_record->fcounters[POSIX_F_READ_TIME], __tm1, __tm2, file->last_read_end); \
} while(0)

#define POSIX_RECORD_WRITE(__ret, __fd, __pwrite_flag, __pwrite_offset, __aligned, __stream_flag, __tm1, __tm2) do{ \
    size_t stride; \
    int64_t this_offset; \
    struct posix_file_runtime* file; \
    int64_t file_alignment; \
    double __elapsed = __tm2-__tm1; \
    if(__ret < 0) break; \
    file = posix_file_by_fd(__fd); \
    if(!file) break; \
    if(__pwrite_flag) \
        this_offset = __pwrite_offset; \
    else \
        this_offset = file->offset; \
    if(this_offset > file->last_byte_written) \
        file->file_record->counters[POSIX_SEQ_WRITES] += 1; \
    if(this_offset == (file->last_byte_written + 1)) \
        file->file_record->counters[POSIX_CONSEC_WRITES] += 1; \
    if(this_offset > 0 && this_offset > file->last_byte_written \
        && file->last_byte_written != 0) \
        stride = this_offset - file->last_byte_written - 1; \
    else \
        stride = 0; \
    file->last_byte_written = this_offset + __ret - 1; \
    file->offset = this_offset + __ret; \
    if(file->file_record->counters[POSIX_MAX_BYTE_WRITTEN] < (this_offset + __ret - 1)) \
        file->file_record->counters[POSIX_MAX_BYTE_WRITTEN] = (this_offset + __ret - 1); \
    file->file_record->counters[POSIX_BYTES_WRITTEN] += __ret; \
    if(__stream_flag) \
        file->file_record->counters[POSIX_FWRITES] += 1; \
    else \
        file->file_record->counters[POSIX_WRITES] += 1; \
    DARSHAN_BUCKET_INC(&(file->file_record->counters[POSIX_SIZE_WRITE_0_100]), __ret); \
    darshan_common_val_counter(&file->access_root, &file->access_count, __ret); \
    darshan_common_val_counter(&file->stride_root, &file->stride_count, stride); \
    if(!__aligned) \
        file->file_record->counters[POSIX_MEM_NOT_ALIGNED] += 1; \
    file_alignment = file->file_record->counters[POSIX_FILE_ALIGNMENT]; \
    if(file_alignment > 0 && (this_offset % file_alignment) != 0) \
        file->file_record->counters[POSIX_FILE_NOT_ALIGNED] += 1; \
    if(file->last_io_type == DARSHAN_IO_READ) \
        file->file_record->counters[POSIX_RW_SWITCHES] += 1; \
    file->last_io_type = DARSHAN_IO_WRITE; \
    if(file->file_record->fcounters[POSIX_F_WRITE_START_TIMESTAMP] == 0) \
        file->file_record->fcounters[POSIX_F_WRITE_START_TIMESTAMP] = __tm1; \
    file->file_record->fcounters[POSIX_F_WRITE_END_TIMESTAMP] = __tm2; \
    if(file->file_record->fcounters[POSIX_F_MAX_WRITE_TIME] < __elapsed) { \
        file->file_record->fcounters[POSIX_F_MAX_WRITE_TIME] = __elapsed; \
        file->file_record->counters[POSIX_MAX_WRITE_TIME_SIZE] = __ret; } \
    DARSHAN_TIMER_INC_NO_OVERLAP(file->file_record->fcounters[POSIX_F_WRITE_TIME], __tm1, __tm2, file->last_write_end); \
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
    DARSHAN_TIMER_INC_NO_OVERLAP((__file)->file_record->fcounters[POSIX_F_META_TIME], __tm1, __tm2, (__file)->last_meta_end); \
    (__file)->file_record->counters[POSIX_STATS] += 1; \
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

    MAP_OR_FAIL(read);

    if((unsigned long)buf % darshan_mem_alignment == 0) aligned_flag = 1;

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

    if((unsigned long)buf % darshan_mem_alignment == 0) aligned_flag = 1;

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

    if((unsigned long)buf % darshan_mem_alignment == 0) aligned_flag = 1;

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

    if((unsigned long)buf % darshan_mem_alignment == 0) aligned_flag = 1;

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

    if((unsigned long)buf % darshan_mem_alignment == 0) aligned_flag = 1;

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

    if((unsigned long)buf % darshan_mem_alignment == 0) aligned_flag = 1;

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
    int aligned_flag = 1;
    int i;
    double tm1, tm2;

    MAP_OR_FAIL(readv);

    for(i=0; i<iovcnt; i++)
    {
        if(((unsigned long)iov[i].iov_base % darshan_mem_alignment) != 0)
            aligned_flag = 0;
    }

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
    int aligned_flag = 1;
    int i;
    double tm1, tm2;

    MAP_OR_FAIL(writev);

    for(i=0; i<iovcnt; i++)
    {
        if(((unsigned long)iov[i].iov_base % darshan_mem_alignment) != 0)
            aligned_flag = 0;
    }

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

    if((unsigned long)ptr % darshan_mem_alignment == 0) aligned_flag = 1;

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

    if((unsigned long)ptr % darshan_mem_alignment == 0) aligned_flag = 1;

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
            DARSHAN_TIMER_INC_NO_OVERLAP(
                file->file_record->fcounters[POSIX_F_META_TIME],
                tm1, tm2, file->last_meta_end);
            file->file_record->counters[POSIX_SEEKS] += 1;
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
            DARSHAN_TIMER_INC_NO_OVERLAP(
                file->file_record->fcounters[POSIX_F_META_TIME],
                tm1, tm2, file->last_meta_end);
            file->file_record->counters[POSIX_SEEKS] += 1;
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
            DARSHAN_TIMER_INC_NO_OVERLAP(
                file->file_record->fcounters[POSIX_F_META_TIME],
                tm1, tm2, file->last_meta_end);
            file->file_record->counters[POSIX_FSEEKS] += 1;
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
        file->file_record->counters[POSIX_MMAPS] += 1;
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
        file->file_record->counters[POSIX_MMAPS] += 1;
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
        DARSHAN_TIMER_INC_NO_OVERLAP(
            file->file_record->fcounters[POSIX_F_WRITE_TIME],
            tm1, tm2, file->last_write_end);
        file->file_record->counters[POSIX_FSYNCS] += 1;
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
        DARSHAN_TIMER_INC_NO_OVERLAP(
            file->file_record->fcounters[POSIX_F_WRITE_TIME],
            tm1, tm2, file->last_write_end);
        file->file_record->counters[POSIX_FDSYNCS] += 1;
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
        file->file_record->fcounters[POSIX_F_CLOSE_TIMESTAMP] =
            darshan_core_wtime();
        DARSHAN_TIMER_INC_NO_OVERLAP(
            file->file_record->fcounters[POSIX_F_META_TIME],
            tm1, tm2, file->last_meta_end);
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
        file->file_record->fcounters[POSIX_F_CLOSE_TIMESTAMP] =
            darshan_core_wtime();
        DARSHAN_TIMER_INC_NO_OVERLAP(
            file->file_record->fcounters[POSIX_F_META_TIME],
            tm1, tm2, file->last_meta_end);
        posix_file_close_fd(fd);
    }
    POSIX_UNLOCK();

    return(ret);
}

int DARSHAN_DECL(aio_read)(struct aiocb *aiocbp)
{
    int ret;

    MAP_OR_FAIL(aio_read);

    ret = __real_aio_read(aiocbp);
    if(ret == 0)
    {
        POSIX_LOCK();
        posix_runtime_initialize();
        posix_aio_tracker_add(aiocbp->aio_fildes, aiocbp);
        POSIX_UNLOCK();
    }

    return(ret);
}

int DARSHAN_DECL(aio_write)(struct aiocb *aiocbp)
{
    int ret;

    MAP_OR_FAIL(aio_write);

    ret = __real_aio_write(aiocbp);
    if(ret == 0)
    {
        POSIX_LOCK();
        posix_runtime_initialize();
        posix_aio_tracker_add(aiocbp->aio_fildes, aiocbp);
        POSIX_UNLOCK();
    }

    return(ret);
}

int DARSHAN_DECL(aio_read64)(struct aiocb64 *aiocbp)
{
    int ret;

    MAP_OR_FAIL(aio_read64);

    ret = __real_aio_read64(aiocbp);
    if(ret == 0)
    {
        POSIX_LOCK();
        posix_runtime_initialize();
        posix_aio_tracker_add(aiocbp->aio_fildes, aiocbp);
        POSIX_UNLOCK();
    }

    return(ret);
}

int DARSHAN_DECL(aio_write64)(struct aiocb64 *aiocbp)
{
    int ret;

    MAP_OR_FAIL(aio_write64);

    ret = __real_aio_write64(aiocbp);
    if(ret == 0)
    {
        POSIX_LOCK();
        posix_runtime_initialize();
        posix_aio_tracker_add(aiocbp->aio_fildes, aiocbp);
        POSIX_UNLOCK();
    }

    return(ret);
}

ssize_t DARSHAN_DECL(aio_return)(struct aiocb *aiocbp)
{
    int ret;
    double tm2;
    struct posix_aio_tracker *tmp;
    int aligned_flag = 0;

    MAP_OR_FAIL(aio_return);

    ret = __real_aio_return(aiocbp);
    tm2 = darshan_core_wtime();

    POSIX_LOCK();
    posix_runtime_initialize();
    tmp = posix_aio_tracker_del(aiocbp->aio_fildes, aiocbp);
    if (tmp)
    {
        if((unsigned long)aiocbp->aio_buf % darshan_mem_alignment == 0)
            aligned_flag = 1;
        if(aiocbp->aio_lio_opcode == LIO_WRITE)
        {
            POSIX_RECORD_WRITE(ret, aiocbp->aio_fildes,
                1, aiocbp->aio_offset, aligned_flag, 0,
                tmp->tm1, tm2);
        }
        else if(aiocbp->aio_lio_opcode == LIO_READ)
        {
            POSIX_RECORD_READ(ret, aiocbp->aio_fildes,
                1, aiocbp->aio_offset, aligned_flag, 0,
                tmp->tm1, tm2);
        }
        free(tmp);
    }
    POSIX_UNLOCK();

    return(ret);
}

ssize_t DARSHAN_DECL(aio_return64)(struct aiocb64 *aiocbp)
{
    int ret;
    double tm2;
    struct posix_aio_tracker *tmp;
    int aligned_flag = 0;

    MAP_OR_FAIL(aio_return64);

    ret = __real_aio_return64(aiocbp);
    tm2 = darshan_core_wtime();

    POSIX_LOCK();
    posix_runtime_initialize();
    tmp = posix_aio_tracker_del(aiocbp->aio_fildes, aiocbp);
    if (tmp)
    {
        if((unsigned long)aiocbp->aio_buf % darshan_mem_alignment == 0)
            aligned_flag = 1;
        if(aiocbp->aio_lio_opcode == LIO_WRITE)
        {
            POSIX_RECORD_WRITE(ret, aiocbp->aio_fildes,
                1, aiocbp->aio_offset, aligned_flag, 0,
                tmp->tm1, tm2);
        }
        else if(aiocbp->aio_lio_opcode == LIO_READ)
        {
            POSIX_RECORD_READ(ret, aiocbp->aio_fildes,
                1, aiocbp->aio_offset, aligned_flag, 0,
                tmp->tm1, tm2);
        }
        free(tmp);
    }
    POSIX_UNLOCK();

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
        POSIX_LOCK();
        posix_runtime_initialize();
        for(i = 0; i < nitems; i++)
        {
            posix_aio_tracker_add(aiocb_list[i]->aio_fildes, aiocb_list[i]);
        }
        POSIX_UNLOCK();
    }

    return(ret);
}

int DARSHAN_DECL(lio_listio64)(int mode, struct aiocb64 *const aiocb_list[],
    int nitems, struct sigevent *sevp)
{
    int ret;
    int i;

    MAP_OR_FAIL(lio_listio64);

    ret = __real_lio_listio64(mode, aiocb_list, nitems, sevp);
    if(ret == 0)
    {
        POSIX_LOCK();
        posix_runtime_initialize();
        for(i = 0; i < nitems; i++)
        {
            posix_aio_tracker_add(aiocb_list[i]->aio_fildes, aiocb_list[i]);
        }
        POSIX_UNLOCK();
    }

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
        .begin_shutdown = &posix_begin_shutdown,
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
        &my_rank,
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

    return;
}

/* get a POSIX file record for the given file path */
static struct posix_file_runtime* posix_file_by_name(const char *name)
{
    struct posix_file_runtime *file = NULL;
    char *newname = NULL;
    darshan_record_id file_id;
    int file_alignment;
    int limit_flag;

    if(!posix_runtime || instrumentation_disabled)
        return(NULL);

    newname = darshan_clean_file_path(name);
    if(!newname)
        newname = (char*)name;

    limit_flag = (posix_runtime->file_array_ndx >= posix_runtime->file_array_size);

    /* get a unique id for this file from darshan core */
    darshan_core_register_record(
        (void*)newname,
        strlen(newname),
        DARSHAN_POSIX_MOD,
        1,
        limit_flag,
        &file_id,
        &file_alignment);

    /* the file record id is set to 0 if no memory is available for tracking
     * new records -- just fall through and ignore this record
     */
    if(file_id == 0)
    {
        if(newname != name)
            free(newname);
        return(NULL);
    }

    /* search the hash table for this file record, and return if found */
    HASH_FIND(hlink, posix_runtime->file_hash, &file_id, sizeof(darshan_record_id), file);
    if(file)
    {
        if(newname != name)
            free(newname);
        return(file);
    }

    /* no existing record, assign a new file record from the global array */
    file = &(posix_runtime->file_runtime_array[posix_runtime->file_array_ndx]);
    file->file_record = &(posix_runtime->file_record_array[posix_runtime->file_array_ndx]);
    file->file_record->f_id = file_id;
    file->file_record->rank = my_rank;
    file->file_record->counters[POSIX_MEM_ALIGNMENT] = darshan_mem_alignment;
    file->file_record->counters[POSIX_FILE_ALIGNMENT] = file_alignment;

    /* add new record to file hash table */
    HASH_ADD(hlink, posix_runtime->file_hash, file_record->f_id, sizeof(darshan_record_id), file);
    posix_runtime->file_array_ndx++;

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
static int posix_record_compare(const void* a_p, const void* b_p)
{
    const struct darshan_posix_file* a = a_p;
    const struct darshan_posix_file* b = b_p;

    if(a->rank < b->rank)
        return 1;
    if(a->rank > b->rank)
        return -1;

    return 0;
}

/* finds the tracker structure for a given aio operation, removes it from
 * the linked list for the darshan_file structure, and returns a pointer.  
 *
 * returns NULL if aio operation not found
 */
static struct posix_aio_tracker* posix_aio_tracker_del(int fd, void *aiocbp)
{
    struct posix_aio_tracker *tracker = NULL, *iter, *tmp;
    struct posix_file_runtime* file;

    file = posix_file_by_fd(fd);
    if (file)
    {
        LL_FOREACH_SAFE(file->aio_list, iter, tmp)
        {
            if (iter->aiocbp == aiocbp)
            {
                LL_DELETE(file->aio_list, iter);
                tracker = iter;
                break;
            }
        }
    }

    return(tracker);
}

/* adds a tracker for the given aio operation */
static void posix_aio_tracker_add(int fd, void *aiocbp)
{
    struct posix_aio_tracker* tracker;
    struct posix_file_runtime* file;

    file = posix_file_by_fd(fd);
    if (file)
    {
        tracker = malloc(sizeof(*tracker));
        if (tracker)
        {
            tracker->tm1 = darshan_core_wtime();
            tracker->aiocbp = aiocbp;
            LL_PREPEND(file->aio_list, tracker);
        }
    }

    return;
}

static void posix_record_reduction_op(void* infile_v, void* inoutfile_v,
    int *len, MPI_Datatype *datatype)
{
    struct darshan_posix_file tmp_file;
    struct darshan_posix_file *infile = infile_v;
    struct darshan_posix_file *inoutfile = inoutfile_v;
    int i, j, k;

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
        for(j=POSIX_CONSEC_READS; j<=POSIX_MEM_NOT_ALIGNED; j++)
        {
            tmp_file.counters[j] = infile->counters[j] + inoutfile->counters[j];
        }

        tmp_file.counters[POSIX_MEM_ALIGNMENT] = infile->counters[POSIX_MEM_ALIGNMENT];

        /* sum */
        for(j=POSIX_FILE_NOT_ALIGNED; j<=POSIX_FILE_NOT_ALIGNED; j++)
        {
            tmp_file.counters[j] = infile->counters[j] + inoutfile->counters[j];
        }

        tmp_file.counters[POSIX_FILE_ALIGNMENT] = infile->counters[POSIX_FILE_ALIGNMENT];

        /* skip POSIX_MAX_*_TIME_SIZE; handled in floating point section */

        for(j=POSIX_SIZE_READ_0_100; j<=POSIX_SIZE_WRITE_1G_PLUS; j++)
        {
            tmp_file.counters[j] = infile->counters[j] + inoutfile->counters[j];
        }

        /* first collapse any duplicates */
        for(j=POSIX_STRIDE1_STRIDE; j<=POSIX_STRIDE4_STRIDE; j++)
        {
            for(k=POSIX_STRIDE1_STRIDE; k<=POSIX_STRIDE4_STRIDE; k++)
            {
                if(infile->counters[j] == inoutfile->counters[k])
                {
                    infile->counters[j+4] += inoutfile->counters[k+4];
                    inoutfile->counters[k] = 0;
                    inoutfile->counters[k+4] = 0;
                }
            }
        }

        /* first set */
        for(j=POSIX_STRIDE1_STRIDE; j<=POSIX_STRIDE4_STRIDE; j++)
        {
            DARSHAN_COMMON_VAL_COUNTER_INC(&(tmp_file.counters[POSIX_STRIDE1_STRIDE]),
                &(tmp_file.counters[POSIX_STRIDE1_COUNT]), infile->counters[j],
                infile->counters[j+4]);
        }
        /* second set */
        for(j=POSIX_STRIDE1_STRIDE; j<=POSIX_STRIDE4_STRIDE; j++)
        {
            DARSHAN_COMMON_VAL_COUNTER_INC(&(tmp_file.counters[POSIX_STRIDE1_STRIDE]),
                &(tmp_file.counters[POSIX_STRIDE1_COUNT]), inoutfile->counters[j],
                inoutfile->counters[j+4]);
        }

        /* same for access counts */

        /* first collapse any duplicates */
        for(j=POSIX_ACCESS1_ACCESS; j<=POSIX_ACCESS4_ACCESS; j++)
        {
            for(k=POSIX_ACCESS1_ACCESS; k<=POSIX_ACCESS4_ACCESS; k++)
            {
                if(infile->counters[j] == inoutfile->counters[k])
                {
                    infile->counters[j+4] += inoutfile->counters[k+4];
                    inoutfile->counters[k] = 0;
                    inoutfile->counters[k+4] = 0;
                }
            }
        }

        /* first set */
        for(j=POSIX_ACCESS1_ACCESS; j<=POSIX_ACCESS4_ACCESS; j++)
        {
            DARSHAN_COMMON_VAL_COUNTER_INC(&(tmp_file.counters[POSIX_ACCESS1_ACCESS]),
                &(tmp_file.counters[POSIX_ACCESS1_COUNT]), infile->counters[j],
                infile->counters[j+4]);
        }
        /* second set */
        for(j=POSIX_ACCESS1_ACCESS; j<=POSIX_ACCESS4_ACCESS; j++)
        {
            DARSHAN_COMMON_VAL_COUNTER_INC(&(tmp_file.counters[POSIX_ACCESS1_ACCESS]),
                &(tmp_file.counters[POSIX_ACCESS1_COUNT]), inoutfile->counters[j],
                inoutfile->counters[j+4]);
        }

        /* min non-zero (if available) value */
        for(j=POSIX_F_OPEN_TIMESTAMP; j<=POSIX_F_WRITE_START_TIMESTAMP; j++)
        {
            if(infile->fcounters[j] > inoutfile->fcounters[j] &&
               inoutfile->fcounters[j] > 0)
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

static void posix_shared_record_variance(MPI_Comm mod_comm,
    struct darshan_posix_file *inrec_array, struct darshan_posix_file *outrec_array,
    int shared_rec_count)
{
    MPI_Datatype var_dt;
    MPI_Op var_op;
    int i;
    struct darshan_variance_dt *var_send_buf = NULL;
    struct darshan_variance_dt *var_recv_buf = NULL;

    DARSHAN_MPI_CALL(PMPI_Type_contiguous)(sizeof(struct darshan_variance_dt),
        MPI_BYTE, &var_dt);
    DARSHAN_MPI_CALL(PMPI_Type_commit)(&var_dt);

    DARSHAN_MPI_CALL(PMPI_Op_create)(darshan_variance_reduce, 1, &var_op);

    var_send_buf = malloc(shared_rec_count * sizeof(struct darshan_variance_dt));
    if(!var_send_buf)
        return;

    if(my_rank == 0)
    {
        var_recv_buf = malloc(shared_rec_count * sizeof(struct darshan_variance_dt));

        if(!var_recv_buf)
            return;
    }

    /* get total i/o time variances for shared records */

    for(i=0; i<shared_rec_count; i++)
    {
        var_send_buf[i].n = 1;
        var_send_buf[i].S = 0;
        var_send_buf[i].T = inrec_array[i].fcounters[POSIX_F_READ_TIME] +
                            inrec_array[i].fcounters[POSIX_F_WRITE_TIME] +
                            inrec_array[i].fcounters[POSIX_F_META_TIME];
    }

    DARSHAN_MPI_CALL(PMPI_Reduce)(var_send_buf, var_recv_buf, shared_rec_count,
        var_dt, var_op, 0, mod_comm);

    if(my_rank == 0)
    {
        for(i=0; i<shared_rec_count; i++)
        {
            outrec_array[i].fcounters[POSIX_F_VARIANCE_RANK_TIME] =
                (var_recv_buf[i].S / var_recv_buf[i].n);
        }
    }

    /* get total bytes moved variances for shared records */

    for(i=0; i<shared_rec_count; i++)
    {
        var_send_buf[i].n = 1;
        var_send_buf[i].S = 0;
        var_send_buf[i].T = (double)
                            inrec_array[i].counters[POSIX_BYTES_READ] +
                            inrec_array[i].counters[POSIX_BYTES_WRITTEN];
    }

    DARSHAN_MPI_CALL(PMPI_Reduce)(var_send_buf, var_recv_buf, shared_rec_count,
        var_dt, var_op, 0, mod_comm);

    if(my_rank == 0)
    {
        for(i=0; i<shared_rec_count; i++)
        {
            outrec_array[i].fcounters[POSIX_F_VARIANCE_RANK_BYTES] =
                (var_recv_buf[i].S / var_recv_buf[i].n);
        }
    }

    DARSHAN_MPI_CALL(PMPI_Type_free)(&var_dt);
    DARSHAN_MPI_CALL(PMPI_Op_free)(&var_op);
    free(var_send_buf);
    free(var_recv_buf);

    return;
}

/************************************************************************
 * Functions exported by this module for coordinating with darshan-core *
 ************************************************************************/

static void posix_begin_shutdown()
{
    assert(posix_runtime);

    POSIX_LOCK();
    /* disable further instrumentation while Darshan shuts down */
    instrumentation_disabled = 1;
    POSIX_UNLOCK();

    return;
}

static void posix_get_output_data(
    MPI_Comm mod_comm,
    darshan_record_id *shared_recs,
    int shared_rec_count,
    void **posix_buf,
    int *posix_buf_sz)
{
    struct posix_file_runtime *file;
    struct posix_file_runtime *tmp;
    int i;
    double posix_time;
    struct darshan_posix_file *red_send_buf = NULL;
    struct darshan_posix_file *red_recv_buf = NULL;
    MPI_Datatype red_type;
    MPI_Op red_op;

    assert(posix_runtime);

    /* go through file access data for each record and set the 4 most common
     * stride/access size counters.
     */
    for(i = 0; i < posix_runtime->file_array_ndx; i++)
    {
        tmp = &(posix_runtime->file_runtime_array[i]);

        /* common accesses */
        darshan_walk_common_vals(tmp->access_root,
            &(tmp->file_record->counters[POSIX_ACCESS1_ACCESS]),
            &(tmp->file_record->counters[POSIX_ACCESS1_COUNT]));
        /* common strides */
        darshan_walk_common_vals(tmp->stride_root,
            &(tmp->file_record->counters[POSIX_STRIDE1_STRIDE]),
            &(tmp->file_record->counters[POSIX_STRIDE1_COUNT]));
    }

    /* if there are globally shared files, do a shared file reduction */
    /* NOTE: the shared file reduction is also skipped if the 
     * DARSHAN_DISABLE_SHARED_REDUCTION environment variable is set.
     */
    if(shared_rec_count && !getenv("DARSHAN_DISABLE_SHARED_REDUCTION"))
    {
        /* necessary initialization of shared records */
        for(i = 0; i < shared_rec_count; i++)
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
            sizeof(struct darshan_posix_file), posix_record_compare);

        /* make *send_buf point to the shared files at the end of sorted array */
        red_send_buf =
            &(posix_runtime->file_record_array[posix_runtime->file_array_ndx-shared_rec_count]);
        
        /* allocate memory for the reduction output on rank 0 */
        if(my_rank == 0)
        {
            red_recv_buf = malloc(shared_rec_count * sizeof(struct darshan_posix_file));
            if(!red_recv_buf)
            {
                return;
            }
        }

        /* construct a datatype for a POSIX file record.  This is serving no purpose
         * except to make sure we can do a reduction on proper boundaries
         */
        DARSHAN_MPI_CALL(PMPI_Type_contiguous)(sizeof(struct darshan_posix_file),
            MPI_BYTE, &red_type);
        DARSHAN_MPI_CALL(PMPI_Type_commit)(&red_type);

        /* register a POSIX file record reduction operator */
        DARSHAN_MPI_CALL(PMPI_Op_create)(posix_record_reduction_op, 1, &red_op);

        /* reduce shared POSIX file records */
        DARSHAN_MPI_CALL(PMPI_Reduce)(red_send_buf, red_recv_buf,
            shared_rec_count, red_type, red_op, 0, mod_comm);

        /* get the time and byte variances for shared files */
        posix_shared_record_variance(mod_comm, red_send_buf, red_recv_buf,
            shared_rec_count);

        /* clean up reduction state */
        if(my_rank == 0)
        {
            int tmp_ndx = posix_runtime->file_array_ndx - shared_rec_count;
            memcpy(&(posix_runtime->file_record_array[tmp_ndx]), red_recv_buf,
                shared_rec_count * sizeof(struct darshan_posix_file));
            free(red_recv_buf);
        }
        else
        {
            posix_runtime->file_array_ndx -= shared_rec_count;
        }

        DARSHAN_MPI_CALL(PMPI_Type_free)(&red_type);
        DARSHAN_MPI_CALL(PMPI_Op_free)(&red_op);
    }

    *posix_buf = (void *)(posix_runtime->file_record_array);
    *posix_buf_sz = posix_runtime->file_array_ndx * sizeof(struct darshan_posix_file);

    return;
}

static void posix_shutdown()
{
    struct posix_file_runtime_ref *ref, *tmp;

    assert(posix_runtime);

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
