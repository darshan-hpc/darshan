/*
 * Copyright (C) 2015 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

/* TODO list (general) for this module:
 * - add stdio page to darshan-job-summary
 * - figure out what to do about posix module compatibility
 *   - remove stdio counters in POSIX or keep and set to -1?
 *   - affected counters in posix module:
 *     - POSIX_FOPENS
 *     - POSIX_FREADS
 *     - POSIX_FWRITES
 *     - POSIX_FSEEKS
 */

/* catalog of stdio functions instrumented by this module
 *
 * functions for opening streams
 * --------------
 * FILE    *fdopen(int, const char *);                      DONE
 * FILE    *fopen(const char *, const char *);              DONE
 * FILE    *fopen64(const char *, const char *);            DONE
 * FILE    *freopen(const char *, const char *, FILE *);    DONE
 * FILE    *freopen64(const char *, const char *, FILE *);  DONE
 *
 * functions for closing streams
 * --------------
 * int      fclose(FILE *);                                 DONE
 *
 * functions for flushing streams
 * --------------
 * int      fflush(FILE *);                                 DONE
 *
 * functions for reading data
 * --------------
 * int      fgetc(FILE *);                                  DONE
 * char    *fgets(char *, int, FILE *);                     DONE
 * size_t   fread(void *, size_t, size_t, FILE *);          DONE
 * int      fscanf(FILE *, const char *, ...);              DONE
 * int      vfscanf(FILE *, const char *, va_list);         DONE
 * int      getc(FILE *);                                   DONE
 * int      getw(FILE *);                                   DONE
 *
 * functions for writing data
 * --------------
 * int      fprintf(FILE *, const char *, ...);             DONE
 * int      vfprintf(FILE *, const char *, va_list);        DONE
 * int      fputc(int, FILE *);                             DONE
 * int      fputs(const char *, FILE *);                    DONE
 * size_t   fwrite(const void *, size_t, size_t, FILE *);   DONE
 * int      putc(int, FILE *);                              DONE
 * int      putw(int, FILE *);                              DONE
 *
 * functions for changing file position
 * --------------
 * int      fseek(FILE *, long int, int);                   DONE
 * int      fseeko(FILE *, off_t, int);                     DONE
 * int      fseeko64(FILE *, off_t, int);                   DONE
 * int      fsetpos(FILE *, const fpos_t *);                DONE
 * int      fsetpos64(FILE *, const fpos_t *);              DONE
 * void     rewind(FILE *);                                 DONE
 *
 * Omissions: 
 *   - _unlocked() variants of the various flush, read, and write
 *     functions.  There are many of these, but they are not available on all
 *     systems and the man page advises not to use them.
 *   - ungetc()
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
#include <pthread.h>

#include "uthash.h"
#include "utlist.h"

#include "darshan.h"
#include "darshan-dynamic.h"

DARSHAN_FORWARD_DECL(fopen, FILE*, (const char *path, const char *mode));
DARSHAN_FORWARD_DECL(fopen64, FILE*, (const char *path, const char *mode));
DARSHAN_FORWARD_DECL(fdopen, FILE*, (int fd, const char *mode));
DARSHAN_FORWARD_DECL(freopen, FILE*, (const char *path, const char *mode, FILE *stream));
DARSHAN_FORWARD_DECL(freopen64, FILE*, (const char *path, const char *mode, FILE *stream));
DARSHAN_FORWARD_DECL(fclose, int, (FILE *fp));
DARSHAN_FORWARD_DECL(fflush, int, (FILE *fp));
DARSHAN_FORWARD_DECL(fwrite, size_t, (const void *ptr, size_t size, size_t nmemb, FILE *stream));
DARSHAN_FORWARD_DECL(fputc, int, (int c, FILE *stream));
DARSHAN_FORWARD_DECL(putw, int, (int w, FILE *stream));
DARSHAN_FORWARD_DECL(fputs, int, (const char *s, FILE *stream));
DARSHAN_FORWARD_DECL(fprintf, int, (FILE *stream, const char *format, ...));
DARSHAN_FORWARD_DECL(vfprintf, int, (FILE *stream, const char *format, va_list));
DARSHAN_FORWARD_DECL(fread, size_t, (void *ptr, size_t size, size_t nmemb, FILE *stream));
DARSHAN_FORWARD_DECL(fgetc, int, (FILE *stream));
DARSHAN_FORWARD_DECL(getw, int, (FILE *stream));
DARSHAN_FORWARD_DECL(_IO_getc, int, (FILE *stream));
DARSHAN_FORWARD_DECL(_IO_putc, int, (int, FILE *stream));
DARSHAN_FORWARD_DECL(fscanf, int, (FILE *stream, const char *format, ...));
DARSHAN_FORWARD_DECL(__isoc99_fscanf, int, (FILE *stream, const char *format, ...));
DARSHAN_FORWARD_DECL(vfscanf, int, (FILE *stream, const char *format, va_list ap));
DARSHAN_FORWARD_DECL(fgets, char*, (char *s, int size, FILE *stream));
DARSHAN_FORWARD_DECL(fseek, int, (FILE *stream, long offset, int whence));
DARSHAN_FORWARD_DECL(fseeko, int, (FILE *stream, off_t offset, int whence));
DARSHAN_FORWARD_DECL(fseeko64, int, (FILE *stream, off_t offset, int whence));
DARSHAN_FORWARD_DECL(fsetpos, int, (FILE *stream, const fpos_t *pos));
DARSHAN_FORWARD_DECL(fsetpos64, int, (FILE *stream, const fpos_t *pos));
DARSHAN_FORWARD_DECL(rewind, void, (FILE *stream));

/* The stdio_file_runtime structure maintains necessary runtime metadata
 * for the STDIO file record (darshan_stdio_record structure, defined in
 * darshan-stdio-log-format.h) pointed to by 'file_record'. This metadata
 * assists with the instrumenting of specific statistics in the file record.
 * 'hlink' is a hash table link structure used to add/remove this record
 * from the hash table of STDIO file records for this process. 
 *
 * RATIONALE: the STDIO module needs to track some stateful, volatile 
 * information about each open file (like the current file offset, most recent 
 * access time, etc.) to aid in instrumentation, but this information can't be
 * stored in the darshan_stdio_record struct because we don't want it to appear in
 * the final darshan log file.  We therefore associate a stdio_file_runtime
 * struct with each darshan_stdio_record struct in order to track this information.
  *
 * NOTE: There is a one-to-one mapping of stdio_file_runtime structs to
 * darshan_stdio_record structs.
 *
 * NOTE: The stdio_file_runtime struct contains a pointer to a darshan_stdio_record
 * struct (see the *file_record member) rather than simply embedding an entire
 * darshan_stdio_record struct.  This is done so that all of the darshan_stdio_record
 * structs can be kept contiguous in memory as a single array to simplify
 * reduction, compression, and storage.
 */
struct stdio_file_runtime
{
    struct darshan_stdio_record* file_record;
    int64_t offset;
    double last_meta_end;
    double last_read_end;
    double last_write_end;
    UT_hash_handle hlink;
};

/* The stdio_file_runtime_ref structure is used to associate a STDIO
 * stream with an already existing STDIO file record. This is
 * necessary as many STDIO I/O functions take only an input stream,
 * but STDIO file records are indexed by their full file paths (i.e., darshan
 * record identifiers for STDIO files are created by hashing the file path).
 * In other words, this structure is necessary as it allows us to look up a
 * file record either by a pathname (stdio_file_runtime) or by STDIO stream 
 * (stdio_file_runtime_ref), depending on which parameters are
 * available. This structure includes another hash table link, since separate
 * hashes are maintained for stdio_file_runtime structures and stdio_file_runtime_ref
 * structures.
 *
 * RATIONALE: In theory the FILE* information could be included in the
 * stdio_file_runtime struct rather than in a separate structure here.  The
 * reason we don't do that is because the same file could be opened multiple
 * times by a given process with different stream pointers and thus
 * simulataneously referenced using different stream pointers.  This practice is
 * not common, but we must support it.
 *
 * NOTE: there are potentially multiple stdio_file_runtime_ref structures
 * referring to a single stdio_file_runtime structure.  Most of the time there is
 * only one, however.
 */
struct stdio_file_runtime_ref
{
    struct stdio_file_runtime* file;
    FILE* stream;
    UT_hash_handle hlink;
};

/* The stdio_runtime structure maintains necessary state for storing
 * STDIO file records and for coordinating with darshan-core at 
 * shutdown time.
 */
struct stdio_runtime
{
    struct stdio_file_runtime* file_runtime_array;
    struct darshan_stdio_record* file_record_array;
    int file_array_size;
    int file_array_ndx;
    struct stdio_file_runtime* file_hash;
    struct stdio_file_runtime_ref* stream_hash;
};

static struct stdio_runtime *stdio_runtime = NULL;
static pthread_mutex_t stdio_runtime_mutex = PTHREAD_RECURSIVE_MUTEX_INITIALIZER_NP;
static int instrumentation_disabled = 0;
static int darshan_mem_alignment = 1;
static int my_rank = -1;

static void stdio_runtime_initialize(void);
static struct stdio_file_runtime* stdio_file_by_name(const char *name);
static struct stdio_file_runtime* stdio_file_by_name_setstream(const char* name, FILE *stream);
static struct stdio_file_runtime* stdio_file_by_stream(FILE* stream);
static void stdio_file_close_stream(FILE *stream);

static void stdio_begin_shutdown(void);
static void stdio_get_output_data(MPI_Comm mod_comm, darshan_record_id *shared_recs,
    int shared_rec_count, void **stdio_buf, int *stdio_buf_sz);
static void stdio_shutdown(void);
static void stdio_record_reduction_op(void* infile_v, void* inoutfile_v,
    int *len, MPI_Datatype *datatype);
static int stdio_record_compare(const void* a, const void* b);

#define STDIO_LOCK() pthread_mutex_lock(&stdio_runtime_mutex)
#define STDIO_UNLOCK() pthread_mutex_unlock(&stdio_runtime_mutex)

#define STDIO_RECORD_OPEN(__ret, __path, __tm1, __tm2) do { \
    struct stdio_file_runtime* file; \
    char* exclude; \
    int tmp_index = 0; \
    if(__ret == NULL) break; \
    while((exclude = darshan_path_exclusions[tmp_index])) { \
        if(!(strncmp(exclude, __path, strlen(exclude)))) \
            break; \
        tmp_index++; \
    } \
    if(exclude) break; \
    file = stdio_file_by_name_setstream(__path, __ret); \
    if(!file) break; \
    file->offset = 0; \
    file->file_record->counters[STDIO_OPENS] += 1; \
    if(file->file_record->fcounters[STDIO_F_OPEN_START_TIMESTAMP] == 0 || \
     file->file_record->fcounters[STDIO_F_OPEN_START_TIMESTAMP] > __tm1) \
        file->file_record->fcounters[STDIO_F_OPEN_START_TIMESTAMP] = __tm1; \
    file->file_record->fcounters[STDIO_F_OPEN_END_TIMESTAMP] = __tm2; \
    DARSHAN_TIMER_INC_NO_OVERLAP(file->file_record->fcounters[STDIO_F_META_TIME], __tm1, __tm2, file->last_meta_end); \
} while(0)


#define STDIO_RECORD_READ(__fp, __bytes,  __tm1, __tm2) do{ \
    int64_t this_offset; \
    struct stdio_file_runtime* file; \
    file = stdio_file_by_stream(__fp); \
    if(!file) break; \
    this_offset = file->offset; \
    file->offset = this_offset + __bytes; \
    if(file->file_record->counters[STDIO_MAX_BYTE_READ] < (this_offset + __bytes - 1)) \
        file->file_record->counters[STDIO_MAX_BYTE_READ] = (this_offset + __bytes - 1); \
    file->file_record->counters[STDIO_BYTES_READ] += __bytes; \
    file->file_record->counters[STDIO_READS] += 1; \
    if(file->file_record->fcounters[STDIO_F_READ_START_TIMESTAMP] == 0 || \
     file->file_record->fcounters[STDIO_F_READ_START_TIMESTAMP] > __tm1) \
        file->file_record->fcounters[STDIO_F_READ_START_TIMESTAMP] = __tm1; \
    file->file_record->fcounters[STDIO_F_READ_END_TIMESTAMP] = __tm2; \
    DARSHAN_TIMER_INC_NO_OVERLAP(file->file_record->fcounters[STDIO_F_READ_TIME], __tm1, __tm2, file->last_write_end); \
} while(0)

#define STDIO_RECORD_WRITE(__fp, __bytes,  __tm1, __tm2, __fflush_flag) do{ \
    int64_t this_offset; \
    struct stdio_file_runtime* file; \
    file = stdio_file_by_stream(__fp); \
    if(!file) break; \
    this_offset = file->offset; \
    file->offset = this_offset + __bytes; \
    if(file->file_record->counters[STDIO_MAX_BYTE_WRITTEN] < (this_offset + __bytes - 1)) \
        file->file_record->counters[STDIO_MAX_BYTE_WRITTEN] = (this_offset + __bytes - 1); \
    file->file_record->counters[STDIO_BYTES_WRITTEN] += __bytes; \
    if(__fflush_flag) \
        file->file_record->counters[STDIO_FLUSHES] += 1; \
    else \
        file->file_record->counters[STDIO_WRITES] += 1; \
    if(file->file_record->fcounters[STDIO_F_WRITE_START_TIMESTAMP] == 0 || \
     file->file_record->fcounters[STDIO_F_WRITE_START_TIMESTAMP] > __tm1) \
        file->file_record->fcounters[STDIO_F_WRITE_START_TIMESTAMP] = __tm1; \
    file->file_record->fcounters[STDIO_F_WRITE_END_TIMESTAMP] = __tm2; \
    DARSHAN_TIMER_INC_NO_OVERLAP(file->file_record->fcounters[STDIO_F_WRITE_TIME], __tm1, __tm2, file->last_write_end); \
} while(0)

FILE* DARSHAN_DECL(fopen)(const char *path, const char *mode)
{
    FILE* ret;
    double tm1, tm2;

    MAP_OR_FAIL(fopen);

    tm1 = darshan_core_wtime();
    ret = __real_fopen(path, mode);
    tm2 = darshan_core_wtime();

    STDIO_LOCK();
    stdio_runtime_initialize();
    STDIO_RECORD_OPEN(ret, path, tm1, tm2);
    STDIO_UNLOCK();

    return(ret);
}

FILE* DARSHAN_DECL(fopen64)(const char *path, const char *mode)
{
    FILE* ret;
    double tm1, tm2;

    MAP_OR_FAIL(fopen);

    tm1 = darshan_core_wtime();
    ret = __real_fopen64(path, mode);
    tm2 = darshan_core_wtime();

    STDIO_LOCK();
    stdio_runtime_initialize();
    STDIO_RECORD_OPEN(ret, path, tm1, tm2);
    STDIO_UNLOCK();

    return(ret);
}

FILE* DARSHAN_DECL(fdopen)(int fd, const char *mode)
{
    FILE* ret;
    double tm1, tm2;

    MAP_OR_FAIL(fdopen);

    tm1 = darshan_core_wtime();
    ret = __real_fdopen(fd, mode);
    tm2 = darshan_core_wtime();

    STDIO_LOCK();
    stdio_runtime_initialize();
    STDIO_RECORD_OPEN(ret, "UNKNOWN-FDOPEN", tm1, tm2);
    STDIO_UNLOCK();

    return(ret);
}

FILE* DARSHAN_DECL(freopen)(const char *path, const char *mode, FILE *stream)
{
    FILE* ret;
    double tm1, tm2;

    MAP_OR_FAIL(freopen);

    tm1 = darshan_core_wtime();
    ret = __real_freopen(path, mode, stream);
    tm2 = darshan_core_wtime();

    STDIO_LOCK();
    stdio_runtime_initialize();
    STDIO_RECORD_OPEN(ret, path, tm1, tm2);
    STDIO_UNLOCK();

    return(ret);
}

FILE* DARSHAN_DECL(freopen64)(const char *path, const char *mode, FILE *stream)
{
    FILE* ret;
    double tm1, tm2;

    MAP_OR_FAIL(freopen64);

    tm1 = darshan_core_wtime();
    ret = __real_freopen64(path, mode, stream);
    tm2 = darshan_core_wtime();

    STDIO_LOCK();
    stdio_runtime_initialize();
    STDIO_RECORD_OPEN(ret, path, tm1, tm2);
    STDIO_UNLOCK();

    return(ret);
}


int DARSHAN_DECL(fflush)(FILE *fp)
{
    double tm1, tm2;
    int ret;

    MAP_OR_FAIL(fflush);

    tm1 = darshan_core_wtime();
    ret = __real_fflush(fp);
    tm2 = darshan_core_wtime();

    STDIO_LOCK();
    stdio_runtime_initialize();
    if(ret >= 0)
        STDIO_RECORD_WRITE(fp, 0, tm1, tm2, 1);
    STDIO_UNLOCK();

    return(ret);
}

int DARSHAN_DECL(fclose)(FILE *fp)
{
    struct stdio_file_runtime* file;
    double tm1, tm2;
    int ret;

    MAP_OR_FAIL(fclose);

    tm1 = darshan_core_wtime();
    ret = __real_fclose(fp);
    tm2 = darshan_core_wtime();

    STDIO_LOCK();
    stdio_runtime_initialize();
    file = stdio_file_by_stream(fp);
    if(file)
    {
        if(file->file_record->fcounters[STDIO_F_CLOSE_START_TIMESTAMP] == 0 ||
         file->file_record->fcounters[STDIO_F_CLOSE_START_TIMESTAMP] > tm1)
           file->file_record->fcounters[STDIO_F_CLOSE_START_TIMESTAMP] = tm1;
        file->file_record->fcounters[STDIO_F_CLOSE_END_TIMESTAMP] = tm2;
        DARSHAN_TIMER_INC_NO_OVERLAP(
            file->file_record->fcounters[STDIO_F_META_TIME],
            tm1, tm2, file->last_meta_end);
        stdio_file_close_stream(fp);
    }
    STDIO_UNLOCK();

    return(ret);
}

size_t DARSHAN_DECL(fwrite)(const void *ptr, size_t size, size_t nmemb, FILE *stream)
{
    size_t ret;
    double tm1, tm2;

    MAP_OR_FAIL(fwrite);

    tm1 = darshan_core_wtime();
    ret = __real_fwrite(ptr, size, nmemb, stream);
    tm2 = darshan_core_wtime();

    STDIO_LOCK();
    stdio_runtime_initialize();
    if(ret > 0)
        STDIO_RECORD_WRITE(stream, size*ret, tm1, tm2, 0);
    STDIO_UNLOCK();

    return(ret);
}


int DARSHAN_DECL(fputc)(int c, FILE *stream)
{
    int ret;
    double tm1, tm2;

    MAP_OR_FAIL(fputc);

    tm1 = darshan_core_wtime();
    ret = __real_fputc(c, stream);
    tm2 = darshan_core_wtime();

    STDIO_LOCK();
    stdio_runtime_initialize();
    if(ret != EOF)
        STDIO_RECORD_WRITE(stream, 1, tm1, tm2, 0);
    STDIO_UNLOCK();

    return(ret);
}

int DARSHAN_DECL(putw)(int w, FILE *stream)
{
    int ret;
    double tm1, tm2;

    MAP_OR_FAIL(putw);

    tm1 = darshan_core_wtime();
    ret = __real_putw(w, stream);
    tm2 = darshan_core_wtime();

    STDIO_LOCK();
    stdio_runtime_initialize();
    if(ret != EOF)
        STDIO_RECORD_WRITE(stream, sizeof(int), tm1, tm2, 0);
    STDIO_UNLOCK();

    return(ret);
}



int DARSHAN_DECL(fputs)(const char *s, FILE *stream)
{
    int ret;
    double tm1, tm2;

    MAP_OR_FAIL(fputs);

    tm1 = darshan_core_wtime();
    ret = __real_fputs(s, stream);
    tm2 = darshan_core_wtime();

    STDIO_LOCK();
    stdio_runtime_initialize();
    if(ret != EOF && ret > 0)
        STDIO_RECORD_WRITE(stream, strlen(s), tm1, tm2, 0);
    STDIO_UNLOCK();

    return(ret);
}

int DARSHAN_DECL(vfprintf)(FILE *stream, const char *format, va_list ap)
{
    int ret;
    double tm1, tm2;
    long start_off, end_off;

    MAP_OR_FAIL(vfprintf);

    tm1 = darshan_core_wtime();
    start_off = ftell(stream);
    ret = __real_vfprintf(stream, format, ap);
    end_off = ftell(stream);
    tm2 = darshan_core_wtime();

    STDIO_LOCK();
    stdio_runtime_initialize();
    if(ret > 0)
        STDIO_RECORD_WRITE(stream, (end_off-start_off), tm1, tm2, 0);
    STDIO_UNLOCK();

    return(ret);
}


int DARSHAN_DECL(fprintf)(FILE *stream, const char *format, ...)
{
    int ret;
    double tm1, tm2;
    va_list ap;
    long start_off, end_off;

    MAP_OR_FAIL(vfprintf);

    tm1 = darshan_core_wtime();
    /* NOTE: we intentionally switch to vfprintf here to handle the variable
     * length arguments.
     */
    start_off = ftell(stream);
    va_start(ap, format);
    ret = __real_vfprintf(stream, format, ap);
    va_end(ap);
    end_off = ftell(stream);
    tm2 = darshan_core_wtime();

    STDIO_LOCK();
    stdio_runtime_initialize();
    if(ret > 0)
        STDIO_RECORD_WRITE(stream, (end_off-start_off), tm1, tm2, 0);
    STDIO_UNLOCK();

    return(ret);
}

size_t DARSHAN_DECL(fread)(void *ptr, size_t size, size_t nmemb, FILE *stream)
{
    size_t ret;
    double tm1, tm2;

    MAP_OR_FAIL(fread);

    tm1 = darshan_core_wtime();
    ret = __real_fread(ptr, size, nmemb, stream);
    tm2 = darshan_core_wtime();

    STDIO_LOCK();
    stdio_runtime_initialize();
    if(ret > 0)
        STDIO_RECORD_READ(stream, size*ret, tm1, tm2);
    STDIO_UNLOCK();

    return(ret);
}

size_t DARSHAN_DECL(fgetc)(FILE *stream)
{
    int ret;
    double tm1, tm2;

    MAP_OR_FAIL(fgetc);

    tm1 = darshan_core_wtime();
    ret = __real_fgetc(stream);
    tm2 = darshan_core_wtime();

    STDIO_LOCK();
    stdio_runtime_initialize();
    if(ret != EOF)
        STDIO_RECORD_READ(stream, 1, tm1, tm2);
    STDIO_UNLOCK();

    return(ret);
}

/* NOTE: stdio.h typically implements getc() as a macro pointing to _IO_getc */
size_t DARSHAN_DECL(_IO_getc)(FILE *stream)
{
    int ret;
    double tm1, tm2;

    MAP_OR_FAIL(_IO_getc);

    tm1 = darshan_core_wtime();
    ret = __real__IO_getc(stream);
    tm2 = darshan_core_wtime();

    STDIO_LOCK();
    stdio_runtime_initialize();
    if(ret != EOF)
        STDIO_RECORD_READ(stream, 1, tm1, tm2);
    STDIO_UNLOCK();

    return(ret);
}

/* NOTE: stdio.h typically implements putc() as a macro pointing to _IO_putc */
size_t DARSHAN_DECL(_IO_putc)(int c, FILE *stream)
{
    int ret;
    double tm1, tm2;

    MAP_OR_FAIL(_IO_putc);

    tm1 = darshan_core_wtime();
    ret = __real__IO_putc(c, stream);
    tm2 = darshan_core_wtime();

    STDIO_LOCK();
    stdio_runtime_initialize();
    if(ret != EOF)
        STDIO_RECORD_WRITE(stream, 1, tm1, tm2, 0);
    STDIO_UNLOCK();

    return(ret);
}
size_t DARSHAN_DECL(getw)(FILE *stream)
{
    int ret;
    double tm1, tm2;

    MAP_OR_FAIL(getw);

    tm1 = darshan_core_wtime();
    ret = __real_getw(stream);
    tm2 = darshan_core_wtime();

    STDIO_LOCK();
    stdio_runtime_initialize();
    if(ret != EOF || ferror(stream) == 0)
        STDIO_RECORD_READ(stream, sizeof(int), tm1, tm2);
    STDIO_UNLOCK();

    return(ret);
}

/* NOTE: some glibc versions use __isoc99_fscanf as the underlying symbol
 * rather than fscanf
 */
int DARSHAN_DECL(__isoc99_fscanf)(FILE *stream, const char *format, ...)
{
    int ret;
    double tm1, tm2;
    va_list ap;
    long start_off, end_off;

    MAP_OR_FAIL(vfscanf);

    tm1 = darshan_core_wtime();
    /* NOTE: we intentionally switch to vfscanf here to handle the variable
     * length arguments.
     */
    start_off = ftell(stream);
    va_start(ap, format);
    ret = __real_vfscanf(stream, format, ap);
    va_end(ap);
    end_off = ftell(stream);
    tm2 = darshan_core_wtime();

    STDIO_LOCK();
    stdio_runtime_initialize();
    if(ret != 0)
        STDIO_RECORD_READ(stream, (end_off-start_off), tm1, tm2);
    STDIO_UNLOCK();

    return(ret);
}


int DARSHAN_DECL(fscanf)(FILE *stream, const char *format, ...)
{
    int ret;
    double tm1, tm2;
    va_list ap;
    long start_off, end_off;

    MAP_OR_FAIL(vfscanf);

    tm1 = darshan_core_wtime();
    /* NOTE: we intentionally switch to vfscanf here to handle the variable
     * length arguments.
     */
    start_off = ftell(stream);
    va_start(ap, format);
    ret = __real_vfscanf(stream, format, ap);
    va_end(ap);
    end_off = ftell(stream);
    tm2 = darshan_core_wtime();

    STDIO_LOCK();
    stdio_runtime_initialize();
    if(ret != 0)
        STDIO_RECORD_READ(stream, (end_off-start_off), tm1, tm2);
    STDIO_UNLOCK();

    return(ret);
}

int DARSHAN_DECL(vfscanf)(FILE *stream, const char *format, va_list ap)
{
    int ret;
    double tm1, tm2;
    long start_off, end_off;

    MAP_OR_FAIL(vfscanf);

    tm1 = darshan_core_wtime();
    start_off = ftell(stream);
    ret = __real_vfscanf(stream, format, ap);
    end_off = ftell(stream);
    tm2 = darshan_core_wtime();

    STDIO_LOCK();
    stdio_runtime_initialize();
    if(ret != 0)
        STDIO_RECORD_READ(stream, end_off-start_off, tm1, tm2);
    STDIO_UNLOCK();

    return(ret);
}


char* DARSHAN_DECL(fgets)(char *s, int size, FILE *stream)
{
    char *ret;
    double tm1, tm2;

    MAP_OR_FAIL(fgets);

    tm1 = darshan_core_wtime();
    ret = __real_fgets(s, size, stream);
    tm2 = darshan_core_wtime();

    STDIO_LOCK();
    stdio_runtime_initialize();
    if(ret != NULL)
        STDIO_RECORD_READ(stream, strlen(ret), tm1, tm2);
    STDIO_UNLOCK();

    return(ret);
}


void DARSHAN_DECL(rewind)(FILE *stream)
{
    struct stdio_file_runtime* file;
    double tm1, tm2;

    MAP_OR_FAIL(rewind);

    tm1 = darshan_core_wtime();
    __real_rewind(stream);
    tm2 = darshan_core_wtime();

    STDIO_LOCK();
    stdio_runtime_initialize();
    file = stdio_file_by_stream(stream);
    if(file)
    {
        file->offset = 0;
        DARSHAN_TIMER_INC_NO_OVERLAP(
            file->file_record->fcounters[STDIO_F_META_TIME],
            tm1, tm2, file->last_meta_end);
        file->file_record->counters[STDIO_SEEKS] += 1;
    }
    STDIO_UNLOCK();

    return;
}

int DARSHAN_DECL(fseek)(FILE *stream, long offset, int whence)
{
    int ret;
    struct stdio_file_runtime* file;
    double tm1, tm2;

    MAP_OR_FAIL(fseek);

    tm1 = darshan_core_wtime();
    ret = __real_fseek(stream, offset, whence);
    tm2 = darshan_core_wtime();

    if(ret >= 0)
    {
        STDIO_LOCK();
        stdio_runtime_initialize();
        file = stdio_file_by_stream(stream);
        if(file)
        {
            file->offset = ftell(stream);
            DARSHAN_TIMER_INC_NO_OVERLAP(
                file->file_record->fcounters[STDIO_F_META_TIME],
                tm1, tm2, file->last_meta_end);
            file->file_record->counters[STDIO_SEEKS] += 1;
        }
        STDIO_UNLOCK();
    }

    return(ret);
}

int DARSHAN_DECL(fseeko)(FILE *stream, off_t offset, int whence)
{
    int ret;
    struct stdio_file_runtime* file;
    double tm1, tm2;

    MAP_OR_FAIL(fseeko);

    tm1 = darshan_core_wtime();
    ret = __real_fseeko(stream, offset, whence);
    tm2 = darshan_core_wtime();

    if(ret >= 0)
    {
        STDIO_LOCK();
        stdio_runtime_initialize();
        file = stdio_file_by_stream(stream);
        if(file)
        {
            file->offset = ftell(stream);
            DARSHAN_TIMER_INC_NO_OVERLAP(
                file->file_record->fcounters[STDIO_F_META_TIME],
                tm1, tm2, file->last_meta_end);
            file->file_record->counters[STDIO_SEEKS] += 1;
        }
        STDIO_UNLOCK();
    }

    return(ret);
}

int DARSHAN_DECL(fseeko64)(FILE *stream, off_t offset, int whence)
{
    int ret;
    struct stdio_file_runtime* file;
    double tm1, tm2;

    MAP_OR_FAIL(fseeko64);

    tm1 = darshan_core_wtime();
    ret = __real_fseeko64(stream, offset, whence);
    tm2 = darshan_core_wtime();

    if(ret >= 0)
    {
        STDIO_LOCK();
        stdio_runtime_initialize();
        file = stdio_file_by_stream(stream);
        if(file)
        {
            file->offset = ftell(stream);
            DARSHAN_TIMER_INC_NO_OVERLAP(
                file->file_record->fcounters[STDIO_F_META_TIME],
                tm1, tm2, file->last_meta_end);
            file->file_record->counters[STDIO_SEEKS] += 1;
        }
        STDIO_UNLOCK();
    }

    return(ret);
}

int DARSHAN_DECL(fsetpos)(FILE *stream, const fpos_t *pos)
{
    int ret;
    struct stdio_file_runtime* file;
    double tm1, tm2;

    MAP_OR_FAIL(fsetpos);

    tm1 = darshan_core_wtime();
    ret = __real_fsetpos(stream, pos);
    tm2 = darshan_core_wtime();

    if(ret >= 0)
    {
        STDIO_LOCK();
        stdio_runtime_initialize();
        file = stdio_file_by_stream(stream);
        if(file)
        {
            file->offset = ftell(stream);
            DARSHAN_TIMER_INC_NO_OVERLAP(
                file->file_record->fcounters[STDIO_F_META_TIME],
                tm1, tm2, file->last_meta_end);
            file->file_record->counters[STDIO_SEEKS] += 1;
        }
        STDIO_UNLOCK();
    }

    return(ret);
}

int DARSHAN_DECL(fsetpos64)(FILE *stream, const fpos_t *pos)
{
    int ret;
    struct stdio_file_runtime* file;
    double tm1, tm2;

    MAP_OR_FAIL(fsetpos64);

    tm1 = darshan_core_wtime();
    ret = __real_fsetpos64(stream, pos);
    tm2 = darshan_core_wtime();

    if(ret >= 0)
    {
        STDIO_LOCK();
        stdio_runtime_initialize();
        file = stdio_file_by_stream(stream);
        if(file)
        {
            file->offset = ftell(stream);
            DARSHAN_TIMER_INC_NO_OVERLAP(
                file->file_record->fcounters[STDIO_F_META_TIME],
                tm1, tm2, file->last_meta_end);
            file->file_record->counters[STDIO_SEEKS] += 1;
        }
        STDIO_UNLOCK();
    }

    return(ret);
}


/**********************************************************
 * Internal functions for manipulating STDIO module state *
 **********************************************************/

/* initialize internal STDIO module data structures and register with darshan-core */
static void stdio_runtime_initialize()
{
    int mem_limit;
    struct darshan_module_funcs stdio_mod_fns =
    {
        .begin_shutdown = &stdio_begin_shutdown,
        .get_output_data = &stdio_get_output_data,
        .shutdown = &stdio_shutdown
    };

    /* don't do anything if already initialized or instrumenation is disabled */
    if(stdio_runtime || instrumentation_disabled)
        return;

    /* register the stdio module with darshan core */
    darshan_core_register_module(
        DARSHAN_STDIO_MOD,
        &stdio_mod_fns,
        &my_rank,
        &mem_limit,
        &darshan_mem_alignment);

    /* return if no memory assigned by darshan core */
    if(mem_limit == 0)
        return;

    stdio_runtime = malloc(sizeof(*stdio_runtime));
    if(!stdio_runtime)
        return;
    memset(stdio_runtime, 0, sizeof(*stdio_runtime));

    /* set maximum number of file records according to max memory limit */
    /* NOTE: maximum number of records is based on the size of a stdio file record */
    /* TODO: should we base memory usage off file record or total runtime structure sizes? */
    stdio_runtime->file_array_size = mem_limit / sizeof(struct darshan_stdio_record);
    stdio_runtime->file_array_ndx = 0;

    /* allocate array of runtime file records */
    stdio_runtime->file_runtime_array = malloc(stdio_runtime->file_array_size *
                                               sizeof(struct stdio_file_runtime));
    stdio_runtime->file_record_array = malloc(stdio_runtime->file_array_size *
                                              sizeof(struct darshan_stdio_record));
    if(!stdio_runtime->file_runtime_array || !stdio_runtime->file_record_array)
    {
        stdio_runtime->file_array_size = 0;
        return;
    }
    memset(stdio_runtime->file_runtime_array, 0, stdio_runtime->file_array_size *
           sizeof(struct stdio_file_runtime));
    memset(stdio_runtime->file_record_array, 0, stdio_runtime->file_array_size *
           sizeof(struct darshan_stdio_record));

    return;
}

/* get a STDIO file record for the given file path */
static struct stdio_file_runtime* stdio_file_by_name(const char *name)
{
    struct stdio_file_runtime *file = NULL;
    char *newname = NULL;
    darshan_record_id file_id;
    int file_alignment;
    int limit_flag;

    if(!stdio_runtime || instrumentation_disabled)
        return(NULL);

    newname = darshan_clean_file_path(name);
    if(!newname)
        newname = (char*)name;

    limit_flag = (stdio_runtime->file_array_ndx >= stdio_runtime->file_array_size);

    /* get a unique id for this file from darshan core */
    darshan_core_register_record(
        (void*)newname,
        strlen(newname),
        DARSHAN_STDIO_MOD,
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
    HASH_FIND(hlink, stdio_runtime->file_hash, &file_id, sizeof(darshan_record_id), file);
    if(file)
    {
        if(newname != name)
            free(newname);
        return(file);
    }

    /* no existing record, assign a new file record from the global array */
    file = &(stdio_runtime->file_runtime_array[stdio_runtime->file_array_ndx]);
    file->file_record = &(stdio_runtime->file_record_array[stdio_runtime->file_array_ndx]);
    file->file_record->f_id = file_id;
    file->file_record->rank = my_rank;

    /* add new record to file hash table */
    HASH_ADD(hlink, stdio_runtime->file_hash, file_record->f_id, sizeof(darshan_record_id), file);
    stdio_runtime->file_array_ndx++;

    if(newname != name)
        free(newname);
    return(file);
}

/* get a STDIO file record for the given file path, and also create a
 * reference structure using the returned stream
 */
static struct stdio_file_runtime* stdio_file_by_name_setstream(const char* name, FILE *stream)
{
    struct stdio_file_runtime* file;
    struct stdio_file_runtime_ref* ref;

    if(!stdio_runtime || instrumentation_disabled)
        return(NULL);

    /* find file record by name first */
    file = stdio_file_by_name(name);

    if(!file)
        return(NULL);

    /* search hash table for existing file ref for this stream */
    HASH_FIND(hlink, stdio_runtime->stream_hash, &stream, sizeof(FILE*), ref);
    if(ref)
    {
        /* we have a reference.  Make sure it points to the correct file
         * and return it
         */
        ref->file = file;
        return(file);
    }

    /* if we hit this point, then we don't have a reference for this stream 
     * in the table yet.  Add it.
     */
    ref = malloc(sizeof(*ref));
    if(!ref)
        return(NULL);
    memset(ref, 0, sizeof(*ref));

    ref->file = file;
    ref->stream = stream;    
    HASH_ADD(hlink, stdio_runtime->stream_hash, stream, sizeof(FILE*), ref);

    return(file);
}

/* get a STDIO file record for the given stream */
static struct stdio_file_runtime* stdio_file_by_stream(FILE *stream)
{
    struct stdio_file_runtime_ref* ref;

    if(!stdio_runtime || instrumentation_disabled)
        return(NULL);

    /* search hash table for existing file ref for this stream */
    HASH_FIND(hlink, stdio_runtime->stream_hash, &stream, sizeof(FILE*), ref);
    if(ref)
        return(ref->file);

    return(NULL);
}

/* free up reference data structures for the given stream */
static void stdio_file_close_stream(FILE *stream)
{
    struct stdio_file_runtime_ref* ref;

    if(!stdio_runtime || instrumentation_disabled)
        return;

    /* search hash table for this stream */
    HASH_FIND(hlink, stdio_runtime->stream_hash, &stream, sizeof(FILE*), ref);
    if(ref)
    {
        /* we have a reference, delete it */
        HASH_DELETE(hlink, stdio_runtime->stream_hash, ref);
        free(ref);
    }

    return;
}

/************************************************************************
 * Functions exported by this module for coordinating with darshan-core *
 ************************************************************************/

static void stdio_begin_shutdown()
{
    assert(stdio_runtime);

    STDIO_LOCK();
    /* disable further instrumentation while Darshan shuts down */
    instrumentation_disabled = 1;
    STDIO_UNLOCK();

    return;
}

static void stdio_get_output_data(
    MPI_Comm mod_comm,
    darshan_record_id *shared_recs,
    int shared_rec_count,
    void **stdio_buf,
    int *stdio_buf_sz)
{
    struct stdio_file_runtime *file;
    int i;
    struct darshan_stdio_record *red_send_buf = NULL;
    struct darshan_stdio_record *red_recv_buf = NULL;
    MPI_Datatype red_type;
    MPI_Op red_op;

    assert(stdio_runtime);

    /* if there are globally shared files, do a shared file reduction */
    /* NOTE: the shared file reduction is also skipped if the 
     * DARSHAN_DISABLE_SHARED_REDUCTION environment variable is set.
     */
    if(shared_rec_count && !getenv("DARSHAN_DISABLE_SHARED_REDUCTION"))
    {
        /* necessary initialization of shared records */
        for(i = 0; i < shared_rec_count; i++)
        {
            HASH_FIND(hlink, stdio_runtime->file_hash, &shared_recs[i],
                sizeof(darshan_record_id), file);
            assert(file);

            file->file_record->rank = -1;
        }

        /* sort the array of files descending by rank so that we get all of the 
         * shared files (marked by rank -1) in a contiguous portion at end 
         * of the array
         */
        qsort(stdio_runtime->file_record_array, stdio_runtime->file_array_ndx,
            sizeof(struct darshan_stdio_record), stdio_record_compare);

        /* make *send_buf point to the shared files at the end of sorted array */
        red_send_buf =
            &(stdio_runtime->file_record_array[stdio_runtime->file_array_ndx-shared_rec_count]);

        /* allocate memory for the reduction output on rank 0 */
        if(my_rank == 0)
        {
            red_recv_buf = malloc(shared_rec_count * sizeof(struct darshan_stdio_record));
            if(!red_recv_buf)
            {
                return;
            }
        }

        /* construct a datatype for a STDIO file record.  This is serving no purpose
         * except to make sure we can do a reduction on proper boundaries
         */
        DARSHAN_MPI_CALL(PMPI_Type_contiguous)(sizeof(struct darshan_stdio_record),
            MPI_BYTE, &red_type);
        DARSHAN_MPI_CALL(PMPI_Type_commit)(&red_type);

        /* register a STDIO file record reduction operator */
        DARSHAN_MPI_CALL(PMPI_Op_create)(stdio_record_reduction_op, 1, &red_op);

        /* reduce shared STDIO file records */
        DARSHAN_MPI_CALL(PMPI_Reduce)(red_send_buf, red_recv_buf,
            shared_rec_count, red_type, red_op, 0, mod_comm);

        /* clean up reduction state */
        if(my_rank == 0)
        {
            int tmp_ndx = stdio_runtime->file_array_ndx - shared_rec_count;
            memcpy(&(stdio_runtime->file_record_array[tmp_ndx]), red_recv_buf,
                shared_rec_count * sizeof(struct darshan_stdio_record));
            free(red_recv_buf);
        }
        else
        {
            stdio_runtime->file_array_ndx -= shared_rec_count;
        }

        DARSHAN_MPI_CALL(PMPI_Type_free)(&red_type);
        DARSHAN_MPI_CALL(PMPI_Op_free)(&red_op);
    }

    *stdio_buf = (void *)(stdio_runtime->file_record_array);
    *stdio_buf_sz = stdio_runtime->file_array_ndx * sizeof(struct darshan_stdio_record);

    return;
}

/* compare function for sorting file records by descending rank */
static int stdio_record_compare(const void* a_p, const void* b_p)
{
    const struct darshan_stdio_record* a = a_p;
    const struct darshan_stdio_record* b = b_p;

    if(a->rank < b->rank)
        return 1;
    if(a->rank > b->rank)
        return -1;

    return 0;
}

static void stdio_record_reduction_op(void* infile_v, void* inoutfile_v,
    int *len, MPI_Datatype *datatype)
{
    struct darshan_stdio_record tmp_file;
    struct darshan_stdio_record *infile = infile_v;
    struct darshan_stdio_record *inoutfile = inoutfile_v;
    int i, j;

    assert(stdio_runtime);

    for(i=0; i<*len; i++)
    {
        memset(&tmp_file, 0, sizeof(struct darshan_stdio_record));
        tmp_file.f_id = infile->f_id;
        tmp_file.rank = -1;

        /* sum */
        for(j=STDIO_OPENS; j<=STDIO_BYTES_READ; j++)
        {
            tmp_file.counters[j] = infile->counters[j] + inoutfile->counters[j];
        }
        
        /* max */
        for(j=STDIO_MAX_BYTE_READ; j<=STDIO_MAX_BYTE_WRITTEN; j++)
        {
            if(infile->counters[j] > inoutfile->counters[j])
                tmp_file.counters[j] = infile->counters[j];
            else
                tmp_file.counters[j] = inoutfile->counters[j];
        }

        /* sum */
        for(j=STDIO_F_META_TIME; j<=STDIO_F_READ_TIME; j++)
        {
            tmp_file.fcounters[j] = infile->fcounters[j] + inoutfile->fcounters[j];
        }

        /* min non-zero (if available) value */
        for(j=STDIO_F_OPEN_START_TIMESTAMP; j<=STDIO_F_READ_START_TIMESTAMP; j++)
        {
            if((infile->fcounters[j] < inoutfile->fcounters[j] &&
               infile->fcounters[j] > 0) || inoutfile->fcounters[j] == 0) 
                tmp_file.fcounters[j] = infile->fcounters[j];
            else
                tmp_file.fcounters[j] = inoutfile->fcounters[j];
        }

        /* max */
        for(j=STDIO_F_OPEN_END_TIMESTAMP; j<=STDIO_F_READ_END_TIMESTAMP; j++)
        {
            if(infile->fcounters[j] > inoutfile->fcounters[j])
                tmp_file.fcounters[j] = infile->fcounters[j];
            else
                tmp_file.fcounters[j] = inoutfile->fcounters[j];
        }

        /* update pointers */
        *inoutfile = tmp_file;
        inoutfile++;
        infile++;
    }

    return;
}

static void stdio_shutdown()
{
    struct stdio_file_runtime_ref *ref, *tmp;

    assert(stdio_runtime);

    HASH_ITER(hlink, stdio_runtime->stream_hash, ref, tmp)
    {
        HASH_DELETE(hlink, stdio_runtime->stream_hash, ref);
        free(ref);
    }

    HASH_CLEAR(hlink, stdio_runtime->file_hash); /* these entries are freed all at once below */

    free(stdio_runtime->file_runtime_array);
    free(stdio_runtime->file_record_array);
    free(stdio_runtime);
    stdio_runtime = NULL;
    instrumentation_disabled = 0;
    
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
