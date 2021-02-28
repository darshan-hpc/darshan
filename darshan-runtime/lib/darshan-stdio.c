/*
 * Copyright (C) 2015 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
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

#ifdef HAVE_CONFIG_H
# include <darshan-runtime-config.h>
#endif

#define _XOPEN_SOURCE 500
#define _GNU_SOURCE

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
#include <stdint.h>
#include <limits.h>

#include "darshan.h"
#include "darshan-dynamic.h"

#ifndef HAVE_OFF64_T
typedef int64_t off64_t;
#endif

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
DARSHAN_FORWARD_DECL(printf, int, (const char *format, ...));
DARSHAN_FORWARD_DECL(vfprintf, int, (FILE *stream, const char *format, va_list));
DARSHAN_FORWARD_DECL(vprintf, int, (const char *format, va_list));
DARSHAN_FORWARD_DECL(fread, size_t, (void *ptr, size_t size, size_t nmemb, FILE *stream));
DARSHAN_FORWARD_DECL(fgetc, int, (FILE *stream));
DARSHAN_FORWARD_DECL(getw, int, (FILE *stream));
DARSHAN_FORWARD_DECL(_IO_getc, int, (FILE *stream));
DARSHAN_FORWARD_DECL(_IO_putc, int, (int, FILE *stream));
DARSHAN_FORWARD_DECL(fscanf, int, (FILE *stream, const char *format, ...));
#ifndef HAVE_FSCANF_REDIRECT
DARSHAN_FORWARD_DECL(__isoc99_fscanf, int, (FILE *stream, const char *format, ...));
#endif
DARSHAN_FORWARD_DECL(vfscanf, int, (FILE *stream, const char *format, va_list ap));
DARSHAN_FORWARD_DECL(fgets, char*, (char *s, int size, FILE *stream));
DARSHAN_FORWARD_DECL(fseek, int, (FILE *stream, long offset, int whence));
DARSHAN_FORWARD_DECL(fseeko, int, (FILE *stream, off_t offset, int whence));
DARSHAN_FORWARD_DECL(fseeko64, int, (FILE *stream, off64_t offset, int whence));
DARSHAN_FORWARD_DECL(fsetpos, int, (FILE *stream, const fpos_t *pos));
DARSHAN_FORWARD_DECL(fsetpos64, int, (FILE *stream, const fpos64_t *pos));
DARSHAN_FORWARD_DECL(rewind, void, (FILE *stream));

/* structure to track stdio stats at runtime */
struct stdio_file_record_ref
{
    struct darshan_stdio_file* file_rec;
    int64_t offset;
    double last_meta_end;
    double last_read_end;
    double last_write_end;
    int fs_type;
};

/* The stdio_runtime structure maintains necessary state for storing
 * STDIO file records and for coordinating with darshan-core at 
 * shutdown time.
 */
struct stdio_runtime
{
    void *rec_id_hash;
    void *stream_hash;
    int file_rec_count;
};

static struct stdio_runtime *stdio_runtime = NULL;
static pthread_mutex_t stdio_runtime_mutex = PTHREAD_RECURSIVE_MUTEX_INITIALIZER_NP;
static int darshan_mem_alignment = 1;
static int my_rank = -1;

static void stdio_runtime_initialize(
    void);
static struct stdio_file_record_ref *stdio_track_new_file_record(
    darshan_record_id rec_id, const char *path);
#ifdef HAVE_MPI
static void stdio_record_reduction_op(void* infile_v, void* inoutfile_v,
    int *len, MPI_Datatype *datatype);
static void stdio_shared_record_variance(
    MPI_Comm mod_comm, struct darshan_stdio_file *inrec_array,
    struct darshan_stdio_file *outrec_array, int shared_rec_count);
static void stdio_mpi_redux(
    void *stdio_buf, MPI_Comm mod_comm,
    darshan_record_id *shared_recs, int shared_rec_count);
#endif
static void stdio_output(
    void **stdio_buf, int *stdio_buf_sz);
static void stdio_cleanup(
    void);

/* extern function def for querying record name from a POSIX fd */
extern char *darshan_posix_lookup_record_name(int fd);

/* we need access to fileno (defined in POSIX module) for instrumenting fopen calls */
#ifdef DARSHAN_PRELOAD
extern int (*__real_fileno)(FILE *stream);
#else
extern int __real_fileno(FILE *stream);
#endif

#define STDIO_LOCK() pthread_mutex_lock(&stdio_runtime_mutex)
#define STDIO_UNLOCK() pthread_mutex_unlock(&stdio_runtime_mutex)

#define STDIO_PRE_RECORD() do { \
    STDIO_LOCK(); \
    if(!darshan_core_disabled_instrumentation()) { \
        if(!stdio_runtime) stdio_runtime_initialize(); \
        if(stdio_runtime) break; \
    } \
    STDIO_UNLOCK(); \
    return(ret); \
} while(0)

#define STDIO_POST_RECORD() do { \
    STDIO_UNLOCK(); \
} while(0)

#define STDIO_RECORD_OPEN(__ret, __path, __tm1, __tm2) do { \
    darshan_record_id __rec_id; \
    struct stdio_file_record_ref *__rec_ref; \
    char *__newpath; \
    int __fd; \
    MAP_OR_FAIL(fileno); \
    if(!__ret || !__path) break; \
    __newpath = darshan_clean_file_path(__path); \
    if(!__newpath) __newpath = (char*)__path; \
    if(darshan_core_excluded_path(__newpath)) { \
        if(__newpath != (char*)__path) free(__newpath); \
        break; \
    } \
    __rec_id = darshan_core_gen_record_id(__newpath); \
    __rec_ref = darshan_lookup_record_ref(stdio_runtime->rec_id_hash, &__rec_id, sizeof(darshan_record_id)); \
    if(!__rec_ref) __rec_ref = stdio_track_new_file_record(__rec_id, __newpath); \
    if(!__rec_ref) { \
        if(__newpath != (char*)__path) free(__newpath); \
        break; \
    } \
    _STDIO_RECORD_OPEN(__ret, __rec_ref, __tm1, __tm2, 1, -1); \
    __fd = __real_fileno(__ret); \
    darshan_instrument_fs_data(__rec_ref->fs_type, __newpath, __fd); \
    if(__newpath != (char*)__path) free(__newpath); \
} while(0)

#define STDIO_RECORD_REFOPEN(__ret, __rec_ref, __tm1, __tm2, __ref_counter) do { \
    if(!ret || !rec_ref) break; \
    _STDIO_RECORD_OPEN(__ret, __rec_ref, __tm1, __tm2, 0, __ref_counter); \
} while(0)

#define _STDIO_RECORD_OPEN(__ret, __rec_ref, __tm1, __tm2, __reset_flag, __ref_counter) do { \
    if(__reset_flag) __rec_ref->offset = 0; \
    __rec_ref->file_rec->counters[STDIO_OPENS] += 1; \
    if(__ref_counter >= 0) __rec_ref->file_rec->counters[__ref_counter] += 1; \
    if(__rec_ref->file_rec->fcounters[STDIO_F_OPEN_START_TIMESTAMP] == 0 || \
     __rec_ref->file_rec->fcounters[STDIO_F_OPEN_START_TIMESTAMP] > __tm1) \
        __rec_ref->file_rec->fcounters[STDIO_F_OPEN_START_TIMESTAMP] = __tm1; \
    __rec_ref->file_rec->fcounters[STDIO_F_OPEN_END_TIMESTAMP] = __tm2; \
    DARSHAN_TIMER_INC_NO_OVERLAP(__rec_ref->file_rec->fcounters[STDIO_F_META_TIME], __tm1, __tm2, __rec_ref->last_meta_end); \
    darshan_add_record_ref(&(stdio_runtime->stream_hash), &(__ret), sizeof(__ret), __rec_ref); \
} while(0)


#define STDIO_RECORD_READ(__fp, __bytes,  __tm1, __tm2) do{ \
    struct stdio_file_record_ref* rec_ref; \
    int64_t this_offset; \
    rec_ref = darshan_lookup_record_ref(stdio_runtime->stream_hash, &(__fp), sizeof(__fp)); \
    if(!rec_ref) break; \
    this_offset = rec_ref->offset; \
    rec_ref->offset = this_offset + __bytes; \
    if(rec_ref->file_rec->counters[STDIO_MAX_BYTE_READ] < (this_offset + __bytes - 1)) \
        rec_ref->file_rec->counters[STDIO_MAX_BYTE_READ] = (this_offset + __bytes - 1); \
    rec_ref->file_rec->counters[STDIO_BYTES_READ] += __bytes; \
    rec_ref->file_rec->counters[STDIO_READS] += 1; \
    if(rec_ref->file_rec->fcounters[STDIO_F_READ_START_TIMESTAMP] == 0 || \
     rec_ref->file_rec->fcounters[STDIO_F_READ_START_TIMESTAMP] > __tm1) \
        rec_ref->file_rec->fcounters[STDIO_F_READ_START_TIMESTAMP] = __tm1; \
    rec_ref->file_rec->fcounters[STDIO_F_READ_END_TIMESTAMP] = __tm2; \
    DARSHAN_TIMER_INC_NO_OVERLAP(rec_ref->file_rec->fcounters[STDIO_F_READ_TIME], __tm1, __tm2, rec_ref->last_read_end); \
} while(0)

#define STDIO_RECORD_WRITE(__fp, __bytes,  __tm1, __tm2, __fflush_flag) do{ \
    struct stdio_file_record_ref* rec_ref; \
    int64_t this_offset; \
    rec_ref = darshan_lookup_record_ref(stdio_runtime->stream_hash, &(__fp), sizeof(__fp)); \
    if(!rec_ref) break; \
    this_offset = rec_ref->offset; \
    rec_ref->offset = this_offset + __bytes; \
    if(rec_ref->file_rec->counters[STDIO_MAX_BYTE_WRITTEN] < (this_offset + __bytes - 1)) \
        rec_ref->file_rec->counters[STDIO_MAX_BYTE_WRITTEN] = (this_offset + __bytes - 1); \
    rec_ref->file_rec->counters[STDIO_BYTES_WRITTEN] += __bytes; \
    if(__fflush_flag) \
        rec_ref->file_rec->counters[STDIO_FLUSHES] += 1; \
    else \
        rec_ref->file_rec->counters[STDIO_WRITES] += 1; \
    if(rec_ref->file_rec->fcounters[STDIO_F_WRITE_START_TIMESTAMP] == 0 || \
     rec_ref->file_rec->fcounters[STDIO_F_WRITE_START_TIMESTAMP] > __tm1) \
        rec_ref->file_rec->fcounters[STDIO_F_WRITE_START_TIMESTAMP] = __tm1; \
    rec_ref->file_rec->fcounters[STDIO_F_WRITE_END_TIMESTAMP] = __tm2; \
    DARSHAN_TIMER_INC_NO_OVERLAP(rec_ref->file_rec->fcounters[STDIO_F_WRITE_TIME], __tm1, __tm2, rec_ref->last_write_end); \
} while(0)

FILE* DARSHAN_DECL(fopen)(const char *path, const char *mode)
{
    FILE* ret;
    double tm1, tm2;

    MAP_OR_FAIL(fopen);

    tm1 = darshan_core_wtime();
    ret = __real_fopen(path, mode);
    tm2 = darshan_core_wtime();

    STDIO_PRE_RECORD();
    STDIO_RECORD_OPEN(ret, path, tm1, tm2);
    STDIO_POST_RECORD();

    return(ret);
}

FILE* DARSHAN_DECL(fopen64)(const char *path, const char *mode)
{
    FILE* ret;
    double tm1, tm2;

    MAP_OR_FAIL(fopen64);

    tm1 = darshan_core_wtime();
    ret = __real_fopen64(path, mode);
    tm2 = darshan_core_wtime();

    STDIO_PRE_RECORD();
    STDIO_RECORD_OPEN(ret, path, tm1, tm2);
    STDIO_POST_RECORD();

    return(ret);
}

FILE* DARSHAN_DECL(fdopen)(int fd, const char *mode)
{
    FILE* ret;
    double tm1, tm2;
    darshan_record_id rec_id;
    struct stdio_file_record_ref *rec_ref;

    MAP_OR_FAIL(fdopen);

    tm1 = darshan_core_wtime();
    ret = __real_fdopen(fd, mode);
    tm2 = darshan_core_wtime();

    if(ret)
    {
        char *rec_name = darshan_posix_lookup_record_name(fd);
        if(rec_name)
        {
            rec_id = darshan_core_gen_record_id(rec_name);

            STDIO_PRE_RECORD();
            rec_ref = darshan_lookup_record_ref(stdio_runtime->rec_id_hash,
                &rec_id, sizeof(darshan_record_id));
            if(!rec_ref)
                rec_ref = stdio_track_new_file_record(rec_id, rec_name);
            STDIO_RECORD_REFOPEN(ret, rec_ref, tm1, tm2, STDIO_FDOPENS);
            STDIO_POST_RECORD();
        }
    }


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

    STDIO_PRE_RECORD();
    STDIO_RECORD_OPEN(ret, path, tm1, tm2);
    STDIO_POST_RECORD();

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

    STDIO_PRE_RECORD();
    STDIO_RECORD_OPEN(ret, path, tm1, tm2);
    STDIO_POST_RECORD();

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

    STDIO_PRE_RECORD();
    if(ret >= 0)
        STDIO_RECORD_WRITE(fp, 0, tm1, tm2, 1);
    STDIO_POST_RECORD();

    return(ret);
}

int DARSHAN_DECL(fclose)(FILE *fp)
{
    double tm1, tm2;
    int ret;
    struct stdio_file_record_ref *rec_ref;

    MAP_OR_FAIL(fclose);

    tm1 = darshan_core_wtime();
    ret = __real_fclose(fp);
    tm2 = darshan_core_wtime();

    STDIO_PRE_RECORD();
    rec_ref = darshan_lookup_record_ref(stdio_runtime->stream_hash, &fp, sizeof(fp));
    if(rec_ref)
    {
        if(rec_ref->file_rec->fcounters[STDIO_F_CLOSE_START_TIMESTAMP] == 0 ||
         rec_ref->file_rec->fcounters[STDIO_F_CLOSE_START_TIMESTAMP] > tm1)
           rec_ref->file_rec->fcounters[STDIO_F_CLOSE_START_TIMESTAMP] = tm1;
        rec_ref->file_rec->fcounters[STDIO_F_CLOSE_END_TIMESTAMP] = tm2;
        DARSHAN_TIMER_INC_NO_OVERLAP(
            rec_ref->file_rec->fcounters[STDIO_F_META_TIME],
            tm1, tm2, rec_ref->last_meta_end);
        darshan_delete_record_ref(&(stdio_runtime->stream_hash), &fp, sizeof(fp));
    }
    STDIO_POST_RECORD();

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

    STDIO_PRE_RECORD();
    if(ret > 0)
        STDIO_RECORD_WRITE(stream, size*ret, tm1, tm2, 0);
    STDIO_POST_RECORD();

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

    STDIO_PRE_RECORD();
    if(ret != EOF)
        STDIO_RECORD_WRITE(stream, 1, tm1, tm2, 0);
    STDIO_POST_RECORD();

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

    STDIO_PRE_RECORD();
    if(ret != EOF)
        STDIO_RECORD_WRITE(stream, sizeof(int), tm1, tm2, 0);
    STDIO_POST_RECORD();

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

    STDIO_PRE_RECORD();
    if(ret != EOF && ret > 0)
        STDIO_RECORD_WRITE(stream, strlen(s), tm1, tm2, 0);
    STDIO_POST_RECORD();

    return(ret);
}

int DARSHAN_DECL(vprintf)(const char *format, va_list ap)
{
    int ret;
    double tm1, tm2;

    MAP_OR_FAIL(vprintf);

    tm1 = darshan_core_wtime();
    ret = __real_vprintf(format, ap);
    tm2 = darshan_core_wtime();

    STDIO_PRE_RECORD();
    if(ret > 0)
        STDIO_RECORD_WRITE(stdout, ret, tm1, tm2, 0);
    STDIO_POST_RECORD();

    return(ret);
}

int DARSHAN_DECL(vfprintf)(FILE *stream, const char *format, va_list ap)
{
    int ret;
    double tm1, tm2;

    MAP_OR_FAIL(vfprintf);

    tm1 = darshan_core_wtime();
    ret = __real_vfprintf(stream, format, ap);
    tm2 = darshan_core_wtime();

    STDIO_PRE_RECORD();
    if(ret > 0)
        STDIO_RECORD_WRITE(stream, ret, tm1, tm2, 0);
    STDIO_POST_RECORD();

    return(ret);
}


int DARSHAN_DECL(printf)(const char *format, ...)
{
    int ret;
    double tm1, tm2;
    va_list ap;

    MAP_OR_FAIL(vprintf);

    tm1 = darshan_core_wtime();
    /* NOTE: we intentionally switch to vprintf here to handle the variable
     * length arguments.
     */
    va_start(ap, format);
    ret = __real_vprintf(format, ap);
    va_end(ap);
    tm2 = darshan_core_wtime();

    STDIO_PRE_RECORD();
    if(ret > 0)
        STDIO_RECORD_WRITE(stdout, ret, tm1, tm2, 0);
    STDIO_POST_RECORD();

    return(ret);
}

int DARSHAN_DECL(fprintf)(FILE *stream, const char *format, ...)
{
    int ret;
    double tm1, tm2;
    va_list ap;

    MAP_OR_FAIL(vfprintf);

    tm1 = darshan_core_wtime();
    /* NOTE: we intentionally switch to vfprintf here to handle the variable
     * length arguments.
     */
    va_start(ap, format);
    ret = __real_vfprintf(stream, format, ap);
    va_end(ap);
    tm2 = darshan_core_wtime();

    STDIO_PRE_RECORD();
    if(ret > 0)
        STDIO_RECORD_WRITE(stream, ret, tm1, tm2, 0);
    STDIO_POST_RECORD();

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

    STDIO_PRE_RECORD();
    if(ret > 0)
        STDIO_RECORD_READ(stream, size*ret, tm1, tm2);
    STDIO_POST_RECORD();

    return(ret);
}

int DARSHAN_DECL(fgetc)(FILE *stream)
{
    int ret;
    double tm1, tm2;

    MAP_OR_FAIL(fgetc);

    tm1 = darshan_core_wtime();
    ret = __real_fgetc(stream);
    tm2 = darshan_core_wtime();

    STDIO_PRE_RECORD();
    if(ret != EOF)
        STDIO_RECORD_READ(stream, 1, tm1, tm2);
    STDIO_POST_RECORD();

    return(ret);
}

/* NOTE: stdio.h typically implements getc() as a macro pointing to _IO_getc */
int DARSHAN_DECL(_IO_getc)(FILE *stream)
{
    int ret;
    double tm1, tm2;

    MAP_OR_FAIL(_IO_getc);

    tm1 = darshan_core_wtime();
    ret = __real__IO_getc(stream);
    tm2 = darshan_core_wtime();

    STDIO_PRE_RECORD();
    if(ret != EOF)
        STDIO_RECORD_READ(stream, 1, tm1, tm2);
    STDIO_POST_RECORD();

    return(ret);
}

/* NOTE: stdio.h typically implements putc() as a macro pointing to _IO_putc */
int DARSHAN_DECL(_IO_putc)(int c, FILE *stream)
{
    int ret;
    double tm1, tm2;

    MAP_OR_FAIL(_IO_putc);

    tm1 = darshan_core_wtime();
    ret = __real__IO_putc(c, stream);
    tm2 = darshan_core_wtime();

    STDIO_PRE_RECORD();
    if(ret != EOF)
        STDIO_RECORD_WRITE(stream, 1, tm1, tm2, 0);
    STDIO_POST_RECORD();

    return(ret);
}

int DARSHAN_DECL(getw)(FILE *stream)
{
    int ret;
    double tm1, tm2;

    MAP_OR_FAIL(getw);

    tm1 = darshan_core_wtime();
    ret = __real_getw(stream);
    tm2 = darshan_core_wtime();

    STDIO_PRE_RECORD();
    if(ret != EOF || ferror(stream) == 0)
        STDIO_RECORD_READ(stream, sizeof(int), tm1, tm2);
    STDIO_POST_RECORD();

    return(ret);
}

#ifndef HAVE_FSCANF_REDIRECT
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

    STDIO_PRE_RECORD();
    if(ret != 0)
        STDIO_RECORD_READ(stream, (end_off-start_off), tm1, tm2);
    STDIO_POST_RECORD();

    return(ret);
}
#endif

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

    STDIO_PRE_RECORD();
    if(ret != 0)
        STDIO_RECORD_READ(stream, (end_off-start_off), tm1, tm2);
    STDIO_POST_RECORD();

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

    STDIO_PRE_RECORD();
    if(ret != 0)
        STDIO_RECORD_READ(stream, end_off-start_off, tm1, tm2);
    STDIO_POST_RECORD();

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

    STDIO_PRE_RECORD();
    if(ret != NULL)
        STDIO_RECORD_READ(stream, strlen(ret), tm1, tm2);
    STDIO_POST_RECORD();

    return(ret);
}


void DARSHAN_DECL(rewind)(FILE *stream)
{
    double tm1, tm2;
    struct stdio_file_record_ref *rec_ref;

    MAP_OR_FAIL(rewind);

    tm1 = darshan_core_wtime();
    __real_rewind(stream);
    tm2 = darshan_core_wtime();

    /* NOTE: we don't use STDIO_PRE_RECORD here because there is no return
     * value in this wrapper.
     */
    STDIO_LOCK();
    if(darshan_core_disabled_instrumentation()) {
        STDIO_UNLOCK();
        return;
    }
    if(!stdio_runtime) stdio_runtime_initialize();
    if(!stdio_runtime) {
        STDIO_UNLOCK();
        return;
    }

    rec_ref = darshan_lookup_record_ref(stdio_runtime->stream_hash, &stream, sizeof(stream));

    if(rec_ref)
    {
        rec_ref->offset = 0;
        DARSHAN_TIMER_INC_NO_OVERLAP(
            rec_ref->file_rec->fcounters[STDIO_F_META_TIME],
            tm1, tm2, rec_ref->last_meta_end);
        rec_ref->file_rec->counters[STDIO_SEEKS] += 1;
    }
    STDIO_POST_RECORD();

    return;
}

int DARSHAN_DECL(fseek)(FILE *stream, long offset, int whence)
{
    int ret;
    struct stdio_file_record_ref *rec_ref;
    double tm1, tm2;

    MAP_OR_FAIL(fseek);

    tm1 = darshan_core_wtime();
    ret = __real_fseek(stream, offset, whence);
    tm2 = darshan_core_wtime();

    if(ret >= 0)
    {
        STDIO_PRE_RECORD();
        rec_ref = darshan_lookup_record_ref(stdio_runtime->stream_hash, &stream, sizeof(stream));
        if(rec_ref)
        {
            rec_ref->offset = ftell(stream);
            DARSHAN_TIMER_INC_NO_OVERLAP(
                rec_ref->file_rec->fcounters[STDIO_F_META_TIME],
                tm1, tm2, rec_ref->last_meta_end);
            rec_ref->file_rec->counters[STDIO_SEEKS] += 1;
        }
        STDIO_POST_RECORD();
    }

    return(ret);
}

int DARSHAN_DECL(fseeko)(FILE *stream, off_t offset, int whence)
{
    int ret;
    struct stdio_file_record_ref *rec_ref;
    double tm1, tm2;

    MAP_OR_FAIL(fseeko);

    tm1 = darshan_core_wtime();
    ret = __real_fseeko(stream, offset, whence);
    tm2 = darshan_core_wtime();

    if(ret >= 0)
    {
        STDIO_PRE_RECORD();
        rec_ref = darshan_lookup_record_ref(stdio_runtime->stream_hash, &stream, sizeof(stream));
        if(rec_ref)
        {
            rec_ref->offset = ftell(stream);
            DARSHAN_TIMER_INC_NO_OVERLAP(
                rec_ref->file_rec->fcounters[STDIO_F_META_TIME],
                tm1, tm2, rec_ref->last_meta_end);
            rec_ref->file_rec->counters[STDIO_SEEKS] += 1;
        }
        STDIO_POST_RECORD();
    }

    return(ret);
}

int DARSHAN_DECL(fseeko64)(FILE *stream, off64_t offset, int whence)
{
    int ret;
    struct stdio_file_record_ref *rec_ref;
    double tm1, tm2;

    MAP_OR_FAIL(fseeko64);

    tm1 = darshan_core_wtime();
    ret = __real_fseeko64(stream, offset, whence);
    tm2 = darshan_core_wtime();

    if(ret >= 0)
    {
        STDIO_PRE_RECORD();
        rec_ref = darshan_lookup_record_ref(stdio_runtime->stream_hash, &stream, sizeof(stream));
        if(rec_ref)
        {
            rec_ref->offset = ftell(stream);
            DARSHAN_TIMER_INC_NO_OVERLAP(
                rec_ref->file_rec->fcounters[STDIO_F_META_TIME],
                tm1, tm2, rec_ref->last_meta_end);
            rec_ref->file_rec->counters[STDIO_SEEKS] += 1;
        }
        STDIO_POST_RECORD();
    }

    return(ret);
}

int DARSHAN_DECL(fsetpos)(FILE *stream, const fpos_t *pos)
{
    int ret;
    struct stdio_file_record_ref *rec_ref;
    double tm1, tm2;

    MAP_OR_FAIL(fsetpos);

    tm1 = darshan_core_wtime();
    ret = __real_fsetpos(stream, pos);
    tm2 = darshan_core_wtime();

    if(ret >= 0)
    {
        STDIO_PRE_RECORD();
        rec_ref = darshan_lookup_record_ref(stdio_runtime->stream_hash, &stream, sizeof(stream));
        if(rec_ref)
        {
            rec_ref->offset = ftell(stream);
            DARSHAN_TIMER_INC_NO_OVERLAP(
                rec_ref->file_rec->fcounters[STDIO_F_META_TIME],
                tm1, tm2, rec_ref->last_meta_end);
            rec_ref->file_rec->counters[STDIO_SEEKS] += 1;
        }
        STDIO_POST_RECORD();
    }

    return(ret);
}

int DARSHAN_DECL(fsetpos64)(FILE *stream, const fpos64_t *pos)
{
    int ret;
    struct stdio_file_record_ref *rec_ref;
    double tm1, tm2;

    MAP_OR_FAIL(fsetpos64);

    tm1 = darshan_core_wtime();
    ret = __real_fsetpos64(stream, pos);
    tm2 = darshan_core_wtime();

    if(ret >= 0)
    {
        STDIO_PRE_RECORD();
        rec_ref = darshan_lookup_record_ref(stdio_runtime->stream_hash, &stream, sizeof(stream));
        if(rec_ref)
        {
            rec_ref->offset = ftell(stream);
            DARSHAN_TIMER_INC_NO_OVERLAP(
                rec_ref->file_rec->fcounters[STDIO_F_META_TIME],
                tm1, tm2, rec_ref->last_meta_end);
            rec_ref->file_rec->counters[STDIO_SEEKS] += 1;
        }
        STDIO_POST_RECORD();
    }

    return(ret);
}

/**********************************************************
 * Internal functions for manipulating STDIO module state *
 **********************************************************/

/* initialize internal STDIO module data structures and register with darshan-core */
static void stdio_runtime_initialize()
{
    size_t stdio_buf_size;
    darshan_module_funcs mod_funcs = {
#ifdef HAVE_MPI
    .mod_redux_func = &stdio_mpi_redux,
#endif
    .mod_output_func = &stdio_output,
    .mod_cleanup_func = &stdio_cleanup
    };

    /* try to store default number of records for this module */
    stdio_buf_size = DARSHAN_DEF_MOD_REC_COUNT * sizeof(struct darshan_stdio_file);

    /* register the stdio module with darshan core */
    darshan_core_register_module(
        DARSHAN_STDIO_MOD,
        mod_funcs,
        &stdio_buf_size,
        &my_rank,
        &darshan_mem_alignment);

    stdio_runtime = malloc(sizeof(*stdio_runtime));
    if(!stdio_runtime)
    {
        darshan_core_unregister_module(DARSHAN_STDIO_MOD);
        return;
    }
    memset(stdio_runtime, 0, sizeof(*stdio_runtime));

    /* instantiate records for stdin, stdout, and stderr */
    STDIO_RECORD_OPEN(stdin, "<STDIN>", 0, 0);
    STDIO_RECORD_OPEN(stdout, "<STDOUT>", 0, 0);
    STDIO_RECORD_OPEN(stderr, "<STDERR>", 0, 0);
}

static struct stdio_file_record_ref *stdio_track_new_file_record(
    darshan_record_id rec_id, const char *path)
{
    struct darshan_stdio_file *file_rec = NULL;
    struct stdio_file_record_ref *rec_ref = NULL;
    struct darshan_fs_info fs_info;
    int ret;

    rec_ref = malloc(sizeof(*rec_ref));
    if(!rec_ref)
        return(NULL);
    memset(rec_ref, 0, sizeof(*rec_ref));

    /* add a reference to this file record based on record id */
    ret = darshan_add_record_ref(&(stdio_runtime->rec_id_hash), &rec_id,
        sizeof(darshan_record_id), rec_ref);
    if(ret == 0)
    {
        free(rec_ref);
        return(NULL);
    }

    /* register the actual file record with darshan-core so it is persisted
     * in the log file
     */
    file_rec = darshan_core_register_record(
        rec_id,
        path,
        DARSHAN_STDIO_MOD,
        sizeof(struct darshan_stdio_file),
        &fs_info);

    if(!file_rec)
    {
        darshan_delete_record_ref(&(stdio_runtime->rec_id_hash),
            &rec_id, sizeof(darshan_record_id));
        free(rec_ref);
        return(NULL);
    }

    /* registering this file record was successful, so initialize some fields */
    file_rec->base_rec.id = rec_id;
    file_rec->base_rec.rank = my_rank;
    rec_ref->fs_type = fs_info.fs_type;
    rec_ref->file_rec = file_rec;
    stdio_runtime->file_rec_count++;

    return(rec_ref);
}

#ifdef HAVE_MPI
static void stdio_record_reduction_op(void* infile_v, void* inoutfile_v,
    int *len, MPI_Datatype *datatype)
{
    struct darshan_stdio_file tmp_file;
    struct darshan_stdio_file *infile = infile_v;
    struct darshan_stdio_file *inoutfile = inoutfile_v;
    int i, j;

    assert(stdio_runtime);

    for(i=0; i<*len; i++)
    {
        memset(&tmp_file, 0, sizeof(struct darshan_stdio_file));
        tmp_file.base_rec.id = infile->base_rec.id;
        tmp_file.base_rec.rank = -1;

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

        /* min (zeroes are ok here; some procs don't do I/O) */
        if(infile->fcounters[STDIO_F_FASTEST_RANK_TIME] <
           inoutfile->fcounters[STDIO_F_FASTEST_RANK_TIME])
        {
            tmp_file.counters[STDIO_FASTEST_RANK] =
                infile->counters[STDIO_FASTEST_RANK];
            tmp_file.counters[STDIO_FASTEST_RANK_BYTES] =
                infile->counters[STDIO_FASTEST_RANK_BYTES];
            tmp_file.fcounters[STDIO_F_FASTEST_RANK_TIME] =
                infile->fcounters[STDIO_F_FASTEST_RANK_TIME];
        }
        else
        {
            tmp_file.counters[STDIO_FASTEST_RANK] =
                inoutfile->counters[STDIO_FASTEST_RANK];
            tmp_file.counters[STDIO_FASTEST_RANK_BYTES] =
                inoutfile->counters[STDIO_FASTEST_RANK_BYTES];
            tmp_file.fcounters[STDIO_F_FASTEST_RANK_TIME] =
                inoutfile->fcounters[STDIO_F_FASTEST_RANK_TIME];
        }

        /* max */
        if(infile->fcounters[STDIO_F_SLOWEST_RANK_TIME] >
           inoutfile->fcounters[STDIO_F_SLOWEST_RANK_TIME])
        {
            tmp_file.counters[STDIO_SLOWEST_RANK] =
                infile->counters[STDIO_SLOWEST_RANK];
            tmp_file.counters[STDIO_SLOWEST_RANK_BYTES] =
                infile->counters[STDIO_SLOWEST_RANK_BYTES];
            tmp_file.fcounters[STDIO_F_SLOWEST_RANK_TIME] =
                infile->fcounters[STDIO_F_SLOWEST_RANK_TIME];
        }
        else
        {
            tmp_file.counters[STDIO_SLOWEST_RANK] =
                inoutfile->counters[STDIO_SLOWEST_RANK];
            tmp_file.counters[STDIO_SLOWEST_RANK_BYTES] =
                inoutfile->counters[STDIO_SLOWEST_RANK_BYTES];
            tmp_file.fcounters[STDIO_F_SLOWEST_RANK_TIME] =
                inoutfile->fcounters[STDIO_F_SLOWEST_RANK_TIME];
        }

        /* update pointers */
        *inoutfile = tmp_file;
        inoutfile++;
        infile++;
    }

    return;
}

static void stdio_shared_record_variance(MPI_Comm mod_comm,
    struct darshan_stdio_file *inrec_array, struct darshan_stdio_file *outrec_array,
    int shared_rec_count)
{
    MPI_Datatype var_dt;
    MPI_Op var_op;
    int i;
    struct darshan_variance_dt *var_send_buf = NULL;
    struct darshan_variance_dt *var_recv_buf = NULL;

    PMPI_Type_contiguous(sizeof(struct darshan_variance_dt),
        MPI_BYTE, &var_dt);
    PMPI_Type_commit(&var_dt);

    PMPI_Op_create(darshan_variance_reduce, 1, &var_op);

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
        var_send_buf[i].T = inrec_array[i].fcounters[STDIO_F_READ_TIME] +
                            inrec_array[i].fcounters[STDIO_F_WRITE_TIME] +
                            inrec_array[i].fcounters[STDIO_F_META_TIME];
    }

    PMPI_Reduce(var_send_buf, var_recv_buf, shared_rec_count,
        var_dt, var_op, 0, mod_comm);

    if(my_rank == 0)
    {
        for(i=0; i<shared_rec_count; i++)
        {
            outrec_array[i].fcounters[STDIO_F_VARIANCE_RANK_TIME] =
                (var_recv_buf[i].S / var_recv_buf[i].n);
        }
    }

    /* get total bytes moved variances for shared records */

    for(i=0; i<shared_rec_count; i++)
    {
        var_send_buf[i].n = 1;
        var_send_buf[i].S = 0;
        var_send_buf[i].T = (double)
                            inrec_array[i].counters[STDIO_BYTES_READ] +
                            inrec_array[i].counters[STDIO_BYTES_WRITTEN];
    }

    PMPI_Reduce(var_send_buf, var_recv_buf, shared_rec_count,
        var_dt, var_op, 0, mod_comm);

    if(my_rank == 0)
    {
        for(i=0; i<shared_rec_count; i++)
        {
            outrec_array[i].fcounters[STDIO_F_VARIANCE_RANK_BYTES] =
                (var_recv_buf[i].S / var_recv_buf[i].n);
        }
    }

    PMPI_Type_free(&var_dt);
    PMPI_Op_free(&var_op);
    free(var_send_buf);
    free(var_recv_buf);

    return;
}
#endif

char *darshan_stdio_lookup_record_name(FILE *stream)
{
    struct stdio_file_record_ref *rec_ref;
    char *rec_name = NULL;

    STDIO_LOCK();
    if(stdio_runtime)
    {
        rec_ref = darshan_lookup_record_ref(stdio_runtime->stream_hash,
            &stream, sizeof(stream));
        if(rec_ref)
            rec_name = darshan_core_lookup_record_name(rec_ref->file_rec->base_rec.id);
    }
    STDIO_UNLOCK();

    return(rec_name);
}

/************************************************************************
 * Functions exported by this module for coordinating with darshan-core *
 ************************************************************************/

#ifdef HAVE_MPI
static void stdio_mpi_redux(
    void *stdio_buf,
    MPI_Comm mod_comm,
    darshan_record_id *shared_recs,
    int shared_rec_count)
{
    int stdio_rec_count;
    struct stdio_file_record_ref *rec_ref;
    struct darshan_stdio_file *stdio_rec_buf = (struct darshan_stdio_file *)stdio_buf;
    double stdio_time;
    struct darshan_stdio_file *red_send_buf = NULL;
    struct darshan_stdio_file *red_recv_buf = NULL;
    MPI_Datatype red_type;
    MPI_Op red_op;
    int i;

    STDIO_LOCK();
    assert(stdio_runtime);

    stdio_rec_count = stdio_runtime->file_rec_count;

    /* necessary initialization of shared records */
    for(i = 0; i < shared_rec_count; i++)
    {
        rec_ref = darshan_lookup_record_ref(stdio_runtime->rec_id_hash,
            &shared_recs[i], sizeof(darshan_record_id));
        assert(rec_ref);

        stdio_time =
            rec_ref->file_rec->fcounters[STDIO_F_READ_TIME] +
            rec_ref->file_rec->fcounters[STDIO_F_WRITE_TIME] +
            rec_ref->file_rec->fcounters[STDIO_F_META_TIME];

        /* initialize fastest/slowest info prior to the reduction */
        rec_ref->file_rec->counters[STDIO_FASTEST_RANK] =
            rec_ref->file_rec->base_rec.rank;
        rec_ref->file_rec->counters[STDIO_FASTEST_RANK_BYTES] =
            rec_ref->file_rec->counters[STDIO_BYTES_READ] +
            rec_ref->file_rec->counters[STDIO_BYTES_WRITTEN];
        rec_ref->file_rec->fcounters[STDIO_F_FASTEST_RANK_TIME] =
            stdio_time;

        /* until reduction occurs, we assume that this rank is both
         * the fastest and slowest. It is up to the reduction operator
         * to find the true min and max.
         */
        rec_ref->file_rec->counters[STDIO_SLOWEST_RANK] =
            rec_ref->file_rec->counters[STDIO_FASTEST_RANK];
        rec_ref->file_rec->counters[STDIO_SLOWEST_RANK_BYTES] =
            rec_ref->file_rec->counters[STDIO_FASTEST_RANK_BYTES];
        rec_ref->file_rec->fcounters[STDIO_F_SLOWEST_RANK_TIME] =
            rec_ref->file_rec->fcounters[STDIO_F_FASTEST_RANK_TIME];

        rec_ref->file_rec->base_rec.rank = -1;
    }

    /* sort the array of files descending by rank so that we get all of the 
     * shared files (marked by rank -1) in a contiguous portion at end 
     * of the array
     */
    darshan_record_sort(stdio_rec_buf, stdio_rec_count, sizeof(struct darshan_stdio_file));

    /* make *send_buf point to the shared files at the end of sorted array */
    red_send_buf = &(stdio_rec_buf[stdio_rec_count-shared_rec_count]);

    /* allocate memory for the reduction output on rank 0 */
    if(my_rank == 0)
    {
        red_recv_buf = malloc(shared_rec_count * sizeof(struct darshan_stdio_file));
        if(!red_recv_buf)
        {
            STDIO_UNLOCK();
            return;
        }
    }

    /* construct a datatype for a STDIO file record.  This is serving no purpose
     * except to make sure we can do a reduction on proper boundaries
     */
    PMPI_Type_contiguous(sizeof(struct darshan_stdio_file),
        MPI_BYTE, &red_type);
    PMPI_Type_commit(&red_type);

    /* register a STDIO file record reduction operator */
    PMPI_Op_create(stdio_record_reduction_op, 1, &red_op);

    /* reduce shared STDIO file records */
    PMPI_Reduce(red_send_buf, red_recv_buf,
        shared_rec_count, red_type, red_op, 0, mod_comm);

    /* get the time and byte variances for shared files */
    stdio_shared_record_variance(mod_comm, red_send_buf, red_recv_buf,
        shared_rec_count);

    /* update module state to account for shared file reduction */
    if(my_rank == 0)
    {
        /* overwrite local shared records with globally reduced records */
        int tmp_ndx = stdio_rec_count - shared_rec_count;
        memcpy(&(stdio_rec_buf[tmp_ndx]), red_recv_buf,
            shared_rec_count * sizeof(struct darshan_stdio_file));
        free(red_recv_buf);
    }
    else
    {
        /* drop shared records on non-zero ranks */
        stdio_runtime->file_rec_count -= shared_rec_count;
    }

    PMPI_Type_free(&red_type);
    PMPI_Op_free(&red_op);

    STDIO_UNLOCK();
    return;
}
#endif

static void stdio_output(
    void **stdio_buf,
    int *stdio_buf_sz)
{
    int stdio_rec_count;
    struct darshan_stdio_file *stdio_rec_buf = *(struct darshan_stdio_file **)stdio_buf;
    int i;

    STDIO_LOCK();
    assert(stdio_runtime);

    stdio_rec_count = stdio_runtime->file_rec_count;

    /* filter out any records that have no activity on them; this is
     * specifically meant to filter out unused stdin, stdout, or stderr
     * entries
     *
     * NOTE: we can no longer use the darshan_lookup_record_ref()
     * function at this point to find specific records, because the
     * logic above has likely broken the mapping to the static array.
     * We walk it manually here instead.
     */
    darshan_record_id stdin_rec_id = darshan_core_gen_record_id("<STDIN>");
    darshan_record_id stdout_rec_id = darshan_core_gen_record_id("<STDOUT>");
    darshan_record_id stderr_rec_id = darshan_core_gen_record_id("<STDERR>");
    for(i=0; i<stdio_rec_count; i++)
    {
        if((stdio_rec_buf[i].base_rec.id == stdin_rec_id) ||
           (stdio_rec_buf[i].base_rec.id == stdout_rec_id) ||
           (stdio_rec_buf[i].base_rec.id == stderr_rec_id))
        {
            if(stdio_rec_buf[i].counters[STDIO_WRITES] == 0 &&
                stdio_rec_buf[i].counters[STDIO_READS] == 0)
            {
                if(i != (stdio_rec_count-1))
                {
                    memmove(&stdio_rec_buf[i], &stdio_rec_buf[i+1],
                        (stdio_rec_count-i-1)*sizeof(stdio_rec_buf[i]));
                    i--;
                }
                stdio_rec_count--;
            }
        }
    }

    /* just pass back our updated total buffer size -- no need to update buffer */
    *stdio_buf_sz = stdio_rec_count * sizeof(struct darshan_stdio_file);

    STDIO_UNLOCK();
    return;
}

static void stdio_cleanup()
{
    STDIO_LOCK();
    assert(stdio_runtime);

    /* cleanup internal structures used for instrumenting */
    darshan_clear_record_refs(&(stdio_runtime->stream_hash), 0);
    darshan_clear_record_refs(&(stdio_runtime->rec_id_hash), 1);

    free(stdio_runtime);
    stdio_runtime = NULL;

    STDIO_UNLOCK();
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
