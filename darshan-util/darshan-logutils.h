/*
 * Copyright (C) 2015 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

#ifndef __DARSHAN_LOG_UTILS_H
#define __DARSHAN_LOG_UTILS_H

#include <linux/limits.h>
#include <zlib.h>
#ifdef HAVE_LIBBZ2
#include <bzlib.h>
#endif

#include "uthash-1.9.2/src/uthash.h"

#include "darshan-log-format.h"

struct darshan_fd_int_state;

/* darshan file descriptor definition */
struct darshan_fd_s
{
    /* log file version */
    char version[8];
    /* flag indicating whether byte swapping needs to be
     * performed on log file data */
    int swap_flag;
    /* log file offset/length maps for each log file region */
    struct darshan_log_map job_map;
    struct darshan_log_map rec_map;
    struct darshan_log_map mod_map[DARSHAN_MAX_MODS];

    /* KEEP OUT -- remaining state hidden in logutils source */
    struct darshan_fd_int_state *state;
};
typedef struct darshan_fd_s* darshan_fd;

struct darshan_record_ref
{
    struct darshan_record rec;
    UT_hash_handle hlink;
};

/* functions to be implemented by each module for integration with
 * darshan log file utilities (e.g., parser & convert tools)
 */
struct darshan_mod_logutil_funcs
{
    /* retrieve a single module record from the log file */
    int (*log_get_record)(
        darshan_fd fd,
        void* buf,
        darshan_record_id* rec_id
    );
    /* put a single module record into the log file */
    int (*log_put_record)(
        darshan_fd fd,
        void *buf
    );
    /* print the counters for a given log file record */
    void (*log_print_record)(
        void *file_rec,
        char *file_name,
        char *mnt_pt,
        char *fs_type
    );
};

extern struct darshan_mod_logutil_funcs *mod_logutils[];

#include "darshan-posix-logutils.h"
#include "darshan-mpiio-logutils.h"
#include "darshan-hdf5-logutils.h"
#include "darshan-pnetcdf-logutils.h"
#include "darshan-bgq-logutils.h"

darshan_fd darshan_log_open(const char *name);
darshan_fd darshan_log_create(const char *name, enum darshan_comp_type comp_type);
int darshan_log_getjob(darshan_fd fd, struct darshan_job *job);
int darshan_log_putjob(darshan_fd fd, struct darshan_job *job);
int darshan_log_getexe(darshan_fd fd, char *buf);
int darshan_log_putexe(darshan_fd fd, char *buf);
int darshan_log_getmounts(darshan_fd fd, char*** mnt_pts,
    char*** fs_types, int* count);
int darshan_log_putmounts(darshan_fd fd, char** mnt_pts,
    char** fs_types, int count);
int darshan_log_gethash(darshan_fd fd, struct darshan_record_ref **hash);
int darshan_log_puthash(darshan_fd fd, struct darshan_record_ref *hash);
int darshan_log_getmod(darshan_fd fd, darshan_module_id mod_id,
    void *buf, int len);
int darshan_log_putmod(darshan_fd fd, darshan_module_id mod_id,
    void *mod_buf, int mod_buf_sz);
void darshan_log_close(darshan_fd file);

/* convenience macros for printing Darshan counters */
#define DARSHAN_PRINT_HEADER() \
    printf("\n#<module>\t<rank>\t<file>\t<counter>\t<value>" \
           "\t<file name>\t<mount pt>\t<fs type>\n")

#define DARSHAN_COUNTER_PRINT(__mod_name, __rank, __file_id, \
                              __counter, __counter_val, __file_name, \
                              __mnt_pt, __fs_type) do { \
    printf("%s\t%" PRId64 "\t%" PRIu64 "\t%s\t%" PRId64 "\t%s\t%s\t%s\n", \
        __mod_name, __rank, __file_id, __counter, __counter_val, \
        __file_name, __mnt_pt, __fs_type); \
} while(0)

#define DARSHAN_F_COUNTER_PRINT(__mod_name, __rank, __file_id, \
                                __counter, __counter_val, __file_name, \
                                __mnt_pt, __fs_type) do { \
    printf("%s\t%" PRId64 "\t%" PRIu64 "\t%s\t%f\t%s\t%s\t%s\n", \
        __mod_name, __rank, __file_id, __counter, __counter_val, \
        __file_name, __mnt_pt, __fs_type); \
} while(0)

/* naive byte swap implementation */
#define DARSHAN_BSWAP64(__ptr) do {\
    char __dst_char[8]; \
    char* __src_char = (char*)__ptr; \
    __dst_char[0] = __src_char[7]; \
    __dst_char[1] = __src_char[6]; \
    __dst_char[2] = __src_char[5]; \
    __dst_char[3] = __src_char[4]; \
    __dst_char[4] = __src_char[3]; \
    __dst_char[5] = __src_char[2]; \
    __dst_char[6] = __src_char[1]; \
    __dst_char[7] = __src_char[0]; \
    memcpy(__ptr, __dst_char, 8); \
} while(0)
#define DARSHAN_BSWAP32(__ptr) do {\
    char __dst_char[4]; \
    char* __src_char = (char*)__ptr; \
    __dst_char[0] = __src_char[3]; \
    __dst_char[1] = __src_char[2]; \
    __dst_char[2] = __src_char[1]; \
    __dst_char[3] = __src_char[0]; \
    memcpy(__ptr, __dst_char, 4); \
} while(0)

#endif
