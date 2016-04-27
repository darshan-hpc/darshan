/*
 * Copyright (C) 2015 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

#ifndef __DARSHAN_LOG_UTILS_H
#define __DARSHAN_LOG_UTILS_H

#include <limits.h>
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
    /* flag indicating whether a log file contains partial data */
    int partial_flag;
    /* log file offset/length maps for each log file region */
    struct darshan_log_map job_map;
    struct darshan_log_map rec_map;
    struct darshan_log_map mod_map[DARSHAN_MAX_MODS];
    /* module-specific log-format versions contained in log */
    uint32_t mod_ver[DARSHAN_MAX_MODS];

    /* KEEP OUT -- remaining state hidden in logutils source */
    struct darshan_fd_int_state *state;
};
typedef struct darshan_fd_s* darshan_fd;

struct darshan_record_ref
{
    struct darshan_record rec;
    UT_hash_handle hlink;
};

struct darshan_mnt_info
{
    char mnt_type[DARSHAN_EXE_LEN];
    char mnt_path[DARSHAN_EXE_LEN];
};

/* functions to be implemented by each module for integration with
 * darshan log file utilities (e.g., parser & convert tools)
 */
struct darshan_mod_logutil_funcs
{
    /* retrieve a single module record from the log file. 
     * return 1 on successful read of record, 0 on no more
     * module data, -1 on error
     *      - 'fd' is the file descriptor to get record from
     *      - 'buf' is the buffer to store the record in
     *      - 'rec_id' is the corresponding darshan record id
     */
    int (*log_get_record)(
        darshan_fd fd,
        void* buf,
        darshan_record_id* rec_id
    );
    /* put a single module record into the log file.
     * return 0 on success, -1 on error
     *      - 'fd' is the file descriptor to put record into
     *      - 'buf' is the buffer containing the record data
     *      - 'rec_id' is the corresponding darshan record id
     */
    int (*log_put_record)(
        darshan_fd fd,
        void *buf,
        int ver
    );
    /* print the counters for a given log record
     *      - 'file_rec' is the record's data buffer
     *      - 'file_name' is the file path string for the record
     *      - 'mnt-pt' is the file path mount point string
     *      - 'fs_type' is the file system type string
     *      - 'ver' is the version of the record
     */
    void (*log_print_record)(
        void *file_rec,
        char *file_name,
        char *mnt_pt,
        char *fs_type,
        int ver
    );
    /* print module-specific description of I/O characterization data */
    void (*log_print_description)(void);
    /* print a text diff of 2 module I/O records */
    void (*log_print_diff)(
        void *rec1,
        char *name1,
        void *rec2,
        char *name2
    );
};

extern struct darshan_mod_logutil_funcs *mod_logutils[];

#include "darshan-posix-logutils.h"
#include "darshan-mpiio-logutils.h"
#include "darshan-hdf5-logutils.h"
#include "darshan-pnetcdf-logutils.h"
#include "darshan-bgq-logutils.h"

darshan_fd darshan_log_open(const char *name);
darshan_fd darshan_log_create(const char *name, enum darshan_comp_type comp_type,
    int partial_flag);
int darshan_log_getjob(darshan_fd fd, struct darshan_job *job);
int darshan_log_putjob(darshan_fd fd, struct darshan_job *job);
int darshan_log_getexe(darshan_fd fd, char *buf);
int darshan_log_putexe(darshan_fd fd, char *buf);
int darshan_log_getmounts(darshan_fd fd, struct darshan_mnt_info **mnt_data_array,
    int* count);
int darshan_log_putmounts(darshan_fd fd, struct darshan_mnt_info *mnt_data_array,
    int count);
int darshan_log_gethash(darshan_fd fd, struct darshan_record_ref **hash);
int darshan_log_puthash(darshan_fd fd, struct darshan_record_ref *hash);
int darshan_log_getmod(darshan_fd fd, darshan_module_id mod_id,
    void *mod_buf, int mod_buf_sz);
int darshan_log_putmod(darshan_fd fd, darshan_module_id mod_id,
    void *mod_buf, int mod_buf_sz, int ver);
void darshan_log_close(darshan_fd file);

/* convenience macros for printing Darshan counters */
#define DARSHAN_PRINT_HEADER() \
    printf("\n#<module>\t<rank>\t<record id>\t<counter>\t<value>" \
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
