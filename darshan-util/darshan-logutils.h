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

/* Maximum size of a record - Lustre OST lists can get huge, but 81920 is enough
 * for 10K OSTs 
 */
#define DEF_MOD_BUF_SIZE 81920

struct darshan_fd_int_state;

/* darshan file descriptor definition */
struct darshan_fd_s
{
    /* log file version */
    char version[8];
    /* flag indicating whether byte swapping needs to be
     * performed on log file data
     */
    int swap_flag;
    /* flag indicating whether a log file contains partial data */
    int partial_flag;
    /* compression type used on log file */
    enum darshan_comp_type comp_type;
    /* log file offset/length maps for each log file region */
    struct darshan_log_map job_map;
    struct darshan_log_map name_map;
    struct darshan_log_map mod_map[DARSHAN_MAX_MODS];
    /* module-specific log-format versions contained in log */
    uint32_t mod_ver[DARSHAN_MAX_MODS];

    /* KEEP OUT -- remaining state hidden in logutils source */
    struct darshan_fd_int_state *state;
};
typedef struct darshan_fd_s* darshan_fd;

struct darshan_name_record_ref
{
    struct darshan_name_record *name_record;
    UT_hash_handle hlink;
};

/* DXT */
struct lustre_record_ref
{
	struct darshan_lustre_record *rec;
	UT_hash_handle hlink;
};

struct darshan_mnt_info
{
    char mnt_type[DARSHAN_EXE_LEN];
    char mnt_path[DARSHAN_EXE_LEN];
};

struct darshan_mod_info
{
    const char *name;
    int  len;
    int  ver;
    int  idx;
};

struct darshan_name_record_info
{
    darshan_record_id id;
    char *name;
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
     *      - 'buf' is a pointer to a buffer address to store the record in
     *          * NOTE: if the buffer pointed to is NULL, the record memory is malloc'ed
     */
    int (*log_get_record)(
        darshan_fd fd,
        void** buf
    );
    /* put a single module record into the log file.
     * return 0 on success, -1 on error
     *      - 'fd' is the file descriptor to put record into
     *      - 'buf' is the buffer containing the record data
     */
    int (*log_put_record)(
        darshan_fd fd,
        void *buf
    );
    /* print the counters for a given log record
     *      - 'rec' is the record's data buffer
     *      - 'name' is the name string associated with this record (or NULL if there isn't one)
     *      - 'mnt_pt' is the file path mount point string
     *      - 'fs_type' is the file system type string
     */
    void (*log_print_record)(
        void *rec,
        char *file_name,
        char *mnt_pt,
        char *fs_type
    );
    /* print module-specific description of I/O characterization data
     *      - 'ver' is the version of the record
     */
    void (*log_print_description)(
        int ver);
    /* print a text diff of 2 module records */
    void (*log_print_diff)(
        void *rec1,
        char *name1,
        void *rec2,
        char *name2
    );
    /* combine two records into a single aggregate record */
    void (*log_agg_records)(
        void *rec,
        void *agg_rec,
        int init_flag
    );
};

extern struct darshan_mod_logutil_funcs *mod_logutils[];

#include "darshan-posix-logutils.h"
#include "darshan-mpiio-logutils.h"
#include "darshan-hdf5-logutils.h"
#include "darshan-pnetcdf-logutils.h"
#include "darshan-bgq-logutils.h"
#include "darshan-lustre-logutils.h"
#include "darshan-stdio-logutils.h"

/* DXT */
#include "darshan-dxt-logutils.h"
#include "darshan-mdhim-logutils.h"

#ifdef DARSHAN_USE_APXC
#include "darshan-apxc-logutils.h"
#endif
#ifdef DARSHAN_USE_APMPI
#include "darshan-apmpi-logutils.h"
#endif

darshan_fd darshan_log_open(const char *name);
darshan_fd darshan_log_create(const char *name, enum darshan_comp_type comp_type,
    int partial_flag);
int darshan_log_get_job(darshan_fd fd, struct darshan_job *job);
int darshan_log_put_job(darshan_fd fd, struct darshan_job *job);
int darshan_log_get_exe(darshan_fd fd, char *buf);
int darshan_log_put_exe(darshan_fd fd, char *buf);
int darshan_log_get_mounts(darshan_fd fd, struct darshan_mnt_info **mnt_data_array,
    int* count);
int darshan_log_put_mounts(darshan_fd fd, struct darshan_mnt_info *mnt_data_array,
    int count);
int darshan_log_get_namehash(darshan_fd fd, struct darshan_name_record_ref **hash);
int darshan_log_put_namehash(darshan_fd fd, struct darshan_name_record_ref *hash);
int darshan_log_get_mod(darshan_fd fd, darshan_module_id mod_id,
    void *mod_buf, int mod_buf_sz);
int darshan_log_put_mod(darshan_fd fd, darshan_module_id mod_id,
    void *mod_buf, int mod_buf_sz, int ver);
void darshan_log_close(darshan_fd file);
void darshan_log_print_version_warnings(const char *version_string);
char *darshan_log_get_lib_version(void);
void darshan_log_get_modules (darshan_fd fd, struct darshan_mod_info **mods, int* count);
void darshan_log_get_name_records(darshan_fd fd,
                              struct darshan_name_record_info **mods,
                              int* count);
int darshan_log_get_record (darshan_fd fd, int mod_idx, void **buf);

void darshan_log_get_filtered_name_records(darshan_fd fd,
                              struct darshan_name_record_info **mods,
                              int* count,
                              darshan_record_id *whitelist, int whitelist_count
                              );


/* convenience macros for printing Darshan counters */
#define DARSHAN_PRINT_HEADER() \
    printf("\n#<module>\t<rank>\t<record id>\t<counter>\t<value>" \
           "\t<file name>\t<mount pt>\t<fs type>\n")

#define DARSHAN_D_COUNTER_PRINT(__mod_name, __rank, __file_id, \
                              __counter, __counter_val, __file_name, \
                              __mnt_pt, __fs_type) do { \
    printf("%s\t%" PRId64 "\t%" PRIu64 "\t%s\t%" PRId64 "\t%s\t%s\t%s\n", \
        __mod_name, __rank, __file_id, __counter, __counter_val, \
        __file_name, __mnt_pt, __fs_type); \
} while(0)

#define DARSHAN_U_COUNTER_PRINT(__mod_name, __rank, __file_id, \
                              __counter, __counter_val, __file_name, \
                              __mnt_pt, __fs_type) do { \
    printf("%s\t%" PRId64 "\t%" PRIu64 "\t%s\t%" PRIu64 "\t%s\t%s\t%s\n", \
        __mod_name, __rank, __file_id, __counter, __counter_val, \
        __file_name, __mnt_pt, __fs_type); \
} while(0)

#define DARSHAN_I_COUNTER_PRINT(__mod_name, __rank, __file_id, \
                              __counter, __counter_val, __file_name, \
                              __mnt_pt, __fs_type) do { \
    printf("%s\t%" PRId64 "\t%" PRIu64 "\t%s\t%d\t%s\t%s\t%s\n", \
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

#define DARSHAN_S_COUNTER_PRINT(__mod_name, __rank, __file_id, \
                              __counter, __counter_val, __file_name, \
                              __mnt_pt, __fs_type) do { \
    printf("%s\t%" PRId64 "\t%" PRIu64 "\t%s\t%s\t%s\t%s\t%s\n", \
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
