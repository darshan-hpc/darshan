/*
 * Copyright (C) 2015 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

#ifndef __DARSHAN_CORE_H
#define __DARSHAN_CORE_H

#include <unistd.h>
#include <sys/types.h>
#include <stdint.h>
#include <limits.h>

#ifdef HAVE_MPI
#include <mpi.h>
#endif

#include "uthash.h"
#include "darshan.h"
#include "darshan-log-format.h"

/* Environment variable to override __DARSHAN_JOBID */
#define DARSHAN_JOBID_OVERRIDE "DARSHAN_JOBID"

/* Environment variable to override __DARSHAN_LOG_PATH */
#define DARSHAN_LOG_PATH_OVERRIDE "DARSHAN_LOGPATH"

/* Environment variable to override __DARSHAN_LOG_HINTS */
#define DARSHAN_LOG_HINTS_OVERRIDE "DARSHAN_LOGHINTS"

/* Environment variable to override __DARSHAN_MEM_ALIGNMENT */
#define DARSHAN_MEM_ALIGNMENT_OVERRIDE "DARSHAN_MEMALIGN"

/* Environment variable to override memory per module */
#define DARSHAN_MOD_MEM_OVERRIDE "DARSHAN_MODMEM"

/* Environment variable to enable profiling without MPI */
#define DARSHAN_ENABLE_NONMPI "DARSHAN_ENABLE_NONMPI" 

#ifdef __DARSHAN_ENABLE_MMAP_LOGS
/* Environment variable to override default mmap log path */
#define DARSHAN_MMAP_LOG_PATH_OVERRIDE "DARSHAN_MMAP_LOGPATH"

/* default path for storing mmap log files is '/tmp' */
#define DARSHAN_DEF_MMAP_LOG_PATH "/tmp"
#endif

/* Maximum runtime memory consumption per process (in MiB) across
 * all instrumentation modules
 */
#ifdef __DARSHAN_MOD_MEM_MAX
#define DARSHAN_MOD_MEM_MAX (__DARSHAN_MOD_MEM_MAX * 1024L * 1024L)
#else
#define DARSHAN_MOD_MEM_MAX (4 * 1024 * 1024) /* 4 MiB default */
#endif

/* default name record buf can store 2048 records of size 100 bytes */
#define DARSHAN_NAME_RECORD_BUF_SIZE (2048 * 100)

typedef union
{
    int nompi_fd;
#ifdef HAVE_MPI
    MPI_File mpi_fh;
#endif
} darshan_core_log_fh;

/* FS mount information */
#define DARSHAN_MAX_MNTS 64
#define DARSHAN_MAX_MNT_PATH 256
#define DARSHAN_MAX_MNT_TYPE 32
struct darshan_core_mnt_data
{
    char path[DARSHAN_MAX_MNT_PATH];
    char type[DARSHAN_MAX_MNT_TYPE];
    struct darshan_fs_info fs_info;
};

/* structure to track registered modules */
struct darshan_core_module
{
    void *rec_buf_start;
    void *rec_buf_p;
    size_t rec_mem_avail;
    darshan_module_funcs mod_funcs;
};

/* strucutre for keeping a reference to registered name records */
struct darshan_core_name_record_ref
{
    struct darshan_name_record *name_record;
    uint64_t mod_flags;
    uint64_t global_mod_flags;
    UT_hash_handle hlink;
};

/* in memory structure to keep up with job level data */
struct darshan_core_runtime
{
    /* pointers to each log file component */
    struct darshan_header *log_hdr_p;
    struct darshan_job *log_job_p;
    char *log_exemnt_p;
    void *log_name_p;
    void *log_mod_p;

    /* darshan-core internal data structures */
    struct darshan_core_module* mod_array[DARSHAN_MAX_MODS];
    size_t mod_mem_used;
    struct darshan_core_name_record_ref *name_hash;
    size_t name_mem_used;
    double wtime_offset;
    char *comp_buf;
#ifdef __DARSHAN_ENABLE_MMAP_LOGS
    char mmap_log_name[PATH_MAX];
#endif
#ifdef HAVE_MPI
    MPI_Comm mpi_comm;
#endif
    int pid;
};

void darshan_core_initialize(int argc, char **argv);
void darshan_core_shutdown(int write_log);

uint32_t darshan_hashlittle(const void *key, size_t length, uint32_t initval);
uint64_t darshan_hash(const register unsigned char *k, register uint64_t length, register uint64_t level);

#endif /* __DARSHAN_CORE_H */
