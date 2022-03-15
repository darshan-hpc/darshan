/*
 * Copyright (C) 2015 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

#ifndef __DARSHAN_H
#define __DARSHAN_H

#include "darshan-runtime-config.h"

#include <unistd.h>
#include <sys/types.h>
#include <stdint.h>
#include <time.h>
#include <pthread.h>
#include <stdio.h>
#include <regex.h>

#ifdef HAVE_MPI
#include <mpi.h>
#endif
#ifdef HAVE_STDATOMIC_H
#include <stdatomic.h>
#endif
#ifdef HAVE_X86INTRIN_H
    #include <x86intrin.h>
#endif

#include "uthash.h"
#include "darshan-log-format.h"
#include "darshan-common.h"

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

/* Environment variable to override memory for name records */
#define DARSHAN_NAME_MEM_OVERRIDE "DARSHAN_NAMEMEM"

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
/* 4 MiB default */
#define DARSHAN_MOD_MEM_MAX (4 * 1024 * 1024)
#endif

/* Maximum runtime memory consumption per process for name records */
/* 200 KiB default */
#define DARSHAN_NAME_MEM_MAX (1024 * 200)

/* maximum buffer size for full paths, for internal use only */
#define __DARSHAN_PATH_MAX 4096

typedef union
{
    int nompi_fd;
#ifdef HAVE_MPI
    MPI_File mpi_fh;
#endif
} darshan_core_log_fh;

/* stores FS info from statfs calls for a given mount point */
struct darshan_fs_info
{
    int fs_type;
    int block_size;
    int ost_count;
    int mdt_count;
};

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

/* structure for keeping a reference to registered name records */
struct darshan_core_name_record_ref
{
    struct darshan_name_record *name_record;
    uint64_t mod_flags;
    uint64_t global_mod_flags;
    UT_hash_handle hlink;
};

/* structure for keeping track of record name inclusions/exclusions */
struct darshan_core_name_regex
{
    regex_t regex;
    uint64_t mod_flags;
    struct darshan_core_name_regex *next;
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
    uint64_t mod_disabled;
    size_t mod_max_records_override[DARSHAN_MAX_MODS];
    struct darshan_core_name_regex *app_exclusion_list;
    struct darshan_core_name_regex *app_inclusion_list;
    struct darshan_core_name_regex *rec_exclusion_list;
    struct darshan_core_name_regex *rec_inclusion_list;
    size_t mod_mem_used;
    struct darshan_core_name_record_ref *name_hash;
    size_t name_mem_used;
    char *comp_buf;
#ifdef __DARSHAN_ENABLE_MMAP_LOGS
    char mmap_log_name[__DARSHAN_PATH_MAX];
#endif
#ifdef HAVE_MPI
    MPI_Comm mpi_comm;
#endif
    int pid;
};

/* core constructs for use in macros and inline functions; the __ prefix
 * denotes that these should not be accessed directly by Darshan modules or
 * other Darshan library components.
 */
extern struct darshan_core_runtime *__darshan_core;
extern double __darshan_core_wtime_offset;
#ifdef HAVE_STDATOMIC_H
extern atomic_flag __darshan_core_mutex;
#define __DARSHAN_CORE_LOCK() \
    while (atomic_flag_test_and_set(&__darshan_core_mutex))
#define __DARSHAN_CORE_UNLOCK() \
    atomic_flag_clear(&__darshan_core_mutex)
#else
extern pthread_mutex_t __darshan_core_mutex;
#define __DARSHAN_CORE_LOCK() pthread_mutex_lock(&__darshan_core_mutex)
#define __DARSHAN_CORE_UNLOCK() pthread_mutex_unlock(&__darshan_core_mutex)
#endif

/* macros for declaring wrapper functions and calling MPI routines
 * consistently regardless of whether static or dynamic linking is used
 */
#ifdef DARSHAN_PRELOAD

#include <dlfcn.h>
#include <stdlib.h>

#define DARSHAN_FORWARD_DECL(__func,__ret,__args) \
  __ret (*__real_ ## __func)__args = NULL

#define DARSHAN_DECL(__func) __func

/* creates P* variant of MPI symbols for LD_PRELOAD so that we can handle
 * language bindings that map to MPI or PMPI symbols under the covers.
 *
 * We use an alias attribute rather than generating a function shim in order
 * to prevent accidental function call loop if there a conventional PMPI
 * profiler is attempting to intercept the same function name.
  */
#define DARSHAN_WRAPPER_MAP(__func,__ret,__args,__fcall) \
    __ret __func __args __attribute__ ((alias (#__fcall)));

/* Map the desired function call to a pointer called __real_NAME at run
 * time.  Note that we fall back to looking for the same symbol with a P
 * prefix to handle MPI bindings that call directly to the PMPI layer.
 */
#define MAP_OR_FAIL(__func) \
    if (!(__real_ ## __func)) \
    { \
        __real_ ## __func = dlsym(RTLD_NEXT, #__func); \
        if(!(__real_ ## __func)) { \
            darshan_core_fprintf(stderr, "Darshan failed to map symbol: %s\n", #__func); \
            exit(1); \
       } \
    } \
    int __darshan_disabled = darshan_core_disabled_instrumentation();
#else

#define DARSHAN_FORWARD_DECL(__name,__ret,__args) \
  extern __ret __real_ ## __name __args;

#define DARSHAN_DECL(__name) __wrap_ ## __name

/* creates P* variant of MPI symbols for static linking so that we can handle
 * language bindings that map to MPI or PMPI symbols under the covers.
 *
 * We use an alias attribute rather than generating a function shim in order
 * to prevent accidental function call loop if there a conventional PMPI
 * profiler is attempting to intercept the same function name.
 */
#define DARSHAN_WRAPPER_MAP(__func,__ret,__args,__fcall) \
    __ret __wrap_ ## __func __args __attribute__ ((alias ("__wrap_" #__fcall)));

#define MAP_OR_FAIL(__func) \
    int __darshan_disabled = darshan_core_disabled_instrumentation()

#endif

/* default number of records to attempt to store for each module */
#define DARSHAN_DEF_MOD_REC_COUNT 1024

#ifdef HAVE_MPI
/*
 * module developers _may_ define a 'darshan_module_redux' function
 * to run collective MPI operations at shutdown time. Typically this
 * functionality has been used to reduce records shared globablly (given
 * in the 'shared_recs' array) into a single data record. Set to NULL
 * avoid any reduction steps.
 */
typedef void (*darshan_module_redux)(
    void *mod_buf, /* input parameter indicating module's buffer address */
    MPI_Comm mod_comm,  /* MPI communicator to run collectives with */
    darshan_record_id *shared_recs, /* list of shared data record ids */
    int shared_rec_count /* count of shared data records */
);
#endif
/*
 * module developers _must_ define a 'darshan_module_output' function
 * for allowing darshan-core to call into a module and retrieve final
 * output data to be saved in the log.
 */
typedef void (*darshan_module_output)(
    void **mod_buf, /* output parameter to save module buffer address */
    int *mod_buf_sz /* output parameter to save module buffer size */
);
/*
 * module developers _must_ define a 'darshan_module_cleanup' function
 * for allowing darshan-core to call into a module and cleanup its
 * runtime state (i.e., drop tracked file records and free all memory).
 */
typedef void (*darshan_module_cleanup)(void);
typedef struct darshan_module_funcs
{
#ifdef HAVE_MPI
    darshan_module_redux mod_redux_func;
#endif
    darshan_module_output mod_output_func;
    darshan_module_cleanup mod_cleanup_func;
} darshan_module_funcs;

/* structure to track registered modules */
struct darshan_core_module
{
    void *rec_buf_start;
    void *rec_buf_p;
    size_t rec_mem_avail;
    darshan_module_funcs mod_funcs;
};

/* darshan_core_register_module()
 *
 * Register module identifier 'mod_id' with the darshan-core runtime
 * environment, allowing the module to store I/O characterization data.
 * 'mod_funcs' is a set of function pointers that implement module-specific
 * shutdown functionality (including a possible data reduction step when
 * using MPI). 'rec_size' is the default size of records generated by this
 * module. 'inout_rec_count' is an input/output argument, with it being set
 * to the requested number of module records to reserve on input, and set to
 * the total amount reserved by darshan-core on output. If Darshan is built
 * with MPI support, 'rank' is a pointer to an integer which will contain the
 * calling process's MPI rank on return. If given, 'sys_mem_alignment' is a
 * pointer to an integer which will contain the memory alignment value Darshan
 * was configured with on return.
 *
 * Returns 0 on success, -1 on failure.
 */
int darshan_core_register_module(
    darshan_module_id mod_id,
    darshan_module_funcs mod_funcs,
    size_t rec_size,
    size_t *inout_rec_count,
    int *rank,
    int *sys_mem_alignment);

/* darshan_core_unregister_module()
 *
 * Unregisters module identifier 'mod_id' with the darshan-core runtime,
 * removing the given module from the resulting I/O characterization log.
 */
void darshan_core_unregister_module(
    darshan_module_id mod_id);

/* darshan_instrument_fs_data()
 *
 * Allow file system-specific modules to instrument data for the file
 * stored at 'path'. 'fs_type' is checked to determine the underlying
 * filesystem and calls into the corresponding file system instrumentation
 * module, if defined -- currently we only have a Lustre module. 'fd' is
 * the file descriptor corresponding to the file, which may be needed by
 * the file system to retrieve specific parameters.
 */
void darshan_instrument_fs_data(
    int fs_type,
    const char *path,
    int fd);

/* darshan_core_gen_record_id()
 *
 * Returns the Darshan record ID correpsonding to input string 'name'.
 */
darshan_record_id darshan_core_gen_record_id(
    const char *name);

/* darshan_core_register_record()
 *
 * Register a record with the darshan-core runtime, allowing it to be
 * properly tracked and (potentially) correlated with records from other
 * modules. 'rec_id' is the Darshan record id as given by the
 * `darshan_core_gen_record_id` function. 'name' is the the name of the
 * Darshan record (e.g., the full file path), which is ignored if NULL is
 * passed. 'mod_id' is the identifier of the calling module. 'rec_size'
 * is the size of the record being registered with Darshan. If given,
 * 'fs_info' is a pointer to a structure containing information on
 * the underlying FS this record is associated with (determined by
 * matching the file name prefix with Darshan's list of tracked mount
 * points). Returns a pointer to the address the record should be
 * written to on success, NULL on failure.
 */
void *darshan_core_register_record(
    darshan_record_id rec_id,
    const char *name,
    darshan_module_id mod_id,
    size_t rec_size,
    struct darshan_fs_info *fs_info);


/* darshan_core_lookup_record_name()
 *
 * Looks up the name associated with a given Darshan record ID.
 */
char *darshan_core_lookup_record_name(
    darshan_record_id rec_id);

/* darshan_core_disabled_instrumentation
 *
 * Returns true (1) if Darshan has currently disabled instrumentation,
 * false (0) otherwise. If instrumentation is disabled, modules should
 * no longer update any file records as part of the intercepted function
 * wrappers.
 */
static inline int darshan_core_disabled_instrumentation(void)
{
    int ret;

    __DARSHAN_CORE_LOCK();
    if(__darshan_core)
        ret = 0;
    else
        ret = 1;
    __DARSHAN_CORE_UNLOCK();

    return(ret);
}

/* retrieve absolute wtime */
static inline double darshan_core_wtime_absolute(void)
{
#ifdef __DARSHAN_RDTSCP_FREQUENCY
    /* user configured darshan-runtime explicitly to use rtdscp for timing */
    unsigned flag;
    unsigned long long ts;

    ts = __rdtscp(&flag);
    return((double)ts/(double)__DARSHAN_RDTSCP_FREQUENCY);
#else
    /* normal path */
    struct timespec tp;
    /* some notes on what function to use to retrieve time as of 2021-05:
     * - clock_gettime() is faster than MPI_Wtime() across platforms
     * - clock_gettime() is at least competitive with gettimeofday()
     * - on some platforms, the _COARSE variants may provide sufficient
     *   resolution with a slight performance improvement, but there are a
     *   couple of problems:
     *   - the _COARSE variants are not implemented with vDSO on POWER
     *     platforms
     *   - it is not well defined how much precision will be sacrificed
     */
    clock_gettime(CLOCK_REALTIME, &tp);
    return(((double)tp.tv_sec) + 1.0e-9 * ((double)tp.tv_nsec));
#endif
}

/* darshan_core_wtime()
 *
 * Returns the elapsed time relative to (roughly) the start of
 * the application.
 */
static inline double darshan_core_wtime(void)
{
    return(darshan_core_wtime_absolute() - __darshan_core_wtime_offset);
}

/* darshan_core_fprintf()
 *
 * Prints internal Darshan output on a given stream.
 */
void darshan_core_fprintf(
    FILE *stream,
    const char *format,
    ...);

/* darshan_core_excluded_path()
 *
 * Returns true (1) if the given file path 'path' is in Darshan's
 * list of excluded file paths, false (0) otherwise.
 */
int darshan_core_excluded_path(
    const char * path);

void darshan_core_initialize(int argc, char **argv);
void darshan_core_shutdown(int write_log);

uint32_t darshan_hashlittle(const void *key, size_t length, uint32_t initval);
uint64_t darshan_hash(const register unsigned char *k, register uint64_t length, register uint64_t level);

#endif /* __DARSHAN_H */
