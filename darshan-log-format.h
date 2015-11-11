/*
 * Copyright (C) 2015 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

#ifndef __DARSHAN_LOG_FORMAT_H
#define __DARSHAN_LOG_FORMAT_H

#include <unistd.h>
#include <sys/types.h>
#include <stdint.h>
#include <inttypes.h>

#if !defined PRId64
#error failed to detect PRId64
#endif
#if !defined PRIu64
#error failed to detect PRIu64
#endif

/* update this on file format changes */
#define DARSHAN_LOG_VERSION "3.00"

/* magic number for validating output files and checking byte order */
#define DARSHAN_MAGIC_NR 6567223

/* size (in bytes) of job record */
#define DARSHAN_JOB_RECORD_SIZE 4096

/* max length of exe string within job record (not counting '\0') */
#define DARSHAN_EXE_LEN (DARSHAN_JOB_RECORD_SIZE - sizeof(struct darshan_job) - 1)

#define DARSHAN_MAX_MODS 16

/* X-macro for keeping module ordering consistent */
/* NOTE: first val used to define module enum values, 
 * second val used to define module name strings, and
 * third val is used to provide the name of a 
 * corresponding logutils structure for parsing module
 * data out of the log file (only used in darshan-util
 * component -- NULL can be passed if there are no
 * logutil definitions)
 */
#define DARSHAN_MODULE_IDS \
    X(DARSHAN_NULL_MOD, "NULL", NULL) \
    X(DARSHAN_POSIX_MOD, "POSIX", &posix_logutils) \
    X(DARSHAN_MPIIO_MOD, "MPI-IO", &mpiio_logutils) \
    X(DARSHAN_HDF5_MOD, "HDF5", &hdf5_logutils) \
    X(DARSHAN_PNETCDF_MOD, "PNETCDF", &pnetcdf_logutils) \
    X(DARSHAN_BGQ_MOD, "BG/Q", &bgq_logutils)

/* unique identifiers to distinguish between available darshan modules */
/* NOTES: - valid ids range from [0...DARSHAN_MAX_MODS-1]
 *        - order of ids control module shutdown order (and consequently, order in log file)
 */
#define X(a, b, c) a,
typedef enum
{
    DARSHAN_MODULE_IDS
} darshan_module_id;
#undef X

/* module name strings */
#define X(a, b, c) b,
static char * const darshan_module_names[] =
{
    DARSHAN_MODULE_IDS
};
#undef X

/* simple macros for accessing module flag bitfields */
#define DARSHAN_MOD_FLAG_SET(flags, id) flags = (flags | (1 << id))
#define DARSHAN_MOD_FLAG_UNSET(flags, id) flags = (flags & ~(1 << id))
#define DARSHAN_MOD_FLAG_ISSET(flags, id) (flags & (1 << id))

/* compression method used on darshan log file */
enum darshan_comp_type
{
    DARSHAN_ZLIB_COMP,
    DARSHAN_BZIP2_COMP,
};

typedef uint64_t darshan_record_id;

/* the darshan_log_map structure is used to indicate the location of
 * specific module data in a Darshan log. Note that 'off' and 'len' are
 * the respective offset and length of the data in the file, in
 * *compressed* terms
 */
struct darshan_log_map
{
    uint64_t off;
    uint64_t len;
};

/* the darshan header stores critical metadata needed for correctly
 * reading the contents of the corresponding Darshan log
 */
struct darshan_header
{
    char version_string[8];
    int64_t magic_nr;
    unsigned char comp_type;
    uint32_t partial_flag;
    struct darshan_log_map rec_map;
    struct darshan_log_map mod_map[DARSHAN_MAX_MODS];
};

/* job-level metadata stored for this application */
#define DARSHAN_JOB_METADATA_LEN 1024 /* including null terminator! */
struct darshan_job
{
    int64_t uid;
    int64_t start_time;
    int64_t end_time;
    int64_t nprocs;
    int64_t jobid;
    char metadata[DARSHAN_JOB_METADATA_LEN];
};

/* minimal record stored for each file/object accessed by Darshan */
struct darshan_record
{
    char* name;
    darshan_record_id id;
};

#endif /* __DARSHAN_LOG_FORMAT_H */
