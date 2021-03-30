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
/* NOTE: this should be updated when general changes are made to the
 * log format version, NOT when a new version of a module record is
 * introduced -- we have module-specific versions to handle that
 */
#define DARSHAN_LOG_VERSION "3.21"

/* magic number for validating output files and checking byte order */
#define DARSHAN_MAGIC_NR 6567223

/* size (in bytes) of job record */
#define DARSHAN_JOB_RECORD_SIZE 4096

/* max length of exe string within job record (not counting '\0') */
#define DARSHAN_EXE_LEN (DARSHAN_JOB_RECORD_SIZE - sizeof(struct darshan_job) - 1)

/* max number of modules that can be used in a darshan log */
#define DARSHAN_MAX_MODS 16

/* simple macros for accessing module flag bitfields */
#define DARSHAN_MOD_FLAG_SET(flags, id) flags = (flags | (1 << id))
#define DARSHAN_MOD_FLAG_UNSET(flags, id) flags = (flags & ~(1 << id))
#define DARSHAN_MOD_FLAG_ISSET(flags, id) (flags & (1 << id))

/* compression method used on darshan log file */
enum darshan_comp_type
{
    DARSHAN_ZLIB_COMP,
    DARSHAN_BZIP2_COMP,
    DARSHAN_NO_COMP, 
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
    struct darshan_log_map name_map;
    struct darshan_log_map mod_map[DARSHAN_MAX_MODS];
    uint32_t mod_ver[DARSHAN_MAX_MODS];
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

/* record to store name->darshan_id mapping for each registered record */
struct darshan_name_record
{
    darshan_record_id id;
    char name[1];
};

/* base record definition that can be used by modules */
struct darshan_base_record
{
    darshan_record_id id;
    int64_t rank;
};


/************************************************
 *** module-specific includes and definitions ***
 ************************************************/

#include "darshan-null-log-format.h"
#include "darshan-posix-log-format.h"
#include "darshan-mpiio-log-format.h"
#include "darshan-hdf5-log-format.h"
#include "darshan-pnetcdf-log-format.h"
#include "darshan-bgq-log-format.h"
#include "darshan-lustre-log-format.h"
#include "darshan-stdio-log-format.h"
/* DXT */
#include "darshan-dxt-log-format.h"
#include "darshan-mdhim-log-format.h"
#ifdef DARSHAN_USE_APXC
#include "darshan-apxc-log-format.h"
#endif
#ifdef DARSHAN_USE_APMPI
#include "darshan-apmpi-log-format.h"
#endif

/* X-macro for keeping module ordering consistent */
/* NOTE: first val used to define module enum values, 
 * second val used to define module name strings,
 * third val is the log format version for the module,
 * and fourth val is used to provide the name of a 
 * corresponding logutils structure for parsing module
 * data out of the log file (only used in darshan-util
 * component -- NULL can be passed if there are no
 * logutil definitions)
 */

/* NOTE: if APXC support is not enabled, we still want to hold it's spot
 * in the module id space
 */
#ifdef DARSHAN_USE_APXC
#define __APXC_VER APXC_VER
#define __apxc_logutils &apxc_logutils
#else
#define __APXC_VER 0
#define __apxc_logutils NULL
#endif
#ifdef DARSHAN_USE_APMPI
#define __APMPI_VER APMPI_VER
#define __apmpi_logutils &apmpi_logutils
#else
#define __APMPI_VER 0
#define __apmpi_logutils NULL
#endif

#define DARSHAN_MODULE_IDS \
    X(DARSHAN_NULL_MOD,     "NULL",       DARSHAN_NULL_VER,      NULL) \
    X(DARSHAN_POSIX_MOD,    "POSIX",      DARSHAN_POSIX_VER,     &posix_logutils) \
    X(DARSHAN_MPIIO_MOD,    "MPI-IO",     DARSHAN_MPIIO_VER,     &mpiio_logutils) \
    X(DARSHAN_H5F_MOD,      "H5F",        DARSHAN_H5F_VER,       &hdf5_file_logutils) \
    X(DARSHAN_H5D_MOD,      "H5D",        DARSHAN_H5D_VER,       &hdf5_dataset_logutils) \
    X(DARSHAN_PNETCDF_MOD,  "PNETCDF",    DARSHAN_PNETCDF_VER,   &pnetcdf_logutils) \
    X(DARSHAN_BGQ_MOD,      "BG/Q",       DARSHAN_BGQ_VER,       &bgq_logutils) \
    X(DARSHAN_LUSTRE_MOD,   "LUSTRE",     DARSHAN_LUSTRE_VER,    &lustre_logutils) \
    X(DARSHAN_STDIO_MOD,    "STDIO",      DARSHAN_STDIO_VER,     &stdio_logutils) \
    X(DXT_POSIX_MOD,        "DXT_POSIX",  DXT_POSIX_VER,         &dxt_posix_logutils) \
    X(DXT_MPIIO_MOD,        "DXT_MPIIO",  DXT_MPIIO_VER,         &dxt_mpiio_logutils) \
    X(DARSHAN_MDHIM_MOD,    "MDHIM",      DARSHAN_MDHIM_VER,     &mdhim_logutils) \
    X(DARSHAN_APXC_MOD,     "APXC", 	  __APXC_VER,            __apxc_logutils) \
    X(DARSHAN_APMPI_MOD,    "APMPI",      __APMPI_VER,           __apmpi_logutils) 

/* unique identifiers to distinguish between available darshan modules */
/* NOTES: - valid ids range from [0...DARSHAN_MAX_MODS-1]
 *        - order of ids control module shutdown order (and consequently, order in log file)
 */
#define X(a, b, c, d) a,
typedef enum
{
    DARSHAN_MODULE_IDS
} darshan_module_id;
#undef X

/* module name strings */
#define X(a, b, c, d) b,
static const char * const darshan_module_names[] =
{
    DARSHAN_MODULE_IDS
};
#undef X

/* module version numbers */
#define X(a, b, c, d) c,
static const int darshan_module_versions[] =
{
    DARSHAN_MODULE_IDS
};
#undef X

#endif /* __DARSHAN_LOG_FORMAT_H */
