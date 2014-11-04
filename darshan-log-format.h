/*
 *  (C) 2009 by Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
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
#define CP_VERSION "2.05"

/* magic number for validating output files and checking byte order */
#define CP_MAGIC_NR 6567223

/* size (in bytes) of job record */
#define CP_JOB_RECORD_SIZE 4096

/* max length of exe string within job record (not counting '\0') */
#define CP_EXE_LEN (CP_JOB_RECORD_SIZE - sizeof(struct darshan_job) - 1)

/* statistics for the job as a whole */
#define DARSHAN_JOB_METADATA_LEN 1024 /* including null terminator! */
struct darshan_job
{
    char version_string[8];
    int64_t magic_nr;
    int64_t uid;
    int64_t start_time;
    int64_t end_time;
    int64_t nprocs;
    int64_t jobid;
    char metadata[DARSHAN_JOB_METADATA_LEN];
};

#endif /* __DARSHAN_LOG_FORMAT_H */
