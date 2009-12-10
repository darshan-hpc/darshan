/*
 *  (C) 2009 by Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
 */

#ifndef __DARSHAN_LOG_UTILS_H
#define __DARSHAN_LOG_UTILS_H
#include <darshan-log-format.h>
#include <zlib.h>
typedef gzFile darshan_fd;

extern char *darshan_names[];
extern char *darshan_f_names[];

darshan_fd darshan_log_open(const char *name);
int darshan_log_getjob(darshan_fd file, struct darshan_job *job);
int darshan_log_getfile(darshan_fd fd, 
    struct darshan_job* job, 
    struct darshan_file *file);
int darshan_log_getexe(darshan_fd fd, char *buf, int *flag);
void darshan_log_close(darshan_fd file);
void darshan_log_print_version_warnings(struct darshan_job *job);

/* convenience macros for printing out counters */
#define CP_PRINT_HEADER() printf("#<rank>\t<file>\t<counter>\t<value>\t<name suffix>\n")
#define CP_PRINT(__job, __file, __counter) do {\
        printf("%d\t%llu\t%s\t%lld\t...%s\n", \
            (__file)->rank, llu((__file)->hash), darshan_names[__counter], \
            lld((__file)->counters[__counter]), (__file)->name_suffix); \
} while(0)
#define CP_F_PRINT(__job, __file, __counter) do {\
        printf("%d\t%llu\t%s\t%f\t...%s\n", \
            (__file)->rank, llu((__file)->hash), darshan_f_names[__counter], \
            (__file)->fcounters[__counter], (__file)->name_suffix); \
} while(0)

#endif
