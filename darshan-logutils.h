#ifndef __DARSHAN_LOG_UTILS_H
#define __DARSHAN_LOG_UTILS_H
#include <darshan-log-format.h>
#include <zlib.h>
typedef gzFile darshan_fd;

extern char *darshan_names[];
extern char *darshan_f_names[];

darshan_fd darshan_open(char *name);
int darshan_job_init(darshan_fd file, struct darshan_job *job);
int darshan_getfile(darshan_fd fd, struct darshan_file *file);
int darshan_getexe(darshan_fd fd, char *buf, int *flag);
void darshan_finalize(darshan_fd file);

#endif
