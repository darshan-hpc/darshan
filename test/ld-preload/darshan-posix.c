/*
 *  (C) 2009 by Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
 */

#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdarg.h>
#include <string.h>
#include <time.h>
#include <stdlib.h>
#include <errno.h>
#include <sys/uio.h>
#include <pthread.h>
#include <sys/mman.h>
#include <search.h>
#include <assert.h>


extern int __real_open(const char *path, int flags, ...);
extern int __real_open64(const char *path, int flags, ...);

int __wrap_open64(const char* path, int flags, ...)
{
    int mode = 0;
    int ret;

    printf("Hello world, I hijacked open64()!\n");

    if (flags & O_CREAT) 
    {
        va_list arg;
        va_start(arg, flags);
        mode = va_arg(arg, int);
        va_end(arg);

        ret = __real_open64(path, flags, mode);
    }
    else
    {
        ret = __real_open64(path, flags);
    }

    return(ret);
}

int __wrap_open(const char *path, int flags, ...)
{
    int mode = 0;
    int ret;

    printf("Hello world, I hijacked open()!\n");

    if (flags & O_CREAT) 
    {
        va_list arg;
        va_start(arg, flags);
        mode = va_arg(arg, int);
        va_end(arg);

        ret = __real_open(path, flags, mode);
    }
    else
    {
        ret = __real_open(path, flags);
    }

    return(ret);
}

/*
 * Local variables:
 *  c-indent-level: 4
 *  c-basic-offset: 4
 * End:
 *
 * vim: ts=8 sts=4 sw=4 expandtab
 */
