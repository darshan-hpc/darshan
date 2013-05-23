/*
 *  (C) 2009 by Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
 */

#include "darshan-runtime-config.h"
#include <stdio.h>
#include <pthread.h>
#include <string.h>
#include "mpi.h"
#include "darshan.h"

/* hope this doesn't change any time soon */
typedef int hid_t; 
typedef int herr_t;

#ifndef DARSHAN_PRELOAD

hid_t __real_H5Fcreate(const char *filename, unsigned flags, hid_t create_plist, hid_t access_plist)
{
    return -1;
}

hid_t __real_H5Fopen(const char *filename, unsigned flags, hid_t access_plist)
{
    return -1;
}

herr_t __real_H5Fclose(hid_t file_id)
{
    return -1;
}

#endif

/*
 * Local variables:
 *  c-indent-level: 4
 *  c-basic-offset: 4
 * End:
 *
 * vim: ts=8 sts=4 sw=4 expandtab
 */
