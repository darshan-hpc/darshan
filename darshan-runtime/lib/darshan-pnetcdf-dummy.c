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

#ifndef DARSHAN_PRELOAD

int __real_ncmpi_create(MPI_Comm comm, const char *path, int cmode, MPI_Info info, int *ncidp)
{
    return -1;
}

int __real_ncmpi_open(MPI_Comm comm, const char *path, int omode, MPI_Info info, int *ncidp)
{
    return -1;
}

int __real_ncmpi_close(int ncid)
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
