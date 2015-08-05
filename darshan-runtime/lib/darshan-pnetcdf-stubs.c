/*
 * Copyright (C) 2015 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

/* This file contains stubs for the ncmpi functions intercepted by Darshan.
 * They are defined as weak symbols in order to satisfy dependencies of the
 * pnetcdf wrappers in cases where pnetcdf is not being used.
 */

#include "darshan-runtime-config.h"
#include <stdio.h>
#include <pthread.h>
#include <string.h>
#include "mpi.h"
#include "darshan.h"

int ncmpi_create(MPI_Comm comm, const char *path, 
    int cmode, MPI_Info info, int *ncidp) __attribute__((weak));

int ncmpi_create(MPI_Comm comm, const char *path, 
    int cmode, MPI_Info info, int *ncidp)
{
    int rank;

    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    if(rank == 0)
    {
        fprintf(stderr, "WARNING: Darshan ncmpi_create() stub called; this is probably the result of a link-time problem.\n");
    }

    return(-1);
}

int ncmpi_open(MPI_Comm comm, const char *path, 
    int omode, MPI_Info info, int *ncidp) __attribute__((weak));

int ncmpi_open(MPI_Comm comm, const char *path, 
    int omode, MPI_Info info, int *ncidp)
{
    int rank;

    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    if(rank == 0)
    {
        fprintf(stderr, "WARNING: Darshan ncmpi_open() stub called; this is probably the result of a link-time problem.\n");
    }

    return(-1);
}

int ncmpi_close(int ncid) __attribute__((weak));

int ncmpi_close(int ncid)
{
    int rank;

    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    if(rank == 0)
    {
        fprintf(stderr, "WARNING: Darshan ncmpi_close() stub called; this is probably the result of a link-time problem.\n");
    }

    return(-1);

}


/*
 * Local variables:
 *  c-indent-level: 4
 *  c-basic-offset: 4
 * End:
 *
 * vim: ts=8 sts=4 sw=4 expandtab
 */
