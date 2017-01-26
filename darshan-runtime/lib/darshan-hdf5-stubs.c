/*
 * Copyright (C) 2015 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

/* This file contains stubs for the H5F functions intercepted by Darshan.
 * They are defined as weak symbols in order to satisfy dependencies of the
 * hdf5 wrappers in cases where hdf5 is not being used.
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

hid_t H5Fcreate(const char *filename, unsigned flags,
    hid_t create_plist, hid_t access_plist) __attribute__((weak));

hid_t H5Fcreate(const char *filename, unsigned flags,
    hid_t create_plist, hid_t access_plist)
{
    int rank;

    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    if(rank == 0)
    {
        fprintf(stderr, "WARNING: Darshan H5Fcreate() stub called; this is probably the result of a link-time problem.\n");
    }

    return(-1);
}

hid_t H5Fopen(const char *filename, unsigned flags,
    hid_t access_plist) __attribute__((weak));

hid_t H5Fopen(const char *filename, unsigned flags,
    hid_t access_plist)
{
    int rank;

    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    if(rank == 0)
    {
        fprintf(stderr, "WARNING: Darshan H5Fopen() stub called; this is probably the result of a link-time problem.\n");
    }

    return(-1);
}

herr_t H5Fclose(hid_t file_id) __attribute__((weak));

herr_t H5Fclose(hid_t file_id)
{
    int rank;

    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    if(rank == 0)
    {
        fprintf(stderr, "WARNING: Darshan H5Fclose() stub called; this is probably the result of a link-time problem.\n");
    }

    return(-1);
}

herr_t H5get_libversion(unsigned *majnum, unsigned *minnum, unsigned *relnum) __attribute__((weak));

herr_t H5get_libversion(unsigned *majnum, unsigned *minnum, unsigned *relnum)
{
    int rank;

    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    if(rank == 0)
    {
        fprintf(stderr, "WARNING: Darshan H5get_libversion() stub called; this is probably the result of a link-time problem.\n");
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
