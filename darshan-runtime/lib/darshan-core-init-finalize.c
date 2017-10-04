/*
 * Copyright (C) 2015 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

#define _XOPEN_SOURCE 500
#define _GNU_SOURCE

#include "darshan-runtime-config.h"

#include <stdio.h>
#include <stdlib.h>
#include <mpi.h>

#include "darshan.h"
#include "darshan-core.h"
#include "darshan-dynamic.h"

DARSHAN_FORWARD_DECL(PMPI_Finalize, int, ());
DARSHAN_FORWARD_DECL(PMPI_Init, int, (int *argc, char ***argv));
DARSHAN_FORWARD_DECL(PMPI_Init_thread, int, (int *argc, char ***argv, int required, int *provided));

int DARSHAN_DECL(MPI_Init)(int *argc, char ***argv)
{
    int ret;

    MAP_OR_FAIL(PMPI_Init);

    ret = __real_PMPI_Init(argc, argv);
    if(ret != MPI_SUCCESS)
    {
        return(ret);
    }

    if(argc && argv)
    {
        darshan_core_initialize(*argc, *argv);
    }
    else
    {
        /* we don't see argc and argv here in fortran */
        darshan_core_initialize(0, NULL);
    }

    return(ret);
}
DARSHAN_WRAPPER_MAP(PMPI_Init, int, (int *argc, char ***argv), MPI_Init(argc,argv))

int DARSHAN_DECL(MPI_Init_thread)(int *argc, char ***argv, int required, int *provided)
{
    int ret;

    MAP_OR_FAIL(PMPI_Init_thread);

    ret = __real_PMPI_Init_thread(argc, argv, required, provided);
    if(ret != MPI_SUCCESS)
    {
        return(ret);
    }

    if(argc && argv)
    {
        darshan_core_initialize(*argc, *argv);
    }
    else
    {
        /* we don't see argc and argv here in fortran */
        darshan_core_initialize(0, NULL);
    }

    return(ret);
}
DARSHAN_WRAPPER_MAP(PMPI_Init_thread, int, (int *argc, char ***argv, int required, int *provided), MPI_Init_thread(argc,argv,required,provided))

int DARSHAN_DECL(MPI_Finalize)(void)
{
    int ret;

    MAP_OR_FAIL(PMPI_Finalize);

    darshan_core_shutdown();

    ret = __real_PMPI_Finalize();
    return(ret);
}
DARSHAN_WRAPPER_MAP(PMPI_Finalize, int, (void), MPI_Finalize())

/*
 * Local variables:
 *  c-indent-level: 4
 *  c-basic-offset: 4
 * End:
 *
 * vim: ts=8 sts=4 sw=4 expandtab
 */
