/*
 * Copyright (C) 2015 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

#ifdef HAVE_CONFIG_H
# include <darshan-runtime-config.h>
#endif

#define _XOPEN_SOURCE 500
#define _GNU_SOURCE

#include <stdio.h>
#include <stdlib.h>
#ifdef HAVE_MPI
#include <mpi.h>
#endif

#include "darshan.h"
#include "darshan-dynamic.h"

#ifdef HAVE_MPI
DARSHAN_FORWARD_DECL(PMPI_Finalize, int, ());
DARSHAN_FORWARD_DECL(PMPI_Init, int, (int *argc, char ***argv));
DARSHAN_FORWARD_DECL(PMPI_Init_thread, int, (int *argc, char ***argv, int required, int *provided));

int DARSHAN_DECL(MPI_Init)(int *argc, char ***argv)
{
    int ret;

    MAP_OR_FAIL(PMPI_Init);
    (void)__darshan_disabled;

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
DARSHAN_WRAPPER_MAP(PMPI_Init, int, (int *argc, char ***argv), MPI_Init)

int DARSHAN_DECL(MPI_Init_thread)(int *argc, char ***argv, int required, int *provided)
{
    int ret;

    MAP_OR_FAIL(PMPI_Init_thread);
    (void)__darshan_disabled;

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
DARSHAN_WRAPPER_MAP(PMPI_Init_thread, int, (int *argc, char ***argv, int required, int *provided), MPI_Init_thread)

int DARSHAN_DECL(MPI_Finalize)(void)
{
    int ret;

    MAP_OR_FAIL(PMPI_Finalize);
    (void)__darshan_disabled;

    darshan_core_shutdown(1);

    ret = __real_PMPI_Finalize();
    return(ret);
}
DARSHAN_WRAPPER_MAP(PMPI_Finalize, int, (void), MPI_Finalize)
#endif

/*
 * Initialization hook that does not rely on MPI
 */
#ifdef __GNUC__
__attribute__((constructor)) void serial_init(void)
{
    char *no_mpi = getenv("DARSHAN_ENABLE_NONMPI");
    if (no_mpi)
        darshan_core_initialize(0, NULL);
    return;
}

__attribute__((destructor)) void serial_finalize(void)
{
    char *no_mpi = getenv("DARSHAN_ENABLE_NONMPI");
    if (no_mpi)
        darshan_core_shutdown(1);
    return;
}
#endif

#if defined(DARSHAN_PRELOAD) && defined(__DARSHAN_ENABLE_EXIT_WRAPPER)
void (*__real__exit)(int status) __attribute__ ((noreturn)) = NULL;
void _exit(int status)
{
    MAP_OR_FAIL(_exit);
    (void)__darshan_disabled;

    char *no_mpi = getenv("DARSHAN_ENABLE_NONMPI");
    if (no_mpi)
        darshan_core_shutdown(1);

    __real__exit(status);
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
