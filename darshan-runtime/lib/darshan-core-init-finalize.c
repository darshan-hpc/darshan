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

#ifdef DARSHAN_PRELOAD

DARSHAN_FORWARD_DECL(PMPI_File_close, int, (MPI_File *fh));
DARSHAN_FORWARD_DECL(PMPI_File_iread_at, int, (MPI_File fh, MPI_Offset offset, void *buf, int count, MPI_Datatype datatype, __D_MPI_REQUEST *request));
DARSHAN_FORWARD_DECL(PMPI_File_iread, int, (MPI_File fh, void  *buf, int  count, MPI_Datatype  datatype, __D_MPI_REQUEST  *request));
DARSHAN_FORWARD_DECL(PMPI_File_iread_shared, int, (MPI_File fh, void *buf, int count, MPI_Datatype datatype, __D_MPI_REQUEST *request));
#ifdef HAVE_MPIIO_CONST
DARSHAN_FORWARD_DECL(PMPI_File_iwrite_at, int, (MPI_File fh, MPI_Offset offset, const void *buf, int count, MPI_Datatype datatype, __D_MPI_REQUEST *request));
#else
DARSHAN_FORWARD_DECL(PMPI_File_iwrite_at, int, (MPI_File fh, MPI_Offset offset, void *buf, int count, MPI_Datatype datatype, __D_MPI_REQUEST *request));
#endif
#ifdef HAVE_MPIIO_CONST
DARSHAN_FORWARD_DECL(PMPI_File_iwrite, int, (MPI_File fh, const void *buf, int count, MPI_Datatype datatype, __D_MPI_REQUEST *request));
#else
DARSHAN_FORWARD_DECL(PMPI_File_iwrite, int, (MPI_File fh, void *buf, int count, MPI_Datatype datatype, __D_MPI_REQUEST *request));
#endif
#ifdef HAVE_MPIIO_CONST
DARSHAN_FORWARD_DECL(PMPI_File_iwrite_shared, int, (MPI_File fh, const void *buf, int count, MPI_Datatype datatype, __D_MPI_REQUEST *request));
#else
DARSHAN_FORWARD_DECL(PMPI_File_iwrite_shared, int, (MPI_File fh, void *buf, int count, MPI_Datatype datatype, __D_MPI_REQUEST *request));
#endif
#ifdef HAVE_MPIIO_CONST
DARSHAN_FORWARD_DECL(PMPI_File_open, int, (MPI_Comm comm, const char *filename, int amode, MPI_Info info, MPI_File *fh));
#else
DARSHAN_FORWARD_DECL(PMPI_File_open, int, (MPI_Comm comm, char *filename, int amode, MPI_Info info, MPI_File *fh));
#endif
DARSHAN_FORWARD_DECL(PMPI_File_read_all_begin, int, (MPI_File fh, void *buf, int count, MPI_Datatype datatype));
DARSHAN_FORWARD_DECL(PMPI_File_read_all, int, (MPI_File fh, void *buf, int count, MPI_Datatype datatype, MPI_Status *status));
DARSHAN_FORWARD_DECL(PMPI_File_read_at_all, int, (MPI_File fh, MPI_Offset offset, void *buf, int count, MPI_Datatype datatype, MPI_Status *status));
DARSHAN_FORWARD_DECL(PMPI_File_read_at_all_begin, int, (MPI_File fh, MPI_Offset offset, void *buf, int count, MPI_Datatype datatype));
DARSHAN_FORWARD_DECL(PMPI_File_read_at, int, (MPI_File fh, MPI_Offset offset, void *buf, int count, MPI_Datatype datatype, MPI_Status *status));
DARSHAN_FORWARD_DECL(PMPI_File_read, int, (MPI_File fh, void *buf, int count, MPI_Datatype datatype, MPI_Status *status));
DARSHAN_FORWARD_DECL(PMPI_File_read_ordered_begin, int, (MPI_File fh, void *buf, int count, MPI_Datatype datatype));
DARSHAN_FORWARD_DECL(PMPI_File_read_ordered, int, (MPI_File fh, void *buf, int count, MPI_Datatype datatype, MPI_Status *status));
DARSHAN_FORWARD_DECL(PMPI_File_read_shared, int, (MPI_File fh, void *buf, int count, MPI_Datatype datatype, MPI_Status *status));
#ifdef HAVE_MPIIO_CONST
DARSHAN_FORWARD_DECL(PMPI_File_set_view, int, (MPI_File fh, MPI_Offset disp, MPI_Datatype etype, MPI_Datatype filetype, const char *datarep, MPI_Info info));
#else
DARSHAN_FORWARD_DECL(PMPI_File_set_view, int, (MPI_File fh, MPI_Offset disp, MPI_Datatype etype, MPI_Datatype filetype, char *datarep, MPI_Info info));
#endif
DARSHAN_FORWARD_DECL(PMPI_File_sync, int, (MPI_File fh));
#ifdef HAVE_MPIIO_CONST
DARSHAN_FORWARD_DECL(PMPI_File_write_all_begin, int, (MPI_File fh, const void *buf, int count, MPI_Datatype datatype));
#else
DARSHAN_FORWARD_DECL(PMPI_File_write_all_begin, int, (MPI_File fh, void *buf, int count, MPI_Datatype datatype));
#endif
#ifdef HAVE_MPIIO_CONST
DARSHAN_FORWARD_DECL(PMPI_File_write_all, int, (MPI_File fh, const void *buf, int count, MPI_Datatype datatype, MPI_Status *status));
#else
DARSHAN_FORWARD_DECL(PMPI_File_write_all, int, (MPI_File fh, void *buf, int count, MPI_Datatype datatype, MPI_Status *status));
#endif
#ifdef HAVE_MPIIO_CONST
DARSHAN_FORWARD_DECL(PMPI_File_write_at_all_begin, int, (MPI_File fh, MPI_Offset offset, const void *buf, int count, MPI_Datatype datatype));
#else
DARSHAN_FORWARD_DECL(PMPI_File_write_at_all_begin, int, (MPI_File fh, MPI_Offset offset, void *buf, int count, MPI_Datatype datatype));
#endif
#ifdef HAVE_MPIIO_CONST
DARSHAN_FORWARD_DECL(PMPI_File_write_at_all, int, (MPI_File fh, MPI_Offset offset, const void *buf, int count, MPI_Datatype datatype, MPI_Status *status));
#else
DARSHAN_FORWARD_DECL(PMPI_File_write_at_all, int, (MPI_File fh, MPI_Offset offset, void *buf, int count, MPI_Datatype datatype, MPI_Status *status));
#endif
#ifdef HAVE_MPIIO_CONST
DARSHAN_FORWARD_DECL(PMPI_File_write_at, int, (MPI_File fh, MPI_Offset offset, const void *buf, int count, MPI_Datatype datatype, MPI_Status *status));
#else
DARSHAN_FORWARD_DECL(PMPI_File_write_at, int, (MPI_File fh, MPI_Offset offset, void *buf, int count, MPI_Datatype datatype, MPI_Status *status));
#endif
#ifdef HAVE_MPIIO_CONST
DARSHAN_FORWARD_DECL(PMPI_File_write, int, (MPI_File fh, const void *buf, int count, MPI_Datatype datatype, MPI_Status *status));
#else
DARSHAN_FORWARD_DECL(PMPI_File_write, int, (MPI_File fh, void *buf, int count, MPI_Datatype datatype, MPI_Status *status));
#endif
#ifdef HAVE_MPIIO_CONST
DARSHAN_FORWARD_DECL(PMPI_File_write_ordered_begin, int, (MPI_File fh, const void *buf, int count, MPI_Datatype datatype));
#else
DARSHAN_FORWARD_DECL(PMPI_File_write_ordered_begin, int, (MPI_File fh, void *buf, int count, MPI_Datatype datatype));
#endif
#ifdef HAVE_MPIIO_CONST
DARSHAN_FORWARD_DECL(PMPI_File_write_ordered, int, (MPI_File fh, const void *buf, int count, MPI_Datatype datatype, MPI_Status *status));
#else
DARSHAN_FORWARD_DECL(PMPI_File_write_ordered, int, (MPI_File fh, void *buf, int count, MPI_Datatype datatype, MPI_Status *status));
#endif
#ifdef HAVE_MPIIO_CONST
DARSHAN_FORWARD_DECL(PMPI_File_write_shared, int, (MPI_File fh, const void *buf, int count, MPI_Datatype datatype, MPI_Status *status));
#else
DARSHAN_FORWARD_DECL(PMPI_File_write_shared, int, (MPI_File fh, void *buf, int count, MPI_Datatype datatype, MPI_Status *status));
#endif
DARSHAN_FORWARD_DECL(PMPI_Finalize, int, ());
DARSHAN_FORWARD_DECL(PMPI_Init, int, (int *argc, char ***argv));
DARSHAN_FORWARD_DECL(PMPI_Init_thread, int, (int *argc, char ***argv, int required, int *provided));
DARSHAN_FORWARD_DECL(PMPI_Wtime, double, ());
DARSHAN_FORWARD_DECL(PMPI_Allreduce, int, (void *sendbuf, void *recvbuf, int count, MPI_Datatype datatype, MPI_Op op, MPI_Comm comm));
DARSHAN_FORWARD_DECL(PMPI_Bcast, int, (void *buffer, int count, MPI_Datatype datatype, int root, MPI_Comm comm));
DARSHAN_FORWARD_DECL(PMPI_Comm_rank, int, (MPI_Comm comm, int *rank));
DARSHAN_FORWARD_DECL(PMPI_Comm_size, int, (MPI_Comm comm, int *size));
DARSHAN_FORWARD_DECL(PMPI_Scan, int, (void *sendbuf, void *recvbuf, int count, MPI_Datatype datatype, MPI_Op op, MPI_Comm comm));
DARSHAN_FORWARD_DECL(PMPI_Type_commit, int, (MPI_Datatype *datatype));
DARSHAN_FORWARD_DECL(PMPI_Type_contiguous, int, (int count, MPI_Datatype oldtype, MPI_Datatype *newtype));
DARSHAN_FORWARD_DECL(PMPI_Type_extent, int, (MPI_Datatype datatype, MPI_Aint *extent));
DARSHAN_FORWARD_DECL(PMPI_Type_free, int, (MPI_Datatype *datatype));
DARSHAN_FORWARD_DECL(PMPI_Type_hindexed, int, (int count, int *array_of_blocklengths, MPI_Aint *array_of_displacements, MPI_Datatype oldtype, MPI_Datatype *newtype));
DARSHAN_FORWARD_DECL(PMPI_Type_get_envelope, int, (MPI_Datatype datatype, int *num_integers, int *num_addresses, int *num_datatypes, int *combiner));
DARSHAN_FORWARD_DECL(PMPI_Type_size, int, (MPI_Datatype datatype, int *size));
DARSHAN_FORWARD_DECL(PMPI_Op_create, int, (MPI_User_function *function, int commute, MPI_Op *op));
DARSHAN_FORWARD_DECL(PMPI_Op_free, int, (MPI_Op *op));
#ifdef HAVE_MPIIO_CONST
DARSHAN_FORWARD_DECL(PMPI_Reduce, int, (const void *sendbuf, void *recvbuf, int count, MPI_Datatype datatype, MPI_Op op, int root, MPI_Comm comm));
#else
DARSHAN_FORWARD_DECL(PMPI_Reduce, int, (void *sendbuf, void *recvbuf, int count, MPI_Datatype datatype, MPI_Op op, int root, MPI_Comm comm));
#endif
#ifdef HAVE_MPIIO_CONST
DARSHAN_FORWARD_DECL(PMPI_Send, int, (const void *buf, int count, MPI_Datatype datatype, int dest, int tag, MPI_Comm comm));
#else
DARSHAN_FORWARD_DECL(PMPI_Send, int, (void *buf, int count, MPI_Datatype datatype, int dest, int tag, MPI_Comm comm));
#endif
DARSHAN_FORWARD_DECL(PMPI_Recv, int, (void *buf, int count, MPI_Datatype datatype, int source, int tag, MPI_Comm comm, MPI_Status *status));
#ifdef HAVE_MPIIO_CONST
DARSHAN_FORWARD_DECL(PMPI_Gather, int, (const void *sendbuf, int sendcount, MPI_Datatype sendtype, void *recvbuf, int recvcount, MPI_Datatype recvtype, int root, MPI_Comm comm));
#else
DARSHAN_FORWARD_DECL(PMPI_Gather, int, (void *sendbuf, int sendcount, MPI_Datatype sendtype, void *recvbuf, int recvcount, MPI_Datatype recvtype, int root, MPI_Comm comm));
#endif
DARSHAN_FORWARD_DECL(PMPI_Barrier, int, (MPI_Comm comm));

void resolve_mpi_symbols (void)
{
    /*
     * Overloaded functions
     */
    MAP_OR_FAIL(PMPI_File_close);
    MAP_OR_FAIL(PMPI_File_iread_at);
    MAP_OR_FAIL(PMPI_File_iread);
    MAP_OR_FAIL(PMPI_File_iread_shared);
    MAP_OR_FAIL(PMPI_File_iwrite_at);
    MAP_OR_FAIL(PMPI_File_iwrite);
    MAP_OR_FAIL(PMPI_File_iwrite_shared);
    MAP_OR_FAIL(PMPI_File_open);
    MAP_OR_FAIL(PMPI_File_read_all_begin);
    MAP_OR_FAIL(PMPI_File_read_all);
    MAP_OR_FAIL(PMPI_File_read_at_all_begin);
    MAP_OR_FAIL(PMPI_File_read_at_all);
    MAP_OR_FAIL(PMPI_File_read_at);
    MAP_OR_FAIL(PMPI_File_read);
    MAP_OR_FAIL(PMPI_File_read_ordered_begin);
    MAP_OR_FAIL(PMPI_File_read_ordered);
    MAP_OR_FAIL(PMPI_File_read_shared);
    MAP_OR_FAIL(PMPI_File_set_view);
    MAP_OR_FAIL(PMPI_File_sync);
    MAP_OR_FAIL(PMPI_File_write_all_begin);
    MAP_OR_FAIL(PMPI_File_write_all);
    MAP_OR_FAIL(PMPI_File_write_at_all_begin);
    MAP_OR_FAIL(PMPI_File_write_at_all);
    MAP_OR_FAIL(PMPI_File_write_at);
    MAP_OR_FAIL(PMPI_File_write);
    MAP_OR_FAIL(PMPI_File_write_ordered_begin);
    MAP_OR_FAIL(PMPI_File_write_ordered);
    MAP_OR_FAIL(PMPI_File_write_shared);
    MAP_OR_FAIL(PMPI_Finalize);
    MAP_OR_FAIL(PMPI_Init);
    MAP_OR_FAIL(PMPI_Init_thread);
    /*
     * These function are not intercepted but are used
     * by darshan itself.
     */
    MAP_OR_FAIL(PMPI_Wtime);
    MAP_OR_FAIL(PMPI_Allreduce);
    MAP_OR_FAIL(PMPI_Bcast);
    MAP_OR_FAIL(PMPI_Comm_rank);
    MAP_OR_FAIL(PMPI_Comm_size);
    MAP_OR_FAIL(PMPI_Scan);
    MAP_OR_FAIL(PMPI_Type_commit);
    MAP_OR_FAIL(PMPI_Type_contiguous);
    MAP_OR_FAIL(PMPI_Type_extent);
    MAP_OR_FAIL(PMPI_Type_free);
    MAP_OR_FAIL(PMPI_Type_hindexed);
    MAP_OR_FAIL(PMPI_Type_get_envelope);
    MAP_OR_FAIL(PMPI_Type_size);
    MAP_OR_FAIL(PMPI_Op_create);
    MAP_OR_FAIL(PMPI_Op_free);
    MAP_OR_FAIL(PMPI_Reduce);
    MAP_OR_FAIL(PMPI_Send);
    MAP_OR_FAIL(PMPI_Recv);
    MAP_OR_FAIL(PMPI_Gather);
    MAP_OR_FAIL(PMPI_Barrier);

    return;
}

#endif

int DARSHAN_MPI_DECL(MPI_Init)(int *argc, char ***argv)
{
    int ret;
#ifdef DARSHAN_PRELOAD
    resolve_mpi_symbols();
#endif

    ret = DARSHAN_MPI_CALL(PMPI_Init)(argc, argv);
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

DARSHAN_MPI_MAP(MPI_Init, int, (int *argc, char ***argv), MPI_Init(argc,argv))

int MPI_Init_thread(int *argc, char ***argv, int required, int *provided)
{
    int ret;

#ifdef DARSHAN_PRELOAD
    resolve_mpi_symbols();
#endif

    ret = DARSHAN_MPI_CALL(PMPI_Init_thread)(argc, argv, required, provided);
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

int DARSHAN_MPI_DECL(MPI_Finalize)(void)
{
    int ret;

    darshan_core_shutdown();

    ret = DARSHAN_MPI_CALL(PMPI_Finalize)();
    return(ret);
}
DARSHAN_MPI_MAP(MPI_Finalize, int, (void), MPI_Finalize())

/*
 * Local variables:
 *  c-indent-level: 4
 *  c-basic-offset: 4
 * End:
 *
 * vim: ts=8 sts=4 sw=4 expandtab
 */
