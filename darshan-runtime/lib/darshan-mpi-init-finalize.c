/*
 *  (C) 2012 by Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
 */

#define _XOPEN_SOURCE 500
#define _GNU_SOURCE /* for RTLD_NEXT */

#include "darshan-runtime-config.h"

#include <stdlib.h>
#include <stdio.h>

#include "mpi.h"
#include "darshan.h"
#include "darshan-dynamic.h"

#ifdef DARSHAN_PRELOAD

#include <dlfcn.h>

#define DARSHAN_FORWARD_DECL(name,ret,args) \
  ret (*__real_ ## name)args = NULL;

#define MAP_OR_FAIL(func) \
    __real_ ## func = dlsym(RTLD_NEXT, #func); \
    if (!(__real_ ## func)) { \
        fprintf(stderr, "Darshan failed to map symbol: %s\n", #func); \
    }

DARSHAN_FORWARD_DECL(PMPI_File_close, int, (MPI_File *fh));
DARSHAN_FORWARD_DECL(PMPI_File_set_size, int, (MPI_File fh, MPI_Offset size));
DARSHAN_FORWARD_DECL(PMPI_File_iread_at, int, (MPI_File fh, MPI_Offset offset, void *buf, int count, MPI_Datatype datatype, __D_MPI_REQUEST *request));
DARSHAN_FORWARD_DECL(PMPI_File_iread, int, (MPI_File fh, void  *buf, int  count, MPI_Datatype  datatype, __D_MPI_REQUEST  *request));
DARSHAN_FORWARD_DECL(PMPI_File_iread_shared, int, (MPI_File fh, void *buf, int count, MPI_Datatype datatype, __D_MPI_REQUEST *request));
DARSHAN_FORWARD_DECL(PMPI_File_iwrite_at, int, (MPI_File fh, MPI_Offset offset, const void *buf, int count, MPI_Datatype datatype, __D_MPI_REQUEST *request));
DARSHAN_FORWARD_DECL(PMPI_File_iwrite, int, (MPI_File fh, const void *buf, int count, MPI_Datatype datatype, __D_MPI_REQUEST *request));
DARSHAN_FORWARD_DECL(PMPI_File_iwrite_shared, int, (MPI_File fh, const void *buf, int count, MPI_Datatype datatype, __D_MPI_REQUEST *request));
DARSHAN_FORWARD_DECL(PMPI_File_open, int, (MPI_Comm comm, const char *filename, int amode, MPI_Info info, MPI_File *fh));
DARSHAN_FORWARD_DECL(PMPI_File_read_all_begin, int, (MPI_File fh, void *buf, int count, MPI_Datatype datatype));
DARSHAN_FORWARD_DECL(PMPI_File_read_all, int, (MPI_File fh, void *buf, int count, MPI_Datatype datatype, MPI_Status *status));
DARSHAN_FORWARD_DECL(PMPI_File_read_at_all, int, (MPI_File fh, MPI_Offset offset, void *buf, int count, MPI_Datatype datatype, MPI_Status *status));
DARSHAN_FORWARD_DECL(PMPI_File_read_at_all_begin, int, (MPI_File fh, MPI_Offset offset, void *buf, int count, MPI_Datatype datatype));
DARSHAN_FORWARD_DECL(PMPI_File_read_at, int, (MPI_File fh, MPI_Offset offset, void *buf, int count, MPI_Datatype datatype, MPI_Status *status));
DARSHAN_FORWARD_DECL(PMPI_File_read, int, (MPI_File fh, void *buf, int count, MPI_Datatype datatype, MPI_Status *status));
DARSHAN_FORWARD_DECL(PMPI_File_read_ordered_begin, int, (MPI_File fh, void *buf, int count, MPI_Datatype datatype));
DARSHAN_FORWARD_DECL(PMPI_File_read_ordered, int, (MPI_File fh, void *buf, int count, MPI_Datatype datatype, MPI_Status *status));
DARSHAN_FORWARD_DECL(PMPI_File_read_shared, int, (MPI_File fh, void *buf, int count, MPI_Datatype datatype, MPI_Status *status));
DARSHAN_FORWARD_DECL(PMPI_File_set_view, int, (MPI_File fh, MPI_Offset disp, MPI_Datatype etype, MPI_Datatype filetype, const char *datarep, MPI_Info info));
DARSHAN_FORWARD_DECL(PMPI_File_sync, int, (MPI_File fh));
DARSHAN_FORWARD_DECL(PMPI_File_write_all_begin, int, (MPI_File fh, const void *buf, int count, MPI_Datatype datatype));
DARSHAN_FORWARD_DECL(PMPI_File_write_all, int, (MPI_File fh, const void *buf, int count, MPI_Datatype datatype, MPI_Status *status));
DARSHAN_FORWARD_DECL(PMPI_File_write_at_all_begin, int, (MPI_File fh, MPI_Offset offset, const void *buf, int count, MPI_Datatype datatype));
DARSHAN_FORWARD_DECL(PMPI_File_write_at_all, int, (MPI_File fh, MPI_Offset offset, const void *buf, int count, MPI_Datatype datatype, MPI_Status *status));
DARSHAN_FORWARD_DECL(PMPI_File_write_at, int, (MPI_File fh, MPI_Offset offset, const void *buf, int count, MPI_Datatype datatype, MPI_Status *status));
DARSHAN_FORWARD_DECL(PMPI_File_write, int, (MPI_File fh, const void *buf, int count, MPI_Datatype datatype, MPI_Status *status));
DARSHAN_FORWARD_DECL(PMPI_File_write_ordered_begin, int, (MPI_File fh, const void *buf, int count, MPI_Datatype datatype));
DARSHAN_FORWARD_DECL(PMPI_File_write_ordered, int, (MPI_File fh, const void *buf, int count, MPI_Datatype datatype, MPI_Status *status));
DARSHAN_FORWARD_DECL(PMPI_File_write_shared, int, (MPI_File fh, const void *buf, int count, MPI_Datatype datatype, MPI_Status *status));
DARSHAN_FORWARD_DECL(PMPI_Finalize, int, ());
DARSHAN_FORWARD_DECL(PMPI_Init, int, (int *argc, char ***argv));
DARSHAN_FORWARD_DECL(PMPI_Init_thread, int, (int *argc, char ***argv, int required, int *provided));

DARSHAN_FORWARD_DECL(PMPI_Wtime, double, ());
//DARSHAN_FORWARD_DECL(PMPI_Allreduce, int, (void *sendbuf, void *recvbuf, int count, MPI_Datatype datatype, MPI_Op op, MPI_Comm comm));
//DARSHAN_FORWARD_DECL(PMPI_Bcast, int, (void *buffer, int count, MPI_Datatype datatype, int root, MPI_Comm comm));
DARSHAN_FORWARD_DECL(PMPI_Comm_rank, int, (MPI_Comm comm, int *rank));
DARSHAN_FORWARD_DECL(PMPI_Comm_size, int, (MPI_Comm comm, int *size));
DARSHAN_FORWARD_DECL(PMPI_Scan, int, (void *sendbuf, void *recvbuf, int count, MPI_Datatype datatype, MPI_Op op, MPI_Comm comm));
DARSHAN_FORWARD_DECL(PMPI_Type_commit, int, (MPI_Datatype *datatype));
DARSHAN_FORWARD_DECL(PMPI_Type_contiguous, int, (int count, MPI_Datatype oldtype, MPI_Datatype *newtype));
DARSHAN_FORWARD_DECL(PMPI_Type_extent, int, (MPI_Datatype datatype, MPI_Aint *extent));
DARSHAN_FORWARD_DECL(PMPI_Type_free, int, (MPI_Datatype *datatype));
DARSHAN_FORWARD_DECL(PMPI_Type_hindexed, int, (int count, int *array_of_blocklengths, MPI_Aint *array_of_displacements, MPI_Datatype oldtype, MPI_Datatype *newtype));
DARSHAN_FORWARD_DECL(PMPI_Op_create, int, (MPI_User_function *function, int commute, MPI_Op *op));
DARSHAN_FORWARD_DECL(PMPI_Op_free, int, (MPI_Op *op));
//DARSHAN_FORWARD_DECL(PMPI_Reduce, int, (void *sendbuf, void *recvbuf, int count, MPI_Datatype datatype, MPI_Op op, int root, MPI_Comm comm));
DARSHAN_FORWARD_DECL(PMPI_Type_get_envelope, int, (MPI_Datatype datatype, int *num_integers, int *num_addresses, int *num_datatypes, int *combiner));
DARSHAN_FORWARD_DECL(PMPI_Type_size, int, (MPI_Datatype datatype, int *size));

DARSHAN_FORWARD_DECL(PMPI_Send, int, (const void* buf, int count, MPI_Datatype datatype, int dest, int tag, MPI_Comm comm));
DARSHAN_FORWARD_DECL(PMPI_Recv, int, (void* buf, int count, MPI_Datatype datatype, int source, int tag, MPI_Comm comm, MPI_Status *status));
DARSHAN_FORWARD_DECL(PMPI_Isend, int, (const void *buf, int count, MPI_Datatype datatype, int dest, int tag, MPI_Comm comm, MPI_Request *request));
DARSHAN_FORWARD_DECL(PMPI_Irecv, int, (void *buf, int count, MPI_Datatype datatype, int source, int tag, MPI_Comm comm, MPI_Request *request));
DARSHAN_FORWARD_DECL(PMPI_Wait, int, (MPI_Request *request, MPI_Status *status));
DARSHAN_FORWARD_DECL(PMPI_Test, int, (MPI_Request *request, int *flag, MPI_Status *status));
DARSHAN_FORWARD_DECL(PMPI_Waitany, int, (int count, MPI_Request array_of_requests[], int *indx, MPI_Status *status));
DARSHAN_FORWARD_DECL(PMPI_Testany, int, (int count, MPI_Request array_of_requests[], int *indx, int *flag, MPI_Status *status));
DARSHAN_FORWARD_DECL(PMPI_Waitall, int, (int count, MPI_Request array_of_requests[], MPI_Status array_of_statuses[]));
DARSHAN_FORWARD_DECL(PMPI_Testall, int, (int count, MPI_Request array_of_requests[], int *flag, MPI_Status array_of_statuses[]));
DARSHAN_FORWARD_DECL(PMPI_Barrier, int, (MPI_Comm comm));
DARSHAN_FORWARD_DECL(PMPI_Bcast, int, (void *buffer, int count, MPI_Datatype datatype, int root, MPI_Comm comm));
DARSHAN_FORWARD_DECL(PMPI_Gather, int, (const void *sendbuf, int sendcount, MPI_Datatype sendtype, void *recvbuf, int recvcount, MPI_Datatype recvtype, int root, MPI_Comm comm));
DARSHAN_FORWARD_DECL(PMPI_Gatherv, int, (const void *sendbuf, int sendcount, MPI_Datatype sendtype, void *recvbuf, const int *recvcounts, const int *displs, MPI_Datatype recvtype, int root, MPI_Comm comm));
DARSHAN_FORWARD_DECL(PMPI_Scatter, int, (const void *sendbuf, int sendcount, MPI_Datatype sendtype, void *recvbuf, int recvcount, MPI_Datatype recvtype, int root, MPI_Comm comm));
DARSHAN_FORWARD_DECL(PMPI_Scatterv, int, (const void *sendbuf, const int *sendcounts, const int *displs, MPI_Datatype sendtype, void *recvbuf, int recvcount, MPI_Datatype recvtype, int root, MPI_Comm comm));
DARSHAN_FORWARD_DECL(PMPI_Allgather, int, (const void *sendbuf, int sendcount, MPI_Datatype sendtype, void *recvbuf, int recvcount, MPI_Datatype recvtype, MPI_Comm comm));
DARSHAN_FORWARD_DECL(PMPI_Allgatherv, int, (const void *sendbuf, int sendcount, MPI_Datatype sendtype, void *recvbuf, const int *recvcounts, const int *displs, MPI_Datatype recvtype, MPI_Comm comm));
DARSHAN_FORWARD_DECL(PMPI_Alltoall, int, (const void *sendbuf, int sendcount, MPI_Datatype sendtype, void *recvbuf, int recvcount, MPI_Datatype recvtype, MPI_Comm comm));
DARSHAN_FORWARD_DECL(PMPI_Alltoallv, int, (const void *sendbuf, const int *sendcounts, const int *sdispls, MPI_Datatype sendtype, void *recvbuf, const int *recvcounts, const int *rdispls, MPI_Datatype recvtype, MPI_Comm comm));
DARSHAN_FORWARD_DECL(PMPI_Reduce, int, (const void *sendbuf, void *recvbuf, int count, MPI_Datatype datatype, MPI_Op op, int root, MPI_Comm comm));
DARSHAN_FORWARD_DECL(PMPI_Allreduce, int, (const void *sendbuf, void *recvbuf, int count, MPI_Datatype datatype, MPI_Op op, MPI_Comm comm));



void resolve_mpi_symbols (void)
{
    /*
     * Overloaded functions
     */
    MAP_OR_FAIL(PMPI_File_close);
    MAP_OR_FAIL(PMPI_File_set_size);
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


    MAP_OR_FAIL(PMPI_Send);
    MAP_OR_FAIL(PMPI_Recv);
    MAP_OR_FAIL(PMPI_Isend);
    MAP_OR_FAIL(PMPI_Irecv);
    MAP_OR_FAIL(PMPI_Wait);
    MAP_OR_FAIL(PMPI_Test);
    MAP_OR_FAIL(PMPI_Waitany);
    MAP_OR_FAIL(PMPI_Testany);
    MAP_OR_FAIL(PMPI_Waitall);
    MAP_OR_FAIL(PMPI_Testall);
    MAP_OR_FAIL(PMPI_Barrier);
    MAP_OR_FAIL(PMPI_Bcast);
    MAP_OR_FAIL(PMPI_Gather);
    MAP_OR_FAIL(PMPI_Gatherv);
    MAP_OR_FAIL(PMPI_Scatter);
    MAP_OR_FAIL(PMPI_Scatterv);
    MAP_OR_FAIL(PMPI_Allgather);
    MAP_OR_FAIL(PMPI_Allgatherv);
    MAP_OR_FAIL(PMPI_Alltoall);
    MAP_OR_FAIL(PMPI_Alltoallv);
    MAP_OR_FAIL(PMPI_Reduce);
    MAP_OR_FAIL(PMPI_Allreduce);
   
    /*
     * These function are not intercepted but are used
     * by darshan itself.
     */
    MAP_OR_FAIL(PMPI_Wtime);
    // MAP_OR_FAIL(PMPI_Allreduce);
    // MAP_OR_FAIL(PMPI_Bcast);
    MAP_OR_FAIL(PMPI_Comm_rank);
    MAP_OR_FAIL(PMPI_Comm_size);
    MAP_OR_FAIL(PMPI_Scan);
    MAP_OR_FAIL(PMPI_Type_commit);
    MAP_OR_FAIL(PMPI_Type_contiguous);
    MAP_OR_FAIL(PMPI_Type_extent);
    MAP_OR_FAIL(PMPI_Type_free);
    MAP_OR_FAIL(PMPI_Type_size);
    MAP_OR_FAIL(PMPI_Type_hindexed);
    MAP_OR_FAIL(PMPI_Op_create);
    MAP_OR_FAIL(PMPI_Op_free);
    // MAP_OR_FAIL(PMPI_Reduce);
    MAP_OR_FAIL(PMPI_Type_get_envelope);

    return;
}

#endif

int MPI_Init(int *argc, char ***argv)
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

    darshan_mpi_initialize(argc, argv);

    return(ret);
}

int MPI_Init_thread (int *argc, char ***argv, int required, int *provided)
{
    int ret;

#ifdef DARSHAN_PRELOAD
    resolve_mpi_symbols();
#endif

    ret = DARSHAN_MPI_CALL(PMPI_Init_thread)(argc, argv, required, provided);
    if (ret != MPI_SUCCESS)
    {
        return(ret);
    }

    darshan_mpi_initialize(argc, argv);

    return(ret);
}

int MPI_Finalize(void)
{
    int ret;

    if(getenv("DARSHAN_INTERNAL_TIMING"))
        darshan_shutdown(1);
    else
        darshan_shutdown(0);

    ret = DARSHAN_MPI_CALL(PMPI_Finalize)();
    return(ret);
}

