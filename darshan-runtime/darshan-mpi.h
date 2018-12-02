#ifndef __DARSHAN_MPI_H
#define __DARSHAN_MPI_H

#ifdef HAVE_MPI
#include <mpi.h>

struct darshan_mpi_file {
    enum {
        FH_TYPE_MPI,
        FH_TYPE_POSIX
    } fhtype;
    union {
        MPI_File mpi;
        int posix;
    } fh;
};

int darshan_mpi_comm_size(MPI_Comm comm, int *_nprocs);
int darshan_mpi_comm_rank(MPI_Comm comm, int *me);
double darshan_mpi_wtime();

int darshan_mpi_allreduce(void *sendbuf, void *recvbuf, int count, MPI_Datatype datatype, MPI_Op op, MPI_Comm comm);
int darshan_mpi_reduce(void *sendbuf, void *recvbuf, int count, MPI_Datatype datatype, MPI_Op op, int root, MPI_Comm comm);
int darshan_mpi_reduce_records(void *sendbuf, void *recvbuf, int count, int record_size, MPI_Op op, int root, MPI_Comm comm);
int darshan_mpi_scan(void *sendbuf, void *recvbuf, int count, MPI_Datatype datatype, MPI_Op op, MPI_Comm comm);
int darshan_mpi_gather(void *sendbuf, int sendcount, MPI_Datatype sendtype, void *recvbuf, int recvcount, MPI_Datatype recvtype, int root, MPI_Comm comm);
int darshan_mpi_barrier(MPI_Comm comm);
int darshan_mpi_bcast(void *buf, int count, MPI_Datatype datatype, int root, MPI_Comm comm);

int darshan_mpi_op_create(MPI_User_function *user_fn, int commute, MPI_Op *op);
int darshan_mpi_op_free(MPI_Op *op);

int darshan_mpi_file_open(MPI_Comm comm, const char *filename, int amode, MPI_Info info, struct darshan_mpi_file *fh);
int darshan_mpi_file_close(struct darshan_mpi_file *fh);
int darshan_mpi_file_write_at(struct darshan_mpi_file fh, MPI_Offset offset,
    const void *buf, int count, MPI_Datatype datatype, MPI_Status *status);
int darshan_mpi_file_write_at_all(struct darshan_mpi_file fh, MPI_Offset offset,
    const void *buf, int count, MPI_Datatype datatype, MPI_Status *status);
int darshan_mpi_info_create(MPI_Info *info);
int darshan_mpi_info_set(MPI_Info info, char *key, char *value);
int darshan_mpi_info_free(MPI_Info *info);

int darshan_mpi_type_contiguous(int count, MPI_Datatype oldtype, MPI_Datatype *newtype);
int darshan_mpi_type_commit(MPI_Datatype *datatype);
int darshan_mpi_type_free(MPI_Datatype *datatype);
#endif /* #ifdef HAVE_MPI */

#endif /* __DARSHAN_MPI_H */
