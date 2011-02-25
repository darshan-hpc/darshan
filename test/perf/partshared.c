#include <stdlib.h>
#include <stdio.h>
#include <assert.h>
#include <mpi.h>

#define FILE_COUNT (64)

int main (int argc, char **argv)
{
    int rc;
    int world_rank;
    int world_size;
    int sub_rank;
    int count;
    char* zerobuf;
    double stime;
    double etime;
    double ttime;
    double maxtime;
    char fname[256];
    MPI_Offset offset;
    MPI_Comm sub_comm;
    MPI_File fh;
    MPI_Status status;

    count = 1024*1024*50;

    zerobuf = malloc(count);
    assert(zerobuf); 
    memset(zerobuf, 0, count);

    rc = MPI_Init(&argc, &argv);
    if (rc != MPI_SUCCESS)
    {
        fprintf(stderr, "MPI_Init failed: %d\n", rc);
    }

    rc = MPI_Comm_rank (MPI_COMM_WORLD, &world_rank);
    if (rc != MPI_SUCCESS)
    {
        fprintf(stderr, "MPI_Comm_rank failed: %d\n", rc);
    }

    rc = MPI_Comm_size (MPI_COMM_WORLD, &world_size);
    if (rc != MPI_SUCCESS)
    {
        fprintf(stderr, "MPI_Comm_size failed: %d\n", rc);
    }

    if ((world_size % FILE_COUNT) && (world_rank == 0))
    {
        fprintf(stderr, "Not evenly disible node size\n");
    }

    rc = MPI_Comm_split(MPI_COMM_WORLD,
                        (world_rank%FILE_COUNT),
                        world_rank,
                        &sub_comm);
    if (rc != MPI_SUCCESS)
    {
        fprintf(stderr, "MPI_Comm_split failed: %d\n", rc);
    }

    rc = MPI_Comm_rank(sub_comm, &sub_rank);
    if (rc != MPI_SUCCESS)
    {
        fprintf(stderr, "MPI_Comm_rank2 failed: %d\n", rc);
    }

    memset(fname, 0, sizeof(fname));

    if (sub_rank == 0)
    {
        sprintf(fname, "%s.%d", "partfile", world_rank);
    }

    MPI_Barrier(sub_comm);

    MPI_Bcast(fname, sizeof(fname), MPI_BYTE, 0, sub_comm);

    MPI_Barrier(MPI_COMM_WORLD);

    stime = MPI_Wtime();

    rc = MPI_File_open(sub_comm,
                       fname,
                       (MPI_MODE_CREATE|MPI_MODE_DELETE_ON_CLOSE|MPI_MODE_RDWR),
                       MPI_INFO_NULL,
                       &fh);
    if (rc != MPI_SUCCESS)
    {
        fprintf(stderr, "MPI_File_open failed: %d\n", rc);
    }

    offset = sub_rank * count;

    rc = MPI_File_write_at(fh, offset, zerobuf, count, MPI_BYTE, &status);
    if (rc != MPI_SUCCESS)
    {
        fprintf(stderr, "MPI_File_write_at failed: %d\n", rc);
    }

    rc = MPI_File_close(&fh);
    if (rc != MPI_SUCCESS)
    {
        fprintf(stderr, "MPI_File_close failed: %d\n", rc);
    }

    etime = MPI_Wtime();
    ttime = etime-stime;

    MPI_Comm_free (&sub_comm);

    MPI_Barrier(MPI_COMM_WORLD);

    rc= MPI_Reduce(&ttime, &maxtime, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);

   if (world_rank == 0)
   {
       double total_Mbytes = ((double)count * (double)world_size) / (1024.0*1024.0);
       printf("slowest time: %lf\n", maxtime);
       printf("total Mbytes: %lf\n", total_Mbytes);
       printf("rate: %lf\n", (total_Mbytes/maxtime));
   }

    MPI_Finalize();

    return 0;
}
