/*
 *  (C) 2009 by Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
 */

/* Arguments: an integer specifying the number of iterations to run of each
 * test phase
 */

#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include <mpi.h>

/* NOTE: we deliberately provide our own function declaration here; there is
 * no header installed with the instrumentation package that defines the
 * benchmarking hooks for us.  This should only be used by special-purpose
 * benchmarking tools.
 */
void darshan_shutdown_bench(int argc, char** argv, int rank, int nprocs);

int main(int argc, char **argv) 
{
    int nprocs;
    int mynod;
    int ret;
    int iters;

    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &mynod);
    MPI_Comm_size(MPI_COMM_WORLD, &nprocs);

    /* parse argument for number of iterations */
    if(argc != 2)
    {
        fprintf(stderr, "Usage: %s <number of iterations>\n", argv[0]);
        MPI_Finalize();
        return(-1);
    }

    ret = sscanf(argv[1], "%d", &iters);
    if(ret != 1)
    {
        fprintf(stderr, "Usage: %s <number of iterations>\n", argv[0]);
        MPI_Finalize();
        return(-1);
    }

    if(mynod == 0)
    {
        printf("ignoring iters argument for now.\n");
    }

    darshan_shutdown_bench(argc, argv, mynod, nprocs);

    MPI_Finalize();
    return(0);
}
