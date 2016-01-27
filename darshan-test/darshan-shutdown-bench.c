/*
 *  (C) 2015 by Argonne National Laboratory.
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
void darshan_shutdown_bench(int argc, char** argv);

int main(int argc, char **argv) 
{
    MPI_Init(&argc, &argv);

    if(argc != 1)
    {
        fprintf(stderr, "Usage: %s\n", argv[0]);
        MPI_Finalize();
        return(-1);
    }

    darshan_shutdown_bench(argc, argv);

    MPI_Finalize();
    return(0);
}
