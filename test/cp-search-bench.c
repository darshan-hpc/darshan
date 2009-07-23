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
#include "darshan.h"

int main(int argc, char **argv) 
{
    int nprocs;
    int mynod;
    int i;
    int ret;
    int fd;
    int iters;

    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &mynod);
    MPI_Comm_size(MPI_COMM_WORLD, &nprocs);

    /* we only want one proc */
    if(nprocs > 1)
    {
        if(mynod == 0)
        {
            fprintf(stderr, "Error: this benchmark should be run with exactly one process.\n");
        }
        MPI_Finalize();
        return(-1);
    }

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

    darshan_search_bench(argc, argv, iters);

    MPI_Finalize();
    return(0);
}
