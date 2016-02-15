/*
 *  (C) 2009 by Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
 */

/* Benchmark to measure cost of realpath() calls
 */

/* Arguments: path to check and an integer specifying the number of 
 * iterations to run 
 */

#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdlib.h>
#include <limits.h>

#include <mpi.h>

int main(int argc, char **argv) 
{
    int nprocs;
    int mynod;
    int i;
    int ret;
    int iters;
    double time1, time2;
    char* path;

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
    if(argc != 3)
    {
        fprintf(stderr, "Usage: %s <path> <number of iterations>\n", argv[0]);
        MPI_Finalize();
        return(-1);
    }

    ret = sscanf(argv[2], "%d", &iters);
    if(ret != 1)
    {
        fprintf(stderr, "Usage: %s <number of iterations>\n", argv[0]);
        MPI_Finalize();
        return(-1);
    }

    time1 = MPI_Wtime();
    for(i=0; i<iters; i++)
    {
        path = realpath(argv[1], NULL);    
        free(path);
    }
    time2 = MPI_Wtime();

    sleep(1);

    /* print out some timing info */
    printf("#<op>\t<iters>\t<total (s)>\t<per op (s)>\n");
    printf("wtime\t%d\t%.9f\t%.9f\n", iters, time2-time1, (time2-time1)/(double)iters);

    MPI_Finalize();
    return(0);
}
