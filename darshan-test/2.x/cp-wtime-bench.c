/*
 *  (C) 2009 by Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
 */

/* Benchmark to measure Chutzpah overhead.  This is an MPI program that uses
 * exactly one process to perform I/O on /dev/zero for testing purposes
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

int main(int argc, char **argv) 
{
    int nprocs;
    int mynod;
    int i;
    int ret;
    int fd;
    int iters;
    double time1, time2, read1, read2, host1, host2;
    char onechar;
    char host[256];

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
        fprintf(stderr, "Usage: %s <filename> <number of iterations>\n", argv[0]);
        MPI_Finalize();
        return(-1);
    }

    ret = sscanf(argv[2], "%d", &iters);
    if(ret != 1)
    {
        fprintf(stderr, "Usage: %s <filename> <number of iterations>\n", argv[0]);
        MPI_Finalize();
        return(-1);
    }

    /* first measure cost of wtime */
    time1 = MPI_Wtime();
    for(i=0; i<iters; i++)
    {
        read1 = MPI_Wtime();
    }
    time2 = MPI_Wtime();

    sleep(1);

    fd = open(argv[1], O_RDONLY);
    if(fd < 0)
    {
        perror("open");
        MPI_Finalize();
        return(-1);
    }

    /* read a single byte */
    read1 = MPI_Wtime();
    for(i=0; i<iters; i++)
    {
        ret = read(fd, &onechar, 1);
        if(ret < 0)
        {
            perror("read");
            MPI_Finalize();
            return(-1);
        }
    }
    read2 = MPI_Wtime();

    close(fd);

    sleep(1);
 
    /* get hostname */
    host1 = MPI_Wtime();
    for(i=0; i<iters; i++)
    {
        ret = gethostname(host, 256);
        if(ret < 0)
        {
            perror("gethostname");
            MPI_Finalize();
            return(-1);
        }
    }
    host2 = MPI_Wtime();

    /* print out some timing info */
    printf("#<op>\t<iters>\t<total (s)>\t<per op (s)>\n");
    printf("wtime\t%d\t%.9f\t%.9f\n", iters, time2-time1, (time2-time1)/(double)iters);
    printf("read\t%d\t%.9f\t%.9f\n", iters, read2-read1, (read2-read1)/(double)iters);
    printf("gethostname\t%d\t%.9f\t%.9f\n", iters, host2-host1, (host2-host1)/(double)iters);

    MPI_Finalize();
    return(0);
}
