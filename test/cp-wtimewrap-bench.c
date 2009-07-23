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

extern ssize_t __real_write(int fd, const void *buf, size_t count);
extern int __real_gethostname(char *name, size_t namelen);

int __wrap_gethostname(char *name, size_t namelen)
{
    int ret;
    double time1, time2;
    double time3;

    time1 = MPI_Wtime();
    ret = __real_gethostname(name, namelen);
    time2 = MPI_Wtime();

    time3 = time2-time1;

    return(ret);
}

ssize_t __wrap_write(int fd, const void *buf, size_t count)
{
    ssize_t ret;
    double time1, time2;
    double time3;

    time1 = MPI_Wtime();
    ret = __real_write(fd, buf, count);
    time2 = MPI_Wtime();

    time3 = time2-time1;

    return(ret);
}

int main(int argc, char **argv) 
{
    int nprocs;
    int mynod;
    int i;
    int ret;
    int fd;
    int iters;
    double time1, time2, write1, write2, host1, host2;
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

    /* first measure cost of wtime */
    time1 = MPI_Wtime();
    for(i=0; i<iters; i++)
    {
        write1 = MPI_Wtime();
    }
    time2 = MPI_Wtime();

    sleep(1);

    /* open dev null */
    fd = open("/dev/null", O_RDWR);
    if(fd < 0)
    {
        perror("open");
        MPI_Finalize();
        return(-1);
    }

    /* write a single byte */
    write1 = MPI_Wtime();
    for(i=0; i<iters; i++)
    {
        ret = write(fd, &onechar, 1);
        if(ret < 0)
        {
            perror("write");
            MPI_Finalize();
            return(-1);
        }
    }
    write2 = MPI_Wtime();

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
    printf("write\t%d\t%.9f\t%.9f\n", iters, write2-write1, (write2-write1)/(double)iters);
    printf("gethostname\t%d\t%.9f\t%.9f\n", iters, host2-host1, (host2-host1)/(double)iters);

    MPI_Finalize();
    return(0);
}
