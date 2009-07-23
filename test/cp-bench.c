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
    int fd[8];
    char path[256];
    int iters;
    double open1, open2, stat1, stat2, read1, read2, write1, write2;
    double readmult1, readmult2, writemult1, writemult2, statmult1, statmult2;
    struct stat statbuf;
    char onechar;

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

    /* create 8 symlinks to /dev/zero so that we can operate on what appears
     * to be 8 independent files to Chutzpah
     */
    for(i=0; i<8; i++)
    {
        sprintf(path, "/tmp/test%d", i);
        ret = symlink("/dev/zero", path);
        if(ret < 0)
        {
            perror("symlink");
            MPI_Finalize();
            return(-1);
        }
    }

    /* open and close a single file */
    open1 = MPI_Wtime();
    for(i=0; i<iters; i++)
    {
        fd[0] = open("/tmp/test0", O_RDWR);
        if(fd[0] < 0)
        {
            perror("open");
            MPI_Finalize();
            return(-1);
        }

        close(fd[0]);
    }
    open2 = MPI_Wtime();

    /* reopen for testing */
    fd[0] = open("/tmp/test0", O_RDWR);
    if(fd[0] < 0)
    {
        perror("open");
        MPI_Finalize();
        return(-1);
    }

    /* stat a single file */
    stat1 = MPI_Wtime();
    for(i=0; i<iters; i++)
    {
        ret = fstat(fd[0], &statbuf);
        if(ret < 0)
        {
            perror("fstat");
            MPI_Finalize();
            return(-1);
        }
    }
    stat2 = MPI_Wtime();
 
    /* read a single file */
    read1 = MPI_Wtime();
    for(i=0; i<iters; i++)
    {
        ret = read(fd[0], &onechar, 1);
        if(ret < 0)
        {
            perror("read");
            MPI_Finalize();
            return(-1);
        }
    }
    read2 = MPI_Wtime();
  
    /* write a single file */
    write1 = MPI_Wtime();
    for(i=0; i<iters; i++)
    {
        ret = write(fd[0], &onechar, 1);
        if(ret < 0)
        {
            perror("write");
            MPI_Finalize();
            return(-1);
        }
    }
    write2 = MPI_Wtime();

    /* now open remaining files */
    for(i=1; i<8; i++)
    {
        sprintf(path, "/tmp/test%d", i);
        fd[i] = open(path, O_RDWR);
        if(fd[i] < 0)
        {
            perror("open");
            MPI_Finalize();
            return(-1);
        }

    }

    /* repeat stat test with multiple files open */
    statmult1 = MPI_Wtime();
    for(i=0; i<iters; i++)
    {
        ret = fstat(fd[i%8], &statbuf);
        if(ret < 0)
        {
            perror("fstat");
            MPI_Finalize();
            return(-1);
        }
    }
    statmult2 = MPI_Wtime();

    /* repeat read test with multiple files open */
    readmult1 = MPI_Wtime();
    for(i=0; i<iters; i++)
    {
        ret = read(fd[i%8], &onechar, 1);
        if(ret < 0)
        {
            perror("read");
            MPI_Finalize();
            return(-1);
        }
    }
    readmult2 = MPI_Wtime();
 
    /* repeat write test with multiple files open */
    writemult1 = MPI_Wtime();
    for(i=0; i<iters; i++)
    {
        ret = write(fd[i%8], &onechar, 1);
        if(ret < 0)
        {
            perror("write");
            MPI_Finalize();
            return(-1);
        }
    }
    writemult2 = MPI_Wtime();
 
    for(i=1; i<8; i++)
    {
        close(fd[i]);
    }

    /* clean up symlinks */
    for(i=0; i<8; i++)
    {
        sprintf(path, "/tmp/test%d", i);
        ret = unlink(path);
        if(ret < 0)
        {
            perror("unlink");
            MPI_Finalize();
            return(-1);
        }
    }

    /* print out some timing info */
    printf("#<op>\t<iters>\t<total (s)>\t<per op (s)>\n");
    printf("open\t%d\t%.9f\t%.9f\n", iters, open2-open1, (open2-open1)/(double)iters);
    printf("stat\t%d\t%.9f\t%.9f\n", iters, stat2-stat1, (stat2-stat1)/(double)iters);
    printf("read\t%d\t%.9f\t%.9f\n", iters, read2-read1, (read2-read1)/(double)iters);
    printf("write\t%d\t%.9f\t%.9f\n", iters, write2-write1, (write2-write1)/(double)iters);
    printf("statmult\t%d\t%.9f\t%.9f\n", iters, statmult2-statmult1, (statmult2-statmult1)/(double)iters);
    printf("readmult\t%d\t%.9f\t%.9f\n", iters, readmult2-readmult1, (readmult2-readmult1)/(double)iters);
    printf("writemult\t%d\t%.9f\t%.9f\n", iters, writemult2-writemult1, (writemult2-writemult1)/(double)iters);

    MPI_Finalize();
    return(0);
}
