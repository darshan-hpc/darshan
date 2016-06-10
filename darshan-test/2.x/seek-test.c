/*
 *  (C) 2009 by Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
 */

/* Test program to excercise both lseek and pwrite() for large and small
 * files
 */

/* Argument: path to place four sparse files: lseek-small.dat,
 * pwrite-small.dat, lseek-big.dat, and pwrite-big.dat.
 */

#define _XOPEN_SOURCE 500
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>

#include <mpi.h>

int main(int argc, char **argv) 
{
    int nprocs;
    int mynod;
    int ret;
    int fd;
    char path_lseek_small[256];
    char path_lseek_big[256];
    char path_pwrite_small[256];
    char path_pwrite_big[256];
    off_t offset;
    off_t lseek_ret;
    char data;
    ssize_t pwrite_ret;

    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &mynod);
    MPI_Comm_size(MPI_COMM_WORLD, &nprocs);

    /* we only want one proc */
    if(nprocs > 1)
    {
        if(mynod == 0)
        {
            fprintf(stderr, "Error: this test should be run with exactly one process.\n");
        }
        MPI_Finalize();
        return(-1);
    }

    if(argc != 2)
    {
        fprintf(stderr, "Usage: %s <path>\n", argv[0]);
        MPI_Finalize();
        return(-1);
    }

    /* generate name of target files */
    sprintf(path_lseek_small, "%s/lseek-small.dat", argv[1]);
    sprintf(path_lseek_big, "%s/lseek-big.dat", argv[1]);
    sprintf(path_pwrite_small, "%s/pwrite-small.dat", argv[1]);
    sprintf(path_pwrite_big, "%s/pwrite-big.dat", argv[1]);

    /* lseek and write on small file */
    fd = open(path_lseek_small, O_CREAT|O_EXCL|O_WRONLY, S_IRUSR|S_IWUSR);
    if(fd < 0)
    {
        perror("open");
        fprintf(stderr, "%s\n", path_lseek_small);
        MPI_Finalize();
        return(-1);
    }

    offset = 4095;
    lseek_ret = lseek(fd, offset, SEEK_SET);    
    if(lseek_ret != offset)
    {
        perror("lseek");
        fprintf(stderr, "%s\n", path_lseek_small);
        MPI_Finalize();
        return(-1);
    }

    ret = write(fd, &data, 1);
    if(ret < 0)
    {
        perror("write");
        fprintf(stderr, "%s\n", path_lseek_small);
        MPI_Finalize();
        return(-1);
    }
    close(fd);
 
    /* lseek and write on big file */
    fd = open(path_lseek_big, O_CREAT|O_EXCL|O_WRONLY, S_IRUSR|S_IWUSR);
    if(fd < 0)
    {
        perror("open");
        fprintf(stderr, "%s\n", path_lseek_big);
        MPI_Finalize();
        return(-1);
    }

    offset = 1024;
    offset *= 1024;
    offset *= 1024;
    offset *= 3;
    offset -= 1;
    lseek_ret = lseek(fd, offset, SEEK_SET);    
    if(lseek_ret != offset)
    {
        perror("lseek");
        fprintf(stderr, "%s\n", path_lseek_big);
        MPI_Finalize();
        return(-1);
    }

    ret = write(fd, &data, 1);
    if(ret < 0)
    {
        perror("write");
        fprintf(stderr, "%s\n", path_lseek_big);
        MPI_Finalize();
        return(-1);
    }
    
    close(fd);

    /* pwrite on small file */
    fd = open(path_pwrite_small, O_CREAT|O_EXCL|O_WRONLY, S_IRUSR|S_IWUSR);
    if(fd < 0)
    {
        perror("open");
        fprintf(stderr, "%s\n", path_pwrite_small);
        MPI_Finalize();
        return(-1);
    }

    offset = 4095;
    pwrite_ret = pwrite(fd, &data, 1, offset);    
    if(pwrite_ret != 1)
    {
        perror("pwrite");
        fprintf(stderr, "%s\n", path_pwrite_small);
        MPI_Finalize();
        return(-1);
    }
    close(fd);
 
    /* pwrite and write on big file */
    fd = open(path_pwrite_big, O_CREAT|O_EXCL|O_WRONLY, S_IRUSR|S_IWUSR);
    if(fd < 0)
    {
        perror("open");
        fprintf(stderr, "%s\n", path_pwrite_big);
        MPI_Finalize();
        return(-1);
    }

    offset = 1024;
    offset *= 1024;
    offset *= 1024;
    offset *= 3;
    offset -= 1;
    pwrite_ret = pwrite(fd, &data, 1, offset);    
    if(pwrite_ret != 1)
    {
        perror("pwrite");
        fprintf(stderr, "%s\n", path_pwrite_big);
        MPI_Finalize();
        return(-1);
    }
    
    close(fd);

    MPI_Finalize();
    return(0);
}
