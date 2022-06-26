#include <fcntl.h>
#include <unistd.h>
#include <stdio.h>
#include <mpi.h>
#include <stdlib.h>
#include <errno.h>

int main(int argc, char** argv)
{
    int  rank;
    int  fd;
    char c[5000000];
    int  ret;
    char template[] = "testfileXXXXXX";

    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    /* create temporary file in current directory */
    fd = mkstemp(template);
    if (fd < 0) {
        perror("mkstemp");
        return (-1);
    }
    /* write bytes, to help ensure heatmap is instantiated */
    ret = write(fd, &c, 5000000);
    if (ret < 0) {
        perror("write");
        return (-1);
    }
    /* issue a bunch of zero byte reads */
    for (int i = 0; i < 5000; i++) {
        ret = read(fd, &c, 1);
        if (ret < 0) {
            perror("read");
            return (-1);
        }
    }
    /* write bytes, to help ensure heatmap is instantiated */
    ret = write(fd, &c, 5000000);
    if (ret < 0) {
        perror("write");
        return (-1);
    }

    /* close and unlink test file */
    close(fd);
    unlink(template);

    MPI_Finalize();
}
