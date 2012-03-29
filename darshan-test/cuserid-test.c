#define _XOPEN_SOURCE

#include <stdio.h>
#include <unistd.h>

#include <mpi.h>

int main(int argc, char **argv)
{
    int rank;
    char user_string[L_cuserid];

    MPI_Init(&argc,&argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    cuserid(user_string);

    printf("rank %d: user: %s\n", rank, user_string);

    MPI_Finalize();
    return(0);
}

