#ifdef HAVE_CONFIG_H
#include <darshan-runtime-config.h> /* output of 'configure' */
#endif

#include <mpi.h>

int main(int argc, char **argv)
{
    MPI_Init(&argc, &argv);

    MPI_Finalize();
    return 0;
}


