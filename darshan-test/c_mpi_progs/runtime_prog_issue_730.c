#include <fcntl.h>
#include <unistd.h>
#include <stdio.h>
#include <mpi.h>
#include <stdlib.h>


int main(int argc, char **argv)
{
   int rank;
   int fd;
   char c;
   
   MPI_Init(&argc, &argv);
   MPI_Comm_rank(MPI_COMM_WORLD, &rank);

   fd = open("test.txt", O_RDONLY);
   read(fd, &c, 1);
   for(int i = 0; i < 5000; i++) {
       read(fd, &c, 0);
   }
   MPI_Finalize();
}
