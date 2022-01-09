#define _GNU_SOURCE			// asprintf

#include <mpi.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <time.h>





int main(int argc, char** argv)
{
	int i;
	int ret = 0;

    // Get the number of processes
    int world_size;
    int world_rank;
    char processor_name[MPI_MAX_PROCESSOR_NAME];
    int name_len;

    // Initialize the MPI environment
    MPI_Init(NULL, NULL);
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);
    MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);
    MPI_Get_processor_name(processor_name, &name_len);


    // Print off a hello world message
    printf("Hello world from processor %s, rank %d"
           " out of %d processors\n",
           processor_name, world_rank, world_size);


    char *filename = "A";
    if (argc > 1) filename = argv[1];

	
	int size = 10000;
	int opcount = 10;
	
    char* buf = (char*) malloc(sizeof(char) * size);
#ifdef WRITE 
    for (i = 0; i < size; i++) {
        buf[i] = filename[0];
    }
#endif




	// write to non existing file
	int fd = open(filename, O_RDWR | O_CREAT, S_IWUSR | S_IRUSR | S_IWGRP | S_IRGRP | S_IROTH);
	if ( fd != -1 )
	{

		for (i = 0; i < opcount; i++)
		{
		    int pos =  (size/opcount) * i;
            int len = size/opcount;
#ifdef WRITE
            printf("write(from_pos=%d, len=%d)\n", pos, len);
			ret = write(fd, buf+pos, len);
#endif
#ifdef READ 
            printf("read(to_pos=%d, len=%d)\n", pos, len);
			ret = read(fd, buf+pos, len);
#endif
		}
		ret = close(fd);
	}


    for (i = 0; i < size; i++) 
    {
        printf("%c", buf[i]);
    }
    printf("\n");


	MPI_Finalize();
}

