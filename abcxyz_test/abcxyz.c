#include <stdio.h>
#include <fcntl.h>
#include <unistd.h>


#include <mpi.h>

#include "libabcxyz.h"


int main(int argc, char const* argv[])
{
	MPI_Init(NULL, NULL);


	// have some calls which are captured by the new instrumentation module
	foo("hello", 21);
	foo("hello", 42);
	foo("hello", 84);


	// have some posix I/O, too
	int fd = open("testfile", O_WRONLY | O_CREAT,  S_IWUSR | S_IRUSR | S_IWGRP | S_IRGRP | S_IROTH);
	write(fd, "abc", 3);
	close(fd);


	MPI_Finalize();

	return 0;
}

