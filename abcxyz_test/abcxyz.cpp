#include <mpi.h>
#include <stdio.h>

#include "mycpplib.hpp"


int main(int argc, char const* argv[])
{
	MPI_Init(NULL, NULL);

	X x;
	x.fn1("abc", 42);
	x.fn2();

	MPI_Finalize();

	return 0;
}
