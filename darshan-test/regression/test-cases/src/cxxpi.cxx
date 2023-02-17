/* -*- Mode: C++; c-basic-offset:4 ; -*- */
/*  
 *  (C) 2004 by Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
 */

/* NOTE: This example originated from the MPICH repo:
 * (https://github.com/pmodels/mpich/blob/main/examples/cxx/cxxpi.cxx).
 * We have since modified the code to stop using MPI C++ bindings as
 * they have been deprecated and can cause compile failures.
 */

#include "mpi.h"
#include <iostream>
using namespace std;
#include <math.h>

double f(double);

double f(double a)
{
    return (4.0 / (1.0 + a*a));
}

int main(int argc,char **argv)
{
    int n, myid, numprocs, i;
    double PI25DT = 3.141592653589793238462643;
    double mypi, pi, h, sum, x;
    double startwtime = 0.0, endwtime;
    int  namelen;
    char processor_name[MPI_MAX_PROCESSOR_NAME];

    MPI_Init(&argc,&argv);
    MPI_Comm_size(MPI_COMM_WORLD, &numprocs);
    MPI_Comm_rank(MPI_COMM_WORLD, &myid);
    MPI_Get_processor_name(processor_name,&namelen);

    cout << "Process " << myid << " of " << numprocs << " is on " <<
	processor_name << endl;

    n = 10000;			/* default # of rectangles */
    if (myid == 0)
	startwtime = MPI_Wtime();

    MPI_Bcast(&n, 1, MPI_INT, 0, MPI_COMM_WORLD);

    h   = 1.0 / (double) n;
    sum = 0.0;
    /* A slightly better approach starts from large i and works back */
    for (i = myid + 1; i <= n; i += numprocs)
    {
	x = h * ((double)i - 0.5);
	sum += f(x);
    }
    mypi = h * sum;

    MPI_Reduce(&mypi, &pi, 1, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);

    if (myid == 0) {
	endwtime = MPI_Wtime();
	cout << "pi is approximately " << pi << " Error is " <<
	    fabs(pi - PI25DT) << endl;
	cout << "wall clock time = " << endwtime-startwtime << endl;
    }

    MPI_Finalize();
    return 0;
}
