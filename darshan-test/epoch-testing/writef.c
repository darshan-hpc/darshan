/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil ; -*- */
/*  
 *  (C) 2001 by Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
 */
#include "mpi.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <darshan-ext.h>

static void handle_error(int errcode, char *str)
{
        char msg[MPI_MAX_ERROR_STRING];
        int resultlen;
        MPI_Error_string(errcode, msg, &resultlen);
        fprintf(stderr, "%s: %s\n", str, msg);
        MPI_Abort(MPI_COMM_WORLD, 1);
}

/* The file name is taken as a command-line argument. */


int main(int argc, char **argv)
{
    int i, errcode;
    int  nprocs, len, *buf, bufcount, rank;
    MPI_File fh,fh2;
    MPI_Status status;
    double stim, write_tim, new_write_tim, write_bw;
    char *filename;

    MPI_Init(&argc,&argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &nprocs);
/* process 0 takes the file name as a command-line argument and 
   broadcasts it to other processes */
    if (!rank) {
	i = 1;
	while ((i < argc) && strcmp("-fname", *argv)) {
	    i++;
	    argv++;
	}
	if (i >= argc) {
	    fprintf(stderr, "\n*#  Usage: coll_perf -fname filename\n\n");
	    MPI_Abort(MPI_COMM_WORLD, 1);
	}
	argv++;
	len = strlen(*argv);
	filename = (char *) malloc(len+1);
	strcpy(filename, *argv);
	MPI_Bcast(&len, 1, MPI_INT, 0, MPI_COMM_WORLD);
	MPI_Bcast(filename, len+1, MPI_CHAR, 0, MPI_COMM_WORLD);
    }
    else {
	MPI_Bcast(&len, 1, MPI_INT, 0, MPI_COMM_WORLD);
	filename = (char *) malloc(len+1);
	MPI_Bcast(filename, len+1, MPI_CHAR, 0, MPI_COMM_WORLD);
    }


    bufcount = 128*128*128;
    buf = (int *) malloc(bufcount * sizeof(int));

    darshan_start_epoch();
    errcode = MPI_File_open(MPI_COMM_SELF, filename, 
                    MPI_MODE_CREATE | MPI_MODE_RDWR, MPI_INFO_NULL, &fh);
    darshan_end_epoch();


    if (errcode != MPI_SUCCESS) handle_error(errcode, "MPI_File_open(1)");

    MPI_Barrier(MPI_COMM_WORLD);
    stim = MPI_Wtime();

    darshan_start_epoch();
    MPI_File_write_all(fh, buf, bufcount, MPI_INT, &status);

    errcode = MPI_File_open(MPI_COMM_SELF, "abc", 
                    MPI_MODE_CREATE | MPI_MODE_RDWR, MPI_INFO_NULL, &fh2);
 
    MPI_File_write_all(fh2, buf, bufcount, MPI_INT, &status);

    darshan_end_epoch();
    write_tim = MPI_Wtime() - stim;

    darshan_start_epoch();
    MPI_File_write_all(fh, buf, bufcount, MPI_INT, &status);
    darshan_end_epoch();

    MPI_File_close(&fh);
    MPI_File_close(&fh2);
 

    MPI_Allreduce(&write_tim, &new_write_tim, 1, MPI_DOUBLE, MPI_MAX,
                    MPI_COMM_WORLD);

    if (rank == 0) {
      write_bw = (bufcount*sizeof(int))/(new_write_tim*1024.0*1024.0);
      fprintf(stderr, "Each of %d processes writes buf size=%ld\n",nprocs, bufcount*sizeof(int));
      fprintf(stderr, "Collective write time = %f sec, Collective write bandwidth = %f Mbytes/sec\n", new_write_tim, write_bw);
    }
       
    free(filename);
    free(buf);

    MPI_Finalize();
    return 0;
}
