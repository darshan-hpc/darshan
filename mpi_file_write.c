#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h> /* unlink() */
#include <mpi.h>

#define CHECK_ERROR(fnc) { \
    if (err != MPI_SUCCESS) { \
        int errorStringLen; \
        char errorString[MPI_MAX_ERROR_STRING]; \
        MPI_Error_string(err, errorString, &errorStringLen); \
        printf("Error at line %d when calling %s: %s\n",__LINE__,fnc,errorString); \
    } \
}

#define NELEMS 8

/*----< main() >------------------------------------------------------------*/
int main(int argc, char **argv)
{
    char buf[NELEMS];
    int i, err, rank, omode;
    MPI_Offset offset;
    MPI_Count nbytes;
    MPI_File fh;
    MPI_Status status;

    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    offset = rank * NELEMS;
    nbytes = NELEMS;
    for (i=0; i<NELEMS; i++)
        buf[i] = 'a'+rank+i;

    omode = MPI_MODE_CREATE | MPI_MODE_RDWR;

    err = MPI_File_open(MPI_COMM_WORLD, "testfile", omode, MPI_INFO_NULL, &fh);
    CHECK_ERROR("MPI_File_open")

    err = MPI_File_seek(fh, offset, MPI_SEEK_SET);
    CHECK_ERROR("MPI_File_seek")

    err = MPI_File_write(fh, buf, nbytes, MPI_BYTE, &status);
    CHECK_ERROR("MPI_File_write")

    err = MPI_File_close(&fh);
    CHECK_ERROR("MPI_File_close")

    MPI_Finalize();
    return 0;
}


