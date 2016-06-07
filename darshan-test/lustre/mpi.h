/*
 *  VERY primitive stubs that allow darshan.h to be included in non-MPI
 *  applications like darshan-tester
 */
typedef int MPI_Comm;
typedef int MPI_Datatype;
typedef int MPI_Op;
typedef long MPI_Aint;
unsigned char MPI_BYTE;
#define MPI_COMM_WORLD 0
