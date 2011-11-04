/*
 *  (C) 2010 by Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
 */

/*
 * Code to exercise various darshan counters
 *
 * Build with darshan enabled mpicc.
 *
 * mpicc -static -o io-sample io-sample.c -lm
 * mpirun -n 4 ./io-sample -p </fs1/path1:/fs2/path2> -b <#bytes_per_rank>
 *
 * need to provide read_only.n files in /fs/path dirs.
 * dd if=/dev/zero of=read_only.0 bs=n*bytes_per_rank count=1
 */
#define _XOPEN_SOURCE 500

#include <stdlib.h>
#include <stdio.h>
#include <time.h>
#include <unistd.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <mpi.h>
#include <math.h>


#define MPI_CHECK(r,f) { if(r != MPI_SUCCESS) { printf("%s: (%d) failed: %d\n", f, __LINE__, r); } }
#define POSIX_CHECK(r,f) { if (r == -1) { printf("%s: (%d) failed: %d\n", f, __LINE__, r); } }

/*
 * Test Case should return 1 for success and 0 for failure
 */
struct test_case
{
    char* name;
    int (*func)(char*,int,int);
};

/*
 * Prototypes for Tests
 */
int header(char*,int,int);
int mpi_io_shared(char*,int,int);
int posix_unique(char*,int,int);
int read_write_only (char*,int,int);
int sweep_access_size(char *,int,int);
int posix_ops(char *path, int size, int rank);
int posix_consecutive(char *path, int size, int rank);

/*
 * List of tests
 */
struct test_case tests[] =
{
    {"header", header},
    {"mpi-io-shared", mpi_io_shared},
    {"posix-unique", posix_unique},
    {"read_write_only", read_write_only},
    {"sweep_acces_size", sweep_access_size},
    {"posix_ops", posix_ops},
    {"posix_consecutive", posix_consecutive},
    {NULL, NULL}
};

int BYTES_PER_RANK = (1024*1024*1);

int main(int argc, char **argv)
{
    struct test_case *tc;
    int rc;
    int rank;
    int size;
    int path_count = 0;
    int opt;
    int verbose = 0;
    int i;
    char *p;
    char *path[4] = { "/tmp", NULL, NULL, NULL };

    while((opt = getopt(argc, argv, "b:p:v")) != -1)
    {
        switch(opt)
        {
        case 'p':
            path[path_count] = optarg;
            for (p=optarg;((p!=NULL) && (*p!=0));p++)
            {
                if (*p == ':')
                {
                    *p = 0;
                    path_count++;
                    path[path_count] = p+1;
                }
            }
            break;
        case 'b':
            BYTES_PER_RANK = atoi(optarg);
            break;
        case 'v':
            verbose = 1;
            break;
        }
    }
    path_count++;

    rc = MPI_Init(&argc, &argv);
    MPI_CHECK(rc,"MPI_Init");

    rc = MPI_Comm_size(MPI_COMM_WORLD, &size);
    MPI_CHECK(rc,"MPI_Comm_size");

    rc = MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_CHECK(rc,"MPI_Comm_rank");

    if ((verbose) && (rank == 0))
    {
        printf("bytes_per_rank: %d\n", BYTES_PER_RANK);
        for (i=0; i < path_count; i++)
        { printf("path[%d]: %s\n", i, path[i]); }
    }

    for (tc=&tests[0];tc->func!=NULL;tc++)
    {
        int trc = 0;
        int all = 0;

        trc = tc->func(path[rand()%path_count],size, rank);
        rc = MPI_Reduce(&trc,&all,1,MPI_INT,
                        MPI_LAND,0,MPI_COMM_WORLD);
        if (rank == 0)
        {
            printf("%s: %s\n", tc->name, (all?"passed":"failed"));
        }
    }

    rc = MPI_Finalize();
    MPI_CHECK(rc,"MPI_Finalize");

    return 0;
}

/*
 * header
 *
 * prints out a header for the test run
 */
int header (char* path, int size, int rank)
{
    if (rank == 0)
    {
        char timestr[256];
        time_t now;

        now = time(NULL);
        (void) strftime(timestr, sizeof(timestr), "%F %T", localtime(&now));

        printf("Darshan Test Run\n");
        printf("date: %s\n", timestr);
        printf(" uid: %d\n", getuid());
        printf("path: %s\n", path);
    }

    return 1;
}

/*
 * mpi_io_shared
 *
 * creates a single-shared-file
 * writes with independent-io
 * reads with independent-io
 * writes with collective-io
 * reads with collective-io
 */
int mpi_io_shared (char *path, int size, int rank)
{
    MPI_File fh;
    char filepath[512];
    MPI_Offset offset;
    MPI_Status status;
    void *buf;
    int bufcount = BYTES_PER_RANK;
    int rc;

    buf = malloc(bufcount);
    if (!buf) { return 0; }

    memset(buf, 0xa, bufcount);

    sprintf(filepath, "%s/%s", path, "cp-bench-mpio-shared");
    rc = MPI_File_open(MPI_COMM_WORLD,
                       filepath,
                       (MPI_MODE_CREATE|MPI_MODE_RDWR|MPI_MODE_DELETE_ON_CLOSE),
                       MPI_INFO_NULL,
                       &fh);
    MPI_CHECK(rc,"MPI_File_open");

    /* Indep Write */
    offset = rank * bufcount;
    rc = MPI_File_write_at(fh,offset,buf,bufcount,MPI_BYTE,&status);
    MPI_CHECK(rc,"MPI_File_write_at");

    MPI_Barrier(MPI_COMM_WORLD);

    /* Indep Read */
    offset = ((rank+1)%size) * bufcount;
    rc = MPI_File_read_at(fh,offset,buf,bufcount,MPI_BYTE,&status);
    MPI_CHECK(rc,"MPI_File_read_at");

    /* Collective Write */
    offset = rank * bufcount;
    rc = MPI_File_write_at_all(fh, offset, buf, bufcount, MPI_BYTE, &status);
    MPI_CHECK(rc,"MPI_File_write_at_all");

    /* Collective Read */
    offset = ((rank+1)%size) * bufcount;
    rc = MPI_File_read_at_all(fh, offset, buf, bufcount, MPI_BYTE, &status);
    MPI_CHECK(rc,"MPI_File_read_at_all");

    rc = MPI_File_close(&fh);
    MPI_CHECK(rc,"MPI_File_close");

    free(buf);

    return 1;
}

/*
 * posix_unique
 *
 * create file-per-process
 * posix read
 * posix write
 * posix stat
 */
int posix_unique (char* path, int size, int rank)
{
    char filepath[512];
    void *buf;
    int bufcount = BYTES_PER_RANK;
    int rc;
    int fd;
    int res = 1;
    struct stat statb;
    off_t off;

    buf = malloc(bufcount);
    if (!buf) { return 0; }

    memset(buf, 0xa, bufcount);

    sprintf(filepath, "%s/%s.%d", path, "posix-unique", rank);

    fd = open(filepath, (O_CREAT|O_RDWR), 0644);

    rc = write(fd, buf, bufcount);
    POSIX_CHECK(rc,"write");

    rc = fstat(fd, &statb);
    POSIX_CHECK(rc,"fstat");

    off = lseek(fd, 0, SEEK_SET);
    POSIX_CHECK((int)off,"lseek");

    rc = read(fd, buf, bufcount);
    POSIX_CHECK(rc,"read");

    close(fd);

    unlink(filepath);

    free(buf);

    return res;
}

/*
 * read_write_only
 *
 * create a file for write
 *   write data to it
 * open a file for read
 *   files must be prepared ahead of time
 *   read from it
 *
 * posix unique files
 */
int read_write_only (char *path, int size, int rank)
{
    int fd;
    int rc;
    int bufcount = BYTES_PER_RANK;
    void* buf;
    off_t off;
    char filepath[512];

    buf = malloc(bufcount);
    if (!buf) { return 0; }

    memset(buf, 0xa, bufcount);

    /*
     * Write Only
     */
    sprintf(filepath, "%s/%s.%d", path, "write_only", rank);
    fd = open(filepath, (O_CREAT|O_WRONLY|O_TRUNC), 0644);
    POSIX_CHECK(fd,"open");

    off = lseek(fd, BYTES_PER_RANK, SEEK_SET);
    POSIX_CHECK((int)off,"lseek");

    rc = write(fd, buf, bufcount);
    POSIX_CHECK(rc,"write");

    close(fd);
    unlink(filepath);

    /*
     * Read Only
     */
    sprintf(filepath, "%s/%s.%d", path, "read_only", rank);
    fd = open(filepath, (O_RDONLY));
    POSIX_CHECK(fd,"open");

    rc = read(fd, buf, bufcount);
    POSIX_CHECK(rc,"read");

    close(fd);

    free(buf);

    return rc;
}

/*
 * sweep_access_size
 *
 * read and write from file using various access sizes that
 * fall into the 10 buckets.
 *
 * mpi-io
 * mpi contiguous data type
 * shared file
 */
int sweep_access_size(char *path, int size, int rank)
{
    MPI_File fh;
    MPI_Offset offset;
    MPI_Datatype dt;
    MPI_Status status;
    int rc;
    int blksize;
    int pblksize;
    int count;
    void* buf;
    char filepath[512];

    sprintf(filepath, "%s/%s", path, "access_sweep");

    rc = MPI_File_open(MPI_COMM_WORLD, filepath,
                   (MPI_MODE_CREATE|MPI_MODE_RDWR|MPI_MODE_DELETE_ON_CLOSE),
                       MPI_INFO_NULL,
                       &fh);
    MPI_CHECK(rc,"MPI_File_open");

    for (pblksize=0,count=6,blksize=(((int)pow(2,count))+1);
         count <= 30;
         count++,pblksize=blksize,blksize=(((int)pow(2,count))+1))
    {
        int test;

        rc = MPI_Type_contiguous(blksize, MPI_BYTE, &dt);
        MPI_CHECK(rc,"MPI_Type_contiguous");

        rc = MPI_Type_commit(&dt);
        MPI_CHECK(rc,"MPI_Type_commit");

        buf = malloc(blksize);
        if (!buf) { return 0; }

        offset = ((MPI_Offset)pblksize*(MPI_Offset)size)+
                 ((MPI_Offset)rank*(MPI_Offset)blksize);

        rc = MPI_File_write_at(fh, offset, buf, 1, dt, &status);
        MPI_CHECK(rc,"MPI_File_write_at");

        MPI_Barrier(MPI_COMM_WORLD);

        offset = ((MPI_Offset)pblksize*(MPI_Offset)size)+
                 ((MPI_Offset)((rank+1)%size)*(MPI_Offset)blksize);

        rc = MPI_File_read_at(fh, offset, buf, 1, dt, &status);
        MPI_CHECK(rc,"MPI_File_read_at");

        rc = MPI_Type_free(&dt);
        MPI_CHECK(rc,"MPI_Type_free");

        free(buf);
    }

    rc = MPI_File_close(&fh);
    MPI_CHECK(rc,"MPI_File_close");

    return 1; 
}

/*
 * posix_ops
 *
 * Perform each posix operation that darshan tracks
 *
 * open
 * seek
 * stat
 * sync
 * mmap
 * close
 */
int posix_ops(char *path, int size, int rank)
{
    int fd;
    int rc;
    void *addr;
    struct stat statb;
    char filepath[512];

    sprintf(filepath, "%s/%s.%d", path, "op_test", rank);

    fd = open(filepath, (O_CREAT|O_RDWR|O_TRUNC), 0644);
    POSIX_CHECK(fd,"open");

    rc = lseek(fd, 1000, SEEK_SET);
    POSIX_CHECK(fd,"lseek");

    rc = fstat(fd, &statb);
    POSIX_CHECK(rc,"fstat");

    rc = fsync(fd);
    POSIX_CHECK(rc,"fsync");

    addr = mmap(NULL, 100, PROT_READ, MAP_PRIVATE, fd, 0);
    if (addr == NULL) { printf("mmap failed\n"); }

    rc = munmap(addr, 100);
    POSIX_CHECK(rc,"munmap");

    close(fd);

    unlink(filepath);

    return 1;
}

/*
 * posix_consecutive
 *
 * generate both read and write consecutive accesses
 *
 * posix unique file
 */
int posix_consecutive(char *path, int size, int rank)
{
    int fd;
    int rc;
    int i;
    char filepath[512];
    char buf[1024];

    sprintf(filepath, "%s/%s.%d", path, "consecutive_test", rank);

    fd = open(filepath, (O_CREAT|O_RDWR|O_TRUNC), 0644);
    POSIX_CHECK(fd,"open");

    for (i=0; i < 10; i++)
    {
        rc = write(fd, buf, sizeof(buf));
        POSIX_CHECK(fd,"write");
    }

    for (i=0; i< 10; i++)
    {
        rc = read(fd, buf, sizeof(buf));
        POSIX_CHECK(fd,"read");
    }
  
    close(fd);

    unlink(filepath);

    return 1;
}
