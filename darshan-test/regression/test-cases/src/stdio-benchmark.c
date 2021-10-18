/*
 *  (C) 2021 by Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
 */

/* The purpose of this test is to measure overhead for a large number of
 * sequential stdio operations (specifically: fprintf(), fscanf(), and
 * fread()).
 *
 * The command line argumens specify a file name (which will be created), a
 * number of iterations, and an access size.
 *
 * Small sequential stdio operations are low latency in most cases (due to
 * libc buffering and/or kernel buffering); this benchmark is therefore a
 * good measure of wrapper overhead that might otherwise be difficult to
 * observe in relation to I/O operation cost.
 */

#include <stdio.h>
#include <stdlib.h>
#include <mpi.h>
#include <assert.h>
#include <unistd.h>

int main(int argc, char* argv[])
{
    int           npes, myrank;
    long unsigned iters;
    long unsigned i;
    int           access_size;
    int           ret;
    char*         buffer;
    char          format[64] = "";
    double        t1, t2;

    MPI_Init(&argc, &argv);

    if (argc != 4) {
        fprintf(stderr,
                "Usage: fscanf-bench <filename> <iters> <access_size>\n");
        fprintf(stderr, "       (note: filename will be created at runtime)\n");
        return (-1);
    }

    ret = sscanf(argv[2], "%lu", &iters);
    if (ret != 1) {
        fprintf(stderr,
                "Usage: fscanf-bench <filename> <iters> <access_size>\n");
        fprintf(stderr, "       (note: filename will be created at runtime)\n");
        return (-1);
    }
    ret = sscanf(argv[3], "%d", &access_size);
    if (ret != 1) {
        fprintf(stderr,
                "Usage: fscanf-bench <filename> <iters> <access_size>\n");
        fprintf(stderr, "       (note: filename will be created at runtime)\n");
        return (-1);
    }

    MPI_Comm_size(MPI_COMM_WORLD, &npes);
    MPI_Comm_rank(MPI_COMM_WORLD, &myrank);

    if (npes != 1) {
        fprintf(stderr, "Error: one rank only please.\n");
        return (-1);
    }

    FILE* fp = fopen(argv[1], "w+");
    if (!fp) {
        perror("fopen");
        return (-1);
    }

    buffer = calloc(access_size + 1, 1);
    assert(buffer);
    for (i = 0; i < access_size; i++) buffer[i] = 'A';

    t1 = MPI_Wtime();
    for (i = 0; i < iters; i++) {
        ret = fprintf(fp, "%s", buffer);
        assert(ret == access_size);
    }
    t2 = MPI_Wtime();
    printf("%lu fprintf()s of size %d each in %f seconds (%f ops/s)\n", iters,
           access_size, t2 - t1, ((double)iters) / (t2 - t1));

    rewind(fp);

    t1 = MPI_Wtime();
    for (i = 0; i < iters; i++) {
        sprintf(format, "%%%ds", access_size);
        ret = fscanf(fp, format, buffer);
        assert(ret == 1);
    }
    t2 = MPI_Wtime();
    printf("%lu fscanf()s of size %d each in %f seconds (%f ops/s)\n", iters,
           access_size, t2 - t1, ((double)iters) / (t2 - t1));

    rewind(fp);

    t1 = MPI_Wtime();
    for (i = 0; i < iters; i++) {
        ret = fread(buffer, 1, access_size, fp);
        assert(ret == access_size);
    }
    t2 = MPI_Wtime();
    printf("%lu fread()s of size %d each in %f seconds (%f ops/s)\n", iters,
           access_size, t2 - t1, ((double)iters) / (t2 - t1));

    fclose(fp);
    unlink(argv[1]);

    MPI_Finalize();

    return 0;
}
