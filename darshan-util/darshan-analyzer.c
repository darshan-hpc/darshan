#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <ftw.h>
#include <zlib.h>

#include "darshan-logutils.h"

#define BUCKET1 0.20
#define BUCKET2 0.40
#define BUCKET3 0.60
#define BUCKET4 0.80

char * base = NULL;

int total_single = 0;
int total_multi  = 0;
int total_mpio   = 0;
int total_pnet   = 0;
int total_hdf5   = 0;
int total_count  = 0;

int bucket1 = 0;
int bucket2 = 0;
int bucket3 = 0;
int bucket4 = 0;
int bucket5 = 0;
int fail    = 0;

int process_log(const char *fname, double *io_ratio, int *used_mpio, int *used_pnet, int *used_hdf5, int *used_multi, int *used_single)
{
    struct darshan_job job;
    struct darshan_file cp_file;
    char tmp_string[4096];
    darshan_fd zfile;
    int ret;
    int f_count;
    double total_io_time;
    double total_job_time;

    zfile = darshan_log_open(fname, "r");
    if (zfile == NULL)
    {
        fprintf(stderr, "darshan_log_open() failed to open %s\n.", fname);
        return -1;
    }

    ret = darshan_log_getjob(zfile, &job);
    if (ret < 0)
    {
        perror("darshan_log_getjob");
        fprintf(stderr, "%s\n", fname);
        darshan_log_close(zfile);
        return -1;
    }

    ret = darshan_log_getexe(zfile, tmp_string);
    if (ret < 0)
    {
        perror("darshan_log_getexe");
        fprintf(stderr, "%s\n", fname);
        darshan_log_close(zfile);
        return -1;
    }

    f_count = 0;
    total_io_time = 0.0;

    while ((ret = darshan_log_getfile(zfile, &job, &cp_file)) == 1)
    {
        f_count   += 1;

        if (cp_file.rank == -1)
            *used_single = 1;
        else
            *used_multi = 1;

        *used_mpio += cp_file.counters[CP_INDEP_OPENS];
        *used_mpio += cp_file.counters[CP_COLL_OPENS];
        *used_pnet += cp_file.counters[CP_INDEP_NC_OPENS];
        *used_pnet += cp_file.counters[CP_COLL_NC_OPENS];
        *used_hdf5 += cp_file.counters[CP_HDF5_OPENS];

        total_io_time += cp_file.fcounters[CP_F_POSIX_READ_TIME];
        total_io_time += cp_file.fcounters[CP_F_POSIX_WRITE_TIME];
        total_io_time += cp_file.fcounters[CP_F_POSIX_META_TIME];
    }

    total_job_time = (double)job.end_time - (double)job.start_time;
    if (total_job_time < 1.0)
    {
        total_job_time = 1.0;
    }

    if (f_count > 0)
    {
        *io_ratio = total_io_time/total_job_time;
    }
    else
    {
        *io_ratio = 0.0;
    }

    darshan_log_close(zfile);

    return 0;
}

int tree_walk (const char *fpath, const struct stat *sb, int typeflag)
{
    double io_ratio = 0.0;
    int used_mpio = 0;
    int used_pnet = 0;
    int used_hdf5 = 0;
    int used_multi = 0;
    int used_single = 0;

    if (typeflag != FTW_F) return 0;

    process_log(fpath,&io_ratio,&used_mpio,&used_pnet,&used_hdf5,&used_multi,&used_single);

    total_count++;

    if (used_mpio > 0) total_mpio++;
    if (used_pnet > 0) total_pnet++;
    if (used_hdf5 > 0) total_hdf5++;
    if (used_single > 0) total_single++;
    if (used_multi  > 0) total_multi++;

    if (io_ratio <= BUCKET1)
        bucket1++;
    else if ((io_ratio > BUCKET1) && (io_ratio <= BUCKET2))
        bucket2++;
    else if ((io_ratio > BUCKET2) && (io_ratio <= BUCKET3))
        bucket3++;
    else if ((io_ratio > BUCKET3) && (io_ratio <= BUCKET4))
        bucket4++;
    else if (io_ratio > BUCKET4)
        bucket5++;
    else
    {
        printf("iorat: %lf\n", io_ratio);
        fail++;
    }

    return 0;
}

int main(int argc, char **argv)
{
    int ret = 0;

    if(argc != 2)
    {
        fprintf(stderr, "Error: bad arguments.\n");
        return(-1);
    }

    base = argv[1];

    ret = ftw(base, tree_walk, 512);
    if(ret != 0)
    {
        fprintf(stderr, "Error: failed to walk path: %s\n", base);
        return(-1);
    }

    printf ("   log: %s\n", base);
    printf (" total: %d\n", total_count);
    printf ("single: %lf [%d]\n", (double)total_single/(double)total_count, total_single);
    printf (" multi: %lf [%d]\n", (double)total_multi/(double)total_count, total_multi);
    printf ("  mpio: %lf [%d]\n", (double)total_mpio/(double)total_count, total_mpio);
    printf ("  pnet: %lf [%d]\n", (double)total_pnet/(double)total_count, total_pnet);
    printf ("  hdf5: %lf [%d]\n", (double)total_hdf5/(double)total_count, total_hdf5);
    printf ("%.2lf-%.2lf: %d\n", (double)0.0,     (double)BUCKET1, bucket1);
    printf ("%.2lf-%.2lf: %d\n", (double)BUCKET1, (double)BUCKET2, bucket2);
    printf ("%.2lf-%.2lf: %d\n", (double)BUCKET2, (double)BUCKET3, bucket3);
    printf ("%.2lf-%.2lf: %d\n", (double)BUCKET3, (double)BUCKET4, bucket4);
    printf ("%.2lf-%.2lf: %d\n", (double)BUCKET4, (double)100.0,   bucket5);
    return 0;
}
