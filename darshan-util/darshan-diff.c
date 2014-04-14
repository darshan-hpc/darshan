/*
 *  (C) 2009 by Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
 */

#include <stdio.h>
#include <zlib.h>
#include <string.h>
#include "darshan-log-format.h"
#include "darshan-logutils.h"


/* utility functions just for darshan-diff */

static void cd_print_str(char * prefix, char * arg1, char *arg2)
{
    printf("- %s %s\n", prefix, arg1);
    printf("+ %s %s\n", prefix, arg2);
}
static void cd_print_int(char * prefix, int arg1, int arg2)
{
    printf("- %s %d\n", prefix, arg1);
    printf("+ %s %d\n", prefix, arg2);
}
static void cd_print_int64(char * prefix, int64_t arg1, int64_t arg2)
{
    printf("- %s %" PRId64 "\n", prefix, arg1);
    printf("+ %s %" PRId64 "\n", prefix, arg2);
}


int main(int argc, char ** argv)
{
    darshan_fd file1, file2;
    struct darshan_job job1, job2;
    struct darshan_file cp_file1, cp_file2;
    char exe1[4096], exe2[4096];
    int i, ret1,ret2;

    if (argc != 3)
    {
        fprintf(stderr, "Usage: %s <file1> <file2>\n", argv[0]);
        return(-1);
    }

    file1 = darshan_log_open(argv[1], "r");
    if(!file1) {
        fprintf(stderr, "darshan_log_open() failed to open %s\n.", argv[1]);
        return(-1);
    }
    file2 = darshan_log_open(argv[2], "r");
    if(!file2) {
        fprintf(stderr, "darshan_log_open() failed to open %s\n.", argv[2]);
        return(-1);
    }

    if (darshan_log_getjob(file1, &job1))
    {
        darshan_log_close(file1);
        return(-1);
    }
    if (darshan_log_getjob(file2, &job2))
    {
        darshan_log_close(file2);
        return(-1);
    }

    if (darshan_log_getexe(file1, exe1))
    {
        darshan_log_close(file1);
        return(-1);
    }
    if (darshan_log_getexe(file2, exe2))
    {
        darshan_log_close(file2);
        return(-1);
    }

    if (strcmp(exe1, exe2)) 
        cd_print_str("# exe: ", exe1, exe2);

    if (job1.uid != job2.uid)
        cd_print_int("# uid:", job1.uid, job2.uid);
    if (job1.start_time != job2.start_time)
        cd_print_int64("# start_time:", 
                (int64_t)job1.start_time, (int64_t)job2.start_time);
    if (job1.end_time!= job2.end_time)
        cd_print_int64("# end_time:", 
                (int64_t)job1.end_time,(int64_t)job2.end_time);
    if (job1.nprocs!= job2.nprocs)
        cd_print_int("# nprocs:", job1.nprocs, job2.nprocs);
    if ((job1.end_time-job1.start_time) != (job2.end_time - job2.start_time))
        cd_print_int64("# run time:", 
                (int64_t)(job1.end_time - job1.start_time +1),
                (int64_t)(job2.end_time - job2.start_time + 1));

    /* if for some reason no files were accessed, then we'll have to fix-up the
     * buffers in the while loop */

    do {
        ret1 = darshan_log_getfile(file1, &job1, &cp_file1);
	if (ret1 < 0) 
	{
		perror("darshan_log_getfile");
		darshan_log_close(file1);
		return(-1);
	}
        ret2 = darshan_log_getfile(file2, &job2, &cp_file2);
	if (ret2 < 0) 
	{
		perror("darshan_log_getfile");
		darshan_log_close(file2);
		return(-1);
	}

        for(i=0; i<CP_NUM_INDICES; i++) {
            if (cp_file1.counters[i] != cp_file2.counters[i]) {
		printf("- ");
		printf("%" PRId64 "\t%" PRIu64 "\t%s\t%" PRId64 "\t...%s\n",
			cp_file1.rank, cp_file1.hash, darshan_names[i], 
			cp_file1.counters[i], cp_file1.name_suffix);
		printf("+ ");
		printf("%" PRId64 "\t%" PRIu64 "\t%s\t%" PRId64 "\t...%s\n",
			cp_file2.rank, cp_file2.hash, darshan_names[i], 
			cp_file2.counters[i], cp_file2.name_suffix);
            }
        }
        for(i=0; i<CP_F_NUM_INDICES; i++) {
            if (cp_file1.fcounters[i] != cp_file2.fcounters[i]) {
		printf("- ");
		printf("%" PRId64 "\t%" PRIu64 "\t%s\t%f\t...%s\n",
			cp_file1.rank, cp_file1.hash, darshan_f_names[i], 
			cp_file1.fcounters[i], cp_file1.name_suffix);
		printf("+ ");
		printf("%" PRId64 "\t%" PRIu64 "\t%s\t%f\t...%s\n",
			cp_file2.rank, cp_file2.hash, darshan_f_names[i], 
			cp_file2.fcounters[i], cp_file2.name_suffix);
            }
        }


    } while (ret1 == 1 || ret2 == 1);


    darshan_log_close(file1);
    darshan_log_close(file2);
    return(0);
}

/*
 * Local variables:
 *  c-indent-level: 4
 *  c-basic-offset: 4
 * End:
 *
 * vim: ts=8 sts=4 sw=4 expandtab
 */
