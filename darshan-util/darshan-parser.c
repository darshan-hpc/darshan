/*
 *  (C) 2009 by Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
 */

#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <zlib.h>
#include <time.h>
#include <stdlib.h>
#include <getopt.h>
#include <assert.h>

#include "darshan-logutils.h"

#include "uthash-1.9.2/src/uthash.h"

/*
 * Options
 */
#define OPTION_BASE  (1 << 0)  /* darshan log fields */
#define OPTION_TOTAL (1 << 1)  /* aggregated fields */
#define OPTION_PERF  (1 << 2)  /* derived performance */
#define OPTION_FILE  (1 << 3)  /* file count totals */
#define OPTION_FILE_LIST  (1 << 4)  /* per-file summaries */
#define OPTION_FILE_LIST_DETAILED  (1 << 6)  /* per-file summaries with extra detail */
#define OPTION_ALL (\
  OPTION_BASE|\
  OPTION_TOTAL|\
  OPTION_PERF|\
  OPTION_FILE|\
  OPTION_FILE_LIST|\
  OPTION_FILE_LIST_DETAILED)

#define FILETYPE_SHARED (1 << 0)
#define FILETYPE_UNIQUE (1 << 1)
#define FILETYPE_PARTSHARED (1 << 2)

#define max(a,b) (((a) > (b)) ? (a) : (b))
#define max3(a,b,c) (((a) > (b)) ? (((a) > (c)) ? (a) : (c)) : (((b) > (c)) ? (b) : (c)))

/*
 * Datatypes
 */
typedef struct hash_entry_s
{
    UT_hash_handle hlink;
    uint64_t hash;
    int64_t type;
    int64_t procs;
    int64_t counters[CP_NUM_INDICES];
    double  fcounters[CP_F_NUM_INDICES];
    double cumul_time;
    double meta_time;
    double slowest_time;
    char name_suffix[CP_NAME_SUFFIX_LEN+1];
} hash_entry_t;

typedef struct perf_data_s
{
    int64_t total_bytes;
    double slowest_rank_time;
    double slowest_rank_meta_time;
    double shared_time_by_cumul;
    double shared_time_by_open;
    double shared_time_by_open_lastio;
    double shared_time_by_slowest;
    double shared_meta_time;
    double agg_perf_by_cumul;
    double agg_perf_by_open;
    double agg_perf_by_open_lastio;
    double agg_perf_by_slowest;
    double *rank_cumul_io_time;
    double *rank_cumul_md_time;
} perf_data_t;

typedef struct file_data_s
{
    int64_t total;
    int64_t total_size;
    int64_t total_max;
    int64_t read_only;
    int64_t read_only_size;
    int64_t read_only_max;
    int64_t write_only;
    int64_t write_only_size;
    int64_t write_only_max;
    int64_t read_write;
    int64_t read_write_size;
    int64_t read_write_max;
    int64_t unique;
    int64_t unique_size;
    int64_t unique_max;
    int64_t shared;
    int64_t shared_size;
    int64_t shared_max;
} file_data_t;

/*
 * Prototypes
 */
void accum_perf(struct darshan_file *, hash_entry_t *, perf_data_t *);
void calc_perf(struct darshan_job *, hash_entry_t *, perf_data_t *);

void accum_file(struct darshan_job *, struct darshan_file *, hash_entry_t *, file_data_t *);
void calc_file(struct darshan_job *, hash_entry_t *, file_data_t *);
void file_list(struct darshan_job *, hash_entry_t *, int);

int usage (char *exename)
{
    fprintf(stderr, "Usage: %s [options] <filename>\n", exename);
    fprintf(stderr, "    --all   : all sub-options are enabled\n");
    fprintf(stderr, "    --base  : darshan log field data [default]\n");
    fprintf(stderr, "    --file  : total file counts\n");
    fprintf(stderr, "    --file-list  : per-file summaries\n");
    fprintf(stderr, "    --file-list-detailed  : per-file summaries with additional detail\n");
    fprintf(stderr, "    --perf  : derived perf data\n");
    fprintf(stderr, "    --total : aggregated darshan field data\n");

    exit(1);
}

int parse_args (int argc, char **argv, char **filename)
{
    int index;
    int mask;
    static struct option long_opts[] =
    {
        {"all",   0, NULL, OPTION_ALL},
        {"base",  0, NULL, OPTION_BASE},
        {"file",  0, NULL, OPTION_FILE},
        {"file-list",  0, NULL, OPTION_FILE_LIST},
        {"file-list-detailed",  0, NULL, OPTION_FILE_LIST_DETAILED},
        {"perf",  0, NULL, OPTION_PERF},
        {"total", 0, NULL, OPTION_TOTAL},
        {"help",  0, NULL, 0},
        {0, 0, 0, 0}
    };

    mask = 0;

    while(1)
    {
        int c = getopt_long(argc, argv, "", long_opts, &index);

        if (c == -1) break;

        switch(c)
        {
            case OPTION_ALL:
            case OPTION_BASE:
            case OPTION_FILE:
            case OPTION_FILE_LIST:
            case OPTION_FILE_LIST_DETAILED:
            case OPTION_PERF:
            case OPTION_TOTAL:
                mask |= c;
                break;
            case 0:
            case '?':
            default:
                usage(argv[0]);
                break;
        }
    }

    if (optind < argc)
    {
        *filename = argv[optind];
    }
    else
    {
        usage(argv[0]);
    }

    /* default mask value if none specified */
    if (mask == 0)
    {
        mask = OPTION_BASE;
    }

    return mask;
}

int main(int argc, char **argv)
{
    int ret;
    int mask;
    char *filename;
    struct darshan_job job;
    struct darshan_file cp_file;
    char tmp_string[4096];
    time_t tmp_time = 0;
    darshan_fd file;
    int i;
    int mount_count;
    int64_t* devs;
    char** mnt_pts;
    char** fs_types;
    int last_rank = 0;
    char *token;
    char *save;
    char buffer[DARSHAN_JOB_METADATA_LEN];

    hash_entry_t *file_hash = NULL;
    hash_entry_t *curr = NULL;
    hash_entry_t *tmp = NULL;
    hash_entry_t total;
    perf_data_t pdata;
    file_data_t fdata;

    memset(&pdata, 0, sizeof(pdata));
    memset(&total, 0, sizeof(total));

    mask = parse_args(argc, argv, &filename);

    file = darshan_log_open(filename, "r");
    if(!file)
    {
        fprintf(stderr, "darshan_log_open() failed to open %s\n.", filename);
        return(-1);
    }
   
    /* read job info */
    ret = darshan_log_getjob(file, &job);
    if(ret < 0)
    {
        fprintf(stderr, "Error: unable to read job information from log file.\n");
        darshan_log_close(file);
        return(-1);
    }

    /* warn user about any missing information in this log format */
    darshan_log_print_version_warnings(&job);

    ret = darshan_log_getexe(file, tmp_string);
    if(ret < 0)
    {
        fprintf(stderr, "Error: unable to read trailing job information.\n");
        darshan_log_close(file);
        return(-1);
    }

    /* print job summary */
    printf("# darshan log version: %s\n", job.version_string);
    printf("# size of file statistics: %zu bytes\n", sizeof(cp_file));
    printf("# size of job statistics: %zu bytes\n", sizeof(job));
    printf("# exe: %s\n", tmp_string);
    printf("# uid: %" PRId64 "\n", job.uid);
    printf("# jobid: %" PRId64 "\n", job.jobid);
    printf("# start_time: %" PRId64 "\n", job.start_time);
    tmp_time += job.start_time;
    printf("# start_time_asci: %s", ctime(&tmp_time));
    printf("# end_time: %" PRId64 "\n", job.end_time);
    tmp_time = 0;
    tmp_time += job.end_time;
    printf("# end_time_asci: %s", ctime(&tmp_time));
    printf("# nprocs: %" PRId64 "\n", job.nprocs);
    printf("# run time: %" PRId64 "\n", job.end_time - job.start_time + 1);
    for(token=strtok_r(job.metadata, "\n", &save);
        token != NULL;
        token=strtok_r(NULL, "\n", &save))
    {
        char *key;
        char *value;
        /* NOTE: we intentionally only split on the first = character.
         * There may be additional = characters in the value portion
         * (for example, when storing mpi-io hints).
         */
        strcpy(buffer, token);
        key = buffer;
        value = index(buffer, '=');
        if(!value)
            continue;
        /* convert = to a null terminator to split key and value */
        value[0] = '\0';
        value++;
        printf("# metadata: %s = %s\n", key, value);
    }
 
    /* print table of mounted file systems */
    ret = darshan_log_getmounts(file, &devs, &mnt_pts, &fs_types, &mount_count);
    printf("\n# mounted file systems (device, mount point, and fs type)\n");
    printf("# -------------------------------------------------------\n");
    for(i=0; i<mount_count; i++)
    {
        printf("# mount entry: %" PRId64 "\t%s\t%s\n", devs[i], mnt_pts[i], fs_types[i]);
    }
  
    /* try to retrieve first record (may not exist) */
    ret = darshan_log_getfile(file, &job, &cp_file);
    if(ret < 0)
    {
        fprintf(stderr, "Error: failed to parse log file.\n");
        fflush(stderr);
        return(-1);
    }
    if(ret == 0)
    {
        /* it looks like the app didn't open any files */
        printf("# no files opened.\n");
        darshan_log_close(file);
        return(0);
    }

    if ((mask & OPTION_BASE))
    {
        printf("\n# description of columns:\n");
        printf("#   <rank>: MPI rank.  -1 indicates that the file is shared\n");
        printf("#      across all processes and statistics are aggregated.\n");
        printf("#   <file>: hash of file path.  0 indicates that statistics\n");
        printf("#      are condensed to refer to all files opened at the given\n");
        printf("#      process.\n");
        printf("#   <counter> and <value>: statistical counters.\n");
        printf("#      A value of -1 indicates that Darshan could not monitor\n");
        printf("#      that counter, and its value should be ignored.\n");
        printf("#   <name suffix>: last %d characters of file name.\n", CP_NAME_SUFFIX_LEN);
        printf("#   <mount pt>: mount point that the file resides on.\n");
        printf("#   <fs type>: type of file system that the file resides on.\n");
        printf("\n# description of counters:\n");
        printf("#   CP_POSIX_*: posix operation counts.\n");
        printf("#   CP_COLL_*: MPI collective operation counts.\n");
        printf("#   CP_INDEP_*: MPI independent operation counts.\n");
        printf("#   CP_SPIT_*: MPI split collective operation counts.\n");
        printf("#   CP_NB_*: MPI non blocking operation counts.\n");
        printf("#   READS,WRITES,OPENS,SEEKS,STATS, and MMAPS are types of operations.\n");
        printf("#   CP_*_NC_OPENS: number of indep. and collective pnetcdf opens.\n");
        printf("#   CP_HDF5_OPENS: number of hdf5 opens.\n");
        printf("#   CP_COMBINER_*: combiner counts for MPI mem and file datatypes.\n");
        printf("#   CP_HINTS: number of times MPI hints were used.\n");
        printf("#   CP_VIEWS: number of times MPI file views were used.\n");
        printf("#   CP_MODE: mode that file was opened in.\n");
        printf("#   CP_BYTES_*: total bytes read and written.\n");
        printf("#   CP_MAX_BYTE_*: highest offset byte read and written.\n");
        printf("#   CP_CONSEC_*: number of exactly adjacent reads and writes.\n");
        printf("#   CP_SEQ_*: number of reads and writes from increasing offsets.\n");
        printf("#   CP_RW_SWITCHES: number of times access alternated between read and write.\n");
        printf("#   CP_*_ALIGNMENT: memory and file alignment.\n");
        printf("#   CP_*_NOT_ALIGNED: number of reads and writes that were not aligned.\n");
        printf("#   CP_MAX_*_TIME_SIZE: size of the slowest read and write operations.\n");
        printf("#   CP_SIZE_READ_*: histogram of read access sizes.\n");
        printf("#   CP_SIZE_READ_AGG_*: histogram of MPI datatype total sizes.\n");
        printf("#   CP_EXTENT_READ_*: histogram of MPI datatype extents.\n");
        printf("#   CP_STRIDE*_STRIDE: the four most common strides detected.\n");
        printf("#   CP_STRIDE*_COUNT: count of the four most common strides.\n");
        printf("#   CP_ACCESS*_ACCESS: the four most common access sizes.\n");
        printf("#   CP_ACCESS*_COUNT: count of the four most common access sizes.\n");
        printf("#   CP_DEVICE: File system identifier.\n");
        printf("#   CP_SIZE_AT_OPEN: size of file when first opened.\n");
        printf("#   CP_*_RANK_BYTES: fastest, slowest and variance of bytes transfer.\n");
        printf("#   CP_F_OPEN_TIMESTAMP: timestamp of first open (mpi or posix).\n");
        printf("#   CP_F_*_START_TIMESTAMP: timestamp of first read/write (mpi or posix).\n");
        printf("#   CP_F_*_END_TIMESTAMP: timestamp of last read/write (mpi or posix).\n");
        printf("#   CP_F_CLOSE_TIMESTAMP: timestamp of last close (mpi or posix).\n");
        printf("#   CP_F_POSIX_READ/WRITE_TIME: cumulative time spent in posix reads or writes.\n");
        printf("#   CP_F_MPI_READ/WRITE_TIME: cumulative time spent in mpi-io reads or writes.\n");
        printf("#   CP_F_POSIX_META_TIME: cumulative time spent in posix open, close, fsync, stat and seek, .\n");
        printf("#   CP_F_MPI_META_TIME: cumulative time spent in mpi-io open, close, set_view, and sync.\n");
        printf("#   CP_MAX_*_TIME: duration of the slowest read and write operations.\n");
        printf("#   CP_*_RANK_TIME: fastest, slowest variance of transfer time. Note that these counters show MPI-IO time for files accessed using MPI-IO, and POSIX time otherwise.\n");

        printf("\n");
        CP_PRINT_HEADER();
    }

    pdata.rank_cumul_io_time = malloc(sizeof(double)*job.nprocs);
    pdata.rank_cumul_md_time = malloc(sizeof(double)*job.nprocs);
    if (!pdata.rank_cumul_io_time || !pdata.rank_cumul_md_time)
    {
        perror("malloc failed");
        darshan_log_close(file);
        return(-1);
    }
    else
    {
        memset(pdata.rank_cumul_io_time, 0, sizeof(double)*job.nprocs);
        memset(pdata.rank_cumul_md_time, 0, sizeof(double)*job.nprocs);
    }

    do
    {
        char* mnt_pt = NULL;
        char* fs_type = NULL;
        hash_entry_t *hfile = NULL;

        if(cp_file.rank != -1 && cp_file.rank < last_rank)
        {
            fprintf(stderr, "Error: log file contains out of order rank data.\n");
            fflush(stderr);
            return(-1);
        }
        if(cp_file.rank != -1)
            last_rank = cp_file.rank;
        
        for(i=0; i<mount_count; i++)
        {
            if(cp_file.counters[CP_DEVICE] == devs[i])
            {
                mnt_pt = mnt_pts[i];
                fs_type = fs_types[i];
                break;
            }
        }
        if(!mnt_pt)
            mnt_pt = "UNKNOWN";
        if(!fs_type)
            fs_type = "UNKNOWN";

        HASH_FIND(hlink,file_hash,&cp_file.hash,sizeof(int64_t),hfile);
        if (!hfile)
        {
            hfile = (hash_entry_t*) malloc(sizeof(*hfile));
            if (!hfile)
            {
                fprintf(stderr,"malloc failure");
                exit(1);
            }

            /* init */
            memset(hfile, 0, sizeof(*hfile));
            hfile->hash          = cp_file.hash;
            memcpy(hfile->name_suffix, cp_file.name_suffix, CP_NAME_SUFFIX_LEN+1);
            hfile->type          = 0;
            hfile->procs         = 0;
            hfile->cumul_time    = 0.0;
            hfile->meta_time     = 0.0;
            hfile->slowest_time  = 0.0;

            HASH_ADD(hlink,file_hash,hash,sizeof(int64_t),hfile);
        }

        accum_file(&job, &cp_file, &total, NULL);
        accum_file(&job, &cp_file, hfile, &fdata);
        accum_perf(&cp_file, hfile, &pdata);

        if ((mask & OPTION_BASE))
        {
            for(i=0; i<CP_NUM_INDICES; i++)
            {
                CP_PRINT(&job, &cp_file, i, mnt_pt, fs_type);
            }
            for(i=0; i<CP_F_NUM_INDICES; i++)
            {
                CP_F_PRINT(&job, &cp_file, i, mnt_pt, fs_type);
            }
        }
    }while((ret = darshan_log_getfile(file, &job, &cp_file)) == 1);

    /* Total Calc */
    if ((mask & OPTION_TOTAL))
    {
        for(i=0; i<CP_NUM_INDICES; i++)
        {
            printf("total_%s: %" PRId64 "\n",
                   darshan_names[i], total.counters[i]);
        }
        for(i=0; i<CP_F_NUM_INDICES; i++)
        {
            printf("total_%s: %lf\n",
                   darshan_f_names[i], total.fcounters[i]);
        }
    }

    /* Perf Calc */
    calc_perf(&job, file_hash, &pdata);
    if ((mask & OPTION_PERF))
    {
        printf("\n# performance\n");
        printf("# -----------\n");
        printf("# total_bytes: %" PRId64 "\n", pdata.total_bytes);
        printf("#\n");
        printf("# I/O timing for unique files (seconds):\n");
        printf("# ...........................\n");
        printf("# unique files: slowest_rank_time: %lf\n", pdata.slowest_rank_time);
        printf("# unique files: slowest_rank_meta_time: %lf\n", pdata.slowest_rank_meta_time);
        printf("#\n");
        printf("# I/O timing for shared files (seconds):\n");
        printf("# (multiple estimates shown; time_by_slowest is generally the most accurate)\n");
        printf("# ...........................\n");
        printf("# shared files: time_by_cumul_io_only: %lf\n", pdata.shared_time_by_cumul);
        printf("# shared files: time_by_cumul_meta_only: %lf\n", pdata.shared_meta_time);
        printf("# shared files: time_by_open: %lf\n", pdata.shared_time_by_open);
        printf("# shared files: time_by_open_lastio: %lf\n", pdata.shared_time_by_open_lastio);
        printf("# shared files: time_by_slowest: %lf\n", pdata.shared_time_by_slowest);
        printf("#\n");
        printf("# Aggregate performance, including both shared and unique files (MiB/s):\n");
        printf("# (multiple estimates shown; agg_perf_by_slowest is generally the most accurate)\n");
        printf("# ...........................\n");
        printf("# agg_perf_by_cumul: %lf\n", pdata.agg_perf_by_cumul);
        printf("# agg_perf_by_open: %lf\n", pdata.agg_perf_by_open);
        printf("# agg_perf_by_open_lastio: %lf\n", pdata.agg_perf_by_open_lastio);
        printf("# agg_perf_by_slowest: %lf\n", pdata.agg_perf_by_slowest);
    }

    /* File Calc */
    calc_file(&job, file_hash, &fdata);
    if ((mask & OPTION_FILE))
    {
        printf("\n# files\n");
        printf("# -----\n");
        printf("# total: %" PRId64 " %" PRId64 " %" PRId64 "\n",
               fdata.total,
               fdata.total_size,
               fdata.total_max);
        printf("# read_only: %" PRId64 " %" PRId64 " %" PRId64 "\n",
               fdata.read_only,
               fdata.read_only_size,
               fdata.read_only_max);
        printf("# write_only: %" PRId64 " %" PRId64 " %" PRId64 "\n",
               fdata.write_only,
               fdata.write_only_size,
               fdata.write_only_max);
        printf("# read_write: %" PRId64 " %" PRId64 " %" PRId64 "\n",
               fdata.read_write,
               fdata.read_write_size,
               fdata.read_write_max);
        printf("# unique: %" PRId64 " %" PRId64 " %" PRId64 "\n",
               fdata.unique,
               fdata.unique_size,
               fdata.unique_max);
        printf("# shared: %" PRId64 " %" PRId64 " %" PRId64 "\n",
               fdata.shared,
               fdata.shared_size,
               fdata.shared_max);
    }

    if ((mask & OPTION_FILE_LIST) || mask & OPTION_FILE_LIST_DETAILED)
    {
        if(mask & OPTION_FILE_LIST_DETAILED)
            file_list(&job, file_hash, 1);
        else
            file_list(&job, file_hash, 0);
    }

    if(ret < 0)
    {
        fprintf(stderr, "Error: failed to parse log file.\n");
        fflush(stderr);
        return(-1);
    }

    for(i=0; i<mount_count; i++)
    {
        free(mnt_pts[i]);
        free(fs_types[i]);
    }
    if(mount_count > 0)
    {
        free(devs);
        free(mnt_pts);
        free(fs_types);
    }
 
    darshan_log_close(file);

    HASH_ITER(hlink, file_hash, curr, tmp)
    {
        HASH_DELETE(hlink, file_hash, curr);
        free(curr);
    }

    return(0);
}

void accum_file(struct darshan_job *job,
                struct darshan_file *dfile,
                hash_entry_t *hfile, 
                file_data_t *fdata)
{
    int i;

    hfile->procs += 1;

    if (dfile->rank == -1)
    {
        if(job->version_string[0] == '1')
        {
            hfile->slowest_time = 
                max((dfile->fcounters[CP_F_READ_END_TIMESTAMP] 
                    - dfile->fcounters[CP_F_OPEN_TIMESTAMP]),
                    (dfile->fcounters[CP_F_WRITE_END_TIMESTAMP] 
                    - dfile->fcounters[CP_F_OPEN_TIMESTAMP]));
            if(hfile->slowest_time < 0)
                hfile->slowest_time = 0;
        }
        else
        {
            hfile->slowest_time = dfile->fcounters[CP_F_SLOWEST_RANK_TIME];
        }
    }
    else
    {
        if(dfile->counters[CP_INDEP_OPENS] || dfile->counters[CP_COLL_OPENS])
        {
            /* MPI file */
            hfile->slowest_time = max(hfile->slowest_time, 
                (dfile->fcounters[CP_F_MPI_META_TIME] +
                dfile->fcounters[CP_F_MPI_READ_TIME] +
                dfile->fcounters[CP_F_MPI_WRITE_TIME]));
        }
        else
        {
            /* POSIX file */
            hfile->slowest_time = max(hfile->slowest_time, 
                (dfile->fcounters[CP_F_POSIX_META_TIME] +
                dfile->fcounters[CP_F_POSIX_READ_TIME] +
                dfile->fcounters[CP_F_POSIX_WRITE_TIME]));
        }
    }

    if (dfile->rank == -1)
    {
        hfile->procs = job->nprocs;
        hfile->type |= FILETYPE_SHARED;

    }
    else if (hfile->procs > 1)
    {
        hfile->type &= (~FILETYPE_UNIQUE);
        hfile->type |= FILETYPE_PARTSHARED;
    }
    else
    {
        hfile->type |= FILETYPE_UNIQUE;
    }

    if(dfile->counters[CP_INDEP_OPENS] || dfile->counters[CP_COLL_OPENS])
    {
        hfile->cumul_time += dfile->fcounters[CP_F_MPI_META_TIME] +
                             dfile->fcounters[CP_F_MPI_READ_TIME] +
                             dfile->fcounters[CP_F_MPI_WRITE_TIME];
    }
    else
    {
        hfile->cumul_time += dfile->fcounters[CP_F_POSIX_META_TIME] +
                             dfile->fcounters[CP_F_POSIX_READ_TIME] +
                             dfile->fcounters[CP_F_POSIX_WRITE_TIME];
    }

    for (i = 0; i < CP_NUM_INDICES; i++)
    {
        switch(i)
        {
        case CP_DEVICE:
        case CP_MODE:
        case CP_MEM_ALIGNMENT:
        case CP_FILE_ALIGNMENT:
            if(CP_FILE_PARTIAL(hfile))
                hfile->counters[i] = dfile->counters[i];
            break;
        case CP_SIZE_AT_OPEN:
            if (hfile->counters[i] == -1)
            {
                hfile->counters[i] = dfile->counters[i];
            }
            if (hfile->counters[i] > dfile->counters[i] && !CP_FILE_PARTIAL(dfile))
            {
                hfile->counters[i] = dfile->counters[i];
            }
            break;
        case CP_MAX_BYTE_READ:
        case CP_MAX_BYTE_WRITTEN:
            if (hfile->counters[i] < dfile->counters[i])
            {
                hfile->counters[i] = dfile->counters[i];
            }
            break;

        case CP_STRIDE1_STRIDE:
        case CP_STRIDE2_STRIDE:
        case CP_STRIDE3_STRIDE:
        case CP_STRIDE4_STRIDE:
        case CP_ACCESS1_ACCESS:
        case CP_ACCESS2_ACCESS:
        case CP_ACCESS3_ACCESS:
        case CP_ACCESS4_ACCESS:
           /*
            * do nothing here because these will be stored
            * when the _COUNT is accessed.
            */
           break;
 
        case CP_STRIDE1_COUNT:
        case CP_STRIDE2_COUNT:
        case CP_STRIDE3_COUNT:
        case CP_STRIDE4_COUNT:
        case CP_ACCESS1_COUNT:
        case CP_ACCESS2_COUNT:
        case CP_ACCESS3_COUNT:
        case CP_ACCESS4_COUNT:
            if (hfile->counters[i] < dfile->counters[i])
            {
                hfile->counters[i]   = dfile->counters[i];
                hfile->counters[i-4] = dfile->counters[i-4];
            }
            break;
        case CP_FASTEST_RANK:
        case CP_SLOWEST_RANK:
        case CP_FASTEST_RANK_BYTES:
        case CP_SLOWEST_RANK_BYTES:
            hfile->counters[i] = 0;
            break;
        case CP_MAX_READ_TIME_SIZE:
        case CP_MAX_WRITE_TIME_SIZE:
            break;
        default:
            hfile->counters[i] += dfile->counters[i];
            break;
        }
    }

    for (i = 0; i < CP_F_NUM_INDICES; i++)
    {
        switch(i)
        {
            case CP_F_OPEN_TIMESTAMP:
            case CP_F_READ_START_TIMESTAMP:
            case CP_F_WRITE_START_TIMESTAMP:
                if(hfile->fcounters[i] == 0 || 
                    hfile->fcounters[i] > dfile->fcounters[i])
                {
                    hfile->fcounters[i] = dfile->fcounters[i];
                }
                break;
            case CP_F_CLOSE_TIMESTAMP:
            case CP_F_READ_END_TIMESTAMP:
            case CP_F_WRITE_END_TIMESTAMP:
                if(hfile->fcounters[i] == 0 || 
                    hfile->fcounters[i] < dfile->fcounters[i])
                {
                    hfile->fcounters[i] = dfile->fcounters[i];
                }
                break;
            case CP_F_FASTEST_RANK_TIME:
            case CP_F_SLOWEST_RANK_TIME:
            case CP_F_VARIANCE_RANK_TIME:
            case CP_F_VARIANCE_RANK_BYTES:
                hfile->fcounters[i] = 0;
                break;
            case CP_F_MAX_READ_TIME:
                if (hfile->fcounters[i] > dfile->fcounters[i])
                {
                    hfile->fcounters[i] = dfile->fcounters[i];
                    hfile->counters[CP_MAX_READ_TIME_SIZE] =
                        dfile->counters[CP_MAX_READ_TIME_SIZE];
                }
                break;
            case CP_F_MAX_WRITE_TIME:
                if (hfile->fcounters[i] > dfile->fcounters[i])
                {
                    hfile->fcounters[i] = dfile->fcounters[i];
                    hfile->counters[CP_MAX_WRITE_TIME_SIZE] =
                        dfile->counters[CP_MAX_WRITE_TIME_SIZE];
                }
                break;
            default:
                hfile->fcounters[i] += dfile->fcounters[i];
                break;
        }
    }

    return;
}

void file_list(struct darshan_job *djob, hash_entry_t *file_hash, int detail_flag)
{
    hash_entry_t *curr = NULL;
    hash_entry_t *tmp = NULL;
    char* type;
    int i;

    /* TODO: list of columns:
     *
     * normal mode?
     * - hash
     * - suffix
     * - MPI or POSIX
     * - nprocs
     * - slowest I/O time
     * - average cumulative I/O time
     *
     * detailed mode?
     * - first open
     * - first read
     * - first write
     * - last close
     * - last read
     * - last write
     * - MPI indep opens
     * - MPI coll opens
     * - POSIX opens
     * - r histogram (POSIX)
     * - w histogram (POSIX)
     */

    if(detail_flag)
        printf("\n# Per-file summary of I/O activity (detailed).\n");
    else
        printf("\n# Per-file summary of I/O activity.\n");

    printf("# <hash>: hash of file name\n");
    printf("# <suffix>: last %d characters of file name\n", CP_NAME_SUFFIX_LEN);
    printf("# <type>: MPI or POSIX\n");
    printf("# <nprocs>: number of processes that opened the file\n");
    printf("# <slowest>: (estimated) time in seconds consumed in IO by slowest process\n");
    printf("# <avg>: average time in seconds consumed in IO per process\n");
    if(detail_flag)
    {
        printf("# <start_{open/read/write}>: start timestamp of first open, read, or write\n");
        printf("# <end_{open/read/write}>: end timestamp of last open, read, or write\n");
        printf("# <mpi_indep_opens>: independent MPI_File_open calls\n");
        printf("# <mpi_coll_opens>: collective MPI_File_open calls\n");
        printf("# <posix_opens>: POSIX open calls\n");
        printf("# <CP_SIZE_READ_*>: POSIX read size histogram\n");
        printf("# <CP_SIZE_WRITE_*>: POSIX write size histogram\n");
    }
    
    printf("\n# <hash>\t<suffix>\t<type>\t<nprocs>\t<slowest>\t<avg>");
    if(detail_flag)
    {
        printf("\t<start_open>\t<start_read>\t<start_write>");
        printf("\t<end_open>\t<end_read>\t<end_write>");
        printf("\t<mpi_indep_opens>\t<mpi_coll_opens>\t<posix_opens>");
        for(i=CP_SIZE_READ_0_100; i<= CP_SIZE_WRITE_1G_PLUS; i++)
            printf("\t%s", darshan_names[i]);
    }
    printf("\n");

    HASH_ITER(hlink, file_hash, curr, tmp)
    {
        if(curr->counters[CP_INDEP_OPENS] || curr->counters[CP_COLL_OPENS])
            type = "MPI";
        else
            type = "POSIX";

        printf("%" PRIu64 "\t%s\t%s\t%" PRId64 "\t%f\t%f",
            curr->hash,
            curr->name_suffix,
            type,
            curr->procs,
            curr->slowest_time,
            curr->cumul_time/(double)curr->procs);
        if(detail_flag)
        {
            for(i=CP_F_OPEN_TIMESTAMP; i<=CP_F_WRITE_END_TIMESTAMP; i++)
            {
                printf("\t%f", curr->fcounters[i]);
            }
            printf("\t%" PRId64 "\t%" PRId64 "\t%" PRId64, curr->counters[CP_INDEP_OPENS], curr->counters[CP_COLL_OPENS], curr->counters[CP_POSIX_OPENS]);
            for(i=CP_SIZE_READ_0_100; i<= CP_SIZE_WRITE_1G_PLUS; i++)
                printf("\t%" PRId64, curr->counters[i]);
        }
        printf("\n");
    }

    return;
}

void calc_file(struct darshan_job *djob,
               hash_entry_t *file_hash, 
               file_data_t *fdata)
{
    hash_entry_t *curr = NULL;
    hash_entry_t *tmp = NULL;

    memset(fdata, 0, sizeof(*fdata));

    HASH_ITER(hlink, file_hash, curr, tmp)
    {
        int64_t max;
        int64_t r;
        int64_t w;

        max = max3(curr->counters[CP_SIZE_AT_OPEN],
                   curr->counters[CP_MAX_BYTE_READ],
                   curr->counters[CP_MAX_BYTE_WRITTEN]);

        r = (curr->counters[CP_POSIX_READS]+
             curr->counters[CP_POSIX_FREADS]+
             curr->counters[CP_INDEP_READS]+
             curr->counters[CP_COLL_READS]+
             curr->counters[CP_SPLIT_READS]+
             curr->counters[CP_NB_READS]);

        w = (curr->counters[CP_POSIX_WRITES]+
             curr->counters[CP_POSIX_FWRITES]+
             curr->counters[CP_INDEP_WRITES]+
             curr->counters[CP_COLL_WRITES]+
             curr->counters[CP_SPLIT_WRITES]+
             curr->counters[CP_NB_WRITES]);

        fdata->total += 1;
        fdata->total_size += max;
        fdata->total_max = max(fdata->total_max, max);

        if (r && !w)
        {
            fdata->read_only += 1;
            fdata->read_only_size += max;
            fdata->read_only_max = max(fdata->read_only_max, max);
        }

        if (!r && w)
        {
            fdata->write_only += 1;
            fdata->write_only_size += max;
            fdata->write_only_max = max(fdata->write_only_max, max);
        }

        if (r && w)
        {
            fdata->read_write += 1;
            fdata->read_write_size += max;
            fdata->read_write_max = max(fdata->read_write_max, max);
        }

        if ((curr->type & (FILETYPE_SHARED|FILETYPE_PARTSHARED)))
        {
            fdata->shared += 1;
            fdata->shared_size += max;
            fdata->shared_max = max(fdata->shared_max, max);
        }

        if ((curr->type & (FILETYPE_UNIQUE)))
        {
            fdata->unique += 1;
            fdata->unique_size += max;
            fdata->unique_max = max(fdata->unique_max, max);
        }
    }

    return;
}

void accum_perf(struct darshan_file *dfile,
                hash_entry_t *hfile,
                perf_data_t *pdata)
{
    int64_t mpi_file;

    pdata->total_bytes += dfile->counters[CP_BYTES_READ] +
                          dfile->counters[CP_BYTES_WRITTEN];

    mpi_file = dfile->counters[CP_INDEP_OPENS] +
               dfile->counters[CP_COLL_OPENS];

    /*
     * Calculation of Shared File Time
     *   Four Methods!!!!
     *     by_cumul: sum time counters and divide by nprocs
     *               (inaccurate if lots of variance between procs)
     *     by_open: difference between timestamp of open and close
     *              (inaccurate if file is left open without i/o happening)
     *     by_open_lastio: difference between timestamp of open and the
     *                     timestamp of last i/o
     *                     (similar to above but fixes case where file is left
     *                      open after io is complete)
     *     by_slowest: use slowest rank time from log data
     *                 (most accurate but requires newer log version)
     */
    if (dfile->rank == -1)
    {
        /* by_open (same for MPI or POSIX) */
        if (dfile->fcounters[CP_F_CLOSE_TIMESTAMP] >
            dfile->fcounters[CP_F_OPEN_TIMESTAMP])
        {
            pdata->shared_time_by_open +=
                dfile->fcounters[CP_F_CLOSE_TIMESTAMP] -
                dfile->fcounters[CP_F_OPEN_TIMESTAMP];
        }

        /* by_open_lastio (same for MPI or POSIX) */
        if (dfile->fcounters[CP_F_READ_END_TIMESTAMP] >
            dfile->fcounters[CP_F_WRITE_END_TIMESTAMP])
        {
            /* be careful: file may have been opened but not read or written */
            if(dfile->fcounters[CP_F_READ_END_TIMESTAMP] > dfile->fcounters[CP_F_OPEN_TIMESTAMP])
            {
                pdata->shared_time_by_open_lastio += 
                    dfile->fcounters[CP_F_READ_END_TIMESTAMP] - 
                    dfile->fcounters[CP_F_OPEN_TIMESTAMP];
            }
        }
        else
        {
            /* be careful: file may have been opened but not read or written */
            if(dfile->fcounters[CP_F_WRITE_END_TIMESTAMP] > dfile->fcounters[CP_F_OPEN_TIMESTAMP])
            {
                pdata->shared_time_by_open_lastio += 
                    dfile->fcounters[CP_F_WRITE_END_TIMESTAMP] - 
                    dfile->fcounters[CP_F_OPEN_TIMESTAMP];
            }
        }

        /* by_cumul */
        if (mpi_file)
        {
            pdata->shared_time_by_cumul +=
                dfile->fcounters[CP_F_MPI_META_TIME] +
                dfile->fcounters[CP_F_MPI_READ_TIME] +
                dfile->fcounters[CP_F_MPI_WRITE_TIME];
            pdata->shared_meta_time += dfile->fcounters[CP_F_MPI_META_TIME];
        }
        else
        {
            pdata->shared_time_by_cumul +=
                dfile->fcounters[CP_F_POSIX_META_TIME] +
                dfile->fcounters[CP_F_POSIX_READ_TIME] +
                dfile->fcounters[CP_F_POSIX_WRITE_TIME];
            pdata->shared_meta_time += dfile->fcounters[CP_F_POSIX_META_TIME];
        }

        /* by_slowest (same for MPI or POSIX) */
        pdata->shared_time_by_slowest +=
            dfile->fcounters[CP_F_SLOWEST_RANK_TIME];
    }

    /*
     * Calculation of Unique File Time
     *   record the data for each file and sum it 
     */
    else
    {
        if (mpi_file)
        {
            pdata->rank_cumul_io_time[dfile->rank] += dfile->fcounters[CP_F_MPI_META_TIME] +
                                dfile->fcounters[CP_F_MPI_READ_TIME] +
                                dfile->fcounters[CP_F_MPI_WRITE_TIME];
            pdata->rank_cumul_md_time[dfile->rank] += dfile->fcounters[CP_F_MPI_META_TIME];
        }
        else
        {
            pdata->rank_cumul_io_time[dfile->rank] += dfile->fcounters[CP_F_POSIX_META_TIME] +
                                dfile->fcounters[CP_F_POSIX_READ_TIME] +
                                dfile->fcounters[CP_F_POSIX_WRITE_TIME];
            pdata->rank_cumul_md_time[dfile->rank] += dfile->fcounters[CP_F_POSIX_META_TIME];

        }
    }

    return;
}

void calc_perf(struct darshan_job *djob,
               hash_entry_t *hash_rank_uniq,
               perf_data_t *pdata)
{
    int64_t i;

    pdata->shared_time_by_cumul =
        pdata->shared_time_by_cumul / (double)djob->nprocs;

    pdata->shared_meta_time = pdata->shared_meta_time / (double)djob->nprocs;

    for (i=0; i<djob->nprocs; i++)
    {
        if (pdata->rank_cumul_io_time[i] > pdata->slowest_rank_time)
        {
            pdata->slowest_rank_time = pdata->rank_cumul_io_time[i];
            pdata->slowest_rank_meta_time = pdata->rank_cumul_md_time[i];
        }
    }

    if (pdata->slowest_rank_time + pdata->shared_time_by_cumul)
    pdata->agg_perf_by_cumul = ((double)pdata->total_bytes / 1048576.0) /
                                  (pdata->slowest_rank_time +
                                   pdata->shared_time_by_cumul);

    if (pdata->slowest_rank_time + pdata->shared_time_by_open)
    pdata->agg_perf_by_open  = ((double)pdata->total_bytes / 1048576.0) / 
                                   (pdata->slowest_rank_time +
                                    pdata->shared_time_by_open);

    if (pdata->slowest_rank_time + pdata->shared_time_by_open_lastio)
    pdata->agg_perf_by_open_lastio = ((double)pdata->total_bytes / 1048576.0) /
                                     (pdata->slowest_rank_time +
                                      pdata->shared_time_by_open_lastio);

    if (pdata->slowest_rank_time + pdata->shared_time_by_slowest)
    pdata->agg_perf_by_slowest = ((double)pdata->total_bytes / 1048576.0) /
                                     (pdata->slowest_rank_time +
                                      pdata->shared_time_by_slowest);

    return;
}
