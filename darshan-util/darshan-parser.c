/*
 * Copyright (C) 2015 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
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

#include "uthash-1.9.2/src/uthash.h"

#include "darshan-logutils.h"

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

/*
 * Datatypes
 */
typedef struct hash_entry_s
{
    UT_hash_handle hlink;
    darshan_record_id rec_id;
    int64_t type;
    int64_t procs;
    void *rec_dat;
    double cumul_time;
    double slowest_time;
} hash_entry_t;

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

typedef struct perf_data_s
{
    int64_t total_bytes;
    double slowest_rank_time;
    double slowest_rank_meta_time;
    int slowest_rank_rank;
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

/*
 * Prototypes
 */
void posix_accum_file(struct darshan_posix_file *pfile, hash_entry_t *hfile, int64_t nprocs);
void posix_accum_perf(struct darshan_posix_file *pfile, perf_data_t *pdata);
void posix_calc_file(hash_entry_t *file_hash, file_data_t *fdata);
void posix_print_total_file(struct darshan_posix_file *pfile);
void posix_file_list(hash_entry_t *file_hash, struct darshan_record_ref *rec_hash, int detail_flag);

void mpiio_accum_file(struct darshan_mpiio_file *mfile, hash_entry_t *hfile, int64_t nprocs);
void mpiio_accum_perf(struct darshan_mpiio_file *mfile, perf_data_t *pdata);
void mpiio_calc_file(hash_entry_t *file_hash, file_data_t *fdata);
void mpiio_print_total_file(struct darshan_mpiio_file *mfile);
void mpiio_file_list(hash_entry_t *file_hash, struct darshan_record_ref *rec_hash, int detail_flag);

void calc_perf(perf_data_t *pdata, int64_t nprocs);

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
    int i, j;
    char *filename;
    char tmp_string[4096] = {0};
    darshan_fd fd;
    struct darshan_header header;
    struct darshan_job job;
    struct darshan_record_ref *rec_hash = NULL;
    struct darshan_record_ref *ref, *tmp_ref;
    int mount_count;
    char** mnt_pts;
    char** fs_types;
    time_t tmp_time = 0;
    char *token;
    char *save;
    char buffer[DARSHAN_JOB_METADATA_LEN];
    int empty_mods = 0;
    char *mod_buf;
    int mod_buf_sz;

    hash_entry_t *file_hash = NULL;
    hash_entry_t *curr = NULL;
    hash_entry_t *tmp_file = NULL;
    hash_entry_t total;
    file_data_t fdata;
    perf_data_t pdata;

    memset(&total, 0, sizeof(total));
    memset(&fdata, 0, sizeof(fdata));
    memset(&pdata, 0, sizeof(pdata));

    mask = parse_args(argc, argv, &filename);

    fd = darshan_log_open(filename);
    if(!fd)
    {
        fprintf(stderr, "darshan_log_open() failed to open %s\n.", filename);
        return(-1);
    }

    /* read darshan log header */
    ret = darshan_log_getheader(fd, &header);
    if(ret < 0)
    {
        fprintf(stderr, "darshan_log_getheader() failed to read log header.\n");
        darshan_log_close(fd);
        return(-1);
    }

    /* read darshan job info */
    ret = darshan_log_getjob(fd, &job);
    if(ret < 0)
    {
        fprintf(stderr, "darshan_log_getjob() failed to read job data.\n");
        darshan_log_close(fd);
        return(-1);
    }

    /* get the original command line for this job */
    ret = darshan_log_getexe(fd, tmp_string);
    if(ret < 0)
    {
        fprintf(stderr, "Error: unable to read trailing job information.\n");
        darshan_log_close(fd);
        return(-1);
    }

    /* get the mount information for this log */
    ret = darshan_log_getmounts(fd, &mnt_pts, &fs_types, &mount_count);
    if(ret < 0)
    {
        fprintf(stderr, "darshan_log_getmounts() failed to read mount information.\n");
        darshan_log_close(fd);
        return(-1);
    }

    /* read hash of darshan records */
    ret = darshan_log_gethash(fd, &rec_hash);
    if(ret < 0)
    {
        fprintf(stderr, "darshan_log_getmap() failed to read record map.\n");
        darshan_log_close(fd);
        return(-1);
    }

    /* print job summary */
    printf("# darshan log version: %s\n", header.version_string);
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

    /* print breakdown of each log file component's contribution to file size */
    printf("\n# log file component sizes (compressed)\n");
    printf("# -------------------------------------------------------\n");
    printf("# header: %zu bytes (uncompressed)\n", sizeof(struct darshan_header));
    printf("# job data: %zu bytes\n", header.rec_map.off - sizeof(struct darshan_header));
    printf("# record table: %zu bytes\n", header.rec_map.len);
    for(i=0; i<DARSHAN_MAX_MODS; i++)
    {
        if(header.mod_map[i].len)
        {
            printf("# %s module: %zu bytes\n", darshan_module_names[i],
                header.mod_map[i].len);
        }
    }

    /* print table of mounted file systems */
    printf("\n# mounted file systems (mount point and fs type)\n");
    printf("# -------------------------------------------------------\n");
    for(i=0; i<mount_count; i++)
    {
        printf("# mount entry:\t%s\t%s\n", mnt_pts[i], fs_types[i]);
    }

    mod_buf = malloc(DARSHAN_DEF_COMP_BUF_SZ);
    if(!mod_buf)
        return(-1);

    pdata.rank_cumul_io_time = malloc(sizeof(double)*job.nprocs);
    pdata.rank_cumul_md_time = malloc(sizeof(double)*job.nprocs);
    if (!pdata.rank_cumul_io_time || !pdata.rank_cumul_md_time)
    {
        darshan_log_close(fd);
        return(-1);
    }
    else
    {
        memset(pdata.rank_cumul_io_time, 0, sizeof(double)*job.nprocs);
        memset(pdata.rank_cumul_md_time, 0, sizeof(double)*job.nprocs);
    }

    for(i=0; i<DARSHAN_MAX_MODS; i++)
    {
        int mod_bytes_left;
        void *mod_buf_p;
        void *rec_p = NULL;
        darshan_record_id rec_id;
        void *save_io, *save_md;

        /* reset data structures for each module */
        mod_buf_sz = DARSHAN_DEF_COMP_BUF_SZ;
        memset(mod_buf, 0, DARSHAN_DEF_COMP_BUF_SZ);

        /* check each module for any data */
        ret = darshan_log_getmod(fd, i, mod_buf, &mod_buf_sz);
        if(ret < 0)
        {
            fprintf(stderr, "Error: failed to get module %s data\n",
                darshan_module_names[i]);
            darshan_log_close(fd);
            return(-1);
        }
        else if(ret == 0)
        {
            /* skip modules not present in log file */
            empty_mods++;
            continue;
        }

        /* skip modules with no defined logutil handlers */
        if(!mod_logutils[i])
        {
            fprintf(stderr, "Warning: no log utility handlers defined "
                "for module %s, SKIPPING\n", darshan_module_names[i]);
            continue;
        }

        printf("\n# *******************************************************\n");
        printf("# %s module data\n", darshan_module_names[i]);
        printf("# *******************************************************\n");

        /* this module has data to be parsed and printed */
        mod_bytes_left = mod_buf_sz;
        mod_buf_p = mod_buf;

        /* loop over each of this module's records and print them */
        while (mod_bytes_left > 0)
        {
            char *mnt_pt = NULL;
            char *fs_type = NULL;
            hash_entry_t *hfile = NULL;

            ret = mod_logutils[i]->log_get_record(&mod_buf_p, &mod_bytes_left,
                &rec_p, &rec_id, fd->swap_flag);
            if(ret < 0)
            {
                fprintf(stderr, "Error: failed to parse module %s data record\n",
                    darshan_module_names[i]);
                darshan_log_close(fd);
                return(-1);
            }

            /* get the pathname for this record */
            HASH_FIND(hlink, rec_hash, &rec_id, sizeof(darshan_record_id), ref);
            assert(ref);

            /* get mount point and fs type associated with this record */
            for(j=0; j<mount_count; j++)
            {
                if(strncmp(mnt_pts[j], ref->rec.name, strlen(mnt_pts[j])) == 0)
                {
                    mnt_pt = mnt_pts[j];
                    fs_type = fs_types[j];
                    break;
                }
            }
            if(!mnt_pt)
                mnt_pt = "UNKNOWN";
            if(!fs_type)
                fs_type = "UNKNOWN";

            if(mask & OPTION_BASE)
            {
                /* TODO: does each module print header of what each counter means??? */
                DARSHAN_PRINT_HEADER();

                /* print the corresponding module data for this record */
                mod_logutils[i]->log_print_record(rec_p, ref->rec.name,
                    mnt_pt, fs_type);
            }

            /* we calculate more detailed stats for POSIX and MPI-IO modules, 
             * if the parser is executed with more than the base option
             */
            if(i != DARSHAN_POSIX_MOD && i != DARSHAN_MPIIO_MOD)
                break;

            HASH_FIND(hlink, file_hash, &rec_id, sizeof(darshan_record_id), hfile);
            if(!hfile)
            {
                hfile = malloc(sizeof(*hfile));
                if(!hfile)
                    return(-1);

                /* init */
                memset(hfile, 0, sizeof(*hfile));
                hfile->rec_id = rec_id;
                hfile->type = 0;
                hfile->procs = 0;
                hfile->rec_dat = NULL;
                hfile->cumul_time = 0.0;
                hfile->slowest_time = 0.0;

                HASH_ADD(hlink, file_hash, rec_id, sizeof(darshan_record_id), hfile);
            }

            if(i == DARSHAN_POSIX_MOD)
            {
                posix_accum_file((struct darshan_posix_file*)rec_p, &total, job.nprocs);
                posix_accum_file((struct darshan_posix_file*)rec_p, hfile, job.nprocs);
                posix_accum_perf((struct darshan_posix_file*)rec_p, &pdata);
            }
            else if(i == DARSHAN_MPIIO_MOD)
            {
                mpiio_accum_file((struct darshan_mpiio_file*)rec_p, &total, job.nprocs);
                mpiio_accum_file((struct darshan_mpiio_file*)rec_p, hfile, job.nprocs);
                mpiio_accum_perf((struct darshan_mpiio_file*)rec_p, &pdata);
            }
        }

        /* Total Calc */
        if(mask & OPTION_TOTAL)
        {
            if(i == DARSHAN_POSIX_MOD)
            {
                posix_print_total_file((struct darshan_posix_file*)total.rec_dat);
            }
            else if(i == DARSHAN_MPIIO_MOD)
            {
                mpiio_print_total_file((struct darshan_mpiio_file*)total.rec_dat);
            }
        }

        /* File Calc */
        if(mask & OPTION_FILE)
        {
            if(i == DARSHAN_POSIX_MOD)
            {
                posix_calc_file(file_hash, &fdata);
            }
            else if(i == DARSHAN_MPIIO_MOD)
            {
                mpiio_calc_file(file_hash, &fdata);
            }

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

        /* Perf Calc */
        if(mask & OPTION_PERF)
        {
            calc_perf(&pdata, job.nprocs);

            printf("\n# performance\n");
            printf("# -----------\n");
            printf("# total_bytes: %" PRId64 "\n", pdata.total_bytes);
            printf("#\n");
            printf("# I/O timing for unique files (seconds):\n");
            printf("# ...........................\n");
            printf("# unique files: slowest_rank_io_time: %lf\n", pdata.slowest_rank_time);
            printf("# unique files: slowest_rank_meta_time: %lf\n", pdata.slowest_rank_meta_time);
            printf("# unique files: slowest rank: %d\n", pdata.slowest_rank_rank);
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

        if((mask & OPTION_FILE_LIST) || (mask & OPTION_FILE_LIST_DETAILED))
        {
            if(i == DARSHAN_POSIX_MOD)
            {
                if(mask & OPTION_FILE_LIST_DETAILED)
                    posix_file_list(file_hash, rec_hash, 1);
                else
                    posix_file_list(file_hash, rec_hash, 0);
            }
            else if(i == DARSHAN_MPIIO_MOD)
            {
                if(mask & OPTION_FILE_LIST_DETAILED)
                    mpiio_file_list(file_hash, rec_hash, 1);
                else
                    mpiio_file_list(file_hash, rec_hash, 0);
            }
        }

        /* reset data structures for next module */
        if(total.rec_dat) free(total.rec_dat);
        memset(&total, 0, sizeof(total));
        memset(&fdata, 0, sizeof(fdata));
        save_io = pdata.rank_cumul_io_time;
        save_md = pdata.rank_cumul_md_time;
        memset(&pdata, 0, sizeof(pdata));
        memset(save_io, 0, sizeof(double)*job.nprocs);
        memset(save_md, 0, sizeof(double)*job.nprocs);
        pdata.rank_cumul_io_time = save_io;
        pdata.rank_cumul_md_time = save_md;

        HASH_ITER(hlink, file_hash, curr, tmp_file)
        {
            HASH_DELETE(hlink, file_hash, curr);
            if(curr->rec_dat) free(curr->rec_dat);
            free(curr);
        }
    }
    if(empty_mods == DARSHAN_MAX_MODS)
        printf("\n# no module data available.\n");

    darshan_log_close(fd);

    free(mod_buf);
    free(pdata.rank_cumul_io_time);
    free(pdata.rank_cumul_md_time);

    /* free record hash data */
    HASH_ITER(hlink, rec_hash, ref, tmp_ref)
    {
        HASH_DELETE(hlink, rec_hash, ref);
        free(ref->rec.name);
        free(ref);
    }

    /* free mount info */
    for(i=0; i<mount_count; i++)
    {
        free(mnt_pts[i]);
        free(fs_types[i]);
    }
    if(mount_count > 0)
    {
        free(mnt_pts);
        free(fs_types);
    }

    return(0);
}

void posix_accum_file(struct darshan_posix_file *pfile,
                      hash_entry_t *hfile,
                      int64_t nprocs)
{
    int i, j;
    int set;
    int min_ndx;
    int64_t min;
    struct darshan_posix_file* tmp;

    hfile->procs += 1;

    if(pfile->rank == -1)
    {
        hfile->slowest_time = pfile->fcounters[POSIX_F_SLOWEST_RANK_TIME];
    }
    else
    {
        hfile->slowest_time = max(hfile->slowest_time, 
            (pfile->fcounters[POSIX_F_META_TIME] +
            pfile->fcounters[POSIX_F_READ_TIME] +
            pfile->fcounters[POSIX_F_WRITE_TIME]));
    }

    if(pfile->rank == -1)
    {
        hfile->procs = nprocs;
        hfile->type |= FILETYPE_SHARED;

    }
    else if(hfile->procs > 1)
    {
        hfile->type &= (~FILETYPE_UNIQUE);
        hfile->type |= FILETYPE_PARTSHARED;
    }
    else
    {
        hfile->type |= FILETYPE_UNIQUE;
    }

    hfile->cumul_time += pfile->fcounters[POSIX_F_META_TIME] +
                         pfile->fcounters[POSIX_F_READ_TIME] +
                         pfile->fcounters[POSIX_F_WRITE_TIME];

    if(hfile->rec_dat == NULL)
    {
        hfile->rec_dat = malloc(sizeof(struct darshan_posix_file));
        assert(hfile->rec_dat);
        tmp = (struct darshan_posix_file*)hfile->rec_dat;
        memset(tmp, 0, sizeof(struct darshan_posix_file));
    }

    for(i = 0; i < POSIX_NUM_INDICES; i++)
    {
        switch(i)
        {
        case POSIX_MODE:
        case POSIX_MEM_ALIGNMENT:
        case POSIX_FILE_ALIGNMENT:
            if(POSIX_FILE_PARTIAL(tmp))
                tmp->counters[i] = pfile->counters[i];
            break;
        case POSIX_MAX_BYTE_READ:
        case POSIX_MAX_BYTE_WRITTEN:
            if (tmp->counters[i] < pfile->counters[i])
            {
                tmp->counters[i] = pfile->counters[i];
            }
            break;
        case POSIX_STRIDE1_STRIDE:
        case POSIX_STRIDE2_STRIDE:
        case POSIX_STRIDE3_STRIDE:
        case POSIX_STRIDE4_STRIDE:
        case POSIX_ACCESS1_ACCESS:
        case POSIX_ACCESS2_ACCESS:
        case POSIX_ACCESS3_ACCESS:
        case POSIX_ACCESS4_ACCESS:
           /*
            * do nothing here because these will be stored
            * when the _COUNT is accessed.
            */
           break;
        case POSIX_STRIDE1_COUNT:
        case POSIX_STRIDE2_COUNT:
        case POSIX_STRIDE3_COUNT:
        case POSIX_STRIDE4_COUNT:
            set = 0;
            min_ndx = POSIX_STRIDE1_COUNT;
            min = tmp->counters[min_ndx];
            for(j = POSIX_STRIDE1_COUNT; j <= POSIX_STRIDE4_COUNT; j++)
            {
                if(tmp->counters[j-4] == pfile->counters[i-4])
                {
                    tmp->counters[j] += pfile->counters[i];
                    set = 1;
                    break;
                }
                if(tmp->counters[j] < min)
                {
                    min_ndx = j;
                    min = tmp->counters[j];
                }
            }
            if(!set && (pfile->counters[i] > min))
            {
                tmp->counters[min_ndx] = pfile->counters[i];
                tmp->counters[min_ndx-4] = pfile->counters[i-4];
            }
            break;
        case POSIX_ACCESS1_COUNT:
        case POSIX_ACCESS2_COUNT:
        case POSIX_ACCESS3_COUNT:
        case POSIX_ACCESS4_COUNT:
            set = 0;
            min_ndx = POSIX_ACCESS1_COUNT;
            min = tmp->counters[min_ndx];
            for(j = POSIX_ACCESS1_COUNT; j <= POSIX_ACCESS4_COUNT; j++)
            {
                if(tmp->counters[j-4] == pfile->counters[i-4])
                {
                    tmp->counters[j] += pfile->counters[i];
                    set = 1;
                    break;
                }
                if(tmp->counters[j] < min)
                {
                    min_ndx = j;
                    min = tmp->counters[j];
                }
            }
            if(!set && (pfile->counters[i] > min))
            {
                tmp->counters[i] = pfile->counters[i];
                tmp->counters[i-4] = pfile->counters[i-4];
            }
            break;
        case POSIX_FASTEST_RANK:
        case POSIX_SLOWEST_RANK:
        case POSIX_FASTEST_RANK_BYTES:
        case POSIX_SLOWEST_RANK_BYTES:
            tmp->counters[i] = 0;
            break;
        case POSIX_MAX_READ_TIME_SIZE:
        case POSIX_MAX_WRITE_TIME_SIZE:
            break;
        default:
            tmp->counters[i] += pfile->counters[i];
            break;
        }
    }

    for(i = 0; i < POSIX_F_NUM_INDICES; i++)
    {
        switch(i)
        {
            case POSIX_F_OPEN_TIMESTAMP:
            case POSIX_F_READ_START_TIMESTAMP:
            case POSIX_F_WRITE_START_TIMESTAMP:
                if(tmp->fcounters[i] == 0 || 
                    tmp->fcounters[i] > pfile->fcounters[i])
                {
                    tmp->fcounters[i] = pfile->fcounters[i];
                }
                break;
            case POSIX_F_READ_END_TIMESTAMP:
            case POSIX_F_WRITE_END_TIMESTAMP:
            case POSIX_F_CLOSE_TIMESTAMP:
                if(tmp->fcounters[i] == 0 || 
                    tmp->fcounters[i] < pfile->fcounters[i])
                {
                    tmp->fcounters[i] = pfile->fcounters[i];
                }
                break;
            case POSIX_F_FASTEST_RANK_TIME:
            case POSIX_F_SLOWEST_RANK_TIME:
            case POSIX_F_VARIANCE_RANK_TIME:
            case POSIX_F_VARIANCE_RANK_BYTES:
                tmp->fcounters[i] = 0;
                break;
            case POSIX_F_MAX_READ_TIME:
                if(tmp->fcounters[i] < pfile->fcounters[i])
                {
                    tmp->fcounters[i] = pfile->fcounters[i];
                    tmp->counters[POSIX_MAX_READ_TIME_SIZE] =
                        pfile->counters[POSIX_MAX_READ_TIME_SIZE];
                }
                break;
            case POSIX_F_MAX_WRITE_TIME:
                if(tmp->fcounters[i] < pfile->fcounters[i])
                {
                    tmp->fcounters[i] = pfile->fcounters[i];
                    tmp->counters[POSIX_MAX_WRITE_TIME_SIZE] =
                        pfile->counters[POSIX_MAX_WRITE_TIME_SIZE];
                }
                break;
            default:
                tmp->fcounters[i] += pfile->fcounters[i];
                break;
        }
    }

    return;
}

void mpiio_accum_file(struct darshan_mpiio_file *mfile,
                      hash_entry_t *hfile,
                      int64_t nprocs)
{
    int i, j;
    int set;
    int min_ndx;
    int64_t min;
    struct darshan_mpiio_file* tmp;

    hfile->procs += 1;

    if(mfile->rank == -1)
    {
        hfile->slowest_time = mfile->fcounters[MPIIO_F_SLOWEST_RANK_TIME];
    }
    else
    {
        hfile->slowest_time = max(hfile->slowest_time, 
            (mfile->fcounters[MPIIO_F_META_TIME] +
            mfile->fcounters[MPIIO_F_READ_TIME] +
            mfile->fcounters[MPIIO_F_WRITE_TIME]));
    }

    if(mfile->rank == -1)
    {
        hfile->procs = nprocs;
        hfile->type |= FILETYPE_SHARED;

    }
    else if(hfile->procs > 1)
    {
        hfile->type &= (~FILETYPE_UNIQUE);
        hfile->type |= FILETYPE_PARTSHARED;
    }
    else
    {
        hfile->type |= FILETYPE_UNIQUE;
    }

    hfile->cumul_time += mfile->fcounters[MPIIO_F_META_TIME] +
                         mfile->fcounters[MPIIO_F_READ_TIME] +
                         mfile->fcounters[MPIIO_F_WRITE_TIME];

    if(hfile->rec_dat == NULL)
    {
        hfile->rec_dat = malloc(sizeof(struct darshan_mpiio_file));
        assert(hfile->rec_dat);
        tmp = (struct darshan_mpiio_file*)hfile->rec_dat;
        memset(tmp, 0, sizeof(struct darshan_mpiio_file));
    }

    for(i = 0; i < MPIIO_NUM_INDICES; i++)
    {
        switch(i)
        {
        case MPIIO_MODE:
            tmp->counters[i] = mfile->counters[i];
            break;
        case MPIIO_ACCESS1_ACCESS:
        case MPIIO_ACCESS2_ACCESS:
        case MPIIO_ACCESS3_ACCESS:
        case MPIIO_ACCESS4_ACCESS:
            /*
             * do nothing here because these will be stored
             * when the _COUNT is accessed.
             */
            break;
        case MPIIO_ACCESS1_COUNT:
        case MPIIO_ACCESS2_COUNT:
        case MPIIO_ACCESS3_COUNT:
        case MPIIO_ACCESS4_COUNT:
            set = 0;
            min_ndx = MPIIO_ACCESS1_COUNT;
            min = tmp->counters[min_ndx];
            for(j = MPIIO_ACCESS1_COUNT; j <= MPIIO_ACCESS4_COUNT; j++)
            {
                if(tmp->counters[j-4] == mfile->counters[i-4])
                {
                    tmp->counters[j] += mfile->counters[i];
                    set = 1;
                    break;
                }
                if(tmp->counters[j] < min)
                {
                    min_ndx = j;
                    min = tmp->counters[j];
                }
            }
            if(!set && (mfile->counters[i] > min))
            {
                tmp->counters[i] = mfile->counters[i];
                tmp->counters[i-4] = mfile->counters[i-4];
            }
            break;
        case MPIIO_FASTEST_RANK:
        case MPIIO_SLOWEST_RANK:
        case MPIIO_FASTEST_RANK_BYTES:
        case MPIIO_SLOWEST_RANK_BYTES:
            tmp->counters[i] = 0;
            break;
        case MPIIO_MAX_READ_TIME_SIZE:
        case MPIIO_MAX_WRITE_TIME_SIZE:
            break;
        default:
            tmp->counters[i] += mfile->counters[i];
            break;
        }
    }

    for(i = 0; i < MPIIO_F_NUM_INDICES; i++)
    {
        switch(i)
        {
            case MPIIO_F_OPEN_TIMESTAMP:
            case MPIIO_F_READ_START_TIMESTAMP:
            case MPIIO_F_WRITE_START_TIMESTAMP:
                if(tmp->fcounters[i] == 0 || 
                    tmp->fcounters[i] > mfile->fcounters[i])
                {
                    tmp->fcounters[i] = mfile->fcounters[i];
                }
                break;
            case MPIIO_F_READ_END_TIMESTAMP:
            case MPIIO_F_WRITE_END_TIMESTAMP:
            case MPIIO_F_CLOSE_TIMESTAMP:
                if(tmp->fcounters[i] == 0 || 
                    tmp->fcounters[i] < mfile->fcounters[i])
                {
                    tmp->fcounters[i] = mfile->fcounters[i];
                }
                break;
            case MPIIO_F_FASTEST_RANK_TIME:
            case MPIIO_F_SLOWEST_RANK_TIME:
            case MPIIO_F_VARIANCE_RANK_TIME:
            case MPIIO_F_VARIANCE_RANK_BYTES:
                tmp->fcounters[i] = 0;
                break;
            case MPIIO_F_MAX_READ_TIME:
                if(tmp->fcounters[i] < mfile->fcounters[i])
                {
                    tmp->fcounters[i] = mfile->fcounters[i];
                    tmp->counters[MPIIO_MAX_READ_TIME_SIZE] =
                        mfile->counters[MPIIO_MAX_READ_TIME_SIZE];
                }
                break;
            case MPIIO_F_MAX_WRITE_TIME:
                if(tmp->fcounters[i] < mfile->fcounters[i])
                {
                    tmp->fcounters[i] = mfile->fcounters[i];
                    tmp->counters[MPIIO_MAX_WRITE_TIME_SIZE] =
                        mfile->counters[MPIIO_MAX_WRITE_TIME_SIZE];
                }
                break;
            default:
                tmp->fcounters[i] += mfile->fcounters[i];
                break;
        }
    }

    return;
}

void posix_accum_perf(struct darshan_posix_file *pfile,
                      perf_data_t *pdata)
{
    pdata->total_bytes += pfile->counters[POSIX_BYTES_READ] +
                          pfile->counters[POSIX_BYTES_WRITTEN];

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
    if(pfile->rank == -1)
    {
        /* by_open */
        if(pfile->fcounters[POSIX_F_CLOSE_TIMESTAMP] >
            pfile->fcounters[POSIX_F_OPEN_TIMESTAMP])
        {
            pdata->shared_time_by_open +=
                pfile->fcounters[POSIX_F_CLOSE_TIMESTAMP] -
                pfile->fcounters[POSIX_F_OPEN_TIMESTAMP];
        }

        /* by_open_lastio */
        if(pfile->fcounters[POSIX_F_READ_END_TIMESTAMP] >
            pfile->fcounters[POSIX_F_WRITE_END_TIMESTAMP])
        {
            /* be careful: file may have been opened but not read or written */
            if(pfile->fcounters[POSIX_F_READ_END_TIMESTAMP] > pfile->fcounters[POSIX_F_OPEN_TIMESTAMP])
            {
                pdata->shared_time_by_open_lastio += 
                    pfile->fcounters[POSIX_F_READ_END_TIMESTAMP] - 
                    pfile->fcounters[POSIX_F_OPEN_TIMESTAMP];
            }
        }
        else
        {
            /* be careful: file may have been opened but not read or written */
            if(pfile->fcounters[POSIX_F_WRITE_END_TIMESTAMP] > pfile->fcounters[POSIX_F_OPEN_TIMESTAMP])
            {
                pdata->shared_time_by_open_lastio += 
                    pfile->fcounters[POSIX_F_WRITE_END_TIMESTAMP] - 
                    pfile->fcounters[POSIX_F_OPEN_TIMESTAMP];
            }
        }

        pdata->shared_time_by_cumul +=
            pfile->fcounters[POSIX_F_META_TIME] +
            pfile->fcounters[POSIX_F_READ_TIME] +
            pfile->fcounters[POSIX_F_WRITE_TIME];
        pdata->shared_meta_time += pfile->fcounters[POSIX_F_META_TIME];

        /* by_slowest */
        pdata->shared_time_by_slowest +=
            pfile->fcounters[POSIX_F_SLOWEST_RANK_TIME];
    }

    /*
     * Calculation of Unique File Time
     *   record the data for each file and sum it 
     */
    else
    {
        pdata->rank_cumul_io_time[pfile->rank] +=
            (pfile->fcounters[POSIX_F_META_TIME] +
            pfile->fcounters[POSIX_F_READ_TIME] +
            pfile->fcounters[POSIX_F_WRITE_TIME]);
        pdata->rank_cumul_md_time[pfile->rank] += pfile->fcounters[POSIX_F_META_TIME];
    }

    return;
}

void mpiio_accum_perf(struct darshan_mpiio_file *mfile,
                      perf_data_t *pdata)
{
    pdata->total_bytes += mfile->counters[MPIIO_BYTES_READ] +
                          mfile->counters[MPIIO_BYTES_WRITTEN];

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
    if(mfile->rank == -1)
    {
        /* by_open */
        if(mfile->fcounters[MPIIO_F_CLOSE_TIMESTAMP] >
            mfile->fcounters[MPIIO_F_OPEN_TIMESTAMP])
        {
            pdata->shared_time_by_open +=
                mfile->fcounters[MPIIO_F_CLOSE_TIMESTAMP] -
                mfile->fcounters[MPIIO_F_OPEN_TIMESTAMP];
        }

        /* by_open_lastio */
        if(mfile->fcounters[MPIIO_F_READ_END_TIMESTAMP] >
            mfile->fcounters[MPIIO_F_WRITE_END_TIMESTAMP])
        {
            /* be careful: file may have been opened but not read or written */
            if(mfile->fcounters[MPIIO_F_READ_END_TIMESTAMP] > mfile->fcounters[MPIIO_F_OPEN_TIMESTAMP])
            {
                pdata->shared_time_by_open_lastio += 
                    mfile->fcounters[MPIIO_F_READ_END_TIMESTAMP] - 
                    mfile->fcounters[MPIIO_F_OPEN_TIMESTAMP];
            }
        }
        else
        {
            /* be careful: file may have been opened but not read or written */
            if(mfile->fcounters[MPIIO_F_WRITE_END_TIMESTAMP] > mfile->fcounters[MPIIO_F_OPEN_TIMESTAMP])
            {
                pdata->shared_time_by_open_lastio += 
                    mfile->fcounters[MPIIO_F_WRITE_END_TIMESTAMP] - 
                    mfile->fcounters[MPIIO_F_OPEN_TIMESTAMP];
            }
        }

        pdata->shared_time_by_cumul +=
            mfile->fcounters[MPIIO_F_META_TIME] +
            mfile->fcounters[MPIIO_F_READ_TIME] +
            mfile->fcounters[MPIIO_F_WRITE_TIME];
        pdata->shared_meta_time += mfile->fcounters[MPIIO_F_META_TIME];

        /* by_slowest */
        pdata->shared_time_by_slowest +=
            mfile->fcounters[MPIIO_F_SLOWEST_RANK_TIME];
    }

    /*
     * Calculation of Unique File Time
     *   record the data for each file and sum it 
     */
    else
    {
        pdata->rank_cumul_io_time[mfile->rank] +=
            (mfile->fcounters[MPIIO_F_META_TIME] +
            mfile->fcounters[MPIIO_F_READ_TIME] +
            mfile->fcounters[MPIIO_F_WRITE_TIME]);
        pdata->rank_cumul_md_time[mfile->rank] += mfile->fcounters[MPIIO_F_META_TIME];
    }

    return;
}

void posix_calc_file(hash_entry_t *file_hash, 
                     file_data_t *fdata)
{
    hash_entry_t *curr = NULL;
    hash_entry_t *tmp = NULL;
    struct darshan_posix_file *file_rec;

    memset(fdata, 0, sizeof(*fdata));
    HASH_ITER(hlink, file_hash, curr, tmp)
    {
        int64_t bytes;
        int64_t r;
        int64_t w;

        file_rec = (struct darshan_posix_file*)curr->rec_dat;
        assert(file_rec);

        bytes = file_rec->counters[POSIX_BYTES_READ] +
                file_rec->counters[POSIX_BYTES_WRITTEN];

        r = (file_rec->counters[POSIX_READS]+
             file_rec->counters[POSIX_FREADS]);

        w = (file_rec->counters[POSIX_WRITES]+
             file_rec->counters[POSIX_FWRITES]);

        fdata->total += 1;
        fdata->total_size += bytes;
        fdata->total_max = max(fdata->total_max, bytes);

        if (r && !w)
        {
            fdata->read_only += 1;
            fdata->read_only_size += bytes;
            fdata->read_only_max = max(fdata->read_only_max, bytes);
        }

        if (!r && w)
        {
            fdata->write_only += 1;
            fdata->write_only_size += bytes;
            fdata->write_only_max = max(fdata->write_only_max, bytes);
        }

        if (r && w)
        {
            fdata->read_write += 1;
            fdata->read_write_size += bytes;
            fdata->read_write_max = max(fdata->read_write_max, bytes);
        }

        if ((curr->type & (FILETYPE_SHARED|FILETYPE_PARTSHARED)))
        {
            fdata->shared += 1;
            fdata->shared_size += bytes;
            fdata->shared_max = max(fdata->shared_max, bytes);
        }

        if ((curr->type & (FILETYPE_UNIQUE)))
        {
            fdata->unique += 1;
            fdata->unique_size += bytes;
            fdata->unique_max = max(fdata->unique_max, bytes);
        }
    }

    return;
}

void mpiio_calc_file(hash_entry_t *file_hash, 
                     file_data_t *fdata)
{
    hash_entry_t *curr = NULL;
    hash_entry_t *tmp = NULL;
    struct darshan_mpiio_file *file_rec;

    memset(fdata, 0, sizeof(*fdata));
    HASH_ITER(hlink, file_hash, curr, tmp)
    {
        int64_t bytes;
        int64_t r;
        int64_t w;

        file_rec = (struct darshan_mpiio_file*)curr->rec_dat;
        assert(file_rec);

        bytes = file_rec->counters[MPIIO_BYTES_READ] +
                file_rec->counters[MPIIO_BYTES_WRITTEN];

        r = (file_rec->counters[MPIIO_INDEP_READS]+
             file_rec->counters[MPIIO_COLL_READS] +
             file_rec->counters[MPIIO_SPLIT_READS] +
             file_rec->counters[MPIIO_NB_READS]);

        w = (file_rec->counters[MPIIO_INDEP_WRITES]+
             file_rec->counters[MPIIO_COLL_WRITES] +
             file_rec->counters[MPIIO_SPLIT_WRITES] +
             file_rec->counters[MPIIO_NB_WRITES]);

        fdata->total += 1;
        fdata->total_size += bytes;
        fdata->total_max = max(fdata->total_max, bytes);

        if (r && !w)
        {
            fdata->read_only += 1;
            fdata->read_only_size += bytes;
            fdata->read_only_max = max(fdata->read_only_max, bytes);
        }

        if (!r && w)
        {
            fdata->write_only += 1;
            fdata->write_only_size += bytes;
            fdata->write_only_max = max(fdata->write_only_max, bytes);
        }

        if (r && w)
        {
            fdata->read_write += 1;
            fdata->read_write_size += bytes;
            fdata->read_write_max = max(fdata->read_write_max, bytes);
        }

        if ((curr->type & (FILETYPE_SHARED|FILETYPE_PARTSHARED)))
        {
            fdata->shared += 1;
            fdata->shared_size += bytes;
            fdata->shared_max = max(fdata->shared_max, bytes);
        }

        if ((curr->type & (FILETYPE_UNIQUE)))
        {
            fdata->unique += 1;
            fdata->unique_size += bytes;
            fdata->unique_max = max(fdata->unique_max, bytes);
        }
    }

    return;
}

void calc_perf(perf_data_t *pdata,
               int64_t nprocs)
{
    int64_t i;

    pdata->shared_time_by_cumul =
        pdata->shared_time_by_cumul / (double)nprocs;

    pdata->shared_meta_time = pdata->shared_meta_time / (double)nprocs;

    for (i=0; i<nprocs; i++)
    {
        if (pdata->rank_cumul_io_time[i] > pdata->slowest_rank_time)
        {
            pdata->slowest_rank_time = pdata->rank_cumul_io_time[i];
            pdata->slowest_rank_meta_time = pdata->rank_cumul_md_time[i];
            pdata->slowest_rank_rank = i;
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
                                      pdata->slowest_rank_meta_time +
                                      pdata->shared_time_by_slowest);

    return;
}

void posix_print_total_file(struct darshan_posix_file *pfile)
{
    int i;
    printf("\n");
    for(i = 0; i < POSIX_NUM_INDICES; i++)
    {
        printf("total_%s: %"PRId64"\n",
            posix_counter_names[i], pfile->counters[i]);
    }
    for(i = 0; i < POSIX_F_NUM_INDICES; i++)
    {
        printf("total_%s: %lf\n",
            posix_f_counter_names[i], pfile->fcounters[i]);
    }
    return;
}

void mpiio_print_total_file(struct darshan_mpiio_file *mfile)
{
    int i;
    printf("\n");
    for(i = 0; i < MPIIO_NUM_INDICES; i++)
    {
        printf("total_%s: %"PRId64"\n",
            mpiio_counter_names[i], mfile->counters[i]);
    }
    for(i = 0; i < MPIIO_F_NUM_INDICES; i++)
    {
        printf("total_%s: %lf\n",
            mpiio_f_counter_names[i], mfile->fcounters[i]);
    }
    return;
}

void posix_file_list(hash_entry_t *file_hash,
                     struct darshan_record_ref *rec_hash,
                     int detail_flag)
{
    hash_entry_t *curr = NULL;
    hash_entry_t *tmp = NULL;
    struct darshan_posix_file *file_rec = NULL;
    struct darshan_record_ref *ref = NULL;
    int i;

    /* list of columns:
     *
     * normal mode
     * - file id
     * - file name
     * - nprocs
     * - slowest I/O time
     * - average cumulative I/O time
     *
     * detailed mode
     * - first open
     * - first read
     * - first write
     * - last read
     * - last write
     * - last close
     * - POSIX opens
     * - r histogram
     * - w histogram
     */

    if(detail_flag)
        printf("\n# Per-file summary of I/O activity (detailed).\n");
    else
        printf("\n# Per-file summary of I/O activity.\n");

    printf("# <record_id>: darshan record id for this file\n");
    printf("# <file_name>: file name\n");
    printf("# <nprocs>: number of processes that opened the file\n");
    printf("# <slowest>: (estimated) time in seconds consumed in IO by slowest process\n");
    printf("# <avg>: average time in seconds consumed in IO per process\n");
    if(detail_flag)
    {
        printf("# <start_{open/read/write}>: start timestamp of first open, read, or write\n");
        printf("# <end_{read/write/close}>: end timestamp of last read, write, or close\n");
        printf("# <posix_opens>: POSIX open calls\n");
        printf("# <POSIX_SIZE_READ_*>: POSIX read size histogram\n");
        printf("# <POSIX_SIZE_WRITE_*>: POSIX write size histogram\n");
    }
    
    printf("\n# <file_id>\t<file_name>\t<nprocs>\t<slowest>\t<avg>");
    if(detail_flag)
    {
        printf("\t<start_open>\t<start_read>\t<start_write>");
        printf("\t<end_read>\t<end_write>\t<end_close>\t<posix_opens>");
        for(i=POSIX_SIZE_READ_0_100; i<= POSIX_SIZE_WRITE_1G_PLUS; i++)
            printf("\t<%s>", posix_counter_names[i]);
    }
    printf("\n");

    HASH_ITER(hlink, file_hash, curr, tmp)
    {
        file_rec = (struct darshan_posix_file*)curr->rec_dat;
        assert(file_rec);

        HASH_FIND(hlink, rec_hash, &(curr->rec_id), sizeof(darshan_record_id), ref);
        assert(ref);

        printf("%" PRIu64 "\t%s\t%" PRId64 "\t%f\t%f",
            curr->rec_id,
            ref->rec.name,
            curr->procs,
            curr->slowest_time,
            curr->cumul_time/(double)curr->procs);

        if(detail_flag)
        {
            for(i=POSIX_F_OPEN_TIMESTAMP; i<=POSIX_F_CLOSE_TIMESTAMP; i++)
            {
                printf("\t%f", file_rec->fcounters[i]);
            }
            printf("\t%" PRId64, file_rec->counters[POSIX_OPENS]);
            for(i=POSIX_SIZE_READ_0_100; i<= POSIX_SIZE_WRITE_1G_PLUS; i++)
                printf("\t%" PRId64, file_rec->counters[i]);
        }
        printf("\n");
    }

    return;
}

void mpiio_file_list(hash_entry_t *file_hash,
                     struct darshan_record_ref *rec_hash,
                     int detail_flag)
{
    hash_entry_t *curr = NULL;
    hash_entry_t *tmp = NULL;
    struct darshan_mpiio_file *file_rec = NULL;
    struct darshan_record_ref *ref = NULL;
    int i;

    /* list of columns:
     *
     * normal mode
     * - file id
     * - file name
     * - nprocs
     * - slowest I/O time
     * - average cumulative I/O time
     *
     * detailed mode
     * - first open
     * - first read
     * - first write
     * - last read
     * - last write
     * - last close
     * - MPI indep opens
     * - MPI coll opens
     * - r histogram
     * - w histogram
     */

    if(detail_flag)
        printf("\n# Per-file summary of I/O activity (detailed).\n");
    else
        printf("\n# Per-file summary of I/O activity.\n");

    printf("# <record_id>: darshan record id for this file\n");
    printf("# <file_name>: file name\n");
    printf("# <nprocs>: number of processes that opened the file\n");
    printf("# <slowest>: (estimated) time in seconds consumed in IO by slowest process\n");
    printf("# <avg>: average time in seconds consumed in IO per process\n");
    if(detail_flag)
    {
        printf("# <start_{open/read/write}>: start timestamp of first open, read, or write\n");
        printf("# <end_{read/write/close}>: end timestamp of last read, write, or close\n");
        printf("# <mpi_indep_opens>: independent MPI_File_open calls\n");
        printf("# <mpi_coll_opens>: collective MPI_File_open calls\n");
        printf("# <MPIIO_SIZE_READ_AGG_*>: MPI-IO aggregate read size histogram\n");
        printf("# <MPIIO_SIZE_WRITE_AGG_*>: MPI-IO aggregate write size histogram\n");
    }
    
    printf("\n# <file_id>\t<file_name>\t<nprocs>\t<slowest>\t<avg>");
    if(detail_flag)
    {
        printf("\t<start_open>\t<start_read>\t<start_write>");
        printf("\t<end_read>\t<end_write>\t<end_close>");
        printf("\t<mpi_indep_opens>\t<mpi_coll_opens>");
        for(i=MPIIO_SIZE_READ_AGG_0_100; i<= MPIIO_SIZE_WRITE_AGG_1G_PLUS; i++)
            printf("\t<%s>", mpiio_counter_names[i]);
    }
    printf("\n");

    HASH_ITER(hlink, file_hash, curr, tmp)
    {
        file_rec = (struct darshan_mpiio_file*)curr->rec_dat;
        assert(file_rec);

        HASH_FIND(hlink, rec_hash, &(curr->rec_id), sizeof(darshan_record_id), ref);
        assert(ref);

        printf("%" PRIu64 "\t%s\t%" PRId64 "\t%f\t%f",
            curr->rec_id,
            ref->rec.name,
            curr->procs,
            curr->slowest_time,
            curr->cumul_time/(double)curr->procs);

        if(detail_flag)
        {
            for(i=MPIIO_F_OPEN_TIMESTAMP; i<=MPIIO_F_CLOSE_TIMESTAMP; i++)
            {
                printf("\t%f", file_rec->fcounters[i]);
            }
            printf("\t%" PRId64 "\t%" PRId64, file_rec->counters[MPIIO_INDEP_OPENS],
                file_rec->counters[MPIIO_COLL_OPENS]);
            for(i=MPIIO_SIZE_READ_AGG_0_100; i<= MPIIO_SIZE_WRITE_AGG_1G_PLUS; i++)
                printf("\t%" PRId64, file_rec->counters[i]);
        }
        printf("\n");
    }

    return;
}

/*
 * Local variables:
 *  c-indent-level: 4
 *  c-basic-offset: 4
 * End:
 *
 * vim: ts=8 sts=4 sw=4 expandtab
 */
