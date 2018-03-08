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

/*
 * Prototypes
 */
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
    char *comp_str;
    char tmp_string[4096] = {0};
    darshan_fd fd;
    struct darshan_job job;
    struct darshan_name_record_ref *name_hash = NULL;
    struct darshan_name_record_ref *ref, *tmp_ref;
    int mount_count;
    struct darshan_mnt_info *mnt_data_array;
    time_t tmp_time = 0;
    int64_t run_time = 0;
    char *token;
    char *save;
    char buffer[DARSHAN_JOB_METADATA_LEN];
    int empty_mods = 0;
    char *mod_buf;

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
        return(-1);

    /* read darshan job info */
    ret = darshan_log_get_job(fd, &job);
    if(ret < 0)
    {
        darshan_log_close(fd);
        return(-1);
    }

    /* get the original command line for this job */
    ret = darshan_log_get_exe(fd, tmp_string);
    if(ret < 0)
    {
        darshan_log_close(fd);
        return(-1);
    }

    /* get the mount information for this log */
    ret = darshan_log_get_mounts(fd, &mnt_data_array, &mount_count);
    if(ret < 0)
    {
        darshan_log_close(fd);
        return(-1);
    }

    /* read hash of darshan records */
    ret = darshan_log_get_namehash(fd, &name_hash);
    if(ret < 0)
    {
        darshan_log_close(fd);
        return(-1);
    }

    /* print any warnings related to this log file version */
    darshan_log_print_version_warnings(fd->version);

    if(fd->comp_type == DARSHAN_ZLIB_COMP)
        comp_str = "ZLIB";
    else if (fd->comp_type == DARSHAN_BZIP2_COMP)
        comp_str = "BZIP2";
    else if (fd->comp_type == DARSHAN_NO_COMP)
        comp_str = "NONE";
    else
        comp_str = "UNKNOWN";

    /* print job summary */
    printf("# darshan log version: %s\n", fd->version);
    printf("# compression method: %s\n", comp_str);
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
    if(job.end_time >= job.start_time)
        run_time = job.end_time - job.start_time + 1;
    printf("# run time: %" PRId64 "\n", run_time);
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

    /* print breakdown of each log file region's contribution to file size */
    printf("\n# log file regions\n");
    printf("# -------------------------------------------------------\n");
    printf("# header: %zu bytes (uncompressed)\n", sizeof(struct darshan_header));
    printf("# job data: %zu bytes (compressed)\n", fd->job_map.len);
    printf("# record table: %zu bytes (compressed)\n", fd->name_map.len);
    for(i=0; i<DARSHAN_MAX_MODS; i++)
    {
        if(fd->mod_map[i].len)
        {
            printf("# %s module: %zu bytes (compressed), ver=%d\n",
                darshan_module_names[i], fd->mod_map[i].len, fd->mod_ver[i]);
        }
    }

#if 0
    printf("\n# defs ---\n");
    printf("DARSHAN_EXE_LEN = %d\n", DARSHAN_EXE_LEN);
    printf("DARSHAN_APXC_MAGIC = 0x%" PRIu64 "\n", DARSHAN_APXC_MAGIC);
    printf("APXC_NUM_INDICES = %d\n", APXC_NUM_INDICES);
    printf("POSIX_F_NUM_INDICES = %d\n", POSIX_F_NUM_INDICES);
    printf("POSIX_NUM_INDICES = %d\n", POSIX_NUM_INDICES);
    printf("STDIO_F_NUM_INDICES = %d\n", STDIO_F_NUM_INDICES);
    printf("STDIO_NUM_INDICES = %d\n", STDIO_NUM_INDICES);
    printf("MPIIO_F_NUM_INDICES = %d\n", MPIIO_F_NUM_INDICES);
    printf("MPIIO_NUM_INDICES = %d\n", MPIIO_NUM_INDICES);
    printf("PNETCDF_F_NUM_INDICES = %d\n", PNETCDF_F_NUM_INDICES);
    printf("PNETCDF_NUM_INDICES = %d\n", PNETCDF_NUM_INDICES);
    printf("HDF5_F_NUM_INDICES = %d\n", HDF5_F_NUM_INDICES);
    printf("HDF5_NUM_INDICES = %d\n", HDF5_NUM_INDICES);
    printf("BGQ_F_NUM_INDICES = %d\n", BGQ_F_NUM_INDICES);
    printf("BGQ_NUM_INDICES = %d\n", BGQ_NUM_INDICES);
#endif

    /* print table of mounted file systems */
    printf("\n# mounted file systems (mount point and fs type)\n");
    printf("# -------------------------------------------------------\n");
    for(i=0; i<mount_count; i++)
    {
        printf("# mount entry:\t%s\t%s\n", mnt_data_array[i].mnt_path,
            mnt_data_array[i].mnt_type);
    }

    if(mask & OPTION_BASE)
    {
        printf("\n# description of columns:\n");
        printf("#   <module>: module responsible for this I/O record.\n");
        printf("#   <rank>: MPI rank.  -1 indicates that the file is shared\n");
        printf("#      across all processes and statistics are aggregated.\n");
        printf("#   <record id>: hash of the record's file path\n");
        printf("#   <counter name> and <counter value>: statistical counters.\n");
        printf("#      A value of -1 indicates that Darshan could not monitor\n");
        printf("#      that counter, and its value should be ignored.\n");
        printf("#   <file name>: full file path for the record.\n");
        printf("#   <mount pt>: mount point that the file resides on.\n");
        printf("#   <fs type>: type of file system that the file resides on.\n");
    }

    /* warn user if this log file is incomplete */
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

    mod_buf = malloc(DEF_MOD_BUF_SIZE);
    if (!mod_buf) {
        darshan_log_close(fd);
        return(-1);
    }

    for(i=0; i<DARSHAN_MAX_MODS; i++)
    {
        struct darshan_base_record *base_rec;
        void *save_io, *save_md;

        /* check each module for any data */
        if(fd->mod_map[i].len == 0)
        {
            empty_mods++;
            continue;
        }
        /* skip modules with no logutil definitions */
        else if(!mod_logutils[i])
        {
            fprintf(stderr, "Warning: no log utility handlers defined "
                "for module %s, SKIPPING.\n", darshan_module_names[i]);
            continue;
        }
        /* always ignore DXT modules -- those have a standalone parsing utility */
        else if (i == DXT_POSIX_MOD || i == DXT_MPIIO_MOD)
            continue;
        /* currently only POSIX, MPIIO, and STDIO modules support non-base
         * parsing
         */
        else if((i != DARSHAN_POSIX_MOD) && (i != DARSHAN_MPIIO_MOD) &&
                (i != DARSHAN_STDIO_MOD) && !(mask & OPTION_BASE))
            continue;

        /* this module has data to be parsed and printed */
        memset(mod_buf, 0, DEF_MOD_BUF_SIZE);

        printf("\n# *******************************************************\n");
        printf("# %s module data\n", darshan_module_names[i]);
        printf("# *******************************************************\n");

        /* print warning if this module only stored partial data */
        if(DARSHAN_MOD_FLAG_ISSET(fd->partial_flag, i))
            printf("\n# *WARNING*: The %s module contains incomplete data!\n"
                   "#            This happens when a module runs out of\n"
                   "#            memory to store new record data.\n",
                   darshan_module_names[i]);

        if(mask & OPTION_BASE)
        {
            /* print a header describing the module's I/O characterization data */
            if(mod_logutils[i]->log_print_description)
            {
                mod_logutils[i]->log_print_description(fd->mod_ver[i]);
                DARSHAN_PRINT_HEADER();
            }
        }

        /* loop over each of this module's records and print them */
        while(1)
        {
            char *mnt_pt = NULL;
            char *fs_type = NULL;
            char *rec_name = NULL;
            hash_entry_t *hfile = NULL;

            ret = mod_logutils[i]->log_get_record(fd, (void **)&mod_buf);
            if(ret < 1)
            {
                if(ret == -1)
                {
                    fprintf(stderr, "Error: failed to parse %s module record.\n",
                        darshan_module_names[i]);
                }
                break;
            }
            base_rec = (struct darshan_base_record *)mod_buf;

            /* get the pathname for this record */
            HASH_FIND(hlink, name_hash, &(base_rec->id), sizeof(darshan_record_id), ref);

            if(ref)
            {
                rec_name = ref->name_record->name;

                /* get mount point and fs type associated with this record */
                for(j=0; j<mount_count; j++)
                {
                    if(strncmp(mnt_data_array[j].mnt_path, rec_name,
                        strlen(mnt_data_array[j].mnt_path)) == 0)
                    {
                        mnt_pt = mnt_data_array[j].mnt_path;
                        fs_type = mnt_data_array[j].mnt_type;
                        break;
                    }
                }
            }
            else
            {
                if(i == DARSHAN_BGQ_MOD)
                    rec_name = "darshan-bgq-record";
            }

            if(!mnt_pt)
                mnt_pt = "UNKNOWN";
            if(!fs_type)
                fs_type = "UNKNOWN";

            if(mask & OPTION_BASE)
            {
                /* print the corresponding module data for this record */
                mod_logutils[i]->log_print_record(mod_buf, rec_name,
                    mnt_pt, fs_type);
            }

            /*
             * Compute derived statsitics for those modules that support it
             * and if the user requested it.
             */
            HASH_FIND(hlink, file_hash, &(base_rec->id), sizeof(darshan_record_id), hfile);
            if(!hfile)
            {
                hfile = malloc(sizeof(*hfile));
                if(!hfile)
                {
                    ret = -1;
                    goto cleanup;
                }

                /* init */
                memset(hfile, 0, sizeof(*hfile));
                hfile->rec_id = base_rec->id;
                hfile->type = 0;
                hfile->procs = 0;
                hfile->rec_dat = NULL;
                hfile->cumul_time = 0.0;
                hfile->slowest_time = 0.0;

                HASH_ADD(hlink, file_hash,rec_id, sizeof(darshan_record_id), hfile);
            }

            if (mod_logutils[i]->log_accum_file)
            {
                mod_logutils[i]->log_accum_file(mod_buf, &total, job.nprocs);
                mod_logutils[i]->log_accum_file(mod_buf, hfile, job.nprocs);
            }

            if (mod_logutils[i]->log_accum_perf)
            {
                mod_logutils[i]->log_accum_perf(mod_buf, &pdata);
            }

            memset(mod_buf, 0, DEF_MOD_BUF_SIZE);
        }
        if(ret == -1)
            continue; /* move on to the next module if there was an error with this one */

        /* Calculate more detailed stats if the module supports it and
         * the user requested it.
         */
        /* Total Calc */
        if ((mask & OPTION_TOTAL) &&
            (mod_logutils[i]->log_print_total_file))
        {
                mod_logutils[i]->log_print_total_file(total.rec_dat, fd->mod_ver[i]);
        }

        /* File Calc */
        if ((mask & OPTION_FILE) &&
            (mod_logutils[i]->log_calc_file))
        {
            mod_logutils[i]->log_calc_file(file_hash, &fdata);

            printf("\n# Total file counts\n");
            printf("# -----\n");
            printf("# <file_type>: type of file access:\n");
            printf("#    *read_only: file was only read\n");
            printf("#    *write_only: file was only written\n");
            printf("#    *read_write: file was read and written\n");
            printf("#    *unique: file was opened by a single process only\n");
            printf("#    *shared: file was accessed by a group of processes (maybe all processes)\n");
            printf("# <file_count> total number of files of this type\n");
            printf("# <total_bytes> total number of bytes moved to/from files of this type\n");
            printf("# <max_byte_offset> maximum byte offset accessed for a file of this type\n");
            printf("\n# <file_type> <file_count> <total_bytes> <max_byte_offset>\n");
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
        if ((mask & OPTION_PERF) &&
            (mod_logutils[i]->log_calc_perf))
        {
            mod_logutils[i]->log_calc_perf(&pdata, job.nprocs);

            printf("\n# performance\n");
            printf("# -----------\n");
            printf("# total_bytes: %" PRId64 "\n", pdata.total_bytes);
            printf("#\n");
            printf("# I/O timing for unique files (seconds):\n");
            printf("# ...........................\n");
            printf("# unique files: slowest_rank_io_time: %lf\n", pdata.slowest_rank_time);
            printf("# unique files: slowest_rank_meta_only_time: %lf\n", pdata.slowest_rank_meta_time);
            printf("# unique files: slowest_rank: %d\n", pdata.slowest_rank_rank);
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

        if (((mask & OPTION_FILE_LIST) ||
             (mask & OPTION_FILE_LIST_DETAILED)) &&
            (mod_logutils[i]->log_file_list))
        {
            int mode = (mask & OPTION_FILE_LIST_DETAILED) ? 1 : 0;
            mod_logutils[i]->log_file_list(file_hash, name_hash, mode);
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
    ret = 0;

cleanup:
    darshan_log_close(fd);
    free(pdata.rank_cumul_io_time);
    free(pdata.rank_cumul_md_time);
    free(mod_buf);

    /* free record hash data */
    HASH_ITER(hlink, name_hash, ref, tmp_ref)
    {
        HASH_DELETE(hlink, name_hash, ref);
        free(ref->name_record);
        free(ref);
    }

    /* free mount info */
    if(mount_count > 0)
    {
        free(mnt_data_array);
    }

    return(ret);
}

/*
 * Local variables:
 *  c-indent-level: 4
 *  c-basic-offset: 4
 * End:
 *
 * vim: ts=8 sts=4 sw=4 expandtab
 */
