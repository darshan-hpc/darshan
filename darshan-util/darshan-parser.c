/*
 * Copyright (C) 2015 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

#ifdef HAVE_CONFIG_H
# include "darshan-util-config.h"
#endif

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
#define OPTION_SHOW_INCOMPLETE  (1 << 7)  /* show what we have, even if log is incomplete */
#define OPTION_ALL (\
  OPTION_BASE|\
  OPTION_TOTAL|\
  OPTION_PERF|\
  OPTION_FILE|\
  OPTION_SHOW_INCOMPLETE)

#define FILETYPE_SHARED (1 << 0)
#define FILETYPE_UNIQUE (1 << 1)
#define FILETYPE_PARTSHARED (1 << 2)

#define max(a,b) (((a) > (b)) ? (a) : (b))

/*
 * Prototypes
 */
void posix_print_total_file(struct darshan_posix_file *pfile, int posix_ver);
void mpiio_print_total_file(struct darshan_mpiio_file *mfile, int mpiio_ver);
void stdio_print_total_file(struct darshan_stdio_file *pfile, int stdio_ver);
void dfs_print_total_file(struct darshan_dfs_file *pfile, int dfs_ver);
void daos_print_total_file(struct darshan_daos_object *pfile, int daos_ver);

int usage (char *exename)
{
    fprintf(stderr, "Usage: %s [options] <filename>\n", exename);
    fprintf(stderr, "    --all   : all sub-options are enabled\n");
    fprintf(stderr, "    --base  : darshan log field data [default]\n");
    fprintf(stderr, "    --file  : total file counts\n");
    fprintf(stderr, "    --perf  : derived perf data\n");
    fprintf(stderr, "    --total : aggregated darshan field data\n");
    fprintf(stderr, "    --show-incomplete : display results even if log is incomplete\n");

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
        {"perf",  0, NULL, OPTION_PERF},
        {"total", 0, NULL, OPTION_TOTAL},
        {"show-incomplete", 0, NULL, OPTION_SHOW_INCOMPLETE},
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
            case OPTION_PERF:
            case OPTION_TOTAL:
            case OPTION_SHOW_INCOMPLETE:
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
    if (mask == 0 || mask == OPTION_SHOW_INCOMPLETE)
    {
        mask |= OPTION_BASE;
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
    double run_time;
    char *token;
    char *save;
    char buffer[DARSHAN_JOB_METADATA_LEN];
    int empty_mods = 0;
    char *mod_buf;

    darshan_accumulator acc = NULL;
    struct darshan_derived_metrics metrics;

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
    printf("# start_time: %" PRId64 "\n", job.start_time_sec);
    tmp_time += job.start_time_sec;
    printf("# start_time_asci: %s", ctime(&tmp_time));
    printf("# end_time: %" PRId64 "\n", job.end_time_sec);
    tmp_time = 0;
    tmp_time += job.end_time_sec;
    printf("# end_time_asci: %s", ctime(&tmp_time));
    printf("# nprocs: %" PRId64 "\n", job.nprocs);
    darshan_log_get_job_runtime(fd, job, &run_time);
    printf("# run time: %.4lf\n", run_time);
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
    for(i=0; i<DARSHAN_KNOWN_MODULE_COUNT; i++)
    {
        if(fd->mod_map[i].len || DARSHAN_MOD_FLAG_ISSET(fd->partial_flag, i))
        {
            printf("# %s module: %zu bytes (compressed), ver=%d\n",
                darshan_module_names[i], fd->mod_map[i].len, fd->mod_ver[i]);
        }
    }
    for(i=DARSHAN_KNOWN_MODULE_COUNT; i<DARSHAN_MAX_MODS; i++)
    {
        if(fd->mod_map[i].len || DARSHAN_MOD_FLAG_ISSET(fd->partial_flag, i))
        {
            printf("# <UNKNOWN> module (id %d): %zu bytes (compressed), ver=%d\n",
                i, fd->mod_map[i].len, fd->mod_ver[i]);
        }
    }

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

    mod_buf = malloc(DEF_MOD_BUF_SIZE);
    if (!mod_buf) {
        darshan_log_close(fd);
        return(-1);
    }

    for(i=0; i<DARSHAN_MAX_MODS; i++)
    {
        struct darshan_base_record *base_rec;

        /* check each module for any data */
        if(fd->mod_map[i].len == 0)
        {
            empty_mods++;
            if(!DARSHAN_MOD_FLAG_ISSET(fd->partial_flag, i))
                continue;
        }
        /* skip modules that this version of Darshan can't parse */
        else if(i >= DARSHAN_KNOWN_MODULE_COUNT)
        {
            fprintf(stderr, "# Warning: module id %d is unknown. You may need "
                            "a newer version of the Darshan utilities to parse it.\n", i);
            continue;
        }
        /* skip modules with no logutil definitions */
        else if(!mod_logutils[i])
        {
            fprintf(stderr, "# Warning: no log utility handlers defined "
                "for module %s, SKIPPING.\n", darshan_module_names[i]);
            continue;
        }
        /* always ignore DXT modules -- those have a standalone parsing utility */
        else if (i == DXT_POSIX_MOD || i == DXT_MPIIO_MOD)
            continue;
        /* currently only POSIX, MPIIO, STDIO, and DAOS modules support non-base
         * parsing
         */
        else if((i != DARSHAN_POSIX_MOD) && (i != DARSHAN_MPIIO_MOD) &&
                (i != DARSHAN_STDIO_MOD) && (i != DARSHAN_DFS_MOD) &&
                (i != DARSHAN_DAOS_MOD) && !(mask & OPTION_BASE))
            continue;

        /* this module has data to be parsed and printed */
        memset(mod_buf, 0, DEF_MOD_BUF_SIZE);

        printf("\n# *******************************************************\n");
        printf("# %s module data\n", darshan_module_names[i]);
        printf("# *******************************************************\n");

        /* print warning if this module only stored partial data */
        if(DARSHAN_MOD_FLAG_ISSET(fd->partial_flag, i)) {
            if(mask & OPTION_SHOW_INCOMPLETE)
            {
                /* user requested that we show the data we have anyway */
                printf("\n# *WARNING*: "
                       "The %s module contains incomplete data!\n"
                       "#            This happens when a module runs out of\n"
                       "#            memory to store new record data.\n",
                       darshan_module_names[i]);
                printf(
                       "\n# To avoid this error, consult the darshan-runtime\n"
                       "# documentation and consider setting the\n"
                       "# DARSHAN_EXCLUDE_DIRS environment variable to prevent\n"
                       "# Darshan from instrumenting unecessary files.\n");
                if(fd->mod_map[i].len == 0)
                    continue; // no data to parse
            }
            else
            {
                /* hard error */
                fprintf(stderr, "\n# *ERROR*: "
                       "The %s module contains incomplete data!\n"
                       "#            This happens when a module runs out of\n"
                       "#            memory to store new record data.\n",
                       darshan_module_names[i]);
                fprintf(stderr,
                       "\n# To avoid this error, consult the darshan-runtime\n"
                       "# documentation and consider setting the\n"
                       "# DARSHAN_EXCLUDE_DIRS environment variable to prevent\n"
                       "# Darshan from instrumenting unecessary files.\n");
                fprintf(stderr,
                        "\n# You can display the (incomplete) data that is\n"
                        "# present in this log using the --show-incomplete\n"
                        "# option to darshan-parser.\n");
                return(-1);
            }
        }

        if(mask & OPTION_BASE)
        {
            /* print a header describing the module's I/O characterization data */
            if(mod_logutils[i]->log_print_description)
            {
                mod_logutils[i]->log_print_description(fd->mod_ver[i]);
                DARSHAN_PRINT_HEADER();
            }
        }

        /* create an accumulator, if supported */
        /* no explicit error checking; we will just skip injecting if null */
        darshan_accumulator_create(i, job.nprocs, &acc);

        /* loop over each of this module's records and print them */
        while(1)
        {
            char *mnt_pt = NULL;
            char *fs_type = NULL;
            char *rec_name = NULL;

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

            /* accumulated and derived metrics, if supported */
            if(acc)
                darshan_accumulator_inject(acc, mod_buf, 1);
        }
        if(ret == -1)
            continue; /* move on to the next module if there was an error with this one */

        /* calculate derived metrics from accumulator */
        if(acc)
            darshan_accumulator_emit(acc, &metrics, mod_buf);

        /* we calculate more detailed stats for POSIX, MPI-IO, STDIO, and DAOS modules,
         * if the parser is executed with more than the base option
         */
        if(i != DARSHAN_POSIX_MOD && i != DARSHAN_MPIIO_MOD && i != DARSHAN_STDIO_MOD &&
           i != DARSHAN_DFS_MOD && i != DARSHAN_DAOS_MOD)
            continue;

        /* Total Calc */
        if(mask & OPTION_TOTAL)
        {
            if(i == DARSHAN_POSIX_MOD)
            {
                posix_print_total_file((struct darshan_posix_file*)mod_buf, fd->mod_ver[i]);
            }
            else if(i == DARSHAN_MPIIO_MOD)
            {
                mpiio_print_total_file((struct darshan_mpiio_file*)mod_buf, fd->mod_ver[i]);
            }
            else if(i == DARSHAN_STDIO_MOD)
            {
                stdio_print_total_file((struct darshan_stdio_file*)mod_buf, fd->mod_ver[i]);
            }
            else if(i == DARSHAN_DFS_MOD)
            {
                dfs_print_total_file((struct darshan_dfs_file*)mod_buf, fd->mod_ver[i]);
            }
            else if(i == DARSHAN_DAOS_MOD)
            {
                daos_print_total_file((struct darshan_daos_object*)mod_buf, fd->mod_ver[i]);
            }
        }

        /* File Calc */
        if(mask & OPTION_FILE)
        {
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
                   metrics.category_counters[DARSHAN_ALL_FILES].count,
                   metrics.category_counters[DARSHAN_ALL_FILES].total_read_volume_bytes +
                    metrics.category_counters[DARSHAN_ALL_FILES].total_write_volume_bytes,
                   metrics.category_counters[DARSHAN_ALL_FILES].max_offset_bytes);
            printf("# read_only: %" PRId64 " %" PRId64 " %" PRId64 "\n",
                   metrics.category_counters[DARSHAN_RO_FILES].count,
                   metrics.category_counters[DARSHAN_RO_FILES].total_read_volume_bytes +
                    metrics.category_counters[DARSHAN_RO_FILES].total_write_volume_bytes,
                   metrics.category_counters[DARSHAN_RO_FILES].max_offset_bytes);
            printf("# write_only: %" PRId64 " %" PRId64 " %" PRId64 "\n",
                   metrics.category_counters[DARSHAN_WO_FILES].count,
                   metrics.category_counters[DARSHAN_WO_FILES].total_read_volume_bytes +
                    metrics.category_counters[DARSHAN_WO_FILES].total_write_volume_bytes,
                   metrics.category_counters[DARSHAN_WO_FILES].max_offset_bytes);
            printf("# read_write: %" PRId64 " %" PRId64 " %" PRId64 "\n",
                   metrics.category_counters[DARSHAN_RW_FILES].count,
                   metrics.category_counters[DARSHAN_RW_FILES].total_read_volume_bytes +
                    metrics.category_counters[DARSHAN_RW_FILES].total_write_volume_bytes,
                   metrics.category_counters[DARSHAN_RW_FILES].max_offset_bytes);
            printf("# unique: %" PRId64 " %" PRId64 " %" PRId64 "\n",
                   metrics.category_counters[DARSHAN_UNIQ_FILES].count,
                   metrics.category_counters[DARSHAN_UNIQ_FILES].total_read_volume_bytes +
                    metrics.category_counters[DARSHAN_UNIQ_FILES].total_write_volume_bytes,
                   metrics.category_counters[DARSHAN_UNIQ_FILES].max_offset_bytes);
            printf("# shared: %" PRId64 " %" PRId64 " %" PRId64 "\n",
                   metrics.category_counters[DARSHAN_SHARED_FILES].count +
                    metrics.category_counters[DARSHAN_PART_SHARED_FILES].count,
                   metrics.category_counters[DARSHAN_SHARED_FILES].total_read_volume_bytes +
                    metrics.category_counters[DARSHAN_SHARED_FILES].total_write_volume_bytes +
                    metrics.category_counters[DARSHAN_PART_SHARED_FILES].total_read_volume_bytes +
                    metrics.category_counters[DARSHAN_PART_SHARED_FILES].total_write_volume_bytes,
                   metrics.category_counters[DARSHAN_SHARED_FILES].max_offset_bytes +
                    metrics.category_counters[DARSHAN_PART_SHARED_FILES].max_offset_bytes);
        }

        /* Perf Calc */
        if(mask & OPTION_PERF)
        {
            printf("\n# performance\n");
            printf("# -----------\n");
            printf("# total_bytes: %" PRId64 "\n", metrics.total_bytes);
            printf("#\n");
            printf("# I/O timing for unique files (seconds):\n");
            printf("# ...........................\n");
            printf("# unique files: slowest_rank_io_time: %lf\n", metrics.unique_io_total_time_by_slowest);
            printf("# unique files: slowest_rank_meta_only_time: %lf\n", metrics.unique_md_only_time_by_slowest);
            printf("# unique files: slowest_rank_rw_only_time: %lf\n", metrics.unique_rw_only_time_by_slowest);
            printf("# unique files: slowest_rank: %d\n", metrics.unique_io_slowest_rank);
            printf("#\n");
            printf("# I/O timing for shared files (seconds):\n");
            printf("# ...........................\n");
            printf("# shared files: time_by_slowest: %lf\n", metrics.shared_io_total_time_by_slowest);
            printf("#\n");
            printf("# Aggregate performance, including both shared and unique files:\n");
            printf("# ...........................\n");
            printf("# agg_time_by_slowest: %lf # seconds\n", metrics.agg_time_by_slowest);
            printf("# agg_perf_by_slowest: %lf # MiB/s\n", metrics.agg_perf_by_slowest);
        }

        if(acc) {
            darshan_accumulator_destroy(acc);
            acc = NULL;
        }
    }

    if(empty_mods == DARSHAN_MAX_MODS)
        printf("\n# no module data available.\n");
    ret = 0;

    darshan_log_close(fd);
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

void stdio_print_total_file(struct darshan_stdio_file *pfile, int stdio_ver)
{
    int i;

    mod_logutils[DARSHAN_STDIO_MOD]->log_print_description(stdio_ver);
    printf("\n");
    for(i = 0; i < STDIO_NUM_INDICES; i++)
    {
        printf("total_%s: %"PRId64"\n",
            stdio_counter_names[i], pfile->counters[i]);
    }
    for(i = 0; i < STDIO_F_NUM_INDICES; i++)
    {
        printf("total_%s: %lf\n",
            stdio_f_counter_names[i], pfile->fcounters[i]);
    }
    return;
}

void posix_print_total_file(struct darshan_posix_file *pfile, int posix_ver)
{
    int i;

    mod_logutils[DARSHAN_POSIX_MOD]->log_print_description(posix_ver);
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

void mpiio_print_total_file(struct darshan_mpiio_file *mfile, int mpiio_ver)
{
    int i;

    mod_logutils[DARSHAN_MPIIO_MOD]->log_print_description(mpiio_ver);
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

void dfs_print_total_file(struct darshan_dfs_file *pfile, int dfs_ver)
{
    int i;

    mod_logutils[DARSHAN_DFS_MOD]->log_print_description(dfs_ver);
    printf("\n");
    for(i = 0; i < DFS_NUM_INDICES; i++)
    {
        printf("total_%s: %"PRId64"\n",
            dfs_counter_names[i], pfile->counters[i]);
    }
    for(i = 0; i < DFS_F_NUM_INDICES; i++)
    {
        printf("total_%s: %lf\n",
            dfs_f_counter_names[i], pfile->fcounters[i]);
    }
    return;
}

void daos_print_total_file(struct darshan_daos_object *pfile, int daos_ver)
{
    int i;

    mod_logutils[DARSHAN_DAOS_MOD]->log_print_description(daos_ver);
    printf("\n");
    for(i = 0; i < DAOS_NUM_INDICES; i++)
    {
        printf("total_%s: %"PRId64"\n",
            daos_counter_names[i], pfile->counters[i]);
    }
    for(i = 0; i < DAOS_F_NUM_INDICES; i++)
    {
        printf("total_%s: %lf\n",
            daos_f_counter_names[i], pfile->fcounters[i]);
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
