/*
 * Copyright (C) 2022 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

#ifndef __DARSHAN_CONFIG_H
#define __DARSHAN_CONFIG_H

#include "darshan.h"

/* configuration parameters for Darshan runtime */
struct darshan_config
{
    size_t mod_mem;
    size_t name_mem;
    int mem_alignment;
    char *jobid_env;
    char *log_hints;
    char *log_path;
    char *log_path_byenv;
#ifdef __DARSHAN_ENABLE_MMAP_LOGS
    char *mmap_log_path;
#endif
    uint64_t mod_disabled_flags;
    uint64_t mod_enabled_flags;
    uint64_t mod_disabled;
    size_t mod_max_records_override[DARSHAN_KNOWN_MODULE_COUNT];
    char **exclude_dirs;
    char **user_exclude_dirs;
    char **include_dirs;
    struct darshan_core_regex *rec_exclusion_list;
    struct darshan_core_regex *rec_inclusion_list;
    struct darshan_core_regex *app_exclusion_list;
    struct darshan_core_regex *app_inclusion_list;
    char *rank_exclusions;
    char *rank_inclusions;
    struct dxt_trigger *small_io_trigger;
    struct dxt_trigger *unaligned_io_trigger;
    int internal_timing_flag;
    int disable_shared_redux_flag;
    int dump_config_flag;
};

/* initialize a default Darshan configuration */
void darshan_init_config(
    struct darshan_config *cfg);
/* parse Darshan configuraiton from a file */
void darshan_parse_config_file(
    struct darshan_config *cfg);
/* parse Darshan configuraiton from user environment */
void darshan_parse_config_env(
    struct darshan_config *cfg);
/* print final Darshan configuration to stderr */
void darshan_dump_config(
    struct darshan_config *cfg);
/* free any allocated memory for darshan config */
void darshan_free_config(
    struct darshan_config *cfg);

#endif /* __DARSHAN_CONFIG_H */
