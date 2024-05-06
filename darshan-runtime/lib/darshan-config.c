/*
 * Copyright (C) 2022 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

#ifdef HAVE_CONFIG_H
# include <darshan-runtime-config.h>
#endif

#include <ctype.h>
#include <assert.h>
#include <stdlib.h>

#include "utlist.h"
#include "darshan.h"
#include "darshan-config.h"

/* paths prefixed with the following directories are not tracked by darshan */
char* darshan_path_exclusions[] = {
    "/etc/",
    "/dev/",
    "/usr/",
    "/bin/",
    "/boot/",
    "/lib/",
    "/opt/",
    "/sbin/",
    "/sys/",
    "/proc/",
    "/var/",
    NULL
};
/* paths prefixed with the following directories are tracked by darshan even if
 * they share a root with a path listed in darshan_path_exclusions
 */
char* darshan_path_inclusions[] = {
    "/var/opt/cray/dws/mounts/",
    NULL
};

/* allow users to override the path exclusions */
char** user_darshan_path_exclusions = NULL;

/* helper to convert csv of module names to a module id bit field */
static uint64_t darshan_module_csv_to_flags(char *mod_csv)
{
    char *tok;
    int i;
    int max = sizeof(darshan_module_names) / sizeof(*darshan_module_names);
    int found;
    uint64_t mod_flags = 0;

    tok = strtok(mod_csv, ",");
    if(tok == NULL)
        darshan_core_fprintf(stderr, "darshan library warning: "\
            "unable to parse Darshan config module csv \"%s\"\n", mod_csv);
    else if(strcmp(tok, "*") == 0)
        return ((1 << max) - 1); // set all modules if given wildcard '*'

    while(tok != NULL)
    {
        found = 0;
        for(i = 0; i < max; i++)
        {
            if(strcmp(tok, darshan_module_names[i]) == 0)
            {
                DARSHAN_MOD_FLAG_SET(mod_flags, i);
                found = 1;
            }
        }
        if(!found)
            darshan_core_fprintf(stderr, "darshan library warning: "\
                "unknown module \"%s\" in Darshan config module csv\n", tok);

        tok = strtok(NULL, ",");
    }

    return(mod_flags);
}

void darshan_init_config(struct darshan_config *cfg)
{
    cfg->mod_mem = DARSHAN_MOD_MEM_MAX;
    cfg->name_mem = DARSHAN_NAME_MEM_MAX;
    cfg->mem_alignment = __DARSHAN_MEM_ALIGNMENT;
    cfg->jobid_env = strdup(__DARSHAN_JOBID);
    cfg->log_hints = strdup(__DARSHAN_LOG_HINTS);
#ifdef __DARSHAN_LOG_PATH
    cfg->log_path = strdup(__DARSHAN_LOG_PATH);
#endif
#ifdef __DARSHAN_ENABLE_MMAP_LOGS
    cfg->mmap_log_path = strdup(DARSHAN_DEF_MMAP_LOG_PATH);
#endif
    /* enable all modules except DXT by default */
    DARSHAN_MOD_FLAG_SET(cfg->mod_disabled, DXT_POSIX_MOD);
    DARSHAN_MOD_FLAG_SET(cfg->mod_disabled, DXT_MPIIO_MOD);
#ifndef DARSHAN_BGQ
    DARSHAN_MOD_FLAG_SET(cfg->mod_disabled, DARSHAN_BGQ_MOD);
#endif
#ifndef DARSHAN_MDHIM
    DARSHAN_MOD_FLAG_SET(cfg->mod_disabled, DARSHAN_MDHIM_MOD);
#endif
#ifndef DARSHAN_LUSTRE
    DARSHAN_MOD_FLAG_SET(cfg->mod_disabled, DARSHAN_LUSTRE_MOD);
#endif
#ifndef DARSHAN_HDF5
    DARSHAN_MOD_FLAG_SET(cfg->mod_disabled, DARSHAN_H5F_MOD);
    DARSHAN_MOD_FLAG_SET(cfg->mod_disabled, DARSHAN_H5D_MOD);
#endif
#ifndef DARSHAN_USE_APXC
    DARSHAN_MOD_FLAG_SET(cfg->mod_disabled, DARSHAN_APXC_MOD);
#endif
#ifndef DARSHAN_USE_APMPI
    DARSHAN_MOD_FLAG_SET(cfg->mod_disabled, DARSHAN_APMPI_MOD);
#endif
    cfg->exclude_dirs = darshan_path_exclusions;
    cfg->include_dirs = darshan_path_inclusions;

    return;
}

void darshan_parse_config_env(struct darshan_config *cfg)
{
    char *envstr;
    char* string;
    char* token;
    int i;
    int ret;
    struct darshan_core_regex *regex, *tmp_regex;
    uint64_t tmp_mod_flags;
    int success;

    /* allow override of memory quota for darshan modules' records */
    envstr = getenv(DARSHAN_MOD_MEM_OVERRIDE);
    if(envstr)
    {
        DARSHAN_PARSE_NUMBER_FROM_STR(envstr, size_t, cfg->mod_mem, success);
        if(success)
            cfg->mod_mem *= (1024 * 1024); /* convert from MiB */
    }
    /* allow override of memory quota for darshan name records */
    envstr = getenv(DARSHAN_NAME_MEM_OVERRIDE);
    if(envstr)
    {
        DARSHAN_PARSE_NUMBER_FROM_STR(envstr, size_t, cfg->name_mem, success);
        if(success)
            cfg->name_mem *= (1024 * 1024); /* convert from MiB */
    }
    /* allow override of darshan memory alignment value */
    #if (__DARSHAN_MEM_ALIGNMENT < 1)
        #error Darshan must be configured with a positive value for --with-mem-align
    #endif
    envstr = getenv(DARSHAN_MEM_ALIGNMENT_OVERRIDE);
    if(envstr)
    {
        DARSHAN_PARSE_NUMBER_FROM_STR(envstr, int, cfg->mem_alignment, success);
        /* avoid floating point errors on faulty input */
        if(cfg->mem_alignment < 1)
        {
            cfg->mem_alignment = 1;
        }
    }
    /* allow override of darshan job ID environment variable */
    envstr = getenv(DARSHAN_JOBID_OVERRIDE);
    if(envstr)
    {
        /* free current job ID environment string and allocate a new one */
        free(cfg->jobid_env);
        cfg->jobid_env = strdup(envstr);
    }
    /* allow override of MPI-IO hints for darshan log file */
    envstr = getenv(DARSHAN_LOG_HINTS_OVERRIDE);
    if(envstr)
    {
        /* free current log hints and allocate a new one */
        free(cfg->log_hints);
        cfg->log_hints = strdup(envstr);
    }
    /* allow override of darshan log file directory */
    envstr = getenv(DARSHAN_LOG_PATH_OVERRIDE);
    if(envstr)
    {
        if(cfg->log_path) free(cfg->log_path);
        cfg->log_path = strdup(envstr);
    }
#ifdef __DARSHAN_LOG_ENV
    /* allow override of Darhan's date-formatted log file directories with
     * a simple flat directory using environment variables specified by
     * the --with-logpath-by-env configure argument
     */
    char env_check[256];
    char* env_tok;
    /* just silently skip if the environment variable list is too big */
    if(strlen(__DARSHAN_LOG_ENV) < 256)
    {
        /* copy env variable list to a temporary buffer */
        strcpy(env_check, __DARSHAN_LOG_ENV);
        /* tokenize the comma-separated list */
        env_tok = strtok(env_check, ",");
        if(env_tok)
        {
            do
            {
                /* check each env variable in order */
                envstr = getenv(env_tok);
                if(envstr)
                {
                    /* stop as soon as we find a match */
                    cfg->log_path_byenv = strdup(envstr);
                    break;
                }
            }while((env_tok = strtok(NULL, ",")));
        }
    }
#endif
#ifdef __DARSHAN_ENABLE_MMAP_LOGS
    /* allow override of Darshan's mmap log file directory, if enabled */
    envstr = getenv(DARSHAN_MMAP_LOG_PATH_OVERRIDE);
    if(envstr)
    {
        free(cfg->mmap_log_path);
        cfg->mmap_log_path = strdup(envstr);
    }
#endif
    /* allow override of Darshan's default directory exclusions */
    envstr = getenv("DARSHAN_EXCLUDE_DIRS");
    if(envstr)
    {
        /* if DARSHAN_EXCLUDE_DIRS=none, do not exclude any dir */
        if(strncmp(envstr,"none",strlen(envstr))>=0)
        {
            cfg->exclude_dirs[0]=NULL;
        }
        else
        {
            string = strdup(envstr);
            i = 0;
            /* get the comma separated number of directories */
            token = strtok(string, ",");
            while (token != NULL)
            {
                i++;
                token = strtok(NULL, ",");
            }
            cfg->user_exclude_dirs=(char **)malloc((i+1)*sizeof(char *));
            assert(cfg->user_exclude_dirs);

            i = 0;
            strcpy(string, envstr);
            token = strtok(string, ",");
            while (token != NULL)
            {
                cfg->user_exclude_dirs[i]=(char *)malloc(strlen(token)+1);
                assert(cfg->user_exclude_dirs[i]);
                strcpy(cfg->user_exclude_dirs[i],token);
                i++;
                token = strtok(NULL, ",");
            }
            cfg->user_exclude_dirs[i]=NULL;
            free(string);
        }
    }
    char *app_exclude_str = getenv("DARSHAN_APP_EXCLUDE");
    char *app_include_str = getenv("DARSHAN_APP_INCLUDE");
    if(app_exclude_str || app_include_str)
    {
        /* free any configured excludes/includes from Darshan config file */
        LL_FOREACH_SAFE(cfg->app_exclusion_list, regex, tmp_regex)
        {
            LL_DELETE(cfg->app_exclusion_list, regex);
            free(regex->regex_str);
            regfree(&regex->regex);
            free(regex);
        }
        LL_FOREACH_SAFE(cfg->app_inclusion_list, regex, tmp_regex)
        {
            LL_DELETE(cfg->app_inclusion_list, regex);
            free(regex->regex_str);
            regfree(&regex->regex);
            free(regex);
        }
    }
    /* allow exclusion of specific apps from Darshan instrumentation */
    if(app_exclude_str)
    {
        /* parse app exclusion regex list */
        string = strdup(app_exclude_str);
        token = strtok(string, ",");
        while(token != NULL)
        {
            regex = malloc(sizeof(*regex));
            if(regex)
            {
                regex->regex_str = strdup(token);
                if(!regex->regex_str)
                {
                    free(regex);
                    continue;
                }
                ret = regcomp(&regex->regex, token, REG_EXTENDED);
                if(!ret)
                {
                    LL_APPEND(cfg->app_exclusion_list, regex);
                }
                else
                {
                    darshan_core_fprintf(stderr, "darshan library warning: "\
                        "unable to compile Darshan config DARSHAN_APP_EXCLUDE "\
                        "regex %s\n", token);
                    free(regex->regex_str);
                    free(regex);
                }
            }
            token = strtok(NULL, ",");
        }
        free(string);
    }
    /* allow inclusion of specific apps from Darshan instrumentation */
    if(app_include_str)
    {
        /* parse app inclusion regex list */
        string = strdup(app_include_str);
        token = strtok(string, ",");
        while(token != NULL)
        {
            regex = malloc(sizeof(*regex));
            if(regex)
            {
                regex->regex_str = strdup(token);
                if(!regex->regex_str)
                {
                    free(regex);
                    continue;
                }
                ret = regcomp(&regex->regex, token, REG_EXTENDED);
                if(!ret)
                {
                    LL_APPEND(cfg->app_inclusion_list, regex);
                }
                else
                {
                    darshan_core_fprintf(stderr, "darshan library warning: "\
                        "unable to compile Darshan config DARSHAN_APP_INCLUDE "\
                        "regex %s\n", token);
                    free(regex->regex_str);
                    free(regex);
                }
            }
            token = strtok(NULL, ",");
        }
        free(string);
    }
    char *rank_exclude_str = getenv("DARSHAN_RANK_EXCLUDE");
    char *rank_include_str = getenv("DARSHAN_RANK_INCLUDE");
    if(rank_exclude_str || rank_include_str)
    {
        /* override any rank_exclusions from config file */
        if(cfg->rank_exclusions)
        {
            free(cfg->rank_exclusions);
            cfg->rank_exclusions = NULL;
        }
        if(cfg->rank_inclusions)
        {
            free(cfg->rank_inclusions);
            cfg->rank_inclusions = NULL;
        }
    }
    /* allow exclusion of specific ranks from Darshan instrumentation */
    if(rank_exclude_str)
    {
        cfg->rank_exclusions = strdup(rank_exclude_str);
    }
    /* allow inclusion of specific ranks from Darshan instrumentation */
    if(rank_include_str)
    {
        cfg->rank_inclusions = strdup(rank_include_str);
    }
    char *mod_disable_str = getenv("DARSHAN_MOD_DISABLE");
    char *mod_enable_str = getenv("DARSHAN_MOD_ENABLE");
    if(mod_disable_str || mod_enable_str)
    {
        /* override any mod disable/enable settings from darshan config file */
        cfg->mod_disabled_flags = cfg->mod_enabled_flags = 0;
    }
    /* allow disabling of specific Darshan instrumentation modules */
    if(mod_disable_str)
    {
        string = strdup(mod_disable_str);
        tmp_mod_flags = darshan_module_csv_to_flags(string);
        cfg->mod_disabled_flags |= tmp_mod_flags;
        free(string);
    }
    /* allow enabling of specific Darshan instrumentation modules */
    if(mod_enable_str)
    {
        string = strdup(mod_enable_str);
        tmp_mod_flags = darshan_module_csv_to_flags(string);
        cfg->mod_enabled_flags |= tmp_mod_flags;
        free(string);
    }
    /* backwards compatibility with the old way to enable DXT */
    if(getenv("DXT_ENABLE_IO_TRACE"))
    {
        DARSHAN_MOD_FLAG_SET(cfg->mod_enabled_flags, DXT_POSIX_MOD);
        DARSHAN_MOD_FLAG_SET(cfg->mod_enabled_flags, DXT_MPIIO_MOD);
    }
    envstr = getenv("DARSHAN_DXT_SMALL_IO_TRIGGER");
    if(envstr)
    {
        double thresh_pct;
        DARSHAN_PARSE_NUMBER_FROM_STR(envstr, double, thresh_pct, success);
        if(success)
        {
            struct dxt_trigger *trigger = malloc(sizeof(*trigger));
            if(trigger)
            {
                trigger->type = DXT_SMALL_IO_TRIGGER;
                trigger->u.small_io.thresh_pct = thresh_pct;
                if(cfg->small_io_trigger)
                    free(cfg->small_io_trigger);
                cfg->small_io_trigger = trigger;
            }
        }
    }
    envstr = getenv("DARSHAN_DXT_UNALIGNED_IO_TRIGGER");
    if(envstr)
    {
        double thresh_pct;
        DARSHAN_PARSE_NUMBER_FROM_STR(envstr, double, thresh_pct, success);
        if(success)
        {
            struct dxt_trigger *trigger = malloc(sizeof(*trigger));
            if(trigger)
            {
                trigger->type = DXT_UNALIGNED_IO_TRIGGER;
                trigger->u.unaligned_io.thresh_pct = thresh_pct;
                if(cfg->unaligned_io_trigger)
                    free(cfg->unaligned_io_trigger);
                cfg->unaligned_io_trigger = trigger;
            }
        }
    }
    if(getenv("DARSHAN_DUMP_CONFIG"))
        cfg->dump_config_flag = 1;
    if(getenv("DARSHAN_INTERNAL_TIMING"))
        cfg->internal_timing_flag = 1;
    if(getenv("DARSHAN_DISABLE_SHARED_REDUCTION"))
        cfg->disable_shared_redux_flag = 1;

    /* apply disabled/enabled module flags */
    cfg->mod_disabled |= cfg->mod_disabled_flags;
    cfg->mod_disabled &= ~cfg->mod_enabled_flags;

    return;
}

void darshan_parse_config_file(struct darshan_config *cfg)
{
    char *darshan_conf;
    FILE *fp;
    char *line = NULL;
    size_t len = 0;
    char *key, *val, *mods;
    char *token;
    uint64_t tmp_mod_flags;
    size_t tmpmax;
    struct darshan_core_regex *regex;
    int i;
    int ret;
    int success;

    /* get log filters file */
    darshan_conf = getenv("DARSHAN_CONFIG_PATH");
    if(darshan_conf)
    {
        fp = fopen(darshan_conf, "r");
        if(!fp)
        {
            darshan_core_fprintf(stderr, "darshan library warning: "\
                "unable to open Darshan config at path %s\n", darshan_conf);
            return;
        }

        while(getline(&line, &len, fp) != -1)
        {
            const char *c = line;
            while(isspace((unsigned char)*c))
                c++;
            if(*c == '\0')
                continue; // skip lines with only whitespace
            if(*c == '#')
                continue; // skip comments

            /* remove newline if present */
            if(line[strlen(line) - 1] == '\n')
                line[strlen(line) - 1] = '\0';

            /* extract setting key and parameters */
            key = strtok(line, " \t");
            if(strcmp(key, "MODMEM") == 0)
            {
                val = strtok(NULL, " \t");
                DARSHAN_PARSE_NUMBER_FROM_STR(val, size_t, cfg->mod_mem, success);
                if(success)
                    cfg->mod_mem *= (1024 * 1024); /* convert from MiB */
            }
            else if(strcmp(key, "NAMEMEM") == 0)
            {
                val = strtok(NULL, " \t");
                DARSHAN_PARSE_NUMBER_FROM_STR(val, size_t, cfg->name_mem, success);
                if(success)
                    cfg->name_mem *= (1024 * 1024); /* convert from MiB */
            }
            else if(strcmp(key, "MEM_ALIGNMENT") == 0)
            {
                val = strtok(NULL, " \t");
                DARSHAN_PARSE_NUMBER_FROM_STR(val, int, cfg->mem_alignment, success);
                /* avoid floating point errors on faulty input */
                if(cfg->mem_alignment < 1)
                {
                    cfg->mem_alignment = 1;
                }
            }
            else if(strcmp(key, "JOBID") == 0)
            {
                val = strtok(NULL, " \t");
                if(val)
                {
                    /* free current job ID environment string and allocate a new one */
                    free(cfg->jobid_env);
                    cfg->jobid_env = strdup(val);
                }
            }
            else if(strcmp(key, "LOGHINTS") == 0)
            {
                val = strtok(NULL, " \t");
                /* free current log hints and allocate a new one */
                free(cfg->log_hints);
                /* gracefully handle no value, equivalent to no log hints */
                if(val)
                    cfg->log_hints = strdup(val);
                else
                    cfg->log_hints = strdup("");
            }
            else if(strcmp(key, "LOGPATH") == 0)
            {
                val = strtok(NULL, " \t");
                if(val)
                {
                    /* free current job ID environment string and allocate a new one */
                    if(cfg->log_path) free(cfg->log_path);
                    cfg->log_path = strdup(val);
                }
            }
#ifdef __DARSHAN_ENABLE_MMAP_LOGS
            else if(strcmp(key, "MMAP_LOGPATH") == 0)
            {
                val = strtok(NULL, " \t");
                if(val)
                {
                    free(cfg->mmap_log_path);
                    cfg->mmap_log_path = strdup(val);
                }
            }
#endif
            else if(strcmp(key, "APP_EXCLUDE") == 0)
            {
                val = strtok(NULL, " \t");
                if(val)
                {
                    token = strtok(val, ",");
                    while(token != NULL)
                    {
                        regex = malloc(sizeof(*regex));
                        if(!regex) break;
                        regex->regex_str = strdup(token);
                        if(!regex->regex_str)
                        {
                            free(regex);
                            break;
                        }
                        ret = regcomp(&regex->regex, token, REG_EXTENDED);
                        if(!ret)
                        {
                            LL_APPEND(cfg->app_exclusion_list, regex);
                        }
                        else
                        {
                            darshan_core_fprintf(stderr, "darshan library warning: "\
                                "unable to compile Darshan config %s regex %s\n",
                                key, token);
                            free(regex->regex_str);
                            free(regex);
                        }
                        token = strtok(NULL, ",");
                    }
                }
            }
            else if(strcmp(key, "APP_INCLUDE") == 0)
            {
                val = strtok(NULL, " \t");
                if(val)
                {
                    token = strtok(val, ",");
                    while(token != NULL)
                    {
                        regex = malloc(sizeof(*regex));
                        if(!regex) break;
                        regex->regex_str = strdup(token);
                        if(!regex->regex_str)
                        {
                            free(regex);
                            break;
                        }
                        ret = regcomp(&regex->regex, token, REG_EXTENDED);
                        if(!ret)
                        {
                            LL_APPEND(cfg->app_inclusion_list, regex);
                        }
                        else
                        {
                            darshan_core_fprintf(stderr, "darshan library warning: "\
                                "unable to compile Darshan config %s regex %s\n",
                                key, token);
                            free(regex->regex_str);
                            free(regex);
                        }
                        token = strtok(NULL, ",");
                    }
                }
            }
            else if (strcmp(key, "RANK_EXCLUDE") == 0)
            {
                val = strtok(NULL, " \t");
                if(val)
                {
                    if(cfg->rank_exclusions)
                    {
                        size_t tmp_str_sz = strlen(cfg->rank_exclusions) +
                                            strlen(val) + 2; // 2 for NULL + ','
                        char *tmp_str = malloc(tmp_str_sz);
                        if(tmp_str)
                        {
                            sprintf(tmp_str, "%s,%s",
                                cfg->rank_exclusions, val);
                            cfg->rank_exclusions = tmp_str;
                        }
                    }
                    else
                        cfg->rank_exclusions = strdup(val);
                }
            }
            else if (strcmp(key, "RANK_INCLUDE") == 0)
            {
                val = strtok(NULL, " \t");
                if(val)
                {
                    if(cfg->rank_inclusions)
                    {
                        size_t tmp_str_sz = strlen(cfg->rank_inclusions) +
                                            strlen(val) + 2; // 2 for NULL + ','
                        char *tmp_str = malloc(tmp_str_sz);
                        if(tmp_str)
                        {
                            sprintf(tmp_str, "%s,%s",
                                cfg->rank_inclusions, val);
                            cfg->rank_inclusions = tmp_str;
                        }
                    }
                    else
                        cfg->rank_inclusions = strdup(val);
                }
            }
            else if(strcmp(key, "MOD_DISABLE") == 0)
            {
                mods = strtok(NULL, " \t");
                if(mods)
                {
                    tmp_mod_flags = darshan_module_csv_to_flags(mods);
                    cfg->mod_disabled_flags |= tmp_mod_flags;
                }
            }
            else if(strcmp(key, "MOD_ENABLE") == 0)
            {
                mods = strtok(NULL, " \t");
                if(mods)
                {
                    tmp_mod_flags = darshan_module_csv_to_flags(mods);
                    cfg->mod_enabled_flags |= tmp_mod_flags;
                }
            }
            else if(strcmp(key, "MAX_RECORDS") == 0)
            {
                val = strtok(NULL, " \t");
                DARSHAN_PARSE_NUMBER_FROM_STR(val, size_t, tmpmax, success);
                if(success)
                {
                    mods = strtok(NULL, " \t");
                    if(mods)
                    {
                        tmp_mod_flags = darshan_module_csv_to_flags(mods);
                        for(i = 0; i < DARSHAN_KNOWN_MODULE_COUNT; i++)
                        {
                            if(DARSHAN_MOD_FLAG_ISSET(tmp_mod_flags, i))
                                cfg->mod_max_records_override[i] = tmpmax;
                        }
                    }
                }
            }
            else if(strcmp(key, "NAME_EXCLUDE") == 0)
            {
                val = strtok(NULL, " \t");
                if(val)
                {
                    mods = strtok(NULL, " \t");
                    if(mods)
                    {
                        tmp_mod_flags = darshan_module_csv_to_flags(mods);
                        token = strtok(val, ",");
                        while(token != NULL)
                        {
                            regex = malloc(sizeof(*regex));
                            if(!regex) break;
                            regex->regex_str = strdup(token);
                            if(!regex->regex_str)
                            {
                                free(regex);
                                break;
                            }
                            regex->mod_flags = tmp_mod_flags;
                            ret = regcomp(&regex->regex, token, REG_EXTENDED);
                            if(!ret)
                            {
                                LL_APPEND(cfg->rec_exclusion_list, regex);
                            }
                            else
                            {
                                darshan_core_fprintf(stderr, "darshan library warning: "\
                                    "unable to compile Darshan config %s regex %s\n",
                                    key, token);
                                free(regex->regex_str);
                                free(regex);
                            }
                            token = strtok(NULL, ",");
                        }
                    }
                }
            }
            else if(strcmp(key, "NAME_INCLUDE") == 0)
            {
                val = strtok(NULL, " \t");
                if(val)
                {
                    mods = strtok(NULL, " \t");
                    if(mods)
                    {
                        tmp_mod_flags = darshan_module_csv_to_flags(mods);
                        token = strtok(val, ",");
                        while(token != NULL)
                        {
                            regex = malloc(sizeof(*regex));
                            if(!regex) break;
                            regex->regex_str = strdup(token);
                            if(!regex->regex_str)
                            {
                                free(regex);
                                break;
                            }
                            regex->mod_flags = tmp_mod_flags;
                            ret = regcomp(&regex->regex, token, REG_EXTENDED);
                            if(!ret)
                            {
                                LL_APPEND(cfg->rec_inclusion_list, regex);
                            }
                            else
                            {
                                darshan_core_fprintf(stderr, "darshan library warning: "\
                                    "unable to compile Darshan config %s regex %s\n",
                                    key, token);
                                free(regex->regex_str);
                                free(regex);
                            }
                            token = strtok(NULL, ",");
                        }
                    }
                }
            }
            else if(strcmp(key, "DXT_SMALL_IO_TRIGGER") == 0)
            {
                double thresh_pct;
                val = strtok(NULL, " \t");
                DARSHAN_PARSE_NUMBER_FROM_STR(val, double, thresh_pct, success);
                if(success)
                {
                    struct dxt_trigger *trigger = malloc(sizeof(*trigger));
                    if(trigger)
                    {
                        trigger->type = DXT_SMALL_IO_TRIGGER;
                        trigger->u.small_io.thresh_pct = thresh_pct;
                        if(cfg->small_io_trigger)
                            free(cfg->small_io_trigger);
                        cfg->small_io_trigger = trigger;
                    }
                }
            }
            else if(strcmp(key, "DXT_UNALIGNED_IO_TRIGGER") == 0)
            {
                double thresh_pct;
                val = strtok(NULL, " \t");
                DARSHAN_PARSE_NUMBER_FROM_STR(val, double, thresh_pct, success);
                if(success)
                {
                    struct dxt_trigger *trigger = malloc(sizeof(*trigger));
                    if(trigger)
                    {
                        trigger->type = DXT_UNALIGNED_IO_TRIGGER;
                        trigger->u.unaligned_io.thresh_pct = thresh_pct;
                        if(cfg->unaligned_io_trigger)
                            free(cfg->unaligned_io_trigger);
                        cfg->unaligned_io_trigger = trigger;
                    }
                }
            }
            else if(strcmp(key, "DUMP_CONFIG") == 0)
                cfg->dump_config_flag = 1;
            else if(strcmp(key, "INTERNAL_TIMING") == 0)
                cfg->internal_timing_flag = 1;
            else if(strcmp(key, "DISABLE_SHARED_REDUCTION") == 0)
                cfg->disable_shared_redux_flag = 1;
            else
            {
                darshan_core_fprintf(stderr, "darshan library warning: "\
                    "invalid Darshan config setting %s\n", key);
                continue;
            }
        }

        fclose(fp);
        free(line);
    }

    return;
}

void darshan_dump_config(struct darshan_config *cfg)
{
    char **path_exclusions;
    char *path_exclusion;
    int tmp_index = 0;
    int first;
    struct darshan_core_regex *regex;
    int i;

    fprintf(stderr, "##########################\n");
    fprintf(stderr, "##### DARSHAN CONFIG #####\n");
    fprintf(stderr, "##########################\n");
    fprintf(stderr, "# MODMEM = %ld MiB\n", cfg->mod_mem / 1024 / 1024);
    fprintf(stderr, "# NAMEMEM = %ld KiB\n", cfg->name_mem / 1024);
    fprintf(stderr, "# MEM_ALIGNMENT = %d bytes\n", cfg->mem_alignment);
    fprintf(stderr, "# JOBID = %s\n", cfg->jobid_env);
    fprintf(stderr, "# LOGHINTS = %s\n", (strlen(cfg->log_hints) > 0) ?
        cfg->log_hints : "NONE");
    fprintf(stderr, "# LOGPATH = %s\n", cfg->log_path ? cfg->log_path : "NONE");
    fprintf(stderr, "# LOGPATH_BYENV = %s\n", cfg->log_path_byenv ?
        cfg->log_path_byenv : "NONE");
#ifdef __DARSHAN_ENABLE_MMAP_LOGS
    fprintf(stderr, "# MMAP_LOGPATH = %s\n", cfg->mmap_log_path);
#endif
    fprintf(stderr, "# EXCLUDE_DIRS = ");
    if(!cfg->user_exclude_dirs)
        path_exclusions = cfg->exclude_dirs;
    else
        path_exclusions = cfg->user_exclude_dirs;
    first = 1;
    while((path_exclusion = path_exclusions[tmp_index++])) {
        if(!first)
            fprintf(stderr, ",");
        fprintf(stderr, "%s", path_exclusion);
        first = 0;
    }
    fprintf(stderr, "\n");
    fprintf(stderr, "# APP_EXCLUDE = ");
    if(cfg->app_exclusion_list)
    {
        first = 1;
        LL_FOREACH(cfg->app_exclusion_list, regex)
        {
            if(!first)
                fprintf(stderr, ",");
            fprintf(stderr, "%s", regex->regex_str);
            first = 0;
        }
        fprintf(stderr, "\n");
    }
    else
        fprintf(stderr, "NONE\n");
    fprintf(stderr, "# APP_INCLUDE = ");
    if(cfg->app_inclusion_list)
    {
        first = 1;
        LL_FOREACH(cfg->app_inclusion_list, regex)
        {
            if(!first)
                fprintf(stderr, ",");
            fprintf(stderr, "%s", regex->regex_str);
            first = 0;
        }
        fprintf(stderr, "\n");
    }
    else
        fprintf(stderr, "NONE\n");
    fprintf(stderr, "# RANK_EXCLUDE = %s\n", (cfg->rank_exclusions) ?
        cfg->rank_exclusions : "NONE");
    fprintf(stderr, "# RANK_INCLUDE = %s\n", (cfg->rank_inclusions) ?
        cfg->rank_inclusions : "NONE");
    if(cfg->small_io_trigger)
    {
        fprintf(stderr, "# DXT_SMALL_IO_TRIGGER = %.2lf\n",
            cfg->small_io_trigger->u.small_io.thresh_pct);
    }
    if(cfg->unaligned_io_trigger)
    {
        fprintf(stderr, "# DXT_UNALIGNED_IO_TRIGGER = %.2lf\n",
            cfg->unaligned_io_trigger->u.unaligned_io.thresh_pct);
    }
    for(i = 1; i < DARSHAN_KNOWN_MODULE_COUNT; i++)
    {
        fprintf(stderr, "# %s MODULE CONFIG:\n", darshan_module_names[i]);
        if(DARSHAN_MOD_FLAG_ISSET(cfg->mod_disabled, i))
        {
            fprintf(stderr, "#      - DISABLED\n");
            continue;
        }
        fprintf(stderr, "#      - ENABLED\n");
        if(cfg->mod_max_records_override[i] > 0)
            fprintf(stderr, "#      - MAX_RECORDS = %lu\n",
                cfg->mod_max_records_override[i]);
        if(cfg->rec_exclusion_list)
        {
            first = 1;
            LL_FOREACH(cfg->rec_exclusion_list, regex)
            {
                if(!DARSHAN_MOD_FLAG_ISSET(regex->mod_flags, i))
                    continue;
                if(first)
                    fprintf(stderr, "#      - NAME_EXCLUDE = ");
                else
                    fprintf(stderr, ",");
                fprintf(stderr, "%s", regex->regex_str);
                first = 0;
            }
            if(!first)
                fprintf(stderr, "\n");
        }
        if(cfg->rec_inclusion_list)
        {
            first = 1;
            LL_FOREACH(cfg->rec_inclusion_list, regex)
            {
                if(!DARSHAN_MOD_FLAG_ISSET(regex->mod_flags, i))
                    continue;
                if(first)
                    fprintf(stderr, "#      - NAME_INCLUDE = ");
                else
                    fprintf(stderr, ",");
                fprintf(stderr, "%s", regex->regex_str);
                first = 0;
            }
            if(!first)
                fprintf(stderr, "\n");
        }
    }
    fprintf(stderr, "##########################\n");
    fprintf(stderr, "### END DARSHAN CONFIG ###\n");
    fprintf(stderr, "##########################\n");
}

void darshan_free_config(
    struct darshan_config *cfg)
{
    char *path;
    int tmp_index = 0;
    struct darshan_core_regex *regex, *tmp_regex;

    free(cfg->jobid_env);
    free(cfg->log_hints);
    if(cfg->log_path) free(cfg->log_path);
    if(cfg->log_path_byenv) free(cfg->log_path_byenv);
#ifdef __DARSHAN_ENABLE_MMAP_LOGS
    free(cfg->mmap_log_path);
#endif
    if(cfg->user_exclude_dirs)
    {   while((path = cfg->user_exclude_dirs[tmp_index++]))
            free(path);
        free(cfg->user_exclude_dirs);
    }
    LL_FOREACH_SAFE(cfg->app_exclusion_list, regex, tmp_regex)
    {
        LL_DELETE(cfg->app_exclusion_list, regex);
        free(regex->regex_str);
        regfree(&regex->regex);
        free(regex);
    }
    LL_FOREACH_SAFE(cfg->app_inclusion_list, regex, tmp_regex)
    {
        LL_DELETE(cfg->app_inclusion_list, regex);
        free(regex->regex_str);
        regfree(&regex->regex);
        free(regex);
    }
    LL_FOREACH_SAFE(cfg->rec_exclusion_list, regex, tmp_regex)
    {
        LL_DELETE(cfg->rec_exclusion_list, regex);
        free(regex->regex_str);
        regfree(&regex->regex);
        free(regex);
    }
    LL_FOREACH_SAFE(cfg->rec_inclusion_list, regex, tmp_regex)
    {
        LL_DELETE(cfg->rec_inclusion_list, regex);
        free(regex->regex_str);
        regfree(&regex->regex);
        free(regex);
    }
    if(cfg->rank_exclusions) free(cfg->rank_exclusions);
    if(cfg->rank_inclusions) free(cfg->rank_inclusions);
    if(cfg->small_io_trigger) free(cfg->small_io_trigger);
    if(cfg->unaligned_io_trigger) free(cfg->unaligned_io_trigger);

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
