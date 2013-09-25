#include <stdlib.h>
#include <getopt.h>
#include <assert.h>
#include <fcntl.h>

#include "darshan-logutils.h"

#define DARSHAN_MAX_STORED_EVENTS 500000

/* macro for adding darshan trace events to an event structure */
#define DARSHAN_TRACE_STORE_EVENT(__new_event, __type, __file, __time) \
do {                                                                   \
    (__new_event)->event_type = __type;                                \
    (__new_event)->rank = (__file)->rank;                              \
    (__new_event)->file_hash = (__file)->hash;                         \
    (__new_event)->time_stamp = __time;                                \
} while (0)

typedef enum
{
    POSIX_OPEN = 0,
    POSIX_CLOSE,
} darshan_event_type;

static const char *darshan_event_names[] =
{
    "POSIX_OPEN",
    "POSIX_CLOSE",
};

struct darshan_event
{
    darshan_event_type event_type;
    int64_t rank;
    uint64_t file_hash;
    double time_stamp;
};

int generate_file_events(struct darshan_job *job,
                         struct darshan_file *file,
                         struct darshan_event *event_list,
                         int event_list_ndx);

void sort_stored_events(struct darshan_event *event_list,
                        int event_list_cnt,
                        int (*compar)(const void*, const void*));

int event_compare(const void *p1, const void *p2);


int usage(char *exename)
{
    fprintf(stderr, "Usage: %s --log <log_filename> --trace <trace_filename>\n", exename);

    exit(1);
}

void parse_args(int argc, char **argv, char **log_file, char **trace_file)
{
    int index;
    static struct option long_opts[] =
    {
        {"log", 1, NULL, 'l'},
        {"trace", 1, NULL, 't'},
        {"help", 0, NULL, 0},
        {0, 0, 0, 0}
    };

    *log_file = NULL;
    *trace_file = NULL;
    while (1)
    {
        int c = getopt_long(argc, argv, "", long_opts, &index);

        if (c == -1) break;

        switch(c)
        {
            case 'l':
                *log_file = optarg;
                break;
            case 't':
                *trace_file = optarg;
                break;
            case 0:
            case '?':
            default:
                usage(argv[0]);
                break;
        }
    }

    if (optind < argc || !(*log_file) || !(*trace_file))
    {
        usage(argv[0]);
    }

    return;
}

int main(int argc, char *argv[])
{
    int ret;
    char *log_filename;
    char *trace_filename;
    struct darshan_job job;
    struct darshan_file next_file;
    darshan_fd log_file;
    int last_rank = 0;
    int total_event_cnt = 0;    /* total number of events stored in the event_list */
    struct darshan_event *event_list;

    /* parse command line args */
    parse_args(argc, argv, &log_filename, &trace_filename);

    /* open the darshan log file */
    log_file = darshan_log_open(log_filename, "r");
    if (!log_file)
    {
        fprintf(stderr, "darshan_log_open() failed to open %s.\n", log_filename);
        return -1;
    }

    /* get the stats for the entire job */
    ret = darshan_log_getjob(log_file, &job);
    if (ret < 0)
    {
        fprintf(stderr, "Error: unable to read job information from log file.\n");
        darshan_log_close(log_file);
        return -1;
    }

    /* TODO: we should probably be generating traces on a per file system basis ?? */

    /* allocate memory for the event list */
    event_list = malloc(DARSHAN_MAX_STORED_EVENTS * sizeof(*event_list));
    if (!event_list)
    {
        fprintf(stderr, "Error: not enough memory for event list.\n");
        darshan_log_close(log_file);
        return -1;
    }

    /* try to retrieve the first file record */
    ret = darshan_log_getfile(log_file, &job, &next_file);
    if (ret < 0)
    {
        fprintf(stderr, "Error: failed to parse log file.\n");
        fflush(stderr);
        free(event_list);
        return -1;
    }
    if (ret == 0)
    {
        /* the app did not store any IO stats */
        fprintf(stderr, "Error: no files contained in logfile.\n");
        darshan_log_close(log_file);
        free(event_list);
        return 0;
    }

    do
    {
        if (next_file.rank != -1 && next_file.rank < last_rank)
        {
            fprintf(stderr, "Error: log file contains out of order rank data.\n");
            fflush(stderr);
            free(event_list);
            return -1;
        }
        if (next_file.rank != -1)
            last_rank = next_file.rank;

        /* generate all events associated with this file */
        total_event_cnt = generate_file_events(&job, &next_file, event_list, total_event_cnt);       

        /* try to get next file */
    } while((ret = darshan_log_getfile(log_file, &job, &next_file)) == 1);

    if (ret < 0)
    {
        fprintf(stderr, "Error: failed to parse log file.\n");
        fflush(stderr);
        free(event_list);
        return -1;
    }

    darshan_log_close(log_file);

    /* sort the event_list into a time-ordered_list */
    sort_stored_events(event_list, total_event_cnt, event_compare);

    int i;
    for (i = 0; i < total_event_cnt; i++)
    {
        printf("Rank %"PRId64" attempting %s on file %"PRIu64" @ time %lf\n",
               event_list[i].rank,
               darshan_event_names[event_list[i].event_type],
               event_list[i].file_hash,
               event_list[i].time_stamp);
    }

    free(event_list);

    return 0;
}

/* store all events found in a particular file in no particular order */
int generate_file_events(struct darshan_job *job,
                         struct darshan_file *file,
                         struct darshan_event *event_list,
                         int event_list_ndx)
{
    int64_t psx_open_cnt = file->counters[CP_POSIX_OPENS];

    /* check for and generate any open/close events */
    if (psx_open_cnt && file->rank != -1)
    {
        double open_time = file->fcounters[CP_F_OPEN_TIMESTAMP];
        double close_time = file->fcounters[CP_F_CLOSE_TIMESTAMP];

        DARSHAN_TRACE_STORE_EVENT(&(event_list[event_list_ndx]),
                                  POSIX_OPEN,
                                  file,
                                  open_time);
        event_list_ndx++;
        assert(event_list_ndx < DARSHAN_MAX_STORED_EVENTS);        

        DARSHAN_TRACE_STORE_EVENT(&(event_list[event_list_ndx]),
                                  POSIX_CLOSE,
                                  file,
                                  close_time);
        event_list_ndx++;
        assert(event_list_ndx < DARSHAN_MAX_STORED_EVENTS);
    }

    return event_list_ndx;
}

/* use qsort to sort the event_list according to the function compar */
void sort_stored_events(struct darshan_event *event_list,
                        int event_list_cnt,
                        int (*compar)(const void*, const void*))
{
    qsort(event_list, event_list_cnt, sizeof(*event_list), compar);

    return;
}

/* sort events so their timestamps are non-decreasing */
int event_compare(const void *p1, const void *p2)
{
    const struct darshan_event *elem1 = p1;
    const struct darshan_event *elem2 = p2;

    if (elem1->time_stamp < elem2->time_stamp)
    {
        return -1;
    }
    else if (elem1->time_stamp > elem2->time_stamp)
    {
        return 1;
    }
    else
    {
        return 0;
    }
}
