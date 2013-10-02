#include <stdlib.h>
#include <getopt.h>
#include <string.h>
#include <assert.h>
#include <fcntl.h>

#include "darshan-logutils.h"

#define DARSHAN_MAX_STORED_EVENTS 500000

#define NEGLIGIBLE_DELAY_PCT .0001
#define DEF_INTER_IO_DELAY_PCT 0.2
#define DEF_INTER_CYC_DELAY_PCT 0.4

typedef enum
{
    POSIX_OPEN = 0,
    POSIX_CLOSE,
    POSIX_READ,
    POSIX_WRITE,
    BARRIER,
    DELAY,
} darshan_event_type;

static const char *darshan_event_names[] =
{
    "POSIX_OPEN",
    "POSIX_CLOSE",
    "POSIX_READ",
    "POSIX_WRITE",
    "BARRIER",
    "DELAY",
};

struct darshan_event
{
    int64_t rank;
    darshan_event_type type;
    union
    {
        struct
        {
            uint64_t file;
            int create_flag;           
        } open;
        struct
        {
            uint64_t file;
        } close;
        struct
        {
            uint64_t file;
            off_t offset;
            size_t size;
        } read;
        struct
        {
            uint64_t file;
            off_t offset;
            size_t size;
        } write;
        struct
        {
            int proc_count;
            int root;
        } barrier;
        struct
        {
            double seconds;
        } delay;
    } event_params;
};

void generate_psx_file_events(struct darshan_job *job,
                              struct darshan_file *file);

void generate_delay_event(struct darshan_file *file,
                          double seconds);

void generate_psx_open_event(struct darshan_file *file);

void generate_psx_io_events(struct darshan_file *file,
                            int64_t read_cnt,
                            int64_t write_cnt,
                            double inter_io_delay);

void generate_psx_close_event(struct darshan_file *file);

void generate_barrier_event();

void store_event(struct darshan_event *event);


static struct darshan_event *event_list = NULL; /* global list for application events */
static int64_t event_list_cnt = 0;    /* current member count of the event list */
static int64_t app_run_time = 0;

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
    app_run_time = job.end_time - job.start_time + 1;
    
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

    /* TODO: we should probably make sure counters we know we will use are not invalid (-1) */
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
        generate_psx_file_events(&job, &next_file);       

        /* TODO SORT! */

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

    int64_t i;
    for (i = 0; i < event_list_cnt; i++)
    {
        if (event_list[i].type == DELAY)
        {
            printf("Rank %"PRId64" DELAY for %lf seconds\n",
                   event_list[i].rank,
                   event_list[i].event_params.delay.seconds);
        }
        else if (event_list[i].type == POSIX_OPEN)
        {
            printf("Rank %"PRId64" OPEN file %"PRIu64"\n",
                   event_list[i].rank,
                   event_list[i].event_params.open.file);
        }
        else if (event_list[i].type == POSIX_CLOSE)
        {
            printf("Rank %"PRId64" CLOSE file %"PRIu64"\n",
                   event_list[i].rank,
                   event_list[i].event_params.open.file);
        }
        else if (event_list[i].type == POSIX_READ)
        {
            printf("Rank %"PRId64" READ file %"PRIu64"\n",
                   event_list[i].rank,
                   event_list[i].event_params.read.file);
        }
        else if (event_list[i].type == POSIX_WRITE)
        {
            printf("Rank %"PRId64" WRITE file %"PRIu64"\n",
                   event_list[i].rank,
                   event_list[i].event_params.write.file);
        }
    }

    free(event_list);

    return 0;
}

/* store all events found in a particular file in no particular order */
void generate_psx_file_events(struct darshan_job *job,
                              struct darshan_file *file)
{
    int64_t reads_per_cycle;
    int64_t extra_reads;
    int64_t writes_per_cycle;
    int64_t extra_writes;
    double first_io_time;
    double last_io_time;
    double delay_per_cycle;
    double first_io_delay_pct;
    double close_delay_pct;
    double inter_cycle_delay_pct;
    double inter_io_delay_pct;
    double total_delay_pct = 0.0;
    int64_t i;

    /* set last close time to end of execution, if necessary */
    if (!(file->fcounters[CP_F_CLOSE_TIMESTAMP]))
        file->fcounters[CP_F_CLOSE_TIMESTAMP] = app_run_time;

    /* determine amount of io operations per open-io-close cycle */
    reads_per_cycle = file->counters[CP_POSIX_READS] / file->counters[CP_POSIX_OPENS];
    writes_per_cycle = file->counters[CP_POSIX_WRITES] / file->counters[CP_POSIX_OPENS];
    extra_reads = file->counters[CP_POSIX_READS] % file->counters[CP_POSIX_OPENS];
    extra_writes = file->counters[CP_POSIX_WRITES] % file->counters[CP_POSIX_OPENS];

    /* determine delay available per open-io-close cycle */
    delay_per_cycle = ((file->fcounters[CP_F_CLOSE_TIMESTAMP] - file->fcounters[CP_F_OPEN_TIMESTAMP])
                     - (file->fcounters[CP_F_POSIX_READ_TIME] + file->counters[CP_F_POSIX_WRITE_TIME])
                     - (file->fcounters[CP_F_POSIX_META_TIME])) / file->counters[CP_POSIX_OPENS];

    /* determine the time of the first io operation */
    if ((file->fcounters[CP_F_READ_START_TIMESTAMP] < file->fcounters[CP_F_WRITE_START_TIMESTAMP]) &&
        file->fcounters[CP_F_READ_START_TIMESTAMP] != 0.0)
    {
        first_io_time = file->fcounters[CP_F_READ_START_TIMESTAMP];
    }
    else
    {
        first_io_time = file->fcounters[CP_F_WRITE_START_TIMESTAMP];
    }

    /* determine the time of the last io operation */
    if (file->fcounters[CP_F_READ_END_TIMESTAMP] > file->fcounters[CP_F_WRITE_END_TIMESTAMP])
    {
        last_io_time = file->fcounters[CP_F_READ_END_TIMESTAMP];
    }
    else
    {
        last_io_time = file->fcounters[CP_F_WRITE_END_TIMESTAMP];
    }

    /* no delay contribution for intercycle delay if there is only a single cycle */
    if (file->counters[CP_POSIX_OPENS] == 1)
    {
        inter_cycle_delay_pct = 0.0;
    }
    else
    {
        inter_cycle_delay_pct = DEF_INTER_CYC_DELAY_PCT;
    }
    /* no delay contribution for interio delay if there is only a single io op */
    if ((reads_per_cycle + writes_per_cycle) == 1)
    {
        inter_io_delay_pct = 0.0;
    }
    else
    {
        inter_io_delay_pct = DEF_INTER_IO_DELAY_PCT;
    }

    /* determine delay contribution for first io and close delays */
    first_io_delay_pct = (first_io_time - file->fcounters[CP_F_OPEN_TIMESTAMP]) / delay_per_cycle;
    close_delay_pct = (file->fcounters[CP_F_CLOSE_TIMESTAMP] - last_io_time) / delay_per_cycle;

    /* adjust per cycle delay percentages using a simple heuristic */
    total_delay_pct = inter_cycle_delay_pct + inter_io_delay_pct +
                      first_io_delay_pct + close_delay_pct;
    inter_cycle_delay_pct += (inter_cycle_delay_pct / total_delay_pct) * (1 - total_delay_pct);
    inter_io_delay_pct += (inter_io_delay_pct / total_delay_pct) * (1 - total_delay_pct);
    first_io_delay_pct += (first_io_delay_pct / total_delay_pct) * (1 - total_delay_pct);
    close_delay_pct += (close_delay_pct / total_delay_pct) * (1 - total_delay_pct);

    /* generate delay until first open */
    if (file->fcounters[CP_F_OPEN_TIMESTAMP])
    {
        generate_delay_event(file, file->fcounters[CP_F_OPEN_TIMESTAMP]);
    }

    /* if this file was opened by a single process (rank) */
    if (file->rank > -1)
    {
        for (i = 0; i < file->counters[CP_POSIX_OPENS]; i++)
        {
            /* generate an open event */
            generate_psx_open_event(file);

            /* generate potential delay from first open to first io */
            generate_delay_event(file, first_io_delay_pct * delay_per_cycle);

            /* generate io events for this sequence */
            generate_psx_io_events(file, reads_per_cycle, writes_per_cycle,
                                   inter_io_delay_pct * delay_per_cycle);

            /* if this is the last open, do any extra read/write operations */
            if ((i == file->counters[CP_POSIX_OPENS] - 1) && (extra_reads || extra_writes))
            {
                generate_psx_io_events(file, extra_reads, extra_writes, 0.0);
            }

            /* generate potential delay from last io to close */
            generate_delay_event(file, close_delay_pct * delay_per_cycle);

            /* generate a close for the open event at the start of the loop */
            generate_psx_close_event(file);

            /* generate potential intercycle delay if more than one cycle */
            if (i != file->counters[CP_POSIX_OPENS] - 1)
            {
                generate_delay_event(file, inter_cycle_delay_pct * delay_per_cycle);
            }
        }
    }
    else    /* TODO: this is a collective open across all ranks */
    {

    }

    return;
}

void generate_delay_event(struct darshan_file *file,
                          double seconds)
{
    struct darshan_event next_event = { .rank = file->rank };
    
    if (seconds <= NEGLIGIBLE_DELAY_PCT * app_run_time)
        return;

    next_event.type = DELAY;
    next_event.event_params.delay.seconds = seconds;
    store_event(&next_event);

    return;
}

void generate_psx_open_event(struct darshan_file *file)
{
    struct darshan_event next_event = { .rank = file->rank };

    next_event.type = POSIX_OPEN;
    next_event.event_params.open.file = file->hash;
    next_event.event_params.open.create_flag = 0; /* TODO: create flag ?? */
    store_event(&next_event);

    return;
}

void generate_psx_io_events(struct darshan_file *file,
                            int64_t read_cnt,
                            int64_t write_cnt,
                            double inter_io_delay)
{
    int64_t reads = 0, writes = 0;
    double rw_switch = file->counters[CP_RW_SWITCHES] /
                       (file->counters[CP_POSIX_READS] + file->counters[CP_POSIX_WRITES]);
    struct darshan_event next_event = { .rank = file->rank };

    /* read = 0, write = 1 ... initialized to first io op executed in application */
    int rw = ((file->fcounters[CP_F_READ_START_TIMESTAMP] <
               file->fcounters[CP_F_WRITE_START_TIMESTAMP]) &&
              file->fcounters[CP_F_READ_START_TIMESTAMP] != 0.0) ? 0 : 1;

    /* loop to generate all reads/writes for this open/close sequence */
    while (1)
    {
        if (reads == read_cnt)
        {
            rw = 1; /* write */
        }
        else if (writes == write_cnt)
        {
            rw = 0; /* read */
        }
        else
        {
            /* else we have reads and writes to perform */
            if (((double)rand() / (double)RAND_MAX - 1) < rw_switch)
            {
                /* toggle read/write flag */
                rw ^= 1;
            }
        }

        if (!rw)
        {
            /* generate a read event */
            next_event.type = POSIX_READ;
            next_event.event_params.read.file = file->hash;
            next_event.event_params.read.size = 1;      /* TODO: size and offset */
            next_event.event_params.read.offset = 0;
            store_event(&next_event);
            reads++;
        }
        else
        {
            /* generate a write event */
            next_event.type = POSIX_WRITE;
            next_event.event_params.write.file = file->hash;
            next_event.event_params.write.size = 1;      /* TODO: size and offset */
            next_event.event_params.write.offset = 0;
            store_event(&next_event);
            writes++;
        }

        if ((reads == read_cnt) && (writes == write_cnt))
        {
            break;
        }

        /* generate appropriate delay between i/o operations */
        generate_delay_event(file, inter_io_delay / (read_cnt + write_cnt - 1));
    }

    return;
}

void generate_psx_close_event(struct darshan_file *file)
{
    struct darshan_event next_event = { .rank = file->rank };

    next_event.type = POSIX_CLOSE;
    next_event.event_params.close.file = file->hash;
    store_event(&next_event);

    return;
}

void generate_barrier_event()
{

    return;
}

void store_event(struct darshan_event *event)
{
    assert(event_list_cnt != DARSHAN_MAX_STORED_EVENTS);
    memcpy(&(event_list[event_list_cnt++]), event, sizeof(*event));

    return;
}
