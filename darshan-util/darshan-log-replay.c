#include <stdlib.h>
#include <sys/types.h>
#include <getopt.h>
#include <string.h>
#include <assert.h>
#include <fcntl.h>
#include <math.h>

#include "darshan-logutils.h"

#include "uthash-1.9.2/src/uthash.h"

#define MYHASH 5234986228163024165

#define DEF_INTER_IO_DELAY_PCT 0.2
#define DEF_INTER_CYC_DELAY_PCT 0.4
#define MPI_IO_ARTIFACT_OPENS 1
#define RANKS_PER_IO_AGGREGATOR 32

typedef enum
{
    POSIX_OPEN = 0,
    POSIX_CLOSE,
    POSIX_READ,
    POSIX_WRITE,
    BARRIER,
} darshan_event_type;

struct darshan_event
{
    int64_t rank;
    darshan_event_type type;
    double start_time;
    double end_time;
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
            int64_t proc_count;
            int64_t root;
        } barrier;
    } event_params;
};

typedef struct
{
    UT_hash_handle hlink;
    uint64_t hash;
    double time;
} hash_entry_t;

/** function prototypes **/
int event_preprocess(const char *log_filename,
                     int event_file_fd);

void check_file_counters(struct darshan_file *file);

void generate_psx_ind_file_events(struct darshan_file *file);

void generate_psx_coll_file_events(struct darshan_file *file);

double generate_psx_open_event(struct darshan_file *file,
                               int create_flag,
                               double cur_time);

double generate_psx_ind_io_events(struct darshan_file *file,
                                  int64_t read_cnt,
                                  int64_t write_cnt,
                                  double inter_io_delay,
                                  double cur_time);

double generate_psx_coll_io_events(struct darshan_file *file,
                                   int64_t coll_reads,
                                   int64_t coll_writes,
                                   int64_t ind_reads,
                                   int64_t ind_writes,
                                   double inter_io_delay,
                                   double cur_time);

double generate_psx_close_event(struct darshan_file *file,
                                double cur_time);

double generate_barrier_event(struct darshan_file *file,
                              int64_t root,
                              double cur_time);

int event_comp(const void *p1,
               const void *p2);

int merge_file_events(struct darshan_file *file);

int store_rank_events(int event_file_fd,
                      int64_t rank);

int print_events(struct darshan_event *event_list,
                 int64_t event_list_cnt);

/** */

/** global variables used by the workload generator */
static struct darshan_event *rank_event_list = NULL; 
static int64_t rank_event_list_cnt = 0;
static int64_t rank_event_list_max = 0;
static struct darshan_event *file_event_list = NULL;
static int64_t file_event_list_cnt = 0;
static int64_t file_event_list_max = 0;

static uint64_t *header_buf = NULL;

static int64_t app_run_time = 0;
static int64_t nprocs = 0;

static hash_entry_t *created_files_hash = NULL;

/* check variables */
int64_t num_opens = 0, num_reads = 0, num_writes = 0, num_barriers = 0;

/** */

int usage(char *exename)
{
    fprintf(stderr, "Usage: %s --log <log_filename> --out <output_filename>\n", exename);

    exit(1);
}

void parse_args(int argc, char **argv, char **log_file, char **out_file)
{
    int index;
    static struct option long_opts[] =
    {
        {"log", 1, NULL, 'l'},
        {"out", 1, NULL, 'o'},
        {"help", 0, NULL, 0},
        {0, 0, 0, 0}
    };

    *log_file = NULL;
    *out_file = NULL;
    while (1)
    {
        int c = getopt_long(argc, argv, "", long_opts, &index);

        if (c == -1) break;

        switch(c)
        {
            case 'l':
                *log_file = optarg;
                break;
            case 'o':
                *out_file = optarg;
                break;
            case 0:
            case '?':
            default:
                usage(argv[0]);
                break;
        }
    }

    if (optind < argc || !(*log_file) || !(*out_file))
    {
        usage(argv[0]);
    }

    return;
}
 
int main(int argc, char *argv[])
{
    int ret;
    char *log_filename;
    char *events_filename;
    struct darshan_job job;
    struct darshan_file next_file;
    darshan_fd log_file;
    int event_file_fd;
    hash_entry_t *curr, *tmp;
    int64_t last_rank;

    /* TODO: we should probably be generating traces on a per file system basis ?? */

    /* parse command line args */
    parse_args(argc, argv, &log_filename, &events_filename);

    /* seed the random number generator */
    srand(time(NULL));

    /* open the output file for storing generated events */
    event_file_fd = open(events_filename, O_CREAT | O_TRUNC | O_APPEND | O_RDWR, 0644);
    if (event_file_fd == -1)
    {
        fprintf(stderr, "Error: failed to open %s.\n", events_filename);
        fflush(stderr);
        return -1;
    }
 
    /* preprocess the darshan log file to init file/job data and write the output header */
    ret = preprocess_events(log_filename, event_file_fd);
    if (ret < 0)
    {
        return ret;
    }

    /* re-open the darshan log file to get a fresh file pointer */
    log_file = darshan_log_open(log_filename, "r");
    if (!log_file)
    {
        fprintf(stderr, "darshan_log_open() failed to open %s.\n", log_filename);
        fflush(stderr);
        close(event_file_fd);
        free(header_buf);
        return -1;
    }

    /* allocate memory for rank and file event lists */
    rank_event_list = malloc(rank_event_list_max * sizeof(*rank_event_list));
    file_event_list = malloc(file_event_list_max * sizeof(*file_event_list));
    if (!rank_event_list || !file_event_list)
    {
        fprintf(stderr, "Error: unable to allocate memory for event streams.\n");
        fflush(stderr);
        darshan_log_close(log_file);
        close(event_file_fd);
        free(header_buf);
    }
//    printf("rank_event_list_cnt = %"PRId64"\nfile_event_list_cnt = %"PRId64"\n", rank_event_list_max, file_event_list_max);

    /* try to retrieve the first file record */
    ret = darshan_log_getfile(log_file, &job, &next_file);
    if (ret < 0)
    {
        fprintf(stderr, "Error: failed to parse log file.\n");
        fflush(stderr);
        close(event_file_fd);
        free(rank_event_list);
        free(file_event_list);
        free(header_buf);
        return -1;
    }
    if (ret == 0)
    {
        /* the app did not store any IO stats */
        fprintf(stderr, "Error: no files contained in logfile.\n");
        fflush(stderr);
        darshan_log_close(log_file);
        close(event_file_fd);
        free(rank_event_list);
        free(file_event_list);
        free(header_buf);
        return 0;
    }

    last_rank = next_file.rank;
    do
    {
        /* generate all events associated with this file */
//    if (next_file.hash == MYHASH) {
        if (next_file.rank > -1)
        {
            generate_psx_ind_file_events(&next_file);       
        }
        else
        {
           generate_psx_coll_file_events(&next_file);
        }
//}
        /* if the rank is the same as the previous rank, just merge their events together */
        if (next_file.rank == last_rank)
        {
            /* merge the generated events for this file into the global list for this rank */
            ret = merge_file_events(&next_file);
            if (ret < 0)
            {
                free(rank_event_list);
                free(file_event_list);
                darshan_log_close(log_file);
                close(event_file_fd);
                free(header_buf);
                return ret;
            }
        }
        /* else, write last_rank's events to file before merging new events over */
        else
        {
            ret = store_rank_events(event_file_fd, last_rank);
            if (ret < 0)
            {
                free(rank_event_list);
                free(file_event_list);
                darshan_log_close(log_file);
                free(header_buf);
                return ret;
            }
            ret = merge_file_events(&next_file);
            if (ret < 0)
            {
                free(rank_event_list);
                free(file_event_list);
                darshan_log_close(log_file);
                close(event_file_fd);
                free(header_buf);
                return ret;
            }
        }

        last_rank = next_file.rank;
        /* try to get next file */
    } while((ret = darshan_log_getfile(log_file, &job, &next_file)) == 1);

    if (ret < 0)
    {
        fprintf(stderr, "Error: failed to parse log file.\n");
        fflush(stderr);
        free(rank_event_list);
        free(file_event_list);
        close(event_file_fd);
        free(header_buf);
        return -1;
    }

    ret = store_rank_events(event_file_fd, last_rank);
    if (ret < 0)
    {
        free(rank_event_list);
        free(file_event_list);
        darshan_log_close(log_file);
        free(header_buf);
        return ret;
    }

    darshan_log_close(log_file);
    close(event_file_fd);

    HASH_ITER(hlink, created_files_hash, curr, tmp)
    {
        HASH_DELETE(hlink, created_files_hash, curr);
        free(curr);
    }

    free(rank_event_list);
    free(file_event_list);
    free(header_buf);

    return 0;
}

int preprocess_events(const char *log_filename,
                      int event_file_fd)
{
    int ret;
    darshan_fd log_file;
    struct darshan_job job;
    struct darshan_file next_file;
    int64_t last_rank;
    uint64_t file_event_cnt = 0, rank_event_cnt = 0;
    ssize_t bytes_written;
    uint64_t coll_event_cnt = 0;
    uint64_t cur_offset = 0;
    uint64_t i;
    hash_entry_t *hfile = NULL;

    /* open the darshan log file */
    log_file = darshan_log_open(log_filename, "r");
    if (!log_file)
    {
        fprintf(stderr, "darshan_log_open() failed to open %s.\n", log_filename);
        fflush(stderr);
        return -1;
    }

    /* get the stats for the entire job */
    ret = darshan_log_getjob(log_file, &job);
    if (ret < 0)
    {
        fprintf(stderr, "Error: unable to read job information from log file.\n");
        fflush(stderr);
        darshan_log_close(log_file);
        close(event_file_fd);
        return -1;
    }
    app_run_time = job.end_time - job.start_time + 1;
    nprocs = job.nprocs;

    /* allocate memory for the file header, and set its first value equal to the number of ranks */
    header_buf = malloc((nprocs + 2) * sizeof(uint64_t));
    if (!header_buf)
    {
        fprintf(stderr, "Error: no memory available to create output event file header.\n");
        fflush(stderr);
        darshan_log_close(log_file);
        close(event_file_fd);
        return -1;
    }
    header_buf[0] = (uint64_t)nprocs;
    for (i = 1; i < nprocs + 2; i++)
        header_buf[i] = 0;

    /* try to retrieve the first file record */
    ret = darshan_log_getfile(log_file, &job, &next_file);
    if (ret < 0)
    {
        fprintf(stderr, "Error: failed to parse log file.\n");
        fflush(stderr);
        close(event_file_fd);
        free(header_buf);
        return -1;
    }
    if (ret == 0)
    {
        /* the app did not store any IO stats */
        fprintf(stderr, "Error: no files contained in logfile.\n");
        fflush(stderr);
        darshan_log_close(log_file);
        close(event_file_fd);
        free(header_buf);
        return -1;
    }

    last_rank = next_file.rank;
    cur_offset = (nprocs + 2) * sizeof(uint64_t);
    do
    {
        if (last_rank != next_file.rank)
        {
            /* set flag so the offset of collective events can be set last */
            if (last_rank == -1)
            {
                coll_event_cnt = rank_event_cnt;
            }
            else
            {
                /* store last_rank's event count in it's corresponding field of the header */
                header_buf[last_rank + 1] = cur_offset;
                cur_offset += rank_event_cnt * sizeof(struct darshan_event);
            }
         
            /* update maximum number of rank events */   
            if (rank_event_cnt > rank_event_list_max)
                rank_event_list_max = rank_event_cnt;

            rank_event_cnt = 0;
        }

        /* make sure there is no out of order data */
        if (next_file.rank != -1 && next_file.rank < last_rank)
        {
            fprintf(stderr, "Error: log file contains out of order rank data.\n");
            fflush(stderr);
            close(event_file_fd);
            free(header_buf);
            return -1;
        }
        last_rank = next_file.rank;

        /* make sure the counters we use are valid in this log */
        check_file_counters(&next_file);

        /* determine number of events to be generated for this file */
        if (next_file.rank > -1)
        {
            file_event_cnt = (2 * next_file.counters[CP_POSIX_OPENS]) +
                             next_file.counters[CP_POSIX_READS] +
                             next_file.counters[CP_POSIX_WRITES];
        }
        else
        {
            if (next_file.counters[CP_COLL_OPENS])
            {
                file_event_cnt = (3 * (next_file.counters[CP_COLL_OPENS] / nprocs)) +
                                 (next_file.counters[CP_COLL_READS] / nprocs) +
                                 (next_file.counters[CP_COLL_WRITES] / nprocs) +
                                 next_file.counters[CP_POSIX_READS] +
                                 next_file.counters[CP_POSIX_WRITES];
            }
            else
            {
                file_event_cnt = (2 * next_file.counters[CP_POSIX_OPENS]) + 
                                 next_file.counters[CP_POSIX_READS] +
                                 next_file.counters[CP_POSIX_WRITES];
            }
        }

        if (file_event_cnt > file_event_list_max)
            file_event_list_max = file_event_cnt;

        rank_event_cnt += file_event_cnt;

        /*  if this file was created, store the timestamp of the first rank to open it.
         *  a file is determined to have been created if it was written to.
         *  NOTE: this is only necessary for independent files that may be opened by numerous ranks.
         */
        if ((next_file.counters[CP_BYTES_WRITTEN] > 0) && (next_file.rank > -1))
        {
            HASH_FIND(hlink, created_files_hash, &(next_file.hash), sizeof(uint64_t), hfile);

            if (!hfile)
            {
                hfile = malloc(sizeof(*hfile));
                if (!hfile)
                {
                    fprintf(stderr, "Error: unable to allocate hash memory\n");
                    fflush(stderr);
                    darshan_log_close(log_file);
                    close(event_file_fd);
                    free(header_buf);
                    return -1;
                }

                memset(hfile, 0, sizeof(*hfile));
                hfile->hash = next_file.hash;
                hfile->time = next_file.fcounters[CP_F_OPEN_TIMESTAMP];
                HASH_ADD(hlink, created_files_hash, hash, sizeof(uint64_t), hfile);
            }
            else
            {
                if (next_file.fcounters[CP_F_OPEN_TIMESTAMP] < hfile->time)
                    hfile->time = next_file.fcounters[CP_F_OPEN_TIMESTAMP];
            }
        }

        /* try to get next file */
    } while ((ret = darshan_log_getfile(log_file, &job, &next_file)) == 1);

    /* make sure no errors occurred while reading files from the log */
    if (ret < 0)
    {
        fprintf(stderr, "Error: failed to parse log file.\n");
        fflush(stderr);
        close(event_file_fd);
        free(header_buf);
        return -1;
    }

    /* store last_rank's event count in it's corresponding field of the header, if independent file */
    if (last_rank > -1)
    {
        header_buf[last_rank + 1] = cur_offset;
        cur_offset += rank_event_cnt * sizeof(struct darshan_event);
    }
    else
    {
        coll_event_cnt = rank_event_cnt;
    }

    /* set the offset of the collective events, if there are any */
    if (coll_event_cnt)
    {
        header_buf[nprocs + 1] = cur_offset;
    }

    /* write the header to the output events file */
    bytes_written = write(event_file_fd, header_buf, (nprocs + 2) * sizeof(uint64_t));
    if (bytes_written != ((nprocs + 2) * sizeof(uint64_t)))
    {
        fprintf(stderr, "Error: unable to write header to output events file.\n");
        fflush(stderr);
        darshan_log_close(log_file);
        close(event_file_fd);
        free(header_buf);
        return -1;
    }

    if (rank_event_cnt > rank_event_list_max)
        rank_event_list_max = rank_event_cnt;

    darshan_log_close(log_file);

    return 0;
}

void check_file_counters(struct darshan_file *file)
{
    assert(file->counters[CP_POSIX_OPENS] != -1);
    assert(file->fcounters[CP_F_OPEN_TIMESTAMP] != -1);
    assert(file->counters[CP_COLL_OPENS] != -1);
    assert(file->fcounters[CP_F_CLOSE_TIMESTAMP] != -1);
    assert(file->counters[CP_POSIX_READS] != -1);
    assert(file->counters[CP_POSIX_WRITES] != -1);
    assert(file->fcounters[CP_F_POSIX_READ_TIME] != -1);
    assert(file->fcounters[CP_F_POSIX_WRITE_TIME] != -1);
    assert(file->fcounters[CP_F_POSIX_META_TIME] != -1);
    assert(file->fcounters[CP_F_READ_START_TIMESTAMP] != -1);
    assert(file->fcounters[CP_F_WRITE_START_TIMESTAMP] != -1);
    assert(file->fcounters[CP_F_READ_END_TIMESTAMP] != -1);
    assert(file->fcounters[CP_F_WRITE_END_TIMESTAMP] != -1);
    assert(file->counters[CP_BYTES_READ] != -1);
    assert(file->counters[CP_BYTES_WRITTEN] != -1);
    assert(file->counters[CP_RW_SWITCHES] != -1);

    return;
}

/* store all events found in a particular independent file */
void generate_psx_ind_file_events(struct darshan_file *file)
{
    int64_t open_cnt = file->counters[CP_POSIX_OPENS];
    double cur_time = file->fcounters[CP_F_OPEN_TIMESTAMP];
    int64_t reads_per_open;
    int64_t extra_reads;
    int64_t writes_per_open;
    int64_t extra_writes;
    double first_io_time;
    double last_io_time;
    double delay_per_open;
    double first_io_delay_pct = 0.0;
    double close_delay_pct = 0.0;
    double inter_open_delay_pct = 0.0;
    double inter_io_delay_pct = 0.0;
    double total_delay_pct = 0.0;
    int create_flag;
    hash_entry_t *hfile = NULL;
    int64_t i;

    assert(!(file->counters[CP_COLL_OPENS])); /* should not be a collective open for one file */

    /* if the file was never really opened, just return because we have no timing info */
    if (open_cnt == 0)
        return;

    /* set file close time to the end of execution if it is not given */
    if (file->fcounters[CP_F_CLOSE_TIMESTAMP] == 0.0)
        file->fcounters[CP_F_CLOSE_TIMESTAMP] = app_run_time;

    /* determine amount of io operations per open-io-close cycle */
    reads_per_open = file->counters[CP_POSIX_READS] / open_cnt;
    writes_per_open = file->counters[CP_POSIX_WRITES] / open_cnt;
    extra_reads = file->counters[CP_POSIX_READS] % open_cnt;
    extra_writes = file->counters[CP_POSIX_WRITES] % open_cnt;

    /* determine delay available per open-io-close cycle */
    delay_per_open = (file->fcounters[CP_F_CLOSE_TIMESTAMP] - file->fcounters[CP_F_OPEN_TIMESTAMP]
                    - file->fcounters[CP_F_POSIX_READ_TIME] - file->fcounters[CP_F_POSIX_WRITE_TIME]
                    - file->fcounters[CP_F_POSIX_META_TIME]) / open_cnt;

    if (delay_per_open > 0.0)
    {
        /* determine the time of the first io operation */
        if ((file->fcounters[CP_F_READ_START_TIMESTAMP] < file->fcounters[CP_F_WRITE_START_TIMESTAMP])
            && file->fcounters[CP_F_READ_START_TIMESTAMP] != 0.0)
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

        /* no delay contribution for interopen delay if there is only a single open */
        if (open_cnt == 1)
        {
            inter_open_delay_pct = 0.0;
        }
        else
        {
            inter_open_delay_pct = DEF_INTER_CYC_DELAY_PCT;
        }
        /* no delay contribution for interio delay if there is one or less io op */
        if ((reads_per_open + writes_per_open) <= 1)
        {
            inter_io_delay_pct = 0.0;
        }
        else
        {
            inter_io_delay_pct = DEF_INTER_IO_DELAY_PCT;
        }

        /* determine delay contribution for first io and close delays */
        if (first_io_time != 0.0)
        {
            first_io_delay_pct = (first_io_time - file->fcounters[CP_F_OPEN_TIMESTAMP]) /
                                    delay_per_open;
            close_delay_pct = (file->fcounters[CP_F_CLOSE_TIMESTAMP] - last_io_time) /
                                    delay_per_open; 
        }
        else
        {
            first_io_delay_pct = 0.0;
            close_delay_pct = 1 - inter_open_delay_pct;
        }

        /* adjust per open delay percentages using a simple heuristic */
        total_delay_pct = inter_open_delay_pct + inter_io_delay_pct +
                          first_io_delay_pct + close_delay_pct;
        if ((total_delay_pct < 1) && (inter_open_delay_pct || inter_io_delay_pct))
        {
            /* only adjust interopen and interio delays if we underestimate */
            inter_open_delay_pct = (inter_open_delay_pct / 
                                   (inter_open_delay_pct + inter_io_delay_pct)) *
                                   (1 - first_io_delay_pct - close_delay_pct);
            inter_io_delay_pct = (inter_io_delay_pct / (inter_open_delay_pct + inter_io_delay_pct))
                                * (1 - first_io_delay_pct - close_delay_pct);
        }
        else
        {
            inter_open_delay_pct += (inter_open_delay_pct / total_delay_pct) * (1 - total_delay_pct);
            inter_io_delay_pct += (inter_io_delay_pct / total_delay_pct) * (1 - total_delay_pct);
            first_io_delay_pct += (first_io_delay_pct / total_delay_pct) * (1 - total_delay_pct);
            close_delay_pct += (close_delay_pct / total_delay_pct) * (1 - total_delay_pct);
        }
    }

    /* determine whether to set the create flag for the first open generated */
    HASH_FIND(hlink, created_files_hash, &(file->hash), sizeof(uint64_t), hfile);
    if (!hfile)
    {
        create_flag = 0;
    }
    else if (hfile->time == file->fcounters[CP_F_OPEN_TIMESTAMP])
    {
        create_flag = 1;
    }
    else
    {
        create_flag = 0;
    }

    /* generate open/io/close events for all cycles */
    /* TODO: add stats */
    for (i = 0; i < open_cnt; i++)
    {
        /* only set the create flag once */
        if (i == 1) create_flag = 0;

        /* generate an open event */
        cur_time = generate_psx_open_event(file, create_flag, cur_time);

        /* account for potential delay from first open to first io */
        cur_time += (first_io_delay_pct * delay_per_open);

        /* generate io events for this sequence */
        if (reads_per_open || writes_per_open)
        {
            cur_time = generate_psx_ind_io_events(file, reads_per_open, writes_per_open,
                                                  inter_io_delay_pct * delay_per_open, cur_time);
        }

        /* if this is the last open, do any extra read/write operations */
        if ((i == open_cnt - 1) && (extra_reads || extra_writes))
        {
            cur_time = generate_psx_ind_io_events(file, extra_reads, extra_writes, 0.0, cur_time);
        }

        /* account for potential delay from last io to close */
        cur_time += (close_delay_pct * delay_per_open);

        /* generate a close for the open event at the start of the loop */
        cur_time = generate_psx_close_event(file, cur_time);

        /* account for potential interopen delay if more than one open */
        if (i != open_cnt - 1)
        {
            cur_time += (inter_open_delay_pct * delay_per_open);
        }
    }

    return;
}

void generate_psx_coll_file_events(struct darshan_file *file)
{
    int64_t coll_reads = 0, coll_writes = 0;
    int64_t ind_reads = 0, ind_writes = 0;
    int create_flag = 0;
    double cur_time = file->fcounters[CP_F_OPEN_TIMESTAMP];
    double delay_per_cycle;
    double first_io_delay_pct = 0.0;
    double close_delay_pct = 0.0;
    double inter_cycle_delay_pct = 0.0;
    double inter_io_delay_pct = 0.0;
    double total_delay_pct = 0.0;
    int64_t i;

    /* the collective file was never opened (i.e., just stat-ed), so return */
    if (!(file->counters[CP_POSIX_OPENS]))
        return;

    /*  in this case, posix opens are less than mpi opens...
     *  this is probably a mpi deferred open -- assume app will not use this, currently.
     */
    if (file->counters[CP_COLL_OPENS])
        assert(file->counters[CP_POSIX_OPENS] >= file->counters[CP_COLL_OPENS]);

    assert(!(file->counters[CP_INDEP_OPENS])); /* for now, assume no independent opens */

    /* set file close time to the end of execution if it is not given */
    if (file->fcounters[CP_F_CLOSE_TIMESTAMP] == 0.0)
        file->fcounters[CP_F_CLOSE_TIMESTAMP] = app_run_time;

    /* determine collective i/o amount, then assign rest as independent io? */
    assert((file->counters[CP_COLL_READS] % nprocs) == 0);
    assert((file->counters[CP_COLL_WRITES] % nprocs) == 0);

    if (file->counters[CP_BYTES_WRITTEN])
    {
        create_flag = 1;
    }

    /* use assumptions based on the use of MPI to generate the I/O events */
    if (file->counters[CP_COLL_OPENS])
    {
        assert(file->counters[CP_COLL_OPENS] == nprocs);
        assert(!(file->counters[CP_COLL_READS] % nprocs));
        assert(!(file->counters[CP_COLL_WRITES] % nprocs));
        ind_reads = file->counters[CP_INDEP_READS];
        ind_writes = file->counters[CP_INDEP_WRITES];
        coll_reads = file->counters[CP_COLL_READS] / nprocs;
        coll_writes = file->counters[CP_COLL_WRITES] / nprocs;

        /*  if we have leftover opens in a MPI collective open pattern, it is likely due to rank 0
         *  creating the file, then all ranks opening it.
         */
        if (file->counters[CP_POSIX_OPENS] > file->counters[CP_COLL_OPENS])
        {
            assert((file->counters[CP_POSIX_OPENS] - file->counters[CP_COLL_OPENS]) ==
                   MPI_IO_ARTIFACT_OPENS);
            assert(create_flag);

            /* temporarily set the file's rank to 0, so the open/close events are assigned properly */
            file->rank = 0;

            /* generate the open/close events for creating the collective file */
            cur_time = generate_psx_open_event(file, create_flag, cur_time);
            cur_time = generate_psx_close_event(file, cur_time);
            create_flag = 0;
            file->rank = -1;
        }

        /* generate a collective barrier */
        cur_time = generate_barrier_event(file, 0, cur_time);   

        /* open the file */
        cur_time = generate_psx_open_event(file, 0, cur_time);

        /* generate i/o */
        cur_time = generate_psx_coll_io_events(file, coll_reads, coll_writes, 
                                               ind_reads, ind_writes, 0.0, cur_time);

        /* close the file */
        cur_time = generate_psx_close_event(file, cur_time);
    }
    else
    {
        ind_reads = file->counters[CP_POSIX_READS];
        ind_writes = file->counters[CP_POSIX_WRITES];
        int64_t reads_this_open, writes_this_open;
        double max_cur_time = 0.0, tmp_cur_time;

        for (i = 0; i < (file->counters[CP_POSIX_OPENS] / nprocs) * nprocs; i++)
        {
            reads_this_open = (ind_reads + (file->counters[CP_POSIX_OPENS] - i) - 1) /
                              (file->counters[CP_POSIX_OPENS] - i);
            ind_reads -= reads_this_open;
            writes_this_open = (ind_writes + (file->counters[CP_POSIX_OPENS] - i) - 1) /
                               (file->counters[CP_POSIX_OPENS] - i);
            ind_writes -= writes_this_open;

            if ((i % nprocs) == 0)
            {
                cur_time = generate_psx_open_event(file, create_flag, cur_time);
                create_flag = 0;

                file->rank = i % nprocs; 
                tmp_cur_time = generate_psx_ind_io_events(file, reads_this_open, writes_this_open,
                                                          0.0, cur_time);
                if (tmp_cur_time > max_cur_time)
                    max_cur_time = tmp_cur_time;
            }
            else if ((i % nprocs) == nprocs - 1)
            {
                file->rank = i % nprocs;
                tmp_cur_time = generate_psx_ind_io_events(file, reads_this_open, writes_this_open,
                                                          0.0, cur_time);
                if (tmp_cur_time > max_cur_time)
                    max_cur_time = tmp_cur_time;

                file->rank = -1;
                cur_time = generate_psx_close_event(file, max_cur_time);
            }
            else
            {
                file->rank = i % nprocs;
                tmp_cur_time = generate_psx_ind_io_events(file, reads_this_open, writes_this_open,
                                                          0.0, cur_time);

                if (tmp_cur_time > max_cur_time)
                    max_cur_time = tmp_cur_time;
            }
        }

        tmp_cur_time = cur_time;
        for (i = (file->counters[CP_POSIX_OPENS] / nprocs) * nprocs; 
             i < file->counters[CP_POSIX_OPENS]; i++)
        {
            reads_this_open = (ind_reads + (file->counters[CP_POSIX_OPENS] - i) - 1) /
                              (file->counters[CP_POSIX_OPENS] - i);
            ind_reads -= reads_this_open;
            writes_this_open = (ind_writes + (file->counters[CP_POSIX_OPENS] - i) - 1) /
                               (file->counters[CP_POSIX_OPENS] - i);
            ind_writes -= writes_this_open;

            file->rank = i % nprocs;
            cur_time = generate_psx_open_event(file, create_flag, cur_time);
            cur_time = generate_psx_ind_io_events(file, reads_this_open, writes_this_open,
                                                  0.0, cur_time);
            cur_time = generate_psx_close_event(file, cur_time);
            cur_time = tmp_cur_time;
        }

        /* we need to sort the file list, because it is not in order */
        qsort(file_event_list, file_event_list_cnt, sizeof(*file_event_list), event_comp);
    }

    return;
}

double generate_psx_open_event(struct darshan_file *file,
                               int create_flag,
                               double cur_time)
{
    struct darshan_event next_event = { .rank = file->rank,
                                        .type = POSIX_OPEN,
                                        .start_time = cur_time
                                      };

    /* identify the file hash value and whether the file was created or not */
    next_event.event_params.open.file = file->hash;
    next_event.event_params.open.create_flag = create_flag;

    /* set the end time of the event based on time spent in POSIX meta operations */
    cur_time += file->fcounters[CP_F_POSIX_META_TIME] /
                (2 * file->counters[CP_POSIX_OPENS]);
    next_event.end_time = cur_time;

    /* store the open event */
    assert(file_event_list_cnt != file_event_list_max);
    file_event_list[file_event_list_cnt++] = next_event;
    num_opens++;

    return cur_time;
}

double generate_psx_ind_io_events(struct darshan_file *file,
                                  int64_t read_cnt,
                                  int64_t write_cnt,
                                  double inter_io_delay,
                                  double cur_time)
{
    int64_t reads = 0, writes = 0;
    double rd_bw, wr_bw;
    double rw_switch = file->counters[CP_RW_SWITCHES] /
                       (file->counters[CP_POSIX_READS] + file->counters[CP_POSIX_WRITES]);
    struct darshan_event next_event = { .rank = file->rank };

    /* read = 0, write = 1 ... initialized to first io op executed in application */
    int rw = ((file->fcounters[CP_F_READ_START_TIMESTAMP] <
               file->fcounters[CP_F_WRITE_START_TIMESTAMP]) &&
              file->fcounters[CP_F_READ_START_TIMESTAMP] != 0.0) ? 0 : 1;

    /* determine the read/write "bandwidth" seen for this file */
    if (file->fcounters[CP_F_POSIX_READ_TIME])
        rd_bw = file->counters[CP_BYTES_READ] / file->fcounters[CP_F_POSIX_READ_TIME];
    if (file->fcounters[CP_F_POSIX_WRITE_TIME])
        wr_bw = file->counters[CP_BYTES_WRITTEN] / file->fcounters[CP_F_POSIX_WRITE_TIME];

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
        else if ((reads != 0) && (writes != 0))
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
            next_event.start_time = cur_time;
            next_event.event_params.read.file = file->hash;
            next_event.event_params.read.size = 10;      /* TODO: size and offset */
            next_event.event_params.read.offset = 0;

            /* set the end time based on observed bandwidth and io size */
            cur_time += (next_event.event_params.read.size / rd_bw);
            next_event.end_time = cur_time;
            reads++;
            num_reads++;
        }
        else
        {
            /* generate a write event */
            next_event.type = POSIX_WRITE;
            next_event.start_time = cur_time;
            next_event.event_params.write.file = file->hash;
            next_event.event_params.write.size = 10;      /* TODO: size and offset */
            next_event.event_params.write.offset = 0;

            /* set the end time based on observed bandwidth and io size */
            cur_time += (next_event.event_params.write.size / wr_bw);
            next_event.end_time = cur_time;
            writes++;
            num_writes++;
        }

        /* store the i/o event */
        assert(file_event_list_cnt != file_event_list_max);
        file_event_list[file_event_list_cnt++] = next_event;

        if ((reads == read_cnt) && (writes == write_cnt))
            break;

        /* update current time to account for possible delay between i/o operations */
        cur_time += (inter_io_delay / (read_cnt + write_cnt - 1));
    }

    return cur_time;
}

double generate_psx_coll_io_events(struct darshan_file *file,
                                   int64_t coll_read_cnt,
                                   int64_t coll_write_cnt,
                                   int64_t ind_read_cnt,
                                   int64_t ind_write_cnt,
                                   double inter_io_delay,
                                   double cur_time)
{
    double rd_bw, wr_bw;
    double rw_switch = file->counters[CP_RW_SWITCHES] /
                       (file->counters[CP_POSIX_READS] + file->counters[CP_POSIX_WRITES]);
    struct darshan_event next_event;
    int64_t coll_reads = 0, coll_writes = 0;
    int64_t ind_reads = 0, ind_writes = 0;
    int64_t aggregator_cnt = ceil(nprocs / RANKS_PER_IO_AGGREGATOR);
    int64_t extra_psx_reads = file->counters[CP_POSIX_READS] - file->counters[CP_INDEP_READS];
    int64_t extra_psx_writes = file->counters[CP_POSIX_WRITES] - file->counters[CP_INDEP_WRITES];
    int64_t i;
    double max_cur_time;

    /* read = 0, write = 1 ... initialized to first io op executed in application */
    int rw = ((file->fcounters[CP_F_READ_START_TIMESTAMP] <
               file->fcounters[CP_F_WRITE_START_TIMESTAMP]) &&
              file->fcounters[CP_F_READ_START_TIMESTAMP] != 0.0) ? 0 : 1;

    /* determine the read/write "bandwidth" seen for this file */
    if (file->fcounters[CP_F_POSIX_READ_TIME])
        rd_bw = file->counters[CP_BYTES_READ] / file->fcounters[CP_F_POSIX_READ_TIME];
    if (file->fcounters[CP_F_POSIX_WRITE_TIME])
        wr_bw = file->counters[CP_BYTES_WRITTEN] / file->fcounters[CP_F_POSIX_WRITE_TIME];

    /* do collective i/o first */
    while (1)
    {
        if (coll_reads == coll_read_cnt)
        {
            rw = 1; /* write */
        }
        else if (coll_writes == coll_write_cnt)
        {
            rw = 0; /* read */
        }
        else if ((coll_reads != 0) && (coll_writes != 0))
        {
            /* else we have reads and writes to perform */
            if (((double)rand() / (double)RAND_MAX - 1) < rw_switch)
            {
                /* toggle read/write flag */
                rw ^= 1;
            }
        }

        /* generate a barrier event for the collective i/o operation */
        max_cur_time = 0.0;
        generate_barrier_event(file, 0, cur_time);

        /* distribute this round of posix i/o over collective i/o aggregators */
        next_event.rank = (rand() % aggregator_cnt) * RANKS_PER_IO_AGGREGATOR;
        if (!rw)
        {
            for (i = 0; i < ceil(extra_psx_reads / (coll_read_cnt - coll_reads)); i++)
            {
                /* generate a read event */
                next_event.type = POSIX_READ;
                next_event.start_time = cur_time;
                next_event.event_params.read.file = file->hash;
                next_event.event_params.read.size = 10;      /* TODO: size and offset */
                next_event.event_params.read.offset = 0;
                num_reads++;

                /* set the end time based on observed bandwidth and io size */
                next_event.end_time = (next_event.event_params.read.size / rd_bw);
                if (next_event.end_time > max_cur_time)
                    max_cur_time = next_event.end_time;

                /* store the i/o event */
                assert(file_event_list_cnt != file_event_list_max);
                file_event_list[file_event_list_cnt++] = next_event;

                next_event.rank += RANKS_PER_IO_AGGREGATOR;
                if (next_event.rank >= nprocs)
                    next_event.rank = 0;
            }
            extra_psx_reads -= i;
            coll_reads++;
        }
        else
        {
            for (i = 0; i < ceil(extra_psx_writes / (coll_write_cnt - coll_writes)); i++)
            {
                /* generate a write event */
                next_event.type = POSIX_WRITE;
                next_event.start_time = cur_time;
                next_event.event_params.write.file = file->hash;
                next_event.event_params.write.size = 10;      /* TODO: size and offset */
                next_event.event_params.write.offset = 0;
                num_writes++;

                /* set the end time based on observed bandwidth and io size */
                next_event.end_time = (next_event.event_params.write.size / wr_bw);
                if (next_event.end_time > max_cur_time)
                    max_cur_time = next_event.end_time;

                /* store the i/o event */
                assert(file_event_list_cnt != file_event_list_max);
                file_event_list[file_event_list_cnt++] = next_event;

                next_event.rank += RANKS_PER_IO_AGGREGATOR;
                if (next_event.rank >= nprocs)
                    next_event.rank = 0;
            }
            extra_psx_writes -= i;
            coll_writes++;
        }

        cur_time = max_cur_time;
        if ((coll_reads == coll_read_cnt) && (coll_writes == coll_write_cnt))
            break;
    }

    /* then assign the independent i/o in round robin manner, starting with random rank */
    next_event.rank = rand() % nprocs;
    while (1)
    {
        if (ind_reads == ind_read_cnt)
        {
            rw = 1; /* write */
        }
        else if (ind_writes == ind_write_cnt)
        {
            rw = 0; /* read */
        }
        else if ((ind_reads != 0) && (ind_writes != 0))
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
            next_event.start_time = cur_time;
            next_event.event_params.read.file = file->hash;
            next_event.event_params.read.size = 10;      /* TODO: size and offset */
            next_event.event_params.read.offset = 0;
            ind_reads++;
            num_reads++;

            /* set the end time based on observed bandwidth and io size */
            next_event.end_time = (next_event.event_params.read.size / rd_bw);
        }
        else
        {
            /* generate a write event */
            next_event.type = POSIX_WRITE;
            next_event.start_time = cur_time;
            next_event.event_params.write.file = file->hash;
            next_event.event_params.write.size = 10;      /* TODO: size and offset */
            next_event.event_params.write.offset = 0;
            ind_writes++;
            num_writes++;

            /* set the end time based on observed bandwidth and io size */
            next_event.end_time = (next_event.event_params.write.size / wr_bw);
        }

        if (next_event.end_time > max_cur_time)
            max_cur_time = next_event.end_time;

        /* store the i/o event */
        assert(file_event_list_cnt != file_event_list_max);
        file_event_list[file_event_list_cnt++] = next_event;

        /* sync rank times if we start a new cycle of independnet i/o */
        if (!((ind_reads + ind_writes) % nprocs))
            cur_time = max_cur_time;

        if ((ind_reads == ind_read_cnt) && (ind_writes == ind_write_cnt))
            break;
    }

    return cur_time;
}

double generate_psx_close_event(struct darshan_file *file,
                                double cur_time)
{
    struct darshan_event next_event = { .rank = file->rank,
                                        .type = POSIX_CLOSE,
                                        .start_time = cur_time
                                      };

    next_event.event_params.close.file = file->hash;

    /* set the end time of the event based on time spent in POSIX meta operations */
    cur_time += file->fcounters[CP_F_POSIX_META_TIME] /
                (2 * file->counters[CP_POSIX_OPENS]);
    next_event.end_time = cur_time;

    /* store the close event */
    assert(file_event_list_cnt != file_event_list_max);
    file_event_list[file_event_list_cnt++] = next_event;

    return cur_time;
}

double generate_barrier_event(struct darshan_file *file,
                              int64_t root,
                              double cur_time)
{
    struct darshan_event next_event = { .rank = file->rank,
                                        .type = BARRIER,
                                        .start_time = cur_time,
                                        .end_time = cur_time
                                      };

    next_event.event_params.barrier.proc_count = nprocs;
    next_event.event_params.barrier.root = root;

    cur_time += .000001;
    next_event.end_time = cur_time;

    /* store the barrier event */
    assert(file_event_list_cnt != file_event_list_max);
    file_event_list[file_event_list_cnt++] = next_event;
    num_barriers++;

    return cur_time;
}

int event_comp(const void *p1, const void *p2)
{
    struct darshan_event *e1 = (struct darshan_event *)p1;
    struct darshan_event *e2 = (struct darshan_event *)p2;

    if (e1->start_time < e2->start_time)
    {
        return -1;
    }
    else if (e1->start_time > e2->start_time)
    {
        return 1;
    }
    else
    {
        return 0;
    }
}

/* TODO: we will probably want to benchmark this merge, and try to optimize it */
int merge_file_events(struct darshan_file *file)
{
    ssize_t bytes_written;
    int64_t file_list_ndx = 0;
    int64_t rank_list_ndx = 0;
    int64_t temp_list_ndx = 0;
    struct darshan_event *temp_list;
    static double last_close_time;

    /* if there are no file events, just return */
    if (!file_event_list_cnt)
        return 0;

    /* if the rank event list is empty, just copy this file's events over */
    if (!rank_event_list_cnt)
    {
        assert(file_event_list_cnt <= rank_event_list_max);
        memcpy(rank_event_list, file_event_list, file_event_list_cnt * sizeof(*file_event_list));
        rank_event_list_cnt = file_event_list_cnt;
        file_event_list_cnt = 0;
        last_close_time = file->fcounters[CP_F_CLOSE_TIMESTAMP];
    
        return 0;
    }

    /* merge this file's events with the events already gathered for this rank */
    temp_list = malloc((rank_event_list_cnt + file_event_list_cnt) * sizeof(*file_event_list));
    if (!temp_list)
    {
        fprintf(stderr, "Error: No memory to perform merge.\n");
        fflush(stderr);
        return -1;
    }

    /* if all rank events precede this file's open, just tack this file's events on the end */
    if (last_close_time < file->fcounters[CP_F_OPEN_TIMESTAMP])
    {
        memcpy(temp_list, rank_event_list, rank_event_list_cnt * sizeof(*temp_list));
        temp_list_ndx += rank_event_list_cnt;
        rank_list_ndx = rank_event_list_cnt;
        memcpy(&(temp_list[temp_list_ndx]), &(file_event_list[file_list_ndx]),
               (file_event_list_cnt - file_list_ndx) * sizeof(*temp_list));
        temp_list_ndx += file_event_list_cnt;
        file_list_ndx = file_event_list_cnt;
    }
    /* else we need to consider event timestamps to merge the event lists */
    else
    {
        while ((file_list_ndx < file_event_list_cnt) || (rank_list_ndx < rank_event_list_cnt))
        {
            /* if both lists have events to merge, merge based on start timestamp */
            if ((file_list_ndx < file_event_list_cnt) && (rank_list_ndx < rank_event_list_cnt))
            {
                if (rank_event_list[rank_list_ndx].start_time <
                    file_event_list[file_list_ndx].start_time)
                {
                    temp_list[temp_list_ndx++] = rank_event_list[rank_list_ndx++];
                }
                else
                {
                    temp_list[temp_list_ndx++] = file_event_list[file_list_ndx++];
                }
            }
            /* if only file event list has events, copy the rest over */
            else if (file_list_ndx < file_event_list_cnt)
            {
                memcpy(&(temp_list[temp_list_ndx]), &(file_event_list[file_list_ndx]),
                       (file_event_list_cnt - file_list_ndx) * sizeof(*temp_list));
                temp_list_ndx += (file_event_list_cnt - file_list_ndx);
                file_list_ndx = file_event_list_cnt;
            }
            /* if only rank event list has events, copy the rest over */
            else
            {
                memcpy(&(temp_list[temp_list_ndx]), &(rank_event_list[rank_list_ndx]),
                       (rank_event_list_cnt - rank_list_ndx) * sizeof(*temp_list));
                temp_list_ndx += (rank_event_list_cnt - rank_list_ndx);
                rank_list_ndx = rank_event_list_cnt;
            }
        }
    }

    if (file->fcounters[CP_F_CLOSE_TIMESTAMP] > last_close_time) 
    {
        last_close_time = file->fcounters[CP_F_CLOSE_TIMESTAMP];
    }

    /* copy the temp list to the complete event list for this rank */
    assert(temp_list_ndx < rank_event_list_max);
    memcpy(rank_event_list, temp_list, temp_list_ndx * sizeof(*temp_list));
    rank_event_list_cnt = temp_list_ndx;
    file_event_list_cnt = 0;
    free(temp_list);

    return 0;
}

/* TODO: eventually we should probably be compressing the output */
int store_rank_events(int event_file_fd,
                      int64_t rank)
{
    ssize_t bytes_written;

    if (rank > -1)
    {
        bytes_written = pwrite(event_file_fd,
                               rank_event_list,
                               rank_event_list_cnt * sizeof(struct darshan_event),
                               (off_t)&header_buf[rank + 1]);
    }
    else
    {
        bytes_written = pwrite(event_file_fd,
                               rank_event_list,
                               rank_event_list_cnt * sizeof(struct darshan_event),
                               (off_t)&header_buf[nprocs + 1]);
    }

    if (bytes_written != (rank_event_list_cnt * sizeof(struct darshan_event)))
    {
        return -1;
    }

    print_events(rank_event_list, rank_event_list_cnt);
    rank_event_list_cnt = 0;
    
    return 0;
}

int print_events(struct darshan_event *event_list,
                 int64_t event_list_cnt)
{
    int64_t i;
    for (i = 0; i < event_list_cnt; i++)
    {
        if (event_list[i].type == POSIX_OPEN)
        {
            if (event_list[i].event_params.open.create_flag == 0)
            {
                printf("Rank %"PRId64" OPEN %"PRIu64" (%lf - %lf)\n",
                       event_list[i].rank,
                       event_list[i].event_params.open.file,
                       event_list[i].start_time,
                       event_list[i].end_time);
            }
            else
            {
                printf("Rank %"PRId64" CREATE %"PRIu64" (%lf - %lf)\n",
                       event_list[i].rank,
                       event_list[i].event_params.open.file,
                       event_list[i].start_time,
                       event_list[i].end_time);
            }
        }
        else if (event_list[i].type == POSIX_CLOSE)
        {
            printf("Rank %"PRId64" CLOSE %"PRIu64" (%lf - %lf)\n",
                   event_list[i].rank,
                   event_list[i].event_params.open.file,
                   event_list[i].start_time,
                   event_list[i].end_time);
        }
        else if (event_list[i].type == POSIX_READ)
        {
            printf("Rank %"PRId64" READ %"PRIu64" [sz = %"PRId64", off = %"PRId64"] (%lf - %lf)\n",
                   event_list[i].rank,
                   event_list[i].event_params.read.file,
                   (int64_t)event_list[i].event_params.read.size,
                   (int64_t)event_list[i].event_params.read.offset,
                   event_list[i].start_time,
                   event_list[i].end_time);
        }
        else if (event_list[i].type == POSIX_WRITE)
        {
            printf("Rank %"PRId64" WRITE %"PRIu64" [sz = %"PRId64", off = %"PRId64"] (%lf - %lf)\n",
                   event_list[i].rank,
                   event_list[i].event_params.write.file,
                   (int64_t)event_list[i].event_params.write.size,
                   (int64_t)event_list[i].event_params.write.offset,
                   event_list[i].start_time,
                   event_list[i].end_time);
        }
        else if (event_list[i].type == BARRIER)
        {
            printf("**BARRIER** [nprocs = %"PRId64", root = %"PRId64"] (%lf - %lf)\n",
                   event_list[i].event_params.barrier.proc_count,
                   event_list[i].event_params.barrier.root,
                   event_list[i].start_time,
                   event_list[i].end_time);
        }
    }
//    printf("num_opens = %"PRId64"\nnum_reads = %"PRId64"\nnum_writes = %"PRId64"\nnum_barriers = %"PRId64"\n", num_opens, num_reads, num_writes, num_barriers);

    num_opens = num_reads = num_writes = num_barriers = 0;
    printf("\n*****\n*****\n\n");

    return 0;
}
