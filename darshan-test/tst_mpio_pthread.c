/*
 *  (C) 2025 by Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>    /* strcpy(), strncpy() */
#include <unistd.h>    /* _POSIX_BARRIERS */
#include <sys/types.h> /* open() */
#include <sys/stat.h>  /* open() */
#include <fcntl.h>     /* open() */
#include <errno.h>     /* errno */

#include <pthread.h>

#include <mpi.h>

#define NTHREADS 3
#define LEN 100

#define ERR \
    if (err != MPI_SUCCESS) { \
        int errorStringLen; \
        char errorString[MPI_MAX_ERROR_STRING]; \
        MPI_Error_string(err, errorString, &errorStringLen); \
        printf("Error at line %d: %s\n",__LINE__, errorString); \
        nerrs++; \
    }


#if !defined(_POSIX_BARRIERS) || _POSIX_BARRIERS <= 0
/* According to opengroup.org, barriers are defined in the optional part of
 * POSIX standard. For example, Mac OSX does not have pthread_barrier. If
 * barriers were implemented, the _POSIX_BARRIERS macro is defined as a
 * positive number.
 */

typedef int pthread_barrierattr_t;
typedef struct {
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    int count;
    int numThreads;
} pthread_barrier_t;

static int pthread_barrier_init(pthread_barrier_t           *barrier,
                                const pthread_barrierattr_t *attr,
                                unsigned int                 count)
{
    if (count == 0) {
        errno = EINVAL;
        return -1;
    }

    if (pthread_mutex_init(&barrier->mutex, 0) < 0)
        return -1;

    if (pthread_cond_init(&barrier->cond, 0) < 0) {
        pthread_mutex_destroy(&barrier->mutex);
        return -1;
    }
    barrier->numThreads = count;
    barrier->count = 0;

    return 0;
}

static int pthread_barrier_destroy(pthread_barrier_t *barrier)
{
    pthread_cond_destroy(&barrier->cond);
    pthread_mutex_destroy(&barrier->mutex);
    return 0;
}

static int pthread_barrier_wait(pthread_barrier_t *barrier)
{
    int ret;
    pthread_mutex_lock(&barrier->mutex);
    ++(barrier->count);
    if (barrier->count >= barrier->numThreads) {
        barrier->count = 0;
        pthread_cond_broadcast(&barrier->cond);
        ret = 1;
    } else {
        pthread_cond_wait(&barrier->cond, &barrier->mutex);
        ret = 0;
    }
    pthread_mutex_unlock(&barrier->mutex);
    return ret;
}
#endif

/* pthread barrier object */
static pthread_barrier_t barr;

typedef struct {
    int       id;         /* globally unique thread ID */
    MPI_File  fh;         /* file handler */
    int       nprocs;     /* number of MPI processes */
    int       rank;       /* MPI rank ID */
    size_t    count;      /* write length */
    char      fname[256]; /* output file name base */
} thread_arg;

pthread_mutex_t env_mutex = PTHREAD_MUTEX_INITIALIZER;

static int setenv_thread_safe(const char *name, const char *value, int overwrite) {
    int err;
    pthread_mutex_lock(&env_mutex);
    err = setenv(name, value, overwrite);
    pthread_mutex_unlock(&env_mutex);
    return err;
}

/*----< thread_func() >------------------------------------------------------*/
static
void* thread_func(void *arg)
{
    char filename[512];
    int i, id, err, nerrs=0, nprocs, rank, *ret;
    size_t count;
    off_t off;
    char buf[LEN], annotation[64];
    MPI_File fh;
    MPI_Status status;

    /* make a unique file name for each thread */
    id = ((thread_arg*)arg)->id;
    fh = ((thread_arg*)arg)->fh;
    count = ((thread_arg*)arg)->count;
    nprocs = ((thread_arg*)arg)->nprocs;
    rank = ((thread_arg*)arg)->rank;

    for (i=0; i<LEN; i++) buf[i] = rank;

    /* Note Darshan will randomly pick annotation from one of the thread only */
    snprintf(annotation, 64, "annotation of rank %d thread %d", rank, id);
    // err = setenv_thread_safe("DARSHAN_DXT_EXTRA_INFO", annotation, 0);
    err = setenv("DARSHAN_DXT_EXTRA_INFO", annotation, 0);
    if (err == -1)
        printf("Error: rank %s thread %d failed to call setenv (%s)\n",
        rank, id, strerror(errno));

    off = rank * NTHREADS * LEN + id * LEN;

    err = MPI_File_read_at_all(fh, off, buf, count, MPI_BYTE, &status);
    ERR

    off += nprocs * NTHREADS * LEN;

    err = MPI_File_write_at_all(fh, off, buf, count, MPI_BYTE, &status);
    ERR

    pthread_t pid = pthread_self();
    printf("Thread %d has pthread_self() returned ID %lu\n", id, pid);

    /* return number of errors encountered */
    ret = (int*)malloc(sizeof(int));
    *ret = nerrs;

    return ret; /* same as pthread_exit(ret); */
}

/*----< main() >-------------------------------------------------------------*/
int main(int argc, char **argv) {
    extern int optind;
    char filename[256];
    int  i, err, nerrs=0, rank=0, nprocs, providedT;
    MPI_File fh;
    MPI_Status status;
    pthread_t threads[NTHREADS];

    MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &providedT);

    MPI_Comm_size(MPI_COMM_WORLD, &nprocs);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    if (providedT != MPI_THREAD_MULTIPLE) {
        if (!rank)
            printf("Error: MPI does not support MPI_THREAD_MULTIPLE\n");
        MPI_Finalize();
        return 0;
    }

    if (argc == 1) strcpy(filename, "testfile");
    else           strcpy(filename, argv[1]);

    /* create a file */
    err = MPI_File_open(MPI_COMM_SELF, filename, MPI_MODE_CREATE | MPI_MODE_RDWR,
                        MPI_INFO_NULL, &fh);
    ERR

    char buf[LEN*NTHREADS];
    size_t count = LEN * NTHREADS;
    MPI_Offset off = rank * count;
    for (i=0; i<count; i++) buf[i] = '0'+rank;
    err = MPI_File_write_at_all(fh, off, buf, count, MPI_BYTE, &status);
    ERR

    /* initialize thread barrier */
    pthread_barrier_init(&barr, NULL, NTHREADS);

    /* create threads, each calls thread_func() */
    for (i=0; i<NTHREADS; i++) {
        thread_arg t_arg[NTHREADS]; /* must be unique to each thread */
        t_arg[i].id = i + rank * NTHREADS;
        t_arg[i].fh = fh;
        t_arg[i].nprocs = nprocs;
        t_arg[i].rank = rank;
        t_arg[i].count = LEN;
        sprintf(t_arg[i].fname, "%s",filename);
        if (pthread_create(&threads[i], NULL, thread_func, &t_arg[i])) {
            fprintf(stderr, "Error in %s line %d creating thread %d\n",
                    __FILE__, __LINE__, i);
            nerrs++;
        }
        else
            printf("Success create pthread %d with ID %lu\n",i, threads[i]);
    }

    /* wait for all threads to finish */
    for (i=0; i<NTHREADS; i++) {
        void *ret;
        if (pthread_join(threads[i], (void**)&ret)) {
            fprintf(stderr, "Error in %s line %d joining thread %d\n",
                    __FILE__, __LINE__, i);
        }
        nerrs += *(int*)ret;
        free(ret);
    }

    pthread_barrier_destroy(&barr);

    err = MPI_File_close(&fh); ERR

    MPI_Finalize();

    return (nerrs > 0);
}

