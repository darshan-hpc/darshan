#ifdef HAVE_CONFIG_H
#include <darshan-runtime-config.h> /* output of 'configure' */
#endif

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h> /* unlink() */
#include <mpi.h>

#define CHECK_ERROR(fnc) { \
    if (rank == 0 && verbose && strcmp(fnc, "MPI_File_open") \
                             && strcmp(fnc, "MPI_File_close") \
                             && strcmp(fnc, "MPI_File_seek") \
                             && strcmp(fnc, "MPI_Waitall") \
                             && strcmp(fnc, "MPI_File_write_all_end") \
                             && strcmp(fnc, "MPI_File_write_at_all_end") \
                             && strcmp(fnc, "MPI_File_write_ordered_end") \
                             && strcmp(fnc, "MPI_File_read_all_end") \
                             && strcmp(fnc, "MPI_File_read_at_all_end") \
                             && strcmp(fnc, "MPI_File_read_ordered_end")) \
        printf("---- testing %s\n",fnc); \
    if (err != MPI_SUCCESS) { \
        int errorStringLen; \
        char errorString[MPI_MAX_ERROR_STRING]; \
        MPI_Error_string(err, errorString, &errorStringLen); \
        printf("Error at line %d when calling %s: %s\n",__LINE__,fnc,errorString); \
    } \
}

static void
usage(char *argv0)
{
    char *help =
    "Usage: %s [OPTIONS]\n"
    "       [-h] print this help\n"
    "       [-q] quiet mode\n"
    "       [-i] test read API\n"
    "       [-c] test collective API\n"
    "       [-a] test asynchonous API\n"
    "       [-s] test shared API\n"
    "       [-p] test split API\n"
    "       [-o] test ordered API\n"
    "       [-x] test explicit offset API\n"
    "       [-l] test large-count API\n";
    fprintf(stderr, help, argv0);
}

#define NELEMS 8

/*----< main() >------------------------------------------------------------*/
int main(int argc, char **argv)
{
    extern int optind;
    extern char *optarg;
    char filename[512], buf[NELEMS];
    int i, err, rank, np, verbose, omode;
    int test_read, test_collective, test_async, test_shared;
    int test_split, test_order, test_at, test_large_count;
    MPI_Offset offset;
    MPI_Count nbytes;
    MPI_File fh;
    MPI_Request req;
    MPI_Status status;

    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &np);

    verbose          = 1;
    test_read        = 0;
    test_collective  = 0;
    test_async       = 0;
    test_shared      = 0;
    test_split       = 0;
    test_order       = 0;
    test_at          = 0;
    test_large_count = 0;

    while ((i = getopt(argc, argv, "hqdicaspoxl")) != EOF)
        switch(i) {
            case 'q': verbose = 0;
                      break;
            case 'i': test_read = 1;
                      break;
            case 'c': test_collective = 1;
                      break;
            case 'a': test_async = 1;
                      break;
            case 's': test_shared = 1;
                      break;
            case 'p': test_split = 1;
                      break;
            case 'o': test_order = 1;
                      break;
            case 'x': test_at = 1;
                      break;
            case 'l': test_large_count = 1;
                      break;
            case 'h':
            default:  if (rank==0) usage(argv[0]);
                      MPI_Finalize();
                      return 1;
        }

#ifndef HAVE_MPI_LARGE_COUNT
    if (test_large_count == 1) {
        printf("The underlying MPI-IO does not support large-count feature... skip\n");
        MPI_Finalize();
        return 0;
    }
#endif

    if (argv[optind] == NULL) strcpy(filename, "testfile.dat");
    else                      snprintf(filename, 512, "%s", argv[optind]);

    offset = rank * NELEMS;
    nbytes = NELEMS;
    for (i=0; i<NELEMS; i++)
        buf[i] = 'a'+rank+i;

    omode = (test_read) ? MPI_MODE_RDONLY : MPI_MODE_CREATE | MPI_MODE_RDWR;

    err = MPI_File_open(MPI_COMM_WORLD, filename, omode, MPI_INFO_NULL, &fh);
    CHECK_ERROR("MPI_File_open")

    if (test_read) { /* test read APIs */
        if (test_async) {
            if (test_shared) {
#ifdef HAVE_MPI_LARGE_COUNT
                if (test_large_count) {
                    err = MPI_File_iread_shared_c(fh, buf, nbytes, MPI_BYTE, &req);
                    CHECK_ERROR("MPI_File_iread_shared_c")
                } else
#endif
                {
                    err = MPI_File_iread_shared(fh, buf, nbytes, MPI_BYTE, &req);
                    CHECK_ERROR("MPI_File_iread_shared")
                }
            }
            else if (test_at) {
                if (test_collective) {
#ifdef HAVE_MPI_LARGE_COUNT
                    if (test_large_count) {
                        err = MPI_File_iread_at_all_c(fh, offset, buf, nbytes, MPI_BYTE, &req);
                        CHECK_ERROR("MPI_File_iread_at_all_c")
                    } else
#endif
                    {
                        err = MPI_File_iread_at_all(fh, offset, buf, nbytes, MPI_BYTE, &req);
                        CHECK_ERROR("MPI_File_iread_at_all")
                    }
                }
                else {
#ifdef HAVE_MPI_LARGE_COUNT
                    if (test_large_count) {
                        err = MPI_File_iread_at_c(fh, offset, buf, nbytes, MPI_BYTE, &req);
                        CHECK_ERROR("MPI_File_iread_at_c")
                    } else
#endif
                    {
                        err = MPI_File_iread_at(fh, offset, buf, nbytes, MPI_BYTE, &req);
                        CHECK_ERROR("MPI_File_iread_at")
                    }
                }
            }
            else {
                err = MPI_File_seek(fh, offset, MPI_SEEK_SET);
                CHECK_ERROR("MPI_File_seek")

                if (test_collective) {
#ifdef HAVE_MPI_LARGE_COUNT
                    if (test_large_count) {
                        err = MPI_File_iread_all_c(fh, buf, nbytes, MPI_BYTE, &req);
                        CHECK_ERROR("MPI_File_iread_all_c")
                    } else
#endif
                    {
                        err = MPI_File_iread_all(fh, buf, nbytes, MPI_BYTE, &req);
                        CHECK_ERROR("MPI_File_iread_all")
                    }
                }
                else {
#ifdef HAVE_MPI_LARGE_COUNT
                    if (test_large_count) {
                        err = MPI_File_iread_c(fh, buf, nbytes, MPI_BYTE, &req);
                        CHECK_ERROR("MPI_File_iread_c")
                    } else
#endif
                    {
                        err = MPI_File_iread(fh, buf, nbytes, MPI_BYTE, &req);
                        CHECK_ERROR("MPI_File_iread")
                    }
                }
            }
            err = MPI_Waitall(1, &req, &status);
            CHECK_ERROR("MPI_Waitall")
        }
        else if (test_split) {
            if (test_order) {
#ifdef HAVE_MPI_LARGE_COUNT
                if (test_large_count) {
                    err =  MPI_File_read_ordered_begin_c(fh, buf, nbytes, MPI_BYTE);
                    CHECK_ERROR("MPI_File_read_ordered_begin_c")
                } else
#endif
                {
                    err =  MPI_File_read_ordered_begin(fh, buf, nbytes, MPI_BYTE);
                    CHECK_ERROR("MPI_File_read_ordered_begin")
                }
                err =  MPI_File_read_ordered_end(fh, buf, &status);
                CHECK_ERROR("MPI_File_read_ordered_end")
            }
            else {
                if (test_at) {
#ifdef HAVE_MPI_LARGE_COUNT
                    if (test_large_count) {
                        err =  MPI_File_read_at_all_begin_c(fh, offset, buf, nbytes, MPI_BYTE);
                        CHECK_ERROR("MPI_File_read_at_all_begin_c")
                    } else
#endif
                    {
                        err =  MPI_File_read_at_all_begin(fh, offset, buf, nbytes, MPI_BYTE);
                        CHECK_ERROR("MPI_File_read_at_all_begin")
                    }
                    err =  MPI_File_read_at_all_end(fh, buf, &status);
                    CHECK_ERROR("MPI_File_read_at_all_end")
                }
                else {
                    err = MPI_File_seek(fh, offset, MPI_SEEK_SET);
                    CHECK_ERROR("MPI_File_seek")

#ifdef HAVE_MPI_LARGE_COUNT
                    if (test_large_count) {
                        err =  MPI_File_read_all_begin_c(fh, buf, nbytes, MPI_BYTE);
                        CHECK_ERROR("MPI_File_read_all_begin_c")
                    } else
#endif
                    {
                        err =  MPI_File_read_all_begin(fh, buf, nbytes, MPI_BYTE);
                        CHECK_ERROR("MPI_File_read_at_all_begin")
                    }
                    err =  MPI_File_read_all_end(fh, buf, &status);
                    CHECK_ERROR("MPI_File_read_all_end")
                }
            }
        }
        else if (test_shared) {
#ifdef HAVE_MPI_LARGE_COUNT
            if (test_large_count) {
                err = MPI_File_read_shared_c(fh, buf, nbytes, MPI_BYTE, &status);
                CHECK_ERROR("MPI_File_read_shared_c")
            } else
#endif
            {
                err = MPI_File_read_shared(fh, buf, nbytes, MPI_BYTE, &status);
                CHECK_ERROR("MPI_File_read_shared")
            }
        }
        else if (test_order) {
#ifdef HAVE_MPI_LARGE_COUNT
            if (test_large_count) {
                err = MPI_File_read_ordered_c(fh, buf, nbytes, MPI_BYTE, &status);
                CHECK_ERROR("MPI_File_read_ordered_c")
            } else
#endif
            {
                err = MPI_File_read_ordered(fh, buf, nbytes, MPI_BYTE, &status);
                CHECK_ERROR("MPI_File_read_ordered")
            }
        }
        else if (test_at) {
            if (test_collective) {
#ifdef HAVE_MPI_LARGE_COUNT
                if (test_large_count) {
                    err = MPI_File_read_at_all_c(fh, offset, buf, nbytes, MPI_BYTE, &status);
                    CHECK_ERROR("MPI_File_read_at_all_c")
                } else
#endif
                {
                    err = MPI_File_read_at_all(fh, offset, buf, nbytes, MPI_BYTE, &status);
                    CHECK_ERROR("MPI_File_read_at_all")
                }
            }
            else {
#ifdef HAVE_MPI_LARGE_COUNT
                if (test_large_count) {
                    err = MPI_File_read_at_c(fh, offset, buf, nbytes, MPI_BYTE, &status);
                    CHECK_ERROR("MPI_File_read_at_c")
                } else
#endif
                {
                    err = MPI_File_read_at(fh, offset, buf, nbytes, MPI_BYTE, &status);
                    CHECK_ERROR("MPI_File_read_at")
                }
            }
        }
        else {
            err = MPI_File_seek(fh, offset, MPI_SEEK_SET);
            CHECK_ERROR("MPI_File_seek")

            if (test_collective) {
#ifdef HAVE_MPI_LARGE_COUNT
                if (test_large_count) {
                    err = MPI_File_read_all_c(fh, buf, nbytes, MPI_BYTE, &status);
                    CHECK_ERROR("MPI_File_read_all_c")
                } else
#endif
                {
                    err = MPI_File_read_all(fh, buf, nbytes, MPI_BYTE, &status);
                    CHECK_ERROR("MPI_File_read_all")
                }
            }
            else {
#ifdef HAVE_MPI_LARGE_COUNT
                if (test_large_count) {
                    err = MPI_File_read_c(fh, buf, nbytes, MPI_BYTE, &status);
                    CHECK_ERROR("MPI_File_read_c")
                } else
#endif
                {
                    err = MPI_File_read(fh, buf, nbytes, MPI_BYTE, &status);
                    CHECK_ERROR("MPI_File_read")
                }
            }
        }
    }
    else { /* test write APIs */
        if (test_async) {
            if (test_shared) {
#ifdef HAVE_MPI_LARGE_COUNT
                if (test_large_count) {
                    err = MPI_File_iwrite_shared_c(fh, buf, nbytes, MPI_BYTE, &req);
                    CHECK_ERROR("MPI_File_iwrite_shared_c")
                } else
#endif
                {
                    err = MPI_File_iwrite_shared(fh, buf, nbytes, MPI_BYTE, &req);
                    CHECK_ERROR("MPI_File_iwrite_shared")
                }
            }
            else if (test_at) {
                if (test_collective) {
#ifdef HAVE_MPI_LARGE_COUNT
                    if (test_large_count) {
                        err = MPI_File_iwrite_at_all_c(fh, offset, buf, nbytes, MPI_BYTE, &req);
                        CHECK_ERROR("MPI_File_iwrite_at_all_c")
                    } else
#endif
                    {
                        err = MPI_File_iwrite_at_all(fh, offset, buf, nbytes, MPI_BYTE, &req);
                        CHECK_ERROR("MPI_File_iwrite_at_all")
                    }
                }
                else {
#ifdef HAVE_MPI_LARGE_COUNT
                    if (test_large_count) {
                        err = MPI_File_iwrite_at_c(fh, offset, buf, nbytes, MPI_BYTE, &req);
                        CHECK_ERROR("MPI_File_iwrite_at_c")
                    } else
#endif
                    {
                        err = MPI_File_iwrite_at(fh, offset, buf, nbytes, MPI_BYTE, &req);
                        CHECK_ERROR("MPI_File_iwrite_at")
                    }
                }
            }
            else {
                err = MPI_File_seek(fh, offset, MPI_SEEK_SET);
                CHECK_ERROR("MPI_File_seek")

                if (test_collective) {
#ifdef HAVE_MPI_LARGE_COUNT
                    if (test_large_count) {
                        err = MPI_File_iwrite_all_c(fh, buf, nbytes, MPI_BYTE, &req);
                        CHECK_ERROR("MPI_File_iwrite_all_c")
                    } else
#endif
                    {
                        err = MPI_File_iwrite_all(fh, buf, nbytes, MPI_BYTE, &req);
                        CHECK_ERROR("MPI_File_iwrite_all")
                    }
                }
                else {
#ifdef HAVE_MPI_LARGE_COUNT
                    if (test_large_count) {
                        err = MPI_File_iwrite_c(fh, buf, nbytes, MPI_BYTE, &req);
                        CHECK_ERROR("MPI_File_iwrite_c")
                    } else
#endif
                    {
                        err = MPI_File_iwrite(fh, buf, nbytes, MPI_BYTE, &req);
                        CHECK_ERROR("MPI_File_iwrite")
                    }
                }
            }
            err = MPI_Waitall(1, &req, &status);
            CHECK_ERROR("MPI_Waitall")
        }
        else if (test_split) {
            if (test_order) {
#ifdef HAVE_MPI_LARGE_COUNT
                if (test_large_count) {
                    err =  MPI_File_write_ordered_begin_c(fh, buf, nbytes, MPI_BYTE);
                    CHECK_ERROR("MPI_File_write_ordered_begin_c")
                } else
#endif
                {
                    err =  MPI_File_write_ordered_begin(fh, buf, nbytes, MPI_BYTE);
                    CHECK_ERROR("MPI_File_write_ordered_begin")
                }
                err =  MPI_File_write_ordered_end(fh, buf, &status);
                CHECK_ERROR("MPI_File_write_ordered_end")
            }
            else {
                if (test_at) {
#ifdef HAVE_MPI_LARGE_COUNT
                    if (test_large_count) {
                        err =  MPI_File_write_at_all_begin_c(fh, offset, buf, nbytes, MPI_BYTE);
                        CHECK_ERROR("MPI_File_write_at_all_begin_c")
                    } else
#endif
                    {
                        err =  MPI_File_write_at_all_begin(fh, offset, buf, nbytes, MPI_BYTE);
                        CHECK_ERROR("MPI_File_write_at_all_begin")
                    }
                    err =  MPI_File_write_at_all_end(fh, buf, &status);
                    CHECK_ERROR("MPI_File_write_at_all_end")
                }
                else {
                    err = MPI_File_seek(fh, offset, MPI_SEEK_SET);
                    CHECK_ERROR("MPI_File_seek")

#ifdef HAVE_MPI_LARGE_COUNT
                    if (test_large_count) {
                        err =  MPI_File_write_all_begin_c(fh, buf, nbytes, MPI_BYTE);
                        CHECK_ERROR("MPI_File_write_all_begin_c")
                    } else
#endif
                    {
                        err =  MPI_File_write_all_begin(fh, buf, nbytes, MPI_BYTE);
                        CHECK_ERROR("MPI_File_write_all_begin")
                    }
                    err =  MPI_File_write_all_end(fh, buf, &status);
                    CHECK_ERROR("MPI_File_write_all_end")
                }
            }
        }
        else if (test_shared) {
#ifdef HAVE_MPI_LARGE_COUNT
            if (test_large_count) {
                err = MPI_File_write_shared_c(fh, buf, nbytes, MPI_BYTE, &status);
                CHECK_ERROR("MPI_File_write_shared_c")
            } else
#endif
            {
                err = MPI_File_write_shared(fh, buf, nbytes, MPI_BYTE, &status);
                CHECK_ERROR("MPI_File_write_shared")
            }
        }
        else if (test_order) {
#ifdef HAVE_MPI_LARGE_COUNT
            if (test_large_count) {
                err = MPI_File_write_ordered_c(fh, buf, nbytes, MPI_BYTE, &status);
                CHECK_ERROR("MPI_File_write_ordered_c")
            } else
#endif
            {
                err = MPI_File_write_ordered(fh, buf, nbytes, MPI_BYTE, &status);
                CHECK_ERROR("MPI_File_write_ordered")
            }
        }
        else if (test_at) {
            if (test_collective) {
#ifdef HAVE_MPI_LARGE_COUNT
                if (test_large_count) {
                    err = MPI_File_write_at_all_c(fh, offset, buf, nbytes, MPI_BYTE, &status);
                    CHECK_ERROR("MPI_File_write_at_all_c")
                } else
#endif
                {
                    err = MPI_File_write_at_all(fh, offset, buf, nbytes, MPI_BYTE, &status);
                    CHECK_ERROR("MPI_File_write_at_all")
                }
            }
            else {
#ifdef HAVE_MPI_LARGE_COUNT
                if (test_large_count) {
                    err = MPI_File_write_at_c(fh, offset, buf, nbytes, MPI_BYTE, &status);
                    CHECK_ERROR("MPI_File_write_at_c")
                } else
#endif
                {
                    err = MPI_File_write_at(fh, offset, buf, nbytes, MPI_BYTE, &status);
                    CHECK_ERROR("MPI_File_write_at")
                }
            }
        }
        else {
            err = MPI_File_seek(fh, offset, MPI_SEEK_SET);
            CHECK_ERROR("MPI_File_seek")

            if (test_collective) {
#ifdef HAVE_MPI_LARGE_COUNT
                if (test_large_count) {
                    err = MPI_File_write_all_c(fh, buf, nbytes, MPI_BYTE, &status);
                    CHECK_ERROR("MPI_File_write_all_c")
                } else
#endif
                {
                    err = MPI_File_write_all(fh, buf, nbytes, MPI_BYTE, &status);
                    CHECK_ERROR("MPI_File_write_all")
                }
            }
            else {
#ifdef HAVE_MPI_LARGE_COUNT
                if (test_large_count) {
                    err = MPI_File_write_c(fh, buf, nbytes, MPI_BYTE, &status);
                    CHECK_ERROR("MPI_File_write_c")
                } else
#endif
                {
                    err = MPI_File_write(fh, buf, nbytes, MPI_BYTE, &status);
                    CHECK_ERROR("MPI_File_write")
                }
            }
        }
    }

    err = MPI_File_close(&fh);
    CHECK_ERROR("MPI_File_close")

    MPI_Finalize();
    return 0;
}


