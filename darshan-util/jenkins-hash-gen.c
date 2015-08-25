/*
 *  (C) 2011 by Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
 */

/*
 * Small utility to do Jenkins hashes
 */
#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>
#include <inttypes.h>
#include <getopt.h>
#include <string.h>

#define TYPE_32 (1)
#define TYPE_64 (2)

extern uint32_t darshan_hashlittle(const void *key, size_t length, uint32_t initval);
extern void hashlittle2(const void *key, size_t length, uint32_t *pc, uint32_t *pb);

void usage(char *progname)
{
    fprintf(stderr, "usage: %s [--32] [--64] [--key <key>] data\n", progname);
    exit(1);
}

void parse_args (int argc, char **argv, int* type, int* key, char **data)
{
    int index;
    static struct option long_opts[] =
    {
        {"32", 0, NULL, 'a'},
        {"64", 0, NULL, 'b'},
        {"key", 1, NULL, 'k'},
        {"help",  0, NULL, 0}
    };

    while(1)
    {
        int c = getopt_long(argc, argv, "", long_opts, &index);

        if (c == -1) break;

        switch(c)
        {
            case 'a':
                *type = TYPE_32;
                break;
            case 'b':
                *type = TYPE_64;
                break;
            case 'k':
                *key = atoi(optarg);
                break;
            case 0:
            case '?':
            default:
                usage(argv[0]);
                break;
        }
    }

    if (optind + 1 == argc)
    {
        *data = argv[optind];
    }
    else
    {
       usage(argv[0]);
    }

    return;
}

int main (int argc, char **argv)
{
    int type = TYPE_32;
    int key = 0;
    char *data;
    uint32_t hashed_u = 0;
    uint32_t hashed_l = 0;
    uint64_t hashed;

    parse_args (argc, argv, &type, &key, &data);

    switch(type)
    {
    case TYPE_64:
        hashed_u = key;
        hashlittle2(data, strlen(data), &hashed_u, &hashed_l);
        break;
    case TYPE_32:
    default: 
        hashed_l = darshan_hashlittle(data, strlen(data), key);
        break;
    }

    hashed = ((uint64_t)hashed_u << 32) | hashed_l;

    fprintf(stdout, "%" PRIu64 "\n", hashed);

    return 0;
}
