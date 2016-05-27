/*
 * (C) 1995-2001 Clemson University and Argonne National Laboratory.
 *
 * See COPYING in top-level directory.
 */


#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <sys/time.h>
#include <mpi.h>
#include <errno.h>
#include <getopt.h>

/* DEFAULT VALUES FOR OPTIONS */
static char    opt_file[256] = "test-fprintf-fscanf.out";

/* function prototypes */
static int parse_args(int argc, char **argv);
static void usage(void);

/* global vars */
static int mynod = 0;
static int nprocs = 1;

int main(int argc, char **argv)
{
   int namelen;
   char processor_name[MPI_MAX_PROCESSOR_NAME];
   FILE *file;
   char buffer[128] = {0};
   int number = 0;

   /* startup MPI and determine the rank of this process */
   MPI_Init(&argc,&argv);
   MPI_Comm_size(MPI_COMM_WORLD, &nprocs);
   MPI_Comm_rank(MPI_COMM_WORLD, &mynod);
   MPI_Get_processor_name(processor_name, &namelen); 
   
   /* parse the command line arguments */
   parse_args(argc, argv);

   if (mynod == 0) printf("# Using stdio calls.\n");

   file = fopen(opt_file, "w+");
   if(!file)
   {
      perror("fopen");
      return(-1);
   }

   if(mynod == 0)
   {
      fprintf(file, "a number: %d", 12345);
      fseek(file, 0, SEEK_SET);
      fscanf(file, "a number: %d", &number);
   }

   fclose(file);

   MPI_Finalize();
   return(0);
}

static int parse_args(int argc, char **argv)
{
   int c;
   
   while ((c = getopt(argc, argv, "f:")) != EOF) {
      switch (c) {
         case 'f': /* filename */
            strncpy(opt_file, optarg, 255);
            break;
         case '?': /* unknown */
            if (mynod == 0)
                usage();
            exit(1);
         default:
            break;
      }
   }
   return(0);
}

static void usage(void)
{
    printf("Usage: stdio-test [<OPTIONS>...]\n");
    printf("\n<OPTIONS> is one of\n");
    printf(" -f       filename [default: /foo/test.out]\n");
    printf(" -h       print this help\n");
}

/*
 * Local variables:
 *  c-indent-level: 3
 *  c-basic-offset: 3
 *  tab-width: 3
 *
 * vim: ts=3
 * End:
 */ 


