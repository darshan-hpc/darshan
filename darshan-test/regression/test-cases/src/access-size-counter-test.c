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
#include <sys/stat.h>

/* DEFAULT VALUES FOR OPTIONS */
static char    opt_file[256] = "test.out";

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
   char buffer[128] = {0};
   int fd;
   int i = 1;
   int ret;

   /* startup MPI and determine the rank of this process */
   MPI_Init(&argc,&argv);
   MPI_Comm_size(MPI_COMM_WORLD, &nprocs);
   MPI_Comm_rank(MPI_COMM_WORLD, &mynod);
   MPI_Get_processor_name(processor_name, &namelen); 
   
   /* parse the command line arguments */
   parse_args(argc, argv);

   if(mynod == 0)
   {

      fd = open(opt_file, O_WRONLY|O_TRUNC|O_CREAT, S_IRUSR|S_IWUSR|S_IRGRP|S_IWGRP);
      if(fd<0)
      {
         perror("open");
         return(-1);
      }

      /* cycle through a range of unique write access sizes */
      for(i=1; i<128; i++)
      {
         ret = write(fd, buffer, i);
         if(ret < 0)
         {
            perror("write");
            return(-1);
         }
         if(ret != i)
         {
            fprintf(stderr, "Short write: %d\n", ret);
            return(-1);
         }
      }

      close(fd);

   }
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
    printf(" -f       filename [default: test.out]\n");
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


