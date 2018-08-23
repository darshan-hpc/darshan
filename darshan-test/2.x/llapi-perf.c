/*
 *  (C) 2012 by Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
 */

/* llapi-perf.c
 * Time how long it takes to extract various file data from Lustre via
 * ioctl and llapi calls from every process.  -i uses ioctl, -a uses the
 * Lustre API.  This also retains the features of stat-perf.c, which
 * times how long it takes to issue a stat64() call to the designated file
 * from every process.  -f causes it to use fstat64() rather than stat64().  
 * -l causes it to use lseek(SEEK_END) instead of stat64().
 * -c causes it to create the file from scratch rather than operating on an
 *  existing file.  -r issues a realpath() call on the file.
 */

#define _LARGEFILE64_SOURCE

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <string.h>
#include <sys/time.h>
#include <mpi.h>
#include <errno.h>
#include <getopt.h>
#include <sys/ioctl.h>
#ifndef NO_LUSTRE
#include <lustre/lustreapi.h>
#endif

static char* opt_file = NULL;
static int opt_create = 0;
static int opt_fstat = 0;
static int opt_lseek = 0;
static int opt_realpath = 0;
static int opt_ioctl = 0;
static int opt_llapi = 0;
static int opt_llapi_fd = 0;
static int opt_fpp = 0;
static int rank = -1;

static int parse_args(int argc, char **argv);
static void usage(void);

int main(int argc, char **argv)
{
   int fd;
   int ret;
   double stime, etime, elapsed, slowest;
   struct stat64 statbuf;
   int nprocs;
   off64_t offset, orig_offset;
   char* new_path;

   MPI_Init(&argc,&argv);
   MPI_Comm_rank(MPI_COMM_WORLD, &rank);
   MPI_Comm_size(MPI_COMM_WORLD, &nprocs);
   
   /* parse the command line arguments */
   parse_args(argc, argv);

   MPI_Barrier(MPI_COMM_WORLD);

   /* open specified file */
   if(!opt_create)
   {
      fd = open(opt_file, O_RDWR);  
      if(fd < 0)
      {
         perror("open");
         exit(1);
      }
   }
   else
   {
      /* rank 0 create, everyone else open */
      if(rank == 0 || opt_fpp)
      {
         fd = open(opt_file, O_RDWR|O_CREAT|O_EXCL, S_IRUSR|S_IWUSR);
         if(fd < 0)
         {
            perror("open");
            exit(1);
         }
         MPI_Barrier(MPI_COMM_WORLD);
      }
      else
      {
         MPI_Barrier(MPI_COMM_WORLD);
         fd = open(opt_file, O_RDWR);  
         if(fd < 0)
         {
            perror("open");
            exit(1);
         }
      }
   }

   MPI_Barrier(MPI_COMM_WORLD);
   stime = MPI_Wtime();

   ret = 0;
   if(opt_fstat)
      ret = fstat64(fd, &statbuf);
   else if(opt_lseek)
   {
      /* find current position */
      orig_offset = lseek64(fd, 0, SEEK_CUR);
      if(orig_offset < 0)
         ret = -1;
      else
      {
         /* find end of file; this is the size */
         offset = lseek64(fd, 0, SEEK_END);
         if(offset < 0)
            ret = -1;
         else
         {
            /* go back to original position */
            offset = lseek64(fd, orig_offset, SEEK_SET);
            if(offset < 0)
                ret = -1;
         }
      }
   }
   else if(opt_realpath)
   {
      new_path = realpath(opt_file, NULL);
      if(!new_path)
        ret = -1;
      else
        free(new_path);
   }
   else if ( opt_llapi || opt_ioctl || opt_llapi_fd)
   {
#ifdef NO_LUSTRE
      fprintf(stderr, "Not compiled with Lustre support\n");
      ret = -1;
#else
      struct lov_user_md *lum;
      size_t lumsize = sizeof(struct lov_user_md) +
           LOV_MAX_STRIPE_COUNT * sizeof(struct lov_user_ost_data);

      lum = calloc(1, lumsize);
      if (lum == NULL) {
         ret = ENOMEM;
         fprintf(stderr, "No memory\n");
      }
      else {
        if ( opt_llapi ) 
        {
         ret = llapi_file_get_stripe(opt_file, lum);
        }
        else if ( opt_ioctl )
        {
				memset(lum, 0, lumsize);
            lum->lmm_magic = LOV_USER_MAGIC;
            lum->lmm_stripe_count = LOV_MAX_STRIPE_COUNT;
            ret = ioctl( fd, LL_IOC_LOV_GETSTRIPE, (void *)lum );
        }
		  else if (opt_llapi_fd) {
				struct llapi_layout *layout;
				layout = llapi_layout_get_by_fd(fd, 0);
				llapi_layout_stripe_count_get(layout, &(lum->lmm_stripe_count));
				llapi_layout_stripe_size_get(layout, &(lum->lmm_stripe_size));
				llapi_layout_ost_index_get(layout,  0, &(lum->lmm_stripe_offset));
		  }
#ifdef DEBUG
        /* different API/ioctl calls populate only parts of lum */
        printf( "stripe_width=%d stripe_size=%d starting_ost=%d\n",
             lum->lmm_stripe_count,
             lum->lmm_stripe_size,
             lum->lmm_stripe_count );
#endif
        }
#endif
   }
   else
      ret = stat64(opt_file, &statbuf);

   if(ret != 0 && !opt_ioctl && !opt_llapi)
   {
      perror("stat64 or fstat64");
      exit(1);
   }
#ifndef NO_LUSTRE
   else if ( ret < 0 && opt_ioctl )
   {
      perror("ioctl");
      exit(1);
   }
#endif
   
   etime = MPI_Wtime();

   elapsed = etime-stime;
   ret = MPI_Reduce(&elapsed, &slowest, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
   if(ret != 0)
   {
      fprintf(stderr, "Error: MPI_Reduce() failure.\n");
      exit(1);
   }


   slowest *= 1000.0;

   if(rank == 0)
   {
      printf("opt_file: %s, opt_create: %d, opt_fstat: %d, opt_lseek: %d, opt_realpath: %d, opt_llapi: %d, opt_ioctl: %d, opt_fpp: %d, opt_llapi_fd: %d, nprocs: %d, time: %f ms\n",
        opt_file,
        opt_create,
        opt_fstat,
        opt_lseek,
        opt_realpath,
        opt_llapi,
        opt_ioctl,
        opt_fpp,
		  opt_llapi_fd,
        nprocs,
        slowest);
   }

   MPI_Finalize();
   return(0);
}

static int parse_args(int argc, char **argv)
{
   int c;
   
   while ((c = getopt(argc, argv, "fclripaA")) != EOF) {
      switch (c) {
         case 'c': /* create file */
            opt_create = 1;
            break;
         case 'f': /* fstat instead of stat */
            opt_fstat = 1;
            break;
         case 'l': /* lseek instead of stat */
            opt_lseek = 1;
            break;
         case 'r': /* realpath instead of stat */
            opt_realpath = 1;
            break;
         case 'i': /* use ioctl test */
            opt_ioctl = 1;
            break;
         case 'a': /* use llapi test*/
            opt_llapi = 1;
            break;
			case 'A': /* use llapi test with fd */
				opt_llapi_fd = 1;
				break;
         case 'p': /* file per process instead of shared file */
            opt_fpp = 1;
            break;
         case 'h':
            if (rank == 0)
                usage();
            exit(0);
         case '?': /* unknown */
            if (rank == 0)
                usage();
            exit(1);
         default:
            break;
      }
   }

   if(opt_lseek + opt_fstat + opt_realpath + opt_ioctl + opt_llapi + opt_llapi_fd > 1)
   {
      fprintf(stderr, "Error: Only specify one of -l, -f, -i, -a, -A, or -r.\n");
      usage();
      exit(1);
   }

   if(argc-optind != 1)
   {
      if(rank == 0)
          usage();
      exit(1);
   }

   if ( opt_fpp ) 
   {
      opt_file = malloc( sizeof(char) * (strlen( argv[optind] ) + 10) );
      sprintf( opt_file, "%s.%d", argv[optind], rank );
   }
   else 
   {
      opt_file = strdup(argv[optind]);
   }
   assert(opt_file);

   return(0);
}

static void usage(void)
{
    printf("Usage: stat-perf [<OPTIONS>...] <FILE NAME>\n");
    printf("\n<OPTIONS> is one or more of\n");
    printf(" -c       create new file to stat\n");
    printf(" -p       do file-per-process instead of shared file\n");
    printf(" -f       use fstat instead of stat\n");
    printf(" -l       use lseek instead of stat\n");
    printf(" -r       use realpath instead of stat\n");
    printf(" -a       use Lustre API test (filename version)\n");
    printf(" -A       use Lustre API test (fd version) \n");
    printf(" -i       use ioctl Lustre test\n");
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


