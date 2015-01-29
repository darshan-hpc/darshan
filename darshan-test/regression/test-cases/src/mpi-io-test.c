/*
 * (C) 1995-2001 Clemson University and Argonne National Laboratory.
 *
 * See COPYING in top-level directory.
 */

/* mpi-io-test.c
 *
 * This is derived from code given to me by Rajeev Thakur.  Dunno where
 * it originated.
 *
 * It's purpose is to produce aggregate bandwidth numbers for varying
 * block sizes, number of processors, an number of iterations.
 *
 * This is strictly an MPI program - it is used to test the MPI I/O
 * functionality implemented by Romio.
 *
 * Compiling is usually easiest with something like:
 * mpicc -Wall -Wstrict-prototypes mpi-io-test.c -o mpi-io-test
 *
 * NOTE: This code assumes that all command line arguments make it out to all
 * the processes that make up the parallel job, which isn't always the case.
 * So if it doesn't work on some platform, that might be why.
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
static int64_t opt_block     = 16*1024*1024;
static int     opt_iter      = 1;
static int     opt_coll      = 0;
static int     opt_correct   = 0;
static int     opt_sync      = 0;
static int     opt_single    = 0;
static int     opt_verbose   = 0;
static int     opt_rdonly    = 0;
static int     opt_wronly    = 0;
static char    opt_file[256] = "test.out";
static char    opt_pvfs2tab[256] = "notset";
static int     opt_pvfstab_set = 0;

/* function prototypes */
static int parse_args(int argc, char **argv);
static void usage(void);
static void handle_error(int errcode, char *str);

/* global vars */
static int mynod = 0;
static int nprocs = 1;

int main(int argc, char **argv)
{
   char *buf, *tmp=NULL, *check;
   int i, j, v, err, sync_err=0, my_correct = 1, correct, myerrno;
   double stim, etim;
   double write_tim = 0;
   double read_tim = 0;
   double read_bw, write_bw;
   double max_read_tim, max_write_tim;
   double min_read_tim, min_write_tim;
   double ave_read_tim, ave_write_tim;
   double sum_read_tim, sum_write_tim;
   double sq_write_tim, sq_read_tim;
   double sumsq_write_tim, sumsq_read_tim;
   double var_read_tim, var_write_tim;
   int64_t iter_jump = 0;
   int64_t seek_position = 0;
   MPI_File fh;
   MPI_Status status;
	MPI_Comm comm;
   int nchars=0;
   int namelen;
   char processor_name[MPI_MAX_PROCESSOR_NAME];

   /* startup MPI and determine the rank of this process */
   MPI_Init(&argc,&argv);
   MPI_Comm_size(MPI_COMM_WORLD, &nprocs);
   MPI_Comm_rank(MPI_COMM_WORLD, &mynod);
   MPI_Get_processor_name(processor_name, &namelen); 
   
   /* parse the command line arguments */
   parse_args(argc, argv);

   if (opt_verbose) fprintf(stdout,"Process %d of %d is on %s\n",
									 mynod, nprocs, processor_name);

   if (mynod == 0) printf("# Using mpi-io calls.\n");

    //sleep(15);
   
   /* kindof a weird hack- if the location of the pvfstab file was 
    * specified on the command line, then spit out this location into
    * the appropriate environment variable: */
   
   if (opt_pvfstab_set) {
      if ((setenv("PVFS2TAB_FILE", opt_pvfs2tab, 1)) < 0) {
         perror("setenv");
         goto die_jar_jar_die;
      }
   }
   
   /* this is how much of the file data is covered on each iteration of
    * the test.  used to help determine the seek offset on each
    * iteration */
   iter_jump = nprocs * opt_block;
      
   /* setup a buffer of data to write */
   if (!(tmp = malloc((size_t) opt_block + 256))) {
      perror("malloc");
      goto die_jar_jar_die;
   }
   buf = tmp + 128 - (((long)tmp) % 128);  /* align buffer */

   /* open the file for writing */
	if (opt_coll) {
		comm = MPI_COMM_WORLD;
	}
	else {
		comm = MPI_COMM_SELF;
	}
   err = MPI_File_open(comm, opt_file, 
							  MPI_MODE_CREATE | MPI_MODE_RDWR, MPI_INFO_NULL, &fh);
   if (err != MPI_SUCCESS) {
      handle_error(err, "MPI_File_open");
      goto die_jar_jar_die;
   } 
	
	nchars = (int) (opt_block/sizeof(char));
	if (!opt_rdonly) {

   /* now repeat the seek and write operations the number of times
    * specified on the command line */
   for (j=0; j < opt_iter; j++) {

      /* reading and writing to the same block is cheating, but sometimes
       * we want to measure cached performance of file servers */
      if (opt_single == 1)
			seek_position = 0;
      else
			/* seek to an appropriate position depending on the iteration 
			 * and rank of the current process */
			seek_position = (j*iter_jump)+(mynod*opt_block);

      MPI_File_seek(fh, seek_position, MPI_SEEK_SET);

      if (opt_correct) /* fill in buffer for iteration */ {
         for (i=0, v=mynod+j, check=buf; i<opt_block; i++, v++, check++) 
            *check = (char) v;
      }

      /* discover the starting time of the operation */
      MPI_Barrier(MPI_COMM_WORLD);
      stim = MPI_Wtime();

      /* write out the data */
		if (opt_coll) {
			err = MPI_File_write_all(fh, buf, nchars, MPI_CHAR, &status);
		}
		else {
			err = MPI_File_write(fh, buf, nchars, MPI_CHAR, &status);
		}
      if(err){
         fprintf(stderr, "node %d, write error: %s\n", mynod, 
         strerror(errno));
      }
      if (opt_sync) sync_err = MPI_File_sync(fh);
      if (sync_err) {
         fprintf(stderr, "node %d, sync error: %s\n", mynod, 
					  strerror(errno));
      }

      /* discover the ending time of the operation */
      etim = MPI_Wtime();

      write_tim += (etim - stim);
      
      /* we are done with this "write" iteration */
   }
	} /* ! opt_rdonly */

   err = MPI_File_close(&fh);
   if(err){
      fprintf(stderr, "node %d, close error after write\n", mynod);
   }
    
   /* wait for everyone to synchronize at this point */
   MPI_Barrier(MPI_COMM_WORLD);

   /* reopen the file to read the data back out */
   err = MPI_File_open(comm, opt_file, 
			   MPI_MODE_CREATE | MPI_MODE_RDWR, MPI_INFO_NULL, &fh);
   if (err < 0) {
      fprintf(stderr, "node %d, open error: %s\n", mynod, strerror(errno));
      goto die_jar_jar_die;
   }

	if (!opt_wronly) {
   /* we are going to repeat the read operation the number of iterations
    * specified */
   for (j=0; j < opt_iter; j++) {
      /* reading and writing to the same block is cheating, but sometimes
       * we want to measure cached performance of file servers */
      if (opt_single == 1) {
			seek_position = 0;
		}
      else {
			/* seek to an appropriate position depending on the iteration 
			 * and rank of the current process */
			seek_position = (j*iter_jump)+(mynod*opt_block);
		}

      MPI_File_seek(fh, seek_position, MPI_SEEK_SET);

      /* discover the start time */
      MPI_Barrier(MPI_COMM_WORLD);
      stim = MPI_Wtime();

      /* read in the file data */
		if (opt_coll) {
			err = MPI_File_read_all(fh, buf, nchars, MPI_CHAR, &status);
		}
		else {
			err = MPI_File_read(fh, buf, nchars, MPI_CHAR, &status);
		}
      myerrno = errno;

      /* discover the end time */
      etim = MPI_Wtime();
      read_tim += (etim - stim);

      if (err < 0) {
			fprintf(stderr, "node %d, read error, loc = %lld: %s\n",
				mynod, (long long) mynod*opt_block, strerror(myerrno));
		}

      /* if the user wanted to check correctness, compare the write
       * buffer to the read buffer */
      if (opt_correct) {
         int badct = 0;

         for (i=0, v=mynod+j, check=buf;
              i < opt_block && badct < 10;
              i++, v++, check++)
         {
            if (*check != (char) v) {
               my_correct = 0;
               if (badct < 10) {
                  badct++;
                  fprintf(stderr, "buf[%d] = %d, should be %d\n", 
                          i, *check, (char) v);
               }
            }
         }
         MPI_Allreduce(&my_correct, &correct, 1, MPI_INT, MPI_MIN,
                       MPI_COMM_WORLD);
         if (badct == 10) fprintf(stderr, "...\n");
      }

      /* we are done with this read iteration */
   }
	} /* !opt_wronly */

   /* close the file */
   err = MPI_File_close(&fh);
   if (err) {
      fprintf(stderr, "node %d, close error after write\n", mynod);
   }

   /* compute the read and write times */
   MPI_Allreduce(&read_tim, &max_read_tim, 1, MPI_DOUBLE, MPI_MAX,
      MPI_COMM_WORLD);
   MPI_Allreduce(&read_tim, &min_read_tim, 1, MPI_DOUBLE, MPI_MIN,
      MPI_COMM_WORLD);
   MPI_Allreduce(&read_tim, &sum_read_tim, 1, MPI_DOUBLE, MPI_SUM,
      MPI_COMM_WORLD);

   /* calculate our part of the summation used for variance */
   sq_read_tim = read_tim - (sum_read_tim / nprocs);
   sq_read_tim = sq_read_tim * sq_read_tim;
   MPI_Allreduce(&sq_read_tim, &sumsq_read_tim, 1, MPI_DOUBLE, 
                   MPI_SUM, MPI_COMM_WORLD);


   MPI_Allreduce(&write_tim, &max_write_tim, 1, MPI_DOUBLE, MPI_MAX,
      MPI_COMM_WORLD);
   MPI_Allreduce(&write_tim, &min_write_tim, 1, MPI_DOUBLE, MPI_MIN,
      MPI_COMM_WORLD);
   MPI_Allreduce(&write_tim, &sum_write_tim, 1, MPI_DOUBLE, MPI_SUM,
      MPI_COMM_WORLD);

   /* calculate our part of the summation used for variance */
   sq_write_tim = write_tim - (sum_write_tim / nprocs );
   sq_write_tim = sq_write_tim * sq_write_tim;
   MPI_Allreduce(&sq_write_tim, &sumsq_write_tim, 1, MPI_DOUBLE, 
                   MPI_SUM, MPI_COMM_WORLD);

   /* calculate the average from the sum */
   ave_read_tim  = sum_read_tim / nprocs; 
   ave_write_tim = sum_write_tim / nprocs; 

   /* and finally compute variance */
   if (nprocs > 1) {
		var_read_tim  = sumsq_read_tim / (nprocs-1);
		var_write_tim = sumsq_write_tim / (nprocs-1);
   }
   else {
		var_read_tim = 0;
		var_write_tim = 0;
   }
   
   /* print out the results on one node */
   if (mynod == 0) {
      read_bw = (opt_block*nprocs*opt_iter)/(max_read_tim*1.0e6);
      write_bw = (opt_block*nprocs*opt_iter)/(max_write_tim*1.0e6);
      
		printf("nr_procs = %d, nr_iter = %d, blk_sz = %lld, coll = %d\n",
				 nprocs, opt_iter, (long long) opt_block, opt_coll);
		
		printf("# total_size = %lld\n",
				 (long long) opt_block*nprocs*opt_iter);
		printf("# Write: min_t = %f, max_t = %f, mean_t = %f, var_t = %f\n", 
				 min_write_tim, max_write_tim, ave_write_tim, var_write_tim);
		printf("# Read:  min_t = %f, max_t = %f, mean_t = %f, var_t = %f\n", 
				 min_read_tim, max_read_tim, ave_read_tim, var_read_tim);
      
      printf("Write bandwidth = %f Mbytes/sec\n", write_bw);
      printf("Read bandwidth = %f Mbytes/sec\n", read_bw);
      
      if (opt_correct) {
         printf("Correctness test %s.\n", correct ? "passed" : "failed");
      }
   }


die_jar_jar_die:   

   free(tmp);
   MPI_Finalize();
   return(0);
}

static int parse_args(int argc, char **argv)
{
   int c;
   
   while ((c = getopt(argc, argv, "b:i:f:p:CcyShvrw")) != EOF) {
      switch (c) {
         case 'b': /* block size */
            opt_block = atoi(optarg);
            break;
         case 'i': /* iterations */
            opt_iter = atoi(optarg);
            break;
         case 'f': /* filename */
            strncpy(opt_file, optarg, 255);
            break;
         case 'p': /* pvfstab file */
            strncpy(opt_pvfs2tab, optarg, 255);
            opt_pvfstab_set = 1;
            break;
         case 'c': /* correctness */
            opt_correct = 1;
            break;
         case 'C': /* collective I/O */
            opt_coll = 1;
            break;
         case 'y': /* sYnc */
            opt_sync = 1;
            break;
         case 'S': /* Single region */
            opt_single = 1;
            break;
         case 'v': /* verbose */
            opt_verbose = 1;
            break;
			case 'r': /* read-only */
				opt_rdonly = 1;
				break;
			case 'w': /* write-only */
				opt_wronly = 1;
				break;
         case 'h':
            if (mynod == 0)
                usage();
            exit(0);
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
    printf("Usage: mpi-io-test [<OPTIONS>...]\n");
    printf("\n<OPTIONS> is one of\n");
    printf(" -b       block size (in bytes) [default: 16777216]\n");
    printf(" -c       verify correctness of file data [default: off]\n");
    printf(" -C       perform operations Collectively [default: off]\n");
    printf(" -i       iterations [default: 1]\n");
    printf(" -f       filename [default: /foo/test.out]\n");
    printf(" -p       path to pvfs2tab file to use [default: notset]\n");
    printf(" -S       all process write to same Single region of file [default: off]\n");
	 printf(" -r       read-only.  do no writes.  file must already exist\n");
	 printf(" -w       write-only. do no reads.\n");
    printf(" -v       be more verbose\n");
    printf(" -y       sYnc the file after each write [default: off]\n");
    printf(" -h       print this help\n");
}

static void handle_error(int errcode, char *str)
{
    char msg[MPI_MAX_ERROR_STRING];
    int resultlen;

    MPI_Error_string(errcode, msg, &resultlen);
    fprintf(stderr, "%s: %s\n", str, msg);
    MPI_Abort(MPI_COMM_WORLD, 1);
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


