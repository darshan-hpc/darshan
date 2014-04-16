#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <limits.h>
#include <errno.h>

#include <darshan-logutils.h>

/*
struct darshan_trace_record {
    int rank;
    int epoch; 
    int op;
    double tm1;
    double tm2;
    int send_count;
    int recv_count;
    long long int offset;
};
*/
void split_path_file(char** f, char *pf) {
    char *slash = pf, *next;
    while ((next = strpbrk(slash + 1, "\\/"))) slash = next;
    if (pf != slash) slash++;
    *f = strdup(slash);
}

void read_log(char *filename,char *dir, int cnt_epochs, int bin_text) {
    
  int fd,i,j;
  ssize_t bytes_read;
  FILE *fout[11][5];
  int outfd[11][5];
  struct darshan_trace_record d; 
  char outdir[PATH_MAX], outfile[PATH_MAX];
  char *justfile;

  
  if ((fd=open(filename,O_RDONLY))<0){
    perror("Open Failed");
    return;
  }
 
  split_path_file(&justfile,filename);
  //printf(justfile);
 
  sprintf(outdir,"%s/%s",dir,justfile);
  if (mkdir(outdir,0777)<0){
    if (errno !=EEXIST) {
      perror("Mkdir Failed");
      return;
    }
  }
  for (i=0;i<cnt_epochs;i++){
    for (j=0;j<5;j++){
      switch (j) {
      case 0:
	sprintf(outfile,"%s/CP_MPI_ALLREDUCES-%d",outdir,i);
	break;
      case 1:
	sprintf(outfile,"%s/CP_MPI_ALLTOALLS-%d",outdir,i);
	break;
      case 2:
	sprintf(outfile,"%s/CP_MPI_ALLTOALLVS-%d",outdir,i);
	break;
      case 3:
	sprintf(outfile,"%s/CP_POSIX_WRITES-%d",outdir,i);
	break;
      case 4:
	sprintf(outfile,"%s/CP_COLL_WRITES-%d",outdir,i);
	break;
      }
      if (bin_text) {
	if ((fout[i][j]=fopen(outfile,"w"))==NULL){
	  perror("Fopen Failed");
	  return;	
	}
	fprintf(fout[i][j], "rank,epoch,counter,start_time,end_time,write_count,read_count,offset\n");
      }
      else
	if ((outfd[i][j] = open(outfile,O_CREAT|O_WRONLY|O_TRUNC,0666))<0){
	  perror("Fopen 2  Failed");
	  return;	
	}
    } 
  }

  while ((bytes_read = read(fd,&d,sizeof(struct darshan_trace_record)))>0) {
    int j;
    switch (d.op) {
    case CP_MPI_ALLREDUCES: j=0; break;
    case CP_MPI_ALLTOALLS: j=1; break;
    case CP_MPI_ALLTOALLVS: j=2; break;
    case CP_POSIX_WRITES: j=3; break;
    case CP_COLL_WRITES: j=4; break;
    otherwise: {
	printf("Unsupported counter\n");
	continue;
      }
    }
    if (bin_text)
      fprintf(fout[d.epoch][j], "%d,%d,%s,%.6f,%.6f,%d,%d,%lld\n",
	      d.rank, d.epoch, darshan_names[d.op],d.tm1,d.tm2,d.send_count, d.recv_count,d.offset);
    else
      write(outfd[d.epoch][j],&d,sizeof(struct darshan_trace_record));
  }
  
  close(fd);
  for (i=0;i<cnt_epochs;i++)
    for (j=0;j<5;j++)
      if (bin_text)
	fclose(fout[i][j]);
      else
	close(outfd[i][j]);
}


int main(int argc, char **argv)
{

    if (argc < 5) {
	printf("Call %s darshan_trace_file_name processing_dir cnt_epochs binary/text(0/1)\n", argv[0]);
	exit(1);
    }

    read_log(argv[1], argv[2], atoi(argv[3]), atoi(argv[4]));

    return 0;
}
