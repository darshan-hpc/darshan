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

#define MAX_PROCS 8*1024


long long int vmax_lli( long long int *v, int count) {
  if (count <= 0) {
    printf("Error: max can not be computed for a vector of %d elements\n", count);
    exit(1);
    return -1;
  }
  else{
    int i;
    long long int max = v[0];
    for (i=1; i<count; i++) {
      if (v[i] > max)
	max = v[i];
    }
    return max;
  }
}


void read_log(char *filename) {
    
  int fd, prev_rank,i;
  ssize_t bytes_read;
  struct darshan_trace_record d; 
  off_t offsets[MAX_PROCS+1], crt_offsets[MAX_PROCS];
  double vtm1[MAX_PROCS], vtm2[MAX_PROCS];
  long long int  vsend_count[MAX_PROCS], vrecv_count[MAX_PROCS], voffset[MAX_PROCS];

  int cnt_offsets=0;
  int cnt;

  if ((fd=open(filename,O_RDONLY))<0){
    perror("Open Failed");
    return;
  }

  prev_rank=-1;
  while ((bytes_read = read(fd,&d,sizeof(struct darshan_trace_record)))>0) {
    if (d.rank != prev_rank) {
      offsets[cnt_offsets++] = (lseek(fd,0, SEEK_CUR) - sizeof(struct darshan_trace_record));
      prev_rank = d.rank;
    }
  }
  offsets[cnt_offsets] = lseek(fd,0, SEEK_END);

  printf("Offsets=%d\n",cnt_offsets);
  for (i=0;i<cnt_offsets;i++) {
    crt_offsets[i]=offsets[i];
    printf("%d ", offsets[i]);
  }

  cnt = 0;
  for (i=0;i<cnt_offsets;i++) {
    if (crt_offsets[i] < offsets[i+1]) {
      cnt++;
      lseek(fd, crt_offsets[i], SEEK_SET);
      if ((bytes_read = read(fd,&d,sizeof(struct darshan_trace_record)))>0) {
	vtm1[i] = d.tm1; 
	vtm2[i] = d.tm2;
	vsend_count[i] = d.send_count;
	vrecv_count[i] = d.recv_count;
	voffset[i] = d.offset; 
      }
    }
  }
  printf("Max vsend_count= %lld", vmax_lli(vsend_count, cnt));
  
  printf("\n");
  close(fd);
}


int main(int argc, char **argv)
{

    if (argc < 2) {
	printf("Call %s darshan_trace_file_name\n", argv[0]);
	exit(1);
    }

    read_log(argv[1]);

    return 0;
}
