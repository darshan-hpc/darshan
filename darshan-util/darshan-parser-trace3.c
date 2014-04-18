#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <limits.h>
#include <errno.h>

//#include <darshan-logutils.h>
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

#define MAX_PROCS 8*1024
#define HISTOGRAM_BINS 10

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

long long int vmin_lli( long long int *v, int count) {
  if (count <= 0) {
    printf("Error: min can not be computed for a vector of %d elements\n", count);
    exit(1);
    return -1;
  }
  else{
    int i;
    long long int min = v[0];
    for (i=1; i<count; i++) {
      if (v[i] < min)
	min = v[i];
    }
    return min;
  }
}

long long int vmean_lli( long long int *v, int count) {
  if (count <= 0) {
    printf("Error: mean can not be computed for a vector of %d elements\n", count);
    exit(1);
    return -1;
  }
  else{
    int i;
    long long int mean = 0;
    for (i=0; i<count; i++) {
      mean += v[i];
    }
    return mean/count;
  }
}



double vmax_double(double *v, int count) {
  if (count <= 0) {
    printf("Error: max can not be computed for a vector of %d elements\n", count);
    exit(1);
    return -1;
  }
  else{
    int i;
    double max = v[0];
    for (i=1; i<count; i++) {
      if (v[i] > max)
	max = v[i];
    }
    return max;
  }
}

double vmin_double(double *v, int count) {
  if (count <= 0) {
    printf("Error: max can not be computed for a vector of %d elements\n", count);
    exit(1);
    return -1;
  }
  else{
    int i;
    double min = v[0];
    for (i=1; i<count; i++) {
      if (v[i] < min)
	min = v[i];
    }
    return min;
  }
}

double vmean_double(double *v, int count) {
  if (count <= 0) {
    printf("Error: mean can not be computed for a vector of %d elements\n", count);
    exit(1);
    return -1;
  }
  else{
    int i;
    double mean = 0;
    for (i=0; i<count; i++) {
      mean += v[i];
    }
    return mean/count;
  }
}



void get_histogram(double *v, int count, double min, double max, int cnt_bins, int *hist) {
  int i, bin;
  for (i=0; i<cnt_bins; i++){
    hist[i]=0;
  }
  for (i=0; i<count; i++){
    bin = (int) ((v[i]-min)/(max-min)*cnt_bins);
    if (bin == cnt_bins)
      bin--;
    (hist[bin])++;
  }
}

void print_histogram(int cnt_bins, int *hist){
  int i;
  printf("Hist %d bins:", cnt_bins);
  for (i=0; i<cnt_bins; i++){
    printf("%d ", hist[i]);
  }
  printf("\n");
}

void fprintf_csv_histogram(FILE *f,int cnt_bins, int *hist ) {
  int i;
  for (i=0; i<cnt_bins; i++){
    fprintf(f,",%d", hist[i]);
  }  
}

void read_log(char *filename) {
    
  int fd, prev_rank,i;
  ssize_t bytes_read;
  struct darshan_trace_record d; 
  off_t offsets[MAX_PROCS+1], crt_offsets[MAX_PROCS];
  double vtm1[MAX_PROCS], vtm2[MAX_PROCS];
  long long int  vsend_count[MAX_PROCS], vrecv_count[MAX_PROCS], voffset[MAX_PROCS];

  int hist1[HISTOGRAM_BINS],hist2[HISTOGRAM_BINS];

  int cnt_offsets=0;
  int cnt=1;
  FILE *fout;
  char csvfile[PATH_MAX];

  if ((fd=open(filename,O_RDONLY))<0){
    perror("Open Failed");
    return;
  }
  
  sprintf(csvfile,"%s.csv",filename);
  if ((fout=fopen(csvfile,"w"))==NULL){
    perror("Fopen Failed");
    return;	
  }
  fprintf(fout, "count_procs,min_t1,max_t1,mean_t1,hist1_t1,hist2_t1,hist3_t1,hist4_t1,hist5_t1,hist6_t1,hist7_t1,hist8_t1,hist9_t1,hist10_t1");
  fprintf(fout, ",min_t2,max_t2,mean_t2,hist1_t2,hist2_t2,hist3_t2,hist4_t2,hist5_t2,hist6_t2,hist7_t2,hist8_t2,hist9_t2,hist10_t2");
  fprintf(fout, ",min_send_count,max_send_count,min_recv_count,max_recv_count,min_offset,max_offset\n"); 
    

  prev_rank=-1;
  while ((bytes_read = read(fd,&d,sizeof(struct darshan_trace_record)))>0) {
    if (d.rank != prev_rank) {
      offsets[cnt_offsets++] = (lseek(fd,0, SEEK_CUR) - sizeof(struct darshan_trace_record));
      prev_rank = d.rank;
    }
  }
  offsets[cnt_offsets] = lseek(fd,0, SEEK_END);

  //printf("Offsets=%d\n",cnt_offsets);
  for (i=0;i<cnt_offsets;i++) {
    crt_offsets[i]=offsets[i];
    //    printf("%d ", offsets[i]);
  }

  while (cnt>0) {
    cnt = 0;
    for (i=0;i<cnt_offsets;i++) {
      if (crt_offsets[i] < offsets[i+1]) {
	cnt++;
	lseek(fd, crt_offsets[i], SEEK_SET);
	if ((bytes_read = read(fd,&d,sizeof(struct darshan_trace_record)))>0) {
	  vtm1[i] = d.tm1; 
	  vtm2[i] = d.tm2;
	  vsend_count[i] = (long long int) d.send_count;
	  vrecv_count[i] = (long long int ) d.recv_count;
	  voffset[i] = d.offset; 
	}
	crt_offsets[i] += sizeof(struct darshan_trace_record);
      }
    }
    if (cnt > 0) {
      /*      printf("count=%d Min time1=%f Max time1=%f Min time2=%f Max time2=%f Min vsend_count= %lld  Max vsend_count= %lld Min recv_count= %lld  Max recv_count= %lld Min offset= %lld  Max offset= %lld\n",
	     cnt,
	     vmin_double(vtm1,cnt), vmax_double(vtm1,cnt), 
	     vmin_double(vtm2,cnt), vmax_double(vtm2,cnt), 
	     vmin_lli(vsend_count, cnt), vmax_lli(vsend_count, cnt),
	     vmin_lli(vrecv_count, cnt), vmax_lli(vrecv_count, cnt),
	     vmin_lli(voffset, cnt), vmax_lli(voffset, cnt));
      */
      get_histogram(vtm1, cnt, vmin_double(vtm1,cnt), vmax_double(vtm1,cnt),HISTOGRAM_BINS , hist1);
      get_histogram(vtm2, cnt, vmin_double(vtm2,cnt), vmax_double(vtm2,cnt),HISTOGRAM_BINS , hist2);
      
      //print_histogram(HISTOGRAM_BINS , hist1);
      //print_histogram(HISTOGRAM_BINS , hist2);

 
      fprintf(fout,"%d,%f,%f,%f",cnt, vmin_double(vtm1,cnt), vmax_double(vtm1,cnt),vmean_double(vtm1,cnt));
      fprintf_csv_histogram(fout,HISTOGRAM_BINS , hist1);
      fprintf(fout,",%f,%f,%f",vmin_double(vtm2,cnt), vmax_double(vtm2,cnt),vmean_double(vtm2,cnt));
      fprintf_csv_histogram(fout,HISTOGRAM_BINS , hist2);
      fprintf(fout,",%lld,%lld,%lld,%lld,%lld,%lld\n",
	      vmin_lli(vsend_count, cnt), vmax_lli(vsend_count, cnt),
	      vmin_lli(vrecv_count, cnt), vmax_lli(vrecv_count, cnt),
	      vmin_lli(voffset, cnt), vmax_lli(voffset, cnt));
    }
  }
    
  close(fd);
  fclose(fout);
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
