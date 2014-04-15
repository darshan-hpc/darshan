#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

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


void read_log(char *filename, char* csvfile) {
    
    int fd;
    ssize_t bytes_read;
    FILE *fout;
    struct darshan_trace_record d; 

    if ((fd=open(filename,O_RDONLY))<0){
        perror("Open Failed");
        return;
    }
    
    if (csvfile){	
    	if ((fout=fopen(csvfile,"w"))==NULL){
        	perror("Open Failed");
        	return;	
    	}
    }
    else
	fout = stdout;

    fprintf(fout, "rank,epoch,counter,start_time,end_time,write_count,read_count,offset\n");
    while ((bytes_read = read(fd,&d,sizeof(struct darshan_trace_record)))>0) {
        fprintf(fout, "%d,%d,%s,%.6f,%.6f,%d,%d,%lld\n",
               d.rank, d.epoch, darshan_names[d.op],d.tm1,d.tm2,d.send_count, d.recv_count,d.offset);

    }
    
    close(fd);
    if (csvfile)	
    	fclose(fout);

}


int main(int argc, char **argv)
{
    if (argc != 2) {
	printf("Call %s darshan_trace_file_name", argv[0]);
	exit(1);
    }

    read_log(argv[1], NULL);

    return 0;
}
