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


void read_log(char *filename, char* csvfile, int epoch, char* counter) {
    
    int fd;
    ssize_t bytes_read;
    FILE *fout;
    struct darshan_trace_record d; 
    int flag = 1;

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
	if (epoch < 0) { 
		if (counter) { 
			if (strcmp(counter, darshan_names[d.op])) {
				flag = 0;
			}
		}
	}
	else {
		if (epoch != d.epoch) 
			flag = 0;
		else {
			if (counter) {
				if (strcmp(counter, darshan_names[d.op])) {
                                	flag = 0;
                        	}
			}
		}	
	}
	if (flag)
        	fprintf(fout, "%d,%d,%s,%.6f,%.6f,%d,%d,%lld\n",
               d.rank, d.epoch, darshan_names[d.op],d.tm1,d.tm2,d.send_count, d.recv_count,d.offset);

	flag = 1;
    }
    
    close(fd);
    if (csvfile)	
    	fclose(fout);

}


int main(int argc, char **argv)
{
    int epoch = -1;
    char *counter = NULL;	

    if (argc < 2) {
	printf("Call %s darshan_trace_file_name [epoch] [counter] \n", argv[0]);
	exit(1);
    }
    else {
	if (argc > 2) {	
    		epoch = atoi(argv[2]);
		if (argc > 3)
			counter = argv[3];
	} 	
    }
    read_log(argv[1], NULL, epoch, counter);

    return 0;
}
