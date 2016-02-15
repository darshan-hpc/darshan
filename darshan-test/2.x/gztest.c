/*
 *  (C) 2009 by Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
 */

/* this is just a scratch program for playing with zlib */

#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <zlib.h>
#include <assert.h>

int main(int argc, char **argv)
{
    int fd;
    int ret;
    void* normal_buffer;
    void* comp_buffer;
    unsigned long size;
    unsigned long comp_size; 
    z_stream tmp_stream;

    if(argc != 2)
    {
        fprintf(stderr, "Usage: %s <filename>\n", argv[0]);
        return(-1);
    }

    normal_buffer = malloc(1024*1024);
    comp_buffer = malloc(1024*1024 + 100);
    assert(normal_buffer && comp_buffer);

    fd = open(argv[1], O_RDONLY);
    assert(fd);
   
    /* read in entire file */
    size = read(fd, normal_buffer, 1024*1024);
    assert(size >= 0);
    comp_size = size + 100;

    close(fd);

    #if 0
    ret = compress(comp_buffer, &comp_size, normal_buffer, size);
    assert(ret == 0);
    #else

    tmp_stream.next_in = normal_buffer;
    tmp_stream.avail_in = size;
    tmp_stream.zalloc = Z_NULL;
    tmp_stream.zfree = Z_NULL;
    tmp_stream.opaque = Z_NULL;

    ret = deflateInit2(&tmp_stream, Z_DEFAULT_COMPRESSION, Z_DEFLATED, 31, 8, 
        Z_DEFAULT_STRATEGY);
    assert(ret == Z_OK);

    tmp_stream.avail_in = size;
    tmp_stream.next_in = normal_buffer;
    tmp_stream.next_out = comp_buffer;
    tmp_stream.avail_out = deflateBound(&tmp_stream, size);
    printf("deflate bound: %u\n", tmp_stream.avail_out);

    ret = deflate(&tmp_stream, Z_FINISH);

    assert(ret == Z_STREAM_END);
    comp_size = tmp_stream.total_out;
    #endif

    /* keep adding on to output file */
    fd = open("out.gz", O_WRONLY|O_CREAT|O_APPEND, S_IRUSR|S_IWUSR);
    assert(fd);

    size = write(fd, comp_buffer, comp_size);
    assert(size == comp_size);

    close(fd);

    return(0);
}
