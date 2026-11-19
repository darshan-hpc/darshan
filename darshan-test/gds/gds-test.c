/* Exercise the cuFile API against host buffers.  Expected counters (see
 * run-gds-test.sh): 4 sync writes of 4096, 3 sync reads of 4096 (one failing
 * read of an unregistered offset is not issued), 1 batch with 1 write of 8192
 * and 1 read of 512, 1 async write of 1000 and 1 async read of 1000.
 */
#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>
#include "darshan-gds-types.h"

CUfileError_t cuFileDriverOpen(void);
CUfileError_t cuFileHandleRegister(CUfileHandle_t *, CUfileDescr_t *);
void cuFileHandleDeregister(CUfileHandle_t);
ssize_t cuFileRead(CUfileHandle_t, void *, size_t, off_t, off_t);
ssize_t cuFileWrite(CUfileHandle_t, const void *, size_t, off_t, off_t);
CUfileError_t cuFileBatchIOSubmit(CUfileBatchHandle_t, unsigned, CUfileIOParams_t *, unsigned);
CUfileError_t cuFileReadAsync(CUfileHandle_t, void *, size_t *, off_t *, off_t *, ssize_t *, CUstream);
CUfileError_t cuFileWriteAsync(CUfileHandle_t, void *, size_t *, off_t *, off_t *, ssize_t *, CUstream);

int main(int argc, char **argv)
{
    const char *path = argc > 1 ? argv[1] : "gds-test.dat";
    int fd, i;
    CUfileHandle_t fh;
    CUfileDescr_t descr;
    CUfileError_t err;
    void *buf;
    CUfileIOParams_t io[2];
    size_t sz = 1000;
    off_t foff = 0, boff = 0;
    ssize_t bytes;

    fd = open(path, O_RDWR | O_CREAT | O_TRUNC | O_DIRECT, 0644);
    if(fd < 0) fd = open(path, O_RDWR | O_CREAT | O_TRUNC, 0644);
    if(fd < 0) { perror("open"); return 1; }
    if(posix_memalign(&buf, 4096, 16384)) return 1;
    memset(buf, 'x', 16384);

    cuFileDriverOpen();
    memset(&descr, 0, sizeof(descr));
    descr.type = DARSHAN_CU_FILE_HANDLE_TYPE_OPAQUE_FD;
    descr.handle.fd = fd;
    err = cuFileHandleRegister(&fh, &descr);
    if(err.err != DARSHAN_CU_FILE_SUCCESS) { fprintf(stderr, "register failed\n"); return 1; }

    for(i = 0; i < 4; i++)
        if(cuFileWrite(fh, buf, 4096, i * 4096, 0) != 4096) return 1;
    for(i = 0; i < 3; i++)
        if(cuFileRead(fh, buf, 4096, i * 4096, 0) != 4096) return 1;

    memset(io, 0, sizeof(io));
    io[0].mode = 1; io[0].fh = fh; io[0].opcode = DARSHAN_GDS_WRITE;
    io[0].u.batch.devPtr_base = buf; io[0].u.batch.size = 8192; io[0].u.batch.file_offset = 16384;
    io[1].mode = 1; io[1].fh = fh; io[1].opcode = DARSHAN_GDS_READ;
    io[1].u.batch.devPtr_base = buf; io[1].u.batch.size = 512; io[1].u.batch.file_offset = 0;
    err = cuFileBatchIOSubmit(NULL, 2, io, 0);
    if(err.err != DARSHAN_CU_FILE_SUCCESS) return 1;

    foff = 4096;
    cuFileWriteAsync(fh, buf, &sz, &foff, &boff, &bytes, NULL);
    cuFileReadAsync(fh, buf, &sz, &foff, &boff, &bytes, NULL);

    cuFileHandleDeregister(fh);
    close(fd);
    free(buf);
    return 0;
}
