/* Fake libcufile for testing the Darshan GDS module without a GPU or CUDA.
 * Exports the wrapped cuFile symbols; I/O is done synchronously with
 * pread/pwrite on host memory.  Uses only darshan-gds-types.h types.
 */
#define _GNU_SOURCE
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include "darshan-gds-types.h"

struct fake_handle { int fd; };

static CUfileError_t ok(void) { CUfileError_t e = {DARSHAN_CU_FILE_SUCCESS, 0}; return e; }
static CUfileError_t bad(void) { CUfileError_t e = {5, 0}; return e; }

CUfileError_t cuFileDriverOpen(void) { return ok(); }
CUfileError_t cuFileDriverClose_v2(void) { return ok(); }

CUfileError_t cuFileHandleRegister(CUfileHandle_t *fh, CUfileDescr_t *descr)
{
    struct fake_handle *h;
    if(!fh || !descr || descr->type != DARSHAN_CU_FILE_HANDLE_TYPE_OPAQUE_FD)
        return bad();
    h = malloc(sizeof(*h));
    h->fd = descr->handle.fd;
    *fh = h;
    return ok();
}

void cuFileHandleDeregister(CUfileHandle_t fh) { free(fh); }

ssize_t cuFileRead(CUfileHandle_t fh, void *buf, size_t size, off_t off, off_t boff)
{
    return pread(((struct fake_handle *)fh)->fd, (char *)buf + boff, size, off);
}

ssize_t cuFileWrite(CUfileHandle_t fh, const void *buf, size_t size, off_t off, off_t boff)
{
    return pwrite(((struct fake_handle *)fh)->fd, (const char *)buf + boff, size, off);
}

CUfileError_t cuFileBatchIOSubmit(CUfileBatchHandle_t b, unsigned nr, CUfileIOParams_t *io, unsigned flags)
{
    unsigned i;
    for(i = 0; i < nr; i++)
    {
        int fd = ((struct fake_handle *)io[i].fh)->fd;
        char *p = (char *)io[i].u.batch.devPtr_base + io[i].u.batch.devPtr_offset;
        if(io[i].opcode == DARSHAN_GDS_READ)
            (void)!pread(fd, p, io[i].u.batch.size, io[i].u.batch.file_offset);
        else
            (void)!pwrite(fd, p, io[i].u.batch.size, io[i].u.batch.file_offset);
    }
    return ok();
}

CUfileError_t cuFileReadAsync(CUfileHandle_t fh, void *buf, size_t *size_p, off_t *off_p,
    off_t *boff_p, ssize_t *bytes_p, CUstream s)
{
    *bytes_p = pread(((struct fake_handle *)fh)->fd, (char *)buf + *boff_p, *size_p, *off_p);
    return ok();
}

CUfileError_t cuFileWriteAsync(CUfileHandle_t fh, void *buf, size_t *size_p, off_t *off_p,
    off_t *boff_p, ssize_t *bytes_p, CUstream s)
{
    *bytes_p = pwrite(((struct fake_handle *)fh)->fd, (char *)buf + *boff_p, *size_p, *off_p);
    return ok();
}
