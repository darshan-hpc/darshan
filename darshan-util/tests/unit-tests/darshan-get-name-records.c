#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <darshan-logutils.h>

int main(int argc, char* argv[]) {

    darshan_fd fd;
    void *buf=NULL;
    struct darshan_name_record_info *mods=NULL;
    int err=0, count;

    if (argc < 2) {
        fprintf(stderr, "Usage: %s log_file\n", argv[0]);
        return 1;
    }

    fd = darshan_log_open(argv[1]);
    if (fd == NULL) {
        fprintf(stderr, "Failed to open log file %s\n",argv[1]);
        err = 1;
        goto err_out;
    }

    err = darshan_log_get_record(fd, DARSHAN_POSIX_MOD, &buf);
    if (err < 0) {
        fprintf(stderr, "Failed to call darshan_log_get_record()\n");
        err = 1;
        goto err_out;
    }

    err = darshan_log_get_name_records(fd, &mods, &count);
    if (err < 0) {
        fprintf(stderr, "Failed to call darshan_log_get_name_records()\n");
        err = 1;
        goto err_out;
    }

    darshan_log_close(fd);

err_out:
    return err;
}
