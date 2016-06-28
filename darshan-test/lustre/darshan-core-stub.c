#define _XOPEN_SOURCE 500
#define _GNU_SOURCE

#include "darshan-runtime-config.h"
#include "darshan.h"
#include "darshan-core.h"

#include <stdio.h>
#include <sys/types.h>
#include <stdlib.h>
#include <fcntl.h>

#include "darshan-lustre.h"

/*
 *  Global variables
 */
static darshan_record_id next_rec_id = 1;
static int my_rank = 0;
static struct darshan_module_funcs mod_funcs;

/*
 *  Import routines from Lustre module
 */
extern struct lustre_runtime *lustre_runtime;

void darshan_core_register_record(
    void *name,
    int len,
    darshan_module_id mod_id,
    int printable_flag,
    int mod_limit_flag,
    darshan_record_id *rec_id,
    struct darshan_fs_info *fs_info)
{
    *rec_id = next_rec_id++;
 
    if (fs_info)
    {
        memset( fs_info, 0, sizeof(struct darshan_fs_info) );
        fs_info->fs_type = -1;
    }

    return;
}

void darshan_core_register_module(
    darshan_module_id mod_id,
    struct darshan_module_funcs *funcs,
    int *rank,
    int *mod_mem_limit,
    int *sys_mem_alignment)
{
/*  if (sys_mem_alignment) *sys_mem_alignment = darshan_mem_alignment; */
    if (rank) *rank = my_rank;
    *mod_mem_limit = DARSHAN_MOD_MEM_MAX;
    mod_funcs = *funcs;

    return;
}

void darshan_core_shutdown()
{
    darshan_record_id *mod_shared_recs = NULL;
    int mod_shared_rec_cnt = 0;
    void* mod_buf = NULL;
    int mod_buf_sz = 0;

    mod_funcs.begin_shutdown();
    mod_funcs.get_output_data( MPI_COMM_WORLD, mod_shared_recs, mod_shared_rec_cnt, &mod_buf, &mod_buf_sz );

    print_lustre_runtime();

    mod_funcs.shutdown();

    return;
}

int main( int argc, char **argv )
{
    int fd, i;
    char *fname;

    srand(234);

    /* build Darshan records */
    for ( i = 1; i < argc; i++ )
    {
        fname = argv[i];
        printf( "File %3d - processing %s\n", i, fname );
        fd = open( fname, O_RDONLY );
        darshan_instrument_lustre_file( fname, fd );
        close(fd);
    }

    for ( i = 0; i < lustre_runtime->record_count; i++ )
        (lustre_runtime->record_runtime_array[i]).record->rank = rand() % 10;

    print_lustre_runtime();

    darshan_core_shutdown();

    return 0;
}
