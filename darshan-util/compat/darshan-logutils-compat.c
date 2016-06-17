/*
 * Copyright (C) 2015 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

#define _GNU_SOURCE
#include "darshan-util-config.h"
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <inttypes.h>
#include <sys/types.h>

#include "darshan-logutils-compat.h"

int darshan_log_get_namerecs_3_00(void *name_rec_buf, int buf_len,
    int swap_flag, struct darshan_name_record_ref **hash)
{
    struct darshan_name_record_ref *ref;
    char *buf_ptr;
    darshan_record_id *rec_id_ptr;
    uint32_t *path_len_ptr;
    char *path_ptr;
    int rec_len;
    int buf_processed = 0;

    /* work through the name record buffer -- deserialize the mapping data and
     * add to the output hash table
     * NOTE: these mapping pairs are variable in length, so we have to be able
     * to handle incomplete mappings temporarily here
     */
    buf_ptr = name_rec_buf;
    while(buf_len > (sizeof(darshan_record_id) + sizeof(uint32_t)))
    {
        /* see if we have enough buf space to read in the next full record */
        path_len_ptr = (uint32_t *)(buf_ptr + sizeof(darshan_record_id));
        if(swap_flag)
            DARSHAN_BSWAP32(path_len_ptr);
        rec_len = sizeof(darshan_record_id) + sizeof(uint32_t) + *path_len_ptr;

        /* we need to read more before we continue deserializing */
        if(buf_len < rec_len)
            break;

        /* get pointers for each field of this darshan record */
        /* NOTE: darshan record hash serialization method: 
         *          ... darshan_record_id | (uint32_t) path_len | path ...
         */
        rec_id_ptr = (darshan_record_id *)buf_ptr;
        path_ptr = (char *)(buf_ptr + sizeof(darshan_record_id) + sizeof(uint32_t));

        if(swap_flag)
            /* we need to sort out endianness issues before deserializing */
            DARSHAN_BSWAP64(rec_id_ptr);

        HASH_FIND(hlink, *hash, rec_id_ptr, sizeof(darshan_record_id), ref);
        if(!ref)
        {
            ref = malloc(sizeof(*ref));
            if(!ref)
                return(-1);

            ref->name_record = malloc(rec_len - sizeof(uint32_t) + 1);
            if(!ref->name_record)
            {
                free(ref);
                return(-1);
            }

            /* transform the serialized name record into the zero-length
             * array structure darshan uses to track name records
             */
            ref->name_record->id = *rec_id_ptr;
            memcpy(ref->name_record->name, path_ptr, *path_len_ptr);
            ref->name_record->name[*path_len_ptr] = '\0';

            /* add this record to the hash */
            HASH_ADD(hlink, *hash, name_record->id, sizeof(darshan_record_id), ref);
        }

        buf_ptr += rec_len;
        buf_len -= rec_len;
        buf_processed += rec_len;
    }

    return(buf_processed);
}

/*
 * Local variables:
 *  c-indent-level: 4
 *  c-basic-offset: 4
 * End:
 *
 * vim: ts=8 sts=4 sw=4 expandtab
 */
