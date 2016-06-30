struct lustre_record_ref
{
    struct darshan_lustre_record *record;
    size_t record_size;
};

struct lustre_runtime
{
    int   record_count;         /* number of records stored in record_id_hash */
    void *record_id_hash;
    int   record_buffer_size;   /* size of record_buffer in bytes */
    void *record_buffer;
    int   record_ref_array_ndx; /* current index into record_ref_array */
    struct lustre_record_ref **record_ref_array;     
};


