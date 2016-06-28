struct lustre_record_runtime
{
    struct darshan_lustre_record *record;
    size_t record_size;
    UT_hash_handle hlink;
};

struct lustre_runtime
{
    int    record_count;       /* number of records defined */
    size_t record_buffer_max;  /* size of the allocated buffer pointed to by record_buffer */
    size_t record_buffer_used; /* size of the allocated buffer actually used */
    void   *next_free_record;  /* pointer to end of record_buffer */
    void   *record_buffer;     /* buffer in which records are created */
    struct lustre_record_runtime *record_runtime_array;
    struct lustre_record_runtime *record_runtime_hash;
};


