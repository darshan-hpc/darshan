/* from darshan-logutils.h */
struct darshan_mnt_info
{
		char mnt_type[3031];
		char mnt_path[3031];
};

struct darshan_mod_info
{
		char *name;
		int	len;
		int	ver;
		int	idx;
};

/* from darshan-log-format.h */
struct darshan_job
{
		int64_t uid;
		int64_t start_time;
		int64_t end_time;
		int64_t nprocs;
		int64_t jobid;
		char metadata[1024];
};
struct darshan_base_record
{
		uint64_t id;
		int64_t rank;
};

struct darshan_posix_file
{
		struct darshan_base_record base_rec;
		int64_t counters[64];
		double fcounters[17];
};

struct darshan_stdio_file
{
		struct darshan_base_record base_rec;
		int64_t counters[13];
		double fcounters[15];
};

struct darshan_mpiio_file
{
		struct darshan_base_record base_rec;
		int64_t counters[51];
		double fcounters[15];
};

struct darshan_hdf5_file
{
		struct darshan_base_record base_rec;
		int64_t counters[1];
		double fcounters[2];
};

struct darshan_pnetcdf_file
{
		struct darshan_base_record base_rec;
		int64_t counters[2];
		double fcounters[2];
};

struct darshan_bgq_record
{
		struct darshan_base_record base_rec;
		int64_t counters[11];
		double fcounters[1];
};

/* from darshan-apxc-log-format.h */
struct darshan_apxc_header_record
{
		struct darshan_base_record base_rec;
		int64_t magic;
		int nblades;
		int nchassis;
		int ngroups;
		int memory_mode;
		int cluster_mode;
};
struct darshan_apxc_perf_record
{
		struct darshan_base_record base_rec;
		int64_t counters[396];
};

/* counter names */
char *apxc_counter_names[];
char *bgq_counter_names[];
char *bgq_f_counter_names[];
char *hdf5_counter_names[];
char *hdf5_f_counter_names[];
char *mpiio_counter_names[];
char *mpiio_f_counter_names[];
char *pnetcdf_counter_names[];
char *pnetcdf_f_counter_names[];
char *posix_counter_names[];
char *posix_f_counter_names[];
char *stdio_counter_names[];
char *stdio_f_counter_names[];

/* Supported Functions */
void* darshan_log_open(char *);
int darshan_log_get_job(void *, struct darshan_job *);
void darshan_log_close(void*);
int darshan_log_get_exe(void*, char *);
int darshan_log_get_mounts(void*, struct darshan_mnt_info **, int*);
void darshan_log_get_modules(void*, struct darshan_mod_info **, int*);
int darshan_log_get_record(void*, int, void **);
