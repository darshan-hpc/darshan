#include <stdio.h>
#include <stdlib.h>
#include <darshan-logutils.h>





int main(int argc, char const* argv[])
{

	darshan_fd fd = darshan_log_open("example.darshan");

	
	// get modules
	/* * /
	int count;
	struct darshan_mod_info* mods = malloc(sizeof(struct darshan_mod_info));
	// darshan-logutils.h:259:void darshan_log_get_modules (darshan_fd fd, struct darshan_mod_info **mods, int* count);
	darshan_log_get_modules (fd, &mods, &count);
	printf("get_modules(): count=%d, &mod_info=%p\n", count, &mods);
	/* */


	// get stdio record
	void * buf = NULL; // important to initialize as NULL, as internal check relies on this!
	//int mod_idx = 1; // POSIX: known for this example.darshan
	int mod_idx = 7; // STDIO: known for this example.darshan
	// darshan-logutils.h:260:int darshan_log_get_record (darshan_fd fd, int mod_idx, void **buf);
	printf("get_record(): buf=%p\n", buf);
	darshan_log_get_record(fd, mod_idx, &buf);
	printf("get_record(): buf=%p\n", buf);



	// get stdio record
	void * buf2= NULL; // important to initialize as NULL, as internal check relies on this!
	printf("get_record(): buf2=%p\n", buf2);
	darshan_log_get_record(fd, mod_idx, &buf2);
	printf("get_record(): buf2=%p\n", buf2);



	// Darshan Base Record:
	//darshan-log-format.h:101:struct darshan_base_record
	//darshan-log-format.h-102-{
	//darshan-log-format.h-103-    darshan_record_id id;      // darshan-log-format.h:54:typedef uint64_t darshan_record_id;
	//darshan-log-format.h-104-    int64_t rank;
	//darshan-log-format.h-105-};

	// POSIX Record
	//darshan-util/pydarshan.py:37:struct darshan_posix_file
	//darshan-util/pydarshan.py-38-{
	//darshan-util/pydarshan.py-39-    struct darshan_base_record base_rec;
	//darshan-util/pydarshan.py-40-    int64_t counters[64];
	//darshan-util/pydarshan.py-41-    double fcounters[17];
	//darshan-util/pydarshan.py-42-};
	//struct darshan_posix_file * rec = buf;

	// STDIO Record
	//pydarshan.py:44:struct darshan_stdio_file
	//pydarshan.py-45-{
	//pydarshan.py-46-    struct darshan_base_record base_rec;
	//pydarshan.py-47-    int64_t counters[13];
	//pydarshan.py-48-    double fcounters[15];
	//pydarshan.py-49-};
	struct darshan_stdio_file * rec = (struct darshan_stdio_file *) buf;


	printf("rec=%p\n", rec);
	
	printf("rec->base_rec.id=%d\n", rec->base_rec.id);
	printf("rec->base_rec.rank=%d\n", rec->base_rec.rank);






	//darshan_log_close(fd);

	return 0;
}
