#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

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


	// get name records
	

	struct darshan_name_record_info *nrecs = NULL;
	int cnt = 0;


	printf("get_name_records(): nrefs=%p, cnt=%d\n", nrecs, cnt);
	darshan_log_get_name_records(fd, &nrecs, &cnt);
	printf("get_name_records(): nrefs=%p, cnt=%d\n", nrecs, cnt);



	sleep(10);


	printf("--------------------------------------------------------------\n");


	struct darshan_name_record_info *wnrecs = NULL;
	int wcnt = 0;

	darshan_record_id whitelist[] = {123U, 15920181672442173319U, 14734109647742566553U};
	int whitelist_cnt = 3;

	printf("get_name_filtered_records(): nrefs=%p, cnt=%d\n", wnrecs, wcnt);
	darshan_log_get_filtered_name_records(fd, &wnrecs, &wcnt, whitelist, whitelist_cnt);
	printf("get_name_filtered_records(): nrefs=%p, cnt=%d\n", wnrecs, wcnt);






	//darshan_log_close(fd);

	return 0;
}
