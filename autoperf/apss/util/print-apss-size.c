#include <stdio.h>
#include "darshan-log-format.h"
#include "darshan-apss-log-format.h"

int main (int argc, char **argv)
{
   printf ("APSS_NUM_INDICES = %d\n", APSS_NUM_INDICES);
   printf ("sizeof darshan_apss_header_record = %d\n", sizeof(struct darshan_apss_header_record));
   printf ("sizeof darshan_apss_perf_record = %d\n", sizeof(struct darshan_apss_perf_record));

   return 0;
}
