#include <stdio.h>
#include "darshan-log-format.h"
#include "darshan-apmpi-log-format.h"

int main (int argc, char **argv)
{
   printf ("APMPI_NUM_INDICES = %d\n", APMPI_NUM_INDICES);
   printf ("APMPI_F_NUM_INDICES = %d\n", APMPI_F_MPIOP_TOTALTIME_NUM_INDICES);
   printf ("APMPI_F_SYNC_NUM_INDICES = %d\n", APMPI_F_MPIOP_SYNCTIME_NUM_INDICES);
   printf ("APMPI_F_GLOBAL_NUM_INDICES = %d\n", APMPI_F_MPI_GLOBAL_NUM_INDICES);
   printf ("sizeof darshan_apmpi_header_record = %d\n", sizeof(struct darshan_apmpi_header_record));
   printf ("sizeof darshan_apmpi_perf_record = %d\n", sizeof(struct darshan_apmpi_perf_record));

   return 0;
}
