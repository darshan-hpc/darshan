#include <stdio.h>
#include "darshan-log-format.h"

int main(int argc, char **argv) 
{
    printf("version: %s\n", CP_VERSION);
    printf("CP_JOB_RECORD_SIZE: %d\n", CP_JOB_RECORD_SIZE);
    printf("CP_EXE_LEN: %zu\n", CP_EXE_LEN);
    printf("CP_FILE_RECORD_SIZE: %zu\n", CP_FILE_RECORD_SIZE);
    printf("CP_NAME_SUFFIX_LEN: %d\n", CP_NAME_SUFFIX_LEN);
    printf("CP_NUM_INDICES: %d\n", CP_NUM_INDICES);
    printf("CP_F_NUM_INDICES: %d\n", CP_F_NUM_INDICES);
    return(0);
}
