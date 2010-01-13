#
# Total Jobs using HDF5
#
select count(distinct darshan_job_surveyor.jobid) as TotalJobsUsedHDF5 from darshan_job_surveyor, darshan_file_surveyor where darshan_job_surveyor.jobid = darshan_file_surveyor.jobid and darshan_file_surveyor.hdf5_opens > 0;

#
# Total Jobs using PNetCDF
#
select count(distinct darshan_job_surveyor.jobid) as TotalJobsUsedPNet from darshan_job_surveyor, darshan_file_surveyor where darshan_job_surveyor.jobid = darshan_file_surveyor.jobid and (darshan_file_surveyor.nc_indep_opens > 0 or darshan_file_surveyor.nc_coll_opens > 0);

#
# Total Jobs using MPI-IO
#
select count(distinct darshan_job_surveyor.jobid) as TotalJobsUsedMPIO from darshan_job_surveyor, darshan_file_surveyor where darshan_job_surveyor.jobid = darshan_file_surveyor.jobid and (darshan_file_surveyor.mpi_indep_opens > 0 or darshan_file_surveyor.mpi_coll_opens > 0);

#
# Total Jobs using single-shared-file access
#
select count(distinct darshan_job_surveyor.jobid) as TotalJobsUsedSingleSharedFile from darshan_job_surveyor, darshan_file_surveyor where darshan_job_surveyor.jobid = darshan_file_surveyor.jobid and darshan_file_surveyor.rank != -1;

#
# Job Throughput
#
select darshan_job_surveyor.jobid as JobId, (((darshan_file_surveyor.bytes_written / (1024*1024)) / darshan_file_surveyor.posix_write_time) / darshan_job_surveyor.nprocs) as 'Throughput MiB/s' from darshan_job_surveyor, darshan_file_surveyor where darshan_job_surveyor.jobid = darshan_file_surveyor.jobid and darshan_job_surveyor.start_time = darshan_file_surveyor.start_time and darshan_file_surveyor.posix_write_time > 0;

