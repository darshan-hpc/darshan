import darshan
from darshan.backend import cffi_backend
from darshan.log_utils import get_log_path


class RetrieveLogData:
    # see gh-811

    # TODO: maybe produce a version of the log that also
    # has runtime HEATMAP to guard memory leaks on that module;
    # part of the issue here is
    # perhaps the following to pull in new log repo files:
    # https://github.com/darshan-hpc/darshan-logs/issues/15

    params = ['POSIX', 'MPI-IO', 'H5F', 'H5D', 'LUSTRE', 'STDIO', 'DXT_POSIX', 'DXT_MPIIO']
    param_names = ["module"]


    def setup(self, module):
        self.ior_log_path = get_log_path("ior_hdf5_example.darshan")


    def peakmem_log_get_record(self, module):
        for i in range(100000):
            log = cffi_backend.log_open(self.ior_log_path)
            rec = cffi_backend.log_get_record(log, module)
            cffi_backend.log_close(log)


    def peakmem_log_get_mounts(self, module):
        for i in range(10000):
            log = cffi_backend.log_open(self.ior_log_path)
            rec = cffi_backend.log_get_mounts(log)
            cffi_backend.log_close(log)


    def peakmem_log_get_name_recs(self, module):
        for i in range(10000):
            with darshan.DarshanReport(self.ior_log_path,
                                       read_all=True,
                                       lookup_name_records=True) as report:
                pass
