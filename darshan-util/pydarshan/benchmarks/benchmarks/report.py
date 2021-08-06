import importlib
import os

example_logs = importlib.import_module("examples.example-logs")
from tests.input import test_data_files_dxt
import darshan


class ModReadAllRecords:

    params = [['numpy', 'dict', 'pandas'],
              ["examples/example-logs/ior_hdf5_example.darshan",
               "examples/example-logs/dxt.darshan",
               "tests/input/sample-dxt-simple.darshan",
              ],
              ["POSIX",
               "MPI-IO",
               "STDIO",
              ],
              [True, False],
              ]
    param_names = ['dtype', 'darshan_logfile', 'mod', 'read_all']

    def setup(self, dtype, darshan_logfile, mod, read_all):
        filename = os.path.basename(darshan_logfile)
        if "examples" in darshan_logfile:
            self.logfile = example_logs.example_data_files_dxt[filename]
        else:
            self.logfile = test_data_files_dxt[filename]

        self.report = darshan.DarshanReport(self.logfile, read_all=read_all)

    def time_mod_read_all_records(self, dtype, darshan_logfile, mod, read_all):
        self.report.mod_read_all_records(mod=mod,
                                         dtype=dtype)
