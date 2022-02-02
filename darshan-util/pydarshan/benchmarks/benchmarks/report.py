import importlib
import os

example_logs = importlib.import_module("darshan.examples.example_logs")
from darshan.tests.input import test_data_files_dxt, test_data_files
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



class ModReadAllDXTRecords:

    params = [['numpy', 'dict', 'pandas'],
              ["examples/example-logs/dxt.darshan",
               "tests/input/sample-dxt-simple.darshan",
              ],
              ["DXT_POSIX",
               "DXT_MPIIO",
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

    def time_mod_read_all_dxt_records(self, dtype, darshan_logfile, mod, read_all):
        self.report.mod_read_all_dxt_records(mod=mod,
                                             dtype=dtype)

class RecordCollectionToDF:

    params = [["examples/example-logs/ior_hdf5_example.darshan",
               "examples/example-logs/dxt.darshan",
               "tests/input/sample-dxt-simple.darshan",
               "tests/input/sample-badost.darshan",
              ],
              ["POSIX",
               "STDIO",
               "MPI-IO",
               "LUSTRE",
               "DXT_POSIX",
               "DXT_MPIIO",
               "H5D",
               "H5F",
              ],
              ["default", ["id"], ["rank"], [], None],
              ]
    param_names = ['darshan_logfile', 'mod', 'attach']

    def setup(self, darshan_logfile, mod, attach):
        filename = os.path.basename(darshan_logfile)
        if "examples" in darshan_logfile:
            self.logfile = example_logs.example_data_files_dxt[filename]
        elif "sample-badost" in darshan_logfile:
            self.logfile = test_data_files[filename]
        else:
            self.logfile = test_data_files_dxt[filename]

        report = darshan.DarshanReport(self.logfile)
        self.record_collection = report.records[mod]

    def time_record_collection_to_df(self, darshan_logfile, mod, attach):
        self.record_collection.to_df(attach=attach)
