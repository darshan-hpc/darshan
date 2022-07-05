import glob
import subprocess
import os

import darshan
import numpy as np
import pytest


def test_h5oopen_h5py_roundtrip(tmpdir):
    # regression test for gh-690
    n_ranks = 1
    root_path = os.environ.get("DARSHAN_ROOT_PATH")
    darshan_install_path = os.environ.get("DARSHAN_INSTALL_PATH")
    test_script_path = os.path.join(root_path,
                                    "darshan-test",
                                    "python_mpi_scripts",
                                    "runtime_prog_issue_690.py")
    darshan_lib_path = os.path.join(darshan_install_path,
                                    "lib",
                                    "libdarshan.so")
    hdf5_lib_path = os.environ.get("HDF5_LIB")

    with tmpdir.as_cwd():
        cwd = os.getcwd()
        subprocess.check_output(["mpirun",
                     "--allow-run-as-root",
                     "-n",
                     f"{n_ranks}",
                     "-x",
                     f"LD_PRELOAD={darshan_lib_path}:{hdf5_lib_path}",
                     "-x",
                     f"DARSHAN_LOGPATH={cwd}",
                     "python",
                     f"{test_script_path}"])

        log_file_list = glob.glob("*.darshan")
        # only a single log file should be generated
        # by darshan
        assert len(log_file_list) == 1
        path_to_log = os.path.join(cwd, log_file_list[0])
        report = darshan.DarshanReport(path_to_log)
        print("report:", report)
        h5f_df_counters = report.records["H5F"].to_df()["counters"]
        h5d_df_counters = report.records["H5D"].to_df()["counters"]

        # each rank should open an HDF5 file once for
        # reading and one for writing
        assert h5f_df_counters["H5F_OPENS"].to_numpy() == n_ranks * 2
        assert h5d_df_counters["H5D_OPENS"].to_numpy() == n_ranks * 2
        # only 1 read/write event per rank
        assert h5d_df_counters["H5D_READS"].to_numpy() == n_ranks
        assert h5d_df_counters["H5D_WRITES"].to_numpy() == n_ranks
