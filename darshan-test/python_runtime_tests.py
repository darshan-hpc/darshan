import glob
import subprocess
import os
import shutil

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


def test_runtime_heatmap_div_by_zero(tmpdir):
    # regression test for the runtime portion
    # of gh-730
    n_ranks = 1
    root_path = os.environ.get("DARSHAN_ROOT_PATH")
    darshan_install_path = os.environ.get("DARSHAN_INSTALL_PATH")
    test_source_path = os.path.join(root_path,
                                    "darshan-test",
                                    "c_mpi_progs",
                                    "runtime_prog_issue_730.c")
    hdf5_lib_path = os.environ.get("HDF5_LIB")

    with tmpdir.as_cwd():
        cwd = os.getcwd()
        # this test is sensitive to machine timers so we
        # intentionally use a coarser timer to facilitate
        # portable reproduction of the issue

        # recompile a new runtime with the adjustment
        shutil.copytree(os.path.dirname(root_path),
                        os.path.join(cwd),
                        dirs_exist_ok=True)

        file_list = glob.glob("*")
        filepath = os.path.join(cwd,
                                "darshan",
                                "darshan-runtime",
                                "lib",
                                "darshan.h")

        deeper_file_list = glob.glob("./darshan/*")
        with open(filepath) as infile:
            old_str = infile.read()

        with open(filepath, "w") as outfile:
            outfile.write(old_str.replace("CLOCK_REALTIME",
                                          "CLOCK_REALTIME_COARSE"))

        new_install_path = os.path.join(cwd, "darshan", "darshan_install")
        os.chdir(os.path.join("darshan", "darshan-runtime", "build"))
        myenv = os.environ.copy()
        myenv["CC"] = "mpicc"
        subprocess.check_output(["../configure",
                                 f"--prefix={new_install_path}",
                                 "--with-log-path-by-env=DARSHAN_LOGPATH",
                                 "--with-jobid-env=SLURM_JOBID"],
                                 env=myenv)
        subprocess.check_output(["make"], env=myenv)
        subprocess.check_output(["make",
                                 "install"], env=myenv)
        os.chdir(cwd)
        darshan_lib_path = os.path.join(new_install_path,
                                        "lib",
                                        "libdarshan.so")
        # compile test C program
        subprocess.check_output(["mpicc",
                                 f"{test_source_path}",
                                 "-o",
                                 "runtime_prog_issue_730"])
        test_prog_path = os.path.join(cwd,
                                      "runtime_prog_issue_730")
        subprocess.check_output(["mpirun",
                     "--allow-run-as-root",
                     "-n",
                     f"{n_ranks}",
                     "-x",
                     f"LD_PRELOAD={darshan_lib_path}",
                     "-x",
                     f"DARSHAN_LOGPATH={cwd}",
                     f"{test_prog_path}"])

        log_file_list = glob.glob("*.darshan")
        # only a single log file should be generated
        # by darshan
        assert len(log_file_list) == 1
        path_to_log = os.path.join(cwd, log_file_list[0])
        report = darshan.DarshanReport(path_to_log)
        hmap_df = report.heatmaps["POSIX"].to_df(ops=["read", "write"])
        assert (hmap_df < -1).to_numpy().sum() == 0
