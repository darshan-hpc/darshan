# Note: Some tests may currently fail, as this script is still under active development.
# The log files here are from the the darshan-logs repository
import sys
import os
import darshan
from darshan.log_utils import get_log_path
import pandas as pd
from pandas.testing import assert_frame_equal
import pytest
import re
print(sys.path)
from darshan.glob_feature import glob_feature

@pytest.mark.parametrize("log_name, expected_df", [
     ("e3sm_io_heatmap_only.darshan",
      pd.DataFrame({"filename_glob":
                   ["/projects/radix-io/snyder/e3sm/can_I_out_h(.*).nc",
                    "/projects/radix-io/E3SM-IO-inputs/i_case_1344p.nc"],
                    "glob_count": [2, 1]})),


      ("snyder_acme.exe_id1253318_9-27-24239-1515303144625770178_2.darshan",
      pd.DataFrame({"filename_glob":
                   ["/projects/ccsm/inputdata/atm/cam/chem/trop_mozart_aero/emis/(.*).nc",
                    "/gpfs/mira-fs1/projects/radix-io/snyder/acme/test_F_case_cetus_dxt/run/(.*)",
                    "/projects/ccsm/inputdata/atm/cam/physprops/(.*).nc",
                    "/projects/ccsm/inputdata/atm/cam/(.*).nc",
                    "/gpfs/mira-fs1/projects/radix-io/snyder/acme/test_F_case_cetus_dxt/run/(.*).nml",
                    "/gpfs/mira-fs1/projects/radix-io/snyder/acme/test_F_case_cetus_dxt/run/test_F_case_cetus_dxt.c(.*).nc",
                    "/projects/ccsm/inputdata/lnd/clm2/(.*).nc",
                    "/gpfs/mira-fs1/projects/radix-io/snyder/acme/test_F_case_cetus_dxt/run/(.*)",
                    "/projects/radix-io/snyder/acme/test_F_case_cetus_dxt/run/(.*).170927-064246",
                    "/projects/ccsm/inputdata/atm/waccm/(.*).nc",
                    "/projects/ccsm/inputdata/(.*).nc"],
                    "glob_count": [18, 14, 14, 13, 9, 9, 6, 6, 5, 3, 3]})),


     ("darshan-apmpi-2nodes-64mpi.darshan",
      pd.DataFrame({"filename_glob":
                   ["/lus/theta-fs0/projects/Performance/chunduri/MILC/milctestv2-papi-reorder-darshan/MILC_2_526820_2021-06-14-15:58:47/(.*)"],
                    "glob_count": [2]})),


     ("mpi-io-test.darshan",
      pd.DataFrame({"filename_glob":
                   ["/global/cscratch1/sd/ssnyder/tmp/mpi-io-test.tmp.dat"],
                    "glob_count": [1]})),


     ("e3sm_io_heatmap_and_dxt.darshan",
      pd.DataFrame({"filename_glob":
                   ["/projects/radix-io/snyder/e3sm/can_I_out_h(.*).nc",
                    "/projects/radix-io/E3SM-IO-inputs/i_case_1344p.nc"],
                    "glob_count": [2, 1]})),


     ("hdf5_diagonal_write_1_byte_dxt.darshan",
      pd.DataFrame({"filename_glob":
                   ["/users/nawtrey/.conda/envs/pydarshan_hdf5_py38/lib/python3.8/site-packages/h5py/(.*).so",
                    "/users/nawtrey/.conda/envs/pydarshan_hdf5_py38/lib/python3.8/(.*).py",
                    "/users/nawtrey/.conda/envs/pydarshan_hdf5_py38/lib/python3.8/(.*).pyc",
                    "/yellow/users/nawtrey/projects/hdf5_testing/test_files_write_1_bytes/test_(.*).h5",
                    "/yellow/users/nawtrey/projects/hdf5_testing/test_files_write_1_bytes/test_(.*)"],
                    "glob_count": [24, 20, 20, 10, 10]})),

     ("hdf5_diagonal_write_bytes_range_dxt.darshan",
      pd.DataFrame({"filename_glob":
                   ["/users/nawtrey/.conda/envs/pydarshan_hdf5_py38/lib/python3.8/site-packages/h5py/(.*).so",
                    "/users/nawtrey/.conda/envs/pydarshan_hdf5_py38/lib/python3.8/(.*).py",
                    "/users/nawtrey/.conda/envs/pydarshan_hdf5_py38/lib/python3.8/(.*).pyc",
                    "/yellow/users/nawtrey/projects/hdf5_testing/test_files_write_1_bytes/test_(.*).h5",
                    "/yellow/users/nawtrey/projects/hdf5_testing/test_files_write_1_bytes/test_(.*)"],
                    "glob_count": [24, 20, 20, 10, 10]})),

     ("hdf5_diagonal_write_half_flush_dxt.darshan",
      pd.DataFrame({"filename_glob":
                   ["/users/nawtrey/.conda/envs/pydarshan_hdf5_py38/lib/python3.8/site-packages/h5py/(.*).so",
                    "/users/nawtrey/.conda/envs/pydarshan_hdf5_py38/lib/python3.8/(.*).py",
                    "/users/nawtrey/.conda/envs/pydarshan_hdf5_py38/lib/python3.8/(.*).pyc",
                    "/yellow/users/nawtrey/projects/hdf5_testing/test_files_write_1_bytes/test_(.*).h5",
                    "/yellow/users/nawtrey/projects/hdf5_testing/test_files_write_1_bytes/test_(.*)"],
                    "glob_count": [24, 20, 20, 10, 10]})),

     ("hdf5_diagonal_write_half_ranks_dxt.darshan",
      pd.DataFrame({"filename_glob":
                   ["/users/nawtrey/.conda/envs/pydarshan_hdf5_py38/lib/python3.8/site-packages/h5py/(.*).so",
                    "/users/nawtrey/.conda/envs/pydarshan_hdf5_py38/lib/python3.8/(.*).py",
                    "/users/nawtrey/.conda/envs/pydarshan_hdf5_py38/lib/python3.8/(.*).pyc",
                    "/yellow/users/nawtrey/projects/hdf5_testing/test_files_write_1_bytes/test_(.*).h5",
                    "/yellow/users/nawtrey/projects/hdf5_testing/test_files_write_1_bytes/test_(.*)"],
                    "glob_count": [24, 20, 20, 10, 10]})),

     ("hdf5_file_opens_only.darshan",
      pd.DataFrame({"filename_glob":
                   ["/users/nawtrey/.conda/envs/pydarshan_hdf5_py38/lib/python3.8/site-packages/numpy/(.*)",
                    "/users/nawtrey/.conda/envs/pydarshan_hdf5_py38/lib/python3.8/(.*).py",
                    "/users/nawtrey/.conda/envs/pydarshan_hdf5_py38/lib/python3.8/__pycache__/(.*).pyc",
                    "/users/nawtrey/.conda/envs/pydarshan_hdf5_py38/lib/python3.8/site-packages/(.*).so",
                    "/users/nawtrey/.conda/envs/pydarshan_hdf5_py38/lib/python3.8/site-packages/h5py(.*)",
                    "/users/nawtrey/.conda/envs/pydarshan_hdf5_py38/lib/python3.8/lib-dynload/(.*).so",
                    "/users/nawtrey/.conda/envs/pydarshan_hdf5_py38/lib/python3.8/(.*).pyc",
                    "/users/nawtrey/.conda/envs/pydarshan_hdf5_py38/lib/python3.8/json/(.*)",
                    "/users/nawtrey/.conda/envs/pydarshan_hdf5_py38/lib/python3.8/site-packages/(.*).py",
                    "/users/nawtrey/.conda/envs/pydarshan_hdf5_py38/lib/python3.8/site-packages/(.*).pyc",
                    "/users/nawtrey/.conda/envs/pydarshan_hdf5_py38/lib/python3.8/importlib/(.*)",
                    "/users/nawtrey/.conda/envs/pydarshan_hdf5_py38/lib/python3.8/ctypes",
                    "/yellow/users/nawtrey/projects/hdf5_testing/test_h5f_only_(.*).h5"],
                                    "glob_count": [140, 62, 47, 37, 22, 17, 15, 8, 6, 6, 4, 4, 3]})),


     ("treddy_h5d_no_h5f.darshan",
      pd.DataFrame({"filename_glob":
                   ["/home/treddy/python_virtual_envs/python_310_darshan/lib/python3.10/site-packages/h5py/_(.*).pyc",
                    "/home/treddy/rough_work/darshan/issue_709/rank_(.*)"],
                    "glob_count": [15, 6]})),


     ("shane_ior-HDF5_id438090-438090_11-9-41522-17417065676046418211_1.darshan",
      pd.DataFrame({"filename_glob":
                   ["/home/shane/software/ior/build/testFile(.*)"],
                    "glob_count": [2]})),

     ("shane_ior-PNETCDF_id438100-438100_11-9-41525-10280033558448664385_1.darshan",
      pd.DataFrame({"filename_glob":
                   ["/home/shane/software/ior/build/testFile(.*)"],
                    "glob_count": [2]})),


     ("partial_data_stdio.darshan",
     pd.DataFrame({"filename_glob":
                  ["/home/carns/working/dbg/darshan-examples/foo(.*)",
                   "/home/carns/working/dbg/darshan-examples/test.out"],
                   "glob_count": [1021, 1]})),


     ("partial_data_dxt.darshan",
     pd.DataFrame({"filename_glob":
                  ["/home/carns/working/dbg/darshan-examples/test.out"],
                   "glob_count": [1]})),


     ("mpi-io-test-ppc64-3.0.0.darshan",
     pd.DataFrame({"filename_glob":
                  ["/gpfs/mira-fs0/projects/SSSPPg/snyder/tmp/mpi-io-test.tmp.dat"],
                   "glob_count": [1]})),

     ("mpi-io-test-x86_64-3.0.0.darshan",
     pd.DataFrame({"filename_glob":
                  ["/tmp/tmp/mpi-io-test.tmp.dat"],
                   "glob_count": [1]})),

     ("mpi-io-test-x86_64-3.4.0-pre1.darshan",
     pd.DataFrame({"filename_glob":
                  ["/tmp/test/mpi-io-test.tmp.dat"],
                   "glob_count": [1]})),


     ("runtime_and_dxt_heatmaps_diagonal_write_only.darshan",
     pd.DataFrame({"filename_glob":
                  ["/yellow/users/treddy/github_projects/heatmap_diagonal/rank_(.*)_write_1_bytes"],
                   "glob_count": [32]})),


     ("laytonjb_test1_id28730_6-7-43012-2131301613401632697_1.darshan",
     pd.DataFrame({"filename_glob":
                  ["/home/laytonjb/PROJECTS/DARSHAN/TEST/jeff.txt"],
                   "glob_count": [1]})),
])


def test_glob_tables(tmpdir, log_name, expected_df):
    print("Current working directory:", os.getcwd())
    log_path = get_log_path(log_name)
    print("log path is", log_path)
    with tmpdir.as_cwd():
        cwd = os.getcwd()
        outfile = os.path.join(cwd, "output.html")
        glob_feature.main(log_path, outfile, verbose=False)
        actual_table = pd.read_html(outfile)[0]
        print("log path is", log_path)

        # Print the contents of the DataFrames
        print("Actual DataFrame:")
        print(actual_table)
        print("Expected DataFrame:")
        print(expected_df)

        assert_frame_equal(actual_table, expected_df)
