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
     # grow this with more logs...
     ("e3sm_io_heatmap_only.darshan",
      pd.DataFrame({"filename_glob":
                   ["/projects/radix-io/snyder/e3sm/can_I_out_h[.*].nc",
                    "/projects/radix-io/E3SM-IO-inputs/i_case_1344p.nc"],
                    "glob_count": [2, 1]})),

      ("snyder_acme.exe_id1253318_9-27-24239-1515303144625770178_2.darshan",
      pd.DataFrame({"filename_glob":
                   ["/gpfs/mira-fs1/projects/radix-io/snyder/acme/test_F_case_cetus_dxt/run/[.*]",
                    "/projects/ccsm/inputdata/atm/cam/chem/trop_mozart_aero/emis/[.*].nc",
                    "/projects/ccsm/inputdata/atm/cam/physprops/[.*].nc",
                    "/projects/ccsm/inputdata/atm/cam/[.*].nc",
                    "/gpfs/mira-fs1/projects/radix-io/snyder/acme/test_F_case_cetus_dxt/run/test_F_case_cetus_dxt.[.*]00[.*]",
                    "/projects/ccsm/inputdata/lnd/clm2/[.*].nc",
                    "/gpfs/mira-fs1/projects/radix-io/snyder/acme/test_F_case_cetus_dxt/run/timing/[.*]i[.*]",
                    "/projects/radix-io/snyder/acme/test_F_case_cetus_dxt/run/[.*].log.170927-064246",
                    "/projects/ccsm/inputdata/atm/waccm/[.*].nc",
                    "/projects/ccsm/inputdata/[.*]n.[.*].[.*]1[.*]0[.*].nc"], #Note: for this set of grouped paths it might be more benifical to display the individual filepaths 
                    "glob_count": [22, 18, 14, 13, 10, 6, 6, 5, 3, 3]})),

     ("darshan-apmpi-2nodes-64mpi.darshan",
      pd.DataFrame({"filename_glob":
                   ["/lus/theta-fs0/projects/Performance/chunduri/MILC/milctestv2-papi-reorder-darshan/MILC_2_526820_2021-06-14-15:58:47/[.*]n[.*]"],
                    "glob_count": [2]})),

     ("mpi-io-test.darshan",
      pd.DataFrame({"filename_glob":
                   ["/global/cscratch1/sd/ssnyder/tmp/mpi-io-test.tmp.dat"],
                    "glob_count": [1]})),

     ("e3sm_io_heatmap_and_dxt.darshan",
      pd.DataFrame({"filename_glob":
                   ["/projects/radix-io/snyder/e3sm/can_I_out_h[.*].nc",
                    "/projects/radix-io/E3SM-IO-inputs/i_case_1344p.nc"],
                    "glob_count": [2, 1]})),


     ("hdf5_diagonal_write_1_byte_dxt.darshan",
      pd.DataFrame({"filename_glob":
                   ["/users/nawtrey/.conda/envs/pydarshan_hdf5_py38/lib/python3.8/site-packages/h5py/[.*]",
                    "/yellow/users/nawtrey/projects/hdf5_testing/test_files_write_1_bytes/test_[.*].h5[.*]",
                    "/users/nawtrey/.conda/envs/pydarshan_hdf5_py38/lib/python3.8/[.*]"],
                    "glob_count": [54, 20, 10]})),


     ("hdf5_diagonal_write_bytes_range_dxt.darshan",
      pd.DataFrame({"filename_glob":
                   ["/users/nawtrey/.conda/envs/pydarshan_hdf5_py38/lib/python3.8/site-packages/h5py/[.*]",
                    "/yellow/users/nawtrey/projects/hdf5_testing/test_files_write_1_bytes/test_[.*].h5[.*]",
                    "/users/nawtrey/.conda/envs/pydarshan_hdf5_py38/lib/python3.8/[.*]"],
                    "glob_count": [54, 20, 10]})),

     ("hdf5_diagonal_write_half_flush_dxt.darshan",
      pd.DataFrame({"filename_glob":
                   ["/users/nawtrey/.conda/envs/pydarshan_hdf5_py38/lib/python3.8/site-packages/h5py/[.*]",
                    "/yellow/users/nawtrey/projects/hdf5_testing/test_files_write_1_bytes/test_[.*].h5[.*]",
                    "/users/nawtrey/.conda/envs/pydarshan_hdf5_py38/lib/python3.8/[.*]"],
                    "glob_count": [54, 20, 10]})),

     ("hdf5_diagonal_write_half_ranks_dxt.darshan",
      pd.DataFrame({"filename_glob":
                   ["/users/nawtrey/.conda/envs/pydarshan_hdf5_py38/lib/python3.8/site-packages/h5py/[.*]",
                    "/yellow/users/nawtrey/projects/hdf5_testing/test_files_write_1_bytes/test_[.*].h5[.*]",
                    "/users/nawtrey/.conda/envs/pydarshan_hdf5_py38/lib/python3.8/[.*]"],
                    "glob_count": [54, 15, 10]})),

     ("hdf5_file_opens_only.darshan",
      pd.DataFrame({"filename_glob":
                   ["/users/nawtrey/.conda/envs/pydarshan_hdf5_py38/lib/python3.8/site-packages/numpy[.*]",
                    "/users/nawtrey/.conda/envs/pydarshan_hdf5_py38/lib/python3.8/[.*]",
                    "/users/nawtrey/.conda/envs/pydarshan_hdf5_py38/lib/python3.8/site-packages/h5py/[.*]",
                    "/yellow/users/nawtrey/projects/hdf5_testing/test_h5f_only_[.*].h5"],
                    "glob_count": [175, 85, 54, 3 ]})),



     ("treddy_h5d_no_h5f.darshan",
      pd.DataFrame({"filename_glob":
                   ["/home/treddy/python_virtual_envs/python_310_darshan/lib/python3.10/site-packages/h5py/_[.*].pyc",
                    "/home/treddy/rough_work/darshan/issue_709/rank_[.*].h5[.*]"],
                    "glob_count": [15, 6]})),


     ("imbalanced-io.darshan",
      pd.DataFrame({"filename_glob":
                   ["/lus/theta-fs0/[.*]",
                    "//3926523774",
                    "//1958007717",
                    "//946917208",
                    "//3186458368",
                    "//604249092",
                    "//2324418701",
                    "//2142813647",
                    "//3149983296",
                    "//1895353925",
                    "//425392719",
                    "//1053204904",
                    "//2446001947"],
                    "glob_count": [1015, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1]})),


     ("shane_ior-HDF5_id438090-438090_11-9-41522-17417065676046418211_1.darshan",
      pd.DataFrame({"filename_glob":
                   ["/home/shane/software/ior/build/testFile[.*]"],
                    "glob_count": [2]})),

     ("shane_ior-PNETCDF_id438100-438100_11-9-41525-10280033558448664385_1.darshan",
      pd.DataFrame({"filename_glob":
                   ["/home/shane/software/ior/build/testFile[.*]"],
                    "glob_count": [2]})),

     ("partial_data_stdio.darshan",
     pd.DataFrame({"filename_glob":
                  ["/home/carns/working/dbg/darshan-examples/foo[.*]",
                   "/home/carns/working/dbg/darshan-examples/test.out"],
                   "glob_count": [1021, 1]})),


# This log file contains files that are only numeric. 
# I commented them all out because I am unsure if we even want to include these files and if so do we want to group them together
#     ("nonmpi_dxt_anonymized.darshan",
#     pd.DataFrame({"filename_glob":
#                  ["//2585653418",
#                   "//3392535749",
#                   "//1750113851",
#                   "//68752815",
#                   "//155559223",
#                   "//1093384412",
#                   "//3046746762",
#                   "//2617286315",
#                   "//826480344",
#                   "//1571032323",
#                   "//4226169779",
#                   "//2418046705",
#                   "//2010395326",
#                   "//1767127016",
#                   "//4075905285",
#                   "//1067575933",
#                   "//3616928368",
#                   "//983841409",
#                   "//513688402",
#                   "//4287455549",
#                   "//2136275236",
#                   "//3097647757",
#                   "//236164485",
#                   "//1437530161",
#                   "//2689488546",
#                   "//4192870826",
#                   "//309267665",
#                   "//780646879",
#                   "//499632015",
#                   "//2507343021",
#                   "//2695660354",
#                   "//3091680351",
#                   "//3164053573",
#                   "//930552855",
#                   "//1137823565",
#                   "//2598810996",
#                   "//2330561107",
#                   "//2564488601",
#                   "//317014058",
#                   "//3342706664",
#                   "//2160565458",
#                   "//2907700500",
#                   "//2116489843",
#                   "//135439080",
#                   "//3098064231",
#                   "//2967008390",
#                   "//3067634051",
#                   "//1734260232",
#                   "//3120506952",
#                   "//642754434",
#                   "//463702723",
#                   "//1896899807",
#                   "//4260655471",
#                   "//827646422",
#                   "//942747095",
#                   "//432306240",
#                   "//583215908",
#                   "//1673153855",
#                   "//3192604617",
#                   "//3225174794",
#                   "//2990589364",
#                   "//37712466",
#                   "//2173526570",
#                   "//1117575673",
#                   "//3916290828",
#                   "//430181069",
#                   "//3645159644",
#                   "//529183092",
#                   "//3225006356",
#                   "//63288926",
#                   "//798211322",
#                   "//2256136699",
#                   "//4004231621",
#                   "//2379710227",
#                   "//3211841059",
#                   "//3397061505",
#                   "//416688243",
#                   "//1456531123"],
#                   "glob_count": [1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1]})),


     ("partial_data_dxt.darshan",
     pd.DataFrame({"filename_glob":
                  ["/home/carns/working/dbg/darshan-examples/test.out"],
                   "glob_count": [1]})),


     ("partial_data_stdio.darshan",
     pd.DataFrame({"filename_glob":
                  ["/home/carns/working/dbg/darshan-examples/foo[.*]",
                   "/home/carns/working/dbg/darshan-examples/test.out"],
                   "glob_count": [1021 ,1]})),


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
                  ["/yellow/users/treddy/github_projects/heatmap_diagonal/rank_[.*]_write_1_bytes"],
                   "glob_count": [32]})),


# This log file contains no data
     ("treddy_runtime_heatmap_inactive_ranks.darshan",
     pd.DataFrame({"filename_glob":
                  [],
                   "glob_count": []})),


     ("skew-app.darshan",
     pd.DataFrame({"filename_glob":
                  ["/lus/theta-fs0/2934391481"],
                   "glob_count": [1]})),

     ("skew-autobench-ior.darshan",
     pd.DataFrame({"filename_glob":
                  ["//1968299212",
                   "//4207382746"],
                   "glob_count": [1, 1]})),


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
        glob_feature.main(log_path, outfile)
        actual_table = pd.read_html(outfile)[0]
        print("log path is", log_path)
        print("Shape of actual table:", actual_table.shape)
        print("Shape of expected_df:", expected_df.shape)

        # Print the contents of the DataFrames
        print("Actual DataFrame:")
        print(actual_table)
        print("Expected DataFrame:")
        print(expected_df)

        # Compare the two DataFrames
        diff = actual_table['filename_glob'].compare(expected_df['filename_glob'])
        # Print the differences
        print(diff)
        assert_frame_equal(actual_table, expected_df)

