import os

import darshan
from darshan.log_utils import get_log_path, convert_to_parquet

import numpy as np
import pandas as pd
import pytest


@pytest.mark.parametrize("log_filename", [
    "noposixopens.darshan",
    "sample-badost.darshan",
    "sample-dxt-simple.darshan",
    "noposix.darshan",
    "sample-goodost.darshan",
    "sample.darshan",
    "pq_app_read_id71344_7-31-5658-2037904274838284930_55623.darshan",
    "pq_app_readAB_writeC_id71326_7-31-5658-2037904274838284930_55623.darshan",
    "pq_app_write_id71296_7-31-5657-2037904274838284930_55623.darshan",
    "pq_app_write_id71310_7-31-5657-2037904274838284930_55623.darshan",
    "pq_app_read_id71317_7-31-5657-2037904274838284930_55623.darshan",
    "pq_app_write_id71303_7-31-5657-2037904274838284930_55623.darshan",
    "dxt.darshan",
    "example.darshan",
    "ior_hdf5_example.darshan",
    "shane_macsio_id29959_5-22-32552-7035573431850780836_1590156158.darshan",
    "darshan-apmpi-2nodes-64mpi.darshan",
    "imbalanced-io.darshan",
    "nonmpi_dxt_anonymized.darshan",
    "laytonjb_test1_id28730_6-7-43012-2131301613401632697_1.darshan",
    "snyder_acme.exe_id1253318_9-27-24239-1515303144625770178_2.darshan",
    "treddy_mpi-io-test_id4373053_6-2-60198-9815401321915095332_1.darshan",
    "mpi-io-test.darshan",
    ])
def test_retrieve_both_repos(log_filename):
    # test that we retrieve an appropriately named
    # file/path from either the main or the logs
    # repo
    # NOTE: the filenames above can be updated occasionally
    # as new log files are added to the logs repo, but it
    # probably isn't crucial, except for cases where a log file
    # is purged from both repos
    actual_path = get_log_path(log_filename)
    assert log_filename in actual_path
    assert os.path.isfile(actual_path)


def test_failure_bad_logname():
    # test that an error is raised
    # for an invalid log filename used
    # in a pytest run (this test should
    # skip when logs repo is absent)
    with pytest.raises(FileNotFoundError,
                       match="could not be found"):
        get_log_path("garbage_$*(&**.darshan")


@pytest.mark.parametrize("log_filename", [
    "runtime_and_dxt_heatmaps_diagonal_write_only.darshan",
    ])
def test_parquet_convert_simple(tmp_path, log_filename):
    # crude/early stage testing for conversion from
    # in-house binary darshan log format to parquet
    # file format
    try:
        import pyarrow
    except ImportError:
        pytest.skip("pyarrow required for this test")
    log_path = get_log_path(log_filename)
    outfile = tmp_path / f"{log_filename}.gzip"
    convert_to_parquet(log_path, outfile)
    # check for appropriate data shape
    # (POSIX module only for now)
    actual_df = pd.read_parquet(outfile)
    # there are 32 active ranks, and 71
    # counter columns + 17 fcounter columns
    # for POSIX (we don't repeat rank and id)
    assert actual_df.shape == (32, 88)
    actual_dtypes = actual_df.dtypes.values
    # rank is first column and should be int64
    assert actual_dtypes[0] == np.int64
    # id is second column and should be uint64
    assert actual_dtypes[1] == np.uint64
    # next 30 columns should be int64 counters
    counter_type = np.unique(actual_dtypes[2:71])
    assert counter_type.size == 1
    assert counter_type[0] == np.int64
    # next 17 columns should float64 fcounters
    fcounter_type = np.unique(actual_dtypes[71:88])
    assert fcounter_type.size == 1
    assert fcounter_type[0] == np.float64
