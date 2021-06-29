import numpy as np
import pytest
import pandas as pd
from pandas.testing import assert_series_equal

import darshan
from darshan.experimental.plots import data_access_by_filesystem

@pytest.mark.parametrize("series, expected_series", [
    # a Series with a single filesystem root path
    # but the other root paths are absent
    (pd.Series([1], index=['/yellow']),
    # we expect the missing filesystem roots to get
    # added in with values of 0
    pd.Series([1, 0, 0], index=['/yellow', '/tmp', '/home'], dtype=np.float64)
    ),
    # a Series with two filesystem root paths,
    # but the other root path is absent
    (pd.Series([1, 3], index=['/yellow', '/tmp']),
    # we expect the single missing root path to get
    # added in with a value of 0
    pd.Series([1, 3, 0], index=['/yellow', '/tmp', '/home'], dtype=np.float64),
    ),
    # a Series with all filesystem root paths
    # present
    (pd.Series([1, 3, 2], index=['/yellow', '/tmp', '/home']),
    # if all root paths are already accounted for in the
    # Series, it will be just fine for plotting so can remain
    # unchanged
    pd.Series([1, 3, 2], index=['/yellow', '/tmp', '/home'], dtype=np.float64),
    ),
    # a Series with only the final filesystem root path
    (pd.Series([2], index=['/home']),
    # we expect the order of the indices to be
    # preserved from the filesystem_roots provided
    # and 0 values filled in where needed
    pd.Series([0, 0, 2], index=['/yellow', '/tmp', '/home'], dtype=np.float64),
    ),
    ])
def test_empty_series_handler(series, expected_series):
    # the empty_series_handler() function should
    # add indices for any filesystems that are missing
    # from a given Series, along with values of 0 for
    # each of those indices (i.e., no activity for that
    # missing filesystem)--this is mostly to enforce
    # consistent plotting behavior
    filesystem_roots = ['/yellow', '/tmp', '/home']
    actual_series = data_access_by_filesystem.empty_series_handler(series=series,
                                                                   filesystem_roots=filesystem_roots)

    assert_series_equal(actual_series, expected_series)

@pytest.mark.parametrize("file_path, expected_root_path", [
    ("/scratch1/scratchdirs/glock/testFile.00000046",
     "/scratch1"),
    ])
def test_convert_file_path_to_root_path(file_path, expected_root_path):
    actual_root_path = data_access_by_filesystem.convert_file_path_to_root_path(file_path=file_path)
    assert actual_root_path == expected_root_path

@pytest.mark.parametrize("input_id, file_id_dict, expected_file_path", [
    (9.457796068806373e+18,
    {210703578647777632: '/yellow/usr/projects/eap/users/treddy/simple_dxt_mpi_io_darshan/test.out.locktest.0',
     9457796068806373448: '/yellow/usr/projects/eap/users/treddy/simple_dxt_mpi_io_darshan/test.out'},
    '/yellow/usr/projects/eap/users/treddy/simple_dxt_mpi_io_darshan/test.out'
    ),
    # intentionally use an ID that is absent
    # in the dictionary
    (9.357796068806371e+18,
    {210703578647777632: '/yellow/usr/projects/eap/users/treddy/simple_dxt_mpi_io_darshan/test.out.locktest.0',
     9457796068806373448: '/yellow/usr/projects/eap/users/treddy/simple_dxt_mpi_io_darshan/test.out'},
     None
    ),
    ])
def test_convert_file_id_to_path(input_id, file_id_dict, expected_file_path):
    actual_file_path = data_access_by_filesystem.convert_file_id_to_path(input_id=input_id,
                                                                         file_id_dict=file_id_dict)
    assert actual_file_path == expected_file_path

@pytest.mark.parametrize("verbose", [True, False])
@pytest.mark.parametrize("file_id_dict, expected_root_paths", [
    ({210703578647777632: '/yellow/usr/projects/eap/users/treddy/simple_dxt_mpi_io_darshan/test.out.locktest.0',
      14388265063268455899: '/tmp/ompi.sn176.28751/jf.29186/1/test.out_cid-0-3400.sm'},
     ['/yellow', '/tmp']),
    ])
def test_identify_filesystems(capsys, file_id_dict, expected_root_paths, verbose):
    actual_root_paths = data_access_by_filesystem.identify_filesystems(file_id_dict=file_id_dict,
                                                                       verbose=verbose)
    assert actual_root_paths == expected_root_paths
    captured = capsys.readouterr()
    if verbose:
        # check that the same root paths
        # are also printed
        for root_path in actual_root_paths:
            assert root_path in captured.out
    else:
        # nothing should be printed
        assert len(captured.out) == 0

@pytest.mark.parametrize("""report,
                          file_id_dict,
                          expected_df_reads_shape,
                          expected_df_writes_shape""", [
    (darshan.DarshanReport("tests/input/sample.darshan"),
     darshan.DarshanReport("tests/input/sample.darshan").data["name_records"],
     (0, 73),
     (1, 73),
    ),
    (darshan.DarshanReport("tests/input/sample-dxt-simple.darshan"),
     darshan.DarshanReport("tests/input/sample-dxt-simple.darshan").data["name_records"],
     (0, 73),
     (2, 73),
    ),
    ])
def test_rec_to_rw_counter_dfs_with_cols(report,
                                         file_id_dict,
                                         expected_df_reads_shape,
                                         expected_df_writes_shape):
    # check basic shape expectations on the dataframes
    # produced by rec_to_rw_counter_dfs_with_cols()
    actual_df_reads, actual_df_writes = data_access_by_filesystem.rec_to_rw_counter_dfs_with_cols(report=report,
                                                                                                  file_id_dict=file_id_dict,
                                                                                                  mod='POSIX')
    assert actual_df_reads.shape == expected_df_reads_shape
    assert actual_df_writes.shape == expected_df_writes_shape

@pytest.mark.parametrize("read_groups, write_groups, filesystem_roots, expected_read_groups, expected_write_groups", [
       (pd.Series([0, 1, 7], index=['/root', '/tmp', '/yellow']),
        pd.Series([5, 5], index=['/root', '/tmp']),
        ['/root', '/tmp', '/yellow', '/usr', '/scratch1'],
        pd.Series([0, 1, 7, 0, 0], index=['/root', '/tmp', '/yellow', '/usr', '/scratch1'], dtype=np.float64),
        pd.Series([5, 5, 0, 0, 0], index=['/root', '/tmp', '/yellow', '/usr', '/scratch1'], dtype=np.float64),
        ),
        ])
def test_check_empty_series(read_groups,
                            write_groups,
                            filesystem_roots,
                            expected_read_groups,
                            expected_write_groups):
    # check that the reindex operation happened as
    # expected
    actual_read_groups, actual_write_groups = data_access_by_filesystem.check_empty_series(read_groups=read_groups,
                                                                                           write_groups=write_groups,
                                                                                           filesystem_roots=filesystem_roots)
    assert_series_equal(actual_read_groups, expected_read_groups)
    assert_series_equal(actual_write_groups, expected_write_groups)

@pytest.mark.parametrize("df_reads, df_writes, expected_read_groups, expected_write_groups", [
    (pd.DataFrame({'filesystem_root': ['/yellow', '/tmp', '/yellow'],
                   'POSIX_BYTES_READ': [3, 5, 90],
                   'POSIX_BYTES_WRITTEN': [0, 9, 0],
                   'COLUMN3': [np.nan, 5, 8],
                   'COLUMN4': ['a', 'b', 'c']}),
    pd.DataFrame({'filesystem_root': ['/yellow', '/tmp', '/tmp'],
                   'POSIX_BYTES_READ': [1, 11, 17],
                   'POSIX_BYTES_WRITTEN': [2098, 9, 20],
                   'COLUMN3': [np.nan, 5, 1],
                   'COLUMN4': ['a', 'b', 'd']}),
    pd.Series([5, 93], index=['/tmp', '/yellow'], name='POSIX_BYTES_READ'),
    pd.Series([29, 2098], index=['/tmp', '/yellow'], name='POSIX_BYTES_WRITTEN'),
            ),
        ])
def test_process_byte_counts(df_reads, df_writes, expected_read_groups, expected_write_groups):
    expected_read_groups.index = expected_read_groups.index.set_names('filesystem_root')
    expected_write_groups.index = expected_write_groups.index.set_names('filesystem_root')
    actual_read_groups, actual_write_groups = data_access_by_filesystem.process_byte_counts(df_reads=df_reads,
                                                                                            df_writes=df_writes)
    assert_series_equal(actual_read_groups, expected_read_groups)
    assert_series_equal(actual_write_groups, expected_write_groups)
