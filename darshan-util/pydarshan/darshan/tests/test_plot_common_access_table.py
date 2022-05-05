import pytest

import pandas as pd
from pandas.testing import assert_frame_equal

import darshan
from darshan.experimental.plots import plot_common_access_table
from darshan.log_utils import get_log_path


@pytest.mark.parametrize("filename, mod, expected_df",
    [
        (
            "ior_hdf5_example.darshan",
            "POSIX",
            # values from the old report (Perl) code
            pd.DataFrame([[262144, 32], [512, 9], [544, 5], [328, 3]]),
        ),
        (
            "ior_hdf5_example.darshan",
            "MPI-IO",
            # values from the old report (Perl) code
            pd.DataFrame([[262144, 32], [512, 9], [544, 5], [328, 3]]),
        ),
        (
            "ior_hdf5_example.darshan",
            "H5D",
            pd.DataFrame([[262144, 24]]),
        ),
        (
            "nonmpi_dxt_anonymized.darshan",
            "POSIX",
            # values from the old report (Perl) code
            pd.DataFrame([[1024, 7692], [32, 276], [100, 269], [92, 265]]),
        ),
        (
            "imbalanced-io.darshan",
            "POSIX",
            # values from the old report (Perl) code
            pd.DataFrame([[1048576, 100968], [8162, 14664], [512, 1003], [328, 545]]),
        ),
        (
            "imbalanced-io.darshan",
            "MPI-IO",
            # values from the old report (Perl) code
            pd.DataFrame([[241664, 71400], [294912, 18806], [483328, 4862], [512, 1003]]),
        ),
        (
            # "ground truth" log where 10 ranks wrote 1 byte each
            "hdf5_diagonal_write_1_byte_dxt.darshan",
            "H5D",
            pd.DataFrame([[1, 10]]),
        ),
        (
            # "ground truth" log where 10 ranks wrote `10 * (1 + rank)` bytes
            # each (e.g. 10 bytes for rank 0, 20 bytes for rank 1, etc.)
            "hdf5_diagonal_write_bytes_range_dxt.darshan",
            "H5D",
            pd.DataFrame(
                [[10, 1], [20, 1], [30, 1], [40, 1], [50, 1],
                [60, 1], [70, 1], [80, 1], [90, 1], [100, 1]],
            ),
        ),
        (
            # "ground truth" log where 10 ranks wrote 1 byte each and
            # 5 of the ranks called the `flush` operation. Results
            # should be the same as `hdf5_diagonal_write_1_byte_dxt.darshan`
            "hdf5_diagonal_write_half_flush_dxt.darshan",
            "H5D",
            pd.DataFrame([[1, 10]]),
        ),
        (
            # "ground truth" log where 5 ranks wrote 1 byte each
            "hdf5_diagonal_write_half_ranks_dxt.darshan",
            "H5D",
            pd.DataFrame([[1, 5]]),
        ),
    ],
)
def test_common_access_table(filename, mod, expected_df):
    log_path = get_log_path(filename=filename)
    expected_df.columns = ["Access Size", "Count"]
    report = darshan.DarshanReport(log_path)
    # collect the number of rows from the expected dataframe
    n_rows = expected_df.shape[0]
    actual_df = plot_common_access_table.plot_common_access_table(report=report, mod=mod, n_rows=n_rows).df
    assert_frame_equal(actual_df, expected_df)


@pytest.mark.parametrize("func, input_df, expected_df",
    [
        # based on `ior_hdf5_example.darshan` `H5D` module data
        (
            plot_common_access_table.remove_nonzero_rows,
            pd.DataFrame([[262144, 8], [262144, 8], [262144, 8], [0, 0], [0, 0]]),
            pd.DataFrame([[262144, 8], [262144, 8], [262144, 8]]),
        ),
        # check that single zeros in either column remain
        (
            plot_common_access_table.remove_nonzero_rows,
            pd.DataFrame([[262144, 8], [262144, 8], [262144, 8], [1, 0], [0, 1]]),
            pd.DataFrame([[262144, 8], [262144, 8], [262144, 8], [1, 0], [0, 1]]),
        ),
        # based on `ior_hdf5_example.darshan` `H5D` module data
        (
            plot_common_access_table.combine_access_sizes,
            pd.DataFrame([[262144, 8], [262144, 8], [262144, 8]]),
            pd.DataFrame([[262144, 24]]),
        ),
        # synthetic case with multiple identical access sizes
        (
            plot_common_access_table.combine_access_sizes,
            pd.DataFrame([[10, 1], [10, 2], [20, 1], [20, 2], [20, 3]]),
            pd.DataFrame([[10, 3], [20, 6]]),
        ),
        # based on `ior_hdf5_example.darshan` `POSIX` module data
        (
            plot_common_access_table.get_most_common_access_sizes,
            pd.DataFrame([[544, 5], [512, 9], [262144, 32], [328, 3]]),
            pd.DataFrame([[262144, 32], [512, 9], [544, 5], [328, 3]]),
        ),
        # synthetic case with > 4 access sizes
        (
            plot_common_access_table.get_most_common_access_sizes,
            pd.DataFrame([[1, 1], [2, 10], [3, 4], [4, 9], [5, 5], [6, 2], [7, 3]]),
            pd.DataFrame([[2, 10], [4, 9], [5, 5], [3, 4]]),
        ),
        # case where there are < 4 access sizes, based on
        # `ior_hdf5_example.darshan` `H5D` module data
        (
            plot_common_access_table.get_most_common_access_sizes,
            pd.DataFrame([[262144, 24]]),
            pd.DataFrame([[262144, 24]]),
        ),
    ]
)
def test_misc_funcs(func, input_df, expected_df):
    # tests functions that make slight modifications to dataframes
    input_df.columns = ["Access Size", "Count"]
    expected_df.columns = ["Access Size", "Count"]
    actual_df = func(df=input_df)
    assert_frame_equal(actual_df, expected_df)


@pytest.mark.parametrize("input_df, col_name, expected_df",
    [
        # based on `ior_hdf5_example.darshan` `POSIX` module data
        (
            pd.DataFrame(
                data=[[262144, 512, 544, 328]],
                columns=[
                    "POSIX_ACCESS1_ACCESS", "POSIX_ACCESS2_ACCESS",
                    "POSIX_ACCESS3_ACCESS", "POSIX_ACCESS4_ACCESS",
                ],
            ),
            "Access Size",
            pd.DataFrame(
                data=[[262144], [512], [544], [328]],
                columns=["Access Size"],
            ),
        ),
        # based on `ior_hdf5_example.darshan` `POSIX` module data
        (
            pd.DataFrame(
                data=[[32, 9, 5, 3]],
                columns=[
                    "POSIX_ACCESS1_COUNT", "POSIX_ACCESS2_COUNT",
                    "POSIX_ACCESS3_COUNT", "POSIX_ACCESS4_COUNT",
                ],
            ),
            "Count",
            pd.DataFrame(
                data=[[32], [9], [5], [3]],
                columns=["Count"],
            ),
        ),
        # synthetic case to test multiple rows and columns
        (
            pd.DataFrame(
                data=[[1, 4, 7, 10], [2, 5, 8, 11], [3, 6, 9, 12]],
                columns=["col1", "col2", "col3", "col4"],
            ),
            "TEST",
            pd.DataFrame(
                data=[[i] for i in range(1, 13)],
                columns=["TEST"],
            ),
        ),
    ]
)
def test_collapse_access_cols(input_df, col_name, expected_df):
    actual_df = plot_common_access_table.collapse_access_cols(df=input_df, col_name=col_name)
    assert_frame_equal(actual_df, expected_df)


@pytest.mark.parametrize("mod_df, mod, expected_df",
    [
        (
            pd.DataFrame(
                data=[
                    [17, 178, 356, 890, 0, 35, 15, 1, 0],
                    [17, 192, 13266432, 13284480, 96, 7688, 3, 2, 2],
                    [17, 128, 8192, 16384, 0, 50, 15, 1, 0],
                ],
                columns=[
                    "POSIX_OPENS",
                    "POSIX_ACCESS1_ACCESS", "POSIX_ACCESS2_ACCESS",
                    "POSIX_ACCESS3_ACCESS", "POSIX_ACCESS4_ACCESS",
                    "POSIX_ACCESS1_COUNT", "POSIX_ACCESS2_COUNT",
                    "POSIX_ACCESS3_COUNT", "POSIX_ACCESS4_COUNT",
                ],
            ),
            "POSIX",
            pd.DataFrame(
                data=[
                    [178, 35],
                    [192, 7688],
                    [128, 50],
                    [356, 15],
                    [13266432, 3],
                    [8192, 15],
                    [890, 1],
                    [13284480, 2],
                    [16384, 1],
                    [0, 0],
                    [96, 2],
                    [0, 0],
                ],
                columns=["Access Size", "Count"],
            ),
        ),
    ]
)
def test_get_access_count_df(mod_df, mod, expected_df):
    actual_df = plot_common_access_table.get_access_count_df(mod_df=mod_df, mod=mod)
    assert_frame_equal(actual_df, expected_df)
