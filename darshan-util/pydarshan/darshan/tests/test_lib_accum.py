import darshan
from darshan.backend.cffi_backend import log_get_derived_metrics
from darshan.lib.accum import log_get_bytes_bandwidth, log_file_count_summary_table
from darshan.log_utils import get_log_path

import pytest
import pandas as pd
from pandas.testing import assert_frame_equal

@pytest.mark.parametrize("log_path, mod_name, expected_str", [
    # the expected bytes/bandwidth strings are pasted
    # directly from the old perl summary reports;
    # exceptions noted below
    # in some cases we defer to darshan-parser for the expected
    # values; see discussion in gh-839
    ("imbalanced-io.darshan",
     "STDIO",
     "I/O performance estimate (at the STDIO layer): transferred 1.1 MiB at 0.01 MiB/s"),
    ("imbalanced-io.darshan",
     "MPI-IO",
     "I/O performance estimate (at the MPI-IO layer): transferred 126326.8 MiB at 101.58 MiB/s"),
    # imbalanced-io.darshan does have LUSTRE data,
    # but it doesn't support derived metrics at time
    # of writing
    ("imbalanced-io.darshan",
     "LUSTRE",
     "RuntimeError"),
    # APMPI doesn't support derived metrics either
    ("e3sm_io_heatmap_only.darshan",
     "APMPI",
     "RuntimeError"),
    ("imbalanced-io.darshan",
     "POSIX",
     "I/O performance estimate (at the POSIX layer): transferred 101785.8 MiB at 164.99 MiB/s"),
    ("laytonjb_test1_id28730_6-7-43012-2131301613401632697_1.darshan",
     "STDIO",
     "I/O performance estimate (at the STDIO layer): transferred 0.0 MiB at 4.22 MiB/s"),
    ("runtime_and_dxt_heatmaps_diagonal_write_only.darshan",
     "POSIX",
     "I/O performance estimate (at the POSIX layer): transferred 0.0 MiB at 0.02 MiB/s"),
    ("treddy_mpi-io-test_id4373053_6-2-60198-9815401321915095332_1.darshan",
     "STDIO",
     "I/O performance estimate (at the STDIO layer): transferred 0.0 MiB at 16.47 MiB/s"),
    ("e3sm_io_heatmap_only.darshan",
     "STDIO",
     "I/O performance estimate (at the STDIO layer): transferred 0.0 MiB at 3.26 MiB/s"),
    ("e3sm_io_heatmap_only.darshan",
     "MPI-IO",
     "I/O performance estimate (at the MPI-IO layer): transferred 73880.2 MiB at 105.69 MiB/s"),
    ("partial_data_stdio.darshan",
     "MPI-IO",
     "I/O performance estimate (at the MPI-IO layer): transferred 32.0 MiB at 2317.98 MiB/s"),
    ("partial_data_stdio.darshan",
     "STDIO",
     "I/O performance estimate (at the STDIO layer): transferred 16336.0 MiB at 2999.14 MiB/s"),
    # the C derived metrics code can't distinguish
    # between different kinds of errors at this time,
    # but we can still intercept in some cases...
    ("partial_data_stdio.darshan",
     "GARBAGE",
     "ValueError"),
    ("skew-app.darshan",
     "POSIX",
     "I/O performance estimate (at the POSIX layer): transferred 41615.8 MiB at 157.49 MiB/s"),
    ("skew-app.darshan",
     "MPI-IO",
     "I/O performance estimate (at the MPI-IO layer): transferred 41615.8 MiB at 55.22 MiB/s"),
])
def test_derived_metrics_bytes_and_bandwidth(log_path, mod_name, expected_str):
    # test the basic scenario of retrieving
    # the total data transferred and bandwidth
    # for all records in a given module; the situation
    # of accumulating derived metrics with filtering
    # (i.e., for a single filename) is not tested here

    log_path = get_log_path(log_path)
    with darshan.DarshanReport(log_path, read_all=True) as report:
        if expected_str == "ValueError":
            with pytest.raises(ValueError,
                               match=f"mod {mod_name} is not available"):
                report.mod_read_all_records(mod_name, dtype="pandas")
        else:
            report.mod_read_all_records(mod_name, dtype="pandas")
            rec_dict = report.records[mod_name][0]
            nprocs = report.metadata['job']['nprocs']

            if expected_str == "RuntimeError":
                with pytest.raises(RuntimeError,
                                   match=f"{mod_name} module does not support derived"):
                    log_get_derived_metrics(rec_dict, mod_name, nprocs)
            else:
                derived_metrics = log_get_derived_metrics(rec_dict, mod_name, nprocs)
                actual_str = log_get_bytes_bandwidth(derived_metrics=derived_metrics,
                                                     mod_name=mod_name)
                assert actual_str == expected_str


@pytest.mark.parametrize("log_name, mod_name, expected", [
    # we try to match the "File Count Summary"
    # tables from the old Perl reports, but
    # expected values for file counts
    # are from darshan-parser --file
    # because of issues like gh-867

    # this also means that the average size
    # column on the old Perl reports cannot always
    # be relied upon, since that is calculated
    # using the file counts; furthermore,
    # total_max_offset_bytes is not printed by
    # darshan-parser --file, so the avg size column
    # is not checked quite as robustly as file count
    # and max size, though in cases where the Perl
    # report happens to match the file count, it does
    # seem to match

    # futhermore, the old Perl report doesn't print out
    # the file count summary table for all modules, for
    # example often only showing for POSIX, so in those
    # cases we really just verify the file count and
    # the other columns are regression guards against
    # what we currently have (max size may be available
    # in a subset of these cases as well)
    ("e3sm_io_heatmap_only.darshan",
     "POSIX",
     # <file count> <avg size> <max size>
     [[3, "99.74 GiB", "297.71 GiB"],
      [1, "11.18 MiB", "11.18 MiB"],
      [2, "149.60 GiB", "297.71 GiB"],
      [0, "0", "0"]],
    ),
    ("e3sm_io_heatmap_only.darshan",
     "MPI-IO",
     [[3, "0", "0"],
      [1, "0", "0"],
      [2, "0", "0"],
      [0, "0", "0"]],
    ),
    ("e3sm_io_heatmap_only.darshan",
     "STDIO",
     [[1, "5.80 KiB", "5.80 KiB"],
      [0, "0", "0"],
      [1, "5.80 KiB", "5.80 KiB"],
      [0, "0", "0"]],
    ),
    # the Perl report only gets a very
    # small fraction of these values correct;
    # rely on the parser a bit more here; perhaps
    # because of partial data, etc.
    ("imbalanced-io.darshan",
     "POSIX",
     [[1026, "73.96 MiB", "49.30 GiB"],
      [12, "67.73 MiB", "549.32 MiB"],
      [2, "12.00 GiB", "22.63 GiB"],
      [1, "49.30 GiB", "49.30 GiB"]],
    ),
    ("imbalanced-io.darshan",
     "MPI-IO",
     [[3, "0", "0"],
      [0, "0", "0"],
      [2, "0", "0"],
      [1, "0", "0"]],
    ),
    ("imbalanced-io.darshan",
     "STDIO",
     [[12, "93.12 KiB", "964.00 KiB"],
      [1, "1.81 KiB", "1.81 KiB"],
      [10, "111.56 KiB", "964.00 KiB"],
      [0, "0", "0"]],
    ),
    ("snyder_acme.exe_id1253318_9-27-24239-1515303144625770178_2.darshan",
     "POSIX",
     [[100, "1.84 GiB", "100.00 GiB"],
      [73, "514.56 MiB", "13.84 GiB"],
      [19, "66.86 MiB", "1.23 GiB"],
      [8, "18.30 GiB", "100.00 GiB"]],
    ),
    ("snyder_acme.exe_id1253318_9-27-24239-1515303144625770178_2.darshan",
     "MPI-IO",
     [[59, "0", "0"],
      [50, "0", "0"],
      [9, "0", "0"],
      [0, "0", "0"]],
    ),
    ("snyder_acme.exe_id1253318_9-27-24239-1515303144625770178_2.darshan",
     "STDIO",
     [[16, "81.21 KiB", "524.37 KiB"],
      [9, "4 Bytes", "4 Bytes"],
      [7, "185.62 KiB", "524.37 KiB"],
      [0, "0", "0"]],
    ),
])
def test_file_count_summary_table(log_name,
                                  mod_name,
                                  expected):
    expected_df = pd.DataFrame(expected)
    expected_df.columns = ["number of files",
                           "avg. size",
                           "max size"]
    # the team decided that we should exclude
    # "created" files row from the old report because
    # we can't really determine it reliably
    expected_df.index = ["total files",
                         "read-only files",
                         "write-only files",
                         "read/write files"]
    expected_df.index.rename('type', inplace=True)

    log_path = get_log_path(log_name)
    with darshan.DarshanReport(log_path, read_all=True) as report:
        rec_dict = report.records[mod_name].to_df()
        nprocs = report.metadata['job']['nprocs']

    derived_metrics = log_get_derived_metrics(rec_dict, mod_name, nprocs)

    actual_df = log_file_count_summary_table(derived_metrics=derived_metrics,
                                             mod_name=mod_name).df
    assert_frame_equal(actual_df, expected_df)
