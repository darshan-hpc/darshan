from darshan.log_utils import get_log_path
from darshan.lib.accum import log_file_count_summary_table

import pytest
import pandas as pd
from pandas.testing import assert_frame_equal

@pytest.mark.parametrize("log_name, module, expected", [
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
     [[3, "100 GiB", "298 GiB"],
      [1, "12 MiB", "12 MiB"],
      [2, "150 GiB", "298 GiB"],
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
     [[1, "6 KiB", "6 KiB"],
      [0, "0", "0"],
      [1, "6 KiB", "6 KiB"],
      [0, "0", "0"]],
    ),
    # the Perl report only gets a very
    # small fraction of these values correct;
    # rely on the parser a bit more here; perhaps
    # because of partial data, etc.
    ("imbalanced-io.darshan",
     "POSIX",
     [[1026, "74 MiB", "50 GiB"],
      [12, "68 MiB", "550 MiB"],
      [2, "12 GiB", "23 GiB"],
      [1, "50 GiB", "50 GiB"]],
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
     [[12, "94 KiB", "964 KiB"],
      [1, "2 KiB", "2 KiB"],
      [10, "112 KiB", "964 KiB"],
      [0, "0", "0"]],
    ),
    ("snyder_acme.exe_id1253318_9-27-24239-1515303144625770178_2.darshan",
     "POSIX",
     [[100, "2 GiB", "100 GiB"],
      [73, "515 MiB", "14 GiB"],
      [19, "67 MiB", "2 GiB"],
      [8, "19 GiB", "100 GiB"]],
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
     [[16, "82 KiB", "525 KiB"],
      [9, "4 Bytes", "4 Bytes"],
      [7, "186 KiB", "525 KiB"],
      [0, "0", "0"]],
    ),
])
def test_file_count_summary_table(log_name,
                                  module,
                                  expected):
    expected_df = pd.DataFrame(expected)
    expected_df.columns = ["number of files",
                           "avg. size",
                           "max size"]
    # the team decided that we should exclude
    # "created" files row from the old report because
    # we can't really determine it reliably
    expected_df.index = ["total opened",
                         "read-only files",
                         "write-only files",
                         "read/write files"]
    expected_df.index.rename('type', inplace=True)

    log_path = get_log_path(log_name)
    actual_df = log_file_count_summary_table(log_path=log_path,
                                             module=module).df
    assert_frame_equal(actual_df, expected_df)

