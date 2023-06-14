import os
import darshan
from darshan.log_utils import get_log_path
import pandas as pd
print(pd.__version__)
from pandas.testing import assert_frame_equal
import pytest
import re 
print(sys.path)  # Print sys.path again
import glob_feature

print("hello")
@pytest.mark.parametrize("log_name, expected_df", [
     # grow this with more logs...
     ("e3sm_io_heatmap_only.darshan",
      pd.DataFrame({"filename_glob":
                    # NOTE: usage of \\d or r"\d" for a literal backslash followed by "d"
                    ["/projects/radix-io/snyder/e3sm/can_I_out_h\\[.*]d.nc",
                     "/projects/radix-io/E3SM-IO-inputs/i_case_1344p.nc"],
                    "glob_count": [2, 1]})),
])

def test_glob_tables(tmpdir, log_name, expected_df):
    print("Current working directory:", os.getcwd())

    # test the glob table HTML outputs for various
    # log files in the logs repo (and new log files
    # that you creatively design yourself)
    log_path = get_log_path(log_name)
    print("log path is", log_path)
    with tmpdir.as_cwd():
        cwd = os.getcwd()
        # TODO: you shouldn't have a hardcoded HTML filename
        # like this...
        outfile = os.path.join(cwd, "name_record_glob_hd5f.html")
        glob_feature.main(log_path, outfile)
        actual_table = pd.read_html(outfile)[0]
        actual_table.drop("Unnamed: 0", axis=1, inplace=True)  # Drop the "Unnamed: 0" column
        print("actual table is", actual_table)
        print("expected_df is", expected_df)
        print("pandas version is", pd.__version__)
        print("log path is", log_path)
        # Compare the two DataFrames
        diff = actual_table['filename_glob'].compare(expected_df['filename_glob'])
        # Print the differences
        print(diff)
        assert_frame_equal(actual_table, expected_df)


