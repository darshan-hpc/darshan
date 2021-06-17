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
