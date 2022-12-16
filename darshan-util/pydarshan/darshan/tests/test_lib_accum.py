from darshan.lib.accum import log_get_bytes_bandwidth
from darshan.log_utils import get_log_path

import pytest


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
    # TODO: determine if the lack of APMPI and
    # any other "add-ons" in _structdefs is a bug
    # in the control flow for `log_get_derived_metrics()`?
    ("e3sm_io_heatmap_only.darshan",
     "APMPI",
     "KeyError"),
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
    # of accumulating drived metrics with filtering
    # (i.e., for a single filename) is not tested here

    log_path = get_log_path(log_path)
    if expected_str == "RuntimeError":
        with pytest.raises(RuntimeError,
                           match=f"{mod_name} module does not support derived"):
            log_get_bytes_bandwidth(log_path=log_path,
                                    mod_name=mod_name)
    elif expected_str == "ValueError":
        with pytest.raises(ValueError,
                           match=f"{mod_name} is not in the available log"):
            log_get_bytes_bandwidth(log_path=log_path,
                                    mod_name=mod_name)
    elif expected_str == "KeyError":
        with pytest.raises(KeyError, match=f"{mod_name}"):
            log_get_bytes_bandwidth(log_path=log_path,
                                    mod_name=mod_name)
    else:
        actual_str = log_get_bytes_bandwidth(log_path=log_path,
                                             mod_name=mod_name)
        assert actual_str == expected_str
