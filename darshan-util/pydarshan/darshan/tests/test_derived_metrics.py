import darshan
from darshan.log_utils import get_log_path
from darshan import derived_metrics

import pytest


@pytest.mark.parametrize("log_name, module, expected", [
        # expected strings are copy-pasted from the old
        # perl reports
        ("imbalanced-io.darshan",
         "STDIO",
         "I/O performance estimate (at the STDIO layer): transferred 1.1 MiB at 0.01 MiB/s"),
        ("laytonjb_test1_id28730_6-7-43012-2131301613401632697_1.darshan",
         "STDIO",
         "I/O performance estimate (at the STDIO layer): transferred 0.0 MiB at 4.22 MiB/s"),
        ])
def test_perf_estimate(log_name, module, expected):
    log_path = get_log_path(log_name)
    report = darshan.DarshanReport(log_path, read_all=True)
    actual = derived_metrics.perf_estimate(report=report, mod_name=module)
    assert actual == expected
