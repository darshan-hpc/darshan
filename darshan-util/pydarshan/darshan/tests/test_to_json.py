import argparse
import json
import re
from unittest import mock

import pytest

import darshan
from darshan.cli import to_json
from darshan.log_utils import get_log_path

# darshan log for testing
# it contains POSIX, MPI-IO, H5F and H5D records, with names:
#   /tmp/test/macsio-log.log
#   /tmp/test/macsio_hdf5_000.h5
#   /tmp/test/macsio_hdf5_000.h5:/constant   (H5D dataset)
#   /tmp/test/macsio-timings.log
LOG = "shane_macsio_id29959_5-22-32552-7035573431850780836_1590156158.darshan"

def _run_to_json_cli(argv, capsys):
    with mock.patch("sys.argv", argv):
        parser = argparse.ArgumentParser(description="")
        to_json.setup_parser(parser=parser)
        args = parser.parse_args(argv)
    to_json.main(args=args)
    captured = capsys.readouterr()
    assert not captured.err
    return json.loads(captured.out)


@pytest.mark.parametrize(
    "filter_args, filter_patterns, filter_mode, expected_counts", [
        ([],
         None, "exclude",
         {"POSIX": 3, "MPI-IO": 1, "H5F": 1, "H5D": 1}),
        (["--exclude_names=\\.h5$"],
         [r"\.h5$"], "exclude",
         {"POSIX": 2, "MPI-IO": 0, "H5F": 0, "H5D": 1}),
        (["--include_names=\\.h5$"],
         [r"\.h5$"], "include",
         {"POSIX": 1, "MPI-IO": 1, "H5F": 1, "H5D": 0}),
        # repeated flags accumulate patterns
        (["-e", "\\.h5$", "-e", "timings"],
         [r"\.h5$", "timings"], "exclude",
         {"POSIX": 1, "MPI-IO": 0, "H5F": 0, "H5D": 1}),
    ]
)
def test_to_json_name_filters(filter_args, filter_patterns, filter_mode,
                              expected_counts, capsys):
    log_path = get_log_path(LOG)
    output = _run_to_json_cli([*filter_args, log_path], capsys)

    # expected number of records per module after filtering
    for mod, expected in expected_counts.items():
        assert len(output["records"][mod]) == expected
        assert output["modules"][mod]["num_records"] == expected

    # name records honor the filter mode
    names = list(output["name_records"].values())
    if filter_patterns:
        compiled = [re.compile(p) for p in filter_patterns]
        for name in names:
            matched = any(p.search(name) for p in compiled)
            assert matched if filter_mode == "include" else not matched

    # every exported record has a surviving name record
    for mod, expected in expected_counts.items():
        for rec in output["records"][mod]:
            assert str(rec["id"]) in output["name_records"]

    # CLI output matches DarshanReport.to_json() using the same filters
    with darshan.DarshanReport(log_path, filter_patterns=filter_patterns,
                               filter_mode=filter_mode) as report:
        assert output == json.loads(report.to_json())


def test_to_json_include_and_exclude_are_exclusive(capsys):
    argv = ["--include_names=\\.h5$", "--exclude_names=\\.log$", get_log_path(LOG)]
    with mock.patch("sys.argv", argv):
        parser = argparse.ArgumentParser(description="")
        to_json.setup_parser(parser=parser)
        args = parser.parse_args(argv)
    with pytest.raises(ValueError, match="Only one of"):
        to_json.main(args=args)
