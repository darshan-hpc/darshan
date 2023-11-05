import argparse
from unittest import mock
from darshan.log_utils import get_log_path
from darshan.cli import job_stats
import pytest
@pytest.mark.parametrize(
    "argv", [
        [get_log_path("e3sm_io_heatmap_only.darshan"),
         "-mSTDIO",
         "-ototal_bytes",
         "-n5"],
    ]
)
def test_job_stats(argv, capsys):
    with mock.patch("sys.argv", argv):
        # initialize the parser
        parser = argparse.ArgumentParser(description="")
        # run through setup_parser()
        job_stats.setup_parser(parser=parser)
        # parse the input arguments
        args = parser.parse_args(argv)
    job_stats.main(args=args)
    captured = capsys.readouterr()
    assert "3.258853" in captured.out
