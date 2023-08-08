import argparse
from unittest import mock
from darshan.log_utils import get_log_path
from darshan.cli import file_stats
import pytest
@pytest.mark.parametrize(
    "argv", [
        [get_log_path("e3sm_io_heatmap_only.darshan"),
         "-mSTDIO",
         "-oSTDIO_BYTES_READ",
         "-n5"],
    ]
)
def test_file_stats(argv, capsys):
    with mock.patch("sys.argv", argv):
        # initialize the parser
        parser = argparse.ArgumentParser(description="")
        # run through setup_parser()
        file_stats.setup_parser(parser=parser)
        # parse the input arguments
        args = parser.parse_args(argv)
    file_stats.main(args=args)
    captured = capsys.readouterr()
    assert "15920181672442173319" in captured.out

