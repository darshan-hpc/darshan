import argparse
from unittest import mock
from darshan.log_utils import get_log_path
from darshan.cli import file_stats
import pandas as pd
import io
import pytest

@pytest.mark.parametrize(
    "argv", [
        [get_log_path("shane_macsio_id29959_5-22-32552-7035573431850780836_1590156158.darshan"),
         "--csv",
         "--module=POSIX",
         "--order_by=bytes_written",
         "--limit=5"],
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
    # run once with CSV output and spot check some of the output
    file_stats.main(args=args)
    captured = capsys.readouterr()
    assert not captured.err
    assert captured.out
    df = pd.read_csv(io.StringIO(captured.out))
    assert len(df) == 3
    # check the first file (most bytes written)
    expected_first = {
        'file': '/tmp/test/macsio_hdf5_000.h5',
        'bytes_read': 39816960,
        'bytes_written': 54579416,
        'reads': 6,
        'writes': 7699,
        'total_jobs': 1
    }
    row = df.iloc[0]
    for key, value in expected_first.items():
        assert row[key] == value
    # check the last file (least bytes written)
    expected_last = {
        'file': '/tmp/test/macsio-timings.log',
        'bytes_read': 0,
        'bytes_written': 12460,
        'reads': 0,
        'writes': 51,
        'total_jobs': 1
    }
    row = df.iloc[-1]
    for key, value in expected_last.items():
        assert row[key] == value
    assert expected_first['bytes_written'] > expected_last['bytes_written']
    # run again to ensure default Rich print mode runs successfully
    args.csv = False
    file_stats.main(args=args)
