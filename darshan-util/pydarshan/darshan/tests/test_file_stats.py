import argparse
from unittest import mock
from darshan.log_utils import get_log_path
from darshan.cli import file_stats
from darshan.log_utils import _provide_logs_repo_filepaths
import pandas as pd
import io
import pytest

@pytest.mark.parametrize(
    "argv", [
        ["--csv",
         "--module=POSIX",
         "--order_by=bytes_written",
         get_log_path("shane_macsio_id29959_5-22-32552-7035573431850780836_1590156158.darshan")],
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
    assert not captured.err

def _provide_logs_repo_filepaths_filtered():
    return [
        path for path in _provide_logs_repo_filepaths()
        if 'dlio_logs' in path
    ]
@pytest.mark.skipif(not pytest.has_log_repo,
                    reason="missing darshan_logs")
@pytest.mark.parametrize(
    ("argv", "expected"),
    [
        (
            ["--csv",
             "--module=POSIX",
             "--order_by=bytes_read",
             *_provide_logs_repo_filepaths_filtered()],
            {'len': 194,
             'bytes_read': 129953991223,
             'bytes_written': 523946754,
             'reads': 35762,
             'writes': 168,
             'total_jobs': 670}
        ),
        (
            ["--csv",
             "--module=POSIX",
             "--order_by=bytes_read",
             "--limit=5",
             *_provide_logs_repo_filepaths_filtered()],
            {'len': 5,
             'bytes_read': 7214542900,
             'bytes_written': 0,
             'reads': 1830,
             'writes': 0,
             'total_jobs': 5}
        ),
        (
            ["--csv",
             "--module=POSIX",
             "--order_by=bytes_read",
             "--include_names=\\.npz$",
             *_provide_logs_repo_filepaths_filtered()],
            {'len': 168,
             'bytes_read': 129953701195,
             'bytes_written': 0,
             'reads': 34770,
             'writes': 0,
             'total_jobs': 172}
        )
    ]
)
def test_file_stats_multi(argv, expected, capsys):
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
    assert len(df) == expected['len']
    assert df['bytes_read'].sum() == expected['bytes_read']
    assert df['bytes_written'].sum() == expected['bytes_written']
    assert df['reads'].sum() == expected['reads']
    assert df['writes'].sum() == expected['writes']
    assert df['total_jobs'].sum() == expected['total_jobs']
    # run again to ensure default Rich print mode runs successfully
    args.csv = False
    file_stats.main(args=args)
    assert not captured.err
