import argparse
from unittest import mock
from darshan.log_utils import get_log_path
from darshan.cli import job_stats
from darshan.log_utils import _provide_logs_repo_filepaths
from numpy.testing import assert_allclose
import pandas as pd
import io
import pytest

@pytest.mark.parametrize(
    "argv", [
        ["--csv",
         "--module=STDIO",
         "--order_by=total_bytes",
         get_log_path("sample-badost.darshan")],
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
    # run once with CSV output and spot check some of the output
    job_stats.main(args=args)
    captured = capsys.readouterr()
    assert not captured.err
    assert captured.out
    df = pd.read_csv(io.StringIO(captured.out))
    assert len(df) == 1
    expected = {
        'log_file': 'sample-badost.darshan',
        'job_id': 6265799,
        'nprocs': 2048,
        'run_time': 780.0,
        'perf_by_slowest': 8.249708e+06,
        'time_by_slowest': 0.200828,
        'total_bytes': 1656773,
        'total_files': 3,
        'partial_flag': False
    }
    row = df.iloc[0]
    for key, value in expected.items():
        if key == 'perf_by_slowest' or key == 'time_by_slowest':
            assert_allclose(row[key], value, rtol=1e-5, atol=1e-8)
        else:
            assert row[key] == value
    # run again to ensure default Rich print mode runs successfully
    args.csv = False
    job_stats.main(args=args)
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
             "--order_by=perf_by_slowest",
             *_provide_logs_repo_filepaths_filtered()],
            {'perf_by_slowest': 1818543162.0558,
             'time_by_slowest': 89.185973,
             'total_bytes': 130477937977,
             'total_files': 670}
        ),
        (
            ["--csv",
             "--module=POSIX",
             "--order_by=perf_by_slowest",
             "--limit=5",
             *_provide_logs_repo_filepaths_filtered()],
            {'perf_by_slowest': 1818543162.0558,
             'time_by_slowest': 30.823626,
             'total_bytes': 54299532365,
             'total_files': 190}
        )
    ]
)
def test_job_stats_multi(argv, expected, capsys):
    # this case tests job_stats with multiple input logs
    # and ensures that aggregate statistics are as expected
    with mock.patch("sys.argv", argv):
        # initialize the parser
        parser = argparse.ArgumentParser(description="")
        # run through setup_parser()
        job_stats.setup_parser(parser=parser)
        # parse the input arguments
        args = parser.parse_args(argv)
    # run once with CSV output and spot check some of the output
    job_stats.main(args=args)
    captured = capsys.readouterr()
    assert not captured.err
    assert captured.out
    df = pd.read_csv(io.StringIO(captured.out))
    # verify max perf is first row and min perf is last row
    max_perf = df['perf_by_slowest'].max()
    min_perf = df['perf_by_slowest'].min()
    assert df.iloc[0]['perf_by_slowest'] == max_perf
    assert df.iloc[-1]['perf_by_slowest'] == min_perf
    # verify values against expected
    assert_allclose(max_perf, expected['perf_by_slowest'], rtol=1e-5, atol=1e-8)
    assert max_perf == expected['perf_by_slowest']
    total_time = df['time_by_slowest'].sum()
    assert_allclose(total_time, expected['time_by_slowest'], rtol=1e-5, atol=1e-8)
    total_bytes = df['total_bytes'].sum()
    assert total_bytes == expected['total_bytes']
    total_files = df['total_files'].sum()
    assert total_files == expected['total_files']
    # run again to ensure default Rich print mode runs successfully
    args.csv = False
    job_stats.main(args=args)
    assert not captured.err
