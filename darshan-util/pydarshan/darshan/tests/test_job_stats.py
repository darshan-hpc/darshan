import argparse
from unittest import mock
from darshan.log_utils import get_log_path
from darshan.cli import job_stats
from numpy.testing import assert_allclose
import pandas as pd
import io
import pytest

@pytest.mark.parametrize(
    "argv", [
        [get_log_path("sample-badost.darshan"),
         "--csv",
         "--module=STDIO",
         "--order_by=total_bytes",
         "--limit=5"],
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
