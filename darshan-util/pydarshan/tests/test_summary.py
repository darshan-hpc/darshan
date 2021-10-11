import os
import pytest
import argparse
from unittest import mock

import darshan
from darshan.cli import summary


@pytest.mark.parametrize(
    "argv", [
        ["./tests/input/sample.darshan"],
        ["./tests/input/sample.darshan", "--output=test.html"],
    ]
)
def test_setup_parser(argv):
    # test for summary.setup_parser() to verify the
    # correct arguments are being added to the parser

    with mock.patch("sys.argv", argv):
        # initialize the parser
        parser = argparse.ArgumentParser(description="")
        # run through setup_parser()
        summary.setup_parser(parser=parser)
        # parse the input arguments
        args = parser.parse_args(argv)

    # verify the description has been added
    assert parser.description == "Generates a Darshan Summary Report"
    # verify the log path and output filenames are correct
    assert args.log_path == "./tests/input/sample.darshan"
    if args.output:
        assert args.output == "test.html"


@pytest.mark.parametrize(
    "argv", [
        [os.path.abspath("./examples/example-logs/dxt.darshan")],
        [os.path.abspath("./examples/example-logs/dxt.darshan"), "--output=test.html"],
    ]
)
def test_main_with_args(tmpdir, argv):
    # test summary.main() by building a parser
    # and using it as an input

    # initialize the parser, add the arguments, and parse them
    with mock.patch("sys.argv", argv):
        parser = argparse.ArgumentParser(description="")
        summary.setup_parser(parser=parser)
        args = parser.parse_args(argv)

    with tmpdir.as_cwd():
        # run main() using the arguments built by the parser
        summary.main(args=args)

        # get the expected save path
        if len(argv) == 1:
            output_fname = "dxt_report.html"
        else:
            output_fname = "test.html"
        expected_save_path = os.path.abspath(output_fname)

        # verify the HTML file was generated
        assert os.path.exists(expected_save_path)


@pytest.mark.parametrize(
    "argv", [
        [os.path.abspath("./tests/input/noposix.darshan")],
        [os.path.abspath("./tests/input/noposix.darshan"), "--output=test.html"],
        [os.path.abspath("./tests/input/sample-dxt-simple.darshan")],
        [os.path.abspath("./tests/input/sample-dxt-simple.darshan"), "--output=test.html"],
        [None],
    ]
)
def test_main_without_args(tmpdir, argv):
    # test summary.main() by running it without a parser

    with mock.patch("sys.argv", [""] + argv):
        if argv[0]:
            # if a log path is given, generate the summary report
            # in a temporary directory
            with tmpdir.as_cwd():
                summary.main()

                # get the path for the generated summary report
                if len(argv) == 1:
                    log_fname = os.path.basename(argv[0])
                    output_fname = os.path.splitext(log_fname)[0] + "_report.html"
                else:
                    output_fname = "test.html"
                expected_save_path = os.path.abspath(output_fname)

                # verify the HTML file was generated
                assert os.path.exists(expected_save_path)

        else:
            # if no log path is given expect a runtime error
            # due to a failure to open the file
            with pytest.raises(RuntimeError):
                summary.main()


@pytest.mark.skipif(not pytest.has_log_repo, # type: ignore
                    reason="missing darshan_logs")
def test_main_all_logs_repo_files(tmpdir, log_repo_files):
    # similar to `test_main_without_args` but focused
    # on the Darshan logs from the logs repo:
    # https://github.com/darshan-hpc/darshan-logs

    for log_filepath in log_repo_files:
        argv = [log_filepath]
        with mock.patch("sys.argv", [""] + argv):
            with tmpdir.as_cwd():
                # generate the summary report
                summary.main()

                # get the path for the generated summary report
                log_fname = os.path.basename(argv[0])
                output_fname = os.path.splitext(log_fname)[0] + "_report.html"
                expected_save_path = os.path.abspath(output_fname)

                # verify the HTML file was generated
                assert os.path.exists(expected_save_path)
