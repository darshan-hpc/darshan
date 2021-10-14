import os
import pytest
import argparse
from unittest import mock
from datetime import datetime

import pandas as pd

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


class TestReportData:

    @pytest.mark.parametrize(
        "log_path",
        [
            "tests/input/sample.darshan",
            "tests/input/noposix.darshan",
            "tests/input/sample-badost.darshan",
            "tests/input/sample-dxt-simple.darshan",
        ],
    )
    def test_stylesheet(self, log_path):
        # check that the report object is
        # generating the correct attributes
        R = summary.ReportData(log_path=log_path)
        # verify the first line shows up correctly for each log
        expected_str = "p {\n  font-size: 12px;\n}"
        assert expected_str in R.stylesheet

    @pytest.mark.parametrize(
        "log_path, expected_header",
        [
            ("tests/input/sample.darshan", "vpicio_uni (2017-03-20)"),
            # anonymized case
            ("tests/input/noposix.darshan", "Anonymized (2018-01-02)"),
            ("tests/input/sample-badost.darshan", "ior (2017-06-20)"),
            ("tests/input/sample-dxt-simple.darshan", "a.out (2021-04-22)"),
        ],
    )
    def test_header_and_footer(self, log_path, expected_header):
        # check the header and footer strings stored in the report data object
        R = summary.ReportData(log_path=log_path)
        assert R.header == expected_header
        assert "Summary report generated via PyDarshan v" in R.footer

    @pytest.mark.parametrize(
        (
            "filename, expected_jobid, expected_uid, expected_nprocs, "
            "expected_runtime, expected_cmd, expected_lib_ver, expected_log_fmt"
        ),
        [
            (
                "sample.darshan",
                "4478544",
                "69615",
                "2048",
                "116.0",
                (
                    "/global/project/projectdirs/m888/glock/tokio-abc-results/"
                    "bin.edison/vpicio_uni /scratch2/scratchdirs/glock/tokioabc"
                    "-s.4478544/vpicio/vpicio.hdf5 32"
                ),
                "3.1.3",
                "3.10",
            ),
            # anonymized case
            (
                "noposix.darshan",
                "83017637",
                "996599276",
                "512",
                "39212.0",
                "Anonymized",
                "3.1.4",
                "3.10",
            ),
            (
                "sample-dxt-simple.darshan",
                "4233209",
                "28751",
                "16",
                "< 1",
                "/yellow/usr/projects/eap/users/treddy/simple_dxt_mpi_io_darshan/a.out",
                "3.2.1",
                "3.21",
            ),
        ],
    )
    def test_metadata_table(
        self,
        filename,
        expected_jobid,
        expected_uid,
        expected_nprocs,
        expected_runtime,
        expected_cmd,
        expected_lib_ver,
        expected_log_fmt,
    ):
        # regression test for `summary.ReportData.get_metadata_table()`

        # generate the report data
        log_path = os.path.join("tests/input/", filename)
        R = summary.ReportData(log_path=log_path)
        # convert the metadata table back to a pandas dataframe
        actual_metadata_df = pd.read_html(R.metadata_table, index_col=0)[0]

        # check the metadata match the expected outcomes
        assert actual_metadata_df.loc["Job ID"].values[0] == expected_jobid
        assert actual_metadata_df.loc["User ID"].values[0] == expected_uid
        assert actual_metadata_df.loc["# Processes"].values[0] == expected_nprocs
        assert actual_metadata_df.loc["Runtime (s)"].values[0] == expected_runtime
        assert actual_metadata_df.loc["Command"].values[0] == expected_cmd
        assert actual_metadata_df.loc["Log Filename"].values[0] == filename
        assert (
            actual_metadata_df.loc["Runtime Library Version"].values[0]
            == expected_lib_ver
        )
        assert actual_metadata_df.loc["Log Format Version"].values[0] == expected_log_fmt

        # get the time stamps in the current time zone
        expected_start = str(datetime.fromtimestamp(R.report.metadata["job"]["start_time"]))
        expected_end = str(datetime.fromtimestamp(R.report.metadata["job"]["end_time"]))
        assert actual_metadata_df.loc["Start Time"].values[0] == expected_start
        assert actual_metadata_df.loc["End Time"].values[0] == expected_end


    @pytest.mark.parametrize(
        "log_path",
        [
            # each of these logs offers a unique
            # set of modules to verify
            "tests/input/sample.darshan",
            "tests/input/noposix.darshan",
            "tests/input/noposixopens.darshan",
            "tests/input/sample-goodost.darshan",
            "tests/input/sample-dxt-simple.darshan",
        ],
    )
    def test_module_table(self, log_path):
        # regression test for `summary.ReportData.get_module_table()`

        # collect the report data
        R = summary.ReportData(log_path=log_path)
        # convert the module table back to a pandas dataframe
        actual_mod_df = pd.read_html(R.module_table, index_col=0)[0]

        # verify the number of modules in the report is equal to
        # the number of rows in the module table
        expected_module_count = len(R.report.modules.keys())
        assert expected_module_count == actual_mod_df.shape[0]

        # check each entry of the module dataframe by retrieving all row
        # labels and checking they match their values
        if "sample.darshan" in log_path:
            assert actual_mod_df.loc["POSIX (ver=3)"].values[0] == "0.18 KiB"
            assert actual_mod_df.loc["MPI-IO (ver=2)"].values[0] == "0.15 KiB"
            assert actual_mod_df.loc["LUSTRE (ver=1)"].values[0] == "0.08 KiB"
            assert actual_mod_df.loc["STDIO (ver=1)"].values[0] == "3.16 KiB"
        elif "noposix.darshan" in log_path:
            assert actual_mod_df.loc["LUSTRE (ver=1)"].values[0] == "6.07 KiB"
            assert actual_mod_df.loc["STDIO (ver=1)"].values[0] == "0.21 KiB"
        elif "noposixopens.darshan" in log_path:
            assert actual_mod_df.loc["POSIX (ver=3)"].values[0] == "0.04 KiB"
            assert actual_mod_df.loc["STDIO (ver=1)"].values[0] == "0.27 KiB"
        elif "sample-goodost.darshan" in log_path:
            assert actual_mod_df.loc["POSIX (ver=3)"].values[0] == "5.59 KiB"
            assert actual_mod_df.loc["LUSTRE (ver=1)"].values[0] == "1.47 KiB"
            assert actual_mod_df.loc["STDIO (ver=1)"].values[0] == "0.07 KiB"
        elif "sample-dxt-simple.darshan" in log_path:
            assert actual_mod_df.loc["POSIX (ver=4)"].values[0] == "2.94 KiB"
            assert actual_mod_df.loc["MPI-IO (ver=3)"].values[0] == "1.02 KiB"
            assert actual_mod_df.loc["DXT_POSIX (ver=1)"].values[0] == "0.08 KiB"
            assert actual_mod_df.loc["DXT_MPIIO (ver=2)"].values[0] == "0.06 KiB"

    @pytest.mark.parametrize(
        "report, expected_cmd",
        [
            (
                darshan.DarshanReport("tests/input/sample.darshan"),
                (
                    "/global/project/projectdirs/m888/glock/tokio-abc-results/"
                    "bin.edison/vpicio_uni /scratch2/scratchdirs/glock/tokioabc"
                    "-s.4478544/vpicio/vpicio.hdf5 32"
                ),
            ),
            (
                darshan.DarshanReport("tests/input/sample-badost.darshan"),
                (
                    "/global/project/projectdirs/m888/glock/tokio-abc-results/"
                    "bin.edison/ior -H -k -w -o ior-posix.out -s 64 -f /global"
                    "/project/projectdirs/m888/glock/tokio-abc-results/inputs/"
                    "posix1m2.in"
                ),
            ),
            (
                darshan.DarshanReport("tests/input/sample-goodost.darshan"),
                (
                    "/global/homes/g/glock/src/git/ior-lanl/src/ior "
                    "-t 8m -b 256m -s 4 -F -C -e -a POSIX -w -k"
                ),
            ),
            (
                darshan.DarshanReport("tests/input/sample-dxt-simple.darshan"),
                "/yellow/usr/projects/eap/users/treddy/simple_dxt_mpi_io_darshan/a.out ",
            ),
            # anonymized cases
            (darshan.DarshanReport("tests/input/noposix.darshan"), "Anonymized"),
            (darshan.DarshanReport("tests/input/noposixopens.darshan"), "Anonymized"),
            # no executable case
            (darshan.DarshanReport("examples/example-logs/dxt.darshan"), "N/A"),
        ],
    )
    def test_get_full_command(self, report, expected_cmd):
        # regression test for `summary.ReportData.get_full_command()`
        actual_cmd = summary.ReportData.get_full_command(report=report)
        assert actual_cmd == expected_cmd

    @pytest.mark.parametrize(
        "report, expected_runtime",
        [
            (darshan.DarshanReport("tests/input/sample.darshan"), "116.0",),
            (darshan.DarshanReport("tests/input/noposix.darshan"), "39212.0"),
            (darshan.DarshanReport("tests/input/noposixopens.darshan"), "1110.0"),
            (darshan.DarshanReport("tests/input/sample-badost.darshan"), "779.0",),
            (darshan.DarshanReport("tests/input/sample-goodost.darshan"), "4.0",),
            # special case where the calculated run time is 0
            (darshan.DarshanReport("tests/input/sample-dxt-simple.darshan"), "< 1",),
        ],
    )
    def test_get_runtime(self, report, expected_runtime):
        # regression test for `summary.ReportData.get_runtime()`
        actual_runtime = summary.ReportData.get_runtime(report=report)
        assert actual_runtime == expected_runtime
