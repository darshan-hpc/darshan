import os
import pytest
import argparse
from unittest import mock
from datetime import datetime

import numpy as np
import pandas as pd
from pandas.testing import assert_frame_equal

import darshan
from darshan.cli import summary
from darshan.log_utils import get_log_path, _provide_logs_repo_filepaths
from darshan.experimental.plots.data_access_by_filesystem import plot_with_report


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
        ["dxt.darshan"],
        ["dxt.darshan", "--output=test.html"],
    ]
)
def test_main_with_args(tmpdir, argv):
    # test summary.main() by building a parser
    # and using it as an input
    argv[0] = get_log_path(argv[0])

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
    "argv, expected_img_count, expected_table_count", [
        (["noposix.darshan"], 3, 2),
        (["noposix.darshan", "--output=test.html"], 3, 2),
        (["sample-dxt-simple.darshan"], 8, 4),
        (["sample-dxt-simple.darshan", "--output=test.html"], 8, 4),
        (["nonmpi_dxt_anonymized.darshan"], 6, 3),
        (["ior_hdf5_example.darshan"], 11, 5),
        ([None], 0, 0),
    ]
)
def test_main_without_args(tmpdir, argv, expected_img_count, expected_table_count):
    # test summary.main() by running it without a parser
    if argv[0] is not None:
        argv[0] = get_log_path(argv[0])

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

                # verify DXT figures are present for each DXT module
                report = darshan.DarshanReport(filename=argv[0], read_all=False)
                with open(expected_save_path) as html_report:
                    report_str = html_report.read()
                    if "DXT" in "\t".join(report.modules):
                        for dxt_mod in ["DXT_POSIX", "DXT_MPIIO"]:
                            if dxt_mod in report.modules:
                                assert f"Heat Map: {dxt_mod}" in report_str
                    else:
                        # check that help message is present
                        assert "Heatmap data is not available for this job" in report_str
                        assert "Consider enabling the runtime heatmap module" in report_str

                    # check that expected number of figures are found
                    assert report_str.count("<img") == expected_img_count

                    # check that the expected number of tables are found
                    # NOTE: since there are extraneous instances of "table"
                    # in each report, the actual table count is half the
                    # sum of the opening and closing tags
                    actual_table_count = (report_str.count("<table")
                                          + report_str.count("</table>")) / 2
                    assert actual_table_count == expected_table_count
                    # check the number of opening section tags
                    # matches the number of closing section tags
                    assert report_str.count("<section>") == report_str.count("</section>")

                    # check if I/O cost figure is present
                    for mod in report.modules:
                        if mod in ["POSIX", "MPI-IO", "STDIO"]:
                            assert "I/O Cost" in report_str
        else:
            # if no log path is given expect a runtime error
            # due to a failure to open the file
            with pytest.raises(RuntimeError):
                summary.main()


@pytest.mark.skipif(not pytest.has_log_repo,
                    reason="missing darshan_logs")
@pytest.mark.parametrize("log_filepath",
        _provide_logs_repo_filepaths()
        )
def test_main_all_logs_repo_files(tmpdir, log_filepath):
    # similar to `test_main_without_args` but focused
    # on the Darshan logs from the logs repo:
    # https://github.com/darshan-hpc/darshan-logs

    if "e3sm_io_heatmap_and_dxt" in log_filepath:
        pytest.xfail(reason="large memory requirements")
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

            # verify DXT figures are present for each DXT module
            report = darshan.DarshanReport(log_filepath, read_all=False)
            with open(expected_save_path) as html_report:
                report_str = html_report.read()
                if "DXT" in "\t".join(report.modules):
                    for dxt_mod in ["DXT_POSIX", "DXT_MPIIO"]:
                        if dxt_mod in report.modules:
                            assert f"Heat Map: {dxt_mod}" in report_str
                        # MPIIO should come first
                        mpiio_position = report_str.find("Heat Map: DXT_MPIIO")
                        posix_position = report_str.find("Heat Map: DXT_POSIX")
                        if mpiio_position != -1 and posix_position != -1:
                            assert mpiio_position < posix_position
                elif "HEATMAP" in "\t".join(report.modules):
                    assert "Heat Map: HEATMAP" in report_str
                    # enforce the desired order
                    mpiio_position = report_str.find("Heat Map: HEATMAP MPIIO")
                    posix_position = report_str.find("Heat Map: HEATMAP POSIX")
                    stdio_position = report_str.find("Heat Map: HEATMAP STDIO")
                    # the 3-way check is only valid if all heatmap modules
                    # are present:
                    if (mpiio_position > -1 and
                        posix_position > -1 and
                        stdio_position > -1):
                        assert mpiio_position < posix_position < stdio_position
                else:
                    # check that help message is present
                    assert "Heatmap data is not available for this job" in report_str
                    assert "Consider enabling the runtime heatmap module" in report_str

                # check if I/O cost figure is present
                for mod in report.modules:
                    if mod in ["POSIX", "MPI-IO", "STDIO"]:
                        assert "I/O Cost" in report_str

                # check the number of opening section tags
                # matches the number of closing section tags
                assert report_str.count("<section>") == report_str.count("</section>")

                # check for presence of expected runtime HEATMAPs
                actual_runtime_heatmap_titles = report_str.count("<h3>Heat Map: HEATMAP")
                if "e3sm_io_heatmap_only" in log_filepath:
                    assert actual_runtime_heatmap_titles == 3
                elif ("runtime_and_dxt_heatmaps_diagonal_write_only" in log_filepath or
                      "treddy_runtime_heatmap_inactive_ranks" in log_filepath):
                    assert actual_runtime_heatmap_titles == 1
                else:
                    assert actual_runtime_heatmap_titles == 0


class TestReportData:

    @pytest.mark.parametrize(
        "log_path",
        [
            "sample.darshan",
            "noposix.darshan",
            "sample-badost.darshan",
            "sample-dxt-simple.darshan",
        ],
    )
    def test_stylesheet(self, log_path):
        # check that the report object is
        # generating the correct attributes
        log_path = get_log_path(log_path)
        R = summary.ReportData(log_path=log_path)
        # verify the first line shows up correctly for each log
        expected_str = "small {\n  font-size: 80%;\n}"
        assert expected_str in R.stylesheet

    @pytest.mark.parametrize(
        "log_name, expected_header",
        [
            ("sample.darshan", "vpicio_uni (2017-03-20)"),
            # anonymized case
            ("noposix.darshan", "Anonymized (2018-01-02)"),
            ("sample-badost.darshan", "ior (2017-06-20)"),
            ("sample-dxt-simple.darshan", "a.out (2021-04-22)"),
            ("dxt.darshan", "N/A (2020-04-21)"),
        ],
    )
    def test_header_and_footer(self, log_name, expected_header):
        # check the header and footer strings stored in the report data object
        log_path = get_log_path(log_name)
        R = summary.ReportData(log_path=log_path)
        assert R.header == expected_header
        assert "Summary report generated via PyDarshan v" in R.footer

    @pytest.mark.parametrize(
        "log_path, expected_df",
        [
            (
                "sample.darshan",
                pd.DataFrame(
                    index=[
                        "Job ID", "User ID", "# Processes", "Runtime (s)",
                        "Start Time", "End Time", "Command Line",
                    ],
                    data=[
                        "4478544",
                        "69615",
                        "2048",
                        "116",
                        str(datetime.fromtimestamp(1490000867)),
                        str(datetime.fromtimestamp(1490000983)),
                        (
                            "/global/project/projectdirs/m888/glock/tokio-abc-"
                            "results/bin.edison/vpicio_uni /scratch2/scratchdirs"
                            "/glock/tokioabc-s.4478544/vpicio/vpicio.hdf5 32"
                        ),
                    ]
                )
            ),
            # anonymized case
            (
                "noposix.darshan",
                pd.DataFrame(
                    index=[
                        "Job ID", "User ID", "# Processes", "Runtime (s)",
                        "Start Time", "End Time", "Command Line",
                    ],
                    data=[
                        "83017637",
                        "996599276",
                        "512",
                        "39212",
                        str(datetime.fromtimestamp(1514923055)),
                        str(datetime.fromtimestamp(1514962267)),
                        "Anonymized",
                    ]
                )
            ),
            (
                "sample-dxt-simple.darshan",
                pd.DataFrame(
                    index=[
                        "Job ID", "User ID", "# Processes", "Runtime (s)",
                        "Start Time", "End Time", "Command Line",
                    ],
                    data=[
                    "4233209",
                    "28751",
                    "16",
                    "< 1",
                    str(datetime.fromtimestamp(1619109091)),
                    str(datetime.fromtimestamp(1619109091)),
                    (
                        "/yellow/usr/projects/eap/users/treddy"
                        "/simple_dxt_mpi_io_darshan/a.out"
                    ),
                    ]
                )
            ),
        ],
    )
    def test_metadata_table(self, log_path, expected_df):
        # regression test for `summary.ReportData.get_metadata_table()`

        log_path = get_log_path(log_path)
        # generate the report data
        R = summary.ReportData(log_path=log_path)
        # convert the metadata table back to a pandas dataframe
        actual_metadata_df = pd.read_html(R.metadata_table, index_col=0)[0]
        # correct index and columns attributes after
        # `index_col` removed the first column
        actual_metadata_df.index.names = [None]
        actual_metadata_df.columns = [0]

        # check the metadata dataframes
        assert_frame_equal(actual_metadata_df, expected_df)


    @pytest.mark.parametrize(
        "log_path, expected_df, expected_partial_flags",
        [
            # each of these logs offers a unique
            # set of modules to verify
            (
                "sample.darshan",
                pd.DataFrame(
                    index=[
                        "Log Filename", "Runtime Library Version", "Log Format Version",
                        "POSIX (ver=3)", "MPI-IO (ver=2)",
                        "LUSTRE (ver=1)", "STDIO (ver=1)",
                    ],
                    data=[["sample.darshan"], ["3.1.3"], ["3.10"], ["0.18 KiB"],
                          ["0.15 KiB"], ["0.08 KiB"], ["3.16 KiB"]],
                ),
                0,
            ),
            (
                "noposix.darshan",
                pd.DataFrame(
                    index=["Log Filename", "Runtime Library Version", "Log Format Version",
                           "LUSTRE (ver=1)", "STDIO (ver=1)"],
                    data=[["noposix.darshan"], ["3.1.4"], ["3.10"], ["6.07 KiB"], ["0.21 KiB"]],
                ),
                0,
            ),
            (
                "noposixopens.darshan",
                pd.DataFrame(
                    index=["Log Filename", "Runtime Library Version", "Log Format Version",
                           "POSIX (ver=3)", "STDIO (ver=1)"],
                    data=[["noposixopens.darshan"], ["3.1.4"], ["3.10"],
                          ["0.04 KiB"], ["0.27 KiB"]],
                ),
                0,
            ),
            (
                "sample-goodost.darshan",
                pd.DataFrame(
                    index=["Log Filename", "Runtime Library Version", "Log Format Version",
                           "POSIX (ver=3)", "LUSTRE (ver=1)", "STDIO (ver=1)"],
                    data=[["sample-goodost.darshan"], ["3.1.3"], ["3.10"],
                          ["5.59 KiB"], ["1.47 KiB"], ["0.07 KiB"]],
                ),
                0,
            ),
            (
                "sample-dxt-simple.darshan",
                pd.DataFrame(
                    index=[
                        "Log Filename", "Runtime Library Version", "Log Format Version",
                        "POSIX (ver=4)", "MPI-IO (ver=3)",
                        "DXT_POSIX (ver=1)", "DXT_MPIIO (ver=2)",
                    ],
                    data=[["sample-dxt-simple.darshan"], ["3.2.1"], ["3.21"],
                          ["2.94 KiB"], ["1.02 KiB"], ["0.08 KiB"], ["0.06 KiB"]],
                ),
                0,
            ),
            (
                "partial_data_stdio.darshan",
                pd.DataFrame(
                    index=[
                        "Log Filename", "Runtime Library Version", "Log Format Version",
                        "POSIX (ver=4)", "MPI-IO (ver=3)",
                        "STDIO (ver=2)",
                    ],
                    data=[["partial_data_stdio.darshan"], ["3.2.1"], ["3.21"],
                          ["0.15 KiB"], ["0.13 KiB"], ["70.34 KiB"]],
                ),
                1,
            ),
            (
                "partial_data_dxt.darshan",
                pd.DataFrame(
                    index=[
                        "Log Filename", "Runtime Library Version", "Log Format Version",
                        "POSIX (ver=4)", "MPI-IO (ver=3)",
                        "STDIO (ver=2)", "DXT_POSIX (ver=1)",
                        "DXT_MPIIO (ver=2)"
                    ],
                    data=[
                        ["partial_data_dxt.darshan"], ["3.2.1"], ["3.21"],
                        ["0.14 KiB"], ["0.12 KiB"], ["0.06 KiB"],
                        ["574.73 KiB"], ["568.14 KiB"],
                    ],
                ),
                2,
            )
        ],
    )
    def test_module_table(self, log_path, expected_df, expected_partial_flags):
        # regression test for `summary.ReportData.get_module_table()`

        log_path = get_log_path(log_path)
        # collect the report data
        R = summary.ReportData(log_path=log_path)
        # check that number of unicode warning symbols matches expected partial flag count
        assert R.module_table.count("&#x26A0;") == expected_partial_flags
        # convert the module table back to a pandas dataframe
        actual_mod_df = pd.read_html(R.module_table, index_col=0)[0]
        # correct index and columns attributes after
        # `index_col` removed the first column
        actual_mod_df.index.names = [None]
        actual_mod_df.columns = [0, 1]

        # verify the number of modules in the report is equal to
        # the number of rows in the module table, including the 3 rows
        # containing log metadata
        expected_module_count = len(R.report.modules.keys())
        assert actual_mod_df.shape[0] == expected_module_count + 3

        expected_df.rename(index=lambda s: s + " Module Data" if "ver=" in s else s, inplace=True)

        # add new column for partial flags
        expected_df[1] = np.nan
        flag = "\u26A0 Module data incomplete due to runtime memory or record count limits"
        if "partial_data_stdio.darshan" in log_path:
            expected_df.iloc[5, 1] = flag
        if "partial_data_dxt.darshan" in log_path:
            expected_df.iloc[6:, 1] = flag

        # check the module dataframes
        assert_frame_equal(actual_mod_df, expected_df)

    @pytest.mark.parametrize(
        "logname, expected_cmd",
        [
            (
                "sample.darshan",
                (
                    "/global/project/projectdirs/m888/glock/tokio-abc-results/"
                    "bin.edison/vpicio_uni /scratch2/scratchdirs/glock/tokioabc"
                    "-s.4478544/vpicio/vpicio.hdf5 32"
                ),
            ),
            (
                "sample-badost.darshan",
                (
                    "/global/project/projectdirs/m888/glock/tokio-abc-results/"
                    "bin.edison/ior -H -k -w -o ior-posix.out -s 64 -f /global"
                    "/project/projectdirs/m888/glock/tokio-abc-results/inputs/"
                    "posix1m2.in"
                ),
            ),
            (
                "sample-goodost.darshan",
                (
                    "/global/homes/g/glock/src/git/ior-lanl/src/ior "
                    "-t 8m -b 256m -s 4 -F -C -e -a POSIX -w -k"
                ),
            ),
            (
                "sample-dxt-simple.darshan",
                "/yellow/usr/projects/eap/users/treddy/simple_dxt_mpi_io_darshan/a.out ",
            ),
            # anonymized cases
            ("noposix.darshan", "Anonymized"),
            ("noposixopens.darshan", "Anonymized"),
            # no executable case
            ("dxt.darshan", "N/A"),
        ],
    )
    def test_get_full_command(self, logname, expected_cmd):
        # regression test for `summary.ReportData.get_full_command()`
        report = darshan.DarshanReport(get_log_path(logname))
        actual_cmd = summary.ReportData.get_full_command(report=report)
        assert actual_cmd == expected_cmd

    @pytest.mark.parametrize(
        "logname, expected_runtime",
        [
            ("sample.darshan", "116",),
            ("noposix.darshan", "39212"),
            ("noposixopens.darshan", "1110"),
            ("sample-badost.darshan", "779",),
            ("sample-goodost.darshan", "4",),
            # special case where the calculated run time is 0
            ("sample-dxt-simple.darshan", "< 1",),
        ],
    )
    def test_get_runtime(self, logname, expected_runtime):
        # regression test for `summary.ReportData.get_runtime()`
        report = darshan.DarshanReport(get_log_path(logname))
        actual_runtime = summary.ReportData.get_runtime(report=report)
        assert actual_runtime == expected_runtime


class TestReportFigure:

    def test_generate_fig_unsupported_fig_type(self):
        # input figure function that outputs an unsupported figure type
        # to check an error is raised

        with pytest.raises(NotImplementedError) as err:
            # initializing a `ReportFigure` automatically calls `generate_fig()`
            summary.ReportFigure(
                # create a simple function
                fig_func=lambda x:x,
                fig_args=dict(x=1),
                section_title="Test Section Title",
                fig_title="Test Figure Title",
            )

        # verify correct error is being raised
        assert "Figure of type <class 'int'> not supported." in str(err)


def test_issue_717():
    # regression test for issue https://github.com/darshan-hpc/darshan/issues/717
    logname = "ior_hdf5_example.darshan"
    report = darshan.DarshanReport(get_log_path(logname))
    fig = summary.ReportFigure(
        section_title="",
        # set figure title to empty string
        fig_title="",
        fig_func=plot_with_report,
        fig_args=dict(report=report),
    )
    assert not "alt= width" in fig.fig_html
