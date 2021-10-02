import os
import argparse
import datetime
import importlib.resources as importlib_resources
from typing import Any, Union

import pandas as pd
from mako.template import Template

import darshan
import darshan.cli


class ReportData:
    """
    Collects all of the metadata, tables, and figures
    required to generate a Darshan Summary Report.

    Parameters
    ----------
    log_path: path to a darshan log file.

    """
    def __init__(self, log_path: str):
        # store the log path and use it to generate the report
        self.log_path = log_path
        # store the report
        self.report = darshan.DarshanReport(log_path, read_all=True)
        # create the header/footer
        self.get_header()
        self.get_footer()
        # create the metadata and module tables
        self.get_metadata_table()
        self.get_module_table()
        # collect the CSS stylesheet
        self.get_stylesheet()

    @staticmethod
    def get_full_command(report: darshan.report.DarshanReport) -> str:
        """
        Retrieves the full command line from the report metadata.

        Parameters
        ----------
        report: a ``darshan.DarshanReport``.

        Returns
        -------
        cmd : the full command line used to generate the darshan log.

        """
        # assign the executable from the report metadata
        cmd = report.metadata["exe"]
        if not cmd:
            # if there is no executable
            # label as not available
            cmd = "N/A"
        elif cmd.isdigit():
            # if it can be converted to an
            # integer label it anonymized
            cmd = "Anonymized"
        return cmd

    @staticmethod
    def get_runtime(report: darshan.report.DarshanReport) -> str:
        """
        Calculates the run time from the report metadata.

        Parameters
        ----------
        report: a ``darshan.DarshanReport``.

        Returns
        -------
        runtime : the calculated executable run time.

        """
        # calculate the run time
        runtime_val = float(
            report.metadata["job"]["end_time"] - report.metadata["job"]["start_time"]
        )
        if runtime_val < 1.0:
            # to prevent the displayed run time from being 0.0 seconds
            # label anything under 1 second as less than 1
            runtime = "< 1"
        else:
            runtime = str(runtime_val)
        return runtime

    def get_header(self):
        """
        Builds the header string for the summary report.
        """
        command = self.get_full_command(report=self.report)
        if command != "Anonymized":
            # when the command line hasn't been anonymized
            # extract the application from the command line
            app_name = os.path.basename(command.split()[0])
        else:
            app_name = command
        # collect the date from the time stamp
        date = datetime.date.fromtimestamp(self.report.metadata["job"]["start_time"])
        # the header is the application name and the log date
        self.header = f"{app_name} ({date})"

    def get_footer(self):
        """
        Builds the footer string for the summary report.
        """
        lib_ver = darshan.__version__
        self.footer = f"Summary report generated via PyDarshan v{lib_ver}"

    def get_metadata_table(self):
        """
        Builds the metadata table (in html form) for the summary report.
        """
        # assign the metadata dictionary
        job_data = self.report.metadata["job"]
        # TODO: once this is exposed, add the correct entry
        log_fmt_ver = "N/A"
        # build a dictionary with the appropriate metadata
        metadata_dict = {
            "Job ID": job_data["jobid"],
            "User ID": job_data["uid"],
            "# Processes": job_data["nprocs"],
            "Runtime (s)": self.get_runtime(report=self.report),
            "Start Time": datetime.datetime.fromtimestamp(job_data["start_time"]),
            "End Time": datetime.datetime.fromtimestamp(job_data["end_time"]),
            "Command": self.get_full_command(report=self.report),
            "Log Filename": os.path.basename(self.log_path),
            "Runtime Library Version": job_data["metadata"]["lib_ver"],
            "Log Format Version": log_fmt_ver,
        }
        # convert the dictionary into a dataframe
        metadata_df = pd.DataFrame.from_dict(data=metadata_dict, orient="index")
        # write out the table in html
        self.metadata_table = metadata_df.to_html(header=False, border=0)

    def get_module_table(self):
        """
        Builds the module table (in html form) for the summary report.
        """
        # construct a dictionary containing the module names
        # and their respective data stored in KiB
        module_dict = {}
        for mod in self.report.modules:
            # retrieve the module version and buffer sizes
            mod_version = self.report.modules[mod]["ver"]
            # retrieve the buffer size converted to KiB
            mod_buf_size = self.report.modules[mod]["len"] / 1024
            # create the key/value pairs for the dictionary
            key = f"{mod} (ver={mod_version})"
            val = f"{mod_buf_size:.2f} KiB"
            module_dict[key] = val

        # convert the module dictionary into a dataframe
        module_df = pd.DataFrame.from_dict(data=module_dict, orient="index")
        # write out the table in html
        self.module_table = module_df.to_html(header=False, border=0)

    def get_stylesheet(self):
        """
        Retrieves the locally stored CSS.
        """
        # get the path to the style sheet
        with importlib_resources.path(darshan.cli, "style.css") as path:
            # collect the css entries
            with open(path, "r") as f:
                self.stylesheet = "".join(f.readlines())


def setup_parser(parser: argparse.ArgumentParser):
    """
    Configures the command line arguments.

    Parameters
    ----------
    parser : command line argument parser.

    """
    parser.description = "Generates a Darshan Summary Report"
    
    parser.add_argument(
        "log_path",
        type=str,
        help="Specify path to darshan log.",
    )
    parser.add_argument("--output", type=str, help="Specify output filename.")


def main(args: Union[Any, None] = None):
    """
    Generates a Darshan Summary Report.

    Parameters
    ----------
    args: command line arguments.

    """
    if args is None:
        parser = argparse.ArgumentParser(description="")
        setup_parser(parser)
        args = parser.parse_args()

    log_path = args.log_path

    if args.output is None:
        # if no output is provided, use the log file
        # name to create the output filename
        log_filename = os.path.splitext(os.path.basename(log_path))[0]
        report_filename = f"{log_filename}_report.html"
    else:
        report_filename = args.output

    # collect the report data to feed into the template
    report_data = ReportData(log_path=log_path)

    with importlib_resources.path(darshan.cli, "base.html") as base_path:
        # load a template object using the base template
        template = Template(filename=str(base_path))
        # render the base template
        stream = template.render(report_data=report_data)
        with open(report_filename, "w") as f:
            # print a message so users know where to look for their report
            save_path = os.path.join(os.getcwd(), report_filename)
            print(
                f"Report generated successfully. \n"
                f"Saving report at location: {save_path}"
            )
            # save the rendered html
            f.write(stream)


if __name__ == "__main__":
    main()
