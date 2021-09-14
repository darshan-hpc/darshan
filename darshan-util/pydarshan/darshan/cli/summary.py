import os
import argparse
import datetime
import importlib.resources as pkg_resources
from typing import Any

import pandas as pd
from mako.template import Template

import darshan
import darshan.templates


def setup_parser(parser: Any):
    """
    Configures the command line arguments.

    Parameters
    ----------
    parser : ArgumentParser object
        Command line argument parser.
    """
    parser.add_argument(
        "log_path",
        type=str,
        help="Specify path to darshan log.",
    )
    parser.add_argument("--output", type=str, help="Specify output filename.")


def get_runtime(report: Any):
    """
    Calculates the run time from the report metadata.
    """
    # calculate the run time
    runtime = float(
        report.metadata["job"]["end_time"] - report.metadata["job"]["start_time"]
    )
    if runtime < 1.0:
        # to prevent the displayed run time from being 0.0 seconds
        # label anything under 1 second as less than 1
        runtime = "< 1"
    return runtime


def get_full_command(report: Any):
    """
    Retrieves the full command line from the report metadata.
    """
    if report.metadata["exe"]:
        # some logs don't have a stored executable
        try:
            # to detect if the stored executable is not an
            # integer (anonymized) try to convert it to an integer
            command = int(report.metadata["exe"])
        except ValueError as err:
            if "invalid literal" in str(err):
                # if it cannot be converted to an integer
                # assign the executable
                command = report.metadata["exe"]
        else:
            # if it can be converted to an
            # integer label it anonymized
            command = "Anonymized"
    else:
        # if there is no executable
        # label as not available
        command = "N/A"
    return command


def get_header(report: Any):
    """
    Builds the header string for the summary report.
    """
    command = get_full_command(report=report)
    if command != "Anonymized":
        # when the command line hasn't been anonymized
        # extract the application from the command line
        app_name = os.path.basename(command.split()[0])
    else:
        app_name = command
    # collect the date from the time stamp
    date = datetime.date.fromtimestamp(report.metadata["job"]["start_time"])
    # the header is the application name and the log date
    header = f"{app_name} ({date})"
    return header


def get_footer(report: Any):
    """
    Builds the footer string for the summary report.
    """
    lib_ver = darshan.__version__
    footer = f"Summary report generated via PyDarshan v{lib_ver}"
    return footer


def get_metadata_table(report: Any, log_path: str):
    """
    Builds the metadata table (in html form) for the summary report.
    """
    # TODO: once this is exposed, add the correct entry
    log_fmt_ver = "N/A"

    # build a dictionary with the appropriate metadata
    metadata_dict = {
        "Job ID": report.metadata["job"]["jobid"],
        "User ID": report.metadata["job"]["uid"],
        "# Processes": report.metadata["job"]["nprocs"],
        "Runtime (s)": get_runtime(report=report),
        "Start Time": datetime.datetime.fromtimestamp(
            report.metadata["job"]["start_time"]
        ),
        "End Time": datetime.datetime.fromtimestamp(report.metadata["job"]["end_time"]),
        "Command": get_full_command(report=report),
        "Log Filename": os.path.basename(log_path),
        "Runtime Library Version": report.metadata["job"]["metadata"]["lib_ver"],
        "Log Format Version": log_fmt_ver,
    }

    # convert the dictionary into a dataframe
    metadata_df = pd.DataFrame.from_dict(data=metadata_dict, orient="index")
    # write out the table in html
    metadata_table = metadata_df.to_html(header=False, border=0)
    return metadata_table


def get_module_table(report: Any):
    """
    Builds the module table (in HTML form) for the summary report.
    """
    # construct a dictionary containing the module names
    # and their respective data stored in KiB
    module_dict = {}
    for mod in report.modules:
        # retrieve the module version and buffer sizes
        mod_version = report.modules[mod]["ver"]
        # retrieve the buffer size converted to KiB
        mod_buf_size = report.modules[mod]["len"] / 1024
        # create the key/value pairs for the dictionary
        key = f"{mod} (ver={mod_version})"
        val = f"{mod_buf_size:.2f} KiB"
        module_dict[key] = val

    # convert the module dictionary into a dataframe
    module_df = pd.DataFrame.from_dict(data=module_dict, orient="index")
    # write out the table in html
    module_table = module_df.to_html(header=False, border=0)
    return module_table


def build_template_dict(report: Any, log_path: str):
    """
    Builds a dictionary to contain all summary report information.
    """
    temp_dict = {}
    # add entries for the header and footer
    temp_dict["header"] = get_header(report=report)
    temp_dict["footer"] = get_footer(report=report)
    # store the table containing all of the darshan log metadata
    temp_dict["metadata_table"] = get_metadata_table(report=report, log_path=log_path)
    # store the table containing all of the module data
    temp_dict["module_table"] = get_module_table(report=report)
    return temp_dict


def main(args: Any = None):
    """
    Generates the summary report.
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

    # collect the darshan report
    report = darshan.DarshanReport(log_path, read_all=True, dtype="pandas")

    # build the dictionary to be fed into the template
    template_dict = build_template_dict(report=report, log_path=log_path)

    template_path = pkg_resources.path(darshan.templates, "")
    with template_path as path:
        # get the path to the base template
        temp_path = os.path.join(str(path), "base.html")
        # load a template object using the base template
        template = Template(filename=temp_path)
        # render the base template
        stream = template.render(template_dict=template_dict)
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
