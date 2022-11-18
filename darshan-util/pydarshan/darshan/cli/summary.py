import sys
import os
import io
import base64
import argparse
import datetime
from collections import OrderedDict
if sys.version_info >= (3, 7):
    import importlib.resources as importlib_resources
else:
    import importlib_resources

from typing import Any, Union, Callable

import pandas as pd
from mako.template import Template

import darshan
import darshan.cli
from darshan.experimental.plots import (
    plot_dxt_heatmap,
    plot_io_cost,
    plot_common_access_table,
    plot_access_histogram,
    plot_opcounts,
    data_access_by_filesystem,
)

darshan.enable_experimental()


class ReportFigure:
    """
    Stores info for each figure in `ReportData.register_figures`.

    Parameters
    ----------
    section_title : the title of the section the figure belongs to.

    fig_title : the title of the figure.

    fig_func : the function used to generate the figure.

    fig_args : the keyword arguments used for `fig_func`

    fig_description : description of the figure, typically used as the caption.

    fig_width : the width of the figure in pixels.

    """
    def __init__(
        self,
        section_title: str,
        fig_title: str,
        fig_func: Union[Callable, None],
        fig_args: dict,
        fig_description: str = "",
        fig_width: int = 500,
    ):
        self.section_title = section_title
        if not fig_title:
            fig_title = "&nbsp;"
        self.fig_title = fig_title
        self.fig_func = fig_func
        self.fig_args = fig_args
        self.fig_description = fig_description
        self.fig_width = fig_width
        # temporary handling for DXT disabled cases
        # so special error message can be passed
        # in place of an encoded image
        self.fig_html = None
        if self.fig_func:
            self.generate_fig()

    @staticmethod
    def get_encoded_fig(mpl_fig: Any):
        """
        Encode a `matplotlib` figure using base64 encoding.

        Parameters
        ----------
        mpl_fig : ``matplotlib.figure`` object.

        Returns
        -------
        encoded_fig : base64 encoded image.

        """
        tmpfile = io.BytesIO()
        mpl_fig.savefig(tmpfile, format="png", dpi=300)
        encoded_fig = base64.b64encode(tmpfile.getvalue()).decode("utf-8")
        return encoded_fig

    def generate_fig(self):
        """
        Generate a figure using the figure data.
        """
        # generate the figure using the figure's
        # function and function arguments
        fig = self.fig_func(**self.fig_args)
        if hasattr(fig, "savefig"):
            # encode the matplotlib figure
            encoded = self.get_encoded_fig(mpl_fig=fig)
            # create the img string
            self.fig_html = f"<img src=data:image/png;base64,{encoded} alt={self.fig_title} width={self.fig_width}>"
        elif isinstance(fig, plot_common_access_table.DarshanReportTable):
            # retrieve html table from `DarshanReportTable`
            self.fig_html = fig.html
        else:
            err_msg = f"Figure of type {type(fig)} not supported."
            raise NotImplementedError(err_msg)

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
        # register the report figures
        self.register_figures()
        # use the figure data to build the report sections
        self.build_sections()
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
        # get the run time string
        runtime_val = report.metadata["job"]["run_time"]
        runtime = f'{runtime_val:.4f}'
        return runtime

    def get_header(self):
        """
        Builds the header string for the summary report.
        """
        command = self.get_full_command(report=self.report)
        if command in ["Anonymized", "N/A"]:
            app_name = command
        else:
            app_name = os.path.basename(command.split()[0])
        # collect the date from the time stamp
        date = datetime.date.fromtimestamp(self.report.metadata["job"]["start_time_sec"])
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
        # build a dictionary with the appropriate metadata
        metadata_dict = {
            "Job ID": job_data["jobid"],
            "User ID": job_data["uid"],
            "# Processes": job_data["nprocs"],
            "Run time (s)": self.get_runtime(report=self.report),
            "Start Time": datetime.datetime.fromtimestamp(job_data["start_time_sec"]),
            "End Time": datetime.datetime.fromtimestamp(job_data["end_time_sec"]),
            "Command Line": self.get_full_command(report=self.report),
        }
        # convert the dictionary into a dataframe
        metadata_df = pd.DataFrame.from_dict(data=metadata_dict, orient="index")
        # write out the table in html
        self.metadata_table = metadata_df.to_html(header=False, border=0)

    def get_module_table(self):
        """
        Builds the module table (in html form) for the summary report.
        """
        # construct a dictionary containing the module names,
        # their respective data stored in KiB, and the log metadata
        job_data = self.report.metadata["job"]
        module_dict= {
            "Log Filename": [os.path.basename(self.log_path), ""],
            "Runtime Library Version": [job_data["metadata"]["lib_ver"], ""],
            "Log Format Version": [job_data["log_ver"], ""],
        }

        for mod in self.report.modules:
            # retrieve the module version and buffer sizes
            mod_version = self.report.modules[mod]["ver"]
            # retrieve the buffer size converted to KiB
            mod_buf_size = self.report.modules[mod]["len"] / 1024
            # create the key/value pairs for the dictionary
            key = f"{mod} (ver={mod_version}) Module Data"
            val = f"{mod_buf_size:.2f} KiB"
            flag = ""
            if self.report.modules[mod]["partial_flag"]:
                msg = "Module data incomplete due to runtime memory or record count limits"
                flag = f"<p style='color:red'>&#x26A0; {msg}</p>"
            module_dict[key] = [val, flag]

        # convert the module dictionary into a dataframe
        module_df = pd.DataFrame.from_dict(data=module_dict, orient="index")
        # write out the table in html
        self.module_table = module_df.to_html(header=False, border=0, escape=False)

    def get_stylesheet(self):
        """
        Retrieves the locally stored CSS.
        """
        # get the path to the style sheet
        with importlib_resources.path(darshan.cli, "style.css") as path:
            # collect the css entries
            with open(path, "r") as f:
                self.stylesheet = "".join(f.readlines())

    def register_figures(self):
        """
        Collects and registers all figures in the report. This is the
        method users can edit to alter the report contents.

        Examples
        --------
        To add figures to the report, there are a few basic steps:

        1. Make the function used to generate the desired figure
           callable within the scope of this module.
        2. Create an entry in this method that contains all of the required
           information for the figure. This will be described in detail below.
        3. Use the figure information to create a `ReportFigure`.
        4. Add the `ReportFigure` to `ReportData.figures`.

        Step #1 is handled by importing the function from the module it is
        defined in. For step #2, each figure in the report must have the
        following defined:

        * Section title: the desired section for the figure to be placed.
          If the section title is unique to the report, a new section
          will be created for that figure.
        * Figure title: the title of the figure
        * Figure function: the function used to produce the figure. This
          must be callable within the scope of this module (step #1).
        * Figure arguments: the arguments for the figure function

        Some additional details can be provided as well:

        * Figure description: description of the figure used for the caption
        * Figure width: width of the figure in pixels

        To complete steps 2-4, an entry can be added to this method,
        where a typical entry will look like the following:

            # collect all of the info in a dictionary (step #2)
            fig_params = {
                "section_title": "Example Section Title",
                "fig_title": "Example Title",
                "fig_func": example_module.example_function,
                "fig_args": dict(report=self.report),
                "fig_description": "Example Caption",
                "fig_width": 500,
            }
            # feed the dictionary into ReportFigure (step #3)
            example_fig = ReportFigure(**fig_params)
            # add the ReportFigure to ReportData.figures (step #4)
            self.figures.append(example_fig)

        The order of the sections and figures is based on the order in which
        they are placed in `self.figures`. Since the DXT figure(s) are added
        first, they show up at the very top of the figure list.

        """
        self.figures = []

        #########################################
        ## Add the runtime and/or DXT heat map(s)
        #########################################
        # if either or both modules are present, register their figures
        hmap_description = (
            "Heat map of I/O (in bytes) over time broken down by MPI rank. "
            "Bins are populated based on the number of bytes read/written in "
            "the given time interval. The top edge bar graph sums each time "
            "slice across ranks to show aggregate I/O volume over time, while the "
            "right edge bar graph sums each rank across time slices to show I/O "
            "distribution across ranks."
        )
        modules_avail = set(self.report.modules)
        hmap_modules = ["HEATMAP", "DXT_POSIX", "DXT_MPIIO"]
        hmap_grid = OrderedDict([["HEATMAP_MPIIO", None],
                                 ["DXT_MPIIO", None],
                                 ["HEATMAP_POSIX", None],
                                 ["DXT_POSIX", None],
                                 ["HEATMAP_STDIO", None],
                                ])
        if not set(hmap_modules).isdisjoint(modules_avail):
            for mod in hmap_modules:
                if mod in self.report.modules:
                    if mod == "HEATMAP":
                        for possible_submodule in self.report.heatmaps:
                            possible_submodule = possible_submodule.replace("-", "")
                            heatmap_fig = ReportFigure(
                                section_title="I/O Summary",
                                fig_title=f"Heat Map: {mod} {possible_submodule}",
                                fig_func=plot_dxt_heatmap.plot_heatmap,
                                fig_args=dict(report=self.report, mod=mod, submodule=possible_submodule),
                                fig_description=hmap_description,
                            )
                            hmap_grid[f"HEATMAP_{possible_submodule}"] = heatmap_fig
                    else:
                        heatmap_fig = ReportFigure(
                            section_title="I/O Summary",
                            fig_title=f"Heat Map: {mod}",
                            fig_func=plot_dxt_heatmap.plot_heatmap,
                            fig_args=dict(report=self.report, mod=mod),
                            fig_description=hmap_description,
                        )
                        hmap_grid[mod] = heatmap_fig
            for heatmap_fig in hmap_grid.values():
                if heatmap_fig:
                    self.figures.append(heatmap_fig)
        else:
            url = (
                "https://www.mcs.anl.gov/research/projects/darshan/docs/darshan"
                "-runtime.html#_using_the_darshan_extended_tracing_dxt_module"
            )
            temp_message = (
                    "Heatmap data is not available for this job. Consider "
                    "enabling the runtime heatmap module (available and "
                    "enabled by default in Darshan 3.4 or newer) or the "
                    "DXT tracing module (available and optionally enabled "
                    "in Darshan 3.1.3 or newer) if you would like to analyze "
                    "I/O intensity using a heatmap visualization. "
                    "For details, see "
                    f"the <a href={url}>Darshan-runtime documentation</a>."
            )
            fig = ReportFigure(
                section_title="I/O Summary",
                fig_title="Heat Map",
                fig_func=None,
                fig_args=None,
                fig_description=temp_message,
            )
            self.figures.append(fig)

        ################################
        ## Cross-Module Comparisons
        ################################

        # add the I/O cost stacked bar graph
        url = "https://www.mcs.anl.gov/research/projects/darshan/docs/darshan-util.html"

        io_cost_description = (
            "Average (across all ranks) amount of run time that each process "
            "spent performing I/O, broken down by access type. See the right "
            "edge bar graph on heat maps in preceding section to indicate if "
            "I/O activity was balanced across processes."
        )
        io_cost_params = {
            "section_title": "Cross-Module Comparisons",
            "fig_title": "I/O Cost",
            "fig_func": plot_io_cost,
            "fig_args": dict(report=self.report),
            "fig_description": io_cost_description,
            "fig_width": 350,
        }
        io_cost_fig = ReportFigure(**io_cost_params)
        self.figures.append(io_cost_fig)

        ################################
        ## Per-Module Statistics
        ################################

        # for the operation counts, since the `H5D` variant contains
        # both modules' data, we either want `H5F` or `H5D`, not both
        opcounts_mods = ["POSIX", "MPI-IO", "STDIO"]
        if "H5D" in self.report.modules:
            opcounts_mods.append("H5D")
        elif "H5F" in self.report.modules:
            opcounts_mods.append("H5F")
        # for the operation counts, since the `PNETCDF_VAR` variant contains
        # both modules' data, we either want `PNETCDF_FILE` or `PNETCDF_VAR`, not both
        if "PNETCDF_VAR" in self.report.modules:
            opcounts_mods.append("PNETCDF_VAR")
        elif "PNETCDF_FILE" in self.report.modules:
            opcounts_mods.append("PNETCDF_FILE")

        for mod in self.report.modules:

            if "H5" in mod:
                sect_title = "Per-Module Statistics: HDF5"
            elif "PNETCDF" in mod:
                sect_title = "Per-Module Statistics: PNETCDF"
            else:
                sect_title = f"Per-Module Statistics: {mod}"

            if mod in ["POSIX", "MPI-IO", "H5D", "PNETCDF_VAR"]:
                access_hist_description = (
                    "Histogram of read and write access sizes. The specific values "
                    "of the most frequently occurring access sizes can be found in "
                    "the <i>Common Access Sizes</i> table."
                )
                access_hist_fig = ReportFigure(
                    section_title=sect_title,
                    fig_title="Access Sizes",
                    fig_func=plot_access_histogram,
                    fig_args=dict(report=self.report, mod=mod),
                    fig_description=access_hist_description,
                    fig_width=350,
                )
                self.figures.append(access_hist_fig)
                if mod == "MPI-IO":
                    com_acc_tbl_description = (
                        "NOTE: MPI-IO accesses are given in "
                        "terms of aggregate datatype size."
                    )
                else:
                    com_acc_tbl_description = ""
                com_acc_tbl_fig = ReportFigure(
                    section_title=sect_title,
                    fig_title="Common Access Sizes",
                    fig_func=plot_common_access_table.plot_common_access_table,
                    fig_args=dict(report=self.report, mod=mod),
                    fig_description=com_acc_tbl_description,
                    fig_width=350,
                )
                self.figures.append(com_acc_tbl_fig)

            # add the operation counts figure
            if mod in opcounts_mods:
                opcount_fig = ReportFigure(
                    section_title=sect_title,
                    fig_title="Operation Counts",
                    fig_func=plot_opcounts,
                    fig_args=dict(report=self.report, mod=mod),
                    fig_description="Histogram of I/O operation frequency.",
                    fig_width=350,
                )
                self.figures.append(opcount_fig)

        #########################
        # Data Access by Category
        if not {"POSIX", "STDIO"}.isdisjoint(set(self.report.modules)):
            data_access_by_cat_fig = ReportFigure(
                section_title="Data Access by Category",
                fig_title="",
                fig_func=data_access_by_filesystem.plot_with_report,
                fig_args=dict(report=self.report, num_cats=8),
                fig_description="Summary of data access volume "
                                "categorized by storage "
                                "target (e.g., file system "
                                "mount point) and sorted by volume.",
                fig_width=500,
            )
            self.figures.append(data_access_by_cat_fig)



    def build_sections(self):
        """
        Uses figure info to generate the unique sections
        and places the figures in their sections.
        """
        self.sections = {}
        for fig in self.figures:
            # if a section title is not already in sections, add
            # the section title and a corresponding empty dictionary
            # to store its figures
            if fig.section_title not in self.sections:
                self.sections[fig.section_title] = {}
            # add the image to its corresponding section
            self.sections[fig.section_title][fig.fig_title] = fig


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
