"""
Module for generating the Darshan job summary.
"""
import os
import sys
import glob
import argparse
import tempfile

import numpy as np
from fpdf import FPDF

import darshan
from darshan.experimental.plots.plot_dxt_heatmap import plot_heatmap
from darshan.experimental.plots.plot_opcounts import plot_opcounts


def truncate_label(label, label_width, max_width):
    """
    Truncates a label based on the maximum width of the label.

    Parameters
    ----------
    label : str
        The label to be altered.
    label_width : float
        The width of the label in points units.
    max_width : float
        The maximum width of the label in points units.

    Returns
    -------
    truncated_label : str
        The altered label with length less than the input maximum label width.
    """
    # calculate the number of characters per point
    chars_per_pt = len(label) / label_width
    # calculate the number of characters we can keep in the string based on
    # the characters/point estimated above
    # NOTE: this is an approximate value and likely changes with the string
    # since helvetica is not a monospaced font
    n_available_chars = chars_per_pt * max_width
    # the first index is just the first half of the string
    first_idx = int(n_available_chars / 2)
    # the second index is the last half minus 3, adjusted for the 3 dots
    second_idx = first_idx - 3
    truncated_label = label[:first_idx] + "..." + label[-second_idx:]
    return truncated_label


class PDF(FPDF):
    def __init__(self, orientation, unit, format, metadata_dict):
        super().__init__(orientation, unit, format)
        # add the metadata to the PDF object
        self.jobid = metadata_dict["job"]["jobid"]
        self.uid = metadata_dict["job"]["uid"]
        self.nprocs = metadata_dict["job"]["nprocs"]
        self.start_time = metadata_dict["job"]["start_time"]
        self.end_time = metadata_dict["job"]["end_time"]
        self.executable = metadata_dict["exe"]
        # set all margins to 1" (72 points)
        margin_w = 72
        self.set_margin(margin=margin_w)
        # store the x/y values of each margin for later use
        self.left_margin = margin_w
        self.top_margin = margin_w
        self.right_margin = (8.5 * 72) - margin_w
        self.bottom_margin = (11 * 72) - margin_w

    def header(self):
        """
        Creates the header for each page, composed of a
        page-width (excluding the margins) horizontal
        black line and the page number label.
        """
        # set the height of the line
        line_y = 62
        # create the list of points for the line with x
        # values at the left/right margin locations
        point_list = [(self.left_margin, line_y), (self.right_margin, line_y)]
        # draw line at top of page to act as a border
        self.polyline(point_list=point_list)
        # set the font properties for the page number label
        self.set_font(family="helvetica", size=10)
        # set the height of the page number label (just above line)
        self.set_y(48)
        # create the page number label and add it to the header right-justified
        page_no_label = str(self.page_no()) + " of {nb}"
        self.cell(w=0, h=None, txt=page_no_label, border=False, align="R")

    def footer(self):
        """
        Creates the footer for each page, composed of a
        page-width (excluding the margins) horizontal
        black line with the executable shown just below.

        Notes
        -----
        If the executable is too long to fit across the
        page it will be truncated such that it fits.
        """
        # since the bottom of the page is 720 (10" * 72), set the
        # line 2 points above to leave room for the label below
        y_pos = self.bottom_margin - 2
        # create the list of points for the line with x
        # values at the left/right margin locations
        point_list = [(self.left_margin, y_pos), (self.right_margin, y_pos)]
        # draw line at bottom of page to act as a border
        self.polyline(point_list=point_list)
        # collect the executable string/label
        exec_label = self.executable
        # set the font properties for the executable label
        self.set_font(family="helvetica", size=8)
        # get the width of the title string in the current font size
        exec_width = self.get_string_width(s=exec_label)
        # the max executable width is the effective page width
        max_exec_width = self.epw
        if exec_width > max_exec_width:
            # if the executable label is greater than the page width
            # truncate the label and put (3) dots in the middle
            exec_label = truncate_label(
                label=exec_label, label_width=exec_width, max_width=max_exec_width
            )
        # draw the executable label at the very bottom of the page
        self.set_y(self.bottom_margin)
        self.cell(w=0, h=None, txt=exec_label, ln=0, align="C", center=True)

    def set_title(self, log_filename):
        """
        Adds a title to the first page of the PDF.

        Parameters
        ----------
        log_filename : str
            The log file name string used in the title.

        Notes
        -----
        If the title string is too long to fit next to the page
        number label, the title string will be truncated.
        """
        # create the title label using markdown formatting
        title_label = f"**Darshan Job Summary:** {log_filename}"
        # set the font properties for the title label
        self.set_font(family="helvetica", size=12)
        # get the width of the title string in the current font size
        title_width = self.get_string_width(s=title_label)
        # set the max title width to compensate
        # for the page number label
        max_title_width = self.epw - 40
        if title_width > max_title_width:
            # if the title label is longer than the maximum
            # truncate the label and put (3) dots in the middle
            title_label = truncate_label(
                label=title_label, label_width=title_width, max_width=max_title_width
            )
        # draw the title just above the header line, left-justified
        self.set_y(48)
        self.cell(
            w=0,
            h=None,
            txt=title_label,
            border=False,
            align="L",
            center=False,
            markdown=True,
        )

    def add_metadata_table(self):
        """
        Adds a 4-cell horizontal table at the top of
        the first page containing the following
        metadata: jobid, uid, nprocs, and runtime.
        """
        # create the first 3 labels
        jobid_label = f"jobid: {self.jobid}"
        uid_label = f"uid: {self.uid}"
        nprocs_label = f"nprocs: {self.nprocs}"
        # special handling for the run time label since the darshan
        # logs return integer values
        runtime = self.end_time - self.start_time
        if runtime < 1:
            # TODO: it would be nice to just get the decimal version of the
            # run time instead of using this work-around so the run time
            # agrees with the heatmap
            runtime_label = "runtime: < 1 s"
        else:
            runtime_label = f"runtime: {runtime} s"

        # set the font properties for all labels
        self.set_font(family="helvetica", size=10)
        # set the height just below the header
        self.set_y(67)
        # make the cells 1/4 the page width and tall enough to fit the labels
        cell_width = self.epw / 4
        cell_height = 20
        # make a cell for each label with the text left-justified, and each
        # cell just to the right of the previous cell
        self.cell(
            w=cell_width, h=cell_height, txt=jobid_label, border=True, ln=0, align="L"
        )
        self.cell(
            w=cell_width, h=cell_height, txt=uid_label, border=True, ln=0, align="L"
        )
        self.cell(
            w=cell_width, h=cell_height, txt=nprocs_label, border=True, ln=0, align="L"
        )
        self.cell(
            w=cell_width, h=cell_height, txt=runtime_label, border=True, ln=0, align="L"
        )

    def add_figure(
        self,
        log_path,
        plotting_func,
        func_args,
        fig_y,
        fig_x=None,
        fig_w=None,
        report=None,
        center=False,
    ):
        """
        Creates a figure from an input path or darshan
        report, stores it in a temporary directory, then
        adds it to the PDF object at the desired location.

        Parameters
        ----------
        log_path : str
            Path to a darshan log file.
        plotting_func : Callable
            Function used to create the `matplotlib` figure.
        func_args : dict
            Arguments for the input plotting function.
        fig_y : float
            The y-location to place the figure in the PDF.
        fig_x : float (optional)
            The x-location to place the figure in the PDF. If not input,
            x-value will be set such that the figure is center-justified.
        fig_w : float (optional)
            The width of the figure in the PDF. If a width is not input,
            will default to page width.
        report : DarshanReport (optional)
            The darshan report object to pass into the plotting function.
        center : bool (optional)
            Binary used to turn center-justification
            on/off. Default is off.
        """
        if report is None:
            # no report input cases use a function that only
            # requires the input log file path
            try:
                fig = plotting_func(log_path, **func_args)
            except KeyError as err:
                # for cases with no DXT data, return
                if "DXT_POSIX" in str(err):
                    # TODO: figure out how to properly handle this case
                    # the heatmap code should probably be able to handle
                    # this on its own since many logs will not have DXT data
                    return
        else:
            # if a report is input use it to generate the figure
            fig = plotting_func(report, **func_args)

        if fig_x is None:
            # if no x-value is input, turn centering on
            center = True

        if fig_w is None:
            # if no figure width is input, set the width
            # to the effective page width (adjusted for margins)
            fig_w = self.epw

        # if center-justification is turned on, calculate the appropriate
        # x value, compensating for the margins and the figure width
        if center:
            # the page midpoint is the average of the locations
            # of the left and right margins
            page_midpoint = (self.left_margin + self.right_margin) / 2
            # since the figures are anchored on the left hand
            # side, the appropriate x-value is the difference
            # of the page midpoint and half the figure width
            fig_x = page_midpoint - (fig_w / 2)

        with tempfile.TemporaryDirectory() as tmpdirname:
            # save the figure in a temporary directory
            fig_savepath = os.path.join(tmpdirname, "fig.png")
            fig.savefig(fig_savepath, dpi=300)
            # add the saved image to the PDF at the
            # desired location and dimensions
            self.image(name=fig_savepath, x=fig_x, y=fig_y, w=fig_w)


def setup_parser(parser):
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


def main(args=None):
    """
    Generates a PDF report containing a summary
    for an input Darshan log file.
    """
    if args is None:
        # if no arguments are input, use setup_parser()
        parser = argparse.ArgumentParser()
        setup_parser(parser=parser)
        args = parser.parse_args()

    log_path = args.log_path
    # get the filename from the log path
    log_filename = os.path.basename(log_path)
    # load the darshan report
    report = darshan.DarshanReport(log_path, read_all=True)
    # create the PDF object in portrait orientation, letter
    # dimensions (8.5 x 11"), and using point units (72 points/inch)
    pdf = PDF(
        orientation="portrait",
        unit="pt",
        format="letter",
        metadata_dict=report.data["metadata"],
    )
    # add the first page
    pdf.add_page()
    # set the title
    pdf.set_title(log_filename=log_filename)
    # add the summary table at the top of page 1 that contains
    # the jobid/uid/nprocs/runtime info
    pdf.add_metadata_table()
    # construct dictionaries of the arguments for each figure/function
    hmap_func_args = {"mods": ["DXT_POSIX"], "ops": ["read", "write"], "xbins": 200}
    opct_func_args = {"ax": None}
    # pick the figure widths
    fig_w = 400
    # enable the darshan experimental module
    darshan.enable_experimental()
    # add the figures to the PDF
    pdf.add_figure(
        log_path=log_path,
        plotting_func=plot_heatmap,
        func_args=hmap_func_args,
        fig_y=100,
        fig_w=fig_w,
        center=True,
    )
    pdf.add_figure(
        log_path=log_path,
        plotting_func=plot_opcounts,
        func_args=opct_func_args,
        fig_y=410,
        fig_w=fig_w,
        center=True,
        report=report,
    )
    # save the PDF at the desired location with the filename
    fname_noext = os.path.splitext(log_filename)[0]
    output_filename = os.getcwd() + f"/report_{fname_noext}.pdf"
    pdf.output(output_filename)


if __name__ == "__main__":
    main()
