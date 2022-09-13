import os
import sys
import time

if sys.version_info >= (3, 7):
    import importlib.resources as importlib_resources
else:
    import importlib_resources

import darshan
from darshan.cli.summary import ReportData

import rich_click as click
from mako.template import Template

click.rich_click.OPTION_GROUPS = {
    "darshan_summary": [
        {
            "name": "Basic usage",
            "options": ["--log_path", "--output", "--help"],
        },
        {
            "name": "Heatmap Options",
            "options": ["--split-heatmaps", "--force-dxt"],
        },
    ],
}


@click.command()
@click.option('--log_path', help='Path to darshan log file.', required=True)
@click.option('--output', help='Specify output filename.')
# TODO: actually implement heatmap options in summary report
@click.option('--split-heatmaps',
              is_flag=True,
              show_default=True,
              default=False,
              help='Separate heatmaps for read and write IO activity.')
@click.option('--force-dxt',
              is_flag=True,
              show_default=True,
              default=False,
              help='Force plotting of DXT heatmaps even when large.')
def summary_report(log_path,
                   output,
                   split_heatmaps,
                   force_dxt):
    start = time.perf_counter()
    if output is None:
        # if no output is provided, use the log file
        # name to create the output filename
        log_filename = os.path.splitext(os.path.basename(log_path))[0]
        report_filename = f"{log_filename}_report.html"
    else:
        report_filename = output

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
            click.echo(
                f"Report generated successfully. \n"
                f"Saving report at location: {save_path}"
            )
            # save the rendered html
            f.write(stream)
            end = time.perf_counter()
            total_time_sec = end - start
            click.echo(f"Report generation time (s): {total_time_sec:.2f}")
