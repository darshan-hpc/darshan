#!/usr/bin/env python
# coding: utf-8

# DarshanUtils for Python for processing APMPI records
#
# This script gives an overview of features provided by the Python bindings for DarshanUtils.

# By default all APMPI module records, metadata, and the name records are loaded when opening a Darshan log:

import argparse
import darshan
import cffi
import numpy
import pandas
import matplotlib
import matplotlib.pyplot as plt
import seaborn as sns

#import pprint
import pandas as pd
import numpy as np
import jinja2
import logging

from darshan.backend.cffi_backend import ffi

logger = logging.getLogger(__name__)
from darshan.report import DarshanReport
import darshan.backend.cffi_backend as backend
import darshan
import time
'''
from rich import print  as rprint
from rich import pretty
from rich.panel import Panel
from rich import inspect
from rich.color import Color
from rich.console import Console
console = Console()
'''
from matplotlib.backends.backend_pdf import FigureCanvasPdf, PdfPages
from matplotlib.figure import Figure

#pp = pprint.PrettyPrinter()
#pretty.install()
#color = Color.parse("blue")

#inspect(color, methods=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--quiet",
        dest="quiet",
        action="store_true",
        default=False,
        help="Surpress zero count calls",
    )
    parser.add_argument(
        "logname", metavar="logname", type=str, nargs=1, help="Logname to parse"
    )
    args = parser.parse_args()

    report = darshan.DarshanReport(args.logname[0], read_all=False)
    report.info()
    
    if "APMPI" not in report.modules:
        print("This log does not contain AutoPerf MPI data")
        return
    r = report.mod_read_all_apmpi_records("APMPI")
    
    report.update_name_records()
    report.info()
    
    pdf = matplotlib.backends.backend_pdf.PdfPages("apmpi_output.pdf")

    header_rec = report.records["APMPI"][0]
    sync_flag = header_rec["sync_flag"]
    print("sync_flag= ", sync_flag)
    print(
        "APMPI Variance in total mpi time: ", header_rec["variance_total_mpitime"], "\n"
    )
    if sync_flag:
        print(
            "APMPI Variance in total mpi sync time: ",
            header_rec["variance_total_mpisynctime"],
        )

    df_apmpi = pd.DataFrame()
    list_mpiop = []
    list_rank = []
    for rec in report.records["APMPI"][
        1:
    ]:  # skip the first record which is header record
        mpi_nonzero_callcount = []
        for k, v in rec["all_counters"].items():
            if k.endswith("_CALL_COUNT") and v > 0:
                mpi_nonzero_callcount.append(k[: -(len("CALL_COUNT"))])

        df_rank = pd.DataFrame()
        for mpiop in mpi_nonzero_callcount:
            ncall = mpiop
            ncount = mpiop + "CALL_COUNT"
            nsize = mpiop + "TOTAL_BYTES"
            h0 = mpiop + "MSG_SIZE_AGG_0_256"
            h1 = mpiop + "MSG_SIZE_AGG_256_1K"
            h2 = mpiop + "MSG_SIZE_AGG_1K_8K"
            h3 = mpiop + "MSG_SIZE_AGG_8K_256K"
            h4 = mpiop + "MSG_SIZE_AGG_256K_1M"
            h5 = mpiop + "MSG_SIZE_AGG_1M_PLUS"
            ntime = mpiop + "TOTAL_TIME"
            mintime = mpiop + "MIN_TIME"
            maxtime = mpiop + "MAX_TIME"
            if sync_flag:
                totalsync = mpiop + "TOTAL_SYNC_TIME"

            mpiopstat = {}
            mpiopstat["Rank"] = rec["rank"]
            mpiopstat["Node_ID"] = rec["node_name"]
            mpiopstat["Call"] = ncall[:-1]
            mpiopstat["Total_Time"] = rec["all_counters"][ntime]
            mpiopstat["Count"] = rec["all_counters"][ncount]
            mpiopstat["Total_Bytes"] = rec["all_counters"].get(nsize, None)
            mpiopstat["[0-256B]"] = rec["all_counters"].get(h0, None)
            mpiopstat["[256-1KB]"] = rec["all_counters"].get(h1, None)
            mpiopstat["[1K-8KB]"] = rec["all_counters"].get(h2, None)
            mpiopstat["[8K-256KB]"] = rec["all_counters"].get(h3, None)
            mpiopstat["256K-1MB"] = rec["all_counters"].get(h4, None)
            mpiopstat["[>1MB]"] = rec["all_counters"].get(h5, None)
            mpiopstat["Min_Time"] = rec["all_counters"][mintime]
            mpiopstat["Max_Time"] = rec["all_counters"][maxtime]
            if sync_flag and (totalsync in rec["all_counters"]):
                mpiopstat["Total_SYNC_Time"] = rec["all_counters"][totalsync]

            list_mpiop.append(mpiopstat)

        rankstat = {}
        rankstat["Rank"] = rec["rank"]
        rankstat["Node_ID"] = rec["node_name"]
        rankstat["Call"] = "Total_MPI_time"
        rankstat["Total_Time"] = rec["all_counters"]["MPI_TOTAL_COMM_TIME"]
        list_rank.append(rankstat)
    df_rank = pd.DataFrame(list_rank)
    avg_total_time = df_rank["Total_Time"].mean()
    max_total_time = df_rank["Total_Time"].max()
    min_total_time = df_rank["Total_Time"].min()
    max_rank = df_rank.loc[df_rank["Total_Time"].idxmax()]["Rank"]
    min_rank = df_rank.loc[df_rank["Total_Time"].idxmin()]["Rank"]
    # assumption: row index and rank id are same in df_rank 
    # .. need to check if that is an incorrect assumption
    mean_rank = (
        (df_rank["Total_Time"] - df_rank["Total_Time"].mean()).abs().argsort()[:1][0]
    )
    pd.set_option("display.max_rows", None, "display.max_columns", None)

    list_combined = list_mpiop + list_rank
    df_apmpi = pd.DataFrame(list_combined)
    df_apmpi = df_apmpi.sort_values(by=["Rank", "Total_Time"], ascending=[True, False])
    df_call = df_apmpi[['Call', 'Total_Time']]
    #print("MPI stats for rank with maximum MPI time")#, border_style="blue")
    print("MPI stats for rank with maximum MPI time\n", df_apmpi.loc[df_apmpi["Rank"] == max_rank])
    print("\n\n")
    print("MPI stats for rank with minimum MPI time")# border_style="blue")
    print(df_apmpi.loc[df_apmpi["Rank"] == min_rank])
    print("\n\n")
    print("MPI stats for rank with mean MPI time")#, border_style="blue")
    print(df_apmpi.loc[df_apmpi["Rank"] == mean_rank])
    # print(df_apmpi)
    df_apmpi.to_csv('apmpi.csv', index=False)
    df_rank.to_csv('apmpi_rank.csv', index=False)

    env = jinja2.Environment(loader=jinja2.FileSystemLoader(searchpath='.'))
    template = env.get_template('template.html')
    html = template.render()

    fig = Figure()
    #fig, ax = plt.subplots(figsize=(4.5,2.5))
    ax = fig.gca()
    sns_violin = sns.violinplot(x="Call", y="Total_Time", ax=ax, data=df_call)
    sns_violin.set_xticklabels(sns_violin.get_xticklabels(), rotation=60, size=6.5)
    sns_violin.set_yticklabels(sns_violin.get_yticks(), rotation=0, size=6.5)
    sns_violin.set_xlabel('')
    sns_violin.set_ylabel('Time (seconds)', size=7)
    #sns.despine();
    fig.savefig('plot1.svg')

    fig = Figure()
    ax = fig.gca()
    sns_plot = sns.scatterplot(x="Rank", y="Total_Time", ax=ax, data=df_apmpi, s=3)
    sns_plot.set_xticklabels(sns_plot.get_xticklabels(), rotation=0, size=6.5)
    sns_plot.set_yticklabels(sns_plot.get_yticks(), rotation=0, size=6.5)
    sns_plot.set_xlabel('Rank', size=8)
    sns_plot.set_ylabel('Time (seconds)', size=8)
    #sns.despine();
    fig.savefig('plot2.svg')

    # Write the HTML file
    with open('report.html', 'w') as f:
    	f.write(html)
    return


if __name__ == "__main__":
    main()
'''
    fig = Figure()

    ax = fig.gca()
    ax.plot(df_rank["Rank"], df_rank["Total_Time"])
    ax.set_xlabel("Rank")
    ax.set_ylabel("MPI Total time(s)")
    canvas = FigureCanvasPdf(fig)
    canvas.print_figure(pdf)
    fig = Figure()
    ax = fig.gca()
    #fig2.plot(df_apmpi.loc[df_apmpi["Rank"] == max_rank])
    ax.plot(df_apmpi.loc[df_apmpi["Rank"] == max_rank]["Call"], df_apmpi.loc[df_apmpi["Rank"] == max_rank]["Total_Time"])
    ax.set_xlabel("MPI OP")
    ax.set_ylabel("Total time(s)")
    canvas = FigureCanvasPdf(fig)
    #canvas.print_figure(pdf)
    fig = Figure()
    ax = fig.gca()
    ax.plot(df_apmpi.loc[df_apmpi["Rank"] == min_rank]["Call"], df_apmpi.loc[df_apmpi["Rank"] == min_rank]["Total_Time"])
    ax.set_xlabel("MPI OP")
    ax.set_ylabel("Total time(s)")
    ax.set_title("Min rank MPI times")
    canvas = FigureCanvasPdf(fig)
    #canvas.print_figure(pdf)
    #fig3.plot(df_apmpi.loc[df_apmpi["Rank"] == min_rank])
'''
