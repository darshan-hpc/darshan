#!/usr/bin/env python
# DarshanUtils for Python for processing APSS records

# This script gives an overview of features provided by the Python bindings for DarshanUtils.

# By default all APSS module records, metadata, and the name records are loaded when opening a Darshan log:

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
from matplotlib.backends.backend_pdf import FigureCanvasPdf, PdfPages
from matplotlib.figure import Figure


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
   
    if "APSS" not in report.modules:
        print("This log does not contain AutoPerf XC data")
        return
    r = report.mod_read_all_apss_records("APSS")

    report.update_name_records()
    report.info()
   
    #pdf = matplotlib.backends.backend_pdf.PdfPages("apss_output.pdf")

    header_rec = report.records["APSS"][0]
    
    for rec in report.records["APSS"][1:]:
	# skip the first record which is header record
        print(rec)

    return

if __name__ == '__main__':
    main()
