import math

import darshan
from darshan.backend.cffi_backend import log_get_derived_metrics
from darshan.experimental.plots import plot_common_access_table

darshan.enable_experimental()

import numpy as np
import pandas as pd
import humanize


def log_file_count_summary_table(log_path: str,
                                 module: str):
    # the darshan_file_category enum is not really
    # exposed in CFFI/Python layer, so we effectively
    # re-export the content indices we need here
    # so that we can properly index the C-level data
    darshan_file_category = {"total opened":0,
                             "read-only files":1,
                             "write-only files":2,
                             "read/write files":3}
    df = pd.DataFrame.from_dict(darshan_file_category, orient="index")
    df.rename(columns={0:"index"}, inplace=True)
    df.index.rename('type', inplace=True)
    df["number of files"] = np.zeros(4, dtype=int)
    df["avg. size"] = np.zeros(4, dtype=str)
    df["max size"] = np.zeros(4, dtype=str)

    darshan_derived_metrics = log_get_derived_metrics(log_path=log_path,
                                                      mod_name=module)
    for cat_name, index in darshan_file_category.items():
        cat_counters = darshan_derived_metrics.category_counters[index]
        num_files = int(cat_counters.count)
        if num_files == 0:
            max_size = "0"
            avg_size = "0"
        else:
            max_size, binary_units = humanize.naturalsize(cat_counters.max_offset_bytes + 1,
                                                          binary=True).split()
            # some rounding work to match the old Perl
            # report table style...
            max_size = math.ceil(float(max_size)) # type: ignore
            if max_size == 0:
                max_size = "0"
            else:
                max_size = f"{max_size} {binary_units}"
            # NOTE: internal formula based on discussion with Phil Carns
            avg_size, binary_units = humanize.naturalsize((cat_counters.total_max_offset_bytes + num_files) / num_files,
                                                           binary=True).split()
            avg_size = math.ceil(float(avg_size)) # type: ignore
            if avg_size == 0:
                avg_size = "0"
            else:
                avg_size = f"{avg_size} {binary_units}"
        df.iloc[index] = [index, num_files, avg_size, max_size]
    # we don't need the index column once we're done with
    # the CFFI/C interaction
    df.drop(columns="index", inplace=True)
    ret = plot_common_access_table.DarshanReportTable(df,
                                                      col_space=200,
                                                      justify="center")
    return ret
