import darshan
from darshan.experimental.plots import plot_common_access_table

darshan.enable_experimental()

import numpy as np
import pandas as pd
import humanize


def log_file_count_summary_table(derived_metrics,
                                 mod_name: str):
    data_type = "files" if mod_name != "DAOS" else "objects"
    # the darshan_file_category enum is not really
    # exposed in CFFI/Python layer, so we effectively
    # re-export the content indices we need here
    # so that we can properly index the C-level data
    darshan_file_category = {f"total {data_type}":0,
                             f"read-only {data_type}":1,
                             f"write-only {data_type}":2,
                             f"read/write {data_type}":3}
    df = pd.DataFrame.from_dict(darshan_file_category, orient="index")
    df.rename(columns={0:"index"}, inplace=True)
    df.index.rename('type', inplace=True)
    df[f"number of {data_type}"] = np.zeros(4, dtype=int)
    df["avg. size"] = np.zeros(4, dtype=str)
    df["max size"] = np.zeros(4, dtype=str)

    for cat_name, index in darshan_file_category.items():
        cat_counters = derived_metrics.category_counters[index]
        num_files = int(cat_counters.count)
        if num_files == 0:
            max_size = "0"
            avg_size = "0"
        else:
            max_size, binary_units = humanize.naturalsize(cat_counters.max_offset_bytes + 1,
                                                          binary=True,
                                                          format="%.2f").split()
            if max_size != "0":
                max_size = f"{max_size} {binary_units}"
            # NOTE: internal formula based on discussion with Phil Carns
            avg_size, binary_units = humanize.naturalsize((cat_counters.total_max_offset_bytes + num_files) / num_files,
                                                           binary=True,
                                                           format="%.2f").split()
            if avg_size != "0":
                avg_size = f"{avg_size} {binary_units}"
        df.iloc[index] = [index, num_files, avg_size, max_size]
    # we don't need the index column once we're done with
    # the CFFI/C interaction
    df.drop(columns="index", inplace=True)
    ret = plot_common_access_table.DarshanReportTable(df,
                                                      col_space=200,
                                                      border=0,
                                                      justify="center",
                                                      index_names=False)
    return ret

def log_module_overview_table(derived_metrics,
                              mod_name: str):
    mod_overview = []
    total_cat = derived_metrics.category_counters[0]

    total_count = total_cat.count
    data_type = "files" if mod_name != "DAOS" else "objects"
    indices = [f"{data_type} accessed", "bytes read", "bytes written", "I/O performance estimate"]
    mod_overview.append(f"{total_count}")
    total_bytes_read = total_cat.total_read_volume_bytes
    total_bytes_read_str = humanize.naturalsize(total_bytes_read, binary=True, format="%.2f")
    total_bytes_written = total_cat.total_write_volume_bytes
    total_bytes_written_str = humanize.naturalsize(total_bytes_written, binary=True, format="%.2f")
    mod_overview.append(f"{total_bytes_read_str}")
    mod_overview.append(f"{total_bytes_written_str}")
    total_bw = derived_metrics.agg_perf_by_slowest
    mod_overview.append(f"{total_bw:.2f} MiB/s (average)")
    df = pd.DataFrame(mod_overview, index=indices)

    ret = plot_common_access_table.DarshanReportTable(df,
                                                      col_space=500,
                                                      border=0,
                                                      header=False)
    return ret
