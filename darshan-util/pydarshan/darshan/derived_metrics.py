def perf_estimate(report, mod_name: str):
    data = report.data["records"][mod_name].to_df()
    counters_df = data["counters"]
    fcounters_df = data["fcounters"]
    # the old perl reports used MiB so doing
    # that here for consistency, though I note
    # that it might be more natural to use a library
    # like humanize to automatically select i.e., GiB
    # depending on magnitude
    mod_name_adjusted = mod_name.replace("-", "")
    total_mebibytes = (counters_df[f"{mod_name_adjusted}_BYTES_WRITTEN"].sum()
                       + counters_df[f"{mod_name_adjusted}_BYTES_READ"].sum()) / (2 ** 20)
    total_rw_time = (fcounters_df[f"{mod_name_adjusted}_F_READ_TIME"].sum() +
                     fcounters_df[f"{mod_name_adjusted}_F_WRITE_TIME"].sum())
    mebibytes_per_sec = total_mebibytes / total_rw_time
    # construct a string similar to the one used in perl reports,
    # matching in precision of reported values
    # TODO: resolve discrepancy noted in gh-847 vs. perl
    # reports on the bandwidth calculation (even for single record logs!)
    io_perf_string = (f"I/O performance estimate (at the {mod_name} layer): "
                      f"transferred {total_mebibytes:.1f} MiB at {mebibytes_per_sec:.2f} MiB/s")
    return io_perf_string
