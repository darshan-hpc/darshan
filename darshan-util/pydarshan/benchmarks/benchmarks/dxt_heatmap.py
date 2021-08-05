import os
import importlib

import pandas as pd

from darshan.experimental.plots import plot_dxt_heatmap, heatmap_handling
# TODO: no good reason pydarshan should have hyphenated module
# names... for now I hack around it...
example_logs = importlib.import_module("examples.example-logs")
from tests.input import test_data_files_dxt


class PlotDXTHeatMapSmall:
    params = [
        ["examples/example-logs/ior_hdf5_example.darshan",
         "examples/example-logs/dxt.darshan",
         "tests/input/sample-dxt-simple.darshan",
        ],
        [10, 100, 1000],
        ]
    param_names = ['darshan_logfile', 'xbins']

    def setup(self, darshan_logfile, xbins):
        # darshan example files are found in
        # various locs in the code base, so shim
        # around that...
        filename = os.path.basename(darshan_logfile)
        if "examples" in darshan_logfile:
            self.logfile = example_logs.example_data_files_dxt[filename]
        else:
            self.logfile = test_data_files_dxt[filename]

    def time_plot_heatmap_builtin_logs(self, darshan_logfile, xbins):
        # benchmark DXT heatmap plotting for
        # some log files available in the darshan
        # repo proper--these are likely to be quite
        # small for the most part
        plot_dxt_heatmap.plot_heatmap(
            log_path=self.logfile,
            mods=["DXT_POSIX"],
            ops=["read", "write"],
            xbins=xbins)


class GetHeatMapDf:
    params = [[50, 1000, 10000], [10, 50, 250]]
    param_names = ['unique_ranks', 'bin_count']


    def setup(self, unique_ranks, bin_count):
        self.agg_df = pd.DataFrame({'length': [10] * unique_ranks,
                                    'start_time': [0.1] * unique_ranks,
                                    'end_time': [0.9] * unique_ranks,
                                    'rank': range(unique_ranks),
                                   })


    def time_get_heatmap_df(self, unique_ranks, bin_count):
        # benchmark get_heatmap_df() handling of variable
        # numbers of unique ranks/bins
        heatmap_handling.get_heatmap_df(self.agg_df, xbins=bin_count)
