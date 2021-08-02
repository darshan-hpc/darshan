import os
import importlib

from darshan.experimental.plots import plot_dxt_heatmap
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
