from typing import Sequence

import numpy as np
import pandas as pd

class Heatmap():
    """
    The Heatmap class is a convenience wrapper to Darshan Heatmap records.
    The structure can be sparse (e.g., not all ranks need to be populated)
    """

    def __init__(self, mod=None):       
        self._mod = mod
        self._ranks = set()
        self._nbins = None
        self._bin_width_seconds = None
        
        self._num_recs = 0
        self._data = {
            "read": {},
            "write": {}
        }
   
    def __repr__(self):
        type_ = type(self)
        module = type_.__module__
        qualname = type_.__qualname__
        return f"<{module}.{qualname} (mod={self._mod}, nbins={self._nbins}, bin_width={self._bin_width_seconds}s)>"

    def info(self, plot: bool = False):
        """
        Print information about the record for inspection.

        Parameters
        ----------

        plot: show visualization of the heatmap data

        """
        print(self.__repr__())
        print("Module:       ", self._mod)
        
        if len(self._ranks) < 10:
            print("Ranks:        ", self._ranks)
        else:
            print("Ranks (len):  ", len(self._ranks))
            
        print("Num. bins:    ", self._nbins)
        print("Bin width (s):", self._bin_width_seconds)
        
        print("Num. recs:    ", self._num_recs)

        if plot:
            import matplotlib.pyplot as plt
            print()
            for op in ['read', 'write']:
                print(op)
                plt.pcolor(self.to_df(ops=[op]))
                plt.show()

    def add_record(self, rec: dict):  
        """
        Add heatmap record to heatmap.

        Parameters
        ----------
          	
        rec: a heatmap record dictionary as returned by backend._log_get_heatmap_record

        """
        nbins = rec['nbins']
        rank = rec['rank']
              
        # check data
        if self._nbins is  None:
            self._nbins = nbins
        if self._nbins != nbins:
            raise ValueError("Record nbins is not consistent with current heatmap.")
           
        if self._bin_width_seconds is None:
            self._bin_width_seconds = rec['bin_width_seconds']
        if self._bin_width_seconds != rec['bin_width_seconds']:
            raise ValueError("Record bin_width_seconds is not consistent with current heatmap.")

        # actually add data
        self._ranks.add(rec['rank'])
        
        self._data['read'][rank] = rec['read_bins']
        self._data['write'][rank] = rec['write_bins']
            
        self._num_recs += 1

    def to_df(self, ops: Sequence[str], interval_index: bool = True):
        """
        Return heatmap as pandas dataframe.

        Parameters
        ----------

        ops: a sequence of keys designating which operations to use
        for data aggregation. If multiple operations are given, their
        dataframes will be summed. Allowed values: ["read", "write"].

        interval_index: bool to enable/disable interval indices for columns

        """
        for op in ops:
            if op not in self._data:
                raise ValueError(f"{op} not in heatmap.")

        nbins = self._nbins
        bin_width_seconds = self._bin_width_seconds

        if interval_index:
            breaks = np.linspace(start=0, stop=nbins*bin_width_seconds, num=nbins+1)
            columns = pd.IntervalIndex.from_breaks(breaks)
        else:
            columns = np.arange(nbins)

        df_list = []
        for op in ops:
            data = self._data[op]
            df = pd.DataFrame.from_dict(data, orient='index', columns=columns)
            df.index.name = "rank"
            df_list.append(df)

        # sum the dataframes
        return sum(df_list)

