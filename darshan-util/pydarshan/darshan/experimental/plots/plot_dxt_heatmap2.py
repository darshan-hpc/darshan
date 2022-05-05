# -*- coding: utf-8 -*-

from darshan.report import *

import matplotlib
import matplotlib.pyplot as plt
from mpl_toolkits.axes_grid1 import make_axes_locatable 
import numpy as np

def plot_dxt_heatmap2(report, 
        xbins=10, ybins=None, 
        group_by="rank", 
        mods=None, ops=None, 
        display_values=False, cmap=None, figsize=None, ax=None,
        amplify=False):
    """
    Generates a heatmap plot from a report with DXT traces.

    Args:
        report (darshan.DarshanReport): report to generate plot from
        xbins (int): number of bins on the x axis
        ybins (int): number of bins on the y axis
        group_by (str): attribute to group by (e.g., rank, hostname)
        mods (list): modules to include in heatmap (e.g., ['DXT_POSIX', 'DXT_MPIIO'])
        ops (list): operations to consider (e.g., ['read', 'write']
        display_values (bool): show values per heatmap field
        cmap: overwrite colormap (see matplotlib colormaps)
        figsize: change figure size (see matplotlib figsize)

        amplify (int): paint neighbouring cells e.g., when working with many ranks
    """

    runtime = report.end_time - report.start_time       # timedelta
    runtime = runtime.seconds + runtime.microseconds    # to float/int
    if runtime == 0:
        runtime = 1

        
    if mods is None:
        mods = ['DXT_POSIX', 'DXT_MPIIO']
    
    if ops is None:
        ops = ['read', 'write']
    
    
    def get_hostname_mapper(report):
        """ Determine which ranks map to which hostname. """
        hostnames = {}
        i = 0
        for mod in mods:
            for rec in report.records[mod]:
                hostname = rec['hostname']
                if hostname not in hostnames:
                    hostnames[hostname] = i
                    i += 1
        return hostnames
   

    hostnames = get_hostname_mapper(report)
    ranks = report.metadata['job']['nprocs']
    
    # set ybins, yticks and ylabel to match choice of group_by 
    if group_by in ['hostname', 'node']:
        ybins = len(hostnames)
        yticks = list(hostnames.keys())
        ylabel = "Hostname"
    else:
        if ybins is None:
            ybins = ranks 
            ylabel = "Rank"
            #yticks = [str(x) for x in range(ybins)] 
        else:
            ylabel = f"Binned Ranks (binsize={int(ranks/ybins)} ranks)"

    
    # heatmap to be populated with event counts
    events = np.zeros((ybins, xbins))    
    
    for mod in mods:
        for op in ops:
            for rec in report.records[mod]:
                
                if group_by in ['hostname', 'node']:
                    ybin = hostnames[rec['hostname']]
                else:
                    ybin = int((rec['rank'] /ranks) * ybins)
                    
                    
                for event in rec[f'{op}_segments']:
                    xbin = int((event['start_time'] / runtime) * xbins)
                    events[ybin][xbin] += 1
                    if amplify:
                        rng = amplify
                        for i in range(rng):
                            sur = int(rng/2) + i
                            if sur != 0 and (ybin - sur) > 0:
                                events[ybin - sur][xbin] += 1


    if ax is None:
        fig, ax = plt.subplots(figsize=figsize, sharey=True)
    else:
        fig = None
          
    im = ax.imshow(events, cmap=cmap, aspect='auto')
    
    divider = make_axes_locatable(ax)
    cax = divider.append_axes("right", size="5%", pad=0.05)    
    plt.colorbar(im, cax=cax)
    
    if group_by in ['hostname', 'node']:
        ax.set_yticks(np.arange(len(yticks)))
        ax.set_yticklabels(yticks)
    
    # overlay values
    if display_values:
        for i in range(ybins):
            for j in range(xbins):
                text = ax.text(j, i, events[i, j], ha="center", va="center", color="w")

    ax.set_title(f"DXT Heatmap mods={mods}, ops={ops}")
    ax.set_ylabel(ylabel)
    ax.set_xlabel(f"Time (binsize={runtime/xbins} seconds)")

    plt.tight_layout()

    if fig is not None:
        plt.close()
        return fig
