import os
import sys
import glob
import argparse

import darshan
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
from matplotlib.colors import LogNorm


def retrieve_flat_data(report, mods, ops):
    flat_data = {}

    for module_key in mods:

        report.mod_read_all_dxt_records(module_key, dtype="pandas")
        report.update_name_records()
        df = report.records[module_key].to_df()

        flat_data[module_key] = {}
        for op_key in ops:
            flat_data[module_key][op_key] = {
                "rank": [],
                "bytes": [],
                "start_time": [],
                "end_time": [],
            }

        for record in df:
            # keys: 'id', 'rank', 'hostname', 'write_count',
            #       'read_count', 'write_segments', 'read_segments'
            rank_idx = record["rank"]
            for op_key in ops:
                # 'write_segments' or 'read_segments'
                segment_key = op_key + "_segments"

                segment_df = record[segment_key]
                if segment_df.size:
                    # columns: 'offset', 'length', 'start_time', 'end_time'
                    flat_data[module_key][op_key]["bytes"].extend(
                        segment_df["length"].values
                    )
                    flat_data[module_key][op_key]["start_time"].extend(
                        segment_df["start_time"].values
                    )
                    flat_data[module_key][op_key]["end_time"].extend(
                        segment_df["end_time"].values
                    )
                    rank_arr = rank_idx * np.ones(
                        segment_df["start_time"].values.size, dtype=int
                    )
                    flat_data[module_key][op_key]["rank"].extend(rank_arr)

    return flat_data


def aggregate_data(data, mods, ops):
    # TODO: implement data from any/all modules in .darshan log
    # for now just retrieve `DXT_POSIX` and read data from that
    module_key = mods[0]

    start_data = []
    end_data = []
    ranks_data = []
    bytes_data = []
    for op_key in ops:
        start_data.extend(data[module_key][op_key]["start_time"])
        end_data.extend(data[module_key][op_key]["end_time"])
        ranks_data.extend(data[module_key][op_key]["rank"])
        bytes_data.extend(data[module_key][op_key]["bytes"])

    start_data = np.asarray(start_data, dtype=float)
    end_data = np.asarray(end_data, dtype=float)
    ranks_data = np.asarray(ranks_data, dtype=int)
    bytes_data = np.asarray(bytes_data, dtype=int)

    time_data = (end_data + start_data) / 2

    # sort the data arrays in chronological order
    sorted_idx = np.argsort(time_data)
    time_data = time_data[sorted_idx]
    ranks_data = ranks_data[sorted_idx]
    bytes_data = bytes_data[sorted_idx]

    return time_data, ranks_data, bytes_data


def plot_dxt_heatmap(
    report,
    mods=None,
    ops=None,
    xbins=100,
):
    if mods is None:
        mods = ["DXT_POSIX"]
    elif mods[0] != "DXT_POSIX":
        # TODO: for the moment reject any cases that don't input "DXT_POSIX"
        # until we can properly aggregate the data
        raise NotImplementedError(f"DXT heatmap not implemented for module {mods[0]}")
    if ops is None:
        ops = ["read", "write"]

    # retrieve the data
    flat_data = retrieve_flat_data(report=report, mods=mods, ops=ops)
    time_data, ranks_data, bytes_data = aggregate_data(
        data=flat_data, mods=mods, ops=ops
    )

    # get the unique ranks
    unique_ranks = np.unique(ranks_data)
    ybins = unique_ranks.size

    # create a 2D histogram from the time and rank data, with the
    # selected data as the weight
    bins = [xbins, ybins]
    H, _xedges, _yedges = np.histogram2d(
        x=time_data, y=ranks_data, weights=bytes_data, bins=bins
    )
    Hmap_data = H.T

    jgrid = sns.jointplot(kind="hist", bins=bins, space=0.05)
    jgrid.ax_marg_x.cla()
    jgrid.ax_marg_y.cla()

    colorbar_label = f"Data (B): {' + '.join(ops)}"
    colorbar_kws = {"label": colorbar_label}
    hmap = sns.heatmap(
        Hmap_data,
        ax=jgrid.ax_joint,
        # choose a color map that is not white at any value
        cmap="YlOrRd",
        norm=LogNorm(),
        # TODO: want to change the color bar label to use the key names for the
        # specific report data we are mapping, or specialized keys for derived
        # quantities (like bandwidth)
        cbar_kws=colorbar_kws,
    )

    for _, spine in hmap.spines.items():
        spine.set_visible(True)

    if unique_ranks.size > 1:
        jgrid.ax_marg_y.barh(
            y=unique_ranks,
            width=Hmap_data.sum(axis=1),
            align="edge",
            height=-0.99,
            facecolor="black",
            lw=0.5,
        )
    else:
        jgrid.ax_marg_y.axis("off")

    jgrid.ax_marg_x.bar(
        x=np.arange(xbins),
        height=Hmap_data.sum(axis=0),
        facecolor="black",
        align="edge",
    )

    # only create 4 x-axis tick marks, for figure clarity
    n_xlabels = 4
    # create x ticks that scale with the number of xbins in the 2D histogram
    xticks = np.linspace(0, xbins, n_xlabels)
    jgrid.ax_joint.set_xticks(xticks)
    # for the x tick labels, start at 0 and end with the max time (converted to
    # an integer)
    max_time = np.max(time_data)
    if max_time < 1:
        x_ticklabels = np.around(np.linspace(0, max_time, n_xlabels), decimals=2)
    elif (max_time > 1) & (max_time < 10):
        x_ticklabels = np.around(np.linspace(0, max_time, n_xlabels), decimals=1)
    else:
        x_ticklabels = np.asarray(
            np.linspace(0, np.ceil(max_time), n_xlabels), dtype=int
        )

    jgrid.ax_joint.set_xticklabels(x_ticklabels, minor=False)

    if unique_ranks.size > 12:
        # only use 6 labels if there are many ranks to label. Might want to
        # do some checks on how many ranks there are earlier on since any
        # file with 1000's of ranks is too messy to plot this way.
        n_ylabels = 6
        sns_yticks = jgrid.ax_joint.get_yticks()
        yticks = np.linspace(sns_yticks.min(), sns_yticks.max(), n_ylabels)
        jgrid.ax_joint.set_yticks(yticks)
        yticklabels = np.asarray(
            np.linspace(unique_ranks.min(), unique_ranks.max(), n_ylabels), dtype=int
        )
    else:
        # if there are less than 12 unique ranks, just label each
        # row with its rank index
        yticklabels = np.asarray(unique_ranks, dtype=int)
    jgrid.ax_joint.set_yticklabels(yticklabels, minor=False)

    # remove ticks between heatmap and histograms
    jgrid.ax_marg_x.tick_params(
        axis="x", bottom=False, labelbottom=False, top=False, labeltop=False
    )
    jgrid.ax_marg_y.tick_params(
        axis="y", left=False, labelleft=False, right=False, labelright=False
    )
    # remove ticks showing the heights of the histograms
    jgrid.ax_marg_x.tick_params(
        axis="y", left=False, labelleft=False, right=False, labelright=False
    )
    jgrid.ax_marg_y.tick_params(
        axis="x", bottom=False, labelbottom=False, top=False, labeltop=False
    )

    # set the dimensions of the figure to 6.5" wide x 4.5" tall
    jgrid.fig.set_size_inches(6.5, 4.5)

    # set the spacing between subplots
    hspace, wspace = 0.01, 0.02
    # adjust the subplot so the x/y tick labels are legible
    jgrid.fig.subplots_adjust(
        left=0.1, bottom=0.15, top=0.9, hspace=hspace, wspace=wspace
    )
    if unique_ranks.size > 1:
        # if there are multiple ranks we want to move the colorbar on the far
        # right side of the horizontal bar graph

        # set the location of the right side of the figure
        jgrid.fig.subplots_adjust(right=0.84)
        # get the positions of the joint and marginal x axes
        pos_joint_ax = jgrid.ax_joint.get_position()
        pos_marg_x_ax = jgrid.ax_marg_x.get_position()
        # set the position and dimensions of the joint plot such that it fills
        # the space as if there was no colorbar
        jgrid.ax_joint.set_position(
            [pos_joint_ax.x0, pos_joint_ax.y0, pos_marg_x_ax.width, pos_joint_ax.height]
        )
        # set the position of the colorbar such that it is on the
        # right side of the horizontal bar graph, and set its dimensions
        jgrid.fig.axes[-1].set_position(
            [0.85, pos_joint_ax.y0, 0.9, pos_joint_ax.height]
        )
    else:
        # if there is only 1 unique rank there is no horizontal bar graph,
        # so set the subplot dimensions to fill the space

        # set the location of the right side of the figure
        jgrid.fig.subplots_adjust(right=0.92)
        # get the positions of the joint and marginal x axes
        pos_joint_ax = jgrid.ax_joint.get_position()
        pos_marg_x_ax = jgrid.ax_marg_x.get_position()
        # set the position and dimensions of the joint plot such that it fills
        # the space as if there was no horizontal bar graph
        jgrid.ax_joint.set_position(
            [pos_joint_ax.x0, pos_joint_ax.y0, pos_marg_x_ax.width, pos_joint_ax.height]
        )
        # set the position of the colorbar such that it is on the
        # right side of the heatmap, and set its dimensions
        jgrid.fig.axes[-1].set_position(
            [0.82, pos_joint_ax.y0, 0.9, pos_joint_ax.height]
        )

    # invert the y-axis so rank values are increasing
    jgrid.ax_joint.invert_yaxis()

    # turn the frame off for both marginal subplots
    jgrid.ax_marg_x.axis("off")
    jgrid.ax_marg_y.axis("off")

    # set the axis labels
    jgrid.ax_joint.set_xlabel("Time (s)")
    jgrid.ax_joint.set_ylabel("Rank")

    # set the figure title
    title_str = " ,".join(mods)
    jgrid.fig.suptitle(title_str, fontsize=11)
    return jgrid


if __name__ == "__main__":
    # get the path to the file(s) to be read in
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "datapath",
        nargs="?",
        help="Specify path to darshan log file or directory with darshan logs",
    )
    args = parser.parse_args()
    data_path = args.datapath

    if os.path.isfile(data_path):
        # individual file case
        files_to_process = [data_path]
    elif os.path.isdir(data_path):
        # directory case
        files_to_process = glob.glob(os.path.join(data_path, "*.darshan"))

    for filepath in files_to_process:
        # get the file name and the file path
        filename = os.path.basename(filepath)
        # generate the darshan report
        report = darshan.DarshanReport(filepath, read_all=False)

        if "DXT_POSIX" not in report.modules.keys():
            print("\n" + f"Skipping -> No 'DXT_POSIX' module in {filename}")
            continue

        # generate the heatmap figure
        hmap = plot_dxt_heatmap(
            report=report,
            mods=None,
            ops=None,
            xbins=200,
        )

        save_path = os.path.join(os.getcwd(), os.path.splitext(filename)[0] + ".png")
        # save the figure at the specified location
        print(f"Saving figure at location: {save_path}")
        plt.savefig(save_path, dpi=300)
        plt.close()
