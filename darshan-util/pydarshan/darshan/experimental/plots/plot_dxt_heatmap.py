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
    # retrieve the flattened module data from the darshan report

    # initialize an empty dictionary for storing
    # module and read/write data
    flat_data = {}
    # iterate over the modules (i.e. DXT_POSIX)
    for module_key in mods:

        # read in the module data, update the name records, and
        # collect the pandas dataframe from the report
        report.mod_read_all_dxt_records(module_key, dtype="pandas")
        report.update_name_records()
        df = report.records[module_key].to_df()

        # initialize an empty dictionary for each module
        flat_data[module_key] = {}
        # for each operation (read/write), initialize a dictionary
        # with empty lists for storing data to be used
        for op_key in ops:
            # use the same keys that are used in the darshan report data
            flat_data[module_key][op_key] = {
                "rank": [],
                "bytes": [],
                "start_time": [],
                "end_time": [],
            }

        # iterate over each record in the dataframe
        for record in df:
            # record keys: 'id', 'rank', 'hostname', 'write_count',
            #       'read_count', 'write_segments', 'read_segments'
            # get the rank used for this record
            rank = record["rank"]
            for op_key in ops:
                # create the segment key: 'write_segments' or 'read_segments'
                segment_key = op_key + "_segments"

                # get the dataframe for the given segment
                segment_df = record[segment_key]
                if segment_df.size:
                    # segment dataframe columns: 'offset', 'length',
                    # 'start_time', 'end_time'

                    # add the length, start/end times, and rank data to
                    # the flat data dictionary
                    flat_data[module_key][op_key]["bytes"].extend(
                        segment_df["length"].values
                    )
                    flat_data[module_key][op_key]["start_time"].extend(
                        segment_df["start_time"].values
                    )
                    flat_data[module_key][op_key]["end_time"].extend(
                        segment_df["end_time"].values
                    )
                    # since the rank is only listed once per record, use the
                    # rank and the start time array length to store a rank
                    # value for each data point
                    rank_arr = rank * np.ones(
                        segment_df["start_time"].values.size, dtype=int
                    )
                    flat_data[module_key][op_key]["rank"].extend(rank_arr)

    return flat_data


def aggregate_data(data, mods, ops):
    # aggregates the data based on which
    # modules and operations are selected

    # for now just retrieve `DXT_POSIX` and read data from that
    module_key = mods[0]

    # iterate over the operations (i.e. read, write)
    # create empty arrays to store arrays in
    start_data = []
    end_data = []
    ranks_data = []
    bytes_data = []
    for op_key in ops:
        # for each operation, extend the list with the corresponding data
        start_data.extend(data[module_key][op_key]["start_time"])
        end_data.extend(data[module_key][op_key]["end_time"])
        ranks_data.extend(data[module_key][op_key]["rank"])
        bytes_data.extend(data[module_key][op_key]["bytes"])

    # convert the lists into arrays
    start_data = np.asarray(start_data, dtype=float)
    end_data = np.asarray(end_data, dtype=float)
    ranks_data = np.asarray(ranks_data, dtype=int)
    bytes_data = np.asarray(bytes_data, dtype=int)

    # sort the data arrays in chronological order
    sorted_idx = np.argsort(start_data)
    start_data = start_data[sorted_idx]
    end_data = end_data[sorted_idx]
    ranks_data = ranks_data[sorted_idx]
    bytes_data = bytes_data[sorted_idx]

    return start_data, end_data, ranks_data, bytes_data


def get_heatmap_data(start_time_data, end_time_data, ranks_data, data_arr, n_xbins):
    # builds an array similar to a 2D-histogram, where the y data is the unique
    # ranks and the x data is time. Each bin is populated with the data sum
    # and/or proportionate data sum for all IO events read/written during the
    # time spanned by the bin.

    # get the unique ranks
    unique_ranks = np.unique(ranks_data)

    # generate the bin edges by generating an array of length n_bins+1, then
    # taking pairs of data points as the min/max bin value
    min_time = 0.0
    max_time = end_time_data.max()
    bin_edge_data = np.linspace(min_time, max_time, n_xbins + 1)

    # calculated the elapsed time for each data point
    elapsed_time_data = end_time_data - start_time_data

    # generate an array for the heatmap data
    hmap_data = np.zeros((unique_ranks.size, n_xbins), dtype=float)

    # iterate over the unique ranks
    for i, rank in enumerate(unique_ranks):
        # for each rank, generate a mask to select only
        # the data points that correspond to that rank
        rank_mask = ranks_data == rank
        start_data = start_time_data[rank_mask]
        end_data = end_time_data[rank_mask]
        elapsed_data = elapsed_time_data[rank_mask]
        bytes_data = data_arr[rank_mask]

        # iterate over the bins
        for j, (bmin, bmax) in enumerate(zip(bin_edge_data[:-1], bin_edge_data[1:])):
            # create a mask for all data with a start time greater than the
            # bin minimum time
            start_mask_min = start_data >= bmin
            # create a mask for all data with an end time less than the
            # bin maximum time
            end_mask_max = end_data <= bmax
            # now use the above masks to find the indices of all data with
            # start/end times that fall within the bin min/max times
            start_inside_idx = np.nonzero(start_mask_min & (start_data <= bmax))[0]
            end_inside_idx = np.nonzero((end_data > bmin) & end_mask_max)[0]
            # using the start/end indices, find all indices that both start
            # and end within the bin min/max limits
            inside_idx = np.intersect1d(start_inside_idx, end_inside_idx)
            # use the original masks to find the indices of data that start
            # before the bin minimum time and end after the bin maximum time
            outside_idx = np.nonzero(~start_mask_min & ~end_mask_max)[0]

            if inside_idx.size:
                # for data that start/end inside the bin limits,
                # add the sum of the correspondign data to the hmap data
                hmap_data[i, j] += bytes_data[inside_idx].sum()
                # now remove any indices from the start/end index arrays
                # to prevent double counting
                start_inside_idx = np.setdiff1d(start_inside_idx, inside_idx)
                end_inside_idx = np.setdiff1d(end_inside_idx, inside_idx)

            if outside_idx.size:
                # for data that start before the bin min time and end
                # after the bin max time (run longer than 1 bin time),
                # calculate the proportionate data used in 1 bin time
                # and add it to the hmap data
                bin_elapsed_time = bmax - bmin
                proportionate_time = bin_elapsed_time / elapsed_data[outside_idx]
                proportionate_data = proportionate_time * bytes_data[outside_idx]
                hmap_data[i, j] += proportionate_data.sum()

            if start_inside_idx.size:
                # for data with only a start time within the bin limits,
                # calculate the elapsed time (from start to bin max), use the
                # elapsed time to calculate the proportionate data read/written,
                # and add the data sum to the hmap data
                start_elapsed = bmax - start_data[start_inside_idx]
                start_prop_time = start_elapsed / elapsed_data[start_inside_idx]
                start_prop_data = start_prop_time * bytes_data[start_inside_idx]
                hmap_data[i, j] += start_prop_data.sum()

            if end_inside_idx.size:
                # for data with only an end time within the bin limits,
                # calculate the elapsed time (from bin min to end time), use the
                # elapsed time to calculate the proportionate data read/written,
                # and add the data sum to the hmap data
                end_elapsed = end_data[end_inside_idx] - bmin
                end_prop_time = end_elapsed / elapsed_data[end_inside_idx]
                end_prop_data = end_prop_time * bytes_data[end_inside_idx]
                hmap_data[i, j] += end_prop_data.sum()

    # check that the sum of the heatmap data is
    # close to the sum of the input data
    if not np.isclose(hmap_data.sum(), data_arr.sum()):
        raise ValueError(
            f"Heatmap data size does not match input data size. "
            f"Calculated {hmap_data.sum()}, expected {data_arr.sum()}"
        )
    return hmap_data


def plot_dxt_heatmap(
    report,
    mods=None,
    ops=None,
    xbins=200,
):
    if mods is None:
        mods = ["DXT_POSIX"]
    elif mods[0] != "DXT_POSIX":
        # TODO: for the moment reject any cases that don't input "DXT_POSIX"
        # until we can properly aggregate the data
        raise NotImplementedError(f"DXT heatmap not implemented for module {mods[0]}")
    if ops is None:
        ops = ["read", "write"]

    # generate the flattened data dictionary from the report data
    flat_data_dict = retrieve_flat_data(report=report, mods=mods, ops=ops)
    # aggregate the data according to the selected modules and operations
    start_time_data, end_time_data, ranks_data, bytes_data = aggregate_data(
        data=flat_data_dict, mods=mods, ops=ops
    )

    # get the heatmap data array
    Hmap_data = get_heatmap_data(
        start_time_data=start_time_data,
        end_time_data=end_time_data,
        ranks_data=ranks_data,
        data_arr=bytes_data,
        n_xbins=xbins,
    )

    # get the unique ranks
    unique_ranks = np.unique(ranks_data)
    ybins = unique_ranks.size

    # create a 2D histogram from the time and rank data, with the
    # selected data as the weight
    bins = [xbins, ybins]
    jgrid = sns.jointplot(kind="hist", bins=bins, space=0.05)
    jgrid.ax_marg_x.cla()
    jgrid.ax_marg_y.cla()

    colorbar_label = f"Data (B): {', '.join(ops)}"
    colorbar_kws = {"label": colorbar_label}
    hmap = sns.heatmap(
        Hmap_data,
        ax=jgrid.ax_joint,
        # choose a color map that is not white at any value
        cmap="YlOrRd",
        norm=LogNorm(),
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
    # for the x tick labels, start at 0 and end with
    # the max time (converted to an integer)
    min_time = 0.0
    max_time = np.max(end_time_data)
    if max_time <= 1:
        # use 2 decimal places for run times less than 1 second
        x_ticklabels = np.around(np.linspace(min_time, max_time, n_xlabels), decimals=2)
    elif (max_time > 1) & (max_time <= 10):
        # use 1 decimal place for run times between 1 and 10 seconds
        x_ticklabels = np.around(np.linspace(min_time, max_time, n_xlabels), decimals=1)
    else:
        # for run times greater than 10 seconds, round the max
        # time up and round labels to the nearest integer
        x_ticklabels = np.linspace(min_time, np.ceil(max_time), n_xlabels, dtype=int)

    jgrid.ax_joint.set_xticklabels(x_ticklabels, minor=False)

    if unique_ranks.size > 12:
        # only use 6 labels if there are many ranks to label. Might want to
        # do some checks on how many ranks there are earlier on since any
        # file with 1000's of ranks is too messy to plot this way.
        n_ylabels = 6
        sns_yticks = jgrid.ax_joint.get_yticks()
        yticks = np.linspace(sns_yticks.min(), sns_yticks.max(), n_ylabels)
        jgrid.ax_joint.set_yticks(yticks)
        yticklabels = np.linspace(
            unique_ranks.min(), unique_ranks.max(), n_ylabels, dtype=int
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
    title_str = "Module(s): " + ", ".join(mods)
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
            print(f"Skipping -> No 'DXT_POSIX' module in {filename}")
            continue

        # generate the heatmap figure
        hmap = plot_dxt_heatmap(report=report)

        save_path = os.path.join(os.getcwd(), os.path.splitext(filename)[0] + ".png")
        # save the figure at the specified location
        print(f"Saving figure at location: {save_path}")
        plt.savefig(save_path, dpi=300)
        plt.close()
