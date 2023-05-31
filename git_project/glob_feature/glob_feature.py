# Creates a DataFrame with two columns ("glob_filename" and "glob_count") based on the files read by a .darshan file.
# It uses sequence matching and grouping techniques to group similar file paths together and generates an HTML report of the grouped paths and their counts
# Command to run python glob_feature.py -p path/to/log/file.darshan 


import argparse
import pandas as pd
import difflib
import darshan
import re
import os


def make_path_grouper():
    matcher = difflib.SequenceMatcher()
    def group_paths(paths):
        if not matcher.a:
            matcher.set_seq1(paths)
            return paths
        else:
            matcher.set_seq2(paths)
            similarity_ratio = matcher.ratio()
            if similarity_ratio >= 0.8:
                return matcher.a
            else:
                matcher.set_seq1(paths)
                return paths
    return group_paths


def regex_df_condenser(df, paths):
    path_grouper_func = make_path_grouper()

    df["filename_glob"] = df["filename_glob"].apply(path_grouper_func)

    df = df.groupby("filename_glob").size().reset_index(name="glob_count")

    df = df.sort_values(by="glob_count", ascending=False)


    def find_common_prefix(paths):
        # Sort the paths in lexicographical order
        sorted_paths = sorted(paths)

        # Find the common prefix
        common_prefix = os.path.commonprefix(sorted_paths)

        # Trim the common prefix to the last path separator
        last_separator = common_prefix.rfind(os.path.sep)
        common_prefix = common_prefix[:last_separator+1] if last_separator >= 0 else common_prefix

        return common_prefix


    for group in df["filename_glob"].unique():
        group_df = df[df["filename_glob"] == group]
        common_path = find_common_prefix(group_df["filename_glob"])
        df.loc[df["filename_glob"] == group, "filename_glob"] = common_path


    df["filename_glob"] = df.apply(lambda row: (row["filename_glob"]) + r".*", axis=1)

    return df



def main(log_path, output_path):
    report = darshan.DarshanReport(log_path)


    df = pd.DataFrame.from_dict(report.name_records, orient="index", columns=["filename_glob"])

    df = df[df["filename_glob"].str.contains(r"/.*")]
    df["glob_count"] = 1
    df = regex_df_condenser(df, df["filename_glob"])

    style = df.style.background_gradient(axis=0, cmap="viridis", gmap=df["glob_count"])
    style.hide(axis="index")
    style.set_table_styles([
        {"selector": "", "props": [("border", "1px solid grey")]},
        {"selector": "tbody td", "props": [("border", "1px solid grey")]},
        {"selector": "th", "props": [("border", "1px solid grey")]}
    ])
    html = style.to_html()

    # can change name of the output html report here
    with open("name_record_glob_hd5f.html", "w") as html_file:
        html_file.write(html)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--log-path', type=str, help="Path to the log file")
    parser.add_argument('-o', '--output-path', type=str, help="Path to the output HTML file")
    args = parser.parse_args()
    main(log_path=args.log_path , output_path=args.output_path)

