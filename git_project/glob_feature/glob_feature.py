# Creates a DataFrame with two columns ("glob_filename" and "glob_count") based on the files read by a .darshan file.
# It uses sequence matching and grouping techniques to group similar file paths together and generates an HTML report of the grouped paths and their counts
# Command to run python glob_feature.py -p path/to/log/file.darshan 


import argparse
import pandas as pd
import difflib
import darshan


def path_grouper():
    matcher = difflib.SequenceMatcher()
    def group_paths(paths):
        if not matcher.a:
            matcher.set_seq1(paths)
            return paths
        else:
            matcher.set_seq2(paths)
            matchings = matcher.get_matching_blocks()
            if any(size > 25 for _, _, size in matchings): # change size to bigger number for more precise paths
                return matcher.a
            else:
                matcher.set_seq1(paths)
                return paths

    return group_paths


def regex_df_condenser(df, paths):
    path_grouper_func = path_grouper()
    df["filename_glob"] = df["filename_glob"].apply(path_grouper_func)
    df = df.groupby("filename_glob").size().reset_index(name="glob_count")

    return df


def main(log_path):
    report = darshan.DarshanReport(log_path)
    df = pd.DataFrame.from_dict(report.name_records, orient="index", columns=["filename_glob"])
    df = df[df["filename_glob"].str.contains(r"/.*")]
    df["glob_count"] = 1
    df = regex_df_condenser(df, df["filename_glob"])
    df.sort_values(by="glob_count", inplace=True, ascending=False)


    style = df.style.background_gradient(axis=0, cmap="viridis", gmap=df["glob_count"])
    style.hide(axis="index")
    style.set_table_styles([
        {"selector": "", "props": [("border", "1px solid grey")]},
        {"selector": "tbody td", "props": [("border", "1px solid grey")]},
        {"selector": "th", "props": [("border", "1px solid grey")]}
    ])
    html = style.to_html()

# can change name of the output html report here
    with open("name_record_table.html", "w") as html_file:
        html_file.write(html)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--log-path', type=str)
    args = parser.parse_args()
    main(log_path=args.log_path)

