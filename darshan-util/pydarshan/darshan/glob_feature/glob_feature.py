# Creates a DataFrame with two columns ("glob_filename" and "glob_count") based on the files read by a .darshan file.
# It uses sequence matching and grouping techniques to group similar file paths together and generates an HTML report of the grouped paths and their counts
# Command to run python glob_feature.py -p path/to/log/file.darshan 


import argparse
import pandas as pd
import difflib
import darshan
import re
import os


def generalize_filename_glob(df):
    paths = df["filename_glob"].tolist()
    grouped_paths = []

    for i in range(len(paths)):
        if not grouped_paths:
            grouped_paths.append((paths[i],))
        else:
            is_grouped = False
            for j, group in enumerate(grouped_paths):
                matcher = difflib.SequenceMatcher(None, paths[i], group[0])
                similarity_ratio = matcher.ratio()
                if similarity_ratio >= 0.8:
                    grouped_paths[j] = group + (paths[i],)
                    is_grouped = True
                    break
            if not is_grouped:
                grouped_paths.append((paths[i],))

    print("grouped paths list is", grouped_paths)

    new_paths = []
    for group in grouped_paths:
        if len(group) > 1:
            common_prefix = os.path.commonprefix(group)
            pattern = r"({}.*)\d(.*)".format(common_prefix)
            modified_path = re.sub(pattern, r"\1\\d\2", group[0])
            new_paths.append((modified_path, len(group)))
        else:
            new_paths.append((group[0], 1))

    new_paths = [path for path in new_paths if path[0]]

    if len(new_paths) > len(df):
        new_paths = new_paths[:len(df)]

    print("new paths are", new_paths)
    return new_paths


def main(log_path, output_path):

    report = darshan.DarshanReport(log_path)

    df = pd.DataFrame.from_dict(report.name_records, orient="index", columns=["filename_glob"])

    df = df[df["filename_glob"].str.contains(r"/.*")]

    df.reset_index(drop=True, inplace=True)  # Reset the index


    new_paths = generalize_filename_glob(df)
    df = pd.DataFrame(new_paths, columns=["filename_glob", "glob_count"])
    df = df.reset_index(drop=True) 
    df = df.sort_values(by="glob_count", ascending=False)

    style = df.style.background_gradient(axis=0, cmap="viridis")
    style.set_table_styles([
        {"selector": "", "props": [("border", "1px solid grey")]},
        {"selector": "tbody td", "props": [("border", "1px solid grey")]},
        {"selector": "th", "props": [("border", "1px solid grey")]}
    ])

    style = style.hide_index()
    html = style.render()

    with open(output_path, "w") as html_file:
        html_file.write(html)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--log-path', type=str, help="Path to the log file")
    parser.add_argument('-o', '--output_path', type=str, help="Path to the output file")
    args = parser.parse_args()
    main(log_path=args.log_path, output_path=args.output_path)
