# Creates a DataFrame with two columns ("glob_filename" and "glob_count") based on the files read by a .darshan file.
# The script utilizes agglomerative hierarchical clustering to effectively group similar file paths together, based on their characteristics.
# It then displays a dataframe where one file represents a group and uses [.*] to show where filepaths within a group differ 
# The result of this process is an HTML report that provides a comprehensive overview of the grouped paths and their respective counts. 
# Command to run: python glob_feature.py -p path/to/log/file.darshan -o path/to/output_file 
# Command to run with verbose: verbose will display all the files under the representing file 
# python glob_feature.py -p path/to/log/file.darshan -o path/to/output_file  -v

import argparse
import pandas as pd
import darshan
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.cluster import AgglomerativeClustering
from sklearn.metrics import silhouette_score
import numpy as np
import os


def main(log_path, output_path, verbose):

    report = darshan.DarshanReport(log_path)
    df = pd.DataFrame.from_dict(report.name_records, orient="index", columns=["filename_glob"])
    df = df[df["filename_glob"].str.contains(r"/.*")]

    num_files = len(df)
    optimal_k = 2  # Initialize optimal_k to 2
    if num_files == 1:
        print("Only one file detected.")
        optimal_k = 1
        # Process and save results for the single file
        grouped_paths = {0: [df["filename_glob"].iloc[0]]}
        new_paths = [(path, 1) for _, paths in grouped_paths.items() for path in paths]

        print("grouped_paths", grouped_paths)

    else:

        # Convert strings to feature vectors
        vectorizer = TfidfVectorizer()
        X = vectorizer.fit_transform(df["filename_glob"])
        print("X is:", X)

        # Determine the maximum number of clusters dynamically
        max_clusters = int(np.sqrt(len(df)))

        silhouette_scores = []
        for k in range(2, max_clusters + 1):
            print("max clusters is", max_clusters)
            # Perform clustering
            clustering = AgglomerativeClustering(n_clusters=k)
            clusters = clustering.fit_predict(X.toarray())

            # Calculate the silhouette score
            score = silhouette_score(X, clusters)
            print("clusters are:", clusters)
            silhouette_scores.append(score)



            # Find the optimal number of clusters based on the silhouette scores
            optimal_k = np.argmax(silhouette_scores) + 2  # Add 2 because range starts from 2

            print("Optimal number of clusters:", optimal_k)

        # Perform clustering with the optimal number of clusters
        clustering = AgglomerativeClustering(n_clusters=optimal_k)
        clusters = clustering.fit_predict(X.toarray())
        print("clusters are", clusters)
        grouped_paths = {}
        for i, cluster_label in enumerate(clusters):
            if cluster_label not in grouped_paths:
                grouped_paths[cluster_label] = []
            grouped_paths[cluster_label].append(df["filename_glob"].iloc[i])

        # Group paths based on file extensions
        grouped_by_extension = {}
        for cluster_label, paths in grouped_paths.items():
            grouped_by_extension[cluster_label] = {}
            for path in paths:
                 file_extension = os.path.splitext(path)[1]
                 if file_extension not in grouped_by_extension[cluster_label]:
                    grouped_by_extension[cluster_label][file_extension] = []
                 grouped_by_extension[cluster_label][file_extension].append(path)

        new_paths = []
        for _, group in grouped_paths.items():
            if len(group) > 1:
                merged_path = ""
                max_length = max(len(path) for path in group)
                differing_chars_encountered = False
                common_extension = None

                for i in range(max_length):
                    chars = set(path[i] if len(path) > i else "" for path in group)
                    if len(chars) == 1:
                        merged_path += chars.pop()
                        differing_chars_encountered = True
                    else:
                        if differing_chars_encountered:
                            merged_path += "(.*)"
                            differing_chars_encountered = False
                            break

                # Check if all paths have the same file extension
                extensions = [os.path.splitext(path)[1] for path in group]
                common_extension = None
                if len(set(extensions)) == 1:
                    common_extension = extensions[0]


                # Append the common extension if it exists and it's not already in the merged_path
                if common_extension and common_extension not in merged_path:
                    merged_path += common_extension

                new_paths.append((merged_path, len(group)))
            else:
                new_paths.append((group[0], 1))



        if verbose:
            new_paths_verbose = []

            # Sort grouped_paths based on the size of each group (in descending order)
            sorted_groups = sorted(grouped_paths.items(), key=lambda x: len(x[1]), reverse=True)

            for cluster_label, paths in sorted_groups:

                if len(paths) > 1:
                    merged_path = ""
                    max_length = max(len(path) for path in paths)
                    differing_chars_encountered = False
                    common_extension = None


                    for i in range(max_length):
                        chars = set(path[i] if len(path) > i else "" for path in paths)
                        if len(chars) == 1:
                            merged_path += chars.pop()
                            differing_chars_encountered = True
                        else:
                            if differing_chars_encountered:
                                merged_path += "(.*)"
                                differing_chars_encountered = False
                                break

                    # Check if all paths have the same file extension
                    extensions = [os.path.splitext(path)[1] for path in paths]
                    common_extension = None
                    if len(set(extensions)) == 1:
                        common_extension = extensions[0]

                    # Append the merged path if it's not already in the new_paths_verbose list
                    if merged_path and (merged_path, len(paths)) not in new_paths_verbose:
                        new_paths_verbose.append((merged_path, len(paths)))

                    # Append the individual paths beneath the merged path
                    new_paths_verbose.extend([(f"    {path}", 1) for path in paths])
                else:
                    new_paths_verbose.append((group[0], 1))


            df_verbose = pd.DataFrame(new_paths_verbose, columns=["filename_glob", "glob_count"])
            print(df_verbose.to_string(index=False))


        # Display or save the DataFrame using pandas styler
        if verbose:
            df_verbose = pd.DataFrame(new_paths_verbose, columns=["filename_glob", "glob_count"])
            styled_html = df_verbose.style.background_gradient(axis=0, cmap="viridis", gmap=df_verbose["glob_count"])
            styled_html = styled_html.set_properties(subset=["glob_count"], **{"text-align": "right"})
            styled_html.hide(axis="index")
            styled_html.set_table_styles([
                {"selector": "", "props": [("border", "1px solid grey")]},
                {"selector": "tbody td", "props": [("border", "1px solid grey")]},
                {"selector": "th", "props": [("border", "1px solid grey")]}
            ])
            html = styled_html.to_html()

            with open(output_path, "w") as html_file:
                html_file.write(html)

        else:
            df = pd.DataFrame(new_paths, columns=["filename_glob", "glob_count"])
            df = df.sort_values(by="glob_count", ascending=False)

            styled_html = df.style.background_gradient(axis=0, cmap="viridis", gmap=df["glob_count"])
            styled_html = styled_html.set_properties(subset=["glob_count"], **{"text-align": "right"})
            styled_html.hide(axis="index")
            styled_html.set_table_styles([
                {"selector": "", "props": [("border", "1px solid grey")]},
                {"selector": "tbody td", "props": [("border", "1px solid grey")]},
                {"selector": "th", "props": [("border", "1px solid grey")]}
            ])
            html = styled_html.to_html()

            with open(output_path, "w") as html_file:
                html_file.write(html)

            print("Styled results saved to:", output_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--log-path', type=str, help="Path to the log file")
    parser.add_argument('-o', '--output-path', type=str, help="Path to the output HTML file")
    parser.add_argument('-v', '--verbose', action='store_true', help="Display verbose output")
    args = parser.parse_args()
    main(log_path=args.log_path, output_path=args.output_path, verbose=args.verbose)


