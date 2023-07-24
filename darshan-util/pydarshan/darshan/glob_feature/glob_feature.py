# Creates a DataFrame with two columns ("glob_filename" and "glob_count") based on the files read by a .darshan file.
# The script utilizes agglomerative hierarchical clustering to effectively group similar file paths together, based on their characteristics.
# It then displays a dataframe where one file represents a group and uses [.*] to show where filepaths within a group differ 
# The result of this process is an HTML report that provides a comprehensive overview of the grouped paths and their respective counts. 
# Command to run: python glob_feature.py -p path/to/log/file.darshan -o path/to/output_file 

import argparse
import pandas as pd
import darshan
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.cluster import AgglomerativeClustering
from sklearn.metrics import silhouette_score
import numpy as np
import os


def main(log_path, output_path):

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
                            merged_path += "[.*]"
                            differing_chars_encountered = False

                # Checks if all paths have the same file extension
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


    # Save the results to an output file
    df = pd.DataFrame(new_paths, columns=["filename_glob", "glob_count"])

    df = df.sort_values(by="glob_count", ascending=False)
    print("df is", df)
    style = df.style.background_gradient(axis=0, cmap="viridis", gmap=df["glob_count"])
    style = style.set_properties(subset=["glob_count"], **{"text-align": "right"})
    style.hide(axis="index")
    style.set_table_styles([
        {"selector": "", "props": [("border", "1px solid grey")]},
        {"selector": "tbody td", "props": [("border", "1px solid grey")]},
        {"selector": "th", "props": [("border", "1px solid grey")]}
    ])

    html = style.to_html()

    with open(output_path, "w") as html_file:
        html_file.write(html)

    total_count = df["glob_count"].sum()
    print("Total glob_count:", total_count)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--log-path', type=str, help="Path to the log file")
    parser.add_argument('-o', '--output-path', type=str, help="Path to the output HTML file")
    args = parser.parse_args()
    main(log_path=args.log_path, output_path=args.output_path)


