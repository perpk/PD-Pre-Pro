import pandas as pd
import numpy as np
import glob
import os
from tqdm import tqdm
from multiprocessing import Pool, cpu_count


def read_csv(filename):
    df = pd.read_csv(filename, sep="\t", comment="#", header=None, skiprows=1)
    df.columns = ["GeneID", "Chr", "Start", "End", "Strand", "Length", "Counts"]
    df = df[["GeneID", "Counts"]].set_index("GeneID")
    df.rename(columns={"Counts": os.path.basename(filename)}, inplace=True)
    return df

def main():
    num_workers = min(6, cpu_count())
    count_files = glob.glob("/Volumes/Elements/counts/*.txt")
    count_files_len = len(count_files)
    count_files_len=10
    all_counts = []
    with Pool(num_workers) as pool:
        for result in tqdm(pool.imap_unordered(read_csv, count_files[1:10]), total=count_files_len, desc="Reading files"):
            all_counts.append(result)

    all_counts = pd.concat(all_counts, axis=1, join="outer")
    all_counts = all_counts.apply(pd.to_numeric, errors="coerce").fillna(0)
    all_counts.to_csv("ppmi_counts_matrix.csv", index=True)

    print("Done")

    # count_files = glob.glob("/Volumes/Elements/counts/*.txt")
    # count_files_len = len(count_files)
    # print(f"Found {len(count_files)} count files")
    # combined_df = None
    # with Pool(processes=6) as pool, tqdm(total=count_files_len) as pbar:
    #     for result in pool.imap(read_csv, count_files):
    #         pbar.update()
    #         pbar.refresh()
    #         if combined_df is None:
    #             combined_df = result
    #         else:
    #             combined_df = combined_df.merge(result, on="Geneid", how="outer")
    #
    # combined_df.set_index("Geneid", inplace=True)
    # combined_df = combined_df.apply(pd.to_numeric, errors="coerce")
    # combined_df.fillna(0, inplace=True)
    # combined_df = combined_df[(combined_df.sum(axis=1) > 0)]
    # combined_df.to_csv("ppmi_counts_matrix.csv")


if __name__ == '__main__':
    main()

# all_counts = None
# for file in tqdm(count_files, desc="Processing Count Files"):
#     df = pd.read_csv(file, sep="\t", comment="#", header=None)
#     df.columns = ["Geneid", "Chr", "Start", "End", "Strand", "Length", os.path.basename(file)]
#     df = df[["Geneid", os.path.basename(file)]]
#     if all_counts is None:
#         all_counts = df
#     else:
#         all_counts = all_counts.merge(df, on="Geneid", how="outer")
#
# all_counts.set_index("Geneid", inplace=True)
