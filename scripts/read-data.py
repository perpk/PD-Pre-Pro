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

if __name__ == '__main__':
    main()
