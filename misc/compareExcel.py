# This Python file is to compare output produced by Pandas & Spark

import pandas as pd
file1 = pd.read_csv('results/Sep2022_sample_cleaned.csv')
file2 = pd.read_csv('/Users/kenho/Documents/GitHub/AY2425_Ken_Process-Optimisation/results/part-00000-0a45fcc1-3434-4de6-874e-682db167d276-c000.csv')

# Merge the DataFrames to find missing rows
merged_df = file1.merge(file2, how='left', indicator=True)
missing_rows = merged_df[merged_df['_merge'] == 'left_only']
if missing_rows.empty:
    print("No missing rows found. All records from file1.csv are present in file2.csv.")
else:
    print("Missing rows:")
missing_rows = missing_rows.drop(columns=['_merge'])
print(missing_rows)


