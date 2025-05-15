# This Python file is for misc tasks (Splitting data into smaller chunks, etc.) for testing purposes

import pandas as pd

# Load the CSV file in chunks
file_path = "resources/Jan2022_part1.csv"
df = pd.read_csv(file_path)

# Find the middle index to split the data
# mid_index = len(df) // 2

# # Split into two DataFrames
# df1 = df.iloc[:mid_index]
# df2 = df.iloc[mid_index:]

# # Save them as two separate CSV files
# df1.to_csv("resources/Jan2022_part1.1.1.csv", index=False)
# df2.to_csv("resources/Jan2022_part1.1.2.csv", index=False)

print(df.dtypes)

