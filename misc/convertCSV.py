# This Python file is to convert DT_START AND DT_END to appropriate format in Sep2022_sample.csv
import pandas as pd

# Load the CSV file
df = pd.read_csv('/Users/kenho/Documents/GitHub/AY2425_Ken_Process-Optimisation/resources/Sep2022_sample.csv')

df = df.loc[:, ~df.columns.str.contains('^Unnamed')]

df['DT_START'] = pd.to_datetime(df['DT_START'], format='%Y-%m-%d %H:%M:%S').dt.strftime('%d/%m/%Y %H:%M:%S')
df['DT_END'] = pd.to_datetime(df['DT_END'], format='%Y-%d-%m %H:%M:%S').dt.strftime('%d/%m/%Y %H:%M:%S')

# Save to a new CSV file
df.to_csv('processed_data.csv', index=False)
