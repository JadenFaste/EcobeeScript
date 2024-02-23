import numpy as np
import pandas as pd
from pytz import timezone
from datetime import datetime

# Load the files
pd.set_option('display.max_columns', None)
attune_file_path = 'https://raw.githubusercontent.com/JadenFaste/EcobeeScript/main/Attune%20Jan%202024%20Bear%20Creek.csv'
ecobee_file_path = 'https://raw.githubusercontent.com/JadenFaste/EcobeeScript/main/ECOBEE_5MIN_D.csv'

# Read the data with the first two rows for the header to capture units
attune_df = pd.read_csv(attune_file_path, header=[0, 1])
ecobee_df = pd.read_csv(ecobee_file_path)

# Concatenate the header and units
new_header = [f'{a} ({b})' if b else f'{a}' for a, b in zip(attune_df.columns.get_level_values(0), attune_df.columns.get_level_values(1))]
attune_df.columns = new_header

# Drop the columns with specific patterns
cols_to_drop = [col for col in attune_df.columns if col.startswith('D.') or '#D' in col]

# Drop the columns
attune_df = attune_df.drop(columns=cols_to_drop)

# Prepare Attune DataFrame
attune_df.rename(columns={attune_df.columns[0]: 'DateTime'}, inplace=True)
attune_df['DateTime'] = pd.to_datetime(attune_df.iloc[:, 0], errors='coerce')
attune_df = attune_df.dropna(subset=['DateTime'])  # Drop rows where DateTime could not be parsed
attune_df['DateTime'] = attune_df['DateTime'].dt.tz_localize('America/Los_Angeles').dt.tz_convert('UTC')
attune_df.set_index('DateTime', inplace=True)
attune_df_5min = attune_df[attune_df.index.minute % 5 == 0]
attune_df_5min.index = attune_df_5min.index.strftime('%m/%d/%Y %H:%M')

# Prepare Ecobee DataFrame
ecobee_df['TIME'] = pd.to_datetime(ecobee_df['TIME'])
ecobee_df['TIME'] = ecobee_df['TIME'].dt.tz_localize('UTC')
ecobee_df.set_index('TIME', inplace=True)
ecobee_df.index = ecobee_df.index.strftime('%m/%d/%Y %H:%M')

# Perform the merge on the indices, which are now strings and should match
merged_df = pd.merge(attune_df_5min.reset_index(), ecobee_df.reset_index(), left_on='DateTime', right_on='TIME', how='inner')

merged_df.set_index('DateTime', inplace=True)

# Display the first few rows of the merged dataframe to confirm success
print(merged_df.head())

merged_df.to_csv('attune_ecobee_combo_C.csv')
# Display the first few rows of the merged dataframe to confirm success
print(merged_df.head())