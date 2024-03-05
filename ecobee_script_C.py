import numpy as np
import pandas as pd
from pytz import timezone
from datetime import datetime

# Load the files
pd.set_option('display.max_columns', None)
attune_file_path = 'https://raw.githubusercontent.com/JadenFaste/EcobeeScript/main/Attune%20Jan%202024%20Bear%20Creek.csv'
ecobee_file_path = 'https://raw.githubusercontent.com/JadenFaste/EcobeeScript/main/ECOBEE_5MIN_D.csv'

# Read the data
attune_df = pd.read_csv(attune_file_path, skiprows=[1, 2])
ecobee_df = pd.read_csv(ecobee_file_path)

# Drop specific columns from attune_df
cols_to_drop = [col for col in attune_df.columns if col.startswith('C.') or '#C' in col]
attune_df = attune_df.drop(columns=cols_to_drop)

# Prepare Ecobee DataFrame
ecobee_df['TIME'] = pd.to_datetime(ecobee_df['TIME'])
ecobee_df['TIME'] = ecobee_df['TIME'].dt.tz_localize('UTC')
ecobee_df.set_index('TIME', inplace=True)

# Upsample to 5-minute frequency for data merging
ecobee_df = ecobee_df.resample('5T').ffill()

# Convert index to string for merging
ecobee_df.index = ecobee_df.index.strftime('%m/%d/%Y %H:%M')

# Prepare Attune DataFrame
attune_df.rename(columns={attune_df.columns[0]: 'DateTime'}, inplace=True)
attune_df['DateTime'] = pd.to_datetime(attune_df['DateTime'])
attune_df['DateTime'] = attune_df['DateTime'].dt.tz_localize('America/Los_Angeles').dt.tz_convert('UTC')
attune_df.set_index('DateTime', inplace=True)
attune_df.index = attune_df.index.strftime('%m/%d/%Y %H:%M')

# Merge DataFrames
merged_df = pd.merge(attune_df.reset_index(), ecobee_df.reset_index(), left_on='DateTime', right_on='TIME', how='outer')
merged_df.set_index('DateTime', inplace=True)

# Convert merged_df index back to PST
# Convert 'DateTime' index back to datetime, localize to UTC, convert to PST, and optionally convert back to strin
merged_df.index = pd.to_datetime(merged_df.index).tz_localize('UTC').tz_convert('America/Los_Angeles')
merged_df.index = merged_df.index.strftime('%m/%d/%Y %H:%M')

# Display the first few rows of the merged dataframe to confirm success
print(merged_df.head())

# Convert to csv
merged_df.to_csv('test_D6.csv')

