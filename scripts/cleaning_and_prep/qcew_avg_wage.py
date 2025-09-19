# -*- coding: utf-8 -*-
"""
Created on Wed Jul 23 12:54:04 2025

@author: cahase
"""

import pandas as pd
import numpy as np
import glob
import os

inpath = 'G:/My Drive/GSEFM/Research/e_cigarettes/data/raw_data/qcew/'

# list excel files
excel_files = glob.glob(inpath + "*.xlsx") # 18214 files (previously 18940 files)

# # Normalize to use the correct slash for the OS
# excel_files = [os.path.normpath(path) for path in excel_files]

excel_files = [p.replace('\\', '/') for p in excel_files]

print(repr(my_list)) 


to_drop = ['G:/My Drive/GSEFM/Research/e_cigarettes/data/raw_data/qcew/allhlcn22.xlsx', 
                    'G:/My Drive/GSEFM/Research/e_cigarettes/data/raw_data/qcew/allhlcn23.xlsx',
                    'G:/My Drive/GSEFM/Research/e_cigarettes/data/raw_data/qcew/allhlcn24.xlsx']

filtered = [x for x in excel_files if x not in to_drop]

excel_files = filtered.copy()
del filtered

filtered_dfs = []

for path in excel_files:
    df = pd.read_excel(path)
    filtered = df[
        ((df['Area Type'] == 'County') &
        (df['Ownership'] == 'Total Covered'))
    ]
    filtered_dfs.append(filtered)

# Optionally, combine into one DataFrame
combined_df = pd.concat(filtered_dfs, ignore_index=True)

combined_df.columns = combined_df.columns.str.lower()

#%%

cols = list(combined_df)

# Replace all whitespace characters (like \n, \r, \t) with a space
combined_df.columns = combined_df.columns.str.replace(r'\s+', '_', regex=True).str.strip()

combined_df.rename(columns = {
    'average_weekly_wage': 'avg_weekly_wage',
    'area': 'area_text'
    }, inplace = True)


cols_to_keep = ['area_code', 'area_text', 'year', 'qtr', 'avg_weekly_wage']

wage_df = combined_df[cols_to_keep].copy()

wage_df['qtr2'] = 'Q' + wage_df['qtr'].astype(str)
wage_df['qy'] = wage_df['year'].astype(str) + wage_df['qtr2']

wage_df.rename(columns = {'area_code': 'county_fips'}, inplace = True)

#%% crosswalk to zip code

wage_df_backup = wage_df.copy()

#wage_df = wage_df_backup.copy()

crosswalk = pd.read_excel("G:/My Drive/GSEFM/Research/e_cigarettes/data/raw_data/laus/COUNTY_ZIP_122024.xlsx", dtype=str)  # Adjust filename

cols = list(crosswalk)

crosswalk.columns = crosswalk.columns.str.lower()
crosswalk.rename(columns = {'usps_zip_pref_state': 'state'}, inplace = True)

crosswalk = crosswalk[['zip', 'county', 'state', 'tot_ratio']].copy()
crosswalk['zip'] = crosswalk['zip'].str.zfill(5)
crosswalk['county'] = crosswalk['county'].str.zfill(3)

crosswalk.rename(columns = {'county': 'county_fips'}, inplace = True)

#%% merge crosswalk to county_areas

# Strip whitespace from columns
for df in [crosswalk, wage_df]:
    df.columns = [c.strip() for c in df.columns]
    for col in df.select_dtypes(include='object').columns:
        df[col] = df[col].str.strip()

wage_df.dtypes
crosswalk.dtypes

wage_df_crosswalked = pd.merge(crosswalk, wage_df,
                               how = 'left',
                               on = 'county_fips',
                               indicator = True)

head = wage_df_crosswalked.head(10000)

nomerge = wage_df_crosswalked.loc[wage_df_crosswalked['_merge'] != 'both'].copy()
del nomerge

wage_df_crosswalked.drop(columns = '_merge', inplace = True)

#%%

wage_df_crosswalked['tot_ratio'] = wage_df_crosswalked['tot_ratio'].astype('float')

# Step 4: Calculate ZIP-level unemployment rates (with weighting if necessary)
wage_df_crosswalked['weighted_avg_weekly_wage'] = wage_df_crosswalked['avg_weekly_wage'] * wage_df_crosswalked['tot_ratio'].astype(float)

# Aggregate to ZIP level by date
zip_wage = (
    wage_df_crosswalked
    .groupby(['zip', 'qy'])
    .agg(weighted_avg_weekly_wage = ('weighted_avg_weekly_wage', 'sum'))
    .reset_index()
)

head = zip_wage.head()

#%%

zip_wage['qy2'] = pd.to_datetime(zip_wage['qy']).dt.to_period('Q')

zip_wage.drop(columns = 'qy', inplace = True)
zip_wage.rename(columns = {'qy2': 'qy'}, inplace = True)

zip_wage.rename(columns = {'zip': 'zip_code'}, inplace = True)

# Sort the DataFrame by store_id, product_id, and date
zip_wage = zip_wage.sort_values(by=['zip_code', 'qy'])

# Calculate the month difference between consecutive rows within each group
zip_wage['qtr_diff'] = zip_wage.groupby(['zip_code'])['qy'].diff().apply(lambda x: x.n if pd.notna(x) else np.nan)

# Group by 'store_id' and 'product_id' and calculate the lagged 'unit_value' only for consecutive months
zip_wage['lag_weighted_avg_weekly_wage'] = zip_wage.groupby(['zip_code'])['weighted_avg_weekly_wage'].shift(1)

# Set lagged values to NaN if the time difference is not exactly 1 month
zip_wage['lag_weighted_avg_weekly_wage'] = np.where(zip_wage['qtr_diff'] == 1, zip_wage['lag_weighted_avg_weekly_wage'], np.nan)

zip_wage = zip_wage.fillna(value=np.nan)

#%% create index

zip_wage['l_avg_wage_index'] = np.log(zip_wage['weighted_avg_weekly_wage'] / zip_wage['lag_weighted_avg_weekly_wage'])

zip_wage['l_avg_wage_index'].replace([np.inf, -np.inf], np.nan, inplace=True)

#%% save

zip_wage.to_feather('G:/My Drive/GSEFM/Research/e_cigarettes/data/processed_data/qcew/zip_avg_wage.feather')

#%%


















