# -*- coding: utf-8 -*-
"""
Created on Mon Jul 21 19:17:22 2025

@author: cahase
"""

import pandas as pd
import numpy as np

inpath = 'G:/My Drive/GSEFM/Research/e_cigarettes/data/raw_data/laus/'

# read in laus data
area_df  = pd.read_csv(inpath + 'la.area.txt', sep='\t', dtype = 'str')
series_df  = pd.read_csv(inpath + 'la.series.txt', sep='\t', dtype = 'str')
data_df  = pd.read_csv(inpath + 'la.data.txt', sep='\t', dtype = 'str')

#%%# Extract county FIPS from LAUS area_code

# 1. Filter for counties only
area_df = area_df[area_df['area_code'].str.startswith('CN')].copy()

county_areas = area_df.copy()

# 2. Extract state and county FIPS
county_areas['state_fips'] = county_areas['area_code'].str[2:4]
county_areas['county_code'] = county_areas['area_code'].str[4:7]
county_areas['county_fips'] = county_areas['state_fips'] + county_areas['county_code']

#%% Load HUD ZIP-to-County Crosswalk

crosswalk = pd.read_excel(inpath + "COUNTY_ZIP_122024.xlsx", dtype=str)  # Adjust filename

cols = list(crosswalk)

crosswalk.columns = crosswalk.columns.str.lower()
crosswalk.rename(columns = {'usps_zip_pref_state': 'state'}, inplace = True)

crosswalk = crosswalk[['zip', 'county', 'state', 'tot_ratio']].copy()
crosswalk['zip'] = crosswalk['zip'].str.zfill(5)
crosswalk['county'] = crosswalk['county'].str.zfill(3)

# # State abbreviation → FIPS code mapping
# state_to_fips = {
#     'AL': '01', 'AK': '02', 'AZ': '04', 'AR': '05', 'CA': '06',
#     'CO': '08', 'CT': '09', 'DE': '10', 'DC': '11', 'FL': '12',
#     'GA': '13', 'HI': '15', 'ID': '16', 'IL': '17', 'IN': '18',
#     'IA': '19', 'KS': '20', 'KY': '21', 'LA': '22', 'ME': '23',
#     'MD': '24', 'MA': '25', 'MI': '26', 'MN': '27', 'MS': '28',
#     'MO': '29', 'MT': '30', 'NE': '31', 'NV': '32', 'NH': '33',
#     'NJ': '34', 'NM': '35', 'NY': '36', 'NC': '37', 'ND': '38',
#     'OH': '39', 'OK': '40', 'OR': '41', 'PA': '42', 'RI': '44',
#     'SC': '45', 'SD': '46', 'TN': '47', 'TX': '48', 'UT': '49',
#     'VT': '50', 'VA': '51', 'WA': '53', 'WV': '54', 'WI': '55',
#     'WY': '56'
# }

# # Add state FIPS
# crosswalk['state_fips'] = crosswalk['state'].map(state_to_fips)

crosswalk.rename(columns = {'county': 'county_fips'}, inplace = True)

crosswalk.drop(columns = 'state_fips', inplace = True)

#%% merge crosswalk to county_areas

area_df_crosswalked = crosswalk.merge(county_areas[['area_code', 'area_text', 'county_fips']], on='county_fips', how='left')

#%% compile laus data series

# Strip whitespace from columns
for df in [county_areas, series_df, data_df]:
    df.columns = [c.strip() for c in df.columns]
    for col in df.select_dtypes(include='object').columns:
        df[col] = df[col].str.strip()

# Filter to unemployment rate series only (code ends in '0000000003')
series_df = series_df[series_df['series_id'].str.endswith('0000000003')]

# Merge series with data
unemp_df = data_df.merge(series_df[['series_id', 'area_code']], on='series_id')

# Merge with area names
unemp_df = unemp_df.merge(county_areas[['area_code', 'area_text']], on='area_code')

# Remove annual average rows (period = M13)
unemp_df = unemp_df[unemp_df['period'].str.startswith('M') & (unemp_df['period'] != 'M13')].copy()

# Filter by year
unemp_df['year'] = unemp_df['year'].astype(int)
unemp_df = unemp_df[unemp_df['year'].between(2022, 2025)]

# Construct proper date
unemp_df['month'] = unemp_df['period'].str[1:].astype(int)
unemp_df['date'] = pd.to_datetime(dict(year=unemp_df.year, month=unemp_df.month, day=1)).dt.to_period('M')

#%% merge crosswalk df to unemp df

unemp_df.dtypes
area_df_crosswalked.dtypes

# Strip whitespace from columns
for df in [unemp_df, area_df_crosswalked]:
    df.columns = [c.strip() for c in df.columns]
    for col in df.select_dtypes(include='object').columns:
        df[col] = df[col].str.strip()

# merge
merged_df = pd.merge(unemp_df, area_df_crosswalked[['zip', 'county_fips', 'state', 'tot_ratio', 'area_code', 'area_text']], 
                     on = ['area_code', 'area_text'], 
                     how = 'left', 
                     indicator = True)

nomerge = merged_df.loc[merged_df['_merge'] != 'both'].copy()
del nomerge
merged_df.drop(columns = '_merge', inplace = True)

merged_df.rename(columns = {'value': 'unemp_rate'}, inplace = True)

head = merged_df.head(10000)

#%%

merged_df.dtypes

cols = ['unemp_rate', 'tot_ratio']
merged_df[cols] = merged_df[cols].astype('float')

# Step 4: Calculate ZIP-level unemployment rates (with weighting if necessary)
merged_df['weighted_unemp_rate'] = merged_df['unemp_rate'] * merged_df['tot_ratio'].astype(float)

# Aggregate to ZIP level by date
zip_unemp = (
    merged_df
    .groupby(['zip', 'date'])
    .agg(weighted_unemp_rate = ('weighted_unemp_rate', 'sum'))
    .reset_index()
)

head = zip_unemp.head(1000)

zips = pd.DataFrame(zip_unemp['zip'].unique())

zip_unemp.rename(columns = {'zip': 'zip_code'}, inplace = True)

#%% first-difference in unemp rates

# copy for backup
zip_unemp_backup = zip_unemp.copy()

# Sort the DataFrame by store_id, product_id, and date
zip_unemp = zip_unemp.sort_values(by=['zip_code', 'date'])

# Calculate the month difference between consecutive rows within each group
zip_unemp['month_diff'] = zip_unemp.groupby(['zip_code'])['date'].diff().apply(lambda x: x.n if pd.notna(x) else np.nan)

# Group by 'store_id' and 'product_id' and calculate the lagged 'unit_value' only for consecutive months
zip_unemp['lag_weighted_unemp_rate'] = zip_unemp.groupby(['zip_code'])['weighted_unemp_rate'].shift(1)

head = zip_unemp.head(1000)

# Set lagged values to NaN if the time difference is not exactly 1 month
zip_unemp['lag_weighted_unemp_rate'] = np.where(zip_unemp['month_diff'] == 1, zip_unemp['lag_weighted_unemp_rate'], np.nan)

zip_unemp = zip_unemp.fillna(value=np.nan)

head = zip_unemp.head(1000)

# first difference unemp rate
zip_unemp['d_weighted_unemp_rate'] = zip_unemp['weighted_unemp_rate'] - zip_unemp['lag_weighted_unemp_rate']

cols = list(zip_unemp)

#%% subset columns and save
zip_unemp = zip_unemp[['zip_code', 'date', 'd_weighted_unemp_rate']].copy()

zip_unemp.to_feather('G:/My Drive/GSEFM/Research/e_cigarettes/data/processed_data/laus/zip_unemp_rates.feather')

#%%






