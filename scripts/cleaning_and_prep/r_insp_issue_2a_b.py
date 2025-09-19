# -*- coding: utf-8 -*-
"""
Created on Tue Jan  7 13:51:55 2025

@author: cahase
"""

import numpy as np
import pandas as pd
import glob
import os
import shutil
import gc
import pyarrow as pa
import geopandas as gpd

'''
This script assigns treatment to stores in the same zip code as the targeted store, and kicks the targeted store out of the sample.

'''

#%% create panel of store-dates

# list of files (stores)
source_dir = "D:/convenience_store/data/processed/LS_Otter/indexes/subcat_indexes_otp_1b/"

all_files = glob.glob(source_dir + "*.feather") # 18214 files (previously 18940 files)

# Normalize to use the correct slash for the OS
all_files = [os.path.normpath(path) for path in all_files]

# Extract store numbers
store_ids = [os.path.basename(path).split('.')[0] for path in all_files]

# to integer type
#store_ids = [int(x) for x in store_ids]

# Define the date range
date_range = pd.date_range(start="2022-01", end="2025-05", freq="MS")  # 'MS' ensures the start of each month

# Create the panel dataset
panel_data = pd.DataFrame(
    [(store_id, date) for store_id in store_ids for date in date_range],
    columns=["store_id", "date"]
    )

head = panel_data.head(10000)

panel_data['date'] = panel_data['date'].dt.to_period('M')

#%% add store characteristics (zip code, city, state, etc)

store_info = pd.read_csv("D:/convenience_store/data/raw/LS_Otter/STORES NEW/STORES_NEW-0.csv", dtype = {'ZIP_CODE': 'str'})
#store_status_new = pd.read_csv("D:/convenience_store/data/raw/LS_Otter/STORE_STATUS NEW/STORE_STATUS_NEW-0.csv", dtype = {'ZIP_CODE': 'str'})

cols = list(store_info)

store_info.columns = store_info.columns.str.lower()

cols_to_keep = ['store_id', 'city', 'state', 'zip_code', 'store_chain_id']

store_info = store_info[cols_to_keep].copy()

store_info['city'] = store_info['city'].str.strip()
store_info['state'] = store_info['state'].str.strip()
store_info['zip_code'] = store_info['zip_code'].str.strip()

# get rid of any zip codes with hyphens
store_info['zip_code'] = store_info['zip_code'].str.split('-', expand = True)[0]

store_info.dtypes
panel_data.dtypes

store_info['store_id'] = store_info['store_id'].astype('str')
panel_data['store_id'] = panel_data['store_id'].astype('str')

# merge store characterisitcs
panel_data2 = panel_data.copy() # backup copy

panel_data = pd.merge(panel_data, store_info,
                      how = 'left',
                      on = 'store_id',
                      indicator = True)

nomerge = panel_data.loc[panel_data['_merge'] != 'both'].copy()
del nomerge
panel_data.drop(columns = '_merge', inplace = True)

head = panel_data.head(10000)
del panel_data2

# new chain size column
unique_chains = (
    panel_data
    .groupby('store_chain_id')
    ['store_id']
    .nunique()
    .reset_index(name='chain_size')
    )

#panel_data.drop(columns = 'chain_size', inplace = True)

# merge new chain size column to panel data
panel_data = pd.merge(panel_data, unique_chains,
                      how = 'left',
                      on = 'store_chain_id',
                      indicator = True
                      )

nomerge = panel_data.loc[panel_data['_merge'] != 'both'].copy()
del nomerge
panel_data.drop(columns = '_merge', inplace = True)

head = panel_data.head()

#%% read in treatment info

# targeted stores present in pdi data <- we want to drop these from the sample (bad controls)
treatments = pd.read_feather('G:/My Drive/GSEFM/Research/e_cigarettes/data/processed_data/LS_Otter/targeted_stores_treatment.feather')

# list of targeted stores (bad controls)
targeted_list = treatments['store_id'].unique().astype('str').tolist()

# all letters
all_treatments = pd.read_feather('G:/My Drive/GSEFM/Research/e_cigarettes/warning_letters/warning_letters_combined.feather')

all_treatments['insp_date'] = pd.to_datetime(all_treatments['insp_date']).dt.to_period('M')
all_treatments['issue_date'] = pd.to_datetime(all_treatments['issue_date']).dt.to_period('M')

all_treatments['zip_code'] = all_treatments['zip_code'].str.replace('\"', '')
all_treatments['zip_code'] = all_treatments['zip_code'].str.strip()

all_treatments['state'] = all_treatments['state'].str.strip()

head = all_treatments.head()

# number of treatments per zip code (treatment intensity)
targeted_stores_per_zip = (
    all_treatments
    .groupby(['insp_date', 'zip_code'], as_index=False)
    ['firm'].nunique()
    .rename(columns = {'firm': 'stores_targeted_in_zip'})
    )

# number of treatments per state (treatment intensity)
targeted_stores_per_state = (
    all_treatments
    .groupby(['insp_date', 'state'], as_index=False)
    ['firm'].nunique()
    .rename(columns = {'firm': 'stores_targeted_in_state'})
    )

# merge on month_insp and zip
all_treatments = pd.merge(all_treatments, targeted_stores_per_zip,
                          how = 'left',
                          on = ['insp_date', 'zip_code'],
                          indicator = True
                          )

nomerge = all_treatments.loc[all_treatments['_merge'] != 'both'].copy()
del nomerge
all_treatments.drop(columns = '_merge', inplace = True)

all_treatments = pd.merge(all_treatments, targeted_stores_per_state,
                          how = 'left',
                          on = ['insp_date', 'state'],
                          indicator = True
                          )

nomerge = all_treatments.loc[all_treatments['_merge'] != 'both'].copy()
del nomerge
all_treatments.drop(columns = '_merge', inplace = True)

head = all_treatments.head()

cols_to_keep = ['zip_code', 'state', 'insp_date', 'issue_date', 'stores_targeted_in_zip', 'stores_targeted_in_state']

all_treatments = all_treatments[cols_to_keep].copy()

# because some zip codes have multiple stores inspected per cohort there will be duplicates. drop these
all_treatments = all_treatments.drop_duplicates(subset = ['zip_code', 'insp_date'])

#%% prep for treatment assignment

# drop targeted stores from panel_data
panel_data = panel_data.loc[~panel_data['store_id'].isin(targeted_list)].copy()

# dictionary storing store_ids for each insp and issue date
insp_dict = all_treatments.groupby('insp_date')['zip_code'].apply(list).to_dict() 
issue_dict = all_treatments.groupby('issue_date')['zip_code'].apply(list).to_dict() 

# Function to check if a row's store_id appears in the insp_dict for that date
def was_inspected(row):
    return int(row['zip_code'] in insp_dict.get(row['date'], []))

def was_issued(row):
    return int(row['zip_code'] in issue_dict.get(row['date'], []))

#%% assign treatment
panel_data_test = panel_data.copy()

# Apply the function row-wise to assign inspection treatment
panel_data['r_insp'] = panel_data.apply(was_inspected, axis=1)

# Apply the function row-wise to assign issue treatment
panel_data['r_issue'] = panel_data.apply(was_issued, axis=1)

panel_data['r_insp'].unique()

#%% assign static indicator for sister stores

# list of sister store chains
targeted_store_info = store_info.loc[store_info['store_id'].isin(targeted_list)].copy()

# list of targeted chain ids
targeted_chain_id_list = (
    store_info.loc[store_info['store_id'].isin(targeted_list)]
    ['store_chain_id']
    .astype('str')
    .unique()
    .tolist()
    )

# list of stores in targeted chain
targeted_chain_store_id_list = (
    store_info.loc[store_info['store_chain_id'].astype('str').isin(targeted_chain_id_list)]
    ['store_id']
    .unique()
    .tolist()
    )

sister_stores = list(set(targeted_chain_store_id_list) - set(targeted_list))

# add static sister store indicator
panel_data['sister'] = np.where(panel_data['store_id'].isin(sister_stores), 1, 0)

head = panel_data.head()

#%% assign static indicators for rival and control group zips

# list of all stores in data
all_zips = panel_data['zip_code'].unique().tolist()

# static list of targeted zip codes
targeted_zips = all_treatments['zip_code'].unique().tolist()

non_targeted_zips = list(set(all_zips) - set(targeted_zips))

# indicator for non targeted zips (this in conjunction with the sister indicator identifies control stores)
panel_data['rival'] = np.where(panel_data['zip_code'].isin(targeted_zips), 1, 0)
panel_data['non_rival'] = np.where(panel_data['zip_code'].isin(non_targeted_zips), 1, 0)

foo = panel_data.loc[panel_data['rival'] == 1]['store_id'].unique().tolist()
foo2 = panel_data.loc[panel_data['non_rival'] == 1]['store_id'].unique().tolist()
del foo

#%% merge quantity indexes to store-month panel, drop store-months with no index

# read in indexes
qty_indexes = pd.read_feather('D:/convenience_store/data/processed/LS_Otter/indexes/subcat_qty_otp_1b.feather')

qty_indexes.dtypes
panel_data.dtypes

# important step: convert to str type otherwise many wont merge (despite already being object type)
qty_indexes['store_id'] = qty_indexes['store_id'].astype('str')
panel_data['store_id'] = panel_data['store_id'].astype('str')

# merge indexes to panel data
estimation_panel = pd.merge(panel_data, qty_indexes,
                            how = 'left',
                            on = ['store_id', 'date'],
                            indicator = True)

both = estimation_panel.loc[estimation_panel['_merge'] == 'both'].copy() #285160  
left_only = estimation_panel.loc[estimation_panel['_merge'] == 'left_only'].copy() # 209546


'''
Note: 
    - left only means the scanner data and/or quantity indexes was not sufficient
        or available during the sample period.
'''

# work with the 'both' dataframe from here on out
estimation_panel_all = estimation_panel.copy() # keep origional as backup (includes left_only)

# rename to align with code below
estimation_panel = both.copy()

estimation_panel.drop(columns = '_merge', inplace = True)

cols = list(estimation_panel)

treated = estimation_panel.loc[estimation_panel['r_insp'] == 1]['store_id'].unique() # 95
del treated

# check for duplicated rows (including first occurrence)
duplicates_df = estimation_panel[estimation_panel.duplicated(subset=['store_id', 'date'], keep=False)].sort_values(['store_id', 'date'])
del duplicates_df

#%% create insp date and issue columns
'''
There are 95 treated stores in the sample but only 65 observations where t_insp == 1. In other words, 30 targeted stores
do not have data available the month of treatment. To still be able to assign treatment to these stores in Stata (if desireable),
create a column that has the treatment date for each treated store. This column is time-invariant within a store.
'''

# invert insp_dict
# # Step 1: Invert insp_dict to map store_id → treatment date
# store_to_treatment_date = {
#     store_id: date
#     for date, store_list in insp_dict.items()
#     for store_id in store_list
# }

all_treatments = all_treatments.sort_values(by=['zip_code', 'insp_date'], ascending=False)

# Keep only the first inspection_date for each zip_code
first_treatment = all_treatments.groupby('zip_code', as_index=False).first()

first_treatment['zip_code'].nunique()

first_treatment = first_treatment[['zip_code', 'insp_date', 'issue_date']].copy()

first_treatment.dtypes

first_treatment['zip_code'] = first_treatment['zip_code'].astype('str')
estimation_panel['zip_code'] = estimation_panel['zip_code'].astype('str')

cols = list(estimation_panel)

# merge 
estimation_panel2 = pd.merge(estimation_panel, first_treatment,
                             how = 'left',
                             on = 'zip_code',
                             indicator = True)

merged = estimation_panel2.loc[estimation_panel2['_merge'] == 'both']['store_id'].nunique()
del merged

estimation_panel = estimation_panel2.copy()
del estimation_panel2

foo = estimation_panel.loc[~estimation_panel['insp_date'].isna()]['store_id'].nunique()
del foo

estimation_panel.drop(columns = '_merge', inplace = True)

#%% # census divisions

estimation_panel['store_id'] = estimation_panel['store_id'].astype('int')

states = estimation_panel['state'].unique()
div1 = ['IL', 'IN', 'MI', 'OH', 'WI']
div2 = ['AL', 'KY', 'MS', 'TN']
div3 = ['NJ', 'NY', 'PA']
div4 = ['AZ', 'CO', 'ID', 'MT', 'NM', 'NV', 'UT', 'WY']
div5 = ['CT', 'ME', 'MA', 'NH', 'RI', 'VT']
div6 = ['CA', 'OR', 'WA', 'AK', 'HI']
div7 = ['DE', 'FL', 'GA', 'MD', 'NC', 'SC', 'VA', 'WV', 'DC']
div8 = ['IA', 'KS', 'MN', 'MO', 'NE', 'ND', 'SD']
div9 = ['AR', 'LA', 'OK', 'TX']

estimation_panel['division'] = np.where(estimation_panel['state'].isin(div1), 1,
                                np.where(estimation_panel['state'].isin(div2), 2,
                                 np.where(estimation_panel['state'].isin(div3), 3,
                                  np.where(estimation_panel['state'].isin(div4), 4,
                                   np.where(estimation_panel['state'].isin(div5), 5,
                                    np.where(estimation_panel['state'].isin(div6), 6,
                                     np.where(estimation_panel['state'].isin(div7), 7,
                                      np.where(estimation_panel['state'].isin(div8), 8,
                                       np.where(estimation_panel['state'].isin(div9), 9, np.nan)))))))))

estimation_panel['division'].unique()

#%% zip code store concentration

# count stores per zip code
stores_per_zip = (
    estimation_panel
    #.loc[estimation_panel['inspection_treatment'] == 1] # identify rivals
    .groupby('zip_code')
    ['store_id']
    .nunique()
    .reset_index(name = 'stores_in_zip')
    )

# descriptive stats
stores_per_zip['stores_in_zip'].describe()

estimation_panel = pd.merge(estimation_panel, stores_per_zip,
                            how = 'left',
                            on = 'zip_code',
                            indicator = True)

nomerge = estimation_panel.loc[estimation_panel['_merge'] != 'both'].copy()
del nomerge
estimation_panel.drop(columns = '_merge', inplace = True)

# Check for duplicated rows (including first occurrence)
duplicates_df = estimation_panel[estimation_panel.duplicated(subset=['store_id', 'date'], keep=False)].sort_values(['store_id', 'date'])
del duplicates_df

#%% add multi-state chain indicator

head = estimation_panel.head()

# Step 1: Group by `store_chain_id` and count unique states per chain
state_counts = estimation_panel.groupby('store_chain_id')['state'].nunique()

# Step 2: Create a mapping {chain_id: 1 if multi-state, else 0}
chain_to_multistate = (state_counts > 1).astype(int).to_dict()

# Step 3: Map the indicator back to the original DataFrame
estimation_panel['multi_state_chain'] = estimation_panel['store_chain_id'].map(chain_to_multistate)

estimation_panel['multi_state_chain'].describe()

estimation_panel.dtypes

# count multistate chain stores that get rival treatment
estimation_panel.loc[(estimation_panel['rival'] == 1) & (estimation_panel['multi_state_chain'] == 1)]['store_id'].nunique()
estimation_panel.loc[(estimation_panel['rival'] == 1) & (estimation_panel['multi_state_chain'] != 1)]['store_id'].nunique()

'''
Note: the commented out code below requires updating the input dataframes to reflect the new PDI store ids etc.
'''

#%% geographic inspection rates

estimation_panel['state'] = estimation_panel['state'].astype('str')
estimation_panel['zip_code'] = estimation_panel['zip_code'].astype('str')

state_insp = pd.read_csv('G:/My Drive/GSEFM/Research/e_cigarettes/data/processed_data/state_insp_and_viol_rates.csv')
zip_viol_rate = pd.read_csv('G:/My Drive/GSEFM/Research/e_cigarettes/data/processed_data/zip_viol_rate.csv', dtype = {'zip_code': 'str'})

estimation_panel.dtypes
state_insp.dtypes
zip_viol_rate.dtypes

cols = list(estimation_panel)

estimation_panel = pd.merge(estimation_panel, state_insp,
                            how = 'left',
                            on = 'state',
                            indicator = True)

nomerge = estimation_panel.loc[estimation_panel['_merge'] != 'both'].copy()
del nomerge
estimation_panel.drop(columns = '_merge', inplace = True)

zip_viol_rate.drop(columns = 'zip_viols', inplace = True)
zip_viol_rate.rename(columns = {'violations': 'zip_viols'}, inplace = True)

zip_viol_rate['zip_viol_per_insp'] = np.where(zip_viol_rate['zip_viol_per_insp'].isna(), 0, zip_viol_rate['zip_viol_per_insp'])

zip_viol_rate.dtypes
zip_viol_rate['zip_code'] = zip_viol_rate['zip_code'].astype('str')

estimation_panel = pd.merge(estimation_panel, zip_viol_rate,
                            how = 'left',
                            on = 'zip_code',
                            indicator = True)

nomerge = estimation_panel.loc[estimation_panel['_merge'] != 'both'].copy()
del nomerge

# for zip codes with no inspections or violations, replace nan with zero
estimation_panel['zip_insp'] = np.where(estimation_panel['zip_insp'].isna(), 0, estimation_panel['zip_insp'])
estimation_panel['zip_viols'] = np.where(estimation_panel['zip_viols'].isna(), 0, estimation_panel['zip_viols'])
estimation_panel['zip_viol_per_insp'] = np.where(estimation_panel['zip_viol_per_insp'].isna(), 0, estimation_panel['zip_viol_per_insp'])

estimation_panel.drop(columns = '_merge', inplace = True)

# Check for duplicated rows (including first occurrence)
duplicates_df = estimation_panel[estimation_panel.duplicated(subset=['store_id', 'date'], keep=False)].sort_values(['store_id', 'date'])
del duplicates_df

#%% rural urban commuting area

ruca = pd.read_csv('G:/My Drive/GSEFM/Research/e_cigarettes/data/raw_data/RUCA2010zipcode.csv')

ruca.columns = ruca.columns.str.lower()

ruca.rename(columns = {'\'\'zip_code\'\'': 'zip_code'}, inplace = True)

# drop PO boxes
ruca['zip_type'].unique()

ruca = ruca.loc[ruca['zip_type'] == 'Zip Code Area'].copy()

ruca.dtypes

# remove double quotes 
ruca['zip_code'] = ruca['zip_code'].astype('str').str.replace('\'', '')

ruca['zip_code'] = ruca['zip_code'].astype('str')

ruca.rename(columns = {'state': 'state_ruca'}, inplace = True)

# merge to estimation panel
ruca.dtypes
estimation_panel['zip_code'].dtype

estimation_panel['zip_code'] = estimation_panel['zip_code'].astype('str')

estimation_panel2 = pd.merge(estimation_panel, ruca,
                             how = 'left',
                             on = 'zip_code',
                             indicator = True)

nomerge = estimation_panel2.loc[estimation_panel2['_merge'] != 'both'].copy()
del nomerge
estimation_panel2.drop(columns = '_merge', inplace = True)
estimation_panel = estimation_panel2.copy()

# Check for duplicated rows (including first occurrence)
duplicates_df = estimation_panel[estimation_panel.duplicated(subset=['store_id', 'date'], keep=False)].sort_values(['store_id', 'date'])
del duplicates_df

#%% does zip overlap with census urban area?

#estimation_panel2 = estimation_panel.copy()

# Load ZCTA shapefile (ZIP Code boundaries)
zcta_file = 'G:/My Drive/GSEFM/Research/e_cigarettes/data/raw_data/tl_2024_us_zcta520.zip'
zcta = gpd.read_file(zcta_file)  # GeoPandas reads the .shp inside the zip :contentReference[oaicite:1]{index=1}

# Load Urban Area shapefile
urban_file = 'G:/My Drive/GSEFM/Research/e_cigarettes/data/raw_data/tl_2024_us_uac20.zip'
urban_areas = gpd.read_file(urban_file)

# Ensure Matching CRS (Coordinate Reference System)
zcta = zcta.to_crs(urban_areas.crs)

# Spatial Join to Check for Overlap
# Find ZCTAs that intersect with urban areas
zcta_with_urban = gpd.sjoin(zcta, urban_areas, how='left', predicate='intersects')

# Create indicator: 1 if intersected with an urban area, else 0
zcta_with_urban['urban_overlap'] = zcta_with_urban['index_right'].notna().astype(int)

cols = list(zcta_with_urban)
zcta_with_urban.rename(columns={'ZCTA5CE20': 'zip_code'}, inplace = True)

# Keep only the columns you need
zcta_indicator = zcta_with_urban[['zip_code', 'urban_overlap']].copy()

zcta_indicator['zip_code'].nunique()

# check for duplicated rows (including first occurrence)
duplicates_df = zcta_indicator[zcta_indicator.duplicated(subset=['zip_code'], keep=False)].sort_values(['zip_code'])

# Sort so that rows with urban_overlap == 1 come first
zcta_indicator = zcta_indicator.sort_values(by='urban_overlap', ascending=False)

# Drop duplicate zip_codes, keeping the first (which will be the one with urban_overlap == 1 if present)
zcta_indicator = zcta_indicator.drop_duplicates(subset='zip_code', keep='first')

estimation_panel['zip_code'].dtype
zcta_indicator['zip_code'].dtype

estimation_panel['zip_code'] = estimation_panel['zip_code'].astype('str')
zcta_indicator['zip_code'] = zcta_indicator['zip_code'].astype('str')

# Merge into your original DataFrame
estimation_panel = estimation_panel.merge(zcta_indicator, on='zip_code', how='left', indicator = True)

nomerge = estimation_panel.loc[estimation_panel['_merge'] != 'both'].copy()
del nomerge
estimation_panel.drop(columns = '_merge', inplace = True)

# Fill missing values with 0 (for ZIPs not in shapefile)
estimation_panel['urban_overlap'] = estimation_panel['urban_overlap'].fillna(0).astype(int)

estimation_panel['urban_overlap'].describe()

# check for duplicated rows (including first occurrence)
duplicates_df = estimation_panel[estimation_panel.duplicated(subset=['store_id', 'date'], keep=False)].sort_values(['store_id', 'date'])

#%% merge subcat price indexes otp

#estimation_panel = pd.read_feather('G:/My Drive/GSEFM/Research/e_cigarettes/data/processed_data/LS_Otter/estimation_panels/r_insp_issue_2a_b.feather')

indexes = pd.read_feather('D:/convenience_store/data/processed/LS_Otter/indexes/price_otp_1b.feather')

estimation_panel.dtypes
indexes.dtypes

indexes['store_id'] = indexes['store_id'].astype('int')

# merge
estimation_panel2 = pd.merge(estimation_panel, indexes,
                             how = 'left',
                             on = ['store_id', 'date'],
                             indicator = True)


nomerge = estimation_panel2.loc[estimation_panel2['_merge'] != 'both'].copy()
del nomerge
estimation_panel2.drop(columns = '_merge', inplace = True)

estimation_panel = estimation_panel2.copy()

#%% cmp collected vs issued, 2010-2019

#estimation_panel = pd.read_feather('G:/My Drive/GSEFM/Research/e_cigarettes/data/processed_data/LS_Otter/estimation_panels/r_insp_issue_2a_b.feather')

cmp_all = pd.read_csv('G:/My Drive/GSEFM/Research/e_cigarettes/data/processed_data/cmps_issued_vs_collected.csv')

estimation_panel['state'] = estimation_panel['state'].astype('str')
cmp_all['state'] = cmp_all['state'].astype('str')

estimation_panel = pd.merge(estimation_panel, cmp_all,
                            how = 'left',
                            on = 'state',
                            indicator = True)

nomerge = estimation_panel.loc[estimation_panel['_merge'] != 'both'].copy()
nomerge['state'].unique()
del nomerge

estimation_panel.drop(columns = '_merge', inplace = True)

#%% follow up inspection rates, 2010-2019

#estimation_panel = pd.read_feather('G:/My Drive/GSEFM/Research/e_cigarettes/data/processed_data/LS_Otter/estimation_panels/r_insp_issue_2a_b.feather')

fda_all = pd.read_feather('G:/My Drive/GSEFM/Research/e_cigarettes/data/processed_data/fda_follow_up_insps_2010_2019.feather')

estimation_panel['state'] = estimation_panel['state'].astype('str')
fda_all['state'] = fda_all['state'].astype('str')

estimation_panel = pd.merge(estimation_panel, fda_all,
                            how = 'left',
                            on = 'state',
                            indicator = True)

nomerge = estimation_panel.loc[estimation_panel['_merge'] != 'both'].copy()
nomerge['state'].unique()
del nomerge

estimation_panel.drop(columns = '_merge', inplace = True)

#%% US district court penalty collection case rates

#estimation_panel = pd.read_feather('G:/My Drive/GSEFM/Research/e_cigarettes/data/processed_data/LS_Otter/estimation_panels/r_insp_issue_2a_b.feather')

cases = pd.read_feather('G:/My Drive/GSEFM/Research/e_cigarettes/misc/penalty_collection_share_civil_cases_2022.feather')

estimation_panel['state'].dtype
cases['state'].dtype

estimation_panel['state'] = estimation_panel['state'].astype('str')
cases['state'] = cases['state'].astype('str')

estimation_panel = pd.merge(estimation_panel, cases,
                            how = 'left',
                            on = 'state',
                            indicator = True)

nomerge = estimation_panel.loc[estimation_panel['_merge'] != 'both'].copy()
del nomerge

estimation_panel.drop(columns = '_merge', inplace = True)

#estimation_panel.rename(columns = {'forfeitures _and_penalties': 'forfeitures_and_penalties'}, inplace = True)

#%% 2017 penalty cases net civil forfeiture

#estimation_panel = pd.read_feather('G:/My Drive/GSEFM/Research/e_cigarettes/data/processed_data/LS_Otter/estimation_panels/r_insp_issue_2a_b.feather')

cols = list(estimation_panel)

#estimation_panel = estimation_panel.iloc[:, 0:71].copy()

penalties = pd.read_feather('G:/My Drive/GSEFM/Research/e_cigarettes/data/processed_data/penalty_collection_by_district_2017.feather')

estimation_panel['state'].dtype

#penalties.rename(columns = {'district': 'state'}, inplace = True)

estimation_panel['state'] = estimation_panel['state'].astype('str')
penalties['state'] = penalties['state'].astype('str')

estimation_panel = pd.merge(estimation_panel, penalties,
                            how = 'left',
                            on = 'state',
                            indicator = True)


nomerge = estimation_panel.loc[estimation_panel['_merge'] != 'both'].copy()
del nomerge

estimation_panel.drop(columns = '_merge', inplace = True)

#%% index without authorized manufacturers

#estimation_panel = pd.read_feather('G:/My Drive/GSEFM/Research/e_cigarettes/data/processed_data/LS_Otter/estimation_panels/r_insp_issue_2a_b.feather')

indexes = pd.read_feather('D:/convenience_store/data/processed/LS_Otter/indexes/vape_qty_index_1b_no_authorized.feather')

estimation_panel.dtypes
indexes.dtypes

indexes['store_id'] = indexes['store_id'].astype('int')
#estimation_panel['date'] = estimation_panel['date'].astype('str')
#estimation_panel['date'] = pd.to_datetime(estimation_panel['date']).dt.to_period('M')

estimation_panel['date'].dtype

estimation_panel = pd.merge(estimation_panel, indexes,
                            how = 'left',
                            on = ['store_id', 'date'],
                            indicator = True)

nomerge = estimation_panel.loc[estimation_panel['_merge'] != 'both'].copy()
del nomerge

estimation_panel.drop(columns = '_merge', inplace = True)

'''
State rankings for enforcement actions
'''

#%% 1. violations per 100 insp

#estimation_panel = pd.read_feather('G:/My Drive/GSEFM/Research/e_cigarettes/data/processed_data/LS_Otter/estimation_panels/r_insp_issue_2a_b.feather')

state_violations = estimation_panel[['state', 'viol_per_100_insp']].drop_duplicates()

state_violations.dropna(inplace = True)

# Step 2: Rank the states (higher violations = higher rank, so use descending order)
state_violations['state_viol_rank'] = state_violations['viol_per_100_insp'].rank(method='dense', ascending=False).astype(int)

# Step 3: Merge back into the original DataFrame
estimation_panel = estimation_panel.merge(state_violations[['state', 'state_viol_rank']], on='state', how='left')

#%% 2. inspection intensity

state_insp = estimation_panel[['state', 'insp_per_100k']].drop_duplicates()

state_insp.dropna(inplace = True)

# Step 2: Rank the states (higher violations = higher rank, so use descending order)
state_insp['state_insp_rank'] = state_insp['insp_per_100k'].rank(method='dense', ascending=False).astype(int)

# Step 3: Merge back into the original DataFrame
estimation_panel = estimation_panel.merge(state_insp[['state', 'state_insp_rank']], on='state', how='left')

#%% 3. follow-up inspection intensity

estimation_panel.rename(columns = {'pct_viols_w_follow_up_insp_in_12_months': 'pct_viol_flwup_insp_12_mo'}, inplace = True)

state_flwup = estimation_panel[['state', 'pct_viol_flwup_insp_12_mo']].drop_duplicates()

state_flwup.dropna(inplace = True)

# Step 2: Rank the states (higher violations = higher rank, so use descending order)
state_flwup['state_flwup_rank'] = state_flwup['pct_viol_flwup_insp_12_mo'].rank(method='dense', ascending=False).astype(int)

# Step 3: Merge back into the original DataFrame
estimation_panel = estimation_panel.merge(state_flwup[['state', 'state_flwup_rank']], on='state', how='left')

#%% cmp collected vs issued

state_cmp_ratio = estimation_panel[['state', 'avg_cmp_collected_vs_issued']].drop_duplicates()

state_cmp_ratio.dropna(inplace = True)

# Step 2: Rank the states (higher violations = higher rank, so use descending order)
state_cmp_ratio['state_cmp_ratio_rank'] = state_cmp_ratio['avg_cmp_collected_vs_issued'].rank(method='dense', ascending=False).astype(int)

# Step 3: Merge back into the original DataFrame
estimation_panel = estimation_panel.merge(state_cmp_ratio[['state', 'state_cmp_ratio_rank']], on='state', how='left')

#%% penalty filings in US court

state_filings = estimation_panel[['state', 'penalty_share_us_civil_cases_17']].drop_duplicates()

state_filings.dropna(inplace = True)

# Step 2: Rank the states (higher violations = higher rank, so use descending order)
state_filings['state_filing_rank'] = state_filings['penalty_share_us_civil_cases_17'].rank(method='dense', ascending=False).astype(int)

# Step 3: Merge back into the original DataFrame
estimation_panel = estimation_panel.merge(state_filings[['state', 'state_filing_rank']], on='state', how='left')

'''
Add control variables
'''
#%% control variables
#estimation_panel = pd.read_feather('G:/My Drive/GSEFM/Research/e_cigarettes/data/processed_data/LS_Otter/estimation_panels/r_insp_issue_2a_b.feather')

cols = list(estimation_panel)

estimation_panel['date'].dtype

# quarter-year column
estimation_panel['qy'] = estimation_panel['date'].astype('str')
estimation_panel['qy'] = pd.to_datetime(estimation_panel['qy']).dt.to_period('Q')

# unemployment rates
zip_unemp = pd.read_feather('G:/My Drive/GSEFM/Research/e_cigarettes/data/processed_data/laus/zip_unemp_rates.feather')

estimation_panel['zip_code'] = estimation_panel['zip_code'].astype('str')
zip_unemp['zip_code'] = zip_unemp['zip_code'].astype('str')

zip_unemp['date'] = pd.to_datetime(zip_unemp['date'].astype(str)).dt.to_period('M')

zip_unemp.rename(columns = {'d_weighted_unemp_rate': 'd_unemp_rate'}, inplace = True)

# merge
merged = pd.merge(estimation_panel, zip_unemp,
                  on = ['zip_code', 'date'],
                  how = 'left',
                  indicator = True
                  )

nomerge = merged.loc[merged['_merge'] != 'both'].copy() # all 2025 cause data not yet available
del nomerge
merged.drop(columns = '_merge', inplace = True)

estimation_panel = merged.copy()

# average wage
zip_wage = pd.read_feather('G:/My Drive/GSEFM/Research/e_cigarettes/data/processed_data/qcew/zip_avg_wage.feather')

head = zip_wage.head()

zip_wage.dtypes
zip_wage['zip_code'] = zip_wage['zip_code'].astype('str')

estimation_panel['qy'].dtype

# drop unnecessary columns
zip_wage = zip_wage[['zip_code', 'qy', 'l_avg_wage_index']].copy()

# merge
merged = pd.merge(estimation_panel, zip_wage,
                  how = 'left',
                  on = ['zip_code', 'qy'],
                  indicator = True
                  )

nomerge = merged.loc[merged['_merge'] != 'both'].copy() # all 2025 cause data not yet available
del nomerge
merged.drop(columns = '_merge', inplace = True)

estimation_panel = merged.copy()

# home price index
hpi = pd.read_feather('G:/My Drive/GSEFM/Research/e_cigarettes/data/processed_data/fhfa/home_price_index.feather')

estimation_panel['three_zip'] = estimation_panel['zip_code'].str[:3].astype('str')

head = estimation_panel.head()

hpi['three_zip'] = hpi['three_zip'].astype('str')
hpi['qy'].dtype

hpi = hpi[['three_zip', 'qy', 'l_home_price_index']].copy()

# merge
merged = pd.merge(estimation_panel, hpi,
                  on = ['three_zip', 'qy'],
                  how = 'left',
                  indicator = True
                  )

nomerge = merged.loc[merged['_merge'] != 'both'].copy() # all 2025 cause data not yet available
del nomerge
merged.drop(columns = '_merge', inplace = True)

estimation_panel = merged.copy()

#%% indexes with fiscal year weights

#estimation_panel = pd.read_feather('G:/My Drive/GSEFM/Research/e_cigarettes/data/processed_data/LS_Otter/estimation_panels/r_insp_issue_2a_b.feather')

indexes = pd.read_feather('D:/convenience_store/data/processed/LS_Otter/indexes/vape_qty_index_1b.feather')

indexes.dtypes
estimation_panel.dtypes

indexes['store_id'] = indexes['store_id'].astype('int')

#indexes = indexes[['store_id', 'date', 'l_vaping_products']].copy()

#indexes.rename(columns = {'l_vaping_products': 'l_vaping_products_fisc'}, inplace = True)

estimation_panel = pd.merge(estimation_panel, indexes,
                            how = 'left',
                            on = ['store_id', 'date'],
                            indicator = True)


nomerge = estimation_panel.loc[estimation_panel['_merge'] != 'both'].copy() # all 2025 cause data not yet available
del nomerge
estimation_panel.drop(columns = '_merge', inplace = True)

#%% two stage index but with brand as first stage aggregation instead of product type
indexes = pd.read_feather('D:/convenience_store/data/processed/LS_Otter/indexes/vape_qty_index_1d.feather')

indexes.dtypes
estimation_panel.dtypes

indexes['store_id'] = indexes['store_id'].astype('int')

#indexes = indexes[['store_id', 'date', 'l_vaping_products']].copy()

#indexes.rename(columns = {'l_vaping_products': 'l_vaping_products_fisc'}, inplace = True)

estimation_panel = pd.merge(estimation_panel, indexes,
                            how = 'left',
                            on = ['store_id', 'date'],
                            indicator = True)


nomerge = estimation_panel.loc[estimation_panel['_merge'] != 'both'].copy() # all 2025 cause data not yet available
del nomerge
estimation_panel.drop(columns = '_merge', inplace = True)

#%% #%% single-stage vape qty index

indexes = pd.read_feather('D:/convenience_store/data/processed/LS_Otter/indexes/vape_qty_index_1c.feather')

indexes.dtypes
estimation_panel.dtypes

indexes['store_id'] = indexes['store_id'].astype('int')
estimation_panel['store_id'] = estimation_panel['store_id'].astype('int')

#indexes = indexes[['store_id', 'date', 'l_vaping_products']].copy()

#indexes.rename(columns = {'l_vaping_products': 'l_vaping_products_fisc'}, inplace = True)

estimation_panel = pd.merge(estimation_panel, indexes,
                            how = 'left',
                            on = ['store_id', 'date'],
                            indicator = True)


nomerge = estimation_panel.loc[estimation_panel['_merge'] != 'both'].copy() # all 2025 cause data not yet available
del nomerge
estimation_panel.drop(columns = '_merge', inplace = True)

#%% price indexes with fiscal weights

#estimation_panel = pd.read_feather('G:/My Drive/GSEFM/Research/e_cigarettes/data/processed_data/LS_Otter/estimation_panels/r_insp_issue_2a_b.feather')

indexes = pd.read_feather('D:/convenience_store/data/processed/LS_Otter/indexes/vape_price_index_1b.feather')

estimation_panel[['store_id', 'date']].dtypes
indexes[['store_id', 'date']].dtypes

indexes['store_id'] = indexes['store_id'].astype('int')

cols = list(indexes)

#indexes.rename(columns = {'l_price_vaping_products': 'l_price_vaping_products_fisc'}, inplace = True)

#indexes = indexes[['store_id', 'date', 'l_price_vaping_products_fisc']].copy()

estimation_panel = pd.merge(estimation_panel, indexes,
                            how = 'left',
                            on = ['store_id', 'date'],
                            indicator = True)

nomerge = estimation_panel.loc[estimation_panel['_merge'] != 'both'].copy() # all 2025 cause data not yet available
del nomerge
estimation_panel.drop(columns = '_merge', inplace = True)

cols = list(estimation_panel)

#%% vape transaction count index

#estimation_panel = pd.read_feather('G:/My Drive/GSEFM/Research/e_cigarettes/data/processed_data/LS_Otter/estimation_panels/r_insp_issue_2a_b.feather')

trans_count_index = pd.read_feather('D:/convenience_store/data/processed/LS_Otter/indexes/vape_trans_count_index_1a.feather')

estimation_panel[['store_id', 'date']].dtypes
trans_count_index[['store_id', 'date']].dtypes

#estimation_panel['date'] = pd.to_datetime(estimation_panel['date'].astype('str')).dt.to_period('M')
trans_count_index['store_id'] = trans_count_index['store_id'].astype('int')

cols = list(trans_count_index)

trans_count_index = trans_count_index[['store_id', 'date', 'trans_count', 'l_trans_count_index']].copy()

# merge
estimation_panel = pd.merge(estimation_panel, trans_count_index,
                            how = 'left',
                            on = ['store_id', 'date'],
                            indicator = True)

nomerge = estimation_panel.loc[estimation_panel['_merge'] != 'both'].copy() # all 2025 cause data not yet available
del nomerge
estimation_panel.drop(columns = '_merge', inplace = True)

#%% vape qty count index
#estimation_panel = pd.read_feather('G:/My Drive/GSEFM/Research/e_cigarettes/data/processed_data/LS_Otter/estimation_panels/r_insp_issue_2a_b.feather')

qty_count_index = pd.read_feather('D:/convenience_store/data/processed/LS_Otter/indexes/vape_qty_count_index_1a.feather')

estimation_panel[['store_id', 'date']].dtypes
qty_count_index[['store_id', 'date']].dtypes

qty_count_index['store_id'] = qty_count_index['store_id'].astype('int')

qty_count_index = qty_count_index[['store_id', 'date', 'qty_count_index', 'l_qty_count_index']].copy()

# merge
estimation_panel = pd.merge(estimation_panel, qty_count_index,
                            how = 'left',
                            on = ['store_id', 'date'],
                            indicator = True)

nomerge = estimation_panel.loc[estimation_panel['_merge'] != 'both'].copy() # all 2025 cause data not yet available
del nomerge
estimation_panel.drop(columns = '_merge', inplace = True)

del qty_count_index

#%% vape rev index
#estimation_panel = pd.read_feather('G:/My Drive/GSEFM/Research/e_cigarettes/data/processed_data/LS_Otter/estimation_panels/r_insp_issue_2a_b.feather')

vape_rev_index = pd.read_feather('D:/convenience_store/data/processed/LS_Otter/indexes/vape_rev_index_1a.feather')

estimation_panel[['store_id', 'date']].dtypes
vape_rev_index[['store_id', 'date']].dtypes

vape_rev_index['store_id'] = vape_rev_index['store_id'].astype('int')

vape_rev_index.rename(columns = {'rev_index': 'vape_rev_index',
                                 'l_rev_index': 'l_vape_rev_index'}, inplace = True)

vape_rev_index = vape_rev_index[['store_id', 'date', 'vape_rev_index', 'l_vape_rev_index']].copy()

# merge
estimation_panel = pd.merge(estimation_panel, vape_rev_index,
                            how = 'left',
                            on = ['store_id', 'date'],
                            indicator = True)

nomerge = estimation_panel.loc[estimation_panel['_merge'] != 'both'].copy() # all 2025 cause data not yet available
del nomerge
estimation_panel.drop(columns = '_merge', inplace = True)

del vape_rev_index

#%% vape qty per transaction index
#estimation_panel = pd.read_feather('G:/My Drive/GSEFM/Research/e_cigarettes/data/processed_data/LS_Otter/estimation_panels/r_insp_issue_2a_b.feather')

qty_per_trans_index = pd.read_feather('D:/convenience_store/data/processed/LS_Otter/indexes/vape_qty_per_trans_index_1a.feather')

estimation_panel[['store_id', 'date']].dtypes
qty_per_trans_index[['store_id', 'date']].dtypes

qty_per_trans_index['store_id'] = qty_per_trans_index['store_id'].astype('int')

qty_per_trans_index.rename(columns = {'qty_per_trans': 'vape_qty_per_trans',
                                      'l_qty_per_trans_index': 'l_vape_qty_per_trans_index'}, inplace = True)


qty_per_trans_index = qty_per_trans_index[['store_id', 'date', 'vape_qty_per_trans', 'l_vape_qty_per_trans_index']].copy()

cols = list(qty_per_trans_index)

# merge
estimation_panel = pd.merge(estimation_panel, qty_per_trans_index,
                            how = 'left',
                            on = ['store_id', 'date'],
                            indicator = True)

nomerge = estimation_panel.loc[estimation_panel['_merge'] != 'both'].copy() # all 2025 cause data not yet available
del nomerge
estimation_panel.drop(columns = '_merge', inplace = True)

del qty_per_trans_index

#%% placebo treatment

#r_insp_df = estimation_panel.loc[estimation_panel['r_insp'] == 1].drop_duplicates(subset = ['store_id', 'date'])

#%% t+8 effect size

#estimation_panel = pd.read_feather('G:/My Drive/GSEFM/Research/e_cigarettes/data/processed_data/LS_Otter/estimation_panels/r_insp_issue_2a_b.feather')

effect_size = pd.read_feather('G:/My Drive/GSEFM/Research/e_cigarettes/output/r_insp_2a_b_ols_vape_qty_1b_heter_by_zip_t_6_cluster_z.feather')

effect_size['zip_code'].dtype
estimation_panel['zip_code'].dtype

effect_size['zip_code'] = effect_size['zip_code'].astype('str')
estimation_panel['zip_code'] = estimation_panel['zip_code'].astype('str')

# merge
estimation_panel = pd.merge(estimation_panel, effect_size,
                            how = 'left',
                            on = 'zip_code',
                            indicator = True)

nomerge = estimation_panel.loc[estimation_panel['_merge'] == 'both'].copy() # all 2025 cause data not yet available
del nomerge
estimation_panel.drop(columns = '_merge', inplace = True)

#%% export feather
outpath = 'G:/My Drive/GSEFM/Research/e_cigarettes/data/processed_data/LS_Otter/estimation_panels/'
#outpath = os.path.join(temp_destination_dir, filename)
pa.feather.write_feather(estimation_panel, outpath + 'r_insp_issue_2a_b.feather')


#%% export stata
#estimation_panel.drop(columns = 'qy', inplace = True)
estimation_panel.dtypes
# convert date column to datetime (needed for exporting to stata) ------------------------
estimation_panel['date'] = estimation_panel['date'].astype('str') # needed to convert to date below
estimation_panel['date'] = pd.to_datetime(estimation_panel['date'])
estimation_panel['insp_date'] = estimation_panel['insp_date'].astype('str') # needed to convert to date below
estimation_panel['insp_date'] = pd.to_datetime(estimation_panel['insp_date'])
estimation_panel['issue_date'] = estimation_panel['issue_date'].astype('str') # needed to convert to date below
estimation_panel['issue_date'] = pd.to_datetime(estimation_panel['issue_date'])

estimation_panel.rename(columns = {'forfeitures _and_penalties': 'forfeitures_and_penalties'}, inplace = True)
estimation_panel.drop(columns = 'qy', inplace = True)

# rename long names
estimation_panel.rename(columns = {'pct_viols_w_follow_up_insp_in_12_months': 'pct_viol_flwup_insp_12_mo'}, inplace = True)

estimation_panel.replace([np.inf, -np.inf], np.nan, inplace=True)

estimation_panel.to_stata(outpath + 'r_insp_issue_2a_b.dta', convert_dates = {'date': 'tm',
                                                                              'insp_date': 'tm',
                                                                              'issue_date': 'tm'})

#%% END to delete below










#%% shift treatment date 12 months forward
robbed_new = robbed[['retailer_license', 'treatment', 'date']].copy()
robbed_new.dtypes

robbed_new['date'] = robbed_new['date'].astype('str')
robbed_new['date'] = pd.to_datetime(robbed_new['date'])
# Create a new column 'date_new' by shifting 'date' column 12 months forward
robbed_new['date_new'] = robbed_new['date'] - pd.DateOffset(months=12)
# convert to year-month
robbed_new['date_new'] = pd.to_datetime(robbed_new['date_new']).dt.to_period('M')

# drop old date column
robbed_new.drop(columns = 'date', inplace = True)
# rename date column
robbed_new.rename(columns = {'date_new': 'date'}, inplace = True)

# subset for sample period
robbed_new = robbed_new.loc[(robbed_new['date'] >= '2018-03') & (robbed_new['date'] <= '2021-12')].copy()

unique_robbed = list(robbed_new['retailer_license'].unique()) # 58 rows

# replace treatment column in events
events.drop(columns = 'treatment', inplace = True)

events.dtypes
events['date'] = pd.to_datetime(events['date']).dt.to_period('M')

# merge new treatment column
events = pd.merge(events, robbed_new,
                      how = 'left',
                      on = ['retailer_license', 'date'],
                      indicator = True)

foo = events['treatment'].unique()
del foo

events['treatment'] = np.where(events['treatment'].isna(), 0, events['treatment'])

events.drop(columns = '_merge', inplace = True)

treats = events.loc[events['treatment'] == 1].copy()













desc = estimation_panel['stores_targeted_in_state'].value_counts().reset_index()
# descriptive stats - delete

outpath = 'G:/My Drive/GSEFM/Research/e_cigarettes/data/processed_data/estimation_panels/'

#outpath = os.path.join(temp_destination_dir, filename)
estimation_panel = pd.read_feather(outpath + 'r_insp_issue_2a.feather')

head = estimation_panel.head()
cols = list(estimation_panel)

treated = estimation_panel.loc[estimation_panel['month_insp'] == estimation_panel['date']].copy()

treated_chain_size = treated.groupby("chain_size_2", as_index=False)["store_id"].nunique()

chain_113 = treated.loc[treated['chain_size_2'] >= 50]['store_chain_id'].unique().tolist()
chain_113 = treated.loc[treated['chain_size_2'] >= 50]['store_id'].unique().tolist()

chain_115 = treated.loc[treated['chain_size_2'] == 115]['store_chain_id'].unique().tolist()
chain_115 = treated.loc[treated['chain_size_2'] == 115].copy()

chain_125 = treated.loc[treated['chain_size_2'] == 125]['store_chain_id'].unique().tolist()
chain_125 = treated.loc[treated['chain_size_2'] == 125].copy()

chain_67 = treated.loc[treated['chain_size_2'] == 67]['store_chain_id'].unique().tolist()
chain_67 = treated.loc[treated['chain_size_2'] == 67].copy()


treated_stores = treated['store_id'].unique().tolist()

outpath = 'G:/My Drive/GSEFM/Research/e_cigarettes/data/processed_data/'

consolidated_stores = pd.read_csv('G:/My Drive/GSEFM/Research/e_cigarettes/data/processed_data/consolidated_stores.csv')

consolidated_stores = consolidated_stores[['store_id', 'chain_size_2']]

estimation_panel = pd.merge(estimation_panel, consolidated_stores,
                            how = 'left',
                            on = 'store_id',
                            indicator = True)

nomerge = estimation_panel.loc[estimation_panel['_merge'] == 'both'].copy()
del nomerge
estimation_panel.drop(columns = '_merge', inplace = True)

cols = list(estimation_panel)

estimation_panel['count_before'].describe()


treated = estimation_panel.loc[estimation_panel['treated'] == 1]['store_id'].unique().tolist()

rivals = (
    estimation_panel
    .loc[estimation_panel['store_id'].isin(treated)]
    .drop_duplicates(subset = 'store_id')
    )

rivals = estimation_panel.loc[estimation_panel['inspection_treatment'] == 1].copy()

prev = rivals['count_before'].value_counts() # 38


cmps = rivals['cmps_before'].value_counts() # 5




treated = estimation_panel.loc[estimation_panel['treated'] == 1].copy()

treatment_date = treated.loc[estimation_panel['store_id'] == ]




