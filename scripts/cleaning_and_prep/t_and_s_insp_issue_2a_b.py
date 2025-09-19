# -*- coding: utf-8 -*-
"""
Created on Wed Feb 26 13:04:12 2025

@author: cahase
"""

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

#%% read in treatment info

treatments = pd.read_feather('G:/My Drive/GSEFM/Research/e_cigarettes/data/processed_data/LS_Otter/targeted_stores_treatment.feather')

treatments.dtypes

cols_to_keep = ['store_id', 'insp_date', 'issue_date']

treatments = treatments[cols_to_keep].copy()

treatments['insp_date'] = pd.to_datetime(treatments['insp_date']).dt.to_period('M')
treatments['issue_date'] = pd.to_datetime(treatments['issue_date']).dt.to_period('M')

treatments.dtypes
panel_data.dtypes

treatments['store_id'] = treatments['store_id'].astype('str')
panel_data['store_id'] = panel_data['store_id'].astype('str')

treatments['store_id'].nunique()

# targeted store info
targeted_stores = treatments['store_id'].tolist()

targeted_store_info = store_info.loc[store_info['store_id'].isin(targeted_stores)].copy()

targeted_store_info['store_id'] = targeted_store_info['store_id'].astype('str')

# merge to treatment dates
treatments = pd.merge(treatments, targeted_store_info,
                      how = 'left',
                      on = 'store_id')

#%% dictionaries for treatment assignment

treatments.dtypes

treatments['store_id'] = treatments['store_id'].astype('str')
treatments['store_chain_id'] = treatments['store_chain_id'].astype('str')

# dictionary storing store_ids for each insp and issue date
insp_dict = treatments.groupby('insp_date')['store_id'].apply(list).to_dict() 
issue_dict = treatments.groupby('issue_date')['store_id'].apply(list).to_dict() 

# dictionary storing store_chain_ids for each insp and issue date
chain_insp_dict = treatments.groupby('insp_date')['store_chain_id'].apply(list).to_dict() 
chain_issue_dict = treatments.groupby('issue_date')['store_chain_id'].apply(list).to_dict() 

#%% assign treatment indicator to targeted stores

# Function to check if a row's store_id appears in the insp_dict for that date
def was_inspected(row):
    return int(row['store_id'] in insp_dict.get(row['date'], []))

def was_issued(row):
    return int(row['store_id'] in issue_dict.get(row['date'], []))

panel_data.drop(columns = ['insp', 'issue'], inplace = True)

panel_data_test = panel_data.copy()

# Apply the function row-wise to assign inspection treatment
panel_data['t_insp'] = panel_data.apply(was_inspected, axis=1)

# Apply the function row-wise to assign issue treatment
panel_data['t_issue'] = panel_data.apply(was_issued, axis=1)

panel_data['issue'].unique()


#%% assign treatment indicator to sister stores

# fuction for identifying sister store-insp date combos that are not the targeted store itself
def s_was_inspected(row):    
    store = row['store_id']
    date = row['date']
    
    in_chain = store in chain_insp_dict.get(date, []) # store is in the same chain - date
    not_targeted = store not in insp_dict.get(date, []) # but is not the targeted store
    
    return int(in_chain and not_targeted)

# fuction for identifying sister store-issue date combos that are not the targeted store itself
def s_was_issued(row):    
    store = row['store_id']
    date = row['date']
    
    in_chain = store in chain_issue_dict.get(date, []) # store is in the same chain - date
    not_targeted = store not in insp_dict.get(date, []) # but is not the targeted store
    
    return int(in_chain and not_targeted)

# Apply the function row-wise to assign sister store inspection treatment
panel_data['s_insp'] = panel_data.apply(s_was_inspected, axis=1)
panel_data['s_issue'] = panel_data.apply(s_was_issued, axis=1)

# list of sister store chains
targeted_chain_list = treatments['store_chain_id'].unique().tolist()

targeted_chain_stores = store_info.loc[store_info['store_chain_id'].astype('str').isin(targeted_chain_list)]['store_id'].unique().tolist()

sister_stores = list(set(targeted_chain_stores) - set(targeted_stores))

# add generic targeted and sister store indicators
panel_data['targeted'] = np.where(panel_data['store_id'].isin(targeted_stores), 1, 0)
panel_data['sister'] = np.where(panel_data['store_id'].isin(sister_stores), 1, 0)


foo = panel_data.loc[panel_data['targeted'] == 1].copy()
foo1 = panel_data.loc[panel_data['sister'] == 1].copy()
del foo, foo1


#%% merge quantity indexes to store-month panel, drop store-months with no index

qty_indexes = pd.read_feather('D:/convenience_store/data/processed/LS_Otter/indexes/subcat_qty_otp_1b.feather')

head = qty_indexes.head()

panel_data.dtypes
qty_indexes.dtypes

# important step: convert to str type otherwise many wont merge (despite already being object type)
qty_indexes['store_id'] = qty_indexes['store_id'].astype('str')
panel_data['store_id'] = panel_data['store_id'].astype('str')

estimation_panel = pd.merge(panel_data, qty_indexes,
                            how = 'left',
                            on = ['store_id', 'date'],
                            indicator = True)

both = estimation_panel.loc[estimation_panel['_merge'] == 'both'].copy() #285160  
left_only = estimation_panel.loc[estimation_panel['_merge'] == 'left_only'].copy() # 209546
right_only = estimation_panel.loc[estimation_panel['_merge'] == 'right_only'].copy() # 0 (as it should be)
del right_only

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

treated = estimation_panel.loc[estimation_panel['targeted'] == 1]['store_id'].unique() # 95

#%% census divisions
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

#%% drop rival stores from sample - except for sister stores

all_treatments = pd.read_feather('G:/My Drive/GSEFM/Research/e_cigarettes/warning_letters/warning_letters_combined.feather')

all_treatments['zip_code'] = all_treatments['zip_code'].str.replace('\"', '')

# zip codes with a letter
treated_zip_codes = all_treatments['zip_code'].unique().tolist()

# zip codes in panel
estimation_panel['zip_code'].nunique()

all_stores = estimation_panel['store_id'].unique().tolist()

non_treated_stores = list(set(all_stores) - set(targeted_stores) - set(sister_stores)) # non-treated stores

# non-treated stores in same zip code as a targeted store
condition = (estimation_panel['zip_code'].isin(treated_zip_codes)) & (estimation_panel['store_id'].isin(non_treated_stores))

# backup
backup = estimation_panel.copy()

# drop non-targeted stores in treated zip codes
estimation_panel = estimation_panel.loc[~condition].copy()

test = estimation_panel.loc[estimation_panel['targeted'] == 1].copy()
test = estimation_panel.loc[estimation_panel['targeted'] == 1]['store_id'].unique().tolist()
del test, backup

head = estimation_panel.head()

estimation_panel.rename(columns = {'insp_date': 'month_insp',
                                   'issue_date': 'month_issued'}, inplace = True)

#%% control group indicator

cols = list(estimation_panel)

estimation_panel['control'] = np.where(estimation_panel['store_id'].isin(non_treated_stores), 1, 0)


# count rival/treated stores
estimation_panel.loc[estimation_panel['targeted'] == 1]['store_id'].nunique()# 88
estimation_panel.loc[estimation_panel['sister'] == 1]['store_id'].nunique()# 88
estimation_panel.loc[estimation_panel['control'] == 1]['store_id'].nunique()# 14,916

#%% does zip overlap with census urban area?

#estimation_panel = pd.read_feather('G:/My Drive/GSEFM/Research/e_cigarettes/data/processed_data/LS_Otter/estimation_panels/t_and_s_insp_issue_2a_b.feather')

cols = list(estimation_panel)

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

#%% add multi-state chain indicator

#estimation_panel = pd.read_feather('G:/My Drive/GSEFM/Research/e_cigarettes/data/processed_data/LS_Otter/estimation_panels/t_and_s_insp_issue_2a_b.feather')
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
estimation_panel.loc[(estimation_panel['targeted'] == 1) & (estimation_panel['multi_state_chain'] == 1)]['store_id'].nunique()
estimation_panel.loc[(estimation_panel['targeted'] == 1) & (estimation_panel['multi_state_chain'] != 1)]['store_id'].nunique()

#%% merge subcat price indexes otp

#estimation_panel = pd.read_feather('G:/My Drive/GSEFM/Research/e_cigarettes/data/processed_data/LS_Otter/estimation_panels/t_and_s_insp_issue_2a_b.feather')

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

#%% add indicator for being in a state that recieved at least one warning letter

#estimation_panel = pd.read_feather('G:/My Drive/GSEFM/Research/e_cigarettes/data/processed_data/LS_Otter/estimation_panels/t_and_s_insp_issue_2a_b.feather')

all_treatments = pd.read_feather('G:/My Drive/GSEFM/Research/e_cigarettes/warning_letters/warning_letters_combined.feather')

all_treatments['state'] = all_treatments['state'].astype('str').str.strip()
estimation_panel['state'] = estimation_panel['state'].astype('str').str.strip()

states = all_treatments['state'].unique().tolist()

estimation_panel['warned_state'] = np.where(estimation_panel['state'].isin(states), 1, 0)

estimation_panel['warned_state'].describe()

#%% state inspection rates

state_insp = pd.read_csv('G:/My Drive/GSEFM/Research/e_cigarettes/data/processed_data/state_insp_and_viol_rates.csv')

estimation_panel['state'].dtype
state_insp['state'].dtype

estimation_panel['state'] = estimation_panel['state'].astype('str')
state_insp['state'] = state_insp['state'].astype('str')

estimation_panel2 = pd.merge(estimation_panel, state_insp,
                             how = 'left',
                             on = 'state',
                             indicator = True)

nomerge = estimation_panel2.loc[estimation_panel2['_merge'] != 'both'].copy()
del nomerge
estimation_panel2.drop(columns = '_merge', inplace = True)

estimation_panel = estimation_panel2.copy()

del estimation_panel2

cols = list(estimation_panel)

#%% cmp collected vs issued, 2010-2019

#estimation_panel = pd.read_feather('G:/My Drive/GSEFM/Research/e_cigarettes/data/processed_data/LS_Otter/estimation_panels/t_and_s_insp_issue_2a_b.feather')

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

#estimation_panel = pd.read_feather('G:/My Drive/GSEFM/Research/e_cigarettes/data/processed_data/LS_Otter/estimation_panels/t_and_s_insp_issue_2a_b.feather')

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

# estimation_panel = pd.read_feather('G:/My Drive/GSEFM/Research/e_cigarettes/data/processed_data/LS_Otter/estimation_panels/t_and_s_insp_issue_2a_b.feather')

# cases = pd.read_feather('G:/My Drive/GSEFM/Research/e_cigarettes/misc/penalty_collection_share_civil_cases_2022.feather')

# cases = cases.iloc[1:117,:].copy()
# estimation_panel['state'].dtype
# cases['state'].dtype

# estimation_panel['state'] = estimation_panel['state'].astype('str')
# cases['state'] = cases['state'].astype('str')

# estimation_panel = pd.merge(estimation_panel, cases,
#                             how = 'left',
#                             on = 'state',
#                             indicator = True)

# nomerge = estimation_panel.loc[estimation_panel['_merge'] != 'both'].copy()
# del nomerge
# estimation_panel.drop(columns = '_merge', inplace = True)

#estimation_panel.rename(columns = {'forfeitures _and_penalties': 'forfeitures_and_penalties'}, inplace = True)

#%% 2017 penalty cases net civil forfeiture

#estimation_panel = pd.read_feather('G:/My Drive/GSEFM/Research/e_cigarettes/data/processed_data/LS_Otter/estimation_panels/t_and_s_insp_issue_2a_b.feather')

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

'''
State rankings for enforcement actions
'''

#%% 1. violations per 100 insp

#estimation_panel = pd.read_feather('G:/My Drive/GSEFM/Research/e_cigarettes/data/processed_data/LS_Otter/estimation_panels/t_and_s_insp_issue_2a_b.feather')

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

#%% add control variables
#estimation_panel = pd.read_feather('G:/My Drive/GSEFM/Research/e_cigarettes/data/processed_data/LS_Otter/estimation_panels/t_and_s_insp_issue_2a_b.feather')

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

estimation_panel.drop(columns = 'qy', inplace = True)

#%% indexes with fiscal year weights

#estimation_panel = pd.read_feather('G:/My Drive/GSEFM/Research/e_cigarettes/data/processed_data/LS_Otter/estimation_panels/t_and_s_insp_issue_2a_b.feather')

indexes = pd.read_feather('D:/convenience_store/data/processed/LS_Otter/indexes/vape_qty_index_1b.feather')

indexes.dtypes
estimation_panel.dtypes

indexes['store_id'] = indexes['store_id'].astype('int')

#indexes = indexes[['store_id', 'date', 'l_vaping_products']].copy()

#indexes.rename(columns = {'l_vaping_products': 'l_vaping_products_fisc'}, inplace = True)

#estimation_panel['date'] = pd.to_datetime(estimation_panel['date'].astype('str')).dt.to_period('M')

estimation_panel = pd.merge(estimation_panel, indexes,
                            how = 'left',
                            on = ['store_id', 'date'],
                            indicator = True)


nomerge = estimation_panel.loc[estimation_panel['_merge'] != 'both'].copy() # all 2025 cause data not yet available
del nomerge
estimation_panel.drop(columns = '_merge', inplace = True)

#%% vape transaction count index

#estimation_panel = pd.read_feather('G:/My Drive/GSEFM/Research/e_cigarettes/data/processed_data/LS_Otter/estimation_panels/t_and_s_insp_issue_2a_b.feather')

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

#%% export feather

estimation_panel['store_id'] = estimation_panel['store_id'].astype(int)


outpath = 'G:/My Drive/GSEFM/Research/e_cigarettes/data/processed_data/LS_Otter/estimation_panels/'

# Ensure the destination directory exists
os.makedirs(outpath, exist_ok=True)

estimation_panel.replace([np.inf, -np.inf], np.nan, inplace=True)

#outpath = os.path.join(temp_destination_dir, filename)
pa.feather.write_feather(estimation_panel, outpath + 't_and_s_insp_issue_2a_b.feather')

#%% export stata

estimation_panel.dtypes

estimation_panel['date'] = estimation_panel['date'].astype('str') # needed to convert to date below
estimation_panel['date'] = pd.to_datetime(estimation_panel['date'])

cols = list(estimation_panel)

# rename long names
estimation_panel.rename(columns = {'pct_viols_w_follow_up_insp_in_12_months': 'pct_viol_flwup_insp_12_mo'}, inplace = True)

estimation_panel.to_stata(outpath + 't_and_s_insp_issue_2a_b.dta', convert_dates = {'date': 'tm'})
    
#%%
















