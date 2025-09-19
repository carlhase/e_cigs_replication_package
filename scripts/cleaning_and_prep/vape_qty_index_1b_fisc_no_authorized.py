# -*- coding: utf-8 -*-
"""
Created on Wed Aug  6 17:12:22 2025

@author: cahase
"""


# -*- coding: utf-8 -*-
"""
Created on Mon Dec 16 14:50:48 2024

@author: cahase
"""

import numpy as np
import pandas as pd
import glob
import os
import gc
from timeit import default_timer as timer
import pyarrow as pa
from pyarrow import csv
#from fuzzywuzzy import fuzz

#store_path = "D:/convenience_store/data/processed/da_store_id_monthly_ag_feather/"
store_path = "D:/convenience_store/data/processed/LS_Otter/da_store_id_monthly_ag_feather/"


outpath = "D:/convenience_store/data/processed/LS_Otter/indexes/vape_qty_index_1b_no_authorized/"

# Ensure the directory exists, if not, create it
os.makedirs(outpath, exist_ok=True)

#%% list of files (stores)

all_files = glob.glob(store_path + "*.feather") # 18214 files (previously 18940 files)

# Normalize to use the correct slash for the OS
all_files = [os.path.normpath(path) for path in all_files]

# Extract store numbers
store_numbers = [os.path.basename(path).split('.')[0] for path in all_files]

#%% check for differences

#processed_files = glob.glob(outpath + "*.feather") # 
store = '28380'
subcat = 'Vaping Products'

#%% fiscal year

# list of dates for each full fiscal year (
bfy_2022 = pd.date_range(start="2022-01", end="2022-06", freq="MS").to_period('M').astype('str').tolist() # includes last few months of FY 2017
bfy_2023 = pd.date_range(start="2022-07", end="2023-06", freq="MS").to_period('M').astype('str').tolist()
bfy_2024 = pd.date_range(start="2023-07", end="2024-06", freq="MS").to_period('M').astype('str').tolist() # includes first 6 months of FY 2021
bfy_2025 = pd.date_range(start="2024-07", end="2025-06", freq="MS").to_period('M').astype('str').tolist() # includes first 6 months of FY 2021


#%%

# Initialize counter
total_iterations = len(store_numbers)

# Initialize counter
iteration = 0

# authorized list
authorized_manu = ['R.J. Reynolds Vapor Company', 'NJOY', 'NJOY LLC', 'Logic Technology Development LLC']
authorized_brands = ['NJOY', 'Logic', 'Vuse']

#%%

for store in store_numbers:
    
    input_filename = os.path.join(store_path, f"{store}.feather")

    df = pd.read_feather(input_filename)

    # make column names lowercase
    df.columns = df.columns.str.lower()
    
    # create date column with YYYY-MM format
    df['date'] = df['calendar_year'].astype('str') + '-' + df['calendar_month'].astype('str')
    # to datetime
    df['date'] = pd.to_datetime(df['date']).dt.to_period('M')
    
    # keep only gtin scan types
    df = df.loc[df['scan_type'] == 'GTIN'].copy()
    
    if df.empty:
        print(f"Skipping {store} — empty DataFrame")
        continue

    # fiscal year
    df['fiscal_year'] = np.where(df['date'].astype('str').isin(bfy_2022), 2022,
                         np.where(df['date'].astype('str').isin(bfy_2023), 2023,
                          np.where(df['date'].astype('str').isin(bfy_2024), 2024,
                           np.where(df['date'].astype('str').isin(bfy_2025), 2025, 
                            np.nan))))
    
    # subset vaping products
    subcat_df = df.loc[df['subcategory'] == 'Vaping Products'].copy()
    
    # drop authorized products
    subcat_df = subcat_df.loc[~((subcat_df['manufacturer'].isin(authorized_manu)) | (subcat_df['brand'].isin(authorized_brands)))].copy()

    if subcat_df.empty:
        print(f"Skipping {store} — empty DataFrame")
        continue

    # Sort the DataFrame by store_id, product_id, and date
    subcat_df = subcat_df.sort_values(by=['store_id', 'gtin', 'date'])
    
    # Calculate the month difference between consecutive rows within each group
    subcat_df['month_diff'] = subcat_df.groupby(['store_id', 'gtin'])['date'].diff().apply(lambda x: x.n if pd.notna(x) else np.nan)
    
    # Group by 'store_id' and 'product_id' and calculate the lagged 'unit_value' only for consecutive months
    subcat_df['lag_quantity'] = subcat_df.groupby(['store_id', 'gtin'])['quantity'].shift(1)
    
    # Set lagged values to NaN if the time difference is not exactly 1 month
    subcat_df['lag_quantity'] = np.where(subcat_df['month_diff'] == 1, subcat_df['lag_quantity'], np.nan)
    
    subcat_df = subcat_df.fillna(value=np.nan)
    
    # (sub)category annual revenue
    category_revenue = (
        subcat_df
        .groupby(['store_id', 'subcategory', 'fiscal_year'])
        .agg(category_annual_revenue = ('total_revenue_amount', 'sum'))
        .reset_index()
        )
    
    # product type annual revenue - preserve category that each gtin belongs to
    type_revenue = (
        subcat_df
        .groupby(['store_id', 'subcategory', 'product_type', 'fiscal_year'], dropna = False)
        .agg(type_annual_revenue = ('total_revenue_amount', 'sum'))
        .reset_index()
        )

    # product annual revenue - preserve category that each gtin belongs to
    product_revenue = (
        subcat_df
        .groupby(['store_id', 'subcategory', 'product_type', 'gtin', 'fiscal_year'], dropna = False)
        .agg(product_annual_revenue = ('total_revenue_amount', 'sum'))
        .reset_index()
        )

    # stage 1 weight: product share of category revenue (first-stage weight)
    stage_1_weight = pd.merge(product_revenue, type_revenue,
                              how = 'left',
                              on = ['store_id', 'subcategory', 'product_type', 'fiscal_year']
                              )

    stage_1_weight['stage_1_weight'] = stage_1_weight['product_annual_revenue']/stage_1_weight['type_annual_revenue']

    # merge stage 1 weight
    stage_1_df = pd.merge(subcat_df, stage_1_weight,
                          how = 'left',
                          on = ['store_id', 'subcategory', 'product_type', 'gtin', 'fiscal_year']
                          )

    # calculate stage 1 index
    stage_1_df['unit_qty_index'] = (stage_1_df['quantity']/stage_1_df['lag_quantity'])**stage_1_df['stage_1_weight']

    # aggregate to brand index
    type_index = (
        stage_1_df
        .groupby(['store_id', 'subcategory', 'product_type', 'date'], dropna = False)
        .agg(type_index = ('unit_qty_index', 'prod'))
        .reset_index()
        )
    
    # stage 2 weight: type share of (sub)category
    stage_2_weight = pd.merge(type_revenue, category_revenue,
                              how = 'left',
                              on = ['store_id', 'subcategory', 'fiscal_year']
                              )

    stage_2_weight['stage_2_weight'] = stage_2_weight['type_annual_revenue']/stage_2_weight['category_annual_revenue']

    # create year column in brand_index df
    fisc_year_date_df = stage_1_df[['date', 'fiscal_year']].drop_duplicates()

    # merge fisc year df to brand_index
    type_index = pd.merge(type_index, fisc_year_date_df,
                           how = 'left',
                           on = ['date']
                           )
    #brand_index['fiscal_year'] = brand_index['date'].dt.year.astype('int')
    
    # merge stage 2 weight
    stage_2_df = pd.merge(type_index, stage_2_weight,
                          how = 'left',
                          on = ['store_id', 'subcategory', 'product_type', 'fiscal_year']
                          )

    stage_2_df['weighted_type_index'] = stage_2_df['type_index']**stage_2_df['stage_2_weight']
    
    # aggregate to type index (stage 1)
    store_index = (
        stage_2_df
        .groupby(['store_id', 'subcategory', 'date'])
        .agg(vape_qty_index_1b_no_auth = ('weighted_type_index', 'prod'))
        .reset_index()
        # .rename(columns={'category_index': subcat})
        # .assign(**{f'l_{subcat}': lambda df: np.log(df[subcat])})
        .drop(columns = 'subcategory')
        )

    store_index['l_vape_qty_index_1b_no_auth'] = np.log(store_index['vape_qty_index_1b_no_auth'])

    # save each store-specific df
    output_filename = os.path.join(outpath, f"{store}.feather")
    output_filename = os.path.normpath(output_filename)
    pa.feather.write_feather(store_index, output_filename)
    
    # Increment and print the iteration counter
    iteration += 1
    print(f"Iteration {iteration}/{total_iterations}: Processed file {store}")


#% read in and append panels of quantity indexes
# list of files (stores)

source_dir = "D:/convenience_store/data/processed/LS_Otter/indexes/vape_qty_index_1b_no_authorized/"

all_files = glob.glob(source_dir + "*.feather") # 18214 files (previously 18940 files)

# Normalize to use the correct slash for the OS
all_files = [os.path.normpath(path) for path in all_files]

total_iterations = len(all_files)

# Initialize counter
iteration = 0

# read in and concatinate store quantity indexes
li = []
for file in all_files:
    df = pd.read_feather(file)
    li.append(df)
    print(f"Iteration {iteration}/{total_iterations}")
    iteration += 1

# append to create panel    
indexes = pd.concat(li, ignore_index=True)

indexes = indexes.drop_duplicates(subset = ['store_id', 'date'])
indexes['store_id'].dtype
indexes['store_id'] = indexes['store_id'].astype('str')

head = indexes.head(1000)

cols = list(indexes)

# save indexes
indexes.to_feather('D:/convenience_store/data/processed/LS_Otter/indexes/vape_qty_index_1b_no_authorized.feather')
