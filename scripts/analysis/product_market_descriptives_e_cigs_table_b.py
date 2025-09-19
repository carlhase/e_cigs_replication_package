# -*- coding: utf-8 -*-
"""
Created on Wed Mar 12 16:30:50 2025

@author: cahase
"""

import pandas as pd
import numpy as np
import os
import glob
from functools import reduce

'''
Panel b. Tobacco product categories
'''

# root directory for replication - user-specific
#root = 'D:/convenience_store/data/processed/LS_Otter/da_store_id_monthly_ag_feather'

# #%% to_delete
#gtin = pd.read_csv("D:/convenience_store/data/raw/LS_Otter/MASTER_GTIN NEW/MASTER_GTIN_NEW-0.csv")

# gtin.columns = gtin.columns.str.lower()

# categories = gtin['category'].unique()

# cigs = gtin.loc[gtin['category'] == 'Cigarettes'].copy()

# otp = gtin.loc[gtin['category'] == 'Other Tobacco Products']['subcategory'].unique()

# smokeless = gtin.loc[gtin['subcategory'] == 'Smokeless'].copy()
# smokeless_alts = gtin.loc[gtin['subcategory'] == 'Smokeless Tobacco Alternatives'].copy()

#%% read in and store monthly aggregates for pre-treatment period at targeted stores

store_path = "D:/convenience_store/data/processed/LS_Otter/da_store_id_monthly_ag_feather/"

all_files = glob.glob(store_path + "*.feather") # 18214 files (previously 18940 files)

# Normalize to use the correct slash for the OS
all_files = [os.path.normpath(path) for path in all_files]

# Extract store numbers
store_numbers = [os.path.basename(path).split('.')[0] for path in all_files]

total_iterations = len(store_numbers)

# test
store = '24361'

#%% loop
# Initialize counter
iteration = 0

li = []
for store in store_numbers:
 
    input_filename = os.path.join(store_path, f"{store}.feather")

    df = pd.read_feather(input_filename)

    # make column names lowercase
    df.columns = df.columns.str.lower()
    
    # create date column with YYYY-MM format
    df['date'] = df['calendar_year'].astype('str') + '-' + df['calendar_month'].astype('str')
    # pre-process if date is inconsistently formatted
    df['date'] = df['date'].apply(lambda x: f'{x}-01' if len(x.split('-')) == 2 else x)  # Add a day to 'year-month'
    # to datetime
    df['date'] = pd.to_datetime(df['date']).dt.to_period('M')        
    
    # create 'base' store id df to merge the different pre-period characteristics to
    #store_df = df[['store_id']].drop_duplicates()
    df = df.loc[df['date'] <= '2024-11'].copy()

    # subset sample period years
    df = df.loc[df['calendar_year'].isin([2022,2023,2024,2025])].copy()
    
    # subset e-cigarette products
    df = df.loc[df['subcategory'] == 'Vaping Products'].copy()
    
    if df.empty:
        print(f"Skipping {store} — empty DataFrame")
        continue
    
    store_month_aggs = (
        df
        .groupby(['store_id', 'date', 'product_type'], dropna = False)
        .agg(type_month_rev = ('total_revenue_amount', 'sum'))
        .reset_index()
        )

    # monthly_aggs = (
    #     df
    #     .groupby(['store_id', 'date', 'product_type'], dropna = False)
    #     .agg(type_month_rev = ('total_revenue_amount', 'sum'))
    #     .reset_index()
    #     .groupby(['store_id', 'product_type'], dropna = False)
    #     .agg(type_rev = ('type_month_rev', 'mean'))
    #     .reset_index()
    #     )
   
    
    # add cigs to subcat list
    li.append(store_month_aggs)
        
    # Increment and print the iteration counter
    iteration += 1
    print(f"Iteration {iteration}/{total_iterations}: Processed file {store}")

# combine dfs

all_stores = pd.concat(li, ignore_index = True)

# save
all_stores.to_feather('G:/My Drive/GSEFM/Research/e_cigarettes/data/processed_data/LS_Otter/e_cig_store_type_monthly_sales_22_25.feather')


#% sum across stores to get type aggregates
all_stores = pd.read_feather('G:/My Drive/GSEFM/Research/e_cigarettes/data/processed_data/LS_Otter/e_cig_store_type_monthly_sales_22_25.feather')

#all_stores = pd.concat(li, ignore_index = True)

# across stores
type_avg_per_month = (
    all_stores
    .groupby(['date', 'product_type'], dropna = False)
    .agg(monthly_type_sum = ('type_month_rev', 'sum'))
    .reset_index()
    .groupby('product_type', dropna = False)
    .agg(monthly_avg_type_sum = ('monthly_type_sum', 'mean'))
    .reset_index()
    )

type_avg_per_month.to_feather('G:/My Drive/GSEFM/Research/e_cigarettes/data/processed_data/LS_Otter/e_cig_store_type_avg_monthly_sales_22_25.feather')

#%% consolidate types
'''
Many types are simply the brand name. Consolidate type to major types, (e.g. disposable, pod, etc) and put the rest in 'other'
'''

type_avg_per_month = pd.read_feather('G:/My Drive/GSEFM/Research/e_cigarettes/data/processed_data/LS_Otter/e_cig_store_type_avg_monthly_sales_22_25.feather')

# first get oevrview of types with most sales
# monthly_sales_by_type = (
#     type_avg_per_month
#     .groupby('product_type', dropna = False)
#     .agg(total_monthly_sales = ('monthly_avg_type_sum', 'sum'))
#     .reset_index()
#     )

# del monthly_sales_by_type

# now consolidate types
type_avg_per_month['product_type']
types = type_avg_per_month['product_type'].unique().tolist()

# case-insensitive search for "pod"
pod_list = type_avg_per_month.loc[type_avg_per_month['product_type'].str.contains("pod", case=False, na=False), 'product_type'].unique().tolist()

# case-insensitive search for "tank"
tank_list = type_avg_per_month.loc[type_avg_per_month['product_type'].str.contains("tank", case=False, na=False), 'product_type'].unique().tolist()

type_avg_per_month['product_type'] = np.where(type_avg_per_month['product_type'] == 'Disposable Vape', 'Disposable', 
                                             np.where(type_avg_per_month['product_type'].isin(pod_list), 'Pod',
                                                  np.where(type_avg_per_month['product_type'].isin(tank_list), 'Tank',
                                                      type_avg_per_month['product_type'])))

# consolidate types
proper_types = ['Cartridge', 'Disposable', 'Pod', 'Tank', 'Device']

type_avg_per_month['product_type'] = np.where(~type_avg_per_month['product_type'].isin(proper_types), 'Other', type_avg_per_month['product_type'])

# sum types
monthly_sales_by_type = (
    type_avg_per_month
    .groupby('product_type')
    .agg(total_monthly_sales = ('monthly_avg_type_sum', 'sum'))
    .reset_index()
    )


# add market share column
market_size = monthly_sales_by_type['total_monthly_sales'].sum()

monthly_sales_by_type['market_share'] = monthly_sales_by_type['total_monthly_sales']/market_size

# save
monthly_sales_by_type.to_csv('G:/My Drive/GSEFM/Research/e_cigarettes/data/processed_data/LS_Otter/e_cig_type_monthly_sales_22_25.csv', index = False)

#%%












