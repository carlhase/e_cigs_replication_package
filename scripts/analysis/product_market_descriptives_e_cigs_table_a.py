# -*- coding: utf-8 -*-
"""
Created on Mon Jun  2 13:42:43 2025

@author: cahase
"""

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


#%% read in and store monthly aggregates for pre-treatment period at targeted stores

store_path = "D:/convenience_store/data/processed/LS_Otter/da_store_id_monthly_ag_feather/"

all_files = glob.glob(store_path + "*.feather") # 18214 files (previously 18940 files)

# Normalize to use the correct slash for the OS
all_files = [os.path.normpath(path) for path in all_files]

# Extract store numbers
store_numbers = [os.path.basename(path).split('.')[0] for path in all_files]

total_iterations = len(store_numbers)

# # test
#store = '32796'
# # #%% to_delete
# gtin = pd.read_csv("D:/convenience_store/data/raw/LS_Otter/MASTER_GTIN NEW/MASTER_GTIN_NEW-0.csv")

# gtin.columns = gtin.columns.str.lower()

# categories = gtin['category'].unique()

'''
Panel a column 1 (monthly averages per establishment)
'''
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
    
    df = df.loc[df['date'] <= '2024-11'].copy()
    
    # create 'base' store id df to merge the different pre-period characteristics to
    store_df = df[['store_id']].drop_duplicates()

    # subset sample period years
    df = df.loc[df['calendar_year'].isin([2022,2023,2024,2025])].copy()

    # subset e-cigarette products
    df = df.loc[df['subcategory'] == 'Vaping Products'].copy()
    
    if df.empty:
        print(f"Skipping {store} — empty DataFrame")
        continue

    # transactions
    transactions = (
        df
        .groupby(['store_id', 'date'])
        .agg(transactions = ('transaction_count', 'sum'))
        .groupby('store_id')
        ['transactions'].mean()
        .reset_index() # mean for each store
        )

    # units sold
    units_sold = (
        df
        .groupby(['store_id', 'date'])
        .agg(units = ('quantity', 'sum'))
        .groupby('store_id')
        ['units'].mean()
        .reset_index() # mean for each store
        )

    # distinct products
    distinct_products = (
        df
        .groupby(['store_id', 'date'])
        ['gtin'].nunique()
        .groupby('store_id')
        .mean()  # mean for each store
        .reset_index(name='distinct_products')
        )

    revenue = (
        df
        .groupby(['store_id', 'date'])
        .agg(revenue = ('total_revenue_amount', 'sum'))
        .groupby('store_id')
        ['revenue'].mean()
        .reset_index() # mean for each store
        )
    

    # collect store summary stats in single df
    to_merge = [store_df, transactions, units_sold, distinct_products, revenue]
    
    merged_df = reduce(lambda left, right: pd.merge(left, right, on='store_id', how='left'), to_merge)
    
    li.append(merged_df)
    
    # Increment and print the iteration counter
    iteration += 1
    print(f"Iteration {iteration}/{total_iterations}: Processed file {store}")

    

# put store-level means into single dataframe and save
all_stores = pd.concat(li, ignore_index = True)

# save
all_stores.to_feather('G:/My Drive/GSEFM/Research/e_cigarettes/data/processed_data/LS_Otter/ecig_monthly_store_characteristics_22_25.feather')

# export to stata
all_stores.to_stata('G:/My Drive/GSEFM/Research/e_cigarettes/data/processed_data/LS_Otter/ecig_monthly_store_characteristics_22_25.dta')

#%% desc. monthly averages (for appendix)



#%% aggregate over stores to get sample desc stats

all_stores = pd.read_feather('G:/My Drive/GSEFM/Research/e_cigarettes/data/processed_data/LS_Otter/ecig_monthly_store_characteristics_22_25.feather')

head = all_stores.head()
columns_to_summarize = ['transactions', 'units', 'distinct_products', 'revenue']

summary_df = pd.DataFrame({
    'mean': all_stores[columns_to_summarize].mean(),
    'std': all_stores[columns_to_summarize].std()
    }).reset_index()

# save
summary_df.to_csv('G:/My Drive/GSEFM/Research/e_cigarettes/output/tables/ecig_product_market_descriptives_table_a_col_1.csv', index = None)


#%%
'''
Panel a column 2 (sample totals)

'''
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
    
    df = df.loc[df['date'] <= '2024-11'].copy()

    # subset sample period years
    df = df.loc[df['calendar_year'].isin([2022,2023,2024,2025])].copy()

    # subset e-cigarette products
    df = df.loc[df['subcategory'] == 'Vaping Products'].copy()
    
    if df.empty:
        print(f"Skipping {store} — empty DataFrame")
        continue

    # transactions
    transactions = (
        df
        .groupby(['store_id'])
        .agg(transactions = ('transaction_count', 'sum'))
        # .groupby('store_id')
        # ['transactions'].mean()
        .reset_index() # mean for each store
        )

    # units sold
    units_sold = (
        df
        .groupby(['store_id'])
        .agg(units = ('quantity', 'sum'))
        # .groupby('store_id')
        # ['units'].mean()
        .reset_index() # mean for each store
        )

    # distinct products
    # distinct_products = (
    #     df
    #     .groupby(['store_id'])
    #     ['gtin'].nunique()
    #     # .groupby('store_id')
    #     # .mean()  # mean for each store
    #     .reset_index(name='distinct_products')
    #     )

    revenue = (
        df
        .groupby(['store_id'])
        .agg(revenue = ('total_revenue_amount', 'sum'))
        # .groupby('store_id')
        # ['revenue'].mean()
        .reset_index() # mean for each store
        )
    

    # collect store summary stats in single df
    to_merge = [transactions, units_sold, revenue]
    
    merged_df = reduce(lambda left, right: pd.merge(left, right, on='store_id', how='left'), to_merge)
    
    li.append(merged_df)
    
    # Increment and print the iteration counter
    iteration += 1
    print(f"Iteration {iteration}/{total_iterations}: Processed file {store}")

    

# put store-level means into single dataframe and save
all_stores = pd.concat(li, ignore_index = True)

#

# save
all_stores.to_feather('G:/My Drive/GSEFM/Research/e_cigarettes/data/processed_data/LS_Otter/ecig_total_store_sums_22_25.feather')

#%%

all_stores = pd.read_feather('G:/My Drive/GSEFM/Research/e_cigarettes/data/processed_data/LS_Otter/ecig_total_store_sums_22_25.feather')

cols = list(all_stores)
cols_to_sum = cols[1:4] #+ [cols[4]]

# sum
summary_df = pd.DataFrame({
    'sum': all_stores[cols_to_sum].sum(),
    }).reset_index()

# read in column 1 created above
summary_df1 = pd.read_csv('G:/My Drive/GSEFM/Research/e_cigarettes/output/tables/ecig_product_market_descriptives_table_a_col_1.csv')

# merge
merged_df = pd.merge(summary_df, summary_df1, on = 'index', how='right')

# read in GTIN df and count unique products
gtin = pd.read_csv("D:/convenience_store/data/raw/LS_Otter/MASTER_GTIN NEW/MASTER_GTIN_NEW-0.csv")

gtin.columns = gtin.columns.str.lower()

distinct_products = len(gtin.loc[gtin['subcategory'] == 'Vaping Products'])

# add distinct products to column 1
merged_df.at[2, 'sum'] = distinct_products

# rearrange cols to match table in text
cols = [col for col in merged_df.columns if col != 'sum'] + ['sum']
merged_df = merged_df[cols]

# save
merged_df.to_csv('G:/My Drive/GSEFM/Research/e_cigarettes/output/tables/ecig_product_market_descriptives_table_a.csv', index = None)

#%%


