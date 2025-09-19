# -*- coding: utf-8 -*-
"""
Created on Wed Jan 29 14:13:02 2025

@author: cahase
"""

import numpy as np
import pandas as pd
import glob
import os
import shutil
import gc
import pyarrow as pa
from functools import reduce



# '''
# For e-cigarette variable balance checks, read in the dataframe created in the 
# product market descriptive stats table panel a. This dataframe has monthly totals
# '''

# all_stores = pd.read_feather('G:/My Drive/GSEFM/Research/e_cigarettes/data/processed_data/LS_Otter/total_store_characteristics_22_25.feather')

# head = all_stores.head()
'''
Step 1: get monthly totals for each store for each of the variables to be decribed. Save this dataframe.

'''
#%% read in and store monthly aggregates for pre-treatment period at targeted stores

store_path = "D:/convenience_store/data/processed/LS_Otter/da_store_id_monthly_ag_feather/"

all_files = glob.glob(store_path + "*.feather") # 18214 files (previously 18940 files)

# Normalize to use the correct slash for the OS
all_files = [os.path.normpath(path) for path in all_files]

# Extract store numbers
store_numbers = [os.path.basename(path).split('.')[0] for path in all_files]

total_iterations = len(store_numbers)

store = '33967'

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
    
    # # create 'base' store id df to merge the different pre-period characteristics to
    # store_df = df[['store_id']].drop_duplicates()

    # subset sample period years
    df = df.loc[df['calendar_year'].isin([2022,2023,2024,2025])].copy()

    # drop nonscan items (e.g. fuel, lottery)
    df = df.loc[df['scan_type'] != 'NONSCAN'].copy()
    
    ## All products ##
    # transactions
    transactions = (
        df
        .groupby(['store_id', 'date'])
        .agg(transactions = ('transaction_count', 'sum'))
        .reset_index() # mean for each store
        )

    # units sold
    units_sold = (
        df
        .groupby(['store_id', 'date'])
        .agg(units = ('quantity', 'sum'))
        .reset_index() # mean for each store
        )

    # distinct products
    distinct_products = (
        df
        .groupby(['store_id', 'date'])
        ['gtin'].nunique()
        .reset_index(name='distinct_products')
        )

    revenue = (
        df
        .groupby(['store_id', 'date'])
        .agg(revenue = ('total_revenue_amount', 'sum'))
        .reset_index() # mean for each store
        )
    

    ## E-cigarettes ##
    
    ecigs = df.loc[df['subcategory'] == 'Vaping Products'].copy()


    ecig_transactions = (
        ecigs
        .groupby(['store_id', 'date'])
        .agg(ecig_transactions = ('transaction_count', 'sum'))
        .reset_index() # mean for each store
        )

    # units sold
    ecig_units_sold = (
        ecigs
        .groupby(['store_id', 'date'])
        .agg(ecig_units = ('quantity', 'sum'))
        .reset_index() # mean for each store
        )

    # distinct products
    ecig_distinct_products = (
        ecigs
        .groupby(['store_id', 'date'])
        ['gtin'].nunique()
        .reset_index(name='ecig_distinct_products')
        )

    ecig_revenue = (
        ecigs
        .groupby(['store_id', 'date'])
        .agg(ecig_revenue = ('total_revenue_amount', 'sum'))
        .reset_index() # mean for each store
        )

    # collect store summary stats in single df
    to_merge = [transactions, units_sold, distinct_products, revenue, ecig_transactions, ecig_units_sold, ecig_distinct_products, ecig_revenue]
    
    merged_df = reduce(lambda left, right: pd.merge(left, right, on=['store_id', 'date'], how='left'), to_merge)
    
    # e-cigarette revenue share
    merged_df['ecig_revenue_share'] = merged_df['ecig_revenue']/merged_df['revenue']
    
    li.append(merged_df)
    
    # Increment and print the iteration counter
    iteration += 1
    print(f"Iteration {iteration}/{total_iterations}: Processed file {store}")

    

# put store-level means into single dataframe and save
all_stores = pd.concat(li, ignore_index = True)

# save
all_stores.to_feather('G:/My Drive/GSEFM/Research/e_cigarettes/data/processed_data/LS_Otter/monthly_store_totals_for_balance_check.feather')

#%% assign targeted treatment indicator

# read in estimation panel
t_estimation_panel = pd.read_feather('G:/My Drive/GSEFM/Research/e_cigarettes/data/processed_data/LS_Otter/estimation_panels/t_and_s_insp_issue_2a_b.feather')

# read in monthly store sums
all_stores = pd.read_feather('G:/My Drive/GSEFM/Research/e_cigarettes/data/processed_data/LS_Otter/monthly_store_totals_for_balance_check.feather')

# violator treatment dataframe
violations = (
    t_estimation_panel.loc[t_estimation_panel['t_insp'] == 1]
    .groupby('store_id')
    ['date']
    .min()
    .rename('t_insp_date')
    .reset_index()
    )

all_stores['store_id'] = all_stores['store_id'].astype('int')

# Step 2: Merge this back to the original DataFrame
t_all_stores = all_stores.merge(violations, on='store_id', how='left')

t_all_stores['t_insp_date'].describe()

head = t_all_stores.head(10000)

# Step 3: Create the pre-treatment indicator
t_all_stores['pre_treatment'] = (t_all_stores['date'] < t_all_stores['t_insp_date']).astype(int)

#%% assign rival treatment indicator

# read in estimation panel
r_estimation_panel = pd.read_feather('G:/My Drive/GSEFM/Research/e_cigarettes/data/processed_data/LS_Otter/estimation_panels/r_insp_issue_2a_b.feather')

# violator treatment dataframe
rivals = (
    r_estimation_panel.loc[r_estimation_panel['r_insp'] == 1]
    .groupby('store_id')
    ['date']
    .min()
    .rename('r_insp_date')
    .reset_index()
    )

# Step 2: Merge this back to the original DataFrame
r_all_stores = all_stores.merge(rivals, on='store_id', how='left')

r_all_stores['r_insp_date'].describe()

# Step 3: Create the pre-treatment indicator
r_all_stores['pre_treatment'] = (r_all_stores['date'] < r_all_stores['r_insp_date']).astype(int)

#%% assign control indicator

cols = list(r_estimation_panel)

control_stores = r_estimation_panel.loc[(r_estimation_panel['rival'] != 1) & (r_estimation_panel['sister'] != 1)]['store_id'].unique().tolist()

control_panel = all_stores.loc[all_stores['store_id'].isin(control_stores)].copy()

#%% summary stats - violators

head = t_all_stores.head()

transactions = (
    t_all_stores.loc[t_all_stores['pre_treatment'] == 1]
    .groupby(['date'])
    ['transactions'].mean() # mean for each month
    .reset_index()
    .agg(transactions_m = ('transactions', 'mean'),
         transactions_std = ('transactions', 'std')
         )
    .reset_index()
    .rename(columns = {'transactions': 'Violators'})
    )

# units sold
units_sold = (
    t_all_stores.loc[t_all_stores['pre_treatment'] == 1]
    .groupby(['date'])
    ['units'].mean() # mean for each month
    .reset_index()
    .agg(units_m = ('units', 'mean'),
         units_std = ('units', 'std')
         )
    .reset_index()
    .rename(columns = {'units': 'Violators'})
    )

# distinct products
distinct_products = (
    t_all_stores.loc[t_all_stores['pre_treatment'] == 1]
    .groupby(['date'])
    ['distinct_products'].mean() # mean for each month
    .reset_index()
    .agg(distinct_products_m = ('distinct_products', 'mean'),
         distinct_products_std = ('distinct_products', 'std')
         )
    .reset_index()
    .rename(columns = {'distinct_products': 'Violators'})   
    )

revenue = (
    t_all_stores.loc[t_all_stores['pre_treatment'] == 1]
    .groupby(['date'])
    ['revenue'].mean() # mean for each month
    .reset_index()
    .agg(revenue_m = ('revenue', 'mean'),
         revenue_std = ('revenue', 'std')
         )
    .reset_index()
    .rename(columns = {'revenue': 'Violators'})   
    )

ecig_transactions = (
    t_all_stores.loc[t_all_stores['pre_treatment'] == 1]
    .groupby(['date'])
    ['ecig_transactions'].mean() # mean for each month
    .reset_index()
    .agg(ecig_transactions_m = ('ecig_transactions', 'mean'),
         ecig_transactions_std = ('ecig_transactions', 'std')
         )
    .reset_index()
    .rename(columns = {'ecig_transactions': 'Violators'})   
    )

# units sold
ecig_units_sold = (
    t_all_stores.loc[t_all_stores['pre_treatment'] == 1]
    .groupby(['date'])
    ['ecig_units'].mean() # mean for each month
    .reset_index()
    .agg(ecig_units_m = ('ecig_units', 'mean'),
         ecig_units_std = ('ecig_units', 'std')
         )
    .reset_index()
    .rename(columns = {'ecig_units': 'Violators'})   
    )

# distinct products
ecig_distinct_products = (
    t_all_stores.loc[t_all_stores['pre_treatment'] == 1]
    .groupby(['date'])
    ['ecig_distinct_products'].mean() # mean for each month
    .reset_index()
    .agg(ecig_distinct_products_m = ('ecig_distinct_products', 'mean'),
         ecig_distinct_products_std = ('ecig_distinct_products', 'std')
         )
    .reset_index()
    .rename(columns = {'ecig_distinct_products': 'Violators'})   
    )

ecig_revenue = (
    t_all_stores.loc[t_all_stores['pre_treatment'] == 1]
    .groupby(['date'])
    ['ecig_revenue'].mean() # mean for each month
    .reset_index()
    .agg(ecig_revenue_m = ('ecig_revenue', 'mean'),
         ecig_revenue_std = ('ecig_revenue', 'std')
         )
    .reset_index()
    .rename(columns = {'ecig_revenue': 'Violators'})   
    )

# combine into dataframe

violators_column = pd.concat([transactions, units_sold, distinct_products, revenue,
              ecig_transactions, ecig_units_sold, ecig_distinct_products, ecig_revenue], ignore_index = True)


#%% summary stats - rivals

transactions = (
    r_all_stores.loc[r_all_stores['pre_treatment'] == 1]
    .groupby(['date'])
    ['transactions'].mean() # mean for each month
    .reset_index()
    .agg(transactions_m = ('transactions', 'mean'),
         transactions_std = ('transactions', 'std')
         )
    .reset_index()
    .rename(columns = {'transactions': 'Rivals'})
    )

# units sold
units_sold = (
    r_all_stores.loc[r_all_stores['pre_treatment'] == 1]
    .groupby(['date'])
    ['units'].mean() # mean for each month
    .reset_index()
    .agg(units_m = ('units', 'mean'),
         units_std = ('units', 'std')
         )
    .reset_index()
    .rename(columns = {'units': 'Rivals'})
    )

# distinct products
distinct_products = (
    r_all_stores.loc[r_all_stores['pre_treatment'] == 1]
    .groupby(['date'])
    ['distinct_products'].mean() # mean for each month
    .reset_index()
    .agg(distinct_products_m = ('distinct_products', 'mean'),
         distinct_products_std = ('distinct_products', 'std')
         )
    .reset_index()
    .rename(columns = {'distinct_products': 'Rivals'})   
    )

revenue = (
    r_all_stores.loc[r_all_stores['pre_treatment'] == 1]
    .groupby(['date'])
    ['revenue'].mean() # mean for each month
    .reset_index()
    .agg(revenue_m = ('revenue', 'mean'),
         revenue_std = ('revenue', 'std')
         )
    .reset_index()
    .rename(columns = {'revenue': 'Rivals'})   
    )

ecig_transactions = (
    r_all_stores.loc[r_all_stores['pre_treatment'] == 1]
    .groupby(['date'])
    ['ecig_transactions'].mean() # mean for each month
    .reset_index()
    .agg(ecig_transactions_m = ('ecig_transactions', 'mean'),
         ecig_transactions_std = ('ecig_transactions', 'std')
         )
    .reset_index()
    .rename(columns = {'ecig_transactions': 'Rivals'})   
    )

# units sold
ecig_units_sold = (
    r_all_stores.loc[r_all_stores['pre_treatment'] == 1]
    .groupby(['date'])
    ['ecig_units'].mean() # mean for each month
    .reset_index()
    .agg(ecig_units_m = ('ecig_units', 'mean'),
         ecig_units_std = ('ecig_units', 'std')
         )
    .reset_index()
    .rename(columns = {'ecig_units': 'Rivals'})   
    )

# distinct products
ecig_distinct_products = (
    r_all_stores.loc[r_all_stores['pre_treatment'] == 1]
    .groupby(['date'])
    ['ecig_distinct_products'].mean() # mean for each month
    .reset_index()
    .agg(ecig_distinct_products_m = ('ecig_distinct_products', 'mean'),
         ecig_distinct_products_std = ('ecig_distinct_products', 'std')
         )
    .reset_index()
    .rename(columns = {'ecig_distinct_products': 'Rivals'})   
    )

ecig_revenue = (
    r_all_stores.loc[r_all_stores['pre_treatment'] == 1]
    .groupby(['date'])
    ['ecig_revenue'].mean() # mean for each month
    .reset_index()
    .agg(ecig_revenue_m = ('ecig_revenue', 'mean'),
         ecig_revenue_std = ('ecig_revenue', 'std')
         )
    .reset_index()
    .rename(columns = {'ecig_revenue': 'Rivals'})   
    )

# combine into dataframe

rivals_column = pd.concat([transactions, units_sold, distinct_products, revenue,
              ecig_transactions, ecig_units_sold, ecig_distinct_products, ecig_revenue], ignore_index = True)

#%% summary stats - controls

head = control_panel.head()

transactions = (
    control_panel
    .groupby(['date'])
    ['transactions'].mean() # mean for each month
    .reset_index()
    .agg(transactions_m = ('transactions', 'mean'),
         transactions_std = ('transactions', 'std')
         )
    .reset_index()
    .rename(columns = {'transactions': 'Controls'})
    )

# units sold
units_sold = (
    control_panel
    .groupby(['date'])
    ['units'].mean() # mean for each month
    .reset_index()
    .agg(units_m = ('units', 'mean'),
         units_std = ('units', 'std')
         )
    .reset_index()
    .rename(columns = {'units': 'Controls'})
    )

# distinct products
distinct_products = (
    control_panel
    .groupby(['date'])
    ['distinct_products'].mean() # mean for each month
    .reset_index()
    .agg(distinct_products_m = ('distinct_products', 'mean'),
         distinct_products_std = ('distinct_products', 'std')
         )
    .reset_index()
    .rename(columns = {'distinct_products': 'Controls'})   
    )

revenue = (
    control_panel
    .groupby(['date'])
    ['revenue'].mean() # mean for each month
    .reset_index()
    .agg(revenue_m = ('revenue', 'mean'),
         revenue_std = ('revenue', 'std')
         )
    .reset_index()
    .rename(columns = {'revenue': 'Controls'})   
    )

ecig_transactions = (
    control_panel
    .groupby(['date'])
    ['ecig_transactions'].mean() # mean for each month
    .reset_index()
    .agg(ecig_transactions_m = ('ecig_transactions', 'mean'),
         ecig_transactions_std = ('ecig_transactions', 'std')
         )
    .reset_index()
    .rename(columns = {'ecig_transactions': 'Controls'})   
    )

# units sold
ecig_units_sold = (
    control_panel
    .groupby(['date'])
    ['ecig_units'].mean() # mean for each month
    .reset_index()
    .agg(ecig_units_m = ('ecig_units', 'mean'),
         ecig_units_std = ('ecig_units', 'std')
         )
    .reset_index()
    .rename(columns = {'ecig_units': 'Controls'})   
    )

# distinct products
ecig_distinct_products = (
    control_panel
    .groupby(['date'])
    ['ecig_distinct_products'].mean() # mean for each month
    .reset_index()
    .agg(ecig_distinct_products_m = ('ecig_distinct_products', 'mean'),
         ecig_distinct_products_std = ('ecig_distinct_products', 'std')
         )
    .reset_index()
    .rename(columns = {'ecig_distinct_products': 'Controls'})   
    )

ecig_revenue = (
    control_panel
    .groupby(['date'])
    ['ecig_revenue'].mean() # mean for each month
    .reset_index()
    .agg(ecig_revenue_m = ('ecig_revenue', 'mean'),
         ecig_revenue_std = ('ecig_revenue', 'std')
         )
    .reset_index()
    .rename(columns = {'ecig_revenue': 'Controls'})   
    )

# combine into dataframe

controls_column = pd.concat([transactions, units_sold, distinct_products, revenue,
              ecig_transactions, ecig_units_sold, ecig_distinct_products, ecig_revenue], ignore_index = True)

#%% merge violator, rival, control

dfs = [violators_column, rivals_column, controls_column]
df_merged = reduce(lambda left, right: pd.merge(left, right, on='index', how='outer'), dfs)

# save
df_merged.to_csv('G:/My Drive/GSEFM/Research/e_cigarettes/output/tables/balance_check.csv', index = False)

#%%