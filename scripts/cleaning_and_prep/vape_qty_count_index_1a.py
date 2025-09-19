# -*- coding: utf-8 -*-
"""
Created on Wed Aug  6 11:01:17 2025

@author: cahase
"""


import numpy as np
import pandas as pd
import glob
import os
import pyarrow as pa
from pathlib import Path
import re

#from fuzzywuzzy import fuzz

#store_path = "D:/convenience_store/data/processed/da_store_id_monthly_ag_feather/"
store_path = "D:/convenience_store/data/processed/LS_Otter/da_store_id_monthly_ag_feather/"


outpath = "D:/convenience_store/data/processed/LS_Otter/indexes/vape_qty_count_indexes_1a/"

# Ensure the directory exists, if not, create it
os.makedirs(outpath, exist_ok=True)

#%% list of files (stores)

all_files = glob.glob(store_path + "*.feather") # 

# Normalize to use the correct slash for the OS
#all_files = [os.path.normpath(path) for path in all_files]
all_files = [Path(p).as_posix() for p in all_files]

# Extract store numbers
store_numbers = [os.path.basename(path).split('.')[0] for path in all_files]

# store = '361'
# subcat = 'Vaping Products'
# chunk_1 = store_numbers[0:10000]
#chunk_2 = store_numbers[10000:20000]
#chunk_3 = store_numbers[20000:30000]

# Initialize counter
total_iterations = len(store_numbers)

# Initialize counter
iteration = 0

#%% check for differences

# processed_files = glob.glob(outpath + "*.feather") # 

# # normalize file paths
# processed_files = [Path(p).as_posix() for p in processed_files]

# # extract processed store numbers
# processed_stores = [re.search(r'(\d+)\.feather$', path).group(1) for path in processed_files]

# list_complement = list(set(store_numbers) - set(processed_stores))

# total_iterations = len(list_complement)

store = '24361'

#%%

for store in store_numbers:
    
    input_filename = os.path.join(store_path, f"{store}.feather")

    df = pd.read_feather(input_filename)

    if df.empty:
        print(f"Skipping {store} — empty DataFrame")
        continue
    
    # make column names lowercase
    df.columns = df.columns.str.lower()
    
    # create date column with YYYY-MM format
    df['date'] = df['calendar_year'].astype('str') + '-' + df['calendar_month'].astype('str')
    
    # to datetime
    df['date'] = pd.to_datetime(df['date']).dt.to_period('M')
    
    # keep only gtin scan types
    df = df.loc[df['scan_type'] == 'GTIN'].copy()

    # create 'base' store id-month df to merge the different indexes to
    #store_df = df[['store_id', 'date']].drop_duplicates()

    subcat_df = df.loc[df['subcategory'] == 'Vaping Products'].copy()
    
    if subcat_df.empty:
        print(f"Skipping {store} — empty subcat dataFrame")
        continue

    #subcat_df.rename(columns = {'qtyaction_count': 'qty_count'}, inplace = True)

    # monthly total
    monthly_qty_count = (
        subcat_df
        .groupby(['store_id', 'date'])
        .agg(qty_count = ('quantity', 'sum'))
        .reset_index()
        )
    
    # Sort the DataFrame by store_id, product_id, and date
    monthly_qty_count = monthly_qty_count.sort_values(by=['store_id', 'date'])
    
    # Calculate the month difference between consecutive rows within each group
    monthly_qty_count['month_diff'] = monthly_qty_count.groupby(['store_id'])['date'].diff().apply(lambda x: x.n if pd.notna(x) else np.nan)
    
    # Group by 'store_id' and 'product_id' and calculate the lagged 'unit_value' only for consecutive months
    monthly_qty_count['lag_qty_count'] = monthly_qty_count.groupby(['store_id'])['qty_count'].shift(1)
    
    # Set lagged values to NaN if the time difference is not exactly 1 month
    monthly_qty_count['lag_qty_count'] = np.where(monthly_qty_count['month_diff'] == 1, monthly_qty_count['lag_qty_count'], np.nan)
    
    monthly_qty_count = monthly_qty_count.fillna(value=np.nan)
    
    monthly_qty_count['qty_count_index'] = (monthly_qty_count['qty_count']/monthly_qty_count['lag_qty_count'])
    
    monthly_qty_count['l_qty_count_index'] = np.log(monthly_qty_count['qty_count_index'])
        
    # save each store-specific df
    output_filename = os.path.join(outpath, f"{store}.feather")
    output_filename = os.path.normpath(output_filename)
    
    pa.feather.write_feather(monthly_qty_count, output_filename)
    
    # Increment and print the iteration counter
    iteration += 1
    print(f"Iteration {iteration}/{total_iterations}: Processed file {store}")


#% read in and append panels of quantity indexes
# list of files (stores)
source_dir = "D:/convenience_store/data/processed/LS_Otter/indexes/vape_qty_count_indexes_1a/"

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
indexes.to_feather('D:/convenience_store/data/processed/LS_Otter/indexes/vape_qty_count_index_1a.feather')



#%%
