# -*- coding: utf-8 -*-
"""
Created on Sat Mar  2 11:44:09 2024

@author: cahase
"""

import pandas as pd
import os
import pyarrow as pa


"""
This script reads the da chunks and generates one dataframe per store for the entire sample period,
with the monthly product-level aggregations
"""

outpath = "D:/convenience_store/data/processed/LS_Otter/da_store_id_monthly_ag_feather/"

os.makedirs(outpath, exist_ok=True)

# list of chunks
chunk_list = []
for i in range(0, 414): # <- will need to create many more chunks now!
    chunk_list.append("chunk_" + str(i))


#%% get list of all filenames in chunk folders

# folder with chunks
parent_folder = "D:/convenience_store/data/raw/LS_Otter/da_chunks_feather/"

# Initialize an empty list to store file paths
file_names = []
# Iterate over subfolders
for i in range(0, 414):
    subfolder_path = os.path.join(parent_folder, f"chunk_{i}")
    
    # Check if the subfolder exists
    if os.path.exists(subfolder_path):
        # List files in the subfolder
        files = os.listdir(subfolder_path)
        
        # Append file names to the list
        for file_name in files:
            file_names.append(file_name)
            
            
# Create a new list containing unique file paths
unique_store_ids = list(set(file_names)) # 27464

#%% if not all processed, get list of processed/completed stores

# stores that successfully read in and aggregated to monthly
processed_stores = os.listdir(outpath)

# stores remaining to be read in and aggregated
difference = list(set(unique_store_ids) - set(processed_stores))

# chunk1 = difference[0:4500] # done
# chunk2 = difference[4500:9018] # done

#%% list of subfolder paths to read from
subfolder_paths = []
for i in range(0, 414):
    subfolder_path = os.path.join(parent_folder, f"chunk_{i}/")
    # Append file names to the list
    subfolder_paths.append(subfolder_path)

#%% define lists for monthly aggregations

# cols to group by
group_cols = ['STORE_ID', 'CALENDAR_YEAR', 'CALENDAR_MONTH', 'GTIN', 'NONSCAN_CATEGORY', 'NONSCAN_SUBCATEGORY', 'NONSCAN_DETAIL'] # the latter 3 account for the fact that different nonscan items all have GTIN == 0

# Define the columns to sum
sum_cols = ['QUANTITY', 'QUANTITY_WITH_DISCOUNT', 'TRANSACTION_COUNT', 'TRANSACTION_COUNT_WITH_DISCOUNT', 'TOTAL_REVENUE_AMOUNT']

# Columns to reattach (these should include only the ones you want to bring back)
reattach_cols = ['STORE_ID', 'GTIN', 'SCAN_TYPE', 'NONSCAN_CATEGORY', 'NONSCAN_SUBCATEGORY', 'NONSCAN_DETAIL',
                 'CATEGORY', 'SUBCATEGORY', 'MANUFACTURER', 'BRAND', 'PRODUCT_TYPE', 'SUB_PRODUCT_TYPE', 'UNIT_SIZE', 'PACK_SIZE', 'PRODUCT_DESCRIPTION']  # Replace with actual column names

#%% # split into chunks for parallel consoles

# chunk1 = unique_store_ids[0:5000] # done
# chunk2 = unique_store_ids[5000:10000] # done
# chunk3 = unique_store_ids[10000:15000] # done
# chunk4 = unique_store_ids[15000:21000] # done
# chunk5 = unique_store_ids[21000:27465] # done


#%%### testing/playing around

# gtin_df = pd.read_csv("D:/convenience_store/data/raw/LS_Otter/MASTER_GTIN NEW/MASTER_GTIN_NEW-0.csv", dtype = {'GTIN': 'str'})

# store = '29257.feather'

# # empty list to populate
# li = [] 
# # read through each chunk folder and accumulate all files for a given store id
# for path in subfolder_paths:
#     file = path + store
#     # if the store has a file in that chunk, read it in and append to list
#     try:
#         df = pd.read_feather(file)
#         li.append(df)
#     # if not, move to next chunk    
#     except FileNotFoundError:
#         pass  # Move to the next iteration if file doesn't exist

# # After the loop, concatenate the list of dataframes to create single store-specific df
# combined_df = pd.concat(li, ignore_index=True)

# head = combined_df.head(100000)

# zero_gtin = combined_df.loc[combined_df['GTIN'] == '0'].copy()

# non_zero_gtin = combined_df.loc[combined_df['GTIN'] != '0'].copy()

# foo = non_zero_gtin['NONSCAN_DETAIL'].unique()


#%% use chunk1, chunk2, chunk3, or chunk4 depending on console (run multiple in parallel)

# Initialize counter
total_iterations = len(difference)

# Initialize counter
iteration = 0

for store in difference:

    # empty list to populate
    li = [] 
    # read through each chunk folder and accumulate all files for a given store id
    for path in subfolder_paths:
        file = path + store
        # if the store has a file in that chunk, read it in and append to list
        try:
            df = pd.read_feather(file)
            li.append(df)
        # if not, move to next chunk    
        except FileNotFoundError:
            pass  # Move to the next iteration if file doesn't exist
    
    # After the loop, concatenate the list of dataframes to create single store-specific df
    combined_df = pd.concat(li, ignore_index=True)

    # force str type for all important columns
    cols = ['STORE_ID', 'GTIN', 'NONSCAN_CATEGORY', 'NONSCAN_SUBCATEGORY', 'NONSCAN_DETAIL']
    combined_df[cols] = combined_df[cols].astype(str)

    # create calendar month and year columns (some dfs have it already, others don't)
    new = combined_df['DATE'].astype('str').str.split('-', expand = True)
    combined_df['CALENDAR_YEAR'] = new[0].astype('int')
    combined_df['CALENDAR_MONTH'] = new[1].astype('int')

    # Deduplicate descriptive columns in combined_df to ensure unique rows for each store-product
    deduplicated_reattach = combined_df[reattach_cols].drop_duplicates(subset=['STORE_ID', 'GTIN', 'NONSCAN_CATEGORY', 'NONSCAN_SUBCATEGORY', 'NONSCAN_DETAIL']) # the latter 3 account for the fact that different nonscan items all have GTIN == 0

    # sum main columns to up monthly aggregation
    monthly_aggregates = (
        combined_df
        .groupby(group_cols, dropna = False) # very important to include dropna = False
        .agg(
            {col: 'sum' for col in sum_cols}
            )
        .reset_index()
        .merge(deduplicated_reattach, on=['STORE_ID', 'GTIN', 'NONSCAN_CATEGORY', 'NONSCAN_SUBCATEGORY', 'NONSCAN_DETAIL'], how='left')  # reattach columns from original frame that were dropped in the aggregation
    )
    
    # unit value columns
    monthly_aggregates['Q_PLUS_QWD'] = monthly_aggregates['QUANTITY'] + monthly_aggregates['QUANTITY_WITH_DISCOUNT']
    monthly_aggregates['UNIT_VALUE_Q_PLUS_QWD'] = monthly_aggregates['TOTAL_REVENUE_AMOUNT']/monthly_aggregates['Q_PLUS_QWD']
    monthly_aggregates['UNIT_VALUE_Q'] = monthly_aggregates['TOTAL_REVENUE_AMOUNT']/monthly_aggregates['QUANTITY']

    # save each store-specific df
    pa.feather.write_feather(monthly_aggregates, outpath + store)
    
    # Increment and print the iteration counter
    iteration += 1
    print(f"Iteration {iteration}/{total_iterations}: Processed file {store}")


#%% check

