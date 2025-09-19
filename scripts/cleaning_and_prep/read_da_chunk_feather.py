#import deweydatapy as ddp
import pandas as pd
import glob
import os
import gc
from timeit import default_timer as timer
import time

"""
This script reads in 10 csv.gz files at once. For a given store, it collects data 
for that store from across the 10 datasets and combines them into a single store-level dataframe.
This is done for all stores present in the chunk. The store-level frames are stored in a chunk-
specific folder.
"""

# Path of folder containing downloaded csv.gz files
folder_path = "D:/convenience_store/data/raw/LS_Otter/TRANSACTION_ITEMS_DAILY_AGG NEW/" 

# Create a folder to store the subsets
output_path = "D:/convenience_store/data/raw/LS_Otter/da_chunks_feather/"
#os.makedirs(output_folder, exist_ok=True)

all_files = glob.glob(folder_path + "*.csv.gz") #  files

# # list strings for subset of desired files
# to_exclude = ['2021-01-01', '2021-02-01', '2021-03-01', '2021-04-01', '2021-05-01', '2021-06-01',
#               '2021-07-01', '2021-08-01', '2021-09-01', '2021-10-01', '2021-11-01', '2021-12-01',
#               '2022-01-01', '2022-02-01', '2022-03-01', '2022-04-01']

# # Filter file paths to exclude any containing the specified patterns listed above
# files_to_read = [
#     path for path in all_files if not any(pattern in path for pattern in to_exclude)
# ]

test = pd.read_feather('D:/convenience_store/data/raw/LS_Otter/da_chunks_feather/chunk_180/22804.feather')
del test

#%% list the raw .gz files in chunks of 10

chunk_size = 10

smaller_lists = {}

for i in range(0, len(all_files), chunk_size):
    chunk_number = (i // chunk_size)
    smaller_lists[f"chunk_{chunk_number}"] = all_files[i:i + chunk_size]

# Unpack the dictionary into standalone lists
for key, value in smaller_lists.items():
    locals()[key] = value    

chunk_list_all = list(smaller_lists.keys())

"""
Optional: Split chunk_list_all into smaller chunks and run each smaller chunk separately below.

"""
# Split the chunk list into smaller chunk lists of size 20
# chunk_list_1 = chunk_list_all[0:25] # chunks 1-50
# chunk_list_2 = chunk_list_all[25:50] # chunks 50-100
# chunk_list_3 = chunk_list_all[50:75] # chunks 100-150
# chunk_list_4 = chunk_list_all[75:107] # chunks 150-200

#%% specify columns to keep

# read in example df
df = pd.read_csv('D:/convenience_store/data/raw/LS_Otter/TRANSACTION_ITEMS_DAILY_AGG NEW/TRANSACTION_ITEMS_DAILY_AGG_NEW-0-DATE-2022-01-01.csv.gz', engine="pyarrow", dtype={'GTIN': 'str'})

cols = list(df)
columns_to_keep = cols[1:26]
del df
gc.collect()

#%% Iterate though chunks and save

# if I paused the loop, redefine chunk list all#
#chunk_list_all_orig = chunk_list_all.copy()

chunk_list_all = chunk_list_all[319:414].copy()

#%%
for chunk_name in chunk_list_all:  # chunk_name is a string like "chunk_1"
    
    try:
    # Get the list of file paths associated with the chunk name
        chunk_files = globals()[chunk_name]  # Accessing the list by name
        
        # folder to save store-level files for each chunk
        output_folder = os.path.join(output_path, f"{chunk_name}/")
        
        # Ensure the directory exists, if not, create it
        os.makedirs(output_folder, exist_ok=True)
            
        # Initialize a dictionary to store dataframes for each store
        store_dataframes = {}
    
        print(f'Reading and appending: {chunk_name} -----------------------------', sep='\n')
        start = timer()
        for file in chunk_files: # for each file in the list of files 
            
            # Read CSV file
            df = pd.read_csv(file, engine="pyarrow", usecols=columns_to_keep, dtype={'GTIN': 'str'}) 
                            
            # Subset the dataframe based on STORE_ID
            for store_id, subset in df.groupby("STORE_ID"):
                if store_id in store_dataframes:
                    # Append to existing dataframe
                    store_dataframes[store_id] = pd.concat([store_dataframes[store_id], subset])
                else:
                    # Create a new dataframe
                    store_dataframes[store_id] = subset.copy()
        
        end = timer()
        print(f'Appended: {chunk_name} -----------------------------', sep='\n')
        print((end - start)/3600) # time elapsed in hours
        
        print(f'Saving: {chunk_name} -----------------------------', sep='\n')
        start = timer()
        # Save each subset dataframe to a separate CSV file
        for store_id, dataframe in store_dataframes.items():
            #output_filename = output_folder + f"{store_id}.csv"
            output_filename = os.path.join(output_folder, f"{store_id}.feather")
            dataframe.to_feather(output_filename)
        end = timer()
        print(f'Saved: {chunk_name} -----------------------------', sep='\n')
        print((end - start)/3600) # time elapsed in hours
    
    except PermissionError:
        print("Drive temporarily unavailable. Retrying in 60 seconds...")
        time.sleep(60)

#%%


