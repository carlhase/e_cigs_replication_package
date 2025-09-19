# -*- coding: utf-8 -*-
"""
Created on Thu Aug 14 12:49:07 2025

@author: cahase
"""


import numpy as np
import pandas as pd
#import matplotlib.pyplot as plt
#import pyarrow as pa

#%% for horizontal t+4 figure, alternate code


zipnr_df = pd.read_csv('G:/My Drive/GSEFM/Research/e_cigarettes/output/regressions/zipnr_zipcode_mapping_2.csv', dtype = {'zip_code': 'str'}, header = None)

# Extract the number after 'zipnr' and the 5-digit ZIP code within quotes
zipnr_df['zipnr'] = zipnr_df[0].str.extract(r'zipnr (\d+)').astype(int)
zipnr_df['zip_code'] = zipnr_df[0].str.extract(r'"(\d{5})"')

# drop excess columns and duplicates
zipnr_df = zipnr_df[['zipnr', 'zip_code']].drop_duplicates()

# dictionary with state nr and state name
zipnr_dict = zipnr_df.set_index('zipnr')['zip_code'].to_dict()

matrix = pd.read_stata('G:/My Drive/GSEFM/Research/e_cigarettes/output/regressions/r_insp_2a_b_ols_vape_zip_matrices_cluster_z.dta')

matrix['zipnr'] = matrix['zipnr'].astype('int')

zip_list = zipnr_df['zipnr'].unique().tolist()

li = []
for zip in zip_list:
    
    # subset zip3nr
    df = matrix.loc[matrix['zipnr'] == zip].reset_index(drop = True)
    
    # drop columns of other zip3nrs
    df.dropna(axis=1, how='all', inplace = True) 
    
    #if row 0 is na for all columns (ie the effect for that zip3nr is not identified), skip to next iteration
    if df.empty or df.iloc[0].isna().all():
        continue
    
    # Get the column names excluding 'zipnr'
    other_cols = df.columns.difference(['zipnr'], sort=False)
    
    # Sort to keep consistent ordering (if needed)
    other_cols = [col for col in df.columns if col != 'zipnr']
    
    # Create new column names
    new_col_names = {old: f"{i+1}" for i, old in enumerate(other_cols)}

    # Rename columns
    df = df.rename(columns=new_col_names)
    
    if 'zipnr' in df.columns:
        df = df[['zipnr'] + [col for col in df.columns if col != 'zipnr']]
    
    # subset row zero
    df2 = df.iloc[[0]].copy()
    
    # subset column 14 and zipnr
    df2 = df2[['14', 'zipnr']]
    
    li.append(df2)    

# combine to create single dataframe
combined = pd.concat(li)

# add actual zip code
combined = pd.merge(combined, zipnr_df,
                    how = 'left',
                    on = 'zipnr',
                    indicator = True)


nomerge = combined.loc[combined['_merge'] != 'both'].copy()
del nomerge
combined.drop(columns = '_merge', inplace = True)

# prep for saving
combined.drop(columns = 'zipnr', inplace = True)
combined.rename(columns = {'14': 'effect'}, inplace = True)

combined.dtypes

# save
combined.to_feather('G:/My Drive/GSEFM/Research/e_cigarettes/output/r_insp_2a_b_ols_vape_qty_1b_heter_by_zip_t_6_cluster_z.feather')

#%%
# #%% save negative
# negative = combined.loc[combined['effect'] < 0].copy()

# negative.to_feather('G:/My Drive/GSEFM/Research/e_cigarettes/output/r_insp_2a_bsj_vape_heter_by_zip_t_4_cluster_z_neg.feather')

# # save positive
# positive = combined.loc[combined['effect'] > 0].copy()

# positive.to_feather('G:/My Drive/GSEFM/Research/e_cigarettes/output/r_insp_2a_bsj_vape_heter_by_zip_t_4_cluster_z_pos.feather')

# #%% plot figure

# # Creating the DataFrame
# df = pd.DataFrame(combined)

# df.columns = df.columns.astype('str')

# Sort the DataFrame by effect, descending (positive to negative)
df_sorted = df.sort_values('effect', ascending=True).reset_index(drop=True)

# Create the plot
fig, ax = plt.subplots(figsize=(6, 10))
#plt.scatter(df_sorted['effect'], df_sorted.index)
ax.scatter(df_sorted['effect'], df_sorted.index, color='black', label='Coefficient')

ax.axvline(x=0.0, color='red', linestyle='--', linewidth=2, label='Zero Line')

ax.set_xlabel('Treatment effect')

plt.yticks([])
plt.tight_layout()
plt.show()

# Save the figure as a PNG file
plt.savefig('G:/My Drive/GSEFM/Research/e_cigarettes/output/r_insp_2a_bsj_vape_heter_by_zip_t_4_cluster_z.png', format='png', dpi=300)

# Show the plot
plt.show()



#%% creating the dataframe for a map
zipnr_df = pd.read_csv('G:/My Drive/GSEFM/Research/e_cigarettes/data/processed_data/summary_statistics/all_stores_2022_vape_Zip_zipnr_linked.csv', dtype = {'zip_code': 'str'})

# drop excess columns and duplicates
zipnr_df = zipnr_df[['zipnr', 'zip_code']].drop_duplicates()

matrix = pd.read_stata('G:/My Drive/GSEFM/Research/e_cigarettes/output/regressions/r_insp_2a_bjs_vape_zip_matrices_cluster_z.dta')

# drop horiz_t4 columns created in Stata
#matrix = matrix.loc[:, ~matrix.columns.str.contains("horiz_t4")]

matrix['zipnr'] = matrix['zipnr'].astype('int')

zip_list = zipnr_df['zipnr'].unique().tolist()
                
li = []
for zip in zip_list:
    
    # subset zip3nr
    df = matrix.loc[matrix['zipnr'] == zip].reset_index(drop = True)
    
    # drop columns of other zip3nrs
    df.dropna(axis=1, how='all', inplace = True) 
    
    #if row 0 is na for all columns (ie the effect for that zip3nr is not identified), skip to next iteration
    if df.empty or df.iloc[0].isna().all():
        continue    
    # Get the column names excluding 'zip3nr'
    other_cols = df.columns.difference(['zipnr'], sort=False)
    
    # Sort to keep consistent ordering (if needed)
    other_cols = [col for col in df.columns if col != 'zipnr']
    
    # Create new column names
    new_col_names = {old: f"{i+1}" for i, old in enumerate(other_cols)}

    # Rename columns
    df = df.rename(columns=new_col_names)
    
    if 'zipnr' in df.columns:
        df = df[['zipnr'] + [col for col in df.columns if col != 'zipnr']]
    
    # subset row zero
    df2 = df.iloc[[0]].copy()
    
    # subset column 10 and zipnr
    df2 = df2[['10', 'zipnr']]
    
    li.append(df2)    


# combine to create single dataframe
combined = pd.concat(li)

# add actual 3-digit zip code
combined = pd.merge(combined, zipnr_df,
                    how = 'left',
                    on = 'zipnr',
                    indicator = True)


nomerge = combined.loc[combined['_merge'] != 'both'].copy()
del nomerge
combined.drop(columns = '_merge', inplace = True)

combined.rename(columns = {'10': 'effect'}, inplace = True)

positive = combined.loc[combined['effect'] > 0].copy()

negative = combined.loc[combined['effect'] < 0].copy()

# save
positive.to_feather('G:/My Drive/GSEFM/Research/e_cigarettes/data/processed_data/pos_zip.feather')
negative.to_feather('G:/My Drive/GSEFM/Research/e_cigarettes/data/processed_data/neg_zip.feather')

#%% states for pos and neg

store_info = pd.read_csv('G:/My Drive/GSEFM/Research/e_cigarettes/data/processed_data/consolidated_stores.csv')

positive_stores = positive['zip_code'].unique().tolist()
negative_stores = negative['zip_code'].unique().tolist()


positive_info = store_info.loc[store_info['zip_code'].isin(positive_stores)].copy()
positive_info.drop_duplicates(subset = ['zip_code', 'state'], inplace = True)
positive_info = positive_info[['zip_code', 'state']]

negative_info = store_info.loc[store_info['zip_code'].isin(negative_stores)].copy()
negative_info.drop_duplicates(subset = ['zip_code', 'state'], inplace = True)
negative_info = negative_info[['zip_code', 'state']]

# positive zips per state
pos_counts = (
    positive_info
    .groupby('state')
    .agg(pos_count=('state', 'size'))
    .reset_index()
)

# negative zips per state
neg_counts = (
    negative_info
    .groupby('state')
    .agg(neg_count=('state', 'size'))
    .reset_index()
)

counts = pd.merge(pos_counts, neg_counts,
                  how = 'outer',
                  on = 'state')

# save
counts.to_csv('G:/My Drive/GSEFM/Research/e_cigarettes/output/regressions/pos_neg_zip_counts_per_state.csv', index = False)


#%% store type/store info

positive = pd.read_feather('G:/My Drive/GSEFM/Research/e_cigarettes/data/processed_data/pos_zip.feather')
negative = pd.read_feather('G:/My Drive/GSEFM/Research/e_cigarettes/data/processed_data/neg_zip.feather')

combined = pd.concat([positive, negative])

combined.drop(columns = 'zipnr', inplace = True)

rival_zips = combined['zip_code'].tolist()
pos_zips = positive['zip_code'].tolist()
neg_zips = negative['zip_code'].tolist()

store_info = pd.read_csv('G:/My Drive/GSEFM/Research/e_cigarettes/data/processed_data/consolidated_stores.csv')

rival_info = store_info.loc[store_info['zip_code'].isin(rival_zips)].copy()

# positive info
pos_info = store_info.loc[store_info['zip_code'].isin(pos_zips)].copy()

# negative info
neg_info = store_info.loc[store_info['zip_code'].isin(neg_zips)].copy()









# Select columns that start with 'horiz_t4'
# df_subset = matrix.filter(like="horiz_t4")

# drop unnecessary/empty rows
df_subset2 = df_subset.iloc[np.r_[0:6, 7]]

# drop columns with 0 in row 0 (ie states with no coefficient estimate)
df_subset3 = df_subset2.loc[:, df_subset2.loc[0] != 0].copy()

# remove horiz_t4 from coumns names
df_subset3.columns = df_subset3.columns.str.replace('horiz_t4', '', regex=False)

df_subset3.columns = df_subset3.columns.astype(int)

df_new = df_subset3.copy().rename(columns=zipnr_dict)

# Sort the columns by the values in row index 0 (descending order)
df_new2 = df_new.loc[:, df_new.iloc[0].sort_values(ascending=True).index]

# zips with positive effect
positive = df_new.loc[:, df_new.iloc[0] > 0].copy()
positive_and_sig = df_new.loc[:, (df_new.iloc[0] > 0) & (df_new.iloc[3] <= .1)].copy()

# Create df with zip and effect size columns 
pos_and_sig = pd.DataFrame({
    'zip_code': positive_and_sig.columns,
    'effect_size': positive_and_sig.iloc[0]
    })

# save
pos_and_sig.to_csv('G:/My Drive/GSEFM/Research/e_cigarettes/output/r_insp_2a_bsj_vape_heter_by_zip_t_4_cluster_z_pos_and_sig.csv', index = False)

# 3zips with positive effect
negative = df_new.loc[:, df_new.iloc[0] < 0].copy()
negative_and_sig = df_new.loc[:, (df_new.iloc[0] < 0) & (df_new.iloc[3] <= .1)].copy()

# Create df with 3zip and effect size columns 
neg_and_sig = pd.DataFrame({
    'zip_code': negative_and_sig.columns,
    'effect_size': negative_and_sig.iloc[0]
    })

# save
neg_and_sig.to_csv('G:/My Drive/GSEFM/Research/e_cigarettes/output/r_insp_2a_bsj_vape_heter_by_zip_t_4_cluster_z_neg_and_sig.csv', index = False)

# all significant 3zips
significant = df_new.loc[:, df_new.iloc[3] <= .1].copy()

# Create df with 3zip and effect size columns 
sig = pd.DataFrame({
    'zip_code': significant.columns,
    'effect_size': significant.iloc[0]
    })

# save
sig.to_csv('G:/My Drive/GSEFM/Research/e_cigarettes/output/r_insp_2a_bsj_vape_heter_by_zip_t_4_cluster_z_sig.csv', index = False)

# all 3zips (significant or not)
all_zips = pd.DataFrame({
    'zip_code': df_new.columns,
    'effect_size': df_new.iloc[0]
    })

# save
all_zips.to_csv('G:/My Drive/GSEFM/Research/e_cigarettes/output/r_insp_2a_bsj_vape_heter_by_zip_t_4_cluster_z_all_zips.csv', index = False)












