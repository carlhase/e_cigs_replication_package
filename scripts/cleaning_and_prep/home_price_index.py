# -*- coding: utf-8 -*-
"""
Created on Wed Jul 23 15:06:37 2025

@author: cahase
"""

import pandas as pd
import numpy as np

inpath = 'G:/My Drive/GSEFM/Research/e_cigarettes/data/raw_data/fhfa/'


hpi = pd.read_excel('G:/My Drive/GSEFM/Research/e_cigarettes/data/raw_data/fhfa/hpi_at_3zip.xlsx', dtype = 'str')

cols = list(hpi)

new_cols = ['three_zip', 'year', 'quarter', 'hpi', 'index_type']  # your list of new column names

hpi.columns = new_cols

hpi.drop(columns = 'index_type', inplace = True)

hpi['three_zip'] = hpi['three_zip'].str.zfill(3)


hpi = hpi.loc[hpi['year'].isin(['2022', '2023', '2024', '2025'])].copy()

hpi['quarter'] = 'Q' + hpi['quarter'].astype(str)

hpi['qy'] = hpi['year'] + hpi['quarter']

hpi['qy'] = pd.to_datetime(hpi['qy']).dt.to_period('Q')

hpi['hpi'] = hpi['hpi'].astype('float')

#%%


# Sort the DataFrame by store_id, product_id, and date
hpi = hpi.sort_values(by=['three_zip', 'qy'])

# Calculate the month difference between consecutive rows within each group
hpi['qtr_diff'] = hpi.groupby(['three_zip'])['qy'].diff().apply(lambda x: x.n if pd.notna(x) else np.nan)

# Group by 'store_id' and 'product_id' and calculate the lagged 'unit_value' only for consecutive months
hpi['lag_hpi'] = hpi.groupby(['three_zip'])['hpi'].shift(1)

# Set lagged values to NaN if the time difference is not exactly 1 month
hpi['lag_hpi'] = np.where(hpi['qtr_diff'] == 1, hpi['lag_hpi'], np.nan)

hpi = hpi.fillna(value=np.nan)

hpi['index_ratio'] = hpi['hpi'] / hpi['lag_hpi'] 

hpi['l_home_price_index'] = np.log(hpi['index_ratio'])

hpi['l_home_price_index'].replace([np.inf, -np.inf], np.nan, inplace=True)

#%% save

hpi.to_feather('G:/My Drive/GSEFM/Research/e_cigarettes/data/processed_data/fhfa/home_price_index.feather')

#%%












