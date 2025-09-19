# -*- coding: utf-8 -*-
"""
Created on Wed Jun  4 15:23:34 2025

@author: cahase
"""

import numpy as np
import pandas as pd
import pyarrow as pa


fda_all = pd.read_excel("G:/My Drive/GSEFM/Research/e_cigarettes/data/raw_data/fda_follow_up_insps_2010_2019.xlsx")

# transpose
fda_all = fda_all.set_index(fda_all.columns[0]).transpose().reset_index().rename(columns = {'index': 'state'})

# rename columns
new_colnames = ['state', 'avg_cmp_amt_issued', 'avg_cmp_amt_collected', 'total_cmp_amt_issued', 'total_cmp_amt_collected']

# state abbreviation linked
states = pd.read_excel('G:/My Drive/GSEFM/Research/e_cigarettes/misc/state_abbreviations.xlsx')
states.columns = ['state', 'state2']
states['state'] = states['state'].str.strip()
states['state2'] = states['state2'].str.strip()

fda_all['state'] = fda_all['state'].str.strip()

# merge
fda_all2 = fda_all.copy()

fda_all = pd.merge(fda_all, states,
                   how = 'left',
                   on = 'state')

fda_all.drop(columns = 'state', inplace = True)

fda_all.rename(columns = {'state2': 'state'}, inplace = True)

# save to feather
fda_all.to_feather('G:/My Drive/GSEFM/Research/e_cigarettes/data/processed_data/fda_follow_up_insps_2010_2019.feather')
fda_all.to_csv('G:/My Drive/GSEFM/Research/e_cigarettes/data/processed_data/fda_follow_up_insps_2010_2019.csv', index = False)













