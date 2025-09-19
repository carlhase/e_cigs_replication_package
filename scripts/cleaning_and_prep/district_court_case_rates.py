# -*- coding: utf-8 -*-
"""
Created on Wed Jun 25 12:54:38 2025

@author: cahase
"""

import numpy as np
import pandas as pd
import pyarrow as pa

#%% all retailers

all_cases = pd.read_excel("G:/My Drive/GSEFM/Research/e_cigarettes/data/processed_data/us_district_court_case_stats.xlsx")

all_cases.rename(columns = {'Unnamed: 0': 'state',
                            'Unnamed: 1': 'total_civil_cases',
                            'Total  U.S.  Civil  Cases': 'total_us_civil_cases',
                            'Forfeitures  and  Penalties': 'forfeitures_and_penalties'}, 
                 inplace = True)
                            
                            
all_cases['total_us_civil_cases'] = all_cases['total_us_civil_cases'].astype('int')
all_cases['total_civil_cases'] = all_cases['total_civil_cases'].astype('int')
all_cases['forfeitures_and_penalties'] = all_cases['forfeitures_and_penalties'].astype('int')
all_cases['state'] = all_cases['state'].astype('str')

# share of civil cases that are penalty collections
all_cases['collection_share_of_US_cases'] = all_cases['forfeitures_and_penalties']/all_cases['total_us_civil_cases']
all_cases['collection_share_of_all_cases'] = all_cases['forfeitures_and_penalties']/all_cases['total_civil_cases']

all_cases2 = all_cases[['state', 'total_civil_cases', 'total_us_civil_cases', 'forfeitures_and_penalties', 'collection_share_of_US_cases', 'collection_share_of_all_cases']].copy()

# export as feather
all_cases2.to_feather('G:/My Drive/GSEFM/Research/e_cigarettes/misc/penalty_collection_share_civil_cases_2022.feather')




