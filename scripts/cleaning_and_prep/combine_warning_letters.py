# -*- coding: utf-8 -*-
"""
Created on Thu Jan  9 20:48:27 2025

@author: cahase
"""

import numpy as np
import pandas as pd
import openpyxl
#pip install openpyxl

inpath = "G:/My Drive/GSEFM/Research/e_cigarettes/warning_letters/"

# Read the Excel file
cohort_1 = pd.read_excel(inpath + "2023_05_25_announcement.xlsx")
cohort_2 = pd.read_excel(inpath + "2023_06_22_announcement.xlsx")
cohort_3 = pd.read_excel(inpath + "2023_09_28_announcement.xlsx")
cohort_4 = pd.read_excel(inpath + "2024_03_26_announcement.xlsx")
cohort_5 = pd.read_excel(inpath + "2024_07_25_announcement.xlsx")
cohort_6 = pd.read_excel(inpath + "2024_11_26_announcement.xlsx")

cohort_1.columns = cohort_1.columns.str.lower()
cohort_2.columns = cohort_2.columns.str.lower()
cohort_3.columns = cohort_3.columns.str.lower()
cohort_4.columns = cohort_4.columns.str.lower()
cohort_5.columns = cohort_5.columns.str.lower()
cohort_6.columns = cohort_6.columns.str.lower()

cols1 = list(cohort_1)
cols2 = list(cohort_2)
cols3 = list(cohort_3)
cols4 = list(cohort_4)
cols5 = list(cohort_5)
cols6 = list(cohort_6)

cols_names = ['retailer_type', 'insp_date', 'issue_date', 'firm', 'address', 'city', 'state', 'zip_code', 'products']

cohort_1.columns = cols_names
cohort_2.columns = cols_names
cohort_3.columns = cols_names
cohort_4.columns = cols_names
cohort_5.columns = cols_names
cohort_6.columns = cols_names

cohort_1.dtypes
cohort_5.dtypes
cohort_6.dtypes

combined = pd.concat([cohort_1, cohort_2, cohort_3, cohort_4, cohort_5, cohort_6])

combined['zip_code'] = combined['zip_code'].astype('str')
combined['zip_code'] = combined['zip_code'].str.replace('\"','')
# save
combined.to_feather(inpath + "warning_letters_combined.feather")











