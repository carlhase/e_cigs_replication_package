# -*- coding: utf-8 -*-
"""
Created on Thu Jun 27 16:03:02 2024

@author: cahase
"""

import numpy as np
import pandas as pd
from fuzzywuzzy import fuzz
import glob
import os
import re
import unicodedata
import time
import pyarrow as pa


store_path = 'G:/My Drive/GSEFM/Research/e_cigarettes/data/raw_data/LS_otter/'
letter_path = 'G:/My Drive/GSEFM/Research/e_cigarettes/warning_letters/'

processed_path = 'G:/My Drive/GSEFM/Research/e_cigarettes/data/processed_data/LS_Otter/'

# Ensure the destination directory exists
os.makedirs(processed_path, exist_ok=True)


#stores_old = pd.read_csv(store_path + 'Convenience_Retail_Store_Information-0.csv')
#stores = pd.read_csv(store_path + 'PDI_Stores-01.csv')
#store_status = pd.read_csv(store_path + 'Convenience_Retail_Store_Status-0.csv')

stores = pd.read_csv("D:/convenience_store/data/raw/LS_Otter/STORE_STATUS NEW/STORE_STATUS_NEW-0.csv", dtype = {'ZIP_CODE': 'str'}) # important to specify dtype = str, otherwise leading zeros will be dropped

stores_cols = list(stores)

letters = pd.read_feather(letter_path + 'warning_letters_combined.feather')

letters_cols = list(letters)

# rename columns for Letters
letters.rename(columns = 
               {'retailer_type': 'store_type',
                'address': 'address_fda',
                'firm': 'store_name_fda'},
               inplace = True)


online = ['Online', 'online']

# drop online stores
letters = letters.loc[~letters['store_type'].isin(online)].copy()

letters.dtypes
stores.dtypes

str_cols = ['store_name_fda', 'city', 'state']
letters[str_cols] = letters[str_cols].astype('str')

stores.columns = stores.columns.str.lower()

str_cols = ['street_address', 'city', 'state']
stores[str_cols] = stores[str_cols].astype('str')

letters['address_fda'] = letters['address_fda'].str.strip()
letters['city'] = letters['city'].str.strip()
letters['state'] = letters['state'].str.strip()

# lower case
#stores.columns = stores.columns.str.lower()
stores['street_address'] = stores['street_address'].str.strip()
stores['city'] = stores['city'].str.strip()
stores['state'] = stores['state'].str.strip()

stores.rename(columns = {'street_address': 'address_pdi'}, inplace = True)

# remove digits from zip code after hyphenation
stores['zip_code'] = stores['zip_code'].str.replace(r'-.*', '')

# remove " from letters zip code
letters['zip_code'] = letters['zip_code'].str.replace('\"', '')

zips = pd.DataFrame(stores['zip_code'].unique())

#%% merge fda store info to pdi store info
stores.dtypes
letters.dtypes

# first, merge on zip - there may be more than one letter per zip, so duplicates may arise. That is fine for now.
merged = pd.merge(stores, letters,
                  how = 'left',
                  on = 'zip_code',
                  indicator = True
                  )

matched_zips = merged.loc[merged['_merge'] == 'both'].copy()
matched_zips = matched_zips.loc[matched_zips['store_type'] != 'online'].copy()

cols = list(matched_zips)

cols_to_drop = ['city_x', 'city_y', 'state_x', 'state_y', '_merge']
matched_zips.drop(columns = cols_to_drop, inplace = True)

compare = matched_zips[['address_pdi', 'address_fda', 'store_id', 'store_name', 'store_chain_name', 'store_name_fda', 'insp_date', 'issue_date', 'zip_code']].copy()

#%% define functions for cleaning addresses

# Basic cleaning: remove accents, special characters, extra spaces
def basic_clean_address(text):
    if pd.isnull(text):
        return None
    text = unicodedata.normalize('NFKD', text).encode('ascii', 'ignore').decode('ascii')
    text = re.sub(r'[^\w\s,]', '', text)  # keep only words, spaces, commas
    text = re.sub(r'\s+', ' ', text)  # collapse multiple spaces
    return text.strip()

# Expand abbreviations (only for address/street name)
def expand_street_abbreviations(text):
    if pd.isnull(text):
        return None
    abbrev_mapping = {
        r'\bStreet\b': 'St',
        r'\bSt.\b': 'St',
        r'\bAvenue\b': 'Ave',
        r'\bAve.\b': 'Ave',
        r'\bBoulevard\b': 'Blvd',
        r'\bBlvd.\b': 'Blvd',
        r'\bRoad\b': 'Rd',
        r'\bRd.\b': 'Rd',
        r'\bDrive\b': 'Dr',
        r'\bDr.\b': 'Dr',
        r'\bLn\b': 'Lane',
        r'\bLn.\b': 'Lane',
        r'\bPl\b': 'Place',
        r'\bPl.\b': 'Place',
        r'\bCt\b': 'Court',
        r'\bCt.\b': 'Court',
        r'\bHighway\b': 'Hwy',
        r'\bsuite\b.*': '',
        r'\bSuite\b.*': '',
        r'\bste\b': '',
        r'\bSTE\b': '',
        r'\,': '',
        r'\bNortheast\b': 'NE',
        r'\bNorthwest\b': 'NW',
        r'\bSoutheast\b': 'SE',
        r'\bSouthwest\b': 'SW',
        r'\bNorth east\b': 'NE',
        r'\bNorth west\b': 'NW',
        r'\bSouth east\b': 'SE',
        r'\bSouth west\b': 'SW',
        r'\bNorth\b': 'N',
        r'\bSouth\b': 'S',
        r'\bEast\b': 'E',
        r'\bWest\b': 'W',
        r'\bN(?:\.)?\b': 'N',
        r'\bS(?:\.)?\b': 'S',
        r'\bE(?:\.)?\b': 'E',
        r'\bW(?:\.)?\b': 'W',
        r'\bNE(?:\.)?\b': 'NE',
        r'\bNW(?:\.)?\b': 'NW',
        r'\bSE(?:\.)?\b': 'SE',
        r'\bSW(?:\.)?\b': 'SW',
        r'\bN E(?:\.)?\b': 'NE',
        r'\bN W(?:\.)?\b': 'NW',
        r'\bS E(?:\.)?\b': 'SE',
        r'\bS W(?:\.)?\b': 'SW',

    }
    
    for abbrev, full in abbrev_mapping.items():
        text = re.sub(abbrev, full, text, flags=re.IGNORECASE)
    
    return text

# # replace N. with N, S.E. with SE
# def normalize_direction_abbreviations(text):
#     if pd.isnull(text):
#         return None

#     # Replace things like "S.E." or "N.W." (with or without spaces) → "SE", "NW"
#     text = re.sub(r'\b([NSEW])[\.\s]*([NSEW])[\.\s]*\b', r'\1\2', text, flags=re.IGNORECASE)

#     # Replace single-letter abbreviations with a period (e.g., "N.") → "N"
#     text = re.sub(r'\b([NSEW])\.(?!\w)', r'\1', text, flags=re.IGNORECASE)

#     return text


#%% apply clean and build

# drop all text after the word 'suite'
compare['address_pdi'] = compare['address_pdi'].str.replace(r'\bsuite\b.*', '', case=False, regex=True).str.strip()
compare['address_pdi'] = compare['address_pdi'].str.replace(r'\bSTE\b.*', '', case=False, regex=True).str.strip()
compare['address_fda'] = compare['address_fda'].str.replace(r'\bsuite\b.*', '', case=False, regex=True).str.strip()
compare['address_fda'] = compare['address_fda'].str.replace(r'\bSTE\b.*', '', case=False, regex=True).str.strip()

# apply functions
compare['address_pdi2'] = compare['address_pdi'].apply(basic_clean_address)
compare['address_pdi2'] = compare['address_pdi'].apply(expand_street_abbreviations)
#compare['address_pdi2'] = compare['address_pdi'].apply(normalize_direction_abbreviations)

compare['address_fda2'] = compare['address_fda'].apply(basic_clean_address)
compare['address_fda2'] = compare['address_fda'].apply(expand_street_abbreviations)
#compare['address_fda2'] = compare['address_fda'].apply(normalize_direction_abbreviations)

foo = compare[['address_pdi', 'address_pdi2', 'address_fda', 'address_fda2']].copy()
del foo

# rename and drop old address columns
compare['address_fda'] = compare['address_fda2']
compare['address_pdi'] = compare['address_pdi2']
compare.drop(columns = ['address_pdi2', 'address_fda2'], inplace = True)

#%%
# Function to calculate similarity score
def similarity_score(s1, s2):
    return fuzz.token_sort_ratio(s1, s2)

compare = compare.copy()
# Apply similarity score function to each pair of addresses
compare['SimilarityScore'] = compare.apply(lambda x: similarity_score(x['address_pdi'], x['address_fda']), axis=1)

compare.reset_index(inplace = True, drop = True)

compare.rename(columns = {'store_name': 'store_name_pdi',
                          'store_chain_name': 'chain_name_pdi'},
               inplace = True)

compare.dtypes
#%% save 
compare.to_excel(processed_path + 'store_letter_match_by_zip_may25.xlsx', index = False)

#%% subset medium to high matches

med_to_high_matches = compare.loc[compare['SimilarityScore'] >= 50].copy().sort_values(by = 'SimilarityScore', ascending = False)

# save and fill in by hand
#med_to_high_matches.to_excel(processed_path + 'med_to_high_matches_may25.xlsx', index = False)

'''
Next: Before proceeding with code below, first hand-match in excel, save as csv in excel. Then read in hand-matched csv and proceed with code below.
'''

#%% read in dataframe with hand-matched indicators, subset and save

matches = pd.read_excel(processed_path + 'med_to_high_matches_may25.xlsx', dtype = {'zip_code': 'str'})

matches = matches.loc[matches['matching'] == 1].copy() # 113

# drop unnecessary columns
matches.drop(columns = ['matching', 'SimilarityScore'], inplace = True)

matches.dtypes
matches['zip_code'] = matches['zip_code'].astype('str')

# save as feather
matches.to_feather(processed_path + 'targeted_stores_treatment.feather')

'''
Note: there may be some duplicates since some PDI stores changed ownership and hence got a 
new store_id. This means that a single FDA letter can be matched to multipl store_id values that
share a single address (since store_id is a static variable). This is not problematic, since later on when I
create the estimation sample, one of those store_ids wwill not have scanner data for the time frame where the
letter is issued.
'''

#%% dataframe with matching indicator (superfluous)
matching_store_ids = matches['store_id'].unique().tolist()

stores['warning_letter'] = np.where(stores['store_id'].isin(matching_store_ids), 1, 0)

stores['warning_letter'].unique()

check = stores.loc[stores['warning_letter'] == 1].copy()
del check

#%% save

stores.to_feather(processed_path + 'pdi_fda_linked.feather')


#%%






