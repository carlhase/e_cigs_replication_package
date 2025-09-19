# -*- coding: utf-8 -*-
"""
Created on Thu Jun 27 16:03:02 2024

@author: cahase
"""

import numpy as np
import pandas as pd
import gc
from fuzzywuzzy import fuzz
import glob
import os
import re


stores = pd.read_csv("D:/convenience_store/data/raw/LS_Otter/STORES NEW/STORES_NEW-0.csv", dtype = {'ZIP_CODE': 'str'})

stores_cols = list(stores)

stores.columns = stores.columns.str.lower()

inspections = pd.read_csv('G:/My Drive/GSEFM/Research/e_cigarettes/FDA_inspections/all_fda_inspections_combined.csv', dtype = {'zip_code': 'str'})

inspections.dtypes

inspections_cols = list(inspections)

# rename columns for Letters
inspections.rename(columns = 
               {'address': 'address_fda',
                'firm': 'store_name_fda'},
               inplace = True)


inspections['outcome'].unique()

outcome_types = inspections['outcome'].value_counts().reset_index()

# drop stores with no violation observed
#inspections = inspections.loc[inspections['outcome'] != 'No Violations Observed'].copy() # 67k violations

inspections.dtypes
stores.dtypes

# prep for matching
str_cols = ['store_name_fda', 'city', 'state']
inspections[str_cols] = inspections[str_cols].astype('str')

str_cols = ['street_address', 'city', 'state']
stores[str_cols] = stores[str_cols].astype('str')

inspections['address_fda'] = inspections['address_fda'].str.strip()
inspections['city'] = inspections['city'].str.strip()
inspections['state'] = inspections['state'].str.strip()

stores['street_address'] = stores['street_address'].str.strip()
stores['city'] = stores['city'].str.strip()
stores['state'] = stores['state'].str.strip()

stores.rename(columns = {'street_address': 'address_pdi'}, inplace = True)

# remove digits from zip code after hyphenation
stores['zip_code'] = stores['zip_code'].str.replace(r'-.*', '')

# remove " from letters zip code
inspections['zip_code'] = inspections['zip_code'].str.replace('\'', '')

zips = pd.DataFrame(stores['zip_code'].unique())

head = inspections.head()

inspection_cols = list(inspections)

unique_fda_addresses = inspections.drop_duplicates(subset = ['store_name_fda', 'address_fda'])

# LEFT OFF HERE.
'''
Next: merge unique fda addresses to pdi dataframe, then do fuzzy wuzzy. After that,
create artificial balanced panel of that matched dataframe. then merge inspection info
to that panel (since some stores may be inspected more than once.)
'''
#%% merge fda store info to pdi store info
stores.dtypes
inspections.dtypes

# first, merge on zip
merged = pd.merge(stores, inspections,
                  how = 'left',
                  on = 'zip_code',
                  indicator = True
                  )

matched_zips = merged.loc[merged['_merge'] == 'both'].copy()

cols = list(matched_zips)

cols_to_drop = ['city_x', 'city_y', 'state_x', 'state_y', '_merge']
matched_zips.drop(columns = cols_to_drop, inplace = True)

compare = matched_zips[['address_pdi', 'address_fda', 'store_id', 'store_name', 'store_chain_name', 'store_name_fda', 
                        'insp_date', 'issue_date', 'outcome', 'product type', 'nicotine source', 'zip_code']].copy()

#%% prep for fuzzywuzzy (VERSION 1)

# # addresses lower case
# compare['address_pdi'] = compare['address_pdi'].str.lower()
# compare['address_fda'] = compare['address_fda'].str.lower()

# compare_backup = compare.copy()
# #compare = compare_backup.copy()

# replacements = {
#     'road': 'rd', 
#     'street': 'st',
#     'parkway': 'pkwy',
#     'avenue': 'ave',
#     'highway': 'hwy',
#     'boulevard': 'blvd',
#     'drive': 'dr',
#     'ste': 'suite',
#     'northwest': 'nw',
#     'north west': 'nw',
#     'north': 'n',
#     'north east': 'ne',
#     'northeast': 'ne',
#     'east': 'e',
#     'southeast': 'se',
#     'south east': 'se',
#     'south': 's',
#     'southwest': 'sw',
#     'south west': 'sw',
#     'west': 'w',
#     'n\.w\.': 'nw', # remove periods from abbreviations
#     'n\.': 'n', 
#     'n\.e\.': 'ne',
#     'e\.': 'e',
#     's\.e\.': 'se',
#     's\.': 's',
#     's\.w\.': 'sw',
#     'w\.': 'w',
#     'u\.s\.': 'us',
#     '-': ' '
#     }


# # Replace using str.replace with regex=True
# compare['address_fda'] = compare['address_fda'].str.replace(
#     r'(?i)\b(?:' + '|'.join(re.escape(k) for k in replacements.keys()) + r')\b',  # Escape special characters
#     lambda match: replacements.get(match.group(0).lower(), match.group(0)),  # Avoid KeyError
#     regex=True
# )


# # Replace using str.replace with regex=True
# compare['address_pdi'] = compare['address_pdi'].str.replace(
#     r'(?i)\b(?:' + '|'.join(re.escape(k) for k in replacements.keys()) + r')\b',  # Escape special characters
#     lambda match: replacements.get(match.group(0).lower(), match.group(0)),  # Avoid KeyError
#     regex=True
# )

#%% prep for fuzzywuzzy (VERSION 2)
# addresses lower case
compare['address_pdi'] = compare['address_pdi'].str.lower()
compare['address_fda'] = compare['address_fda'].str.lower()

compare_backup = compare.copy()
#compare = compare_backup.copy()

# --- 1) Replacement map (literal keys; no regex escapes here) ---
replacements = {
    'road': 'rd',
    'street': 'st',
    'parkway': 'pkwy',
    'avenue': 'ave',
    'highway': 'hwy',
    'boulevard': 'blvd',
    'drive': 'dr',
    'ste': 'suite',
    'northwest': 'nw',
    'north west': 'nw',
    'north': 'n',
    'north east': 'ne',
    'northeast': 'ne',
    'east': 'e',
    'southeast': 'se',
    'south east': 'se',
    'south': 's',
    'southwest': 'sw',
    'south west': 'sw',
    'west': 'w',
    'n.w.': 'nw',
    'n.': 'n',
    'n.e.': 'ne',
    'e.': 'e',
    's.e.': 'se',
    's.': 's',
    's.w.': 'sw',
    'w.': 'w',
    'u.s.': 'us',
    'st.': 'st',  # treat as "street"
}

# --- 2) Helpers to build and apply the pattern ---
def _build_main_pattern(keys):
    """Compile an alternation of keys, longest-first, with lookarounds to avoid partial-word hits."""
    keys = sorted(keys, key=len, reverse=True)
    pattern = r"(?i)(?<!\w)(?:" + "|".join(re.escape(k) for k in keys) + r")(?!\w)"
    return re.compile(pattern, flags=re.IGNORECASE)

# Pre-pass to collapse dotted compass pairs like "n.e." / "n . e ." -> "ne"
_PAIR_PAT = re.compile(r"(?i)\b([nsew])\s*\.\s*([nsew])\s*\.\b")

def _collapse_compass_pairs(s: str) -> str:
    return _PAIR_PAT.sub(lambda m: m.group(1) + m.group(2), s)

def normalize_address_series(s, repl_map):
    """Lowercase, clean punctuation/whitespace, collapse dotted compass pairs, and apply replacements."""
    # Lowercase + simple punctuation/spacing harmonization
    out = (
        s.str.lower()
         .str.replace("-", " ", regex=False)          # turn hyphens into spaces
         .str.replace(r"\s+", " ", regex=True)        # squeeze whitespace
         .str.strip()
    )
    # Collapse dotted compass pairs
    out = out.apply(_collapse_compass_pairs)

    # Main replacements (longest-first literal alternation)
    main_pat = _build_main_pattern(repl_map.keys())
    out = out.str.replace(
        main_pat,
        lambda m: repl_map.get(m.group(0).lower(), m.group(0)),
        regex=True,
    )
    # Optional: squeeze whitespace again (just in case)
    out = out.str.replace(r"\s+", " ", regex=True).str.strip()
    return out

# --- 3) Apply to your data ---
# assumes `compare['address_fda']` and `compare['address_pdi']` already exist
compare['address_fda'] = normalize_address_series(compare['address_fda'], replacements)
compare['address_pdi'] = normalize_address_series(compare['address_pdi'], replacements)


# # Replace using str.replace with regex=True
# compare['address_fda'] = compare['address_fda'].str.replace(
#     r'(?i)\b(?:' + '|'.join(re.escape(k) for k in replacements.keys()) + r')\b',  # Escape special characters
#     lambda match: replacements.get(match.group(0).lower(), match.group(0)),  # Avoid KeyError
#     regex=True
# )


# # Replace using str.replace with regex=True
# compare['address_pdi'] = compare['address_pdi'].str.replace(
#     r'(?i)\b(?:' + '|'.join(re.escape(k) for k in replacements.keys()) + r')\b',  # Escape special characters
#     lambda match: replacements.get(match.group(0).lower(), match.group(0)),  # Avoid KeyError
#     regex=True
# )

#%%
# Function to extract house number
def extract_house_number(address):
    match = re.match(r'^\d+', address)  # Match leading digits
    return match.group(0) if match else None

def similarity_score(s1, s2):
    score = fuzz.token_sort_ratio(s1, s2)

    house_num1 = extract_house_number(s1)
    house_num2 = extract_house_number(s2)

    if score > 70:  # Apply house number check only if similarity is already reasonable
        if house_num1 and house_num2:
            if house_num1 == house_num2:
                score += 15  # Boost if house numbers match
            # else:
            #     score -= 20  # Penalize if house numbers do not match <- performs worse!

    return max(0, min(score, 100))  # Keep score within 0-100

compare = compare.copy()

# Apply similarity score function to each pair of addresses
compare['SimilarityScore'] = compare.apply(lambda x: similarity_score(x['address_pdi'], x['address_fda']), axis=1)

compare.reset_index(inplace = True, drop = True)

compare.rename(columns = {'store_name': 'store_name_pdi',
                          'store_chain_name': 'chain_name_pdi',
                          'product type': 'product_type',
                          'nicotine source': 'nicotine_source'},
               inplace = True)

compare['zip_code'].dtype
#%% save 
compare.to_csv(processed_path + 'store_violation_match_by_zip_2022_thru_feb25.csv', dtype = {'zip_code': 'str'}, index = False)

#%% subset medium to high matches
'''
98 is a visual cutoff in terms of exact matches. Use this.
'''
ninetyseven = compare.loc[compare['SimilarityScore'] >= 97].copy().sort_values(by = 'SimilarityScore', ascending = True)

ninetyeight = compare.loc[compare['SimilarityScore'] >= 98].copy().sort_values(by = 'SimilarityScore', ascending = True)

ninetyeight.to_csv(processed_path + 'store_violation_match_by_zip_2022_thru_feb25_98threshold.csv', index = False)

#%% read in csv with threshold 98, merge to PDI store dataframe

matches = pd.read_csv(processed_path + 'store_violation_match_by_zip_2022_thru_feb25_98threshold.csv')

#matches = matches.loc[matches['matching'] == 1].copy() # 81

matching_store_ids = matches['store_id'].values.tolist()

# pdi stores dfs
stores_old = pd.read_csv(store_path + 'PDI_Stores-01.csv')
stores_new = pd.read_csv(store_path + 'Stores_Status-0.csv')

cols_old = list(stores_old)
cols_new = list(stores)

stores_old.columns = stores_old.columns.str.lower()

stores_old.drop(columns = 'warning_letter', inplace = True)

# foo1 = stores_old.loc[stores_old['STORE_ID'] == 34608].copy()
# foo2 = stores.loc[stores['STORE_ID'] == 34608].copy()

cols = list(stores)

stores['violation'] = np.where(stores['store_id'].isin(matching_store_ids), 1, 0)

stores['warning_letter'].unique()

check = stores.loc[stores['warning_letter'] == 1].copy()
del check

#%% save

stores.to_csv(processed_path + 'pdi_fda_linked.csv', index = False)

'''
Next: some addresses in the pdi data have more than one store id (e.g. due to change of ownership).
Edit targeted store info to account for duplicated addresses.
'''
#%% read in df with date and location of targeted stores that are also in pdi data
matches = pd.read_csv('G:/My Drive/GSEFM/Research/e_cigarettes/data/processed_data/med_to_high_matches_feb25.csv')

matches.drop(columns = 'SimilarityScore', inplace = True)

#%% get consolidated list of store IDs
'''
The matches dataframe has some addresses with more than one pdi store_id. In another script,
I consolidated the store IDs so that a each address has a single store ID. Using those unique store ID, subset merge1
for those IDs.
'''
source_dir = "D:/convenience_store/data/processed/indexes/subcat_indexes_otp_1b/"

all_files = glob.glob(source_dir + "*.feather") # 18214 files (previously 18940 files)

# Normalize to use the correct slash for the OS
all_files = [os.path.normpath(path) for path in all_files]

# Extract store numbers
store_ids = [os.path.basename(path).split('.')[0] for path in all_files]

# to integer type (store ids never have a leading zero)
store_ids = [int(x) for x in store_ids]

# subset stores in merge1 that are in consolidated id list
consolidated_matches = matches.loc[matches['store_id'].isin(store_ids)].copy() # 62

unique_addresses = consolidated_matches['address_pdi'].unique()
del unique_addresses

treatments = consolidated_matches.copy() 
treatments.drop(columns = 'matching', inplace = True)

outpath = 'G:/My Drive/GSEFM/Research/e_cigarettes/data/processed_data/'
treatments.to_csv(outpath + 'targeted_stores_treatment.csv', index = False)

#%%







