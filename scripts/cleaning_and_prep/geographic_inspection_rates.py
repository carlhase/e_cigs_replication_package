# -*- coding: utf-8 -*-
"""
Created on Fri Mar 28 12:57:52 2025

@author: cahase
"""
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import geopandas as gpd


#%% inspections by state

inpath = "G:/My Drive/GSEFM/Research/e_cigarettes/FDA_inspections/"

all_insp = pd.read_csv(inpath + "all_fda_inspections_combined.csv", dtype = {'zip_code': 'str'}) # important to include str dtype when reading in


head = all_insp.head(10000)

#all_insp['insp_date'] = np.where(all_insp['insp_date'] == 'Not available', np.nan(), all_insp['insp_date'])

all_insp['issue_date'].dtype
all_insp['issue_date'] = pd.to_datetime(all_insp['issue_date']).dt.to_period('M')

all_insp['year'] = all_insp['issue_date'].astype('str').str.split('-', expand = True)[0].astype('int')

# subset 2021 through 2022
pre_period = all_insp.loc[all_insp['year'].isin([2021, 2022])].copy()

# inspections per state during pre_period
state_insp = (
    pre_period
    .groupby(['state'])
    .size()
    .reset_index(name = 'insp')
    )


# inspections per zip
zip_insp = (
    pre_period
    .groupby(['zip_code'])
    .size()
    .reset_index(name = 'insp')
    )



#%% population data
pop = pd.read_excel('G:/My Drive/GSEFM/Research/e_cigarettes/data/raw_data/NST-EST2024-POP.xlsx')

cols = list(pop)
pop2 = pop.iloc[:, [0,4]].copy()

# drop unnecessary rows at top of df
pop = pop2.drop(pop2.index[0:8]).reset_index()

pop = pop.loc[~((pop['index'] == 59) | (pop['index'].isin(range(61, 68))))]

pop.drop(columns = 'index', inplace = True)        

# rename columns
pop.columns = ['state', 'population']

pop['state'] = pop['state'].str.replace('.', '')


states = pop['state'].unique().tolist()
# state abbrevations
abbrevs = ['AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'DC', 'FL', 'GA', 'HI', 'ID', 'IL', 'IN',
           'IA', 'KS', 'KY', 'LA', 'ME', 'MD', 'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ',
           'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 'SD', 'TN', 'TX', 'UT', 'VT',
           'VA', 'WA', 'WV', 'WI', 'WY', 'PR']

# Create a dictionary by zipping the two lists
abbrev_dict = dict(zip(states, abbrevs))

# Create a new column 'state2' by mapping the 'state' column values to the dictionary
pop['state2'] = pop['state'].map(abbrev_dict)

#%% merge pop to inspection rates df


state_insp = pd.merge(state_insp, pop,
                    how = 'left',
                    left_on = 'state',
                    right_on = 'state2',
                    indicator = True)


state_insp = state_insp.loc[state_insp['_merge'] == 'both'].copy()


state_insp.drop(columns = ['state_x', 'state_y', '_merge'], inplace = True)

state_insp.rename(columns = {'state2': 'state'}, inplace = True)

state_insp['insp_per_capita'] = state_insp['insp']/state_insp['population']

state_insp.rename(columns = {'insp': 'state_insp'}, inplace = True)

#%% violations per inspection (proxy for strictness) state level

# state_insps = (
#     pre_period
#     .groupby('state')
#     .size()
#     .reset_index(name = 'inspections')
#     )

state_viols = (
    pre_period
    .loc[pre_period['outcome'] != 'No Violations Observed']
    .groupby('state')
    .size()
    .reset_index(name = 'state_viols')
    )

# merge
state_insp = pd.merge(state_insp, state_viols,
                           how = 'left',
                           on = 'state')


state_insp['state_viols'] = np.where(state_insp['state_viols'].isna(), 0, state_insp['state_viols'])
    
state_insp['viol_per_capita'] = state_insp['state_viols']/state_insp['population']
state_insp['state_viol_per_insp'] = state_insp['state_viols']/state_insp['state_insp']

#%% violations per inspection (proxy for strictness) zip code level

zip_insps = (
    pre_period
    .groupby('zip_code')
    .size()
    .reset_index(name = 'zip_insp')
    )

zip_viols = (
    pre_period
    .loc[pre_period['outcome'] != 'No Violations Observed']
    .groupby('zip_code')
    .size()
    .reset_index(name = 'zip_viols')
    )

# merge
zip_viol_rate = pd.merge(zip_insps, zip_viols,
                         how = 'left',
                         on = 'zip_code')


zip_viol_rate['violations'] = np.where(zip_viol_rate['zip_viols'].isna(), 0, zip_viol_rate['zip_viols'])
    
zip_viol_rate['zip_viol_per_insp'] = zip_viol_rate['zip_viols']/zip_viol_rate['zip_insp']


#%% FDA contracts per capita

contracts = pd.read_excel('G:/My Drive/GSEFM/Research/e_cigarettes/FDA_inspections/fda_contracting_amounts.xlsx')

contracts.columns = contracts.columns.str.lower()
# some states have multiple contracts
contracts['jurisdiction'].nunique()

contracts = (
    contracts
    .groupby('jurisdiction')
    .agg(recent_amt = ('most recent award amount', 'sum'),
         total_amt = ('total awarded to date*', 'sum'))
    .reset_index()
    )

contracts.rename(columns = {'jurisdiction': 'state'}, inplace = True)

# rename to match
contracts['state'] = np.where(contracts['state'] == 'Washington D.C.', 'District of Columbia', contracts['state'])

# merge to population
contracts = pd.merge(contracts, pop,
                      on = 'state',
                      how = 'left',
                      indicator = True)

contracts.drop(columns = '_merge', inplace = True)

# per capita columns
contracts['recent_amt_per_capita'] = contracts['recent_amt']/contracts['population']
contracts['total_amt_per_capita'] = contracts['total_amt']/contracts['population']

#%% FDA contracts, all contracts (including multiple contracts per state)

all_contracts = pd.read_excel('G:/My Drive/GSEFM/Research/e_cigarettes/misc/fda_retail_inspection_contracts.xlsx')

head = all_contracts.head()

all_contracts.columns = all_contracts.columns.str.lower()
# some states have multiple contracts
all_contracts['jurisdiction'].nunique()

all_contracts2 = (
    all_contracts
    .groupby('jurisdiction')
    .agg(recent_amt = ('most recent award amount', 'sum'),
         total_amt = ('total awarded to date*', 'sum'))
    .reset_index()
    )

all_contracts = all_contracts2.copy()

all_contracts.rename(columns = {'jurisdiction': 'state'}, inplace = True)

# rename to match
all_contracts['state'] = np.where(all_contracts['state'] == 'Washington D.C.', 'District of Columbia', all_contracts['state'])

to_drop = ['U.S. Virgin Islands', 'Totals', 'Shoshone-Bannock Tribes', 'Seminole Tribe of Florida', 'Rincon Band of Luiseno Indians', 
           'Northern Mariana Islands', 'Mescalero Apache Tribe', 'Guam', 'American Samoa', 'Puerto Rico']

all_contracts = all_contracts.loc[~all_contracts['state'].isin(to_drop)].copy()

# merge to population
all_contracts = pd.merge(all_contracts, pop,
                      on = 'state',
                      how = 'left',
                      indicator = True)

all_contracts.drop(columns = ['_merge', 'state'], inplace = True)

all_contracts.rename(columns = {'state2': 'state'}, inplace = True)

all_contracts.rename(columns = {'recent_amt': 'recent_amt_all',
                                'total_amt': 'total_amt_all'}, inplace = True)
# per capita columns
all_contracts['recent_amt_per_capita_all'] = all_contracts['recent_amt_all']/all_contracts['population']
all_contracts['total_amt_per_capita_all'] = all_contracts['total_amt_all']/all_contracts['population']



#%% add vape viols

vape_viols = pd.read_csv('G:/My Drive/GSEFM/Research/e_cigarettes/data/processed_data/e_cig_letters_by_state.csv')

vape_viols.rename(columns = {'letters': 'vape_viols'}, inplace = True)

state_insp = pd.read_csv('G:/My Drive/GSEFM/Research/e_cigarettes/data/processed_data/state_insp_and_viol_rates.csv')

# merge
state_insp = pd.merge(state_insp, vape_viols,
                      how = 'left',
                      on = 'state',
                      indicator = True
                      )


nomerge = state_insp.loc[state_insp['_merge'] != 'both'].copy()
del nomerge
state_insp.drop(columns = '_merge', inplace = True)

#%% per capita -> per 100k
state_insp['vape_viols_per_100k'] = (state_insp['vape_viols']/state_insp['population'])*100000
state_insp['insp_per_100k'] = state_insp['insp_per_capita']*100000
state_insp['viol_per_100k'] = state_insp['viol_per_capita']*100000
state_insp['viol_per_100_insp'] = state_insp['state_viol_per_insp']*100

#%% save dataframes

state_insp.to_csv('G:/My Drive/GSEFM/Research/e_cigarettes/data/processed_data/state_insp_and_viol_rates.csv', index = False)
zip_viol_rate.to_csv('G:/My Drive/GSEFM/Research/e_cigarettes/data/processed_data/zip_viol_rate.csv', index = False)
contracts.to_csv('G:/My Drive/GSEFM/Research/e_cigarettes/data/processed_data/fda_state_inspection_contracts.csv', index = False)
all_contracts.to_csv('G:/My Drive/GSEFM/Research/e_cigarettes/data/processed_data/fda_state_inspection_all_contracts.csv', index = False)


#%% Urban vs rural share of enforcement activity

# rural urban commuting area
ruca = pd.read_csv('G:/My Drive/GSEFM/Research/e_cigarettes/data/raw_data/RUCA2010zipcode.csv')

ruca.columns = ruca.columns.str.lower()

ruca.rename(columns = {'\'\'zip_code\'\'': 'zip_code'}, inplace = True)

# drop PO boxes
ruca['zip_type'].unique()

ruca = ruca.loc[ruca['zip_type'] == 'Zip Code Area'].copy()

ruca.dtypes
# remove double quotes 
ruca['zip_code'] = ruca['zip_code'].astype('str').str.replace('\'', '')


ruca.rename(columns = {'state': 'state_ruca'}, inplace = True)

# merge to estimation panel
ruca.dtypes
all_insp['zip_code'].dtype

ruca['zip_code'] = ruca['zip_code'].astype('str')
all_insp['zip_code'] = all_insp['zip_code'].astype('str')

all_insp2 = pd.merge(all_insp, ruca,
                    how = 'left',
                    on = 'zip_code',
                    indicator = True)

nomerge = all_insp2.loc[all_insp2['_merge'] != 'both'].copy()
del nomerge
all_insp2.drop(columns = '_merge', inplace = True)

all_insp = all_insp2.copy()

#%% alternative urban data
# Load ZCTA shapefile (ZIP Code boundaries)
zcta_file = 'G:/My Drive/GSEFM/Research/e_cigarettes/data/raw_data/tl_2024_us_zcta520.zip'
zcta = gpd.read_file(zcta_file)  # GeoPandas reads the .shp inside the zip :contentReference[oaicite:1]{index=1}

# Load Urban Area shapefile
urban_file = 'G:/My Drive/GSEFM/Research/e_cigarettes/data/raw_data/tl_2024_us_uac20.zip'
urban_areas = gpd.read_file(urban_file)

# Ensure Matching CRS (Coordinate Reference System)
zcta = zcta.to_crs(urban_areas.crs)

# Spatial Join to Check for Overlap
# Find ZCTAs that intersect with urban areas
zcta_with_urban = gpd.sjoin(zcta, urban_areas, how='left', predicate='intersects')

# Create indicator: 1 if intersected with an urban area, else 0
zcta_with_urban['urban_overlap'] = zcta_with_urban['index_right'].notna().astype(int)

cols = list(zcta_with_urban)
zcta_with_urban.rename(columns={'ZCTA5CE20': 'zip_code'}, inplace = True)

# Keep only the columns you need
zcta_indicator = zcta_with_urban[['zip_code', 'urban_overlap']].copy()

all_insp['zip_code'].dtype
zcta_indicator['zip_code'].dtype

all_insp['zip_code'] = all_insp['zip_code'].astype('str')
zcta_indicator['zip_code'] = zcta_indicator['zip_code'].astype('str')

all_insp['zip_code'].nunique()

duplicated_df = zcta_indicator.loc[zcta_indicator.duplicated(subset = 'zip_code', keep = False)]
del duplicated_df

# drop duplicate zips
zcta_indicator.drop_duplicates(subset = 'zip_code', inplace = True)
zcta_indicator['zip_code'].nunique()

# Merge into your original DataFrame
all_insp2 = all_insp.merge(zcta_indicator, on='zip_code', how='left', indicator = True)

nomerge = all_insp2.loc[all_insp2['_merge'] != 'both'].copy()
del nomerge
all_insp2.drop(columns = '_merge', inplace = True)

all_insp = all_insp2.copy()

# Fill missing values with 0 (for ZIPs not in shapefile)
all_insp['urban_overlap'] = all_insp['urban_overlap'].fillna(0).astype(int)

all_insp['urban_overlap'].describe() # 84 percent of inspections are in urban areas

# violations
head = all_insp.head()
all_insp['year'].unique()
types = all_insp['outcome'].unique()

# subset 2021 through 2022
pre_period = all_insp.loc[all_insp['year'].isin([2021, 2022])].copy()

insp = pre_period['urban_overlap'].describe() # 83% of FDA inspections were in urban zips (proportional to US population patterns, ie 82% live in urban areas)
viols = pre_period.loc[pre_period['outcome'] != 'No Violations Observed']['urban_overlap'].describe() # 88.5% viols were in urban zips

###############################################################################
#%% PLOTS

insp = pd.read_csv('G:/My Drive/GSEFM/Research/e_cigarettes/data/processed_data/state_insp_and_viol_rates.csv')

lt_10 = insp.loc[insp['viol_per_100_insp'] <= 10].copy()

#%% read in dataframes (if necessary)

state_insp_per_year = pd.read_csv('G:/My Drive/GSEFM/Research/e_cigarettes/data/processed_data/state_insp_and_viol_rates.csv')
state_viol_rate = pd.read_csv('G:/My Drive/GSEFM/Research/e_cigarettes/data/processed_data/state_viol_rate.csv')
contracts = pd.read_csv('G:/My Drive/GSEFM/Research/e_cigarettes/data/processed_data/fda_state_inspection_contracts.csv')

states = state_viol_rate['state'].unique().tolist()
states_to_drop = ['AS', 'PR', 'GU']

state_viol_rate = state_viol_rate.loc[~state_viol_rate['state'].isin(states_to_drop)]
state_insp_per_year = state_insp_per_year.loc[~state_insp_per_year['state'].isin(states_to_drop)]


#%% Inspections per capita

state_insp_per_year['insp_per_k'] = state_insp_per_year['insp_per_capita']*100000
state_insp_per_year['insp_per_k'].describe()

# Sort DataFrame by 'Value' in ascending order
state_insp_per_year = state_insp_per_year.sort_values(by='insp_per_k')

# Define which states should have red dots
highlight_states = {"FL", "NJ", "NV", "LA", "MI", "PA", "NY"}  # Replace with your desired states

# Create color list: red for highlighted states, black otherwise
colors = ['red' if state in highlight_states else 'black' for state in state_insp_per_year['state']]

# Create scatter plot
plt.figure(figsize=(6, 10))
plt.scatter(state_insp_per_year['insp_per_k'], state_insp_per_year['state'], color='black', marker='o')

# Labels and title
plt.xlabel('Inspections per 100,000 inhabitants')
plt.ylabel('State')
#plt.title('Scatter Plot with Y-axis Labels from Column 1')

# Adjust the layout for better visibility
plt.tight_layout()

# Show plot
plt.show()

# Save the figure as a PNG file
plt.savefig('G:/My Drive/GSEFM/Research/e_cigarettes/output/state_insp_per_100k_inhabitants.png', format='png', dpi=300)


#%% Inspections per year

# Sort DataFrame by 'Value' in ascending order
state_insp_per_year = state_insp_per_year.sort_values(by='state_insp_per_year')

# Create scatter plot
plt.figure(figsize=(6, 10))
plt.scatter(state_insp_per_year['state_insp_per_year'], state_insp_per_year['state'], color='black', marker='o')

# Labels and title
plt.xlabel('Inspections per year')
plt.ylabel('State')
#plt.title('Scatter Plot with Y-axis Labels from Column 1')

# Adjust the layout for better visibility
plt.tight_layout()

# Save the figure as a PNG file
plt.savefig('G:/My Drive/GSEFM/Research/e_cigarettes/output/state_insp_per_year.png', format='png', dpi=300)

# Show plot
plt.show()

#%% Inspections overall

# Sort DataFrame by 'Value' in ascending order
state_viol_rate = state_viol_rate.sort_values(by='inspections')

# Create scatter plot
plt.figure(figsize=(6, 10))
plt.scatter(state_viol_rate['inspections'], state_viol_rate['state'], color='black', marker='o')

# Labels and title
plt.xlabel('Inspections')
plt.ylabel('State')
#plt.title('Scatter Plot with Y-axis Labels from Column 1')

# Adjust the layout for better visibility
plt.tight_layout()

# Save the figure as a PNG file
plt.savefig('G:/My Drive/GSEFM/Research/e_cigarettes/output/state_insp.png', format='png', dpi=300)

# Show plot
plt.show()

#%% violations

# Sort DataFrame by 'Value' in ascending order
state_viol_rate = state_viol_rate.sort_values(by='violations')

# Create scatter plot
plt.figure(figsize=(6, 10))
plt.scatter(state_viol_rate['violations'], state_viol_rate['state'], color='black', marker='o')

# Labels and title
plt.xlabel('Violations')
plt.ylabel('State')
#plt.title('Scatter Plot with Y-axis Labels from Column 1')

# Adjust the layout for better visibility
plt.tight_layout()

# Save the figure as a PNG file
plt.savefig('G:/My Drive/GSEFM/Research/e_cigarettes/output/state_viols.png', format='png', dpi=300)

# Show plot
plt.show()

#%% violations per capita

viol_per_capita = pd.merge(state_insp_per_year, state_viol_rate,
                  how = 'left',
                  on = 'state')

viol_per_capita['viol_per_capita'] = (viol_per_capita['violations']/viol_per_capita['population'])*100000

# Plot
# Sort DataFrame by 'Value' in ascending order
viol_per_capita = viol_per_capita.sort_values(by='viol_per_capita')

# Create scatter plot
plt.figure(figsize=(6, 10))
plt.scatter(viol_per_capita['viol_per_capita'], viol_per_capita['state'], color='black', marker='o')

# Labels and title
plt.xlabel('Violations per 100,000 inhabitants')
plt.ylabel('State')
#plt.title('Scatter Plot with Y-axis Labels from Column 1')

# Adjust the layout for better visibility
plt.tight_layout()

# Save the figure as a PNG file
plt.savefig('G:/My Drive/GSEFM/Research/e_cigarettes/output/state_viol_per_100k_inhabitants.png', format='png', dpi=300)

# Show plot
plt.show()


#%% violation rate (violations per inspection)

# Sort DataFrame by 'Value' in ascending order
state_viol_rate['viol_per_100'] = state_viol_rate['state_viol_rate']*100

state_viol_rate = state_viol_rate.sort_values(by='viol_per_100')

# Create scatter plot
plt.figure(figsize=(6, 10))
plt.scatter(state_viol_rate['viol_per_100'], state_viol_rate['state'], color='black', marker='o')

# Labels and title
plt.xlabel('Violations per 100 inspections')
plt.ylabel('State')
#plt.title('Scatter Plot with Y-axis Labels from Column 1')

# Adjust the layout for better visibility
plt.tight_layout()

# Save the figure as a PNG file
plt.savefig('G:/My Drive/GSEFM/Research/e_cigarettes/output/state_viol_per_100_insp.png', format='png', dpi=300)

# Show plot
plt.show()

#%% fda contracts - recent amount per capita

contracts.drop(columns = 'state', inplace = True)
contracts.rename(columns = {'state2': 'state'}, inplace = True)

# Sort DataFrame by 'Value' in ascending order
contracts = contracts.sort_values(by='recent_amt_per_capita')

# Create scatter plot
plt.figure(figsize=(6, 10))
plt.scatter(contracts['recent_amt_per_capita'], contracts['state'], color='black', marker='o')

# Labels and title
plt.xlabel('Dollars per person')
plt.ylabel('State')
#plt.title('Scatter Plot with Y-axis Labels from Column 1')

# Adjust the layout for better visibility
plt.tight_layout()

# Save the figure as a PNG file
plt.savefig('G:/My Drive/GSEFM/Research/e_cigarettes/output/fda_state_recent_amt_per_capita.png', format='png', dpi=300)

# Show plot
plt.show()

#%% fda contracts - total amount per capita

# Sort DataFrame by 'Value' in ascending order
contracts = contracts.sort_values(by='total_amt_per_capita')

# Create scatter plot
plt.figure(figsize=(6, 10))
plt.scatter(contracts['total_amt_per_capita'], contracts['state'], color='black', marker='o')

# Labels and title
plt.xlabel('Dollars per person')
plt.ylabel('State')
#plt.title('Scatter Plot with Y-axis Labels from Column 1')

# Adjust the layout for better visibility
plt.tight_layout()

# Save the figure as a PNG file
plt.savefig('G:/My Drive/GSEFM/Research/e_cigarettes/output/fda_state_total_amt_per_capita.png', format='png', dpi=300)

# Show plot
plt.show()


