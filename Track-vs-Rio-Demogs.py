import pandas as pd
import os
from rapidfuzz import fuzz
from operator import itemgetter
import itertools
import re
from ordered_set import OrderedSet

os.chdir('M:\Rio vs Trak')

rio_full = pd.read_csv('RIO_Full.csv', parse_dates=True)
trak_full = pd.read_csv('Trak_Full.csv', parse_dates=True)

#replace NaNs with empty string to enable address joins
rio_full.fillna('', inplace=True)
trak_full.fillna('', inplace=True)

# build address join column in each dataframe and remove blank values
rio_full['AddressJoin'] = ((rio_full['Address1'].str.lower() + ', ' +  
                               rio_full['Address2'].str.lower() + ', ' + 
                               rio_full['Address3'].str.lower() + ', ' + 
                               rio_full['Address4'].str.lower() + ', ' + 
                               rio_full['PostCode'].str.lower()
                            .str.replace('zz99', ''))
                            .str.replace('channel islands', '', regex=True)
                            .str.replace('guernsey', '', regex=True)
                            .str.strip(',').str.strip()
                          )
rio_full['AddressJoin'] = rio_full['AddressJoin'].str.replace(' ,', '')


trak_full['AddressJoin'] = ((trak_full['Address1'].str.lower() + ', ' +  
                               trak_full['Address2'].str.lower() + ', ' + 
                               trak_full['PostCode'].str.lower())
                            .str.replace('channel islands', '', regex=True)
                            .str.replace('guernsey', '', regex=True)
                            .str.strip(',').str.strip()
                           )
trak_full['AddressJoin'] = rio_full['AddressJoin'].str.replace(' ,', '')

# merge into one df
merged_dfs = pd.concat([trak_full, rio_full], ignore_index=True, axis=0)

# remobve unvanyed address columns
merged_dfs.drop(['Address1', 'Address2', 'PostCode','Address3','Address4' ], axis=1, inplace=True)

# seperate records that appear in both trak and rio with those that don't
urns_in_trak_and_rio = merged_dfs[merged_dfs['URN'].map(merged_dfs['URN'].value_counts()) > 1]
not_urns_in_trak_and_rio = merged_dfs[merged_dfs['URN'].map(merged_dfs['URN'].value_counts()) == 1]

master_list = []

for group_name, group in urns_in_trak_and_rio.groupby('URN'):
    group_list = group.values.tolist() 
    master_list.append(group_list)   
    
# define new columns
cols = urns_in_trak_and_rio.columns.tolist()
cols.append('Ratio')

# create new DataFrame to hold results
df_results = pd.DataFrame(columns = cols)

address_pairs = [] 
for i in range(len(master_list)):
    trak_address = master_list[i][0][6]
    rio_address = master_list[i][1][6]
    address_pairs.append([trak_address, rio_address])

fuzz_score = []
diffA_list = []
diffB_list = []

for i in range(len(address_pairs)):
    x = address_pairs[i][0].lower()
    y = address_pairs[i][1].lower()
    ratio = fuzz.ratio(x, y)
    fuzz_score.append(ratio)
    master_list[i][0].append(ratio)
    master_list[i][1].append(ratio)
    
    splitA = OrderedSet(x.split(' '))
    splitB = OrderedSet(y.split(' '))
    diffA = splitA - splitB
    diffB = splitB - splitA
    
    if diffA != '':
        diffA = ' '.join(diffA) 
        master_list[i][0].append(diffA)
    if diffB != '':
        diffB = ' '.join(diffB) 
        master_list[i][1].append(diffB)
        
master_list
flattened_master_list = [i for s in master_list for i in s]

cols = ['System', 
        'URN', 
        'FirstName', 
        'LastName', 
        'Gender', 
        'DOB', 
        'AddressJoin', 
        'RIO_ID', 
        'RIO_AddressUpdateDate', 
        'Score', 
        'Diffs']

df_res = pd.DataFrame(flattened_master_list, columns = cols)
df_res = df_res[['System', 
        'URN', 
        'RIO_ID',
        'FirstName', 
        'LastName', 
        'Gender', 
        'DOB',
        'AddressJoin',  
        'Score', 
        'Diffs',
        'RIO_AddressUpdateDate']]

df_res.sort_values(by=['Score', 'URN'], ascending=(False, True), inplace=True)

df_res.to_excel('ratio_test.xlsx', index=False)

# number of Track rows
num_trak_rows = len(df_res[df_res['System'] == 'TRAK'])
print(f"Number of Track Rows: {num_trak_rows}")

# number of RIO rows
num_rio_rows = len(df_res[df_res['System'] == 'RIO'])
print(f"Number of RIO Rows:   {num_rio_rows}")

print(f'Difference: {num_rio_rows - num_trak_rows }')
