import pandas as pd
import os
from rapidfuzz import fuzz
from operator import itemgetter
import itertools
import re
from ordered_set import OrderedSet
os.chdir('M:\Rio vs Trak')

# define datasets
rio_full = pd.read_csv('RIO_Full.csv', parse_dates=True)
trak_full = pd.read_csv('Trak_Full.csv', parse_dates=True)

# replace NaNs with empty string to enable address joins and ensure no duplicates
rio_full.fillna('', inplace=True)
rio_full.drop_duplicates(inplace=True)
trak_full.fillna('', inplace=True)
trak_full.drop_duplicates(inplace=True)

# build address join column in each dataframe and remove blank values
rio_full['AddressJoin'] = ((rio_full['Address1'].str.lower() + ', ' +  
                               rio_full['Address2'].str.lower() + ', ' + 
                               rio_full['Address3'].str.lower() + ', ' + 
                               rio_full['Address4'].str.lower() + ', ' + 
                               rio_full['PostCode'].str.lower()
                            # remove unwanted text
                            .str.replace('zz99', ''))
                            .str.replace('channel islands', '', regex=True)
                            .str.replace('guernsey', '', regex=True)
                            .str.strip(',').str.strip()
                          )
# remove empty cells
rio_full['AddressJoin'] = rio_full['AddressJoin'].str.replace(' ,', '')


trak_full['AddressJoin'] = ((trak_full['Address1'].str.lower() + ', ' +  
                               trak_full['Address2'].str.lower() + ', ' + 
                               trak_full['PostCode'].str.lower())
                            # remove unwanted text
                            .str.replace('channel islands', '', regex=True)
                            .str.replace('guernsey', '', regex=True)
                            .str.strip(',').str.strip()
                           )
# remove empty cells
trak_full['AddressJoin'] = rio_full['AddressJoin'].str.replace(' ,', '')

# merge into one dataframe
merged_dfs = pd.concat([trak_full, rio_full], ignore_index=True, axis=0)

# remove unwanted address columns that were joined previously
merged_dfs.drop(['Address1', 'Address2', 'PostCode','Address3','Address4' ], axis=1, inplace=True)

# identify unmerged records in Rio
duplicate_urns_in_rio = rio_full[rio_full['URN'].map(rio_full['URN'].value_counts()) > 1]

# records in Trak and Rio
urns_in_trak_and_rio = merged_dfs[merged_dfs['URN'].map(merged_dfs['URN'].value_counts()) > 1]

# other records
not_urns_in_trak_and_rio = merged_dfs[merged_dfs['URN'].map(merged_dfs['URN'].value_counts()) == 1]

# urns in Trak and Rio minus Rio unmrged records 
rio_unmerged_urns = list(set(duplicate_urns_in_rio['URN'].tolist()))
urns_in_trak_and_rio_minus_rio_unmerged = urns_in_trak_and_rio[~urns_in_trak_and_rio['URN'].isin(rio_unmerged_urns)]

# group rows by URN and convert to a nestled list
master_list = []
for group_name, group in urns_in_trak_and_rio_minus_rio_unmerged.groupby('URN'):
    group_list = group.values.tolist() 
    master_list.append(group_list)   

# separate the string to perform fuzzy matching on and create nestled list
address_pairs = [] 
for i in range(len(master_list)):
    trak_address = master_list[i][0][6]
    rio_address = master_list[i][1][6]
    address_pairs.append([trak_address, rio_address])
    
    fuzz_score = []
diffA_list = []
diffB_list = []

# perform fuzzy match normalising case and append result to list
for i in range(len(address_pairs)):
    x = address_pairs[i][0].lower()
    y = address_pairs[i][1].lower()
    ratio = fuzz.ratio(x, y)
    fuzz_score.append(ratio)
    master_list[i][0].append(ratio)
    master_list[i][1].append(ratio)
    
    # split the address strings and get differences between strings
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
        
# flatten nestled list to enable conversion into dataframe
flattened_master_list = [i for s in master_list for i in s]

# define columns for results dataframe (currently flattened_master_list)
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

# convert flattened_master_list into dataframe
df_res = pd.DataFrame(flattened_master_list, columns = cols)

# re-order columns
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

# sort results dataframe
df_res.sort_values(by=['Score', 'URN'], ascending=(False, True), inplace=True)

# export
df_res.to_excel('records_both_in_trak_and_rio_minus_unmerged_in_rio.xlsx', index=False)
duplicate_urns_in_rio.to_excel('duplicate_urns_in_rio.xlsx', index=False)
urns_in_one_but_not_other.to_excel('urns_in_one_but_not_other.xlsx', index=False)

# reporting
# number of Trak rows
num_trak_rows = len(df_res[df_res['System'] == 'TRAK'])
print(f"Number of Track Rows: {num_trak_rows}")

# number of Rio rows
num_rio_rows = len(df_res[df_res['System'] == 'RIO'])
print(f"Number of RIO Rows:   {num_rio_rows}")
print(f'Difference: {num_rio_rows - num_trak_rows }')
print('\n')
number_of_exact_matches = len(df_res[df_res["Score"] == 100])
print(f'Number of exact matches: {number_of_exact_matches}')

# get current date and time
dt = datetime.now()

# append nnumber of exact matches to txt file 
with open('audit.txt', 'a') as f:
    f.write(f'{dt}: {number_of_exact_matches} exact matches\n')
