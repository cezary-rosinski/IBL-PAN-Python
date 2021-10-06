import io
from tqdm import tqdm
import pandas as pd
import re
from collections import Counter
from my_functions import xml_to_mrk, marc_parser_1_field
import numpy as np
import random

#%% VIAF IDs for people from Czech database

# path_out = "C:/Users/Cezary/Downloads/ucla0110.mrk"
# path_in = "C:/Users/Cezary/Downloads/ucla0110.xml"
   
# xml_to_mrk(path_in, path_out)
 
# file_path = "C:/Users/Cezary/Downloads/ucla0110.mrk"
file_path = "C:/Users/Rosinski/Downloads/ucla0110.mrk"
encoding = 'utf-8'

marc_list = io.open(file_path, 'rt', encoding = encoding).read().splitlines()

records_sample = []
for row in tqdm(marc_list):
    if row.startswith('=LDR') and len(row) > 6:
        records_sample.append([row])
    else:
        if len(row) == 0 or len(row) > 6:
            records_sample[-1].append(row)

sample = random.choices(records_sample, k=20)
sample = [e for sub in sample for e in sub]
sample_txt = io.open("marc21_sample.txt", 'wt', encoding='UTF-8')
for element in sample:
    sample_txt.write(str(element) + '\n')
sample_txt.close()

mrk_list = []
for el in tqdm(marc_list):
    if el.startswith(('=100', '=600', '=700')):
        mrk_list.append(el[6:])
       
# data_counter = dict(Counter(mrk_list).most_common(100))

unique_people = list(set(mrk_list))

df = pd.DataFrame(unique_people, columns=['MARC21_person'])
df['index'] = df.index+1
df_parsed = marc_parser_1_field(df, 'index', 'MARC21_person', '\$').drop(columns=['MARC21_person', '$4', '$2']).drop_duplicates().reset_index(drop=True).replace(r'^\s*$', np.NaN, regex=True)
df_parsed['all'] = df_parsed[df_parsed.columns[1:]].apply(
    lambda x: '❦'.join(x.dropna().astype(str)),
    axis=1
)
 
df_parsed['index'] = df_parsed.groupby('all')['index'].transform(lambda x: '❦'.join(x.drop_duplicates().astype(str)))
df_parsed = df_parsed.drop_duplicates()
#group 1 - people with AUT IDs
group_1 = df_parsed[df_parsed['$7'].notnull()].drop(columns='all')

#group 2 - people without AUT IDs but with a name and a dates
group_2 = df_parsed[(df_parsed['$a'].notnull()) &
                    (df_parsed['$d'].notnull()) &
                    (df_parsed['$7'].isnull())].drop(columns='all')

#group 3 - the rest
group_3 = df_parsed[(~df_parsed['index'].isin(group_1['index'])) &
                    (~df_parsed['index'].isin(group_2['index']))].drop(columns='all')


test = group_3.sample(1000)

















