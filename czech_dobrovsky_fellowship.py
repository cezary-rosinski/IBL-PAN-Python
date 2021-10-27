import io
from tqdm import tqdm
import pandas as pd
import re
from collections import Counter
from my_functions import xml_to_mrk, marc_parser_1_field, marc_parser_dict_for_field
import numpy as np
import random
import requests
from bs4 import BeautifulSoup
from difflib import SequenceMatcher

#%% VIAF IDs for people from Czech database

# path_out = "C:/Users/Cezary/Downloads/ucla0110.mrk"
# path_in = "C:/Users/Cezary/Downloads/ucla0110.xml"
   
# xml_to_mrk(path_in, path_out)
 
file_path = "C:/Users/Cezary/Downloads/ucla0110.mrk"
# file_path = "C:/Users/Rosinski/Downloads/ucla0110.mrk"
encoding = 'utf-8'

marc_list = io.open(file_path, 'rt', encoding = encoding).read().splitlines()

# records_sample = []
# for row in tqdm(marc_list):
#     if row.startswith('=LDR') and len(row) > 6:
#         records_sample.append([row])
#     else:
#         if len(row) == 0 or len(row) > 6:
#             records_sample[-1].append(row)

# sample = random.choices(records_sample, k=20)
# sample = [e for sub in sample for e in sub]
# sample_txt = io.open("marc21_sample.txt", 'wt', encoding='UTF-8')
# for element in sample:
#     sample_txt.write(str(element) + '\n')
# sample_txt.close()

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
group_1 = df_parsed[df_parsed['$7'].notnull()].drop(columns='all').reset_index(drop=True)

#group 2 - people without AUT IDs but with a name and a dates
group_2 = df_parsed[(df_parsed['$a'].notnull()) &
                    (df_parsed['$d'].notnull()) &
                    (df_parsed['$7'].isnull())].drop(columns='all').reset_index(drop=True)

#group 3 - the rest
group_3 = df_parsed[(~df_parsed['index'].isin(group_1['index'])) &
                    (~df_parsed['index'].isin(group_2['index']))].drop(columns='all').reset_index(drop=True)

#%% Working with VIAF API - group 1

group_1['viaf_id'] = ''
for i, row in tqdm(group_1.iterrows(), total=group_1.shape[0]):
    url = f"http://viaf.org/viaf/sourceID/NKC%7C{row['$7']}/json"
    response = requests.get(url).url
    viaf_id = re.findall('\d+', response)[0]
    group_1.at[i, 'viaf_id'] = viaf_id

#%% Working with VIAF API - group 2

group_2['nkc_id'] = ''
group_2['viaf_id'] = ''
group_2['viaf_name'] = ''
for i, row in tqdm(group_2.iterrows(), total=group_2.shape[0]):
    search_name = f"{row['$a']} {row['$d']}"
    url = re.sub('\s+', '%20', f"http://viaf.org/viaf/search?query=local.personalNames%20all%20%22{search_name}%22&sortKeys=holdingscount&httpAccept=application/json")
    response = requests.get(url).json()
    try:
        try:
            nkc_id = [e for e in response['searchRetrieveResponse']['records'][0]['record']['recordData']['sources']['source'] if 'NKC' in e['#text']][0]['@nsid']
            group_2.at[i, 'nkc_id'] = nkc_id
        except TypeError:
            if 'NKC' in response['searchRetrieveResponse']['records'][0]['record']['recordData']['sources']['source']['#text']:
                nkc_id = response['searchRetrieveResponse']['records'][0]['record']['recordData']['sources']['source']['@nsid']
                group_2.at[i, 'nkc_id'] = nkc_id
        except IndexError:
            group_2.at[i, 'nkc_id'] = np.nan
        viaf_id = response['searchRetrieveResponse']['records'][0]['record']['recordData']['viafID']
        try:
            viaf_name = response['searchRetrieveResponse']['records'][0]['record']['recordData']['mainHeadings']['data'][0]['text']
        except KeyError:
            viaf_name = response['searchRetrieveResponse']['records'][0]['record']['recordData']['mainHeadings']['data']['text']
        group_2.at[i, 'viaf_id'] = viaf_id
        group_2.at[i, 'viaf_name'] = viaf_name
    except KeyError:
        try:
            viaf_id = response['searchRetrieveResponse']['records'][0]['record']['recordData']['viafID']
            viaf_name = response['searchRetrieveResponse']['records'][0]['record']['recordData']['mainHeadings']['data'][0]['text']
            group_2.at[i, 'nkc_id'] = np.nan
            group_2.at[i, 'viaf_id'] = viaf_id
            group_2.at[i, 'viaf_name'] = viaf_name
        except KeyError:
            group_2.at[i, 'nkc_id'] = np.nan
            group_2.at[i, 'viaf_id'] = np.nan

#%% Subject Headings
# getting SH from CLB
file_path = "C:/Users/Cezary/Downloads/ucla0110.mrk"
encoding = 'utf-8'
marc_list = io.open(file_path, 'rt', encoding = encoding).read().splitlines()

clb_sh = []
errors = []
for row in tqdm(marc_list):
    if row.startswith('=650  07'):
        if re.findall('(?<=\$7)(.+?)(?=\$|$)', row):
            errors.append((row, re.findall('(?<=\$7)(.+?)(?=\$|$)', row)[0]))
        try:
            sh = re.findall('ph\d+(?=\$|$)', row)[0]
            clb_sh.append(sh)
        except IndexError:
            pass
clb_sh = list(set(clb_sh))
errors = list(set(errors))

# getting all Czech SH in English           
            
file_path = "C:/Users/Cezary/Downloads/150.txt"
encoding = 'utf-8'

marc_list = io.open(file_path, 'rt', encoding = encoding).read().splitlines()
marc_list = [e[10:] for e in marc_list]

list_of_records = []
for row in tqdm(marc_list):
    if row.startswith('LDR'):
        list_of_records.append([row])
    else:
        if row:
            list_of_records[-1].append(row)
            
sh_dict = {}
for index, record in tqdm(enumerate(list_of_records, 1),total=len(list_of_records)):
    for field in record:
        if field.startswith('150'):
            sh_dict[index] = {'cz': field[8:]}
        elif field.startswith('750') and '$$2eczenas' in field:
            if 'en' not in sh_dict[index]:
                sh_dict[index].update({'en': field[8:]})

# filtering SH for literary science
           
literary_sh_dict = {k:v for k,v in sh_dict.items() if marc_parser_dict_for_field(v['cz'], '\$\$')['$$7'] in clb_sh}                     
                
sh_dict = dict(list(literary_sh_dict.items())[:10])

url = 'https://id.loc.gov/search/?q='
rest_url = '&q=cs%3Ahttp%3A%2F%2Fid.loc.gov%2Fauthorities%2Fsubjects'
for key, value in tqdm(sh_dict.items()):
    # key = list(sh_dict.items())[0][0]
    # value = list(sh_dict.items())[0][1]
    
    term = re.findall('(?<=\$\$a)(.+?)(?=\$)', value['en'])[0]    
    query = f'{url}{term}{rest_url}'
    
    response = requests.get(query).text
    soup = BeautifulSoup(response, 'html.parser')
    
    links = zip(soup.select('.tbody-group a'), soup.select('type'))
    locsh_dict = {}
    for i, (name, kind) in enumerate(links, 1):
        if kind.text == 'Topic':
            locsh_dict[i] = {'label': name.text}
            locsh_dict[i].update({'ID': re.findall('sh\d+$', name['href'])[0]})
        
    for k,v in locsh_dict.items():
        query = f"https://id.loc.gov/authorities/subjects/{v['ID']}.json"
        pure_url = query.replace('https', 'http').replace('.json','')
        response = requests.get(query)
        response.encoding = 'UTF-8'
        response = response.json()
        try:
            alt_labels = [e for e in response if pure_url == e['@id']][0]
            alt_labels = list({k:v for k,v in alt_labels.items() if 'core#altLabel' in k}.values())[-1]
            alt_labels = [dictionary['@value'] for dictionary in alt_labels]
            all_names = alt_labels.copy()
            all_names.append(v['label'])
            locsh_dict[k].update({'alternative labels': alt_labels, 'all names': all_names}) 
        except IndexError:
            pass        
    sh_dict[key].update({'LoC SH': locsh_dict})

# string similarity

# put it into a loop

sh_keys = sh_dict[2]['LoC SH'].keys()
empty_list = []
for el in sh_keys:
    cz_name = marc_parser_dict_for_field(sh_dict[2]['en'], '\$\$')['$$a']
    el2 = sh_dict[2]['LoC SH'][el]['all names']
    for i, sh in enumerate(el2):
        coe = SequenceMatcher(a=cz_name, b=sh.lower()).ratio()
        empty_list.append((sh, coe))

proper_one = max(empty_list, key=lambda x: x[-1])[0]
similarity_lvl = max(empty_list, key=lambda x: x[-1])[-1]

sh_dict[2]['LoC SH'] = [{k:v for k,v in sh_dict[2]['LoC SH'].items() if proper_one in v['all names']}, similarity_lvl]





url = 'https://id.loc.gov/search/?q=cultural+change&q=cs%3Ahttp%3A%2F%2Fid.loc.gov%2Fauthorities%2Fsubjects'
response = requests.get(url)
response.encoding = 'UTF-8'
soup = BeautifulSoup(response.text, 'html.parser')
links = zip(soup.select('.tbody-group a'), soup.select('type'))
for a,b in links:
    if b.text == 'Topic':
        print(a['href'])
        print(a.text)
        print(b.text)
        print('____________________')



    
    
    


links = soup.findAll('a', attrs={'href': re.compile("^\?o\=")})
























