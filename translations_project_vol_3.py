import pandas as pd
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
import gspread as gs
from datetime import datetime
import regex as re
from collections import OrderedDict
from gspread_dataframe import set_with_dataframe, get_as_dataframe
import numpy as np
from my_functions import cosine_sim_2_elem, marc_parser_1_field, gsheet_to_df, xml_to_mrk, cSplit, f, df_to_mrc, mrc_to_mrk, mrk_to_df, xml_to_mrk, mrk_to_mrc, get_cosine_result, df_to_gsheet, cluster_strings, cluster_records, simplify_string
import unidecode
import pandasql
import time
# from google_drive_research_folders import cr_projects
from functools import reduce
import sys
import csv
from tqdm import tqdm
import json
import xml.etree.ElementTree as et
import requests
from collections import Counter
import warnings
import io
import glob

warnings.simplefilter(action='ignore', category=FutureWarning)
pd.set_option('display.float_format', lambda x: '%.3f' % x)

#%%
now = datetime.now().date()
year = now.year
month = '{:02}'.format(now.month)
day = '{:02}'.format(now.day)
#%%
# #autoryzacja do tworzenia i edycji plików
# gc = gs.oauth()
# #autoryzacja do penetrowania dysku
# gauth = GoogleAuth()
# gauth.LocalWebserverAuth()
# drive = GoogleDrive(gauth)

#%% Authority file
# cz_authority_df = pd.read_excel("C:/Users/Cezary/Downloads/cz_authority.xlsx", sheet_name='incl_missing')
cz_authority_df = pd.read_excel("C:/Users/Rosinski/Downloads/Translations/cz_authority.xlsx", sheet_name='incl_missing')

cz_authority_list = [e.split('❦') for e in cz_authority_df['IDs'].to_list()]
cz_authority_list = [[e for e in sublist if 'NKC' in e] for sublist in cz_authority_list]
cz_authority_list = list(set([e.split('|')[-1] for sublist in cz_authority_list for e in sublist]))

cz_id_viaf_id = cz_authority_df[['index', 'viaf_id', 'IDs']]
cz_id_viaf_id['viaf_id'] = cz_id_viaf_id['viaf_id'].apply(lambda x: re.sub('(.+?)(\|.+$)', r'\1', x))
cz_id_viaf_id = cSplit(cz_id_viaf_id, 'index', 'IDs', '❦').drop(columns='index').drop_duplicates().reset_index(drop=True)
cz_id_viaf_id = cz_id_viaf_id[cz_id_viaf_id['IDs'].str.contains('NKC')]
cz_id_viaf_id['IDs'] = cz_id_viaf_id['IDs'].apply(lambda x: re.findall('(?<=NKC\|)(.+)', x)[0])
cz_id_viaf_id_dict = {}
for i, row in cz_id_viaf_id.iterrows():
    cz_id_viaf_id_dict[row['IDs']] = row['viaf_id']
    
#%% dictionary for difficult sets of data
difficult_dbs_dict = {}    
#%% Brno database
# file_path = "C:/Users/Cezary/Downloads/scrapeBrno.txt"
file_path = "C:/Users/Rosinski/Downloads/scrapeBrno.txt"
encoding = 'utf-8'
marc_list = io.open(file_path, 'rt', encoding = encoding).read().replace('|','$').splitlines()

records = []
for row in tqdm(marc_list):
    if row.startswith('LDR'):
        records.append([row])
    else:
        if not row.startswith('FMT') and len(row) > 0:
            records[-1].append(row)
            
records = [[e.split('\t') for e in record] for record in records]
records = [[[field[0] if not field[0][:3].isnumeric() else field[0][:3], field[1]] for field in record] for record in records]

final_list = []
for record in tqdm(records):
    slownik = {}
    for [field, content] in record:
        if field in slownik:
            slownik[field] += f"❦{content}"
        else:
            slownik[field] = content
    final_list.append(slownik)

brno_df = pd.DataFrame(final_list)
fields = brno_df.columns.tolist()
fields.sort(key = lambda x: ([str,int].index(type("a" if re.findall(r'\w+', x)[0].isalpha() else 1)), x))
brno_df = brno_df.reindex(columns=fields) 
brno_df = brno_df[(brno_df['DEL'].isnull()) &
                  (brno_df['BAS'].str.contains('CZT')) &
                  (~brno_df['041'].isna()) &
                  (brno_df['041'].str.contains('$h cze', regex=False))].drop(columns=['BAS', 'DEL', 'GBS', 'LID', 'MKL', 'Obalkyknih.cz:', 'PAK', 'PRU', 'RDALD', 'SHOD', 'STA', 'SYS', 'A653', 'B260', 'GBS1', 'GBS2', 'M0ZK', 'T787', 'V015', 'V020', 'V043', 'V490', 'V505', 'V787', 'V902']).reset_index(drop=True)
brno_df['SRC'] = 'Brno'

start_value = 10000000000
brno_df['fakeid'] = pd.Series(range(start_value,start_value+brno_df.shape[0]+1,1))
brno_df['fakeid'] = brno_df['fakeid'].astype('Int64').astype('str')

def get_oclc_id(x):
    try:
        return re.findall('(?<=\(OCoLC\))\d+', x['035'])[0]
    except (IndexError, TypeError):
        return x['fakeid']
        
brno_df['001'] = brno_df.apply(lambda x: get_oclc_id(x), axis=1)
brno_df.drop(columns='fakeid', inplace=True)
brno_df['008'] = brno_df['008'].str.replace('$', '|')

brno_df['260'] = brno_df[['260', '264']].apply(lambda x: x['260'] if pd.notnull(x['260']) else x['264'], axis=1)
brno_df['240'] = brno_df[['240', '246']].apply(lambda x: x['240'] if pd.notnull(x['240']) else x['246'], axis=1)
brno_df['100_unidecode'] = brno_df['100'].apply(lambda x: unidecode.unidecode(x).lower() if pd.notnull(x) else x)

brno_grouped = brno_df.groupby('001')
brno_duplicates = brno_grouped.filter(lambda x: len(x) > 1).sort_values('001')
difficult_dbs_dict.update({'Brno duplicates': brno_duplicates})
brno_df = brno_df[~brno_df['001'].isin(brno_duplicates['001'])]
brno_no_author_in_100 = brno_df[brno_df['100'].isna()]
difficult_dbs_dict.update({'Brno no author in 100': brno_no_author_in_100})
brno_df = brno_df[brno_df['100'].notnull()]
field_100 = marc_parser_1_field(brno_df, '001', '100', '\$')
brno_no_nkc_id = field_100[field_100['$7'] == '']['001'].to_list()
brno_no_nkc_id = brno_df[brno_df['001'].isin(brno_no_nkc_id)]
difficult_dbs_dict.update({'Brno no NKC id for author': brno_no_nkc_id})
brno_df = brno_df[~brno_df['001'].isin(brno_no_nkc_id['001'])]
field_100 = marc_parser_1_field(brno_df, '001', '100', '\$')
field_100['$a'] = field_100['$a'].apply(lambda x: x[:-1] if x[-1] == ',' else x)
field_100['$1'] = ''
new_people = []
for i, row in tqdm(field_100.iterrows(), total=field_100.shape[0]):
    try:
        field_100.at[i,'$1'] = f"http://viaf.org/viaf/{cz_id_viaf_id_dict[row['$7']]}"
    except KeyError:
        url = f"http://viaf.org/viaf/sourceID/NKC%7C{row['$7']}/json"
        response = requests.get(url).url
        viaf_id = re.findall('\d+', response)[0]
        field_100.at[i,'$1'] = f"http://viaf.org/viaf/{viaf_id}"
        new_people.append({'index':viaf_id, 'cz_name':row['$a'], 'cz_dates':row['$d'], 'viaf_id':viaf_id, 'IDs':row['$7']})
brno_df['viaf_id'] = ''
for i, row in tqdm(brno_df.iterrows(), total=brno_df.shape[0]):   
    brno_df.at[i, '100'] = f"{row['100']}$1{field_100[field_100['001'] == row['001']]['$1'].to_list()[0]}"
    viaf_id = field_100[field_100['001'] == row['001']]['$1'].to_list()[0]
    viaf_id = re.findall('\d+', viaf_id)[0]
    brno_df.at[i, 'viaf_id'] = viaf_id
   
brno_df['fiction_type'] = brno_df['008'].apply(lambda x: x[33])
brno_df['audience'] = brno_df['008'].apply(lambda x: x[22])
    
brno_multiple_original_titles = brno_df[brno_df['765'].str.count('\$t') > 1]
difficult_dbs_dict.update({'Brno multiple original titles': brno_multiple_original_titles})
brno_df = brno_df[~brno_df['001'].isin(brno_multiple_original_titles['001'])].reset_index(drop=True)
    
#%% NKC database
# nkc_df = pd.read_excel("C:/Users/Cezary/Downloads/Translations/skc_translations_cz_authority_2021-8-12.xlsx").drop(columns=['cz_id', 'viaf_id', 'cz_name']).drop_duplicates().reset_index(drop=True)
nkc_df = pd.read_excel("C:/Users/Rosinski/Downloads/Translations/skc_translations_cz_authority_2021-8-12.xlsx").drop(columns=['cz_id', 'cz_name']).drop_duplicates().reset_index(drop=True)
def convert_float_to_int(x):
    try:
        return str(np.int64(x))
    except (ValueError, TypeError):
        return np.nan
nkc_df['005'] = nkc_df['005'].apply(lambda x: convert_float_to_int(x))
nkc_df = nkc_df[~nkc_df['language'].isin(['pol', 'fre', 'eng', 'ger'])].drop(columns='language').drop_duplicates().reset_index(drop=True)

nkc_df['fiction_type'] = nkc_df['008'].apply(lambda x: x[33])
nkc_df['audience'] = nkc_df['008'].apply(lambda x: x[22])

nkc_df['260'] = nkc_df[['260', '264']].apply(lambda x: x['260'] if pd.notnull(x['260']) else x['264'], axis=1)
nkc_df['240'] = nkc_df[['240', '246']].apply(lambda x: x['240'] if pd.notnull(x['240']) else x['246'], axis=1)
nkc_df['100_unidecode'] = nkc_df['100'].apply(lambda x: unidecode.unidecode(x).lower() if pd.notnull(x) else x)

start_value = 100000000000
nkc_df['fakeid'] = pd.Series(range(start_value,start_value+nkc_df.shape[0]+1,1))
nkc_df['fakeid'] = nkc_df['fakeid'].astype('Int64').astype('str')      
nkc_df['001'] = nkc_df.apply(lambda x: get_oclc_id(x), axis=1)
nkc_df.drop(columns='fakeid', inplace=True)
nkc_df['SRC'] = 'NKC'

total = pd.concat([brno_df, nkc_df]).reset_index(drop=True)
total['001'] = total['001'].astype(np.int64)

#%% OCLC
# oclc_df = pd.read_excel("C:/Users/Cezary/Downloads/Translations/oclc_all_positive.xlsx").drop(columns=['all_names', 'cz_name', '100_unidecode']).drop_duplicates().reset_index(drop=True)
oclc_df = pd.read_excel("C:/Users/Rosinski/Downloads/Translations/oclc_all_positive.xlsx").drop(columns=['all_names', 'cz_name', 'nature_of_contents']).drop_duplicates().reset_index(drop=True)

oclc_df['fiction_type'] = oclc_df['008'].apply(lambda x: x[33])
oclc_df['audience'] = oclc_df['008'].apply(lambda x: x[22])

def replace_viaf_group(df):
    viaf_groups = {'256578118':'118529174', '83955898':'25095273', '2299152636076120051534':'11196637'}
    df['viaf_id'] = df['viaf_id'].replace(viaf_groups)
    return df
oclc_df = replace_viaf_group(oclc_df).drop_duplicates().reset_index(drop=True)
oclc_df['001'] = oclc_df['001'].astype(np.int64)
oclc_df['SRC'] = 'OCLC'

oclc_multiple_original_titles = oclc_df[oclc_df['240'].str.count('\$a') > 1]
difficult_dbs_dict.update({'OCLC multiple original titles': oclc_multiple_original_titles})
oclc_df = oclc_df[~oclc_df['001'].isin(oclc_multiple_original_titles['001'])].reset_index(drop=True)

total = pd.concat([total, oclc_df])
fields = total.columns.tolist()
fields.sort(key = lambda x: ([str,int].index(type("a" if re.findall(r'\w+', x)[0].isalpha() else 1)), x))
total = total.reindex(columns=fields) 

test = total[['SRC', 'nature_of_contents']]
test = test[test['nature_of_contents'].notnull()]

# ograć duplikaty
total_grouped = total.groupby('001')
duplicates_brno_oclc = total_grouped.filter(lambda x: len(x) > 1).sort_values('001')
total = total[~total['001'].isin(duplicates_brno_oclc['001'])]
duplicates_brno_oclc_grouped = duplicates_brno_oclc.groupby('001')
#3636 duplikatów

brno_oclc_deduplicated = pd.DataFrame()
for name, group in tqdm(duplicates_brno_oclc_grouped, total=len(duplicates_brno_oclc_grouped)):
    for column in group:
        if column in ['fiction_type', 'audience', '020', '041', '240', '245', '260']:
            group[column] = '❦'.join(group[column].dropna().drop_duplicates().astype(str))
        else:
            group[column] = group[group['SRC'] == 'Brno'][column]
    brno_oclc_deduplicated = brno_oclc_deduplicated.append(group)

brno_oclc_deduplicated = brno_oclc_deduplicated[brno_oclc_deduplicated['001'].notnull()]
brno_oclc_deduplicated['001'] = brno_oclc_deduplicated['001'].astype(np.int64)

test = pd.concat([total, brno_oclc_deduplicated]).reset_index(drop=True)  



total.to_excel(f'translation_database_{now}.xlsx', index=False)

writer = pd.ExcelWriter(f'problematic_records_{now}.xlsx', engine = 'xlsxwriter')
for k, v in difficult_dbs_dict.items():
    v.to_excel(writer, index=False, sheet_name=k)
writer.save()
writer.close()  


# przygotować statystyki - ile mamy na wejsciu z kazdego zrodla, ile tracimy w kazdym kroku, co sie dzieje liczbowo
# potrzebujemy danych od samiuskiego poczatku - np. ile dostalismy od oclc na poczatku




# numbers for now: 1. original oclc data, 2. interesting translations (oclc, brno, nkc), 3. HQ and LQ, 4. HQ after deduplication and clustering


#%% clusters

new_people = list({v['index']:v for v in new_people}.values())
# dodać tych ludzi do pliku wzorcowego


#%% deduplikacja
































