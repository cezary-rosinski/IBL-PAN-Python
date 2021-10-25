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
try:
    cz_authority_df = pd.read_excel("C:/Users/Cezary/Downloads/cz_authority.xlsx", sheet_name='incl_missing')
except FileNotFoundError:
    #tu jest stara wesja pliku
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
try:
    file_path = "C:/Users/Cezary/Downloads/scrapeBrno.txt"
except FileNotFoundError:
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
brno_df['language'] = brno_df['008'].apply(lambda x: x[35:38])
brno_df['100_unidecode'] = brno_df['100'].apply(lambda x: unidecode.unidecode(x).lower() if pd.notnull(x) else x)
    
brno_multiple_original_titles = brno_df[(brno_df['765'].str.count('\$t') > 1) |
                                        (brno_df['765'].str.count('\$a') > 1)]
difficult_dbs_dict.update({'Brno multiple original titles': brno_multiple_original_titles})
brno_df = brno_df[~brno_df['001'].isin(brno_multiple_original_titles['001'])].reset_index(drop=True)

brno_df.to_excel('brno_df.xlsx', index=False)
    
#%% NKC database
try:
    nkc_df = pd.read_excel("C:/Users/Cezary/Downloads/Translations/skc_translations_cz_authority_2021-8-12.xlsx").drop(columns=['cz_id', 'cz_name']).drop_duplicates().reset_index(drop=True)
except FileNotFoundError:
    nkc_df = pd.read_excel("C:/Users/Rosinski/Downloads/Translations/skc_translations_cz_authority_2021-8-12.xlsx").drop(columns=['cz_id', 'cz_name']).drop_duplicates().reset_index(drop=True)
def convert_float_to_int(x):
    try:
        return str(np.int64(x))
    except (ValueError, TypeError):
        return np.nan
nkc_df['005'] = nkc_df['005'].apply(lambda x: convert_float_to_int(x))

nkc_df['fiction_type'] = nkc_df['008'].apply(lambda x: x[33])
nkc_df['audience'] = nkc_df['008'].apply(lambda x: x[22])

nkc_df = nkc_df[~nkc_df['language'].isin(['pol', 'fre', 'eng', 'ger'])].drop_duplicates().reset_index(drop=True)

for i, row in tqdm(nkc_df.iterrows(), total=nkc_df.shape[0]):   
    nkc_df.at[i, '100'] = f"{row['100']}$1http://viaf.org/viaf/{row['viaf_id']}"

nkc_df['260'] = nkc_df[['260', '264']].apply(lambda x: x['260'] if pd.notnull(x['260']) else x['264'], axis=1)
nkc_df['240'] = nkc_df[['240', '246']].apply(lambda x: x['240'] if pd.notnull(x['240']) else x['246'], axis=1)
nkc_df['100_unidecode'] = nkc_df['100'].apply(lambda x: unidecode.unidecode(x).lower() if pd.notnull(x) else x)

start_value = 100000000000
nkc_df['fakeid'] = pd.Series(range(start_value,start_value+nkc_df.shape[0]+1,1))
nkc_df['fakeid'] = nkc_df['fakeid'].astype('Int64').astype('str')      
nkc_df['001'] = nkc_df.apply(lambda x: get_oclc_id(x), axis=1)
nkc_df.drop(columns='fakeid', inplace=True)
nkc_df['SRC'] = 'NKC'

nkc_df.to_excel('nkc_df.xlsx', index=False)

total = pd.concat([brno_df, nkc_df]).reset_index(drop=True)
total['001'] = total['001'].astype(np.int64)

#%% new authority file

new_people = list({v['index']:v for v in new_people}.values())
new_cz_authority_df = pd.DataFrame(new_people)
new_cz_authority_df = pd.concat([cz_authority_df, new_cz_authority_df]).reset_index(drop=True)
new_cz_authority_df.to_excel(f'new_cz_authority_df_{now}.xlsx', index=False)

# new_cz_authority_df = pd.read_excel('new_cz_authority_df_2021-10-23.xlsx')
viaf_positives = [e.split('|') for e in new_cz_authority_df['viaf_id'].drop_duplicates().dropna().to_list()]
viaf_positives = [e for sub in viaf_positives for e in sub]

positive_viafs_names = new_cz_authority_df[new_cz_authority_df['viaf_id'].notnull()][['viaf_id', 'all_names']]
positive_viafs_names = cSplit(positive_viafs_names, 'viaf_id', 'all_names', '❦')
positive_viafs_names['all_names'] = positive_viafs_names['all_names'].apply(lambda x: re.sub('(.*?)(\$a.*?)(\$0.*$)', r'\2', x) if pd.notnull(x) else np.nan)
positive_viafs_names = positive_viafs_names[positive_viafs_names['all_names'].notnull()].drop_duplicates()
positive_viafs_names['index'] = positive_viafs_names.index+1
positive_viafs_names = cSplit(positive_viafs_names, 'index', 'viaf_id', '|').drop(columns='index')

positive_viafs_diacritics = new_cz_authority_df[new_cz_authority_df['viaf_id'].notnull()][['viaf_id', 'cz_name']]
positive_viafs_diacritics['cz_name'] = positive_viafs_diacritics['cz_name'].apply(lambda x: unidecode.unidecode(x))
positive_viafs_diacritics['index'] = positive_viafs_diacritics.index+1
positive_viafs_diacritics = cSplit(positive_viafs_diacritics, 'index', 'viaf_id', '|').drop(columns='index')

viaf_positives_dict = {}
for element in tqdm(viaf_positives, total = len(viaf_positives)):
    viaf_positives_dict[re.findall('\d+', element)[0]] = {'viaf id':element}
for i, row in positive_viafs_names.iterrows():
    if 'form of name' in viaf_positives_dict[row['viaf_id']]:
        viaf_positives_dict[row['viaf_id']]['form of name'].append(row['all_names'])
    else:
        viaf_positives_dict[row['viaf_id']].update({'form of name':[row['all_names']]})
for i, row in positive_viafs_diacritics.iterrows():
    if 'unidecode name' in viaf_positives_dict[row['viaf_id']]:
        viaf_positives_dict[row['viaf_id']]['unidecode name'].append(row['cz_name'])
    else:
        viaf_positives_dict[row['viaf_id']].update({'unidecode name':[row['cz_name']]})
        
viaf_positives_dict = dict(sorted(viaf_positives_dict.items(), key = lambda item : len(item[1]['unidecode name']), reverse=True))

# new_cz_authority_df = pd.read_excel('new_cz_authority_df_2021-10-23.xlsx')
#%% OCLC
try:
    oclc_df = pd.read_excel("C:/Users/Cezary/Downloads/Translations/oclc_all_positive.xlsx").drop(columns=['all_names', 'cz_name', 'type of record + bibliographic level', 'nature_of_contents', 'viaf_id']).drop_duplicates().reset_index(drop=True)
except FileNotFoundError:
    oclc_df = pd.read_excel("C:/Users/Rosinski/Downloads/Translations/oclc_all_positive.xlsx").drop(columns=['all_names', 'cz_name', 'nature_of_contents', 'type of record + bibliographic level', 'viaf_id']).drop_duplicates().reset_index(drop=True)

def marc_parser_dict_for_field(string, subfield_code):
    subfield_list = re.findall(f'{subfield_code}.', string)
    dictionary_field = {}
    for subfield in subfield_list:
        subfield_escape = re.escape(subfield)
        string = re.sub(f'({subfield_escape})', r'❦\1', string)
    for subfield in subfield_list:
        subfield_escape = re.escape(subfield)
        regex = f'(^)(.*?\❦{subfield_escape}|)(.*?)(\,{{0,1}})((\❦{subfield_code})(.*)|$)'
        value = re.sub(regex, r'\3', string)
        dictionary_field[subfield] = value
    return dictionary_field

oclc_df['viaf_id'] = ''
for i, row in tqdm(oclc_df.iterrows(), total=oclc_df.shape[0]):
    try:
        oclc_df.at[i, 'viaf_id'] = re.findall('\d+', marc_parser_dict_for_field(row['100'], '\\$')['$1'])[0]
    except (KeyError, TypeError):
        oclc_df.at[i, 'viaf_id'] = np.nan
        
wrong_oclc_viafs = list(set([re.findall('\d+', e)[0] for e in oclc_df['viaf_id'].to_list() if pd.notnull(e)]))
translation_viafs = [k for k,v in viaf_positives_dict.items()]
wrong_oclc_viafs = [e for e in wrong_oclc_viafs if e not in translation_viafs]
wrong_oclc_viafs = oclc_df[oclc_df['viaf_id'].isin(wrong_oclc_viafs)]

difficult_dbs_dict.update({'OCLC wrong viafs': wrong_oclc_viafs})

oclc_df = oclc_df[~oclc_df['001'].isin(wrong_oclc_viafs['001'])].reset_index(drop=True)

oclc_df['fiction_type'] = oclc_df['008'].apply(lambda x: x[33])
oclc_df['audience'] = oclc_df['008'].apply(lambda x: x[22])

def replace_viaf_group(df):
    viaf_groups = {'256578118':'118529174', '83955898':'25095273', '2299152636076120051534':'11196637', '78000938':'163185334', '10883385':'88045299'}
    df['viaf_id'] = df['viaf_id'].replace(viaf_groups)
    return df
oclc_df = replace_viaf_group(oclc_df).drop_duplicates().reset_index(drop=True)
oclc_df['001'] = oclc_df['001'].astype(np.int64)
oclc_df['SRC'] = 'OCLC'

oclc_multiple_original_titles = oclc_df[(oclc_df['240'].str.count('\$a') > 1) | 
                                        (oclc_df['765'].str.count('\$t') > 1)]
difficult_dbs_dict.update({'OCLC multiple original titles': oclc_multiple_original_titles})
oclc_df = oclc_df[~oclc_df['001'].isin(oclc_multiple_original_titles['001'])].reset_index(drop=True)
total = pd.concat([total, oclc_df])

# ograć duplikaty
total_grouped = total.groupby('001')
duplicates_brno_oclc = total_grouped.filter(lambda x: len(x) > 1).sort_values('001')
total = total[~total['001'].isin(duplicates_brno_oclc['001'])]
duplicates_brno_oclc_grouped = duplicates_brno_oclc.groupby('001')
#3630 duplikatów

#765 z brno, 240 z oclc
brno_oclc_deduplicated = pd.DataFrame()
for name, group in tqdm(duplicates_brno_oclc_grouped, total=len(duplicates_brno_oclc_grouped)):
    for column in group:
        if column == '240':
            group[column] = group[group['SRC'] == 'OCLC'][column]
        elif column in ['fiction_type', 'audience', '020', '041', '245', '260']:
            group[column] = '❦'.join(group[column].dropna().drop_duplicates().astype(str))
        else:
            group[column] = group[group['SRC'] == 'Brno'][column]
    brno_oclc_deduplicated = brno_oclc_deduplicated.append(group)

brno_oclc_deduplicated = brno_oclc_deduplicated[brno_oclc_deduplicated['001'].notnull()]
brno_oclc_deduplicated['001'] = brno_oclc_deduplicated['001'].astype(np.int64)

translations_df = pd.concat([total, brno_oclc_deduplicated]).reset_index(drop=True)  
fields = translations_df.columns.tolist()
fields.sort(key = lambda x: ([str,int].index(type(1 if re.findall(r'\w+', x)[0].isnumeric() else "a")), x))
translations_df = translations_df.reindex(columns=fields) 

oclc_without_author = translations_df[translations_df['100'].isnull()]
difficult_dbs_dict.update({'OCLC without author in 100': oclc_without_author})
translations_df = translations_df[~translations_df['001'].isin(oclc_without_author['001'])].reset_index(drop=True)

translations_df.to_excel(f'translation_database_{now}.xlsx', index=False)

writer = pd.ExcelWriter(f'problematic_records_{now}.xlsx', engine = 'xlsxwriter')
for k, v in difficult_dbs_dict.items():
    v.to_excel(writer, index=False, sheet_name=k)
writer.save()
writer.close()  
#55194 records

# przygotować statystyki - ile mamy na wejsciu z kazdego zrodla, ile tracimy w kazdym kroku, co sie dzieje liczbowo
# potrzebujemy danych od samiuskiego poczatku - np. ile dostalismy od oclc na poczatku
 
# numbers for now: 1. original oclc data, 2. interesting translations (oclc, brno, nkc), 3. HQ and LQ, 4. HQ after deduplication and clustering

# original OCLC records number: 2115027
# correct OCLC, Brno and NKC records number: 55194


#%% quality index
# translations_df = pd.read_excel('translation_database_2021-10-23.xlsx')

#quality index
def quality_index(x):
    full_index = 7
    record_index = 0
    if x['008'][35:38] != 'und':
        record_index += 1
    if x['SRC'] != 'Brno' and pd.notnull(x['240']) and '$a' in x['240'] and any(e for e in ['$l', '$i'] if e in x['240']) and x['240'].count('$a') == 1 and '$k' not in x['240']:
        record_index += 3
    elif x['SRC'] == 'Brno' and pd.notnull(x['765']) and '$t' in x['765']:
        record_index += 3
    # elif pd.notnull(x['240']) and '$a' in x['240'] and x['240'].count('$a') == 1:
    #     record_index += 1.5
    if pd.notnull(x['245']) and all(e for e in ['$a', '$c'] if e in x['245']):
        record_index += 1
    elif pd.notnull(x['245']) and any(e for e in ['$a', '$c'] if e in x['245']): 
        record_index += 0.5
    if pd.notnull(x['260']) and all(e for e in ['$a', '$b', '$c'] if e in x['260']):
        record_index += 1
    elif pd.notnull(x['260']) and any(e for e in ['$a', '$b', '$c'] if e in x['260']):
        record_index += 0.5
    if pd.notnull(x['700']) and pd.notnull(x['700']):
        record_index += 1
    full_index = record_index/full_index
    return full_index
    
translations_df['quality_index'] = translations_df.apply(lambda x: quality_index(x), axis=1)  
#%% clustering

people_df = translations_df.copy()[['001', '100_unidecode']]
people_df['100_unidecode'] = people_df['100_unidecode'].apply(lambda x: x if x[0] != '$' else f'1\\{x}')
people_df = marc_parser_1_field(people_df, '001', '100_unidecode', '\\$')[['001', '$a', '$d', '$1']].replace(r'^\s*$', np.nan, regex=True)  
people_df['$ad'] = people_df[['$a', '$d']].apply(lambda x: '$d'.join(x.dropna().astype(str)) if pd.notnull(x['$d']) else x['$a'], axis=1)
people_df['$ad'] = '$a' + people_df['$ad']
people_df['simplify string'] = people_df['$a'].apply(lambda x: simplify_string(x))
people_df['simplify_ad'] = people_df['$ad'].apply(lambda x: simplify_string(x))
people_df['001'] = people_df['001'].astype(np.int64)

# author clusters

people_df['cluster_viaf'] = people_df['$1'].apply(lambda x: re.findall('\d+', x)[0] if pd.notnull(x) else np.nan)

with_cluster_viaf = people_df[people_df['cluster_viaf'].notnull()][['001', 'cluster_viaf']]
without_cluster_viaf = people_df[people_df['cluster_viaf'].isnull()]

additional_viaf_to_remove = ['256578118', '83955898', '2299152636076120051534', '78000938', '10883385']
people_clusters = {k:v for k,v in viaf_positives_dict.copy().items() if k not in additional_viaf_to_remove}

clusters1 = {}
for key in tqdm(people_clusters, total=len(people_clusters)):
    records = []
    try:
        unidecode_lower_forms_of_names = [simplify_string(e) for e in people_clusters[key]['form of name']]
        records_2 = without_cluster_viaf[without_cluster_viaf['$ad'].isin(unidecode_lower_forms_of_names)]['001'].to_list()
        records += records_2
    except KeyError:
        pass
    try:
        unidecode_name = [simplify_string(e) for e in people_clusters[key]['unidecode name']]
        records_3 = without_cluster_viaf[without_cluster_viaf['simplify string'].isin(unidecode_name)]['001'].to_list()
        records += records_3
    except KeyError:
        pass
    try:
        unidecode_lower_forms_of_names = [simplify_string(e) for e in people_clusters[key]['form of name']]
        records_4 = without_cluster_viaf[without_cluster_viaf['simplify_ad'].isin(unidecode_lower_forms_of_names)]['001'].to_list()
        records += records_4
    except KeyError:
        pass
    records = list(set(records))
    clusters1.update({key:records})
    
clusters1 = pd.DataFrame.from_dict(clusters1, orient='index').stack().reset_index(level=0).rename(columns={'level_0':'cluster_viaf', 0:'001'})
clusters1['001'] = clusters1['001'].astype(np.int64)
    
clusters2 = {783455859:'34454129',
637249516:'34454129',
865669063:'54147355',
1033546256:'51719234',
867204870:'7423797',
866289458:'2504978',
1179040970:'4268266',
184969563:'256578118',
82293972:'256578118',
82294001:'256578118',
82294062:'256578118',
82294069:'256578118',
82294008:'256578118',
82293981:'256578118',
82954002:'256578118',
849587533:'256578118',
851595093:'256578118',
851607875:'256578118',
823031404:'256578118',
848518998:'256578118',
848958588:'256578118',
849016354:'256578118',
851595110:'256578118',
848593998:'256578118',
848519033:'256578118',
851595119:'256578118',
848594027:'256578118',
848519012:'256578118',
848958484:'256578118',
848958525:'256578118',
851595118:'256578118',
861819667:'256578118',
823031408:'256578118',
823031412:'256578118',
849198818:'256578118',
861819715:'256578118',
851595094:'256578118',
1109895089:'27873545',
463885493:'109312616',
264082029:'109312616',
276912703:'109312616',
1081454762:'109312616',
264012924:'109312616',
848572988:'109312616',
849540148:'109312616',
856643123:'109312616',
819016468:'109312616',
815279006:'109312616',
816478259:'109312616',
848027844:'109312616',
1035832806:'4931097',
813839022:'4931097',
1152878659:'4931097',
1105620857:'4931097',
181698433:'4931097',
395022414:'4931097',
1032594029:'4931097',
819029262:'4931097',
816402389:'4931097',
813591120:'4931097',
183188252:'4931097',
867187504:'4931097',
880144559:'4931097',
880067340:'4931097',
879969320:'4931097',
865714834:'4931097',
867494341:'4931097',
867406316:'4931097',
867150020:'4931097',
866288805:'4931097',
880079205:'4931097',
880000785:'4931097',
880072132:'4931097',
880039614:'4931097',
865749898:'4931097',
866219404:'4931097',
880081994:'4931097',
880287680:'4931097',
933527628:'4931097',
942414034:'4931097',
865748257:'71466298',
34916277:'266128409',
880257881:'34458072',
1079408967:'109312616',
730156807:'4931097',
1181999052:'34458072',
82332760:'34458072',
850838147:'34458072',
850838152:'34458072',
816260816:'34458072',
813958921:'34458072',
815105012:'34458072',
813958905:'34458072',
814005917:'34458072',
814233532:'34458072',
862454432:'34458072',
867517630:'34458072',
867517629:'34458072',
42175620:'83906181',
880295380:'51691735',
312645378:'34454129',
304157578:'34454129',
899205408:'29531402',
866289339:'29531402',
866288575:'30008961',
866288631:'30008961',
298317146:'32047415',
298312354:'32047415',
814186329:'14821064',
867477485:'32047415',
867477486:'32047415',
813858536:'51691735',
816407487:'51691735',
816254391:'51691735',
818713086:'51691735',
861843131:'51691735',
866081865:'29835535',
865983059:'29835535',
1107988737:'51691735',
1107988913:'51691735',
1089722868:'51691735',
813954552:'51691735',
816332023:'51691735',
849047466:'51691735',
1154802716:'51691735',
1191895461:'51691735',
1083753165:'51691735',
1105608129:'51691735',
1112283727:'51691735',
298309967:'51691735',
1112283751:'51691735',
1199941147:'51691735',
1117322653:'51691735',
813858519:'51691735',
814069814:'51691735',
816136069:'51691735',
816181778:'51691735',
818723545:'51691735',
823022907:'51691735',
847928213:'51691735',
1083753192:'51691735',
816159944:'51691735',
816240075:'51691735',
816482023:'51691735',
856782836:'51691735',
1081310121:'51691735',
1121651484:'51691735',
148405471:'51691735',
1163862592:'51691735',
961194662:'51691735',
607428315:'51691735',
815577148:'51691735',
1081310188:'51691735',
816482630:'51691735',
1105608006:'51691735',
908681331:'51691735',
181707929:'51691735',
862438315:'51691735',
1140416548:'51691735',
1121662980:'51691735',
263056916:'51691735',
301914015:'51691735',
816221101:'51691735',
858337516:'51691735',
813664625:'51691735',
813691451:'51691735',
856687433:'51691735',
867363394:'51691735',
867383100:'51691735',
903315509:'51691735',
743207913:'51691735',
768629183:'51691735',
933531364:'51691735',
867187486:'20491393',
868836544:'4931097',
867188846:'95263761',
866288619:'52272',
866288522:'52272',
866288520:'52272',
867187485:'52272',
633356487:'34454129',
184969550:'2958781',
848593987:'2958781',
76387666:'4931097',
456633532:'4931097',
866288656:'56763450',
865714882:'56763450',
865715579:'56763450',
82293957:'2958781',
82293992:'73927668',
82294074:'73927668',
848594046:'73927668',
861819700:'73927668',
861819692:'73927668',
848593977:'73927668',
848518978:'73927668',
866289247:'79535702',
47522565:'12317734',
155705160:'46978142',
82341666:'46978142',
813939731:'46978142',
856627786:'46978142',
865756106:'76392272',
500030694:'29531402',
857656306:'54147355',
1188362850:'5084664',
816032232:'34454129',
1179040969:'118416563',
989680077:'114231397'}

clusters2 = pd.DataFrame.from_dict(clusters2, orient='index').stack().reset_index(level=0).rename(columns={'level_0':'001', 0:'cluster_viaf'})

df_people_clusters = pd.concat([with_cluster_viaf, clusters1, clusters2])

df_people_clusters = pd.merge(df_people_clusters, translations_df, how='left', on='001').drop_duplicates().reset_index(drop=True)

df_people_clusters.to_excel('people_clusters.xlsx', index=False)

#55165

#!!!!!!!!!!!! tutaj !!!!!!!!!!!!!!
# original title clusters

df_original_titles = df_people_clusters.replace(r'^\s*$', np.nan, regex=True)  
df_original_titles = df_original_titles[(df_original_titles['100'].notnull()) &
                                        (df_original_titles['240'].notnull() | 
                                         df_original_titles['765'])][['001', '100', '240', '765', 'cluster_viaf']]

def get_longest_cell(x):
    try:
        x = x.split('❦')
        return max(x, key=len)
    except AttributeError:
        return np.nan

df_original_titles['240'] = df_original_titles['240'].apply(lambda x: get_longest_cell(x))
df_original_titles['765'] = df_original_titles['765'].apply(lambda x: get_longest_cell(x))
df_original_titles['765_or_240'] = df_original_titles[['765', '240']].apply(lambda x: x['765'] if pd.notnull(x['765']) else x['240'], axis=1)
df_original_titles['765_or_240'] = df_original_titles['765_or_240'].apply(lambda x: x if x[0] != '$' else f'10{x}')
df_original_titles = df_original_titles.drop(columns=['765', '240'])

df_original_titles_100 = marc_parser_1_field(df_original_titles, '001', '100', '\\$')[['001', '$a', '$d', '$1']].rename(columns={'$a':'name', '$d':'dates', '$1':'viaf'}).drop_duplicates().reset_index(drop=True)
df_original_titles_100['dates'] = df_original_titles_100['dates'].apply(lambda x: x if x != '1\\' else np.nan)

df_original_titles_765_240 = marc_parser_1_field(df_original_titles, '001', '765_or_240', '\\$')
df_original_titles_765_240['$a'] = df_original_titles_765_240[['$a', '$t']].apply(lambda x: x['$t'] if x['$t'] != '' else x['$a'], axis=1)
df_original_titles_765_240['$l'] = df_original_titles_765_240[['$l', '$i']].apply(lambda x: 'Polish' if x['$i'] in ['Tyt. oryg.:', 'Tytuł oryginału:', 'Tyt. oryg. :', 'Tyt oryg.:'] else x['$l'], axis=1)
df_original_titles_765_240['$l'] = df_original_titles_765_240[['$l', '$9']].apply(lambda x: x['$9'] if x['$9'] != '' else x['$l'], axis=1)
df_original_titles_765_240 = df_original_titles_765_240[['001', '$a', '$b', '$l']].drop_duplicates()

try:
    df_original_titles_765_240['original title'] = df_original_titles_765_240.apply(lambda x: ''.join([x['$a'], x['$b']]) if x['$b'] != '' else x['$a'], axis=1)
except KeyError:
    df_original_titles_765_240['original title'] = df_original_titles_765_240['$a']

df_original_titles_simple = pd.merge(df_original_titles_100, df_original_titles_765_240, how='left', on='001')
df_original_titles_simple = df_original_titles_simple.merge(df_original_titles[['001', 'cluster_viaf']]).reset_index(drop=True)
df_original_titles_simple['index'] = df_original_titles_simple.index+1

df_original_titles_simple_grouped = df_original_titles_simple.groupby('cluster_viaf')

df_original_titles_simple = pd.DataFrame()
for name, group in tqdm(df_original_titles_simple_grouped, total=len(df_original_titles_simple_grouped)):
    group = df_original_titles_simple_grouped.get_group(name)
    df = cluster_records(group, 'index', ['original title'], similarity_lvl=0.81)
    df_original_titles_simple = df_original_titles_simple.append(df)

df_original_titles_simple = df_original_titles_simple.sort_values(['cluster_viaf', 'cluster']).rename(columns={'cluster':'cluster_titles'}) 
df_original_titles_simple['cluster_titles'] = df_original_titles_simple['cluster_titles'].astype(np.int64)

df_with_original_titles = pd.merge(df_people_clusters, df_original_titles_simple.drop(columns=['index', 'cluster_viaf']), on='001', how='left').drop_duplicates()

correct = df_with_original_titles[df_with_original_titles['quality_index'] > 0.7]

def genre(x):
    genres_dict = {'0':'nonfiction','e':'nonfiction', 'f':'fiction' ,'1':'fiction' ,'h':'fiction' ,'j':'fiction', 'd':'drama','p':'poetry'}
    try:
        if '❦' not in x:
            return genres_dict[x]
        else:
            genres = []
            for el in x.split('❦'):
                if el not in ['|', '\\']:
                    genres.append(genres_dict[el])
            return '❦'.join(list(set(genres)))
    except KeyError:
        return 'other'

def genre_algorithm(df):
    x = [e.split('❦') for e in df['cluster_genre'].to_list()]
    x = [e for sub in x for e in sub]
    length = len(x)
    x = Counter(x)
    if x['nonfiction']/length > 0.8:
        return 'nonfiction'
    elif x['drama']/length > 0.1:
        return 'drama'
    elif x['poetry']/length > 0.1:
        return 'poetry'
    else:
        return 'fiction'

correct['cluster_genre'] = correct['fiction_type'].apply(lambda x: genre(x))

correct_grouped = correct.groupby('cluster_titles')

correct = pd.DataFrame()
for name, group in tqdm(correct_grouped, total=len(correct_grouped)):
    group['cluster_genre'] = genre_algorithm(group)
    correct = correct.append(group)

not_correct = df_with_original_titles[df_with_original_titles['quality_index'] <= 0.7]
writer = pd.ExcelWriter(f'translation_database_clusters_with_quality_index_{now}.xlsx', engine = 'xlsxwriter')
correct.to_excel(writer, index=False, sheet_name='HQ')
not_correct.to_excel(writer, index=False, sheet_name='LQ')
writer.save()
writer.close()

#%% deduplikacja

# skopiować, co jest i dodać krok z ISBN
































