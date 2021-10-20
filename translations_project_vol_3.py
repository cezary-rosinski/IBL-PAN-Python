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
now = datetime.now()
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
cz_authority_df = pd.read_excel("C:/Users/Cezary/Downloads/cz_authority.xlsx", sheet_name='incl_missing')

cz_authority_list = [e.split('❦') for e in cz_authority_df['IDs'].to_list()]
cz_authority_list = [[e for e in sublist if 'NKC' in e] for sublist in cz_authority_list]
cz_authority_list = list(set([e.split('|')[-1] for sublist in cz_authority_list for e in sublist]))
#%% Brno database
file_path = "C:/Users/Cezary/Downloads/scrapeBrno.txt"
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

#czy pozmieniać treści, np. przenieść tytuł oryginalny?

#%% NKC database
nkc_df = pd.read_excel("C:/Users/Cezary/Downloads/Translations/skc_translations_cz_authority_2021-8-12.xlsx").drop(columns=['cz_id', 'viaf_id', 'cz_name']).drop_duplicates().reset_index(drop=True)
def convert_float_to_int(x):
    try:
        return str(np.int64(x))
    except (ValueError, TypeError):
        return np.nan
nkc_df['005'] = nkc_df['005'].apply(lambda x: convert_float_to_int(x))
nkc_df = nkc_df[~nkc_df['language'].isin(['pol', 'fre', 'eng', 'ger'])].drop(columns='language').drop_duplicates().reset_index(drop=True)

start_value = 100000000000
nkc_df['fakeid'] = pd.Series(range(start_value,start_value+nkc_df.shape[0]+1,1))
nkc_df['fakeid'] = nkc_df['fakeid'].astype('Int64').astype('str')      
nkc_df['001'] = nkc_df.apply(lambda x: get_oclc_id(x), axis=1)
nkc_df.drop(columns='fakeid', inplace=True)
nkc_df['SRC'] = 'NKC'

total = pd.concat([brno_df, nkc_df])

total_grouped = total.groupby('001')
duplicates_brno_nkc = total_grouped.filter(lambda x: len(x) > 1).sort_values('001')

total = total[~total['001'].isin(duplicates_brno_nkc['001'])].reset_index(drop=True)
total['001'] = total['001'].astype(np.int64)

#%% OCLC

oclc_df = pd.read_excel("C:/Users/Cezary/Downloads/Translations/oclc_all_positive.xlsx").drop(columns=['all_names', 'cz_name', '100_unidecode']).drop_duplicates().reset_index(drop=True)

def replace_viaf_group(df):
    viaf_groups = {'256578118':'118529174', '83955898':'25095273', '2299152636076120051534':'11196637'}
    df['viaf_id'] = df['viaf_id'].replace(viaf_groups)
    return df
oclc_df = replace_viaf_group(oclc_df)
oclc_df['001'] = oclc_df['001'].astype(np.int64)
oclc_df['SRC'] = 'OCLC'

total2 = pd.concat([total, oclc_df])
total2_grouped = total2.groupby('001')
duplicates_brno_nkc_oclc = total2_grouped.filter(lambda x: len(x) > 1).sort_values('001')

duplicates_brno_nkc_oclc.to_excel('duplicates_brno_nkc_and_oclc.xlsx', index=False)













































