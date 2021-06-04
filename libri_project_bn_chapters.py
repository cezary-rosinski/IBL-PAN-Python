#%% import
import pandas as pd
import numpy as np
from my_functions import marc_parser_1_field, cSplit, df_to_mrc, gsheet_to_df, mrc_to_mrk, f
import pandasql
import json
import requests
import io
import cx_Oracle
import regex as re
from functools import reduce
import glob
from json.decoder import JSONDecodeError
from urllib.parse import urlparse
import datetime
import ast
from tqdm import tqdm
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
import gspread as gs
from google_drive_research_folders import PBL_folder
from gspread_dataframe import get_as_dataframe, set_with_dataframe

#%% date
now = datetime.datetime.now()
year = now.year
month = '{:02}'.format(now.month)
day = '{:02}'.format(now.day)

#%% google authentication & google drive
#autoryzacja do tworzenia i edycji plików
gc = gs.oauth()
#autoryzacja do penetrowania dysku
gauth = GoogleAuth()
gauth.LocalWebserverAuth()
drive = GoogleDrive(gauth)

file_list = drive.ListFile({'q': f"'{PBL_folder}' in parents and trashed=false"}).GetList() 
file_list = drive.ListFile({'q': "'0B0l0pB6Tt9olWlJVcDFZQ010R0E' in parents and trashed=false"}).GetList()
file_list = drive.ListFile({'q': "'1tPr_Ly9Lf0ZwgRQjj_iNVt-T15FSWbId' in parents and trashed=false"}).GetList()
file_list = drive.ListFile({'q': "'1xzqGIfZllmXXTh2dJABeHbRPFAM34nbw' in parents and trashed=false"}).GetList()
#[print(e['title'], e['id']) for e in file_list]
mapping_files_655 = [file['id'] for file in file_list if file['title'] == 'mapowanie BN-Oracle - 655'][0]
mapping_files_650 = [file['id'] for file in file_list if file['title'].startswith('mapowanie BN-Oracle') if file['id'] != mapping_files_655]

#%% deskryptory do harvestowania BN
#lista deskryptorów do wzięcia - wąska (z selekcji Karoliny)
deskryptory_do_filtrowania = [file['id'] for file in file_list if file['title'] == 'deskryptory_do_filtrowania'][0]
deskryptory_do_filtrowania = gc.open_by_key(deskryptory_do_filtrowania)
deskryptory_do_filtrowania = get_as_dataframe(deskryptory_do_filtrowania.worksheet('deskryptory_do_filtrowania'), evaluate_formulas=True).dropna(how='all').dropna(how='all', axis=1)
BN_descriptors = deskryptory_do_filtrowania[deskryptory_do_filtrowania['deskryptor do filtrowania'] == 'tak']['deskryptory'].to_list()
def uproszczenie_nazw(x):
    try:
        if x.index('$') == 0:
            return x[2:]
        elif x.index('$') == 1:
            return x[4:]
    except ValueError:
        return x
BN_descriptors = list(set([e.strip() for e in BN_descriptors]))
BN_descriptors2 = list(set(uproszczenie_nazw(e) for e in BN_descriptors))
roznica = list(set(BN_descriptors2) - set(BN_descriptors))
BN_descriptors.extend(roznica)

#%% BN data extraction

years = range(1989,2021)
   
path = 'F:/Cezary/Documents/IBL/BN/bn_all/2021-02-08/'
files = [file for file in glob.glob(path + '*.mrk', recursive=True)]

encoding = 'utf-8'
new_list = []
for file_path in tqdm(files):
    # file_path = 'F:/Cezary/Documents/IBL/BN/bn_all/2021-02-08\msplit00000024.mrk'
    marc_list = io.open(file_path, 'rt', encoding = encoding).read().splitlines()

    mrk_list = []
    for row in marc_list:
        if row.startswith('=LDR'):
            mrk_list.append([row])
        else:
            if row:
                mrk_list[-1].append(row)
                
    # for sublist in mrk_list:
    #     for el in sublist:
    #         if el.startswith(('=001', '=009')):
    #             if 'b0000006030917' in el:
    #                 print(file_path)
    #                 new_list2.append(sublist)
                               
    for sublist in mrk_list:
        try:
            year_biblio = int(''.join([ele for ele in sublist if ele.startswith('=008')])[13:17])
            bibliographic_level = ''.join([ele for ele in sublist if ele.startswith('=LDR')])[13]
            if year_biblio in years and bibliographic_level == 'a':
                for el in sublist:
                    if el.startswith('=650') or el.startswith('=655'):
                        el = re.sub('\$y.*', '', el[10:]).replace('$2DBN', '').strip()
                        if any(desc == el for desc in BN_descriptors):
                            new_list.append(sublist)
                            break
        except ValueError:
            pass

#opracować reguły filtrowania - jak wydobyć tylko to, co jest rozdziałem w zbiorówce?
test = [e for e in new_list if any('=773' in s for s in e)]

final_list = []
for lista in test:
    slownik = {}
    for el in lista:
        if el[1:4] in slownik:
            slownik[el[1:4]] += f"❦{el[6:]}"
        else:
            slownik[el[1:4]] = el[6:]
    final_list.append(slownik)

df = pd.DataFrame(final_list).drop_duplicates().reset_index(drop=True)
fields = df.columns.tolist()
fields = [i for i in fields if 'LDR' in i or re.compile('\d{3}').findall(i)]
df = df.loc[:, df.columns.isin(fields)]
fields.sort(key = lambda x: ([str,int].index(type("a" if re.findall(r'\w+', x)[0].isalpha() else 1)), x))
df = df.reindex(columns=fields)   

x773 = marc_parser_1_field(df, '001', '773', '\$')
x773['s'] = x773['$g'].apply(lambda x: x[0] == 's' if x != '' else False)
x773 = x773[x773['s'] == True]['001'].to_list()

df = df[df['001'].isin(x773)]

df.to_excel(f'bn_harvested_chapters_{year}_{month}_{day}.xlsx', index=False)
df_to_mrc(df, '❦', f'marc_df_chapters_{year}_{month}_{day}.mrc', f'marc_df_articles_errors_{year}_{month}_{day}.txt')










df = pd.DataFrame(test).drop_duplicates().reset_index(drop=True)
df = marc_parser_1_field(df, '001', '773', '\$')

#to nie działa, bo są też np. rekordy z $g2013
test = [e for e in test if any(re.findall('^=773(?!.*\(\d{4}\)).*$', s) for s in e)]

test = df[df['245'].str.contains('Bogactwo')]





[e for e in BN_descriptors if e and 'Socjologia literatury' in e]
if any(desc == el for desc in BN_descriptors):
    print(el)
[e for e in new_list if '=001  b0000006030917' == e[1]]














