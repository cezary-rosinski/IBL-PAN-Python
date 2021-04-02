import pandas as pd
import io
import requests
from sickle import Sickle
import xml.etree.ElementTree as et
import lxml.etree
import pdfplumber
from google_drive_research_folders import PBL_folder
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
import gspread as gs
import json
from my_functions import cSplit, cluster_strings, marc_parser_1_field
import datetime
import regex as re
from collections import OrderedDict
import difflib
import spacy
from collections import Counter, OrderedDict
from gspread_dataframe import set_with_dataframe, get_as_dataframe
from tqdm import tqdm
import glob
import pandasql

#%% date
now = datetime.datetime.now()
year = now.year
month = now.month
day = now.day

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

#%% dictionary processing
df_650 = pd.DataFrame()
for file in tqdm(mapping_files_650):
    sheet = gc.open_by_key(file)
    df = get_as_dataframe(sheet.worksheet('deskryptory_650'), evaluate_formulas=True).dropna(how='all').dropna(how='all', axis=1)
    df_650 = df_650.append(df)

sheet = gc.open_by_key(mapping_files_655)
df_655 = get_as_dataframe(sheet.worksheet('deskryptory_655'), evaluate_formulas=True).dropna(how='all').dropna(how='all', axis=1)

df_650 = df_650[df_650['decyzja'].isin(['margines', 'zmapowane'])][['X650', 'decyzja', 'dzial_PBL_1', 'dzial_PBL_2', 'dzial_PBL_3', 'haslo_przedmiotowe_PBL_1', 'haslo_przedmiotowe_PBL_2', 'haslo_przedmiotowe_PBL_3']].reset_index(drop=True)

dict_650 = {}
for i, row in tqdm(df_650.iterrows(), total=df_650.shape[0]):
    
    lista_dzialow = row[['dzial' in i for i in row.index]].to_list()
    lista_dzialow = [e for e in lista_dzialow if pd.notnull(e)]
    lista_hasel = row[['haslo' in i for i in row.index]].to_list()
    lista_hasel = [e for e in lista_hasel if pd.notnull(e)]
    dict_650[row['X650']] = {'decyzja': row['decyzja'], 'działy PBL': lista_dzialow, 'hasła PBL': lista_hasel}

# df_655 = df_655[df_655['decyzja'].isin(['margines', 'zmapowane'])][['X655', 'decyzja', 'dzial_PBL_1', 'dzial_PBL_2', 'dzial_PBL_3', 'dzial_PBL_4', 'dzial_PBL_5', 'haslo_przedmiotowe_PBL_1', 'haslo_przedmiotowe_PBL_2', 'haslo_przedmiotowe_PBL_3', 'haslo_przedmiotowe_PBL_4', 'haslo_przedmiotowe_PBL_5']].reset_index(drop=True)

df_655 = df_655[df_655['decyzja'].isin(['zmapowane'])][['X655', 'decyzja', 'dzial_PBL_1', 'dzial_PBL_2', 'dzial_PBL_3', 'dzial_PBL_4', 'dzial_PBL_5', 'haslo_przedmiotowe_PBL_1', 'haslo_przedmiotowe_PBL_2', 'haslo_przedmiotowe_PBL_3', 'haslo_przedmiotowe_PBL_4', 'haslo_przedmiotowe_PBL_5']].reset_index(drop=True)

dict_655 = {}
for i, row in tqdm(df_655.iterrows(), total=df_655.shape[0]):
    
    lista_dzialow = row[['dzial' in i for i in row.index]].to_list()
    lista_dzialow = [e for e in lista_dzialow if pd.notnull(e)]
    lista_hasel = row[['haslo' in i for i in row.index]].to_list()
    lista_hasel = [e for e in lista_hasel if pd.notnull(e)]
    dict_655[row['X655']] = {'decyzja': row['decyzja'], 'działy PBL': lista_dzialow, 'hasła PBL': lista_hasel}

# dict_650['Poradnik']    
# dict_655['Poradnik'] 
#%% harvesting BN
    
#lista deskryptorów do wzięcia
BN_descriptors = list(dict_650.keys())
BN_descriptors.extend(dict_655.keys())
BN_descriptors = list(set([re.sub('\$y.*', '', e) for e in BN_descriptors]))
#zakres lat 
years = range(2013,2020)
   
path = 'F:/Cezary/Documents/IBL/Migracja z BN/bn_all/2021-02-08/'
files = [file for file in glob.glob(path + '*.mrk', recursive=True)]

encoding = 'utf-8'
new_list = []
for file_path in tqdm(files):
    marc_list = io.open(file_path, 'rt', encoding = encoding).read().splitlines()

    mrk_list = []
    for row in marc_list:
        if row.startswith('=LDR'):
            mrk_list.append([row])
        else:
            if row:
                mrk_list[-1].append(row)
                               
    for sublist in mrk_list:
        try:
            year = int(''.join([ele for ele in sublist if ele.startswith('=008')])[13:17])
            bibliographic_level = ''.join([ele for ele in sublist if ele.startswith('=LDR')])[13]
            if year in years and bibliographic_level == 'm':
                for el in sublist:
                    if el.startswith('=650') or el.startswith('=655'):
                        el = re.sub('\$y.*', '', el[10:]).replace('$2DBN', '')
                        if any(desc == el for desc in BN_descriptors):
                            new_list.append(sublist)
                            break
        except ValueError:
            pass

final_list = []
for lista in new_list:
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
df_original = df.copy()

# =============================================================================
# BN_desc_and_decision_650 = [[re.sub('\$y.*', '', k), v['decyzja']] for k, v in dict_650.items()]
# BN_desc_and_decision_655 = [[re.sub('\$y.*', '', k), v['decyzja']] for k, v in dict_655.items()]
# BN_desc_and_decision = BN_desc_and_decision_650 + BN_desc_and_decision_655
# BN_desc_and_decision = list(set(BN_desc_and_decision))
# BN_desc_and_decision = [list(item) for item in set(tuple(row) for row in BN_desc_and_decision)]
# BN_desc_and_decision_df = pd.DataFrame(BN_desc_and_decision, columns=['deskryptor', 'decyzja'])
# 
# marc_df_simple = marc_df[['001', '650', '655']]
# 
# query = "select * from marc_df_simple a join BN_desc_and_decision_df b on a.'650' like '%'||b.deskryptor||'%'"
# query1 = pandasql.sqldf(query)
# query = "select * from marc_df_simple a join BN_desc_and_decision_df b on a.'655' like '%'||b.deskryptor||'%'"
# query2 = pandasql.sqldf(query)
# 
# marc_df_simple = pd.concat([query1, query2]).drop_duplicates()[['001', 'decyzja', 'deskryptor']].rename(columns={'deskryptor': 'deskryptor z mapowania'})
# marc_df_simple['deskryptor z mapowania'] = marc_df_simple.groupby('001')['deskryptor z mapowania'].transform(lambda x: '❦'.join(x.dropna().drop_duplicates().astype(str)))
# marc_df_simple['decyzja'] = marc_df_simple.groupby('001')['decyzja'].transform(lambda x: '❦'.join(x.dropna().drop_duplicates().astype(str)))
# marc_df_simple = marc_df_simple.drop_duplicates().reset_index(drop=True)
# 
# marc_df = pd.merge(marc_df, marc_df_simple, on='001', how='left')
# =============================================================================

#jakimi warunkami zmniejszyć liczbę rekordów?

#%% dalsze filtrowanie
#odsianie rekordów, które nie mają nic wspólnego z PL
def czy_polonik(x):
    polish_language = x['008'][35:38] == 'pol'
    published_in_Poland = x['008'][15:17] == 'pl'
    try:
        x041 = 'pol' in x['041']
    except TypeError:
        x041 = False
    try:
        x044 = 'pol' in x['044']
    except TypeError:
        x044 = False
    if any('pol' in e.lower() for e in [x['500'], x['501'], x['546']] if pd.notnull(e)):
        pol_in_remarks = True
    else:
        pol_in_remarks = False
    if any('polsk' in e.lower() for e in [x['650'], x['655']] if pd.notnull(e)):
        pol_descriptor = True
    else:
        pol_descriptor = False
    if any([polish_language, x041, x044, pol_in_remarks, pol_descriptor]):
        return True
    else:
        return False
    
df['czy polonik'] = df.apply(lambda x: czy_polonik(x), axis=1)
df = df[df['czy polonik'] == True]

#odsianie druków ulotnych
druki_ulotne = df[(df['380'].str.contains('Druki ulotne')) & (df['380'].notnull())]['001'].to_list()
df = df[~df['001'].isin(druki_ulotne)]

#gatunki literackie
deskryptory_spoza_centrum = get_as_dataframe(sheet.worksheet('deskryptory_spoza_centrum'), evaluate_formulas=True).dropna(how='all').dropna(how='all', axis=1)['deskryptor'].to_list()
deskryptory_spoza_centrum = list(set([re.sub('\$y.*', '', e) for e in deskryptory_spoza_centrum]))
list_655 = list(set([re.sub('\$y.*', '', e) for e in dict_655.keys()]))
list_655 = [e for e in list_655 if e not in deskryptory_spoza_centrum]

def gatunki_literackie(x):
    for field in x:
        if pd.notnull(field):
            for element in field.split('❦'):
                element = re.sub('\$y.*', '', element[4:]).replace('$2DBN', '')
                if any(desc == element for desc in list_655):
                    return True
    return False

df['gatunki literackie'] = df[['650', '655']].apply(lambda x: gatunki_literackie(x), axis=1)
#zostawiamy tylko książki lub dobre gatunki literackie lub przynależność do literatury

#TUTAJ!!!!!!!!!!! podjąć decyzję

# df2 = df[(df['380'].str.lower().str.contains('książ|book', regex=True)) | (df['380'].isnull())]
# rekordy_id_bez_gatunkow_literackich = df2['001'].drop_duplicates().to_list()

df = df[(df['380'].str.lower().str.contains('książ|book', regex=True)) | 
        (df['380'].isnull()) | 
        (df['gatunki literackie'] == True)]
# rekordy_id_z_gatunkami_literackimi = df3['001'].drop_duplicates().to_list()

# roznica = list(set(rekordy_id_z_gatunkami_literackimi) - set(rekordy_id_bez_gatunkow_literackich))
# roznica_df = df_original[df_original['001'].isin(roznica)]
# roznica_df.to_excel('roznica.xlsx', index=False)

#%% dobre gatunki literackie
#jeśli coś ma gatunek literacki, to jest z automatu dobre
dobre1_df = df[(df['gatunki literackie'] == True) |
               ((df['650'].str.lower().str.contains('literatur|pisar|poezj')) & df['650'].notnull()) |
               ((df['655'].str.lower().str.contains('literatur|pisar|poezj')) & df['655'].notnull())]

#pozostałe rekordy przetwarzamy
df = df[~df['001'].isin(dobre1_df['001'])]

#%% frekwencje

deskryptory_z_df = df[['001', '655', '650']].reset_index(drop=True)

deskryptory_dict = {}
for i, row in tqdm(deskryptory_z_df.iterrows(), total=deskryptory_z_df.shape[0]):
    lista_deskryptorow = [e for e in f"{row['650']}❦{row['655']}".split('❦') if e != 'nan']
    deskryptory_dict[row['001']] = {'deskryptory w rekordzie': lista_deskryptorow, 'liczba deskryptorów': len(lista_deskryptorow), 'liczba dobrych deskryptorów': 0}
    
    for el in deskryptory_dict[row['001']]['deskryptory w rekordzie']:
        el = re.sub('\$y.*', '', el[4:]).replace('$2DBN', '')
        if any(desc == el for desc in BN_descriptors):
            deskryptory_dict[row['001']]['liczba dobrych deskryptorów'] += 1
            
    deskryptory_dict[row['001']]['procent dobrych deskryptorów'] = deskryptory_dict[row['001']]['liczba dobrych deskryptorów']/deskryptory_dict[row['001']]['liczba deskryptorów']

dobre2_lista = [k for k, v in deskryptory_dict.items() if deskryptory_dict[k]['procent dobrych deskryptorów'] > 0.5]
dobre2_df = df[df['001'].isin(dobre2_lista)]

df = df[~df['001'].isin(dobre2_lista)]

# czy sprawdzić frekwencyjnie deskryptory w df? czy czegoś nie uwzględniliśmy? - podjąć decyzję
# dodać starych ludzi 1. wziąć nazwy bn zmapowane na twórców pbl, 2. wyfiltrować tych, którzy zmarli do 1700, 3. przeszukać bazę pod kątem obecności tych ludzi w polu 100
# czy brać pod uwagę UKD?





#%% filtrowanie BN po roku zgonu

file_list = drive.ListFile({'q': f"'{PBL_folder}' in parents and trashed=false"}).GetList() 
file_list = drive.ListFile({'q': "'0B0l0pB6Tt9olWlJVcDFZQ010R0E' in parents and trashed=false"}).GetList()
file_list = drive.ListFile({'q': "'1tPr_Ly9Lf0ZwgRQjj_iNVt-T15FSWbId' in parents and trashed=false"}).GetList()
file_list = drive.ListFile({'q': "'1OwlXSNuKdrnB9qZDvM-UMh5ul1n5SeyL' in parents and trashed=false"}).GetList()
file_list = drive.ListFile({'q': "'1BT4mZ40m_M1NyYYUiMFOSMtwA8krkrYT' in parents and trashed=false"}).GetList()
#[print(e['title'], e['id']) for e in file_list]
mapowanie_osob = [file['id'] for file in file_list if file['title'].startswith('mapowanie_osob_bn_pbl')]

mapowanie_osob_df = pd.DataFrame()
for file in tqdm(mapowanie_osob):
    sheet = gc.open_by_key(file)
    df = get_as_dataframe(sheet.worksheet('pbl_bn'), evaluate_formulas=True).dropna(how='all').dropna(how='all', axis=1).drop_duplicates()
    df = df[df['czy_ten_sam'] != 'nie'][['pbl_id', 'BN_id', 'BN_name']]
    df['BN_name'] = df['BN_name'].str.replace('\|\(', ' (').str.replace('\;\|', '; ').str.replace('\|$', '')
    df['index'] = df.index + 1
    df = cSplit(df, 'index', 'BN_name', '\|').drop(columns='index')
    mapowanie_osob_df = mapowanie_osob_df.append(df)

mapowanie_osob_df = mapowanie_osob_df.drop_duplicates().reset_index(drop=True)
def rok_zgonu(x):
    try:
        return int(re.search('(?<=\- ca |\-ca |\-ok\. |\-|po )(\d+)', x).group(0))
    except (TypeError, AttributeError):
        return None
mapowanie_osob_df['rok zgonu'] = mapowanie_osob_df['BN_name'].apply(lambda x: rok_zgonu(x))
mapowanie_osob_lista = mapowanie_osob_df[(mapowanie_osob_df['rok zgonu'].notnull()) & (mapowanie_osob_df['rok zgonu'] <= 1700)]['BN_name'].drop_duplicates().to_list()

#ujednolicenie nazw z BN 100 z tabelą wzorcową
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

#zakres lat 
years = range(2013,2020)
   
path = 'F:/Cezary/Documents/IBL/Migracja z BN/bn_all/2021-02-08/'
files = [file for file in glob.glob(path + '*.mrk', recursive=True)]

encoding = 'utf-8'
new_list = []
for file_path in tqdm(files):
    marc_list = io.open(file_path, 'rt', encoding = encoding).read().splitlines()

    mrk_list = []
    for row in marc_list:
        if row.startswith('=LDR'):
            mrk_list.append([row])
        else:
            if row:
                mrk_list[-1].append(row)
            
    for sublist in mrk_list:
        try:
            year = int(''.join([ele for ele in sublist if ele.startswith('=008')])[13:17])
            bibliographic_level = ''.join([ele for ele in sublist if ele.startswith('=LDR')])[13]
            if year in years and bibliographic_level == 'm':
                for el in sublist:
                    if el.startswith('=100'):
                        el = el[6:]
                        el = marc_parser_dict_for_field(el, '\$')
                        el = ' '.join([v for k, v in el.items() if k in ['$a', '$c', '$d']])
                        if any(person == el for person in mapowanie_osob_lista):
                            new_list.append(sublist)
                            break
        except ValueError:
            pass

final_list = []
for lista in new_list:
    slownik = {}
    for el in lista:
        if el[1:4] in slownik:
            slownik[el[1:4]] += f"❦{el[6:]}"
        else:
            slownik[el[1:4]] = el[6:]
    final_list.append(slownik)

df2 = pd.DataFrame(final_list).drop_duplicates().reset_index(drop=True)
fields = df2.columns.tolist()
fields = [i for i in fields if 'LDR' in i or re.compile('\d{3}').findall(i)]
df2 = df2.loc[:, df2.columns.isin(fields)]
fields.sort(key = lambda x: ([str,int].index(type("a" if re.findall(r'\w+', x)[0].isalpha() else 1)), x))
df2 = df2.reindex(columns=fields)   
df_original2 = df2.copy()











