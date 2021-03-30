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
from my_functions import cSplit, cluster_strings
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
mapping_files = [file['id'] for file in file_list if file['title'].startswith('mapowanie BN-Oracle')]
mapping_files_650 = mapping_files[:-1]
mapping_files_655 = mapping_files[-1]

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

df_655 = df_655[df_655['decyzja'].isin(['margines', 'zmapowane'])][['X655', 'decyzja', 'dzial_PBL_1', 'dzial_PBL_2', 'dzial_PBL_3', 'dzial_PBL_4', 'dzial_PBL_5', 'haslo_przedmiotowe_PBL_1', 'haslo_przedmiotowe_PBL_2', 'haslo_przedmiotowe_PBL_3', 'haslo_przedmiotowe_PBL_4', 'haslo_przedmiotowe_PBL_5']].reset_index(drop=True)

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

BN_descriptors = list(dict_650.keys())
BN_descriptors.extend(dict_655.keys())
BN_descriptors = list(set([re.sub('\$y.*', '', e) for e in BN_descriptors]))
 
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
                        if any(desc in el for desc in BN_descriptors):
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

marc_df = pd.DataFrame(final_list).drop_duplicates().reset_index(drop=True)
fields = marc_df.columns.tolist()
fields = [i for i in fields if 'LDR' in i or re.compile('\d{3}').findall(i)]
marc_df = marc_df.loc[:, marc_df.columns.isin(fields)]
fields.sort(key = lambda x: ([str,int].index(type("a" if re.findall(r'\w+', x)[0].isalpha() else 1)), x))
marc_df = marc_df.reindex(columns=fields)   

BN_desc_and_decision_650 = [[re.sub('\$y.*', '', k), v['decyzja']] for k, v in dict_650.items()]
BN_desc_and_decision_655 = [[re.sub('\$y.*', '', k), v['decyzja']] for k, v in dict_655.items()]
BN_desc_and_decision = BN_desc_and_decision_650 + BN_desc_and_decision_655
BN_desc_and_decision = list(set(BN_desc_and_decision))
BN_desc_and_decision = [list(item) for item in set(tuple(row) for row in BN_desc_and_decision)]
BN_desc_and_decision_df = pd.DataFrame(BN_desc_and_decision, columns=['deskryptor', 'decyzja'])

marc_df_simple = marc_df[['001', '650', '655']]

query = "select * from marc_df_simple a join BN_desc_and_decision_df b on a.'650' like '%'||b.deskryptor||'%'"
test = pandasql.sqldf(query)
query = "select * from marc_df_simple a join BN_desc_and_decision_df b on a.'655' like '%'||b.deskryptor||'%'"
test2 = pandasql.sqldf(query)

test = pd.concat([test, test2]).drop_duplicates()
test = test[['001', 'decyzja']]
test['decyzja'] = test.groupby('001')['decyzja'].transform(lambda x: '❦'.join(x.dropna().drop_duplicates().astype(str)))
test = test.drop_duplicates().reset_index(drop=True)


test = pd.merge(marc_df, test, on='001', how='left')

margines = test[~test['decyzja'].str.contains('zmapowane')]
zmapowane = test[test['decyzja'].str.contains('zmapowane')]


margines.to_excel('libri_margines.xlsx', index=False)
zmapowane.to_excel('libri_zmapowane.xlsx', index=False)




dict_650['Gry fabularne$xstosowanie$xnauczanie']

#jakimi warunkami zmniejszyć liczbę rekordów?

#%% live coding z Karoliną
df = pd.read_excel('libri_zmapowane.xlsx')

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

#nie_polonik = df[df['czy polonik'] == False]

df = df[df['czy polonik'] == True]
df.to_excel('libri_zmapowane2.xlsx', index=False)

#twarda lista gatunków, które idą na 100%
#dodać warunek z listy deskryptorów 655
# =============================================================================
# dalsze kroki:
#     - jeśli =LUB(CZY.LICZBA(ZNAJDŹ("książ";LITERY.MAŁE([@380])));CZY.LICZBA(ZNAJDŹ("book";LITERY.MAŁE([@380])))) == Fałsz, to wywalamy
#     - przejrzeć w szczegółach "type of record"
# =============================================================================











nie_polonik.to_excel('nie_polonik.xlsx', index=False)


df['czy polonik'].to_list()    


#usunięcie zagranicznych zapisów, które nie są polonikami 
#na podstawie braku wystąpień frazy "pol" w polach MARC
nie_poloniki <- bn_ok %>%
  filter(if (X501=="") !grepl("pl",substr(X008,16,18))) %>%
  filter(!grepl("pol",substr(X008,36,38))) %>%
  filter(!grepl("pol",X041)) %>%
  filter(!grepl("pl",X044)) %>%
  filter(!grepl("pol",X500,ignore.case = TRUE)) %>%
  filter(!grepl("pol",X501,ignore.case = TRUE)) %>%
  filter(!grepl("pol",X546,ignore.case = TRUE)) %>%
  select(id) %>%
  mutate(czy_polonik = "nie") %>%
  unique()

test = df[df['501'].notnull()]['501']
test = test[test.str.lower().str.contains('pol')]
















