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
import itertools
from tqdm import tqdm
import cx_Oracle

# SQL connection

dsn_tns = cx_Oracle.makedsn('pbl.ibl.poznan.pl', '1521', service_name='xe')
connection = cx_Oracle.connect(user='IBL_SELECT', password='CR333444', dsn=dsn_tns, encoding='windows-1250')

#%% kartoteka osobowa
folder_path = 'F:/Cezary/Documents/IBL/Libri/Iteracja 2021-02/'

files = [file for file in glob.glob(folder_path + 'pbl*.mrk', recursive=True)]

encoding = 'utf-8'
new_list = []
for file_path in files:   
    marc_list = io.open(file_path, 'rt', encoding = encoding).read().splitlines()
    for field in marc_list:
        if field.startswith(('=100', '=600', '=700', '=800')):
            new_list.append(field[6:])

new_list = list(set(new_list))

df = pd.DataFrame(new_list, columns=['osoba'])
df['id'] = df.index+1

df = marc_parser_1_field(df, 'id', 'osoba', '\$').drop(columns=['osoba', 'id', 'indicator']).reset_index(drop=True).replace(r'^\s*$', np.nan, regex=True)
df = df[df['$a'].notnull()]
df['$e'] = df.apply(lambda x: 'Autor' if x['$4'] == 'aut' else x['$e'], axis=1)
df = df.drop(columns='$4')
df['$e'] = df.groupby(['$a', '$d', '$0'],dropna=False)['$e'].transform(lambda x: '❦'.join(x.dropna().drop_duplicates().astype(str))).replace(r'^\s*$', np.nan, regex=True)
df = df.drop_duplicates().reset_index(drop=True)
df['$a'] = df[['$a', '$d']].apply(lambda x: '$d'.join(x.dropna().astype(str)), axis=1)
df.drop(columns=['$d', '$i'], inplace=True)
df = df.drop_duplicates().reset_index(drop=True).rename(columns={'$a':'100', '$e':'368', '$0':'856'})

df['100'] = '1 $a' + df['100']

def add_indicator(x, indicators, marc_field):
    try:
        x = x.split('❦')
        new_field = []
        for e in x:
            e = f"{indicators}{marc_field}{e}"
            new_field.append(e)
        new_field = '❦'.join(new_field)
    except AttributeError:
        new_field = np.nan
    return new_field
        
df['368'] = df['368'].apply(lambda x: add_indicator(x, '  ', '$c'))
df['856'] = df['856'].apply(lambda x: f"40$uhttps://viaf.org/viaf/{x[6:]}/$yVIAF ID$4N" if pd.notnull(x) else x)
df['LDR'] = '-----nz--a22-----n--4500'

df_to_mrc(df, '❦', 'koha_kartoteka_wzorcowa_osob.mrc', 'koha_kartoteka_wzorcowa_osob_bledy.txt')

#%% kartoteka instytucji
folder_path = 'F:/Cezary/Documents/IBL/Libri/Iteracja 2021-02/'

files = [file for file in glob.glob(folder_path + 'pbl*.mrk', recursive=True)]

encoding = 'utf-8'
new_list = []
for file_path in files:   
    marc_list = io.open(file_path, 'rt', encoding = encoding).read().splitlines()
    for field in marc_list:
        if field.startswith(('=110', '=610', '=710', '=810')):
            new_list.append(field[6:])

new_list = list(set(new_list))

df = pd.DataFrame(new_list, columns=['110'])
df['LDR'] = '-----nz--a22-----n--4500'

df_to_mrc(df, '❦', 'koha_kartoteka_wzorcowa_instytucji.mrc', 'koha_kartoteka_wzorcowa_instytucji_bledy.txt')

#%% kartoteka czasopism (źródła + tematy)
folder_path = 'F:/Cezary/Documents/IBL/Libri/Iteracja 2021-02/'

files = [file for file in glob.glob(folder_path + 'pbl*.mrk', recursive=True)]

encoding = 'utf-8'
new_list = []
for file_path in files:   
    marc_list = io.open(file_path, 'rt', encoding = encoding).read().splitlines()
    for field in marc_list:
        if field.startswith(('=773', '=130', '=630', '=730', '=830')):
            new_list.append(field)
            
new_list = list(set(new_list))

x773 = pd.DataFrame([e[6:] for e in new_list if e.startswith('=773')], columns=['journal title'])
x773['index'] = x773.index+1
x773 = marc_parser_1_field(x773, 'index', 'journal title', '\$')[['$t', '$9']].drop_duplicates().rename(columns={'$t':'130', '$9':'rok'})

x773['rok'] = x773.groupby('130').transform(lambda x: ', '.join(x))
x773['130'] = '\\\\$a' + x773['130']
x773 = x773.drop_duplicates()

def ranges(i):
    for a, b in itertools.groupby(enumerate(i), lambda pair: pair[1] - pair[0]):
        b = list(b)
        if len(b) == 1 and b[0][1] == b[-1][1]:
            yield f"{b[0][1]}"
        else:
            yield f"{b[0][1]}-{b[-1][1]}"
            
x773['rok'] = x773['rok'].apply(lambda x: ', '.join(list(ranges(sorted([int(i) for i in x.split(', ') if i])))))

def rok(x, order='pierwszy'):
    if order == 'pierwszy':
        try:
            return '$s' + re.findall('\d{4}', x)[0]
        except IndexError:
            return np.nan
    elif order == 'ostatni':
        try: 
            return '$t' + re.findall('\d{4}', x)[-1]
        except IndexError:
            return np.nan
    else:
        print('Wrong order!')
        
def lata(x):
    pierwszy = rok(x)
    ostatni = rok(x, order='ostatni')
    if isinstance(pierwszy, str) and isinstance(ostatni, str):
        return '\\\\' + ''.join([rok(x), rok(x, order='ostatni')])
    elif isinstance(pierwszy, str):
        return '\\\\' + pierwszy
    elif isinstance(ostatni, str):
        return '\\\\' + ostatni
    else:
        return np.nan

x773['046'] = x773['rok'].apply(lambda x: lata(x))
x773 = x773[['130', '046']]
x773['667'] = '\\\\$aŹródło'
x773['LDR'] = '-----nz--a22-----n--4500'
# ogarnąć temat czasopism jako tematów, bo na ten moment w danych libri nie ma informacji o odwołaniach z bazy Oracle
#inne = pd.DataFrame([e[6:] for e in new_list if not e.startswith('=773')], columns=['journal title'])

#odwołania z ORACLE'a

pbl_journals_query = """select ZA_TYTUL
                    from IBL_OWNER.pbl_zapisy z
                    where (z.za_rz_rodzaj1_id = 641 or z.za_rz_rodzaj2_id = 641)
                    and (z.ZA_DZ_DZIAL1_ID in (1643, 1644, 1645, 1646, 3183) or z.ZA_DZ_DZIAL2_ID in (1643, 1644, 1645, 1646, 3183))"""                

pbl_journals = pd.read_sql(pbl_journals_query, con=connection).fillna(value = np.nan)
pbl_journals['tytuł'] = pbl_journals['ZA_TYTUL'].apply(lambda x: re.findall('(.*?)(?=\(|$)', x)[0].strip())
def get_place(x):
    try:
        return re.findall('(?<=\()(.*?)(?=\d|\))', x)[0].strip()
    except IndexError:
        pass
pbl_journals['miejsce_wydania'] = pbl_journals['ZA_TYTUL'].apply(lambda x: get_place(x))
pbl_journals['miejsce_wydania'] = pbl_journals['miejsce_wydania'].apply(lambda x: re.sub(', od| od|,od', '', ', '.join([e.strip() for e in x.split(',', x.count(',')) if e])) if x else None)
#	ZA_TYTUL	tytuł	miejsce_wydania
# 802	Er(r)go (Katowice 2000 - )	Er	r

pbl_journals['lata_wydania']

', '.join([e.strip() for e in 'Piekary, Bytom,'.split(',', 'Piekary, Bytom,'.count(',')) if e])

df_to_mrc(x773, '❦', 'koha_kartoteka_wzorcowa_czasopism.mrc', 'koha_kartoteka_wzorcowa_czasopism_bledy.txt')

#%% kartoteka utworów



















