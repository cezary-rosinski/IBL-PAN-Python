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

df = marc_parser_1_field(df, 'id', 'osoba', '\$').drop(columns=['osoba', 'id', 'indicator']).reset_index(drop=True)
df = df[df['$a'].notnull()]
df['$e'] = df.apply(lambda x: 'Autor' if x['$4'] == 'aut' else x['$e'], axis=1)
df = df.drop(columns='$4')
df['$e'] = df.groupby(['$a', '$d', '$0'])['$e'].transform(lambda x: '❦'.join(x.drop_duplicates().astype(str)))
df = df.replace(r'^\s*$', np.nan, regex=True)
df['$a'] = df[['$a', '$d']].apply(lambda x: '$d'.join(x.dropna().astype(str)), axis=1)
df.drop(columns='$d', inplace=True)

df = df.drop_duplicates().reset_index(drop=True).rename(columns={'$a':'100', '$e':'368', '$0':'856'})
df['100'] = '1 $a' + df['100']
df['368'] = '  $c' + df['368']
df['856'] = df['856'].apply(lambda x: f"40$uhttps://viaf.org/viaf/{x[6:]}/$yVIAF ID$4N" if pd.notnull(x) else x)
df['LDR'] = '-----nz--a22-----n--4500'

df_to_mrc(df, '❦', 'koha_kartoteka_wzorcowa_osob.mrc', 'koha_kartoteka_wzorcowa_osob_bledy.txt')



