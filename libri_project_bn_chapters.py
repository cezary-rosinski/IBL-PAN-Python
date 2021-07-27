#%% import
import pandas as pd
import numpy as np
from my_functions import marc_parser_1_field, cSplit, df_to_mrc, gsheet_to_df, mrc_to_mrk, f, simplify_string
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
import warnings
import pickle
from functools import reduce

warnings.simplefilter(action='ignore', category=FutureWarning)

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

#%% pobranie pliku z relacjami
chapters_relations_sheet = gc.open_by_key('1hNQh9tS2c1oY10lxk9fOgSyB9Xmtgrgn6kFYo9ASnqo')
chapters_relations_df = get_as_dataframe(chapters_relations_sheet.worksheet('relations'), evaluate_formulas=True).dropna(how='all').dropna(how='all', axis=1)
chapters = chapters_relations_df[chapters_relations_df['type'] == 'chapter']
chapters_id = tuple(chapters['id'].to_list())

#%% wydobycie poprawnych rozdziałów na podstawie pliku z relacjami

path = 'F:/Cezary/Documents/IBL/BN/bn_all/2021-02-08/'
files = [file for file in glob.glob(path + '*.mrk', recursive=True)]

encoding = 'utf-8'
new_list = []
for file_path in tqdm(files):
    # file_path = files[-1]
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
            for el in sublist:
                if el.startswith('=001') or el.startswith('=009'):
                    el = el[6:]
                    if el in chapters_id:
                        new_list.append(sublist)
                        break
        except ValueError:
            pass

final_list = []
for lista in tqdm(new_list):
    slownik = {}
    for el in lista:
        if el[1:4] in slownik:
            slownik[el[1:4]] += f"❦{el[6:]}"
        else:
            slownik[el[1:4]] = el[6:]
    final_list.append(slownik)

df = pd.DataFrame(final_list).drop_duplicates().reset_index(drop=True)
fields = df.columns.tolist()
fields = [i for i in fields if any(a == i for a in ['LDR', 'AVA']) or re.compile('\d{3}').findall(i)]
df = df.loc[:, df.columns.isin(fields)]
fields.sort(key = lambda x: ([str,int].index(type("a" if re.findall(r'\w+', x)[0].isalpha() else 1)), x))
df = df.reindex(columns=fields)   

# df.to_excel('BN_chapters.xlsx', index=False)
# #zobaczyć, co się dzieje z 005 - notacja naukowa
# df = pd.read_excel('BN_chapters.xlsx')


try:
    df = pd.merge(df.drop(columns='856'), chapters.drop(columns='type'), how='left', left_on='001', right_on='id').drop(columns='id')
except KeyError:
    df = pd.merge(df, chapters.drop(columns='type'), how='left', left_on='001', right_on='id').drop(columns='id')

df_to_mrc(df, '❦', f'libri_marc_bn_chapters_{year}-{month}-{day}.mrc', f'libri_bn_chapters_errors_{year}-{month}-{day}.txt')
mrc_to_mrk(f'libri_marc_bn_chapters_{year}-{month}-{day}.mrc', f'libri_marc_bn_chapters_{year}-{month}-{day}.mrk')

# wczytanie pliku z błędami
# rozwiązanie kwestii wielu tomów i tytułów tomów

with open(f'libri_bn_chapters_errors_{year}-{month}-{day}.txt', encoding='utf8') as file:
    errors = file.readlines()
    
errors = [ast.literal_eval(re.findall('\{.+\}', e)[0]) for e in errors if e != '\n']
errors = [{(k):(v if k not in ['856', 856] else re.sub('(\:|\=)(❦)', r'\1 ',v)) for k,v in e.items()} for e in errors]

df2 = pd.DataFrame(errors)
df_to_mrc(df2, '❦', f'libri_marc_bn_chapters2_{year}-{month}-{day}.mrc', f'libri_bn_chapters2_errors_{year}-{month}-{day}.txt')
mrc_to_mrk(f'libri_marc_bn_chapters2_{year}-{month}-{day}.mrc', f'libri_marc_bn_chapters2_{year}-{month}-{day}.mrk')


# =============================================================================
# # proces przygotowania rozdziałów i książek z relacjami - do powtórzenia za jakiś czas
# #%% deskryptory do harvestowania BN
# file_list = drive.ListFile({'q': f"'{PBL_folder}' in parents and trashed=false"}).GetList() 
# file_list = drive.ListFile({'q': "'0B0l0pB6Tt9olWlJVcDFZQ010R0E' in parents and trashed=false"}).GetList()
# file_list = drive.ListFile({'q': "'1tPr_Ly9Lf0ZwgRQjj_iNVt-T15FSWbId' in parents and trashed=false"}).GetList()
# file_list = drive.ListFile({'q': "'1xzqGIfZllmXXTh2dJABeHbRPFAM34nbw' in parents and trashed=false"}).GetList()
# #[print(e['title'], e['id']) for e in file_list]
# mapping_files_655 = [file['id'] for file in file_list if file['title'] == 'mapowanie BN-Oracle - 655'][0]
# mapping_files_650 = [file['id'] for file in file_list if file['title'].startswith('mapowanie BN-Oracle') if file['id'] != mapping_files_655]
# #%% deskryptory do harvestowania BN
# #lista deskryptorów do wzięcia - wąska (z selekcji Karoliny)
# deskryptory_do_filtrowania = [file['id'] for file in file_list if file['title'] == 'deskryptory_do_filtrowania'][0]
# deskryptory_do_filtrowania = gc.open_by_key(deskryptory_do_filtrowania)
# deskryptory_do_filtrowania = get_as_dataframe(deskryptory_do_filtrowania.worksheet('deskryptory_do_filtrowania'), evaluate_formulas=True).dropna(how='all').dropna(how='all', axis=1)
# BN_descriptors = deskryptory_do_filtrowania[deskryptory_do_filtrowania['deskryptor do filtrowania'] == 'tak']['deskryptory'].to_list()
# def uproszczenie_nazw(x):
#     try:
#         if x.index('$') == 0:
#             return x[2:]
#         elif x.index('$') == 1:
#             return x[4:]
#     except ValueError:
#         return x
# BN_descriptors = list(set([e.strip() for e in BN_descriptors]))
# BN_descriptors2 = list(set(uproszczenie_nazw(e) for e in BN_descriptors))
# roznica = list(set(BN_descriptors2) - set(BN_descriptors))
# BN_descriptors.extend(roznica)
# 
# #%% BN data harvesting for chapters
# 
# years = range(1989,2021)
#    
# path = 'F:/Cezary/Documents/IBL/BN/bn_all/2021-02-08/'
# files = [file for file in glob.glob(path + '*.mrk', recursive=True)]
# 
# encoding = 'utf-8'
# new_list = []
# for file_path in tqdm(files):
#     # file_path = 'F:/Cezary/Documents/IBL/BN/bn_all/2021-02-08\msplit00000024.mrk'
#     marc_list = io.open(file_path, 'rt', encoding = encoding).read().splitlines()
# 
#     mrk_list = []
#     for row in marc_list:
#         if row.startswith('=LDR'):
#             mrk_list.append([row])
#         else:
#             if row:
#                 mrk_list[-1].append(row)
#                 
#     # for sublist in mrk_list:
#     #     for el in sublist:
#     #         if el.startswith(('=001', '=009')):
#     #             if 'b0000006030917' in el:
#     #                 print(file_path)
#     #                 new_list2.append(sublist)
#                                
#     for sublist in mrk_list:
#         try:
#             year_biblio = int(''.join([ele for ele in sublist if ele.startswith('=008')])[13:17])
#             bibliographic_level = ''.join([ele for ele in sublist if ele.startswith('=LDR')])[13]
#             if year_biblio in years and bibliographic_level == 'a':
#                 for el in sublist:
#                     if el.startswith('=650') or el.startswith('=655'):
#                         el = re.sub('\$y.*', '', el[10:]).replace('$2DBN', '').strip()
#                         if any(desc == el for desc in BN_descriptors):
#                             new_list.append(sublist)
#                             break
#         except ValueError:
#             pass
# 
# #opracować reguły filtrowania - jak wydobyć tylko to, co jest rozdziałem w zbiorówce?
# new_list = [e for e in new_list if any('=773' in s for s in e)]
# 
# final_list = []
# for lista in new_list:
#     slownik = {}
#     for el in lista:
#         if el[1:4] in slownik:
#             slownik[el[1:4]] += f"❦{el[6:]}"
#         else:
#             slownik[el[1:4]] = el[6:]
#     final_list.append(slownik)
# 
# df = pd.DataFrame(final_list).drop_duplicates().reset_index(drop=True)
# fields = df.columns.tolist()
# fields = [i for i in fields if any(a == i for a in ['LDR', 'AVA']) or re.compile('\d{3}').findall(i)]
# df = df.loc[:, df.columns.isin(fields)]
# fields.sort(key = lambda x: ([str,int].index(type("a" if re.findall(r'\w+', x)[0].isalpha() else 1)), x))
# df = df.reindex(columns=fields)   
# 
# x773 = marc_parser_1_field(df, '001', '773', '\$')
# x773['s'] = x773['$g'].apply(lambda x: x[0] == 's' if x != '' else False)
# x773 = x773[x773['s'] == True]['001'].to_list()
# 
# df = df[df['001'].isin(x773)]
# 
# df.to_excel(f'bn_harvested_chapters_{year}_{month}_{day}.xlsx', index=False)
# df_to_mrc(df, '❦', f'marc_df_chapters_{year}_{month}_{day}.mrc', f'marc_df_articles_errors_{year}_{month}_{day}.txt')
# 
# df = pd.read_excel('bn_harvested_chapters_2021_05_27.xlsx')
# df['year'] = df['008'].apply(lambda x: x[7:11])
# 
# x773 = marc_parser_1_field(df, '001', '773', '\$')[['001', '$t', '$w']].rename(columns={'$t': 'book title', '$w': 'book id'})
# x773['book title'] = x773['book title'].replace('\.$', '', regex=True)
# x773['book id'] = x773['book id'].apply(lambda x: re.findall('b\d{13}', x)[0] if re.findall('b\d{13}', x) else '')
# 
# chapters_and_books = x773[x773['book id'] != ''][['001', 'book id']].rename(columns={'001': 'chapter id'})
# df = pd.merge(df, x773, on='001', how='left')
# df = df[df['book id'] == '']
# chapters_with_missing_books = df[df['book title'] == '']['001'].to_list()
# df = df[~df['001'].isin(chapters_with_missing_books)]
# 
# #%% BN all books harvesting
# 
# years = range(1989,2021)
#    
# path = 'F:/Cezary/Documents/IBL/BN/bn_all/2021-02-08/'
# files = [file for file in glob.glob(path + '*.mrk', recursive=True)]
# 
# encoding = 'utf-8'
# new_list = []
# for file_path in tqdm(files):
#     marc_list = io.open(file_path, 'rt', encoding = encoding).read().splitlines()
# 
#     mrk_list = []
#     for row in marc_list:
#         if row.startswith('=LDR'):
#             mrk_list.append([row])
#         else:
#             if row:
#                 mrk_list[-1].append(row)
#                                
#     for sublist in mrk_list:
#         try:
#             year_biblio = int(''.join([ele for ele in sublist if ele.startswith('=008')])[13:17])
#             bibliographic_level = ''.join([ele for ele in sublist if ele.startswith('=LDR')])[13]
#             if year_biblio in years and bibliographic_level == 'm':
#                 new_list.append(sublist)
#         except ValueError:
#             pass
# 
# marc_fields_for_books = ['001', '008', '009', '245']
# 
# final_list_books = []
# for lista in tqdm(new_list):
#     slownik = {}
#     for el in lista:
#         if el[1:4] in marc_fields_for_books:
#             if el[1:4] in slownik:
#                 slownik[el[1:4]] += f"❦{el[6:]}"
#             else:
#                 slownik[el[1:4]] = el[6:]
#     final_list_books.append(slownik)
# 
# df_books = pd.DataFrame(final_list_books).drop_duplicates().reset_index(drop=True)
# fields = df_books.columns.tolist()
# fields = [i for i in fields if any(a in ['LDR', 'AVA'] for a in i) or re.compile('\d{3}').findall(i)]
# df_books = df_books.loc[:, df_books.columns.isin(fields)]
# fields.sort(key = lambda x: ([str,int].index(type("a" if re.findall(r'\w+', x)[0].isalpha() else 1)), x))
# df_books = df_books.reindex(columns=fields)   
# 
# x245 = marc_parser_1_field(df_books, '001', '245', '\$')[['001', '$a', '$b', '$n', '$p']].replace(r'^\s*$', np.NaN, regex=True)
# x245 = x245.replace(r'^\s*$', np.NaN, regex=True)
# x245['book title'] = x245[x245.columns[1:]].apply(
#     lambda x: ' '.join(x.dropna().astype(str)),
#     axis=1
# )
# x245 = x245[['001', 'book title']]
# x245['book title'] = x245['book title'].replace('\/$', '', regex=True).str.strip().replace('\.$', '', regex=True)
# 
# df_books['year'] = df_books['008'].apply(lambda x: x[7:11])
# df_books = pd.merge(df_books.drop(columns=['008', '009', '245']), x245, on='001', how='left')
# 
# df_simple = df[['001', 'year', 'book title']]
# 
# #df to list of tuples
# df_books['simple'] = df_books['book title'].apply(lambda x: simplify_string(x, with_spaces=False))
# df_simple['simple'] = df_simple['book title'].apply(lambda x: simplify_string(x, with_spaces=False))
# 
# list_of_books = [tuple(r) for r in df_books.to_numpy()]
# list_of_chapters = [tuple(r) for r in df_simple.to_numpy()]
# 
# final_list_of_tuples = []
# # schemat: id rozdziału, id książki, liczba połączeń
# for chapter_id, chapter_year, chapter_title, simple_title in tqdm(list_of_chapters, total=len(list_of_chapters)):
#     query_list = [e for e in list_of_books if simple_title in e[-1]]
#                   # and e[1] == chapter_year]
#     if len(query_list) == 1:
#         tup = (chapter_id, query_list[0][0], 1)
#         final_list_of_tuples.append(tup)
#     elif len(query_list) > 1:
#         for tu in query_list:
#             tup = (chapter_id, tu[0], len(query_list))
#             final_list_of_tuples.append(tup)
#     else:
#         chapters_with_missing_books.append(chapter_id)
#         
# chapters_and_books2 = pd.DataFrame(final_list_of_tuples, columns=['chapter id', 'book id', 'frequency'])
# multiple_maches = chapters_and_books2[chapters_and_books2['frequency'] > 1]
# chapters_and_books2 = chapters_and_books2[chapters_and_books2['frequency'] == 1]
# chapters_with_missing_books = pd.DataFrame(chapters_with_missing_books, columns = ['chapters_with_missing_books'])
# chapters_and_books = pd.concat([chapters_and_books, chapters_and_books2.drop(columns='frequency')])
# 
# #%% creating 856 field with books (for chapters)
# path = 'F:/Cezary/Documents/IBL/BN/bn_all/2021-02-08/'
# files = [file for file in glob.glob(path + '*.mrk', recursive=True)]
# 
# encoding = 'utf-8'
# new_list = []
# for file_path in tqdm(files):
#     marc_list = io.open(file_path, 'rt', encoding = encoding).read().splitlines()
# 
#     mrk_list = []
#     for row in marc_list:
#         if row.startswith('=LDR'):
#             mrk_list.append([row])
#         else:
#             if row:
#                 mrk_list[-1].append(row)
#                                 
#     for sublist in mrk_list:
#         try:
#             for el in sublist:
#                 if el.startswith('=001') or el.startswith('=009'):
#                     el = el[6:]
#                     if el in books:
#                         new_list.append(sublist)
#                         break
#         except ValueError:
#             pass
# 
# final_list = []
# for lista in tqdm(new_list):
#     slownik = {}
#     for el in lista:
#         if el[1:4] in slownik:
#             slownik[el[1:4]] += f"❦{el[6:]}"
#         else:
#             slownik[el[1:4]] = el[6:]
#     final_list.append(slownik)
# 
# books_df = pd.DataFrame(final_list).drop_duplicates().reset_index(drop=True)
# fields = books_df.columns.tolist()
# fields = [i for i in fields if any(a == i for a in ['LDR', 'AVA']) or re.compile('\d{3}').findall(i)]
# books_df = books_df.loc[:, books_df.columns.isin(fields)]
# fields.sort(key = lambda x: ([str,int].index(type("a" if re.findall(r'\w+', x)[0].isalpha() else 1)), x))
# books_df = books_df.reindex(columns=fields)   
# 
# #czy wydobywamy redaktorów? - NA PÓŹNIEJ
# 
# # title
# x245 = marc_parser_1_field(books_df, '001', '245', '\$')[['001', '$a', '$b', '$n', '$p']].replace(r'^\s*$', np.NaN, regex=True)
# x245 = x245.replace(r'^\s*$', np.NaN, regex=True)
# x245['book title'] = x245[x245.columns[1:]].apply(
#     lambda x: ' '.join(x.dropna().astype(str)),
#     axis=1
# )
# x245 = x245[['001', 'book title']]
# x245['book title'] = x245['book title'].replace('\/$', '', regex=True).str.strip().replace('\.$', '', regex=True)
# # publishing house, place and year
# 
# publication = books_df.copy()[['001', '260']]
# publication = publication[publication['260'].notnull()].drop_duplicates().reset_index(drop=True)
# publication['podpola'] = publication['260'].apply(lambda x: ''.join(re.findall('(?<=\$)[a,b]', x)))
# publication['podpola'] = publication['podpola'].apply(lambda x: re.sub('([^a]*a[^a]+)', r'\1❦', x)).str.replace('^❦', '').str.replace('❦$', '')
# 
# def podpola_264(x):
#     podpola = list(x['podpola'])
#     podpole_indeks = 0
#     val = []
#     for podpole in podpola:
#         try:
#             podpole_indeks = x['260'].index(f"${podpole}", podpole_indeks)
#             val.append(str(podpole_indeks))
#         except ValueError:
#             val.append('')
#     val = [int(val[i+1]) if v == '' else v for i, v in enumerate(val)]
#     ile_heder = 0
#     for i, p in zip(val, podpola):
#         if p == '❦':
#             i += ile_heder
#             x['260'] = x['260'][:i] + '❦' + x['260'][i:]
#             ile_heder += 1
#     return x['260']
# 
# publication['260'] = publication.apply(lambda x: podpola_264(x), axis=1)
# 
# publication = cSplit(publication, '001', '260', '❦')
# publication['podpola'] = publication['260'].apply(lambda x: ''.join(re.findall('(?<=\$)[a,b]', x)))
# 
# def adres_wydawniczy(x):
#     if x['podpola'].count('a') == 1 and x['podpola'].count('b') == 1:
#         final_value = x['260']
#     elif x['podpola'].count('a') == 1 and x['podpola'].count('b') > 1:
#         grupa_a = re.findall('(\$a.*?)(?=\$b)', x['260'])[0]
#         podpola_b = re.sub('([^b]*b)', r'❦\1', x['podpola'])
#         podpola_b = list(re.sub('^❦', '', podpola_b))
#         podpole_indeks = 0
#         val = []
#         for podpole in podpola_b:
#             try:
#                 podpole_indeks = x['260'].index(f"${podpole}", podpole_indeks)
#                 val.append(str(podpole_indeks))
#             except ValueError:
#                 val.append('')  
#                 podpole_indeks += 1
#         val = [int(val[i+1]) if v == '' else int(v) for i, v in enumerate(val)]
#         ile_heder = 0
#         for i, p in zip(val, podpola_b):
#             if p == '❦':
#                 i += ile_heder
#                 x['260'] = x['260'][:i] + '❦' + x['260'][i:]
#                 ile_heder += 1    
#         final_value = x['260'].replace('❦', grupa_a)
#     elif x['podpola'].count('a') > 1 and x['podpola'].count('b') == 1:
#         final_value = re.sub('(\s+\W\$a)', ', ', x['260'])
#     elif x['podpola'].count('a') > 1 and x['podpola'].count('b') > 1:
#         x['260'] = re.sub('(\s+\W\$a)', ', ', x['260'])
#         grupa_a = re.findall('(\$a.*?)(?=\$b)', x['260'])[0]
#         podpola_b = re.sub('([^b]*b)', r'❦\1', x['podpola'])
#         podpola_b = list(re.sub('^❦', '', podpola_b))
#         podpole_indeks = 0
#         val = []
#         for podpole in podpola_b:
#             try:
#                 podpole_indeks = x['260'].index(f"${podpole}", podpole_indeks)
#                 val.append(str(podpole_indeks))
#             except ValueError:
#                 val.append('')  
#                 podpole_indeks += 1
#         val = [int(val[i+1]) if v == '' else int(v) for i, v in enumerate(val)]
#         ile_heder = 0
#         for i, p in zip(val, podpola_b):
#             if p == '❦':
#                 i += ile_heder
#                 x['260'] = x['260'][:i] + '❦' + x['260'][i:]
#                 ile_heder += 1    
#         final_value = x['260'].replace('❦', grupa_a)
#     else:
#         final_value = x['260']
#     return final_value
#                 
# publication['260'] = publication.apply(lambda x: adres_wydawniczy(x), axis=1)
# publication['260'] = publication['260'].str.replace('(\s+\W)(\$a)', r'\1❦\2', regex=True)
# publication['index'] = publication.index+1
# publication = cSplit(publication, 'index', '260', '❦')
# 
# x260 = marc_parser_1_field(publication, 'index', '260', '\$').replace(' {0,}[:;]$', '', regex=True)
# x260 = pd.merge(x260, publication[['index', '001']], how='left', on='index').drop_duplicates().reset_index(drop=True)[['001', '$a', '$b', '$c']].rename(columns={'$a':'place', '$b':'publishing house', '$c':'year'})
# 
# def get_year(x):
#     try:
#         return re.findall('\d+', x)[0]
#     except IndexError:
#         return np.nan
#         
# x260['year'] = x260['year'].apply(lambda x: get_year(x)).bfill()
# 
# books_df_simple = pd.merge(x245, x260, how='left', on='001')
# books_df_simple['place_house'] = books_df_simple[['place', 'publishing house']].apply(
#     lambda x: ': '.join(x.dropna().astype(str)),
#     axis=1
# )
# 
# books_df_simple.drop(columns=['place', 'publishing house'], inplace=True)
# 
# for column in books_df_simple.drop(columns='001'):
#     books_df_simple[column] = books_df_simple.groupby('001')[column].transform(lambda x: ', '.join(x.drop_duplicates().astype(str)))
#     
# books_df_simple = books_df_simple.drop_duplicates()
# books_df_simple['856'] = '42$uhttp://libri.ucl.cas.cz/Record/' + books_df_simple['001'] + '$ypart of: ' + books_df_simple['book title'] + ', ' + books_df_simple['place_house'] + ' ' + books_df_simple['year']
# books_df_simple = books_df_simple[['001', '856']].rename(columns={'001':'book id'})
# 
# relations_1 = pd.merge(chapters_relations_df, books_df_simple, how='left', on= 'book id')[['chapter id', '856']].rename(columns={'chapter id':'id'})
# relations_1['type'] = 'chapter'
# 
# #%% creating 856 field with chapters (for books)
# 
# chapters_df = df.copy()
# 
# #autor
# x100 = marc_parser_1_field(chapters_df, '001', '100', '\$')[['001', '$a']].rename(columns={'$a':'author'})
# x100['author'] = x100['author'].str.replace(',', '')
# x700 = marc_parser_1_field(chapters_df, '001', '700', '\$')
# x700 = x700[(x700['$e'] == '') & (x700['$t'] == '')][['001', '$a']].rename(columns={'$a':'author'})
# x700['author'] = x700['author'].str.replace(',', '')
# 
# author = pd.concat([x100, x700])
# author['author'] = author.groupby('001')['author'].transform(lambda x: ', '.join(x.drop_duplicates().astype(str)))
# author = author.drop_duplicates()
# 
# # title
# x245 = marc_parser_1_field(chapters_df, '001', '245', '\$')[['001', '$a', '$b', '$n', '$p']].replace(r'^\s*$', np.NaN, regex=True)
# x245 = x245.replace(r'^\s*$', np.NaN, regex=True)
# x245['book title'] = x245[x245.columns[1:]].apply(
#     lambda x: ' '.join(x.dropna().astype(str)),
#     axis=1
# )
# x245 = x245[['001', 'book title']]
# x245['book title'] = x245['book title'].replace('\/$', '', regex=True).str.strip().replace('\.$', '', regex=True)
# 
# #pages
# 
# x773 = marc_parser_1_field(chapters_df, '001', '773', '\$')[['001', '$g']].rename(columns={'$g':'pages'})
# 
# dfs = [author, x245, x773]
# chapters_df_simple = reduce(lambda left,right: pd.merge(left,right,on='001', how = 'outer'), dfs).drop_duplicates().reset_index(drop=True)
# chapters_df_simple['author'] = chapters_df_simple['author'].apply(lambda x: '[no author]' if x == '' or pd.isnull(x) else x)
# 
# chapters_df_simple['856'] = '42$uhttp://libri.ucl.cas.cz/Record/' + chapters_df_simple['001'] + '$yhas part: ' + chapters_df_simple['author'] + ', ' + chapters_df_simple['book title'] + ' ' + chapters_df_simple['pages']
# chapters_df_simple = chapters_df_simple[['001', '856']].rename(columns={'001':'chapter id'})
# 
# relations_2 = pd.merge(chapters_relations_df, chapters_df_simple, how='left', on= 'chapter id')[['book id', '856']].rename(columns={'book id':'id'})
# relations_2['type'] = 'book'
# 
# 
# relations = pd.concat([relations_1, relations_2])
# 
# 
# # chapters_relations_sheet.add_worksheet(title='relations', rows="100", cols="20")
# # set_with_dataframe(chapters_relations_sheet.worksheet('relations'), relations)
# 
# chapters_books_dict = {'relations':relations,
#                        'chapters_and_books':chapters_and_books,
#                        'chapters_with_missing_books':chapters_with_missing_books,
#                        'multiple_maches':multiple_maches}
# 
# new_sheet = gc.create(f'BN_rozdziały_i_książki_{year}-{month}-{day}', '1aATJM13muNYUB6CGhvuPMPtOzjT5XE8j')
# 
# for k, v in chapters_books_dict.items():
#     try:
#         set_with_dataframe(new_sheet.worksheet(k), v)
#     except gs.WorksheetNotFound:
#         new_sheet.add_worksheet(title=k, rows="100", cols="20")
#         set_with_dataframe(new_sheet.worksheet(k), v)
#     
# new_sheet.del_worksheet(new_sheet.worksheet('Arkusz1'))
# 
# for worksheet in new_sheet.worksheets():
#     new_sheet.batch_update({
#         "requests": [
#             {
#                 "updateDimensionProperties": {
#                     "range": {
#                         "sheetId": worksheet._properties['sheetId'],
#                         "dimension": "ROWS",
#                         "startIndex": 0,
#                         #"endIndex": 100
#                     },
#                     "properties": {
#                         "pixelSize": 20
#                     },
#                     "fields": "pixelSize"
#                 }
#             }
#         ]
#     })   
#     worksheet.freeze(rows=1)
#     worksheet.set_basic_filter()
# =============================================================================

#%% dalsze działania
# 1. na razie uwzględniamy tylko arkusz chapters and books i przy jego wykorzystaniu budujemy w 856 rozdziałów i książek relacje
# 2. dociągnięcie książek, które mają interesujące PBL rozdziały, a do tej pory nie weszły do bazy Libri
# 3. na póżniej:
#     - ręczne uzupełnienie chapters_with_missing_books
#     - wybór właściwej relacji dla multiple_maches
        















