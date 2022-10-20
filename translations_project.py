# dla każdej biblioteki przygotowac nową listę nazewnictw na podstawie kolumny all_names
# poprawione: loc
import xml.etree.ElementTree as et
import requests
import pandas as pd
import re
import numpy as np
import pymarc
import io
from bs4 import BeautifulSoup
from my_functions import cosine_sim_2_elem, marc_parser_1_field, gsheet_to_df, xml_to_mrk, cSplit, f, df_to_mrc, mrc_to_mrk, mrk_to_df, xml_to_mrk, mrk_to_mrc, get_cosine_result, df_to_gsheet, cluster_strings, cluster_records, simplify_string
import glob
import regex
import unidecode
import pandasql
import time
from xml.sax import SAXParseException
import Levenshtein as lev
from difflib import SequenceMatcher  
from google_drive_research_folders import cr_projects
import gspread_pandas as gp
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
# from google_drive_credentials import gc, credentials
import datetime
import itertools
import difflib
# import num2words
from functools import reduce
import sys
import csv
#%%
gauth = GoogleAuth()
gauth.LocalWebserverAuth()

drive = GoogleDrive(gauth)

### def

def add_viaf(x):
    return x + 'viaf.xml'

def author(row, field):
    if row['field'] == field:
        val = row['field']
    else:
        val = np.nan
    return val

def year2word(x):
    try:
        x = num2words.num2words(int(x), lang='pl')   
    except ValueError:
        pass
    return x

### code
    
# =============================================================================
# # table for personal authorities
# path_in = "C:/Users/Cezary/Downloads/CLOselection.txt"
# encoding = 'utf-8'
# reader = io.open(path_in, 'rt', encoding = encoding).read().splitlines()
# 
# reader = list(filter(None, reader))  
# df = pd.DataFrame(reader, columns = ['test'])
# df['id'] = df['test'].replace(r'^(.{9})(.+?$)', r'\1', regex=True)
# df['field'] = df['test'].replace(r'^(.{10})(.{3})(.+?$)', r'\2', regex=True)
# df['content'] = df['test'].replace(r'^(.{18})(.+?$)', r'\2', regex=True)
# df.loc[df['field'] == '   ', 'field'] = 'ZZZ'
# df = df[['id', 'field', 'content']]
# df['content'] = df.groupby(['id', 'field'])['content'].transform(lambda x: '❦'.join(x.drop_duplicates().astype(str)))
# df = df.drop_duplicates().reset_index(drop=True)
# df_wide = df.pivot(index = 'id', columns = 'field', values = 'content')
# 
# fields_order = df_wide.columns.tolist()
# fields_order.remove('LDR')
# fields_order.sort()
# fields_order = ['LDR'] + fields_order
# df_wide = df_wide.reindex(columns=fields_order)
# 
# df_wide.to_excel('cz_authorities.xlsx')
# 
# X100 = marc_parser_1_field(df_wide, '001', '100', '\$\$')
# X100 = X100[(X100['$$7'] != '') |
#             (X100['$$d'] != '')]
# X100['index'] = X100.index + 1
# 
# ns = '{http://viaf.org/viaf/terms#}'
# viaf_enrichment = []
# viaf_errors = []
# for index, row in X100.iterrows():
#     print(str(index) + '/' + str(len(X100)))
#     try:
#         if row['$$7'] != '':
#             url = f"http://viaf.org/viaf/sourceID/NKC%7C{row['$$7']}/viaf.xml"
#             response = requests.get(url)
#             with open('viaf.xml', 'wb') as file:
#                 file.write(response.content)
#             tree = et.parse('viaf.xml')
#             root = tree.getroot()
#             viaf_id = root.findall(f'.//{ns}viafID')[0].text
#             IDs = root.findall(f'.//{ns}mainHeadings/{ns}data/{ns}sources/{ns}sid')
#             IDs = '❦'.join([t.text for t in IDs])
#             nationality = root.findall(f'.//{ns}nationalityOfEntity/{ns}data/{ns}text')
#             nationality = '❦'.join([t.text for t in nationality])
#             occupation = root.findall(f'.//{ns}occupation/{ns}data/{ns}text')
#             occupation = '❦'.join([t.text for t in occupation])
#             language = root.findall(f'.//{ns}languageOfEntity/{ns}data/{ns}text')
#             language = '❦'.join([t.text for t in language])
#             names = root.findall(f'.//{ns}x400/{ns}datafield')
#             sources = root.findall(f'.//{ns}x400/{ns}sources')
#             name_source = []
#             for (name, source) in zip(names, sources):   
#                 person_name = ' '.join([child.text for child in name.getchildren() if child.tag == f'{ns}subfield' and child.attrib['code'].isalpha()])
#                 library = '~'.join([child.text for child in source.getchildren() if child.tag == f'{ns}sid'])
#                 name_source.append([person_name, library])   
#             for i, elem in enumerate(name_source):
#                 name_source[i] = '‽'.join(name_source[i])
#             name_source = '❦'.join(name_source)
#             
#             person = [row['index'], row['$$a'], row['$$d'], viaf_id, IDs, nationality, occupation, language, name_source]
#             viaf_enrichment.append(person)
#         else:
#             try:
#                 url = re.sub('\s+', '%20', f"http://viaf.org/viaf/search?query=local.personalNames%20all%20%22{row['$$a']} {row['$$d']}%22%20and%20local.sources%20any%20%22nkc%22&sortKeys=holdingscount&recordSchema=BriefVIAF")
#                 response = requests.get(url)
#                 response.encoding = 'UTF-8'
#                 soup = BeautifulSoup(response.text, 'html.parser')
#                 people_links = soup.findAll('a', attrs={'href': re.compile("viaf/\d+")})
#                 viaf_people = []
#                 for people in people_links:
#                     person_name = re.sub('\s+', ' ', people.text).strip().split('\u200e ')
#                     person_link = re.sub(r'(.+?)(\#.+$)', r'http://viaf.org\1viaf.xml', people['href'].strip())
#                     viaf_people.append([person_name, person_link])
#                 viaf_people = pd.DataFrame(viaf_people, columns=['viaf name', 'viaf'])
#                 viaf_people = pd.DataFrame(viaf_people['viaf name'].tolist(), viaf_people['viaf']).stack()
#                 viaf_people = viaf_people.reset_index()[[0, 'viaf']]
#                 viaf_people.columns = ['viaf name', 'viaf']
#                 viaf_people['cz name'] = f"{row['$$a']} {row['$$d']}"
#                 for ind, vname in viaf_people.iterrows():
#                     viaf_people.at[ind, 'cosine'] = cosine_sim_2_elem([vname['viaf name'], vname['cz name']]).iloc[:, -1].to_string(index=False).strip()
#                 viaf_people = viaf_people[viaf_people['cosine'] == viaf_people['cosine'].max()]
#                 for i, line in viaf_people.iterrows():
#                     url = line['viaf']
#                     response = requests.get(url)
#                     with open('viaf.xml', 'wb') as file:
#                         file.write(response.content)
#                     tree = et.parse('viaf.xml')
#                     root = tree.getroot()
#                     viaf_id = root.findall(f'.//{ns}viafID')[0].text
#                     IDs = root.findall(f'.//{ns}mainHeadings/{ns}data/{ns}sources/{ns}sid')
#                     IDs = '❦'.join([t.text for t in IDs])
#                     nationality = root.findall(f'.//{ns}nationalityOfEntity/{ns}data/{ns}text')
#                     nationality = '❦'.join([t.text for t in nationality])
#                     occupation = root.findall(f'.//{ns}occupation/{ns}data/{ns}text')
#                     occupation = '❦'.join([t.text for t in occupation])
#                     language = root.findall(f'.//{ns}languageOfEntity/{ns}data/{ns}text')
#                     language = '❦'.join([t.text for t in language])
#                     names = root.findall(f'.//{ns}x400/{ns}datafield')
#                     sources = root.findall(f'.//{ns}x400/{ns}sources')
#                     name_source = []
#                     for (name, source) in zip(names, sources):   
#                         person_name = ' '.join([child.text for child in name.getchildren() if child.tag == f'{ns}subfield' and child.attrib['code'].isalpha()])
#                         library = '~'.join([child.text for child in source.getchildren() if child.tag == f'{ns}sid'])
#                         name_source.append([person_name, library])   
#                     for i, elem in enumerate(name_source):
#                         name_source[i] = '‽'.join(name_source[i])
#                     name_source = '❦'.join(name_source)
#                     
#                     person = [row['index'], row['$$a'], row['$$d'], viaf_id, IDs, nationality, occupation, language, name_source]
#                     viaf_enrichment.append(person)
#             except (KeyError, IndexError):
#                 person = [row['index'], row['$$a'], row['$$d'], '', '', '', '', '', '']
#                 viaf_enrichment.append(person)
#     except IndexError:
#         error = [row['index'], row['$$a'], row['$$d']]
#         viaf_errors.append(error)       
#              
# viaf_df = pd.DataFrame(viaf_enrichment, columns=['index', 'cz_name', 'cz_dates', 'viaf_id', 'IDs', 'nationality', 'occupation', 'language', 'name_and_source'])
# errorfile = io.open('cz_translation_errors.txt', 'wt', encoding = 'UTF-8')
# for element in viaf_errors:
#     errorfile.write(str(element) + '\n\n')
# errorfile.close()
# 
# viaf_df.to_excel('cz_viaf.xlsx', index = False)
# =============================================================================
    
#google sheets
df_names = gsheet_to_df('1QB5EmMhg7qSWfWaJurafHdmXc5uohQpS-K5GBsiqerA', 'Sheet1').drop_duplicates()
df_names = df_names.loc[(~df_names['czy_czech'].isin(['nie', 'boardercase'])) &
                        (df_names['viaf_id'] != '')]
df_names.fillna(value=pd.np.nan, inplace=True)
df_names['index'] = df_names['index'].astype(int)

#czarna lista jest już niepotrzebna, bo w all_names są już dobre wartości
# =============================================================================
# blacklist = gsheet_to_df('1QB5EmMhg7qSWfWaJurafHdmXc5uohQpS-K5GBsiqerA', 'blacklist').values.tolist()
# 
# for viaf, name in blacklist:
#     df = df_names.copy()[df_names['viaf_id'] == viaf]
#     try:
#         df_index = df.index.values.astype(int)[0]
#         df = df.at[df_index, 'name_and_source'].split('❦')
#         df = '❦'.join([s for s in df if s != name])
#         df_names.at[df_index, 'name_and_source'] = df
#     except IndexError:
#         pass
# =============================================================================

# =============================================================================
# # test filtering
# test_names = gsheet_to_df('1QB5EmMhg7qSWfWaJurafHdmXc5uohQpS-K5GBsiqerA', 'test_list').values.tolist()
# test_names = [item for sublist in test_names for item in sublist]
# df_names = df_names[df_names['viaf_id'].isin(test_names)]
# =============================================================================
           
# viaf other names in MARC21 format

# =============================================================================
# viaf_for_search = list(set(df_names['viaf_id'].tolist()))
# 
# viaf_marc_df = pd.DataFrame()
# errorfile = io.open('marc_errors.txt', 'wt', encoding='UTF-8')
# for i, n in enumerate(viaf_for_search):    
#     print(f"{i+1}/{len(viaf_for_search)}")
#     url = f'https://viaf.org/viaf/{n}/marc21.xml'
#     r = requests.get(url, allow_redirects=True)
#     open('test.xml', 'wb').write(r.content)
#     try:
#         xml_to_mrk('test.xml', 'test.mrk')
#         viaf_df = mrk_to_df('test.mrk', '001')
#         viaf_marc_df = viaf_marc_df.append(viaf_df)
#     except SAXParseException:
#         errorfile.write(str(n) + '\n\n')
# errorfile.close()
# 
# fields_order = viaf_marc_df.columns.tolist()
# fields_order.sort(key = lambda x: ([str,int].index(type(1 if re.findall(r'\w+', x)[0].isdigit() else 'a')), x))
# viaf_marc_df = viaf_marc_df.reindex(columns=fields_order)
# viaf_marc_df.to_excel('viaf_other_names.xlsx', index = False)
# 
# viaf_names = viaf_marc_df.copy()[['001', '700']]
# viaf_names.to_excel('viaf_names.xlsx', index=False)
# =============================================================================

# Swedish database
# with dates and without dates - OV decides
swe_names = df_names.copy()
swe_names_cz = swe_names[['index', 'cz_name', 'viaf']]
swe_names = swe_names.loc[swe_names['IDs'].str.contains("SELIBR") == True].reset_index(drop = True)
# swe_names['cz_name'] = swe_names.apply(lambda x: f"{x['cz_name']} {x['cz_dates']}", axis=1)
swe_names = swe_names[['index', 'cz_name', 'viaf', 'IDs', 'name_and_source']]
swe_names = cSplit(swe_names, 'index', 'name_and_source', '❦')
swe1 = swe_names[['index', 'cz_name', 'viaf']].drop_duplicates()
swe_names = swe_names.loc[swe_names['name_and_source'].str.contains("SELIBR") == True].reset_index(drop = True)   
swe_names = cSplit(swe_names, 'index', 'name_and_source', '‽', 'wide', 1).drop_duplicates()
swe2 = swe_names[['index', 'name_and_source_0', 'viaf']]
swe2.columns = swe1.columns
swe_names = pd.concat([swe_names_cz, swe1, swe2]).sort_values(['viaf', 'cz_name']).drop_duplicates().values.tolist()     
        
# Swedish database harvesting
full_data = pd.DataFrame()
errors = []
swe_marc = pymarc.TextWriter(io.open('swe.mrk', 'wt', encoding = 'utf-8'))
for name_index, (index, name, viaf) in enumerate(swe_names):
    print(str(name_index) + '/' + str(len(swe_names)-1))
    url = f'http://libris.kb.se/xsearch?query=forf:({name.replace(" ", "%20")})&start=1&n=200&format=marcxml&format_level=full'
    response = requests.get(url)
    with open('test.xml', 'wb') as file:
        file.write(response.content)
    tree = et.parse('test.xml')
    root = tree.getroot()
    number_of_records = int(root.attrib['records'])
    if 0 < number_of_records <= 200:
        xml_to_mrk('test.xml', 'test.mrk')
        records = pymarc.map_xml(swe_marc.write, 'test.xml')         
        df = pd.read_table('test.mrk',skip_blank_lines=True, header = None)
        df.columns = ['original']
        df['field'] = df['original'].str.extract(r'(?<=\=)(...)')
        df['content'] = df['original'].str.extract(r'(?<=  )(.*$)')
        df['ldr'] = df.apply(lambda x: f(x, 'LDR'), axis = 1).ffill()
        df['id'] = df.apply(lambda x: f(x, '001'), axis = 1)
        df['id'] = df.groupby('ldr')['id'].apply(lambda x: x.ffill().bfill())
        df = df[['id', 'field', 'content']]
        df['content'] = df.groupby(['id', 'field'])['content'].transform(lambda x: '❦'.join(x.drop_duplicates().astype(str)))
        df = df.drop_duplicates().reset_index(drop=True)
        df['name'] = name
        df['viaf'] = viaf
        df['index'] = index
        df_people = df[['id', 'index', 'name', 'viaf']].drop_duplicates()
        df_wide = df.pivot(index = 'id', columns = 'field', values = 'content')
        df_wide = df_wide[df_wide['LDR'].str.contains('^.{6}am', regex=True)]
        df_wide = pd.merge(df_wide, df_people,  how='left', left_on = 'id', right_on = 'id')
        full_data = full_data.append(df_wide)
    elif number_of_records > 200:
        x = range(1, number_of_records + 1, 200)
        for page_index, i in enumerate(x):
            if i == 1:
                xml_to_mrk('test.xml', 'test.mrk')
                records = pymarc.map_xml(swe_marc.write, 'test.xml')
                df = pd.read_table('test.mrk',skip_blank_lines=True, header = None)
                df.columns = ['original']
                df['field'] = df['original'].str.extract(r'(?<=\=)(...)')
                df['content'] = df['original'].str.extract(r'(?<=  )(.*$)')
                df['ldr'] = df.apply(lambda x: f(x, 'LDR'), axis = 1).ffill()
                df['id'] = df.apply(lambda x: f(x, '001'), axis = 1)
                df['id'] = df.groupby('ldr')['id'].apply(lambda x: x.ffill().bfill())
                df = df[['id', 'field', 'content']]
                df['content'] = df.groupby(['id', 'field'])['content'].transform(lambda x: '❦'.join(x.drop_duplicates().astype(str)))
                df = df.drop_duplicates().reset_index(drop=True)
                df['name'] = name
                df['viaf'] = viaf
                df['index'] = index
                df_people = df[['id', 'index', 'name', 'viaf']].drop_duplicates()
                df_wide = df.pivot(index = 'id', columns = 'field', values = 'content')
                df_wide = df_wide[df_wide['LDR'].str.contains('^.{6}am', regex=True)]
                df_wide = pd.merge(df_wide, df_people,  how='left', left_on = 'id', right_on = 'id')
                full_data = full_data.append(df_wide)
            else:
                link = f'http://libris.kb.se/xsearch?query=forf:({name.replace(" ", "%20")})&start={i}&n=200&format=marcxml&format_level=full'
                response = requests.get(link)
                with open('test.xml', 'wb') as file:
                    file.write(response.content)
                xml_to_mrk('test.xml', 'test.mrk')
                records = pymarc.map_xml(swe_marc.write, 'test.xml')
                df = pd.read_table('test.mrk',skip_blank_lines=True, header = None)
                df.columns = ['original']
                df['field'] = df['original'].str.extract(r'(?<=\=)(...)')
                df['content'] = df['original'].str.extract(r'(?<=  )(.*$)')
                df['ldr'] = df.apply(lambda x: f(x, 'LDR'), axis = 1).ffill()
                df['id'] = df.apply(lambda x: f(x, '001'), axis = 1)
                df['id'] = df.groupby('ldr')['id'].apply(lambda x: x.ffill().bfill())
                df = df[['id', 'field', 'content']]
                df['content'] = df.groupby(['id', 'field'])['content'].transform(lambda x: '❦'.join(x.drop_duplicates().astype(str)))
                df = df.drop_duplicates().reset_index(drop=True)
                df['name'] = name
                df['viaf'] = viaf
                df['index'] = index
                df_people = df[['id', 'index', 'name', 'viaf']].drop_duplicates()
                df_wide = df.pivot(index = 'id', columns = 'field', values = 'content')
                df_wide = df_wide[df_wide['LDR'].str.contains('^.{6}am', regex=True)]
                df_wide = pd.merge(df_wide, df_people,  how='left', left_on = 'id', right_on = 'id')
                full_data = full_data.append(df_wide)
    else:
        errors.append([name_index, index, name, viaf])
swe_marc.close()

fields_order = full_data.columns.tolist()
fields_order.sort(key = lambda x: ([str,int].index(type(1 if re.findall(r'\w+', x)[0].isdigit() else 'a')), x))
full_data = full_data.reindex(columns=fields_order)
# full_data.to_excel('swe_data.xlsx', index = False)
full_data.to_excel('swe_data_no_dates.xlsx', index = False)
swe_errors = pd.DataFrame(errors, columns = ['name_index', 'authority_index', 'name', 'viaf'])
swe_errors.to_excel('swe_errors.xlsx', index = False)
swe_names = pd.DataFrame(swe_names, columns = ['authority_index', 'name', 'viaf'])
swe_names.to_excel('swe_names.xlsx', index = False)

# Swedish harvesting Czech original

full_data = pd.DataFrame()
url = 'http://libris.kb.se/xsearch?query=(ORIG:cze)&format_level=full&format=marcxml&start=1&n=200'
response = requests.get(url)
with open('test.xml', 'wb') as file:
    file.write(response.content)
tree = et.parse('test.xml')
root = tree.getroot()
number_of_records = int(root.attrib['records'])
x = range(1, number_of_records + 1, 200)
for page_index, i in enumerate(x):
    print(str(page_index) + '/' + str(len(x)-1))
    if i == 1:
        xml_to_mrk('test.xml', 'test.mrk')
        df = pd.read_table('test.mrk',skip_blank_lines=True, header = None)
        df.columns = ['original']
        df['field'] = df['original'].str.extract(r'(?<=\=)(...)')
        df['content'] = df['original'].str.extract(r'(?<=  )(.*$)')
        df['ldr'] = df.apply(lambda x: f(x, 'LDR'), axis = 1).ffill()
        df['id'] = df.apply(lambda x: f(x, '001'), axis = 1)
        df['id'] = df.groupby('ldr')['id'].apply(lambda x: x.ffill().bfill())
        df = df[['id', 'field', 'content']]
        df['content'] = df.groupby(['id', 'field'])['content'].transform(lambda x: '❦'.join(x.drop_duplicates().astype(str)))
        df = df.drop_duplicates().reset_index(drop=True)
        df_wide = df.pivot(index = 'id', columns = 'field', values = 'content')
        df_wide = df_wide[df_wide['LDR'].str.contains('^.{6}am', regex=True)]
        full_data = full_data.append(df_wide)
    else:
        link = f'http://libris.kb.se/xsearch?query=(ORIG:cze)&format_level=full&format=marcxml&start={i}&n=200'
        response = requests.get(link)
        with open('test.xml', 'wb') as file:
            file.write(response.content)
        xml_to_mrk('test.xml', 'test.mrk')
        df = pd.read_table('test.mrk',skip_blank_lines=True, header = None)
        df.columns = ['original']
        df['field'] = df['original'].str.extract(r'(?<=\=)(...)')
        df['content'] = df['original'].str.extract(r'(?<=  )(.*$)')
        df['ldr'] = df.apply(lambda x: f(x, 'LDR'), axis = 1).ffill()
        df['id'] = df.apply(lambda x: f(x, '001'), axis = 1)
        df['id'] = df.groupby('ldr')['id'].apply(lambda x: x.ffill().bfill())
        df = df[['id', 'field', 'content']]
        df['content'] = df.groupby(['id', 'field'])['content'].transform(lambda x: '❦'.join(x.drop_duplicates().astype(str)))
        df = df.drop_duplicates().reset_index(drop=True)
        df_wide = df.pivot(index = 'id', columns = 'field', values = 'content')
        df_wide = df_wide[df_wide['LDR'].str.contains('^.{6}am', regex=True)]
        full_data = full_data.append(df_wide)

fields_order = full_data.columns.tolist()
fields_order.sort(key = lambda x: ([str,int].index(type(1 if re.findall(r'\w+', x)[0].isdigit() else 'a')), x))
full_data = full_data.reindex(columns=fields_order)
full_data.to_excel('swe_data_cz_origin.xlsx', index = False)

# Polish database
# with dates and without dates - OV decides

pol_names = df_names.copy()
pol_names_cz = pol_names[['index', 'cz_name', 'viaf']]
pol_names = pol_names.loc[pol_names['IDs'].str.contains("PLWABN") == True].reset_index(drop = True)
# pol_names['cz_name'] = pol_names.apply(lambda x: f"{x['cz_name']} {x['cz_dates']}", axis=1)
pol_names = pol_names[['index', 'cz_name', 'viaf', 'IDs', 'name_and_source']]
pol_names = cSplit(pol_names, 'index', 'name_and_source', '❦')
pol1 = pol_names[['index', 'cz_name', 'viaf']].drop_duplicates()
pol_names = pol_names.loc[pol_names['name_and_source'].str.contains("PLWABN") == True].reset_index(drop = True)   
pol_names = cSplit(pol_names, 'index', 'name_and_source', '‽', 'wide', 1).drop_duplicates()
pol2 = pol_names[['index', 'name_and_source_0', 'viaf']]
pol2.columns = pol1.columns
pol2['n_letters'] = pol2['cz_name'].apply(lambda x: len(''.join(re.findall('[a-zA-ZÀ-ž]', x))))
pol2['n_uppers'] = pol2['cz_name'].apply(lambda x: len(''.join(re.findall('[A-ZÀ-Ž]', x))))
pol2['has_digits'] = pol2['cz_name'].apply(lambda x: bool(re.findall('\d+', x)))
pol2 = pol2[(pol2['n_letters'] != pol2['n_uppers']) |
            (pol2['has_digits'] == True)]
pol2['n_initials'] = pol2['cz_name'].apply(lambda x: len(''.join(re.findall('(?<= )([A-ZÀ-Ž])(?=\.)', x))))
pol2['n_words'] = pol2['cz_name'].apply(lambda x: len(re.findall('([a-zA-ZÀ-ž]+)(?=[^a-zA-ZÀ-ž])', x)))
pol2 = pol2[(pol2['n_words'] != pol2['n_initials'] + 1) |
            (pol2['has_digits'] == True)]
pol2 = pol2[['index', 'cz_name', 'viaf']]
pol_names = pd.concat([pol_names_cz, pol1, pol2]).sort_values(['viaf', 'cz_name']).drop_duplicates().values.tolist()

# Polish database harvesting
ns = '{http://www.loc.gov/MARC21/slim}'
full_data = pd.DataFrame()
errors = []
pol_marc = pymarc.TextWriter(io.open('pol.mrk', 'wt', encoding = 'utf-8'))
for name_index, (index, name, viaf) in enumerate(pol_names):
    print(str(name_index) + '/' + str(len(pol_names)-1))
    try:
        url = f'https://data.bn.org.pl/api/bibs.marcxml?author={name.replace(" ", "%20")}&amp;limit=100'
        response = requests.get(url)
        with open('test.xml', 'wb') as file:
            file.write(response.content)
        tree = et.parse('test.xml')
        root = tree.getroot()
        while len(root.findall(f'.//{ns}record')) > 0:
            xml_to_mrk('test.xml', 'test.mrk')
            records = pymarc.map_xml(pol_marc.write, 'test.xml')         
            df = pd.read_table('test.mrk',skip_blank_lines=True, header = None)
            df.columns = ['original']
            df['field'] = df['original'].str.extract(r'(?<=\=)(...)')
            df['content'] = df['original'].str.extract(r'(?<=  )(.*$)')
            df['ldr'] = df.apply(lambda x: f(x, 'LDR'), axis = 1).ffill()
            df['id'] = df.apply(lambda x: f(x, '001'), axis = 1)
            df['id'] = df.groupby('ldr')['id'].apply(lambda x: x.ffill().bfill())
            df = df[['id', 'field', 'content']]
            df['content'] = df.groupby(['id', 'field'])['content'].transform(lambda x: '❦'.join(x.drop_duplicates().astype(str)))
            df = df.drop_duplicates().reset_index(drop=True)
            df['name'] = name
            df['viaf'] = viaf
            df['index'] = index
            df_people = df[['id', 'index', 'name', 'viaf']].drop_duplicates()
            df_wide = df.pivot(index = 'id', columns = 'field', values = 'content')
            df_wide = df_wide[df_wide['LDR'].str.contains('^.{6}am', regex=True)]
            df_wide = pd.merge(df_wide, df_people,  how='left', left_on = 'id', right_on = 'id')
            full_data = full_data.append(df_wide)
            url = root.find(f'.//nextPage').text
            response = requests.get(url)
            with open('test.xml', 'wb') as file:
                file.write(response.content)
            tree = et.parse('test.xml')
            root = tree.getroot()
    except:
        errors.append([name_index, index, name, viaf])
pol_marc.close()

fields_order = full_data.columns.tolist()
fields_order.sort(key = lambda x: ([str,int].index(type(1 if re.findall(r'\w+', x)[0].isdigit() else 'a')), x))
full_data = full_data.reindex(columns=fields_order)
# full_data.to_excel('pol_data.xlsx', index = False)
full_data.to_excel('pol_data_no_dates.xlsx', index = False)
pol_errors = pd.DataFrame(errors, columns = ['name_index', 'authority_index', 'name', 'viaf'])
pol_errors.to_excel('pol_errors.xlsx', index = False)
pol_names = pd.DataFrame(pol_names, columns = ['authority_index', 'name', 'viaf'])
pol_names.to_excel('pol_names.xlsx', index = False)

# Polish harvesting Czech original

ns = '{http://www.loc.gov/MARC21/slim}'
full_data = pd.DataFrame()
url = 'https://data.bn.org.pl/api/bibs.marcxml?limit=100&amp;marc=041h+cze'
response = requests.get(url)
with open('test.xml', 'wb') as file:
    file.write(response.content)
tree = et.parse('test.xml')
root = tree.getroot()
i = 1
while len(root.findall(f'.//{ns}record')) > 0:
    print(str(i))
    xml_to_mrk('test.xml', 'test.mrk')      
    df = pd.read_table('test.mrk',skip_blank_lines=True, header = None)
    df.columns = ['original']
    df['field'] = df['original'].str.extract(r'(?<=\=)(...)')
    df['content'] = df['original'].str.extract(r'(?<=  )(.*$)')
    df['ldr'] = df.apply(lambda x: f(x, 'LDR'), axis = 1).ffill()
    df['id'] = df.apply(lambda x: f(x, '001'), axis = 1)
    df['id'] = df.groupby('ldr')['id'].apply(lambda x: x.ffill().bfill())
    df = df[['id', 'field', 'content']]
    df['content'] = df.groupby(['id', 'field'])['content'].transform(lambda x: '❦'.join(x.drop_duplicates().astype(str)))
    df = df.drop_duplicates().reset_index(drop=True)
    df_wide = df.pivot(index = 'id', columns = 'field', values = 'content')
    df_wide = df_wide[df_wide['LDR'].str.contains('^.{6}am', regex=True)]
    full_data = full_data.append(df_wide)
    url = root.find(f'.//nextPage').text
    response = requests.get(url)
    with open('test.xml', 'wb') as file:
        file.write(response.content)
    tree = et.parse('test.xml')
    root = tree.getroot()
    i += 1
    
fields_order = full_data.columns.tolist()
fields_order.sort(key = lambda x: ([str,int].index(type(1 if re.findall(r'\w+', x)[0].isdigit() else 'a')), x))
full_data = full_data.reindex(columns=fields_order)
full_data.to_excel('pol_data_cz_origin.xlsx', index = False)

# Finnish database    
fi_names = df_names.copy()
fi_names_cz = fi_names[['index', 'cz_name', 'viaf']]
# fi_names['cz_name'] = fi_names.apply(lambda x: f"{x['cz_name']} {x['cz_dates']}", axis=1)
fi_names = fi_names[['index', 'cz_name', 'viaf', 'IDs', 'name_and_source']]
fi_names = cSplit(fi_names, 'index', 'name_and_source', '❦')
fi1 = fi_names[['index', 'cz_name', 'viaf']].drop_duplicates()
fi_names = cSplit(fi_names, 'index', 'name_and_source', '‽', 'wide', 1).drop_duplicates()
fi2 = fi_names[['index', 'name_and_source_0', 'viaf']]
fi2.columns = fi1.columns
def n_letters(x):
    try:
        val = len(''.join(regex.findall('\p{L}', x)))
    except TypeError:
        val = np.nan
    return val
fi2['n_letters'] = fi2['cz_name'].apply(lambda x: n_letters(x))
def n_uppers(x):
    try:
        val = len(''.join(regex.findall('\p{Lu}', x)))
    except TypeError:
        val = np.nan
    return val
fi2['n_uppers'] = fi2['cz_name'].apply(lambda x: n_uppers(x))
def has_digits(x):
    try:
        val = bool(re.findall('\d+', x))
    except TypeError:
        val = np.nan
    return val
fi2['has_digits'] = fi2['cz_name'].apply(lambda x: has_digits(x))
fi2 = fi2[(fi2['n_letters'] != fi2['n_uppers']) |
            (fi2['has_digits'] == True)]
def n_initials(x):
    try:
        val = len(''.join(regex.findall('(?<= )(\p{Lu})(?=\.)', x)))
    except TypeError:
        val = np.nan
    return val
fi2['n_initials'] = fi2['cz_name'].apply(lambda x: n_initials(x))
def n_words(x):
    try:
        val = len(regex.findall('(\p{L}+)(?=[^\p{L}])', x))
    except TypeError:
        val = np.nan
    return val
fi2['n_words'] = fi2['cz_name'].apply(lambda x: n_words(x))
fi2 = fi2[(fi2['n_words'] != fi2['n_initials'] + 1) |
            (fi2['has_digits'] == True)]
fi2 = fi2[['index', 'cz_name', 'viaf']]
fi_names = pd.concat([fi_names_cz, fi1, fi2]).sort_values(['viaf', 'cz_name']).drop_duplicates()
fi_names = fi_names[(fi_names['cz_name'].notna()) &
                    (fi_names['cz_name'].str.contains(' '))]

fi_names_table = fi_names.copy()
fi_names_table.columns = ['index', 'name', 'viaf']

fi_names = fi_names[['cz_name']].drop_duplicates().values.tolist()
fi_names = [re.escape(item) for sublist in fi_names for item in sublist]
fi_names = '|'.join(fi_names)

# Finnish database harvesting
path = 'F:/Cezary/Documents/IBL/Translations/Fennica/'
files = [f for f in glob.glob(path + '*.mrk', recursive=True)]

encoding = 'utf-8'
marc_df = pd.DataFrame()
for i, file_path in enumerate(files):
    print(str(i) + '/' + str(len(files)))
    marc_list = io.open(file_path, 'rt', encoding = encoding).read().splitlines()
    marc_list = list(filter(None, marc_list))  
    df = pd.DataFrame(marc_list, columns = ['test'])
    df['field'] = df['test'].replace(r'(^.)(...)(.+?$)', r'\2', regex = True)
    df['content'] = df['test'].replace(r'(^.)(.....)(.+?$)', r'\3', regex = True)
    df['help'] = df.apply(lambda x: f(x, 'LDR'), axis=1)
    df['help'] = df['help'].ffill()
    df_help = df.copy()[df['field'].isin(['100', '700'])]
    df_help = df_help[df_help['content'].str.contains(fi_names)]['help'].apply(lambda x: int(x)).values.tolist()
    df = df[df['help'].isin(df_help)]
    if len(df) > 0:
        df['id'] = df.apply(lambda x: f(x, '001'), axis = 1)
        df['id'] = df.groupby('help')['id'].ffill().bfill()
        df = df[['id', 'field', 'content']]
        df['content'] = df.groupby(['id', 'field'])['content'].transform(lambda x: '❦'.join(x.drop_duplicates().astype(str)))
        df = df.drop_duplicates().reset_index(drop=True)
        df_wide = df.pivot(index = 'id', columns = 'field', values = 'content')
        df_wide = df_wide[df_wide['LDR'].str.contains('^.{6}am', regex=True)]
        marc_df = marc_df.append(df_wide)

fields = marc_df.columns.tolist()
fields = [i for i in fields if 'LDR' in i or re.compile('\d{3}').findall(i)]
marc_df = marc_df.loc[:, marc_df.columns.isin(fields)]

fi_names = fi_names_table.copy()

X100 = marc_df[['001', '100']]
query = "select * from X100 a join fi_names b on a.'100' like '%'||b.name||'%'"
X100 = pandasql.sqldf(query)
X100 = X100[['001', 'index', 'name', 'viaf']]

X700 = marc_df[['001', '700']]
query = "select * from X700 a join fi_names b on a.'700' like '%'||b.name||'%'"
X700 = pandasql.sqldf(query)
X700 = X700[['001', 'index', 'name', 'viaf']]

X100700 = pd.concat([X100, X700]).drop_duplicates()
marc_df = pd.merge(marc_df, X100700, on='001', how='left')

fields_order = marc_df.columns.tolist()

fields_order.sort(key = lambda x: ([str,int].index(type(1 if re.findall(r'\w+', x)[0].isdigit() else 'a')), x))
marc_df = marc_df.reindex(columns=fields_order)
marc_df.to_excel('fi_data.xlsx', index=False)

#LoC database

loc_names = df_names.copy()[['index', 'cz_name', 'viaf_id', 'all_names']]
loc_names = cSplit(loc_names, 'index', 'all_names', '❦')
loc_names = loc_names.loc[loc_names['all_names'].str.contains("\$0\(LC\)") == True].reset_index(drop = True)
loc_names['all_names'] = loc_names['all_names'].str.replace('(..)(.+?)(\$0.+)', r'\2')

#LoC database harvesting

path = 'F:/Cezary/Documents/IBL/Translations/LoC/'
files = [f for f in glob.glob(path + '*.csv', recursive=True)]

query_100 = "select * from loc_data_slim a join loc_names b on a.'100' like '%'||b.all_names||'%'"
query_700 = "select * from loc_data_slim a join loc_names b on a.'700' like '%'||b.all_names||'%'"
new_df = pd.DataFrame()
for i, file in enumerate(files):
    print(f"{i+1}/{len(files)}")
    loc_data = pd.read_csv(file)
    loc_data_slim = loc_data[['001', '100', '700']]
    df = pandasql.sqldf(query_100)
    df = df[['001', 'index', 'cz_name', 'viaf_id']]
    df['field match'] = '100'
    df = pd.merge(loc_data, df, on='001', how='inner')
    new_df = new_df.append(df)
    df = pandasql.sqldf(query_700)
    df = df[['001', 'index', 'cz_name', 'viaf_id']]
    df['field match'] = '700'
    df = pd.merge(loc_data, df, on='001', how='inner')
    new_df = new_df.append(df)

fields_order = new_df.columns.tolist()
fields_order = [f for f in fields_order if re.findall('[0-9][0-9][0-9]', f) or f in ['LDR', 'cz_name', 'viaf_id', 'index', 'field match']]
fields_order.sort(key = lambda x: ([str,int].index(type(1 if re.findall(r'\w+', x)[0].isdigit() else 'a')), x))
new_df = new_df.reindex(columns=fields_order)
new_df.to_excel('loc_data.xlsx', index = False)

#%%OCLC

import xml.etree.ElementTree as ET
>>> tree = ET.parse('test.xml')
>>> for elem in tree.iter():
...     print elem

#preparation

oclc_viaf = 'F:/Cezary/Documents/IBL/Translations/OCLC/Czech viaf/dr117byviaf.xml'
oclc_lang = 'F:/Cezary/Documents/IBL/Translations/OCLC/Czech origin_trans/dr117bylang.xml'

ov = open(oclc_viaf, "r", encoding="UTF-8")
ind = 0
for line in ov:
    if '1135277855' in line:
        print(line)
        break
    ind += 1
ov_number = ind
print(f"Number of OCLC VIAF records: {ov_number}")

ol = open(oclc_lang, "r", encoding="UTF-8")
ind = 0
for line in ol:
    ind += 1
ol_number = ind
print(f"Number of OCLC Czech records: {ol_number}")

ov = open(oclc_viaf, "r", encoding="UTF-8")
writer = pymarc.TextWriter(io.open('F:/Cezary/Documents/IBL/Translations/OCLC/Czech viaf/oclc_viaf.mrk', 'wt', encoding="utf-8"))
for i, line in enumerate(ov, 1):
    print(f"{i}/{ov_number}")
    with open('test.xml', 'wt', encoding="UTF-8") as file:
        file.write(line)
    records = pymarc.map_xml(writer.write, 'test.xml')
writer.close()

ol = open(oclc_lang, "r", encoding="UTF-8")
ol_errors = []
writer = pymarc.TextWriter(io.open('F:/Cezary/Documents/IBL/Translations/OCLC/Czech origin_trans/oclc_lang.mrk', 'wt', encoding="utf-8"))
for i, line in enumerate(ol, 1):
    print(f"{i}/{ol_number}")
    try:
        with open('test.xml', 'wt', encoding="UTF-8") as file:
            file.write(line)
        records = pymarc.map_xml(writer.write, 'test.xml')
    except PermissionError:
        ol_errors.append(line)
writer.close()

mrk_to_mrc('F:/Cezary/Documents/IBL/Translations/OCLC/Czech viaf/oclc_viaf.mrk', 'F:/Cezary/Documents/IBL/Translations/OCLC/Czech viaf/oclc_viaf.mrc', '001')
mrk_to_mrc('F:/Cezary/Documents/IBL/Translations/OCLC/Czech origin_trans/oclc_lang.mrk', 'F:/Cezary/Documents/IBL/Translations/OCLC/Czech origin_trans/oclc_lang.mrc', '001')

oclc_viaf = mrk_to_df('F:/Cezary/Documents/IBL/Translations/OCLC/Czech viaf/oclc_viaf.mrk', '001')
oclc_viaf.to_excel('oclc_viaf.xlsx', index=False)
oclc_lang, ocl_viaf_errors = mrk_to_df('F:/Cezary/Documents/IBL/Translations/OCLC/Czech viaf/oclc_lang.mrk', '001')
oclc_lang.to_csv('oclc_lang.csv', index=False)

oclc_lang = pd.read_csv('F:/Cezary/Documents/IBL/Translations/OCLC/Czech origin_trans/oclc_lang.csv')

# all records = 1854243
# language from 008
oclc_language_008 = oclc_lang['008'].apply(lambda x: x[35:38]).to_frame()
count = oclc_language_008['008'].value_counts().to_frame()
count['language'] = count.index
count.reset_index(drop=True,inplace=True)
count.to_excel('oclc_lang_count_008.xlsx', index=False)

# records with field 041 = 176445
X041 = marc_parser_1_field(oclc_lang, '001', '041', '\$')
count_041 = X041['$a'].value_counts().to_frame()
count_041['language'] = count_041.index
count_041.reset_index(drop=True,inplace=True)
count_041.to_excel('oclc_lang_count_041.xlsx', index=False)

# records with language = eng based on 008
oclc_lang['language'] = oclc_lang['008'].apply(lambda x: x[35:38])
oclc_eng = oclc_lang[oclc_lang['language'] == 'eng']
oclc_eng.to_excel('oclc_lang_cz_to_eng.xlsx', index=False)

#filter scope by viaf id
viaf_ids = '|'.join(['viaf\/' + r for r in df_names['viaf_id'].drop_duplicates().tolist()])
oclc_by_viaf = oclc_lang.loc[(oclc_lang['100'].str.contains(viaf_ids) == True) | (oclc_lang['700'].str.contains(viaf_ids) == True)]
oclc_by_viaf.to_excel('oclc_by_viaf.xlsx', index=False)

# who created record

X040 = marc_parser_1_field(oclc_lang, '001', '040', '\$')
count_040 = X040.groupby(["$a", "$b"]).size().reset_index(name="frequency")
count_040.to_excel('oclc_cataloguing_agency.xlsx', index=False)

# languages: cz and others
oclc_cz_language = oclc_lang[oclc_lang['language'] == 'cze'].drop(columns='language')
oclc_cz_language.to_csv('oclc_cz_language.csv', index=False)
oclc_other_languages = oclc_lang[oclc_lang['language'] != 'cze'].drop(columns='language')
oclc_other_languages.to_csv('oclc_other_languages.csv', index=False)


# 01.12.2020

oclc_lang = pd.read_csv('F:/Cezary/Documents/IBL/Translations/OCLC/Czech origin_trans/oclc_lang.csv')
oclc_viaf = pd.read_excel('F:/Cezary/Documents/IBL/Translations/OCLC/Czech viaf/oclc_viaf.xlsx', engine='openpyxl')

oclc_full = pd.concat([oclc_lang, oclc_viaf])

oclc_full['language'] = oclc_full['008'].apply(lambda x: x[35:38])
oclc_other_languages = oclc_full[oclc_full['language'] != 'cze']

oclc_other_languages['nature_of_contents'] = oclc_other_languages['008'].apply(lambda x: x[24:27])
oclc_other_languages = oclc_other_languages[oclc_other_languages['nature_of_contents'].isin(['\\\\\\', '6\\\\', '\\6\\', '\\\\6'])]

oclc_other_languages['type of record + bibliographic level'] = oclc_other_languages['LDR'].apply(lambda x: x[6:8])
oclc_other_languages['fiction_type'] = oclc_other_languages['008'].apply(lambda x: x[33])

#11.12.2020

#general approach

fiction_types = ['1', 'd', 'f', 'h', 'j', 'p']

df_languages = oclc_other_languages[(oclc_other_languages['type of record + bibliographic level'] == 'am') &
                                    (oclc_other_languages['041'].str.contains('\$hcz')) &
                                    (oclc_other_languages['fiction_type'].isin(fiction_types))]
df_languages.to_excel("all_languages_first_positive.xlsx", index=False)
count_041 = marc_parser_1_field(df_languages, '001', '041', '\$')
count_041 = count_041['$a'].value_counts().to_frame()
count_041['language'] = count_041.index
count_041.reset_index(drop=True,inplace=True)
count_041.to_excel('count_languages_041a_from_all_positive.xlsx', index=False)

viaf_positives = marc_parser_1_field(df_languages, '001', '100', '\$')['$1'].drop_duplicates().to_list()
viaf_positives = [re.findall('\d+', l)[0] for l in viaf_positives if l] #849 vs. 809 vs. 754 in authority table


#detailed approach
fiction_types = ['1', 'd', 'f', 'h', 'j', 'p']
languages = ['pol', 'swe', 'ita', 'eng']
viaf_positives = gsheet_to_df('1QB5EmMhg7qSWfWaJurafHdmXc5uohQpS-K5GBsiqerA', 'Sheet1')['viaf_positive'].drop_duplicates().to_list()
viaf_positives = [f"http://viaf.org/viaf/{l}" for l in viaf_positives if l]

for language in languages:
    df = oclc_other_languages[(oclc_other_languages['language'] == language)]
    df_language_materials_monographs = df[df['type of record + bibliographic level'] == 'am']
    df_other_types = df[~df['001'].isin(df_language_materials_monographs['001'])]
    df_other_types.to_excel(f"oclc_{language}_other_types_(first_negative).xlsx", index=False)
    df_ok = df_language_materials_monographs[(df_language_materials_monographs['041'].str.contains('\$hcz')) &
                                             (df_language_materials_monographs['fiction_type'].isin(fiction_types))]
    df_ok.to_excel(f"oclc_{language}_positive.xlsx", index=False)
    df_second_positive = df_language_materials_monographs[~df_language_materials_monographs['001'].isin(df_ok['001'])]
    df_second_positive = marc_parser_1_field(df_second_positive, '001', '100', '\$')[['001', '$1']]
    df_second_positive = df_second_positive[df_second_positive['$1'].isin(viaf_positives)]
    df_second_positive = df_language_materials_monographs[(~df_language_materials_monographs['001'].isin(df_ok['001'])) &
                                                          (df_language_materials_monographs['001'].isin(df_second_positive['001']))]
    df_second_positive.to_excel(f"oclc_{language}_second_positive.xlsx", index=False)
    df_no = df_language_materials_monographs[(~df_language_materials_monographs['001'].isin(df_ok['001'])) &
                                             (~df_language_materials_monographs['001'].isin(df_second_positive['001']))]
    df_no.to_excel(f"oclc_{language}_negative.xlsx", index=False)
    print(f"Total records in {language}: {len(df)}")
    print(f"Total language material + monograph records in {language}: {len(df_language_materials_monographs)}")
    print(f"Total records with other types in {language}: {len(df_other_types)}")
    print(f"Total first positive records in {language}: {len(df_ok)}")
    print(f"Total second positive records (by VIAF) in {language}: {len(df_second_positive)}")
    print(f"Total negative records in {language}: {len(df_no)}")
    print("_______________________________________")
    

# 18.12.2020    
# new viafs
    
new_viafs = gsheet_to_df('1QB5EmMhg7qSWfWaJurafHdmXc5uohQpS-K5GBsiqerA', 'viaf_positive')
new_viafs = new_viafs[new_viafs['?'] == '#N/A']['viaf id'].to_list() 
new_viafs = [f"http://viaf.org/viaf/{l}" for l in new_viafs if l]   

test100 = marc_parser_1_field(df_languages, '001', '100', '\$')[['001', '$1']]
test100 = test100[test100['$1'].isin(new_viafs)]
test = df_languages[df_languages['001'].isin(test100['001'])]    
test.to_excel("strange_positives.xlsx", index=False)

# all names from positive viafs in negatives

fiction_types = ['1', 'd', 'f', 'h', 'j', 'p']

positive_viafs = gsheet_to_df('1QB5EmMhg7qSWfWaJurafHdmXc5uohQpS-K5GBsiqerA', 'Sheet1')['viaf_positive'].to_list()
positive_viafs = [f"http://viaf.org/viaf/{l}" for l in positive_viafs if l]
positive_viafs = list(set(positive_viafs))

negative = oclc_other_languages[oclc_other_languages['type of record + bibliographic level'] == 'am']

first_positive = negative[(negative['041'].str.contains('\$hcz')) &
                          (negative['fiction_type'].isin(fiction_types))]

negative = negative[~negative['001'].isin(first_positive['001'])]

second_positive = marc_parser_1_field(negative, '001', '100', '\$')[['001', '$1']]
second_positive = second_positive[second_positive['$1'].isin(positive_viafs)]

negative = negative[~negative['001'].isin(second_positive['001'])].reset_index(drop=True)

positive_viafs_names = gsheet_to_df('1QB5EmMhg7qSWfWaJurafHdmXc5uohQpS-K5GBsiqerA', 'Sheet1')
positive_viafs_names = positive_viafs_names[positive_viafs_names['viaf_positive'] != ''][['viaf_positive', 'all_names']]
positive_viafs_names = cSplit(positive_viafs_names, 'viaf_positive', 'all_names', '❦')
positive_viafs_names['all_names'] = positive_viafs_names['all_names'].apply(lambda x: re.sub('(.*?)(\$a.*?)(\$0.*$)', r'\2', x) if pd.notnull(x) else np.nan)
positive_viafs_names = positive_viafs_names[positive_viafs_names['all_names'].notnull()].drop_duplicates()

first_negative_to_check = "select * from negative a join positive_viafs_names b on a.'100' like '%'||b.all_names||'%'"
first_negative_to_check = pandasql.sqldf(first_negative_to_check)

first_negative_to_check.to_excel("oclc_negative_with_names_from_viaf.xlsx", index=False)

negative = negative[~negative['001'].isin(first_negative_to_check['001'])].reset_index(drop=True)

positive_viafs_diacritics = gsheet_to_df('1QB5EmMhg7qSWfWaJurafHdmXc5uohQpS-K5GBsiqerA', 'Sheet1')
positive_viafs_diacritics = positive_viafs_diacritics[positive_viafs_diacritics['viaf_positive'] != ''][['viaf_positive', 'cz_name']]

positive_viafs_diacritics['cz_name'] = positive_viafs_diacritics['cz_name'].apply(lambda x: unidecode.unidecode(x))

second_negative_to_check = "select * from negative a join positive_viafs_diacritics b on a.'100' like '%'||b.cz_name||'%'"
second_negative_to_check = pandasql.sqldf(second_negative_to_check)

second_negative_to_check.to_excel("oclc_negative_with_no_diacritics.xlsx", index=False)

# 05.01.2021 
# oclc pl and pl harvesting comparison

oclc_pl_files = ['C:/Users/Cezary/Documents/IBL-PAN-Python/oclc_pol_positive.xlsx', 'C:/Users/Cezary/Documents/IBL-PAN-Python/oclc_pol_second_positive.xlsx']

oclc_pl = pd.DataFrame()
for path in oclc_pl_files:
    df = pd.read_excel(path, engine='openpyxl')
    oclc_pl = oclc_pl.append(df)
    
 

oclc_pl = oclc_pl[['001', '008', '100', '245', '260', '300']]
oclc_pl['year'] = oclc_pl['008'].apply(lambda x: x[7:11])
oclc_pl['join column'] = oclc_pl[['245', '260', '300']].apply(lambda x: unidecode.unidecode('❦'.join(x.dropna().astype(str)).lower().replace(' ', '')), axis=1)
    
bn_pl = pd.read_excel('C:/Users/Cezary/Documents/IBL-PAN-Python/pol_data_clean.xlsx', engine='openpyxl')
bn_pl = bn_pl[bn_pl['language'] == 'pol'][['id', '008', '009', '100', '245', '260', '300']]
bn_pl['year'] = bn_pl['008'].apply(lambda x: x[7:11])
bn_pl['join column'] = bn_pl[['245', '260', '300']].apply(lambda x: unidecode.unidecode('❦'.join(x.dropna().astype(str)).lower().replace(' ', '')), axis=1)


test1 = oclc_pl[(oclc_pl['100'].notnull()) & (oclc_pl['100'].str.contains('Hašek'))].reset_index(drop=True)
test2 = bn_pl[(bn_pl['100'].notnull()) & (bn_pl['100'].str.contains('Hašek'))].reset_index(drop=True)

type(test2['join column'])

record = oclc_pl[oclc_pl['001'] == 836286406]

query = "select * from test1 a join test2 b on a.'join column' like b.'join column'"
test_merged = pandasql.sqldf(query)

test_merged = pd.merge(test1, test2, how='outer', on='join column')

'š' == 'š'

t_oclc = test1.iloc[41,:]
t_bn = test2.iloc[40,:]

o = '10$aprzygodydobregowojakaszwejkapodczaswojnyswiatowej.$nt.1,$pszwejknatylach/$cjaroslavhasek;[autoryz.przekl.zczes.p.hulki-laskowskiego].\\$a[warszawa]:$btowarzystwowydawnicze"roj",$c1929$e(warszawa:$f"rola").\\$a287,[1]s.;$c19cm.'

b = '10$aprzygodydobregowojakaszwejkapodczaswojnyswiatowej.$nt.1,$pszwejknatylach/$cjaroslavhasek;[autoryz.przekl.zczes.p.hulki-laskowskiego].\\$a[warszawa]:$btowarzystwowydawnicze"roj",$c1929$e(warszawa:$f"rola").\\$a287,[1]s.;$c19cm.'

o == b

#oclc
oclc245 = unidecode.unidecode('10$aPrzygody dobrego wojaka Szwejka podczas wojny światowej.$nT. 1,$pSzwejk na tyłach /$cJaroslav Hašek ; [autoryz. przekł. z czes. P. Hulki-Laskowskiego].').replace(' ', '')

oclc260 = '\\$a[Warszawa] :$bTowarzystwo Wydawnicze "Rój",$c1929$e(Warszawa :$f"Rola").'

oclc300 = '\\$a287, [1] s. ;$c19 cm.'

#bn

bn245 = unidecode.unidecode('10$aPrzygody dobrego wojaka Szwejka podczas wojny światowej. $nT. 1,$pSzwejk na tyłach /$cJaroslav Hašek ; [autoryz. przekł. z czes. P. Hulki-Laskowskiego].').replace(' ', '')

#unidecode, remove spaces, lowercase, preserve only letters and numbers

# 15.01.2021

oclc_lang = pd.read_csv('F:/Cezary/Documents/IBL/Translations/OCLC/Czech origin_trans/oclc_lang.csv')
oclc_viaf = pd.read_excel('F:/Cezary/Documents/IBL/Translations/OCLC/Czech viaf/oclc_viaf.xlsx', engine='openpyxl')
oclc_full = pd.concat([oclc_lang, oclc_viaf])
oclc_full['language'] = oclc_full['008'].apply(lambda x: x[35:38])
oclc_other_languages = oclc_full[oclc_full['language'] != 'cze']
oclc_other_languages['nature_of_contents'] = oclc_other_languages['008'].apply(lambda x: x[24:27])
oclc_other_languages = oclc_other_languages[oclc_other_languages['nature_of_contents'].isin(['\\\\\\', '6\\\\', '\\6\\', '\\\\6'])]
oclc_other_languages['type of record + bibliographic level'] = oclc_other_languages['LDR'].apply(lambda x: x[6:8])
oclc_other_languages['fiction_type'] = oclc_other_languages['008'].apply(lambda x: x[33])

fiction_types = ['1', 'd', 'f', 'h', 'j', 'p']
languages = ['pol']
viaf_positives = gsheet_to_df('1QB5EmMhg7qSWfWaJurafHdmXc5uohQpS-K5GBsiqerA', 'Sheet1')['viaf_positive'].drop_duplicates().to_list()
viaf_positives = [f"http://viaf.org/viaf/{l}" for l in viaf_positives if l]

positive_viafs_names = gsheet_to_df('1QB5EmMhg7qSWfWaJurafHdmXc5uohQpS-K5GBsiqerA', 'Sheet1')
positive_viafs_names = positive_viafs_names[positive_viafs_names['viaf_positive'] != ''][['viaf_positive', 'all_names']]
positive_viafs_names = cSplit(positive_viafs_names, 'viaf_positive', 'all_names', '❦')
positive_viafs_names['all_names'] = positive_viafs_names['all_names'].apply(lambda x: re.sub('(.*?)(\$a.*?)(\$0.*$)', r'\2', x) if pd.notnull(x) else np.nan)
positive_viafs_names = positive_viafs_names[positive_viafs_names['all_names'].notnull()].drop_duplicates()

positive_viafs_diacritics = gsheet_to_df('1QB5EmMhg7qSWfWaJurafHdmXc5uohQpS-K5GBsiqerA', 'Sheet1')
positive_viafs_diacritics = positive_viafs_diacritics[positive_viafs_diacritics['viaf_positive'] != ''][['viaf_positive', 'cz_name']]
positive_viafs_diacritics['cz_name'] = positive_viafs_diacritics['cz_name'].apply(lambda x: unidecode.unidecode(x))

for language in languages:
    df = oclc_other_languages[(oclc_other_languages['language'] == language)]
    df_language_materials_monographs = df[df['type of record + bibliographic level'] == 'am']
    negative = df_language_materials_monographs.copy()
    df_other_types = df[~df['001'].isin(df_language_materials_monographs['001'])]
    #df_other_types.to_excel(f"oclc_{language}_other_types_(first_negative).xlsx", index=False)
    df_first_positive = df_language_materials_monographs[(df_language_materials_monographs['041'].str.contains('\$hcz')) &
                                                         (df_language_materials_monographs['fiction_type'].isin(fiction_types))]
    negative = negative[~negative['001'].isin(df_first_positive['001'])]
    #df_first_positive.to_excel(f"oclc_{language}_positive.xlsx", index=False)
    df_second_positive = marc_parser_1_field(negative, '001', '100', '\$')[['001', '$1']]
    df_second_positive = df_second_positive[df_second_positive['$1'].isin(viaf_positives)]
    df_second_positive = negative[negative['001'].isin(df_second_positive['001'])]
    #df_second_positive.to_excel(f"oclc_{language}_second_positive.xlsx", index=False)
    negative = negative[~negative['001'].isin(df_second_positive['001'])].reset_index(drop=True)
    
    df_third_positive = "select * from negative a join positive_viafs_names b on a.'100' like '%'||b.all_names||'%'"
    df_third_positive = pandasql.sqldf(df_third_positive)
    
    negative = negative[~negative['001'].isin(df_third_positive['001'])].reset_index(drop=True)

    df_fourth_positive = "select * from negative a join positive_viafs_diacritics b on a.'100' like '%'||b.cz_name||'%'"
    df_fourth_positive = pandasql.sqldf(df_fourth_positive)

    negative = negative[~negative['001'].isin(df_fourth_positive['001'])].reset_index(drop=True)
    #negative.to_excel(f"oclc_{language}_negative.xlsx", index=False)
    
    df_all_positive = pd.concat([df_first_positive, df_second_positive, df_third_positive, df_fourth_positive])
    #df_all_positive.to_excel(f"oclc_{language}_df_all_positive.xlsx", index=False)


# oclc pl and pl harvesting comparison

oclc_pl = df_all_positive.copy().reset_index(drop=True)

#unidecode, remove spaces, lowercase, preserve only letters and numbers

def simplify_string(x):
    x = pd.Series([e[2:] for e in x if type(e) == str])
    x = unidecode.unidecode('❦'.join(x.dropna().astype(str)).lower().replace(' ', ''))
    final_string = ''
    for letter in x:
        if letter.isalnum():
            final_string += letter
    return final_string

oclc_pl['260'] = oclc_pl[['260', '264']].apply(lambda x: x['260'] if pd.notnull(x['260']) else x['264'], axis=1)
oclc_pl['join column'] = oclc_pl[['245', '260', '300']].apply(lambda x: simplify_string(x), axis=1)
oclc_pl['100_unidecode'] = oclc_pl['100'].apply(lambda x: unidecode.unidecode(x).lower() if pd.notnull(x) else x)
   
bn_pl = pd.read_excel('C:/Users/Cezary/Documents/IBL-PAN-Python/pol_data_clean.xlsx', engine='openpyxl')
bn_pl = bn_pl[bn_pl['language'] == 'pol']
#bn_pl['260'] = bn_pl[['260', '264']].apply(lambda x: x['260'] if pd.notnull(x['260']) else x['264'], axis=1)
bn_pl['join column'] = bn_pl[['245', '260', '300']].apply(lambda x: simplify_string(x), axis=1)
bn_pl['100_unidecode'] = bn_pl['100'].apply(lambda x: unidecode.unidecode(x).lower() if pd.notnull(x) else x)

authors = [('Hasek', '1-5embBUrzXP05EPK5LuyY9gv_885O-WQ9XqBNz-nZ_0'), 
           ('Hrabal', '1Y7598g6yba5e4ZgxbX8qzhEwSIGPRxLPgrIyf8wfVHc'), 
           ('Capek', '1dcTAq-qVQsPNvuc-4lopmc1bSGbwn3NFg645CKsuxqk')]

for author, link in authors:
    df_oclc = oclc_pl[(oclc_pl['100_unidecode'].notnull()) & (oclc_pl['100_unidecode'].str.contains(author.lower()))].reset_index(drop=True)
    df_bn = bn_pl[(bn_pl['100_unidecode'].notnull()) & (bn_pl['100_unidecode'].str.contains(author.lower()))].reset_index(drop=True)
      
    merged_sources = pd.merge(df_oclc, df_bn, how='outer', on='join column')
    join_column_index = merged_sources.columns.get_loc("join column")
    oclc_to_check = merged_sources[(merged_sources['001_x'].notnull()) & (merged_sources['001_y'].isnull())].iloc[:,:join_column_index]
    bn_to_check = merged_sources[(merged_sources['001_x'].isnull()) & (merged_sources['001_y'].notnull())].iloc[:,join_column_index+2:]
    print(f"Records for {author} in OCLC: {len(df_oclc)}")
    print(f"Records for {author} in BN: {len(df_bn)}")
    print(f"Records for {author} merged automatically: {len(merged_sources) - len(oclc_to_check) - len(bn_to_check)}")
    print(f"Unmerged records for {author} in OCLC: {len(oclc_to_check)}")
    print(f"Unmerged records for {author} in BN: {len(bn_to_check)}")
    print("_______________________________________")
    
    df_to_gsheet(merged_sources, link, 'all merged')
    # df_to_gsheet(oclc_to_check, link, 'oclc')
    # df_to_gsheet(bn_to_check, link, 'bn')


#%% data from OCLC only - 12.02.2021
    
file_list = drive.ListFile({'q': f"'{cr_projects}' in parents and trashed=false"}).GetList() 
translation_folder = [file['id'] for file in file_list if file['title'] == 'Vimr Project'][0]

# =============================================================================
# oclc_lang = pd.read_csv('F:/Cezary/Documents/IBL/Translations/OCLC/Czech origin_trans/oclc_lang.csv')
# oclc_viaf = pd.read_csv('F:/Cezary/Documents/IBL/Translations/OCLC/Czech viaf/oclc_viaf.csv', sep=';')
# oclc_full = pd.concat([oclc_lang, oclc_viaf])
# oclc_full['language'] = oclc_full['008'].apply(lambda x: x[35:38])
# oclc_other_languages = oclc_full[oclc_full['language'] != 'cze']
# =============================================================================

list_of_records = []
with open('F:/Cezary/Documents/IBL/Translations/OCLC/Czech origin_trans/oclc_lang.csv', 'r', encoding="utf8", errors="surrogateescape") as csv_file:
    reader = csv.reader(csv_file, delimiter=',')
    headers = next(reader)
    position_008 = headers.index('008')
    for row in reader:
        if row[position_008][35:38] != 'cze':
            list_of_records.append(row)
            
oclc_lang = pd.DataFrame(list_of_records, columns=headers)
oclc_lang['language'] = oclc_lang['008'].apply(lambda x: x[35:38])
oclc_viaf = pd.read_excel('F:/Cezary/Documents/IBL/Translations/OCLC/Czech viaf/oclc_viaf.xlsx')
oclc_viaf['language'] = oclc_viaf['008'].apply(lambda x: x[35:38])
oclc_viaf = oclc_viaf[oclc_viaf['language'] != 'cze']

oclc_other_languages = pd.concat([oclc_lang, oclc_viaf])

oclc_other_languages['nature_of_contents'] = oclc_other_languages['008'].apply(lambda x: x[24:27])
oclc_other_languages = oclc_other_languages[oclc_other_languages['nature_of_contents'].isin(['\\\\\\', '6\\\\', '\\6\\', '\\\\6'])]
oclc_other_languages['type of record + bibliographic level'] = oclc_other_languages['LDR'].apply(lambda x: x[6:8])
oclc_other_languages['fiction_type'] = oclc_other_languages['008'].apply(lambda x: x[33])

viaf_positives = gsheet_to_df('1QB5EmMhg7qSWfWaJurafHdmXc5uohQpS-K5GBsiqerA', 'Sheet1')['viaf_positive'].drop_duplicates().to_list()
viaf_positives = [f"http://viaf.org/viaf/{l}" for l in viaf_positives if l]

positive_viafs_names = gsheet_to_df('1QB5EmMhg7qSWfWaJurafHdmXc5uohQpS-K5GBsiqerA', 'Sheet1')
positive_viafs_names = positive_viafs_names[positive_viafs_names['viaf_positive'] != ''][['viaf_positive', 'all_names']]
positive_viafs_names = cSplit(positive_viafs_names, 'viaf_positive', 'all_names', '❦')
positive_viafs_names['all_names'] = positive_viafs_names['all_names'].apply(lambda x: re.sub('(.*?)(\$a.*?)(\$0.*$)', r'\2', x) if pd.notnull(x) else np.nan)
positive_viafs_names = positive_viafs_names[positive_viafs_names['all_names'].notnull()].drop_duplicates()

positive_viafs_diacritics = gsheet_to_df('1QB5EmMhg7qSWfWaJurafHdmXc5uohQpS-K5GBsiqerA', 'Sheet1')
positive_viafs_diacritics = positive_viafs_diacritics[positive_viafs_diacritics['viaf_positive'] != ''][['viaf_positive', 'cz_name']]
positive_viafs_diacritics['cz_name'] = positive_viafs_diacritics['cz_name'].apply(lambda x: unidecode.unidecode(x))

# oclc_other_languages['language'].drop_duplicates().sort_values().to_list()

fiction_types = ['1', 'd', 'f', 'h', 'j', 'p']
languages = ['pol', 'swe', 'ita', 'spa']
#languages = ['ita']

now = datetime.datetime.now()

for language in languages:
    print(language)
    df = oclc_other_languages[(oclc_other_languages['language'] == language)]
    df_language_materials_monographs = df[df['type of record + bibliographic level'] == 'am']
    negative = df_language_materials_monographs.copy()
    df_other_types = df[~df['001'].isin(df_language_materials_monographs['001'])]
    #df_other_types.to_excel(f"oclc_{language}_other_types_(first_negative).xlsx", index=False)
    df_first_positive = df_language_materials_monographs[(df_language_materials_monographs['041'].str.contains('\$hcz')) &
                                                         (df_language_materials_monographs['fiction_type'].isin(fiction_types))]
    negative = negative[~negative['001'].isin(df_first_positive['001'])]
    #df_first_positive.to_excel(f"oclc_{language}_positive.xlsx", index=False)
    df_second_positive = marc_parser_1_field(negative, '001', '100', '\$')[['001', '$1']]
    df_second_positive = df_second_positive[df_second_positive['$1'].isin(viaf_positives)]
    df_second_positive = negative[negative['001'].isin(df_second_positive['001'])]
    #df_second_positive.to_excel(f"oclc_{language}_second_positive.xlsx", index=False)
    negative = negative[~negative['001'].isin(df_second_positive['001'])].reset_index(drop=True)
    
    df_third_positive = "select * from negative a join positive_viafs_names b on a.'100' like '%'||b.all_names||'%'"
    df_third_positive = pandasql.sqldf(df_third_positive)
    
    negative = negative[~negative['001'].isin(df_third_positive['001'])].reset_index(drop=True)

    df_fourth_positive = "select * from negative a join positive_viafs_diacritics b on a.'100' like '%'||b.cz_name||'%'"
    df_fourth_positive = pandasql.sqldf(df_fourth_positive)

    negative = negative[~negative['001'].isin(df_fourth_positive['001'])].reset_index(drop=True)
    #negative.to_excel(f"oclc_{language}_negative.xlsx", index=False)
    
    df_all_positive = pd.concat([df_first_positive, df_second_positive, df_third_positive, df_fourth_positive])
    #df_all_positive.to_excel(f"oclc_{language}_df_all_positive.xlsx", index=False)
    
    df_all_positive['260'] = df_all_positive[['260', '264']].apply(lambda x: x['260'] if pd.notnull(x['260']) else x['264'], axis=1)
    df_all_positive['240'] = df_all_positive[['240', '246']].apply(lambda x: x['240'] if pd.notnull(x['240']) else x['246'], axis=1)
    df_all_positive['100_unidecode'] = df_all_positive['100'].apply(lambda x: unidecode.unidecode(x).lower() if pd.notnull(x) else x)
    
    authors = ['Hasek', 
               'Hrabal', 
               'Capek']
    
    year = now.year
    month = now.month
    day = now.day

    
    for i, author in enumerate(authors):
        sheet = gc.create(f'{author}_{language}_{year}-{month}-{day}', translation_folder)
        s = gp.Spread(sheet.id, creds=credentials)
        authors[i] = [authors[i]]
        authors[i] += [sheet.id, s]
    
    for index, (author, g_id, g_sheet) in enumerate(authors):
        # author = authors[0][0]
        # g_id = authors[0][1]
        # g_sheet = authors[0][2]
        print(f"{index+1}/{len(authors)}")
        
        #all
        
        df_oclc = df_all_positive[(df_all_positive['100_unidecode'].notnull()) &       
                                  (df_all_positive['100_unidecode'].str.contains(author.lower()))].reset_index(drop=True)
        df_oclc['001'] = df_oclc['001'].astype(int)
        sh = gc.open_by_key(g_id)
        wsh = sh.get_worksheet(0)
        wsh.update_title('all')
        g_sheet.df_to_sheet(df_oclc, sheet='all', index=0)
        
        #de-duplication 1: duplicates
        try:
            title = marc_parser_1_field(df_oclc, '001', '245', '\$')[['001', '$a', '$b', '$n', '$p']].replace(r'^\s*$', np.nan, regex=True)
        except KeyError:
            title = marc_parser_1_field(df_oclc, '001', '245', '\$')[['001', '$a', '$b']].replace(r'^\s*$', np.nan, regex=True)
        title['title'] = title[title.columns[1:]].apply(lambda x: simplify_string(x, with_spaces=False), axis=1)    
        title = title[['001', 'title']]
        df_oclc = pd.merge(df_oclc, title, how='left', on='001')
        
        place = marc_parser_1_field(df_oclc, '001', '260', '\$')[['001', '$a']].rename(columns={'$a':'place'})
        place = place[place['place'] != '']
        place['place'] = place['place'].apply(lambda x: simplify_string(x, with_spaces=False))
        df_oclc = pd.merge(df_oclc, place, how='left', on='001')
        
        publisher = marc_parser_1_field(df_oclc, '001', '260', '\$')[['001', '$b']].rename(columns={'$b':'publisher'})
        publisher = publisher.groupby('001').head(1).reset_index(drop=True)
        publisher['publisher'] = publisher['publisher'].apply(lambda x: simplify_string(x, with_spaces=False))
        df_oclc = pd.merge(df_oclc, publisher, how='left', on='001')
        
        
        year = df_oclc.copy()[['001', '008']].rename(columns={'008':'year'})
        year['year'] = year['year'].apply(lambda x: x[7:11])
        df_oclc = pd.merge(df_oclc, year, how='left', on='001')
        
        df_oclc_duplicates = pd.DataFrame()
        df_oclc_grouped = df_oclc.groupby(['title', 'place', 'year'])
        for name, group in df_oclc_grouped:
            if len(group) > 1:
                group['groupby'] = str(name)
                group_ids = '❦'.join([str(e) for e in group['001'].to_list()])
                group['group_ids'] = group_ids
                df_oclc_duplicates = df_oclc_duplicates.append(group)
        df_oclc_duplicates = df_oclc_duplicates.drop_duplicates()
        
        oclc_duplicates_list = df_oclc_duplicates['001'].drop_duplicates().tolist()
        df_oclc_duplicates_grouped = df_oclc_duplicates.groupby(['title', 'place', 'year'])
        
        df_oclc_deduplicated = pd.DataFrame()
        for name, group in df_oclc_duplicates_grouped:
            for column in group:
                if column in ['fiction_type', '490', '500', '650', '655']:
                    group[column] = '❦'.join(group[column].dropna().drop_duplicates().astype(str))
                else:
                    group[column] = group[column].dropna().astype(str).max()
            df_oclc_deduplicated = df_oclc_deduplicated.append(group)
        
        df_oclc_deduplicated = df_oclc_deduplicated.drop_duplicates().replace(r'^\s*$', np.nan, regex=True)
        df_oclc_deduplicated['001'] = df_oclc_deduplicated['001'].astype(int)
        
        df_oclc = df_oclc[~df_oclc['001'].isin(oclc_duplicates_list)]
        df_oclc = pd.concat([df_oclc, df_oclc_deduplicated]).drop(columns='title')
        df_oclc['group_ids'] = df_oclc['group_ids'].apply(lambda x: '❦'.join(set(x.split('❦'))) if pd.notnull(x) else x)
        g_sheet.df_to_sheet(df_oclc, sheet='after_removing_duplicates', index=0)
        
        #de-duplication 2: multiple volumes
        
        title = marc_parser_1_field(df_oclc, '001', '245', '\$')[['001', '$a']].replace(r'^\s*$', np.nan, regex=True)
        title['title'] = title[title.columns[1:]].apply(lambda x: simplify_string(x, with_spaces=False), axis=1)    
        title = title[['001', 'title']]
        df_oclc = pd.merge(df_oclc, title, how='left', on='001')  
        
        df_oclc_grouped = df_oclc.groupby(['title', 'place', 'year'])
            
        df_oclc_multiple_volumes = pd.DataFrame()
        for name, group in df_oclc_grouped:
            if len(group[group['245'].str.contains('\$n', regex=True)]):
                group['groupby'] = str(name)
                group_ids = '❦'.join(set([str(e) for e in group['001'].to_list() + group['group_ids'].to_list() if pd.notnull(e)]))
                group['group_ids'] = group_ids
                df_oclc_multiple_volumes = df_oclc_multiple_volumes.append(group)
                
        if df_oclc_multiple_volumes.shape[0] > 0:
            oclc_multiple_volumes_list = df_oclc_multiple_volumes['001'].drop_duplicates().tolist()
            df_oclc_multiple_volumes_grouped = df_oclc_multiple_volumes.groupby(['title', 'place', 'year'])
        
            df_oclc_multiple_volumes_deduplicated = pd.DataFrame()
            for name, group in df_oclc_multiple_volumes_grouped:
                if len(group[~group['245'].str.contains('\$n', regex=True)]) == 1:
                    for column in group:
                        if column in ['fiction_type', '490', '500', '650', '655']:
                            group[column] = '❦'.join(group[column].dropna().drop_duplicates().astype(str))  
                        elif column in ['001', '245']:
                            pass
                        else:
                            group[column] = group[column].dropna().astype(str).max()
                    df = group[~group['245'].str.contains('\$n', regex=True)]
                    df_oclc_multiple_volumes_deduplicated = df_oclc_multiple_volumes_deduplicated.append(df)
                else:
                    for column in group:
                        if column in ['fiction_type', '490', '500', '650', '655']:
                            group[column] = '❦'.join(group[column].dropna().drop_duplicates().astype(str))
                        elif column == '245':
                            field_245 = marc_parser_1_field(group, '001', '245', '\$').replace(r'^\s*$', np.nan, regex=True)
                            field_245 = field_245.iloc[:, lambda x: x.columns.isin(['001', '$a', '$b', '$c'])]
                            field_245['245'] = field_245[field_245.columns[1:]].apply(lambda x: ''.join(x.dropna().astype(str)), axis=1)
                            field_245 = field_245[['001', '245']]
                            field_245['245'] = '10' + field_245['245']
                            group = pd.merge(group.drop(columns='245'), field_245, how='left', on='001')
                            group[column] = group[column].dropna().astype(str).max()
                        else:
                            group[column] = group[column].dropna().astype(str).max()
                    group = group.drop_duplicates().reset_index(drop=True)
                    df_oclc_multiple_volumes_deduplicated = df_oclc_multiple_volumes_deduplicated.append(group)
                        
            df_oclc_multiple_volumes_deduplicated = df_oclc_multiple_volumes_deduplicated.drop_duplicates().replace(r'^\s*$', np.nan, regex=True)
            df_oclc_multiple_volumes_deduplicated['001'] = df_oclc_multiple_volumes_deduplicated['001'].astype(int)
            
            df_oclc = df_oclc[~df_oclc['001'].isin(oclc_multiple_volumes_list)]
            df_oclc = pd.concat([df_oclc, df_oclc_multiple_volumes_deduplicated]).drop_duplicates().reset_index(drop=True)
            df_oclc['group_ids'] = df_oclc['group_ids'].apply(lambda x: '❦'.join(set(x.split('❦'))) if pd.notnull(x) else x)
            g_sheet.df_to_sheet(df_oclc, sheet='after_removing_multiple_volumes', index=0)
            
        #de-duplication 3: fuzzyness
        df_oclc.drop(columns='title', inplace=True)
        field_245 = marc_parser_1_field(df_oclc, '001', '245', '\$').replace(r'^\s*$', np.nan, regex=True)
        field_245['$a'] = field_245.apply(lambda x: x['$a'] if pd.notnull(x['$a']) else x['indicator'][2:].split('.', 1)[0], axis=1)
        field_245 = field_245.iloc[:, lambda x: x.columns.isin(['001', '$a', '$b'])]
        field_245['title'] = field_245[field_245.columns[1:]].apply(lambda x: simplify_string(x), axis=1)  
        field_245 = field_245[['001', 'title']]
        df_oclc = pd.merge(df_oclc, field_245, how='left', on='001')
        
        #similarity level == 0.85 | columns == ['title', 'publisher', 'year'] | same 'year'
        df_oclc_clusters = cluster_records(df_oclc, '001', ['title', 'publisher', 'year'], 0.85)    
        df_oclc_clusters = df_oclc_clusters[df_oclc_clusters['publisher'] != '']
        df_oclc_duplicates = df_oclc_clusters.groupby(['cluster', 'year']).filter(lambda x: len(x) > 1)
        
        if df_oclc_duplicates.shape[0] > 0:
     
            if df_oclc_duplicates['001'].value_counts().max() > 1:
                sys.exit('ERROR!!!\nclustering problem!!!')
        
            oclc_duplicates_list = df_oclc_duplicates['001'].drop_duplicates().tolist()
            df_oclc_duplicates = df_oclc_duplicates.groupby('cluster')
            
            df_oclc_deduplicated = pd.DataFrame()
            for name, group in df_oclc_duplicates:
                group_ids = '❦'.join(set([str(e) for e in group['001'].to_list() + group['group_ids'].to_list() if pd.notnull(e)]))
                group['group_ids'] = group_ids
                for column in group:
                    if column in ['fiction_type', '490', '500', '650', '655']:
                        group[column] = '❦'.join(group[column].dropna().drop_duplicates().astype(str))
                    elif column == '245':
                        group[column] = group[column][group[column].str.contains('$', regex=False)]
                        group[column] = group[column].dropna().astype(str).max()
                    else:
                        group[column] = group[column].dropna().astype(str).max()
                df_oclc_deduplicated = df_oclc_deduplicated.append(group)
                
            df_oclc_deduplicated = df_oclc_deduplicated.drop_duplicates().replace(r'^\s*$', np.nan, regex=True)
            df_oclc_deduplicated['001'] = df_oclc_deduplicated['001'].astype(int)
            df_oclc = df_oclc[~df_oclc['001'].isin(oclc_duplicates_list)]
            df_oclc = pd.concat([df_oclc, df_oclc_deduplicated])
            df_oclc['group_ids'] = df_oclc['group_ids'].apply(lambda x: '❦'.join(set(x.split('❦'))) if pd.notnull(x) else x)
            g_sheet.df_to_sheet(df_oclc, sheet='after_fuzzy_duplicates_0.85_tit_pub_year', index=0)
            
    # =============================================================================
    #     #similarity level == 0.8 | columns == ['title', 'publisher'] | same 'year'
    #     df_oclc.drop(columns='cluster', inplace=True)
    #     df_oclc_clusters = cluster_records(df_oclc, '001', ['title', 'place', 'publisher', 'year'], 0.85) 
    #     df_oclc_duplicates = df_oclc_clusters.groupby(['cluster', 'year']).filter(lambda x: len(x) > 1)
    #     
    #     df_oclc_duplicates[['001', '245', 'year', '260']].to_excel('test_oclc1.xlsx', index=False)
    #     
    #     if df_oclc_duplicates.shape[0] > 0:
    #     
    #         if df_oclc_duplicates['001'].value_counts().max() > 1:
    #             sys.exit('ERROR!!!\nclustering problem!!!')
    #     
    #         oclc_duplicates_list = df_oclc_duplicates['001'].drop_duplicates().tolist()
    #         df_oclc_duplicates = df_oclc_duplicates.groupby('cluster')
    #         
    #         df_oclc_deduplicated = pd.DataFrame()
    #         for name, group in df_oclc_duplicates:
    #             group_ids = '❦'.join(set([str(e) for e in group['001'].to_list() + group['group_ids'].to_list() if pd.notnull(e)]))
    #             group['group_ids'] = group_ids
    #             for column in group:
    #                 if column in ['fiction_type', '490', '500', '650', '655']:
    #                     group[column] = '❦'.join(group[column].dropna().drop_duplicates().astype(str))
    #                 elif column == '245':
    #                     group[column] = group[column][group[column].str.contains('$', regex=False)]
    #                     group[column] = group[column].dropna().astype(str).max()
    #                 else:
    #                     group[column] = group[column].dropna().astype(str).max()
    #             df_oclc_deduplicated = df_oclc_deduplicated.append(group)
    #             
    #         df_oclc_deduplicated = df_oclc_deduplicated.drop_duplicates().replace(r'^\s*$', np.nan, regex=True)
    #         df_oclc_deduplicated['001'] = df_oclc_deduplicated['001'].astype(int)
    #         df_oclc = df_oclc[~df_oclc['001'].isin(oclc_duplicates_list)]
    #         df_oclc = pd.concat([df_oclc, df_oclc_deduplicated])
    #         df_oclc['group_ids'] = df_oclc['group_ids'].apply(lambda x: '❦'.join(set(x.split('❦'))) if pd.notnull(x) else x)
    #         g_sheet.df_to_sheet(df_oclc, sheet='after_fuzzy_duplicates_0.85_tit_pub_year', index=0)
    # =============================================================================
        
        #editions counter
        edition_clusters = cluster_strings(df_oclc['title'], 0.7)
        edition_clusters_df = pd.DataFrame()
        for k, v in edition_clusters.items():
            df = df_oclc.copy()[df_oclc['title'].str.strip().isin(v)]
            df['edition_cluster'] = k
            edition_clusters_df = edition_clusters_df.append(df)
        edition_clusters_df['edition_index'] = edition_clusters_df.groupby('edition_cluster').cumcount()+1
        df_oclc = edition_clusters_df.copy()
        g_sheet.df_to_sheet(df_oclc, sheet='final_marc21_with_editions_counters', index=0)
           
        #simplify the records
        df_oclc = df_oclc[['001', '080', '100', '245', '240', '260', '650', '655', '700', 'language', 'fiction_type', 'place', 'year', 'edition_cluster', 'edition_index']]
        df_oclc['001'] = df_oclc['001'].astype(int)
        
        identifiers = df_oclc[['001']]
        udc = marc_parser_1_field(df_oclc, '001', '080', '\$')[['001', '$a']].rename(columns={'$a':'universal decimal classification'})
        udc['universal decimal classification'] = udc.groupby('001')['universal decimal classification'].transform(lambda x: '❦'.join(x.dropna().drop_duplicates().astype(str)))
        udc = udc.drop_duplicates().reset_index(drop=True)
        marc_author = marc_parser_1_field(df_oclc, '001', '100', '\$')[['001', '$a', '$d', '$1']].rename(columns={'$a':'author name', '$d':'author birth and death', '$1':'author viaf id'})
        for column in marc_author.columns[1:]:
            marc_author[column] = marc_author.groupby('001')[column].transform(lambda x: '❦'.join(x.dropna().drop_duplicates().astype(str)))
        marc_author = marc_author.drop_duplicates().reset_index(drop=True)
        title = marc_parser_1_field(df_oclc, '001', '245', '\$').replace(r'^\s*$', np.nan, regex=True)
        title['$a'] = title.apply(lambda x: x['245'] if pd.isnull(x['$a']) else x['$a'], axis=1)
        title = title.iloc[:, lambda x: x.columns.isin(['001', '$a', '$b'])]
        title['title'] = title[title.columns[1:]].apply(lambda x: ''.join(x.dropna().astype(str)), axis=1)
        title = title[['001', 'title']]
        original_title = marc_parser_1_field(df_oclc, '001', '240', '\$').replace(r'^\s*$', np.nan, regex=True)[['001', '$a']].rename(columns={'$a':'original title'})
        place_of_publication = marc_parser_1_field(df_oclc, '001', '260', '\$').replace(r'^\s*$', np.nan, regex=True)[['001', '$a']].rename(columns={'$a':'place of publication'})
        #$e as alternative place of publication?
        try:
            contributor = marc_parser_1_field(df_oclc, '001', '700', '\$').replace(r'^\s*$', np.nan, regex=True)[['001', '$a', '$d', '$e', '$1']].rename(columns={'$a':'contributor name', '$d':'contributor birth and death', '$1':'contributor viaf id', '$e':'contributor role'})
            contributor['contributor role'] = contributor['contributor role'].apply(lambda x: x if pd.notnull(x) else 'unknown')
        except KeyError:
            contributor['contributor role'] = 'unknown'
            
        dfs = [identifiers, udc, marc_author, title, original_title, contributor, df_oclc[['001', '650', '655', 'language', 'fiction_type', 'place', 'year', 'edition_cluster', 'edition_index']]]
        df_oclc_final = reduce(lambda left,right: pd.merge(left,right,on='001', how='outer'), dfs).drop_duplicates()
        g_sheet.df_to_sheet(df_oclc_final, sheet='simplified shape', index=0)
        time.sleep(60)


#SystemExit przy drugim autorze - dlaczego???
    
oclc_pl = df_all_positive.copy().reset_index(drop=True)
oclc_pl['260'] = oclc_pl[['260', '264']].apply(lambda x: x['260'] if pd.notnull(x['260']) else x['264'], axis=1)
oclc_pl['240'] = oclc_pl[['240', '246']].apply(lambda x: x['240'] if pd.notnull(x['240']) else x['246'], axis=1)
oclc_pl['100_unidecode'] = oclc_pl['100'].apply(lambda x: unidecode.unidecode(x).lower() if pd.notnull(x) else x)

#oclc_pl.to_excel('oclc_pl_test.xlsx', index=False)

oclc_pl = pd.read_excel('oclc_pl_test.xlsx')    
    
    
    
    
    #duplicates
    
    # try:
    #     title = marc_parser_1_field(df_oclc, '001', '245', '\$')[['001', '$a', '$b', '$n', '$p']].replace(r'^\s*$', np.nan, regex=True)
    # except KeyError:
    #     title = marc_parser_1_field(df_oclc, '001', '245', '\$')[['001', '$a', '$b']].replace(r'^\s*$', np.nan, regex=True)
    # title['title'] = title[title.columns[1:]].apply(lambda x: simplify(x), axis=1)    
    # title = title[['001', 'title']]
    # df_oclc = pd.merge(df_oclc, title, how='left', on='001')
    
    # place = marc_parser_1_field(df_oclc, '001', '260', '\$')[['001', '$a']].rename(columns={'$a':'place'})
    # place['place'] = place['place'].apply(lambda x: simplify(x))
    # df_oclc = pd.merge(df_oclc, place, how='left', on='001')
    
    # year = df_oclc[['001', '008']].rename(columns={'008':'year'})
    # year['year'] = year['year'].apply(lambda x: x[7:11])
    # df_oclc = pd.merge(df_oclc, year, how='left', on='001')
    
    # give up the pages???
    
    # pages = marc_parser_1_field(df_oclc, '001', '300', '\$')[['001', '$a']].rename(columns={'$a':'pages'})
    # pages['pages'] = pages['pages'].apply(lambda x: max([int(e) for e in re.findall('\d+', x)]) if re.findall('\d+', x) else '')
    # pages['pages range'] = pages['pages'].apply(lambda x: range(int((98 * x) / 100), int((102 * x) / 100)) if x!="" else x)
    
    # for i, row in pages.iterrows():
    #     print(f"{i+1}/{len(pages)}")
    #     if row['pages'] != '':
    #         matches = []
    #         for ind, row2 in pages.iterrows():
    #             if row2['pages range'] != '':
    #                 if row2['pages range'][0] <= row['pages'] <= row2['pages range'][-1] and ind != i:
    #                     matches.append(row2['001'])    
    #         pages.at[i, 'matches'] = ','.join(str(x) for x in matches)
    
    # df_oclc = pd.merge(df_oclc, pages, how='left', on='001')
    
    # df_oclc_duplicates = pd.DataFrame()
    # df_oclc_grouped = df_oclc.groupby(['title', 'place', 'year'])
    # for name, group in df_oclc_grouped:
    #     if len(group) > 1:
    #         group['groupby'] = str(name)
    #         df_oclc_duplicates = df_oclc_duplicates.append(group)
    # df_oclc_duplicates['pages range'] = df_oclc_duplicates['pages range'].astype(str)
    # df_oclc_duplicates = df_oclc_duplicates.drop_duplicates()
            
    # df_to_gsheet(df_oclc_duplicates, link, 'duplicates')
    
    #de-duplication
    
    # oclc_duplicates_list = df_oclc_duplicates['001'].drop_duplicates().tolist()
    # df_oclc_duplicates_grouped = df_oclc_duplicates.groupby(['title', 'place', 'year'])
    
    # df_oclc_deduplicated = pd.DataFrame()
    # for name, group in df_oclc_duplicates_grouped:
    #     group_ids = ','.join([str(e) for e in group['001'].to_list()])
    #     group['group_ids'] = group_ids
    #     for column in group:
    #         group[column] = group[column].astype(str).max()
    #     df_oclc_deduplicated = df_oclc_deduplicated.append(group)
    
    # df_oclc_deduplicated = df_oclc_deduplicated.drop_duplicates()
    # df_to_gsheet(df_oclc_deduplicated, link, 'de-duplication')
    
    # # all de-duplicated
    
    # df_oclc = df_oclc[~df_oclc['001'].isin(oclc_duplicates_list)]
    # df_oclc = pd.concat([df_oclc, df_oclc_deduplicated])
    # df_oclc['pages range'] = df_oclc['pages range'].astype(str)
    # df_to_gsheet(df_oclc, link, 'all de-duplicated')
    
    # multiple volumes
    
    # df_oclc = gsheet_to_df(link, 'all')
    
    # title = marc_parser_1_field(df_oclc, '001', '245', '\$')[['001', '$a']].replace(r'^\s*$', np.nan, regex=True)
    # title['title'] = title[title.columns[1:]].apply(lambda x: simplify(x), axis=1)    
    # title = title[['001', 'title']]
    # df_oclc = pd.merge(df_oclc, title, how='left', on='001')
    
    # place = marc_parser_1_field(df_oclc, '001', '260', '\$')[['001', '$a']].rename(columns={'$a':'place'})
    # place['place'] = place['place'].apply(lambda x: simplify(x))
    # df_oclc = pd.merge(df_oclc, place, how='left', on='001')
    
    # year = df_oclc[['001', '008']].rename(columns={'008':'year'})
    # year['year'] = year['year'].apply(lambda x: x[7:11])
    # df_oclc = pd.merge(df_oclc, year, how='left', on='001')
    
    # df_oclc_grouped = df_oclc.groupby(['title', 'place', 'year'])
        
    # df_oclc_multiple_volumes = pd.DataFrame()

    # for name, group in df_oclc_grouped:
    #     if len(group[group['245'].str.contains('\$n', regex=True)]):
    #         group['groupby'] = str(name)
    #         df_oclc_multiple_volumes = df_oclc_multiple_volumes.append(group)
            
    # df_to_gsheet(df_oclc_multiple_volumes, link, 'multiple volumes')
    
    # if len(df_oclc_multiple_volumes) > 0:
    #     df_oclc_multiple_volumes_grouped = df_oclc_multiple_volumes.groupby(['title', 'place', 'year'])
    
    #     df_oclc_multiple_volumes_deduplicated = pd.DataFrame()
    #     df_oclc_multiple_volumes_without_deduplication = pd.DataFrame()
        
    #     for name, group in df_oclc_multiple_volumes_grouped:
    #         if len(group[~group['245'].str.contains('\$n', regex=True)]) == 1:
    #             df = group[~group['245'].str.contains('\$n', regex=True)]
    #             df_oclc_multiple_volumes_deduplicated = df_oclc_multiple_volumes_deduplicated.append(df)
    #         else:
    #             df_oclc_multiple_volumes_without_deduplication = df_oclc_multiple_volumes_without_deduplication.append(group)
        
    #     df_to_gsheet(df_oclc_multiple_volumes_deduplicated, link, 'volumes de-duplicated')        
    #     df_to_gsheet(df_oclc_multiple_volumes_without_deduplication, link, 'volumes not de-duplicated')
    
    # df_simple = gsheet_to_df(link, 'all de-duplicated')[['001', '080', '100', '245', '240', '260', '650', '655', '700', 'language', 'fiction_type']]
    # df_to_gsheet(df_simple, link, 'simple')


#%% test string similarity - 04.02.2021  
      
test1a = unidecode.unidecode("Przygody dobrego wojaka Szwejka").lower()
test1b = unidecode.unidecode("Przygody dobrego wojanka szwejka").lower()
# test1a = "Przygody dobrego wojaka Szwejka"
# test1b = "Przygody dobrego wojanka szwejka"


test2a = unidecode.unidecode("Opowieści o piesku i kotce jak razem gospodarzyli i jeszcze innych rzeczach").lower()
test2b = unidecode.unidecode("Opowieści o piesku i kotce jak razem gospodarzyli i jeszcze o różnych innych rzeczach").lower()

test3a = 'Przygody dobrego wojaka Szwejka :$bpodczas wojny światowej'
test3b = 'Przygody dobrego wojaka Szwejka podczas wojny światowej'


#lev
print("Levenshtein")
print(lev.distance(test1a, test1b))
print(lev.distance(test2a, test2b))
print(lev.distance(test3a, test3b))

#cosine
print("cosine")
print(get_cosine_result(test1a, test1b))
print(get_cosine_result(test2a, test2b))
print(get_cosine_result(test3a, test3b))

#divlib
print("SequenceMatcher")
print(SequenceMatcher(a=test1a, b=test1b).ratio())
print(SequenceMatcher(a=test2a, b=test2b).ratio())
print(SequenceMatcher(a=test3a, b=test3b).ratio())
















 
#%% notatki


oclc_cz_from_all = oclc_full[oclc_full['language'] == 'cze']
oclc_other_languages = oclc_full[oclc_full['language'] != 'cze']

oclc_other_languages['nature_of_contents'] = oclc_other_languages['008'].apply(lambda x: x[24:27])
oclc_other_languages = oclc_other_languages[oclc_other_languages['nature_of_contents'].isin(['\\\\\\', '6\\\\', '\\6\\', '\\\\6'])]

oclc_other_languages['language_material'] = oclc_other_languages['LDR'].apply(lambda x: x[6])
oclc_other_languages['monograph'] = oclc_other_languages['LDR'].apply(lambda x: x[7])
oclc_other_languages = oclc_other_languages[(oclc_other_languages['language_material'] == 'a') &
                                            (oclc_other_languages['monograph'] == 'm')] # wcześniej było "a" i były błędy

oclc_other_languages.to_excel('oclc_other_languages.xlsx', index=False)

oclc_language_008 = oclc_other_languages['008'].apply(lambda x: x[35:38]).to_frame()
count = oclc_language_008['008'].value_counts().to_frame()
count['language'] = count.index
count.reset_index(drop=True,inplace=True)
count.to_excel('oclc_other_lang_count_008.xlsx', index=False)


oclc_other_languages = pd.read_excel('oclc_other_languages.xlsx')






oclc_other_languages['is_fiction'] = oclc_other_languages['008'].apply(lambda x: x[33])
oclc_other_languages_fiction = oclc_other_languages[oclc_other_languages['is_fiction'] != '0']


oclc_pl = oclc_other_languages[oclc_other_languages['language'] == 'pol']

hasek_pl = oclc_pl[(oclc_pl['100'].notnull()) & (oclc_pl['100'].str.contains('4931097'))]
hrabal_pl = oclc_pl[(oclc_pl['100'].notnull()) & (oclc_pl['100'].str.contains('34458072'))]
kundera_pl = oclc_pl[(oclc_pl['100'].notnull()) & (oclc_pl['100'].str.contains('51691735'))]

oclc_swe = oclc_other_languages[oclc_other_languages['language'] == 'swe']
oclc_swe['is_fiction'] = oclc_swe['008'].apply(lambda x: x[33])

oclc_swe_ok = oclc_swe[oclc_swe['is_fiction'] != '0']
oclc_swe_out = oclc_swe[~oclc_swe['001'].isin(oclc_swe_ok['001'])]



hasek_swe = oclc_swe[(oclc_swe['100'].notnull()) & (oclc_swe['100'].str.contains('4931097'))]
hrabal_swe = oclc_swe[(oclc_swe['100'].notnull()) & (oclc_swe['100'].str.contains('34458072'))]
kundera_swe = oclc_swe[(oclc_swe['100'].notnull()) & (oclc_swe['100'].str.contains('51691735'))]

oclc_fin = oclc_other_languages[oclc_other_languages['language'] == 'fin']

hasek_fin = oclc_fin[(oclc_fin['100'].notnull()) & (oclc_fin['100'].str.contains('4931097'))]
hrabal_fin = oclc_fin[(oclc_fin['100'].notnull()) & (oclc_fin['100'].str.contains('34458072'))]
kundera_fin = oclc_fin[(oclc_fin['100'].notnull()) & (oclc_fin['100'].str.contains('51691735'))]


#ita from oclc

oclc_ita = oclc_other_languages[oclc_other_languages['language'] == 'ita']
oclc_ita['is_fiction'] = oclc_ita['008'].apply(lambda x: x[33])

oclc_ita_ok = oclc_ita[oclc_ita['is_fiction'] != '0']
oclc_ita_out = oclc_ita[~oclc_ita['001'].isin(oclc_ita_ok['001'])]
oclc_ita_ok.to_excel('oclc_ita_positive.xlsx', index=False)
oclc_ita_out.to_excel('oclc_ita_negative.xlsx', index=False)


#swe national library file

swe_all = pd.read_excel('swe_data_clean.xlsx')

swe_all['language'] = swe_all['008'].apply(lambda x: x[35:38])

swe_all = swe_all[swe_all['language'] == 'swe']
swe = swe_all.copy()
swe['nature_of_contents'] = swe['008'].apply(lambda x: x[24:27])
swe = swe[swe['nature_of_contents'].isin(['\\\\\\', '6\\\\', '\\6\\', '\\\\6'])]

swe['language_material'] = swe['LDR'].apply(lambda x: x[6])
swe['monograph'] = swe['LDR'].apply(lambda x: x[7])
swe = swe[(swe['language_material'] == 'a') &
          (swe['monograph'] == 'm')] # wcześniej było "a" i były błędy
swe['is_fiction'] = swe['008'].apply(lambda x: x[33])
swe = swe[swe['is_fiction'] != '0']

swe_out = swe_all[~swe_all['001'].isin(swe['001'])]


oclc_swe_ok.to_excel('oclc_swe_positive.xlsx', index=False)
oclc_swe_out.to_excel('oclc_swe_negative.xlsx', index=False)


swe.to_excel('swe_national_library_positive.xlsx', index=False)
swe_out.to_excel('swe_national_library_negative.xlsx', index=False)

records_no = [910969568, 906979376, 1041639715]

test = oclc_full[oclc_full['001'].isin(records_no)]


# skupić się na: pol, swe, eng, ita
# zbudować wnioskowanie i pozytywne pliki dla tych języków

# steps from out to in
#1 041 $aita$hcze + viaf
#2 viaf

# workflow - 1. positive do tabeli, 2. pierwszy positive z negative jako kolejny arkusz; każdy kolejny warunek jako następny arkusz
# można jednocześnie pracować ręcznie na pewnych arkuszach i dodawać kolejne
# ręczną pracę zaczynamy, gdy nie da się nic polepszyć automatycznie
# wtedy ręczne zmiany i ich nie śledzimy - i one prowadzą do przygotowania gotowych plików do dalszego przetwarzania






    
# Czech database    
cz_names = pd.read_excel('cz_authorities.xlsx')
cz_names = marc_parser_1_field(cz_names, '001', '100', '\$\$')
cz_names = cz_names[(cz_names['$$7'] != '') |
                    (cz_names['$$d'] != '')]
cz_names['index'] = cz_names.index + 1
cz_names = pd.merge(cz_names, df_names[['index', 'viaf_id']], on='index', how='left')
cz_viaf = cz_names[['viaf_id']]
# test filtering
# cz_names = cz_names[cz_names['viaf_id'].isin(test_names)]
cz_names_table = cz_names[['index', '$$a', 'viaf_id']]
cz_names_table.columns = ['index', 'name', 'viaf']
cz_names = cz_names['100'].apply(lambda x: x.replace('|', '')).apply(lambda x: x.replace('$$', '$')).values.tolist()
cz_names = [f'{e}$4aut' for e in cz_names]
cz_names = [re.escape(m) for m in cz_names]
cz_names = '|'.join(cz_names)

# Czech database harvesting
path = 'F:/Cezary/Documents/IBL/Translations/Czech database'
files = [f for f in glob.glob(path + '*.mrk', recursive=True)]

encoding = 'utf-8'
marc_df = pd.DataFrame()
for i, file_path in enumerate(files):
    print(str(i) + '/' + str(len(files)))
    marc_list = io.open(file_path, 'rt', encoding = encoding).read().splitlines()
    marc_list = list(filter(None, marc_list))  
    df = pd.DataFrame(marc_list, columns = ['test'])
    df['field'] = df['test'].replace(r'(^.)(...)(.+?$)', r'\2', regex = True)
    df['content'] = df['test'].replace(r'(^.)(.....)(.+?$)', r'\3', regex = True)
    df['help'] = df.apply(lambda x: f(x, 'LDR'), axis=1)
    df['help'] = df['help'].ffill()
    df_help = df.copy()[df['field'].isin(['100', '700'])]
    df_help = df_help[df_help['content'].str.contains(cz_names)]['help'].apply(lambda x: int(x)).values.tolist()
    df = df[df['help'].isin(df_help)]
    if len(df) > 0:
        df['id'] = df.apply(lambda x: f(x, '001'), axis = 1)
        df['id'] = df.groupby('help')['id'].ffill().bfill()
        df = df[['id', 'field', 'content']]
        df['content'] = df.groupby(['id', 'field'])['content'].transform(lambda x: '❦'.join(x.drop_duplicates().astype(str)))
        df = df.drop_duplicates().reset_index(drop=True)
        df_wide = df.pivot(index = 'id', columns = 'field', values = 'content')
        df_wide = df_wide[df_wide['LDR'].str.contains('^.{6}am', regex=True)]
        marc_df = marc_df.append(df_wide)

fields = marc_df.columns.tolist()
fields = [i for i in fields if 'LDR' in i or re.compile('\d{3}').findall(i)]
marc_df = marc_df.loc[:, marc_df.columns.isin(fields)]

cz_names = cz_names_table.copy()

X100 = marc_df[['001', '100']]
query = "select * from X100 a join cz_names b on a.'100' like '%'||b.name||'%'"
X100 = pandasql.sqldf(query)
X100 = X100[['001', 'index', 'name', 'viaf']]

X700 = marc_df[['001', '700']]
query = "select * from X700 a join cz_names b on a.'700' like '%'||b.name||'%'"
X700 = pandasql.sqldf(query)
X700 = X700[['001', 'index', 'name', 'viaf']]

X100700 = pd.concat([X100, X700]).drop_duplicates()
marc_df = pd.merge(marc_df, X100700, on='001', how='left')

fields_order = marc_df.columns.tolist()

fields_order.sort(key = lambda x: ([str,int].index(type(1 if re.findall(r'\w+', x)[0].isdigit() else 'a')), x))
marc_df = marc_df.reindex(columns=fields_order)
marc_df.to_excel('cz_data.xlsx', index=False)

# Cleaning data
swe_data = pd.read_excel('swe_data_no_dates.xlsx')
pol_data = pd.read_excel('pol_data_no_dates.xlsx')

# | zamienić po kolejnym harvestowaniu na ❦
def right_author(x):
    u_name = unidecode.unidecode(x['name']).lower()
    if pd.notnull(x['100']) and pd.isnull(x['700']):
        try:
            test_100 = re.findall(u_name, unidecode.unidecode(x['100'].lower()))[0]
            if '$' not in test_100 and test_100.count(',') <= 1:
                val = True
            else:
                val = False
        except IndexError:
            val = False
    elif pd.isnull(x['100']) and pd.notnull(x['700']):
        if '❦' not in x['700'] and '$4' in x['700']:
            pattern = f"(?>{u_name}\$4)(.{{3}})(?=\$|$)"
            try:
                test_700 = regex.findall(pattern, unidecode.unidecode(x['700'].lower()))[0]
                if test_700 in ['aut', 'oth']:
                    val = True
                else:
                    val = False
            except IndexError:
                val = False
        elif '❦' not in x['700'] and '$4' not in x['700']:
            try:
                re.findall(u_name, unidecode.unidecode(x['700'].lower()))[0]
                if '$e' not in x['700']:
                    val = True
                else:
                    val = False
            except IndexError:
                val = False
        elif '❦' in x['700']:
            x700 = x['700'].split('❦')
            result = []
            for elem in x700:
                if '$4' in elem:
                    u_name = unidecode.unidecode(swe_data.iloc[50,3].lower())
                    elem = swe_data.iloc[50, swe_data.columns.get_loc('700')].split('❦')[0]
                    
                    pattern = f"(?>{u_name}\$4)(.{{3}})(?=\$|$)"
                    try:
                        test_700 = regex.findall(pattern, unidecode.unidecode(elem.lower()))[0]
                        if test_700 in ['aut', 'oth']:
                            val = True
                        else:
                            val = False
                    except IndexError:
                        val = False
                    result.append(val)
                else:
                    try:
                        re.findall(u_name, unidecode.unidecode(elem.lower()))[0]
                        if '$e' not in elem:
                            val = True
                        else:
                            val = False
                    except IndexError:
                        val = False
                    result.append(val)
            if True in result:
                val = True
            else:
                val = False
    elif pd.notnull(x['100']) and pd.notnull(x['700']):
        try:
            test_100 = re.findall(u_name, unidecode.unidecode(x['100'].lower()))[0]
            if '$' not in test_100 and test_100.count(',') <= 1:
                val1 = True
            else:
                val1 = False
        except IndexError:
            val1 = False
        if '❦' not in x['700'] and '$4' in x['700']:
            pattern = f"(?>{u_name}\$4)(.{{3}})(?=\$|$)"
            try:
                test_700 = regex.findall(pattern, unidecode.unidecode(x['700'].lower()))[0]
                if test_700 in ['aut', 'oth']:
                    val2 = True
                else:
                    val2 = False
            except IndexError:
                val2 = False
        elif '❦' not in x['700'] and '$4' not in x['700']:
            try:
                re.findall(u_name, unidecode.unidecode(x['700'].lower()))[0]
                if '$e' not in x['700']:
                    val2 = True
                else:
                    val2 = False
            except IndexError:
                val2 = False
        elif '❦' in x['700']:
            x700 = x['700'].split('❦')
            result = []
            for elem in x700:
                if '$4' in elem:
                    pattern = f"(?>{u_name}\$4)(.{{3}})(?=\$|$)"
                    try:
                        test_700 = regex.findall(pattern, unidecode.unidecode(elem.lower()))[0]
                        if test_700 in ['aut', 'oth']:
                            val2 = True
                        else:
                            val2 = False
                    except IndexError:
                        val2 = False
                    result.append(val2)
                else:
                    try:
                        re.findall(u_name, unidecode.unidecode(elem.lower()))[0]
                        if '$e' not in elem:
                            val2 = True
                        else:
                            val2 = False
                    except IndexError:
                        val2 = False
                    result.append(val2)
            if True in result:
                val2 = True
            else:
                val2 = False
        if val1 == True or val2 == True:
            val = True
        else:
            val = False
    else:
        val = False
    return val

swe_data['right_author'] = swe_data.apply(lambda x: right_author(x), axis=1)
# swe_data = swe_data[swe_data['right_author'] == True]
# del swe_data['right_author']
swe_data.to_excel('swe_data_clean.xlsx', index=False)

pol_data['right_author'] = pol_data.apply(lambda x: right_author(x), axis=1)
# pol_data = pol_data[pol_data['right_author'] == True]
# del pol_data['right_author']
pol_data.to_excel('pol_data_clean.xlsx', index=False)




# Research section
# Connect appearances for several authors from all datasets


pol = pd.read_excel('pol_data_clean.xlsx')
pol['source'] = 'pol'
swe = pd.read_excel('swe_data_clean.xlsx')
swe['source'] = 'swe'
cze = pd.read_excel('cz_data.xlsx')
cze['source'] = 'cze'
cze['id'] = cze.index+1
fin = pd.read_excel('fi_data.xlsx')
fin['source'] = 'fin'

total = pd.concat([cze, pol, swe, fin]).sort_values('viaf')

total['005'] = total['005'].astype(str)
total['008'] = total['008'].str.replace('\\', ' ')

fields = total.columns.tolist()
fields.sort(key = lambda x: ([str,int].index(type("a" if re.findall(r'\w+', x)[0].isalpha() else 1)), x))
total = total.reindex(columns=fields)     
total.to_excel('translations_total.xlsx', index=False)   
        
fields = [i for i in fields if 'LDR' in i or re.compile('\d{3}').findall(i)]
total = total.loc[:, total.columns.isin(fields)]

df_to_mrc(total, '❦', 'translations_total.mrc')
mrc_to_mrk('translations_total.mrc', 'translations_total.mrk')

#no czech originals
no_cz_orig = total.copy()
no_cz_orig['lang'] = no_cz_orig['008'].apply(lambda x: x[35:38])
no_cz_orig = no_cz_orig[no_cz_orig['lang'] != 'cze'].drop(columns='lang')

no_cz_orig.to_excel('no_cz_original.xlsx', index=False) 
df_to_mrc(no_cz_orig, '❦', 'no_cz_original.mrc')
mrc_to_mrk('no_cz_original.mrc', 'no_cz_original.mrk')

# translations search engine
cz_foundation = cze.copy()[['id', 'name', 'viaf', '008', '245']]
cz_foundation['language'] = cz_foundation['008'].apply(lambda x: x[-5:-2])
cz_foundation = cz_foundation[cz_foundation['language'] == 'cze']
cz245 = marc_parser_1_field(cz_foundation, 'id', '245', '\\$')[['id', '$a']]
cz_foundation = pd.merge(cz_foundation, cz245, how='inner', on='id')[['name', 'viaf', '$a']].drop_duplicates()
cz_foundation['$a'] = cz_foundation['$a'].apply(lambda x: x[:-2])
cz_foundation = cz_foundation.drop_duplicates()
cz_foundation['simple'] = cz_foundation['$a'].apply(lambda x: unidecode.unidecode(x))

total = pd.concat([pol, swe, fin]).sort_values('viaf')

test = total.copy()[['index', 'name', 'viaf', '008', '041', '100', '245', '240', '246', '250', '260', '300', '080', 'source']].reset_index(drop=True)
# test = test[test['viaf'] == 34458072].reset_index(drop=True)

# dodać jeszcze warunek, że viaf musi być taki sam dla cz_foundation i w danych

def search_for_simple(x):
    print(str(x.name) + '/' + str(len(test)))
    result = []
    for i, row in cz_foundation.iterrows():
        if pd.notnull(x['245']) and row['simple'] in unidecode.unidecode(x['245']) and row['viaf'] == x['viaf']:
            val = f"{row['viaf']}❦{row['name']}❦{row['$a']}❦field: 245"
        elif pd.notnull(x['240']) and row['simple'] in unidecode.unidecode(x['240']) and row['viaf'] == x['viaf']:
            val = f"{row['viaf']}❦{row['name']}❦{row['$a']}❦field: 240"
        elif pd.notnull(x['246']) and row['simple'] in unidecode.unidecode(x['246']) and row['viaf'] == x['viaf']:
            val = f"{row['viaf']}❦{row['name']}❦{row['$a']}❦field: 246"
        else:
            val = None
        if val != None:
            result.append(val)
    return result

test['exact match'] = test.apply(lambda x: search_for_simple(x), axis=1)

# dodać close match na podstawie cosine similarity

test.to_excel('translation_trajectories_test.xlsx', index=False)

def get_generator(x):
    l = []
    for elem in x:
        l.append(elem)
        # l = [e for e in l if e]
    return l

test['match'] = test['match'].apply(lambda x: get_generator(x))

for i, row in test.iterrows():
    print(row.index.item())

len(test.at[1, 'match'])
for el in test.at[1, 'match']:
    if el != None:
        print(el)
        
pd.notnull(test.at[349, '245']) and cz_foundation.at[10199, 'simple'] in unidecode.unidecode(test.at[349, '245'])
# dlaczego to nie działa w yield?

'Atomova masina znacky Perkeo' in unidecode.unidecode('00$aAtomová mašina značky Perkeo :$btexty z let 1949-1989')


for title in cz_foundation['simple']:
    print(title)
test.at[1, 'match2']

for i, row in test.iterrows():
    print((i, row['match2']))

list(test.at[1, 'match2'])


for row in test.iterrows():
    print(row)

for elem in test.at[1, 'match']:
    print(list(elem))
    



test = test[test['viaf'] == 4931097].reset_index(drop=True)
test['id'] = test.index+1
# test.to_excel('translations_test.xlsx', index=False)




x245 = marc_parser_1_field(test, 'id', '245', '\\$').replace(r'^\s*$', np.NaN, regex=True)
x245['title'] = x245[['$a', '$b', '$n', '$p']].apply(lambda x: ' '.join(x.dropna().astype(str)), axis=1)
x245 = x245[['id', 'title']]

x240 = marc_parser_1_field(test, 'id', '240', '\\$').replace(r'^\s*$', np.NaN, regex=True)
x240['original title'] = x240['$a']
x240 = x240[['id', 'original title']]

together = pd.merge(x245, x240, how='outer', on='id')

physical_description_notes = pd.merge(physical_description_notes1, physical_description_notes2,  how='outer', left_on = 'id', right_on = 'id')         


pol_test = pol[pol['viaf'] == 4931097]
swe_test = swe[swe['viaf'] == 4931097]




    
    





# DIY xml parser - abandoned
# =============================================================================
# data_full = []
# errors = []
# for name_index, (name, viaf) in enumerate(swe_names):
#     try:
#         print(str(name_index) + '/' + str(len(swe_names)-1))
#         url = f'http://libris.kb.se/xsearch?query=forf:({name.replace(" ", "%20")})&start=1&n=200&format=marcxml&format_level=full'
#         response = requests.get(url)
#         with open('test.xml', 'wb') as file:
#             file.write(response.content)
#         tree = et.parse('test.xml')
#         root = tree.getroot()
#         try:
#             number_of_records = int(root.attrib['records'])
#             if 0 < number_of_records <= 200:
#                 for record_index, record in enumerate(root.findall('.//{http://www.loc.gov/MARC21/slim}record')):
#                     for field in record:
#                         field_type = re.sub(r'\{.*?\}', '', field.tag)
#                         if len(list(field.attrib.items())) > 0:
#                             field_name = field.attrib['tag']
#                         else:
#                             field_name = '000'
#                         if field.text is None:
#                             field_content = '|'.join([f'${list(child.attrib.items())[-1][-1]}{child.text}' for child in field.getchildren()])
#                         else:
#                             field_content = field.text
#                         data_full.append([name_index, 0, record_index, field_type, field_name, field_content, name, viaf])
#             elif number_of_records > 200:
#                 x = range(1, number_of_records + 1, 200)
#                 for page_index, i in enumerate(x):
#                     if i == 1:
#                         for record_index, record in enumerate(root.findall('.//{http://www.loc.gov/MARC21/slim}record')):
#                             for field in record:
#                                 field_type = re.sub(r'\{.*?\}', '', field.tag)
#                                 if len(list(field.attrib.items())) > 0:
#                                     field_name = field.attrib['tag']
#                                 else:
#                                     field_name = '000'
#                                 if field.text is None:
#                                     field_content = '|'.join([f'${list(child.attrib.items())[-1][-1]}{child.text}' for child in field.getchildren()])
#                                 else:
#                                     field_content = field.text
#                                 data_full.append([name_index, page_index, record_index, field_type, field_name, field_content, name, viaf])
#                     else:
#                         link = f'http://libris.kb.se/xsearch?query=forf:({name.replace(" ", "%20")})&start={i}&n=200&format=marcxml&format_level=full'
#                         response = requests.get(link)
#                         with open('test.xml', 'wb') as file:
#                             file.write(response.content)
#                         ntree = et.parse('test.xml')
#                         nroot = ntree.getroot()
#                         for record_index, record in enumerate(nroot.findall('.//{http://www.loc.gov/MARC21/slim}record')):
#                             for field in record:
#                                 field_type = re.sub(r'\{.*?\}', '', field.tag)
#                                 if len(list(field.attrib.items())) > 0:
#                                     field_name = field.attrib['tag']
#                                 else:
#                                     field_name = '000'
#                                 if field.text is None:
#                                     field_content = '|'.join([f'${list(child.attrib.items())[-1][-1]}{child.text}' for child in field.getchildren()])
#                                 else:
#                                     field_content = field.text
#                                 data_full.append([name_index, page_index, record_index, field_type, field_name, field_content, name, viaf])
#             else:
#                 data_full.append([name_index, np.nan, np.nan, np.nan, np.nan, np.nan, name, viaf])
#         except KeyError:
#             errors.append([name_index, name, viaf]) 
#     except et.ParseError:
#         errors.append([name_index, name, viaf]) 
#         
# swe_errors = pd.DataFrame(errors, columns = ['index', 'name', 'viaf_id'])
# swe_errors.to_excel('swe_errors1.xlsx', index = False)
# 
# swedish_df = pd.DataFrame(data_full, columns =['name_index', 'page_index', 'record', 'field_type', 'field_name', 'field_content', 'person_name', 'viaf_id']).sort_values(
#     ['name_index', 'page_index', 'record', 'field_name', 'field_content'])
# groups = swedish_df['name_index'].drop_duplicates().values.tolist()
# 
# new_data_frame = pd.DataFrame(columns = swedish_df.columns)
# for index, group in enumerate(groups):
#     print(str(index) + '/' + str(len(groups)))
#     test_df = swedish_df.loc[swedish_df['name_index'] == group]
#     if len(test_df) > 1:    
#         test_df['field_content'] = test_df.groupby(['name_index', 'page_index', 'record', 'field_name'])['field_content'].transform(
#             lambda x: '~'.join(x.drop_duplicates().astype(str)))
#         test_df = test_df.drop_duplicates()    
#         test_df['id'] = test_df.apply(lambda x: f(x, '001'), axis = 1)
#         test_df['id'] = test_df.groupby(['name_index', 'page_index', 'record'])['id'].apply(lambda x: x.ffill().bfill())
#         test_df['field_name'].loc[test_df['field_name'] == '000'] = 'LDR'
#         new_data_frame = pd.concat([new_data_frame, test_df])
#     else:
#         new_data_frame = pd.concat([new_data_frame, test_df])
#         
# 
# #after the loop
# swedish_data = new_data_frame[['id', 'field_name', 'field_content']].drop_duplicates()
# swedish_people = new_data_frame[['id', 'person_name', 'viaf_id']].drop_duplicates()
# swedish_df_wide = swedish_data.pivot(index='id', columns='field_name', values='field_content')
# swedish_df_wide = pd.merge(swedish_df_wide, swedish_people,  how='left', left_on = 'id', right_on = 'id')
# 
# swedish_df_wide.to_excel('swe_data.xlsx', index = False)
# =============================================================================
