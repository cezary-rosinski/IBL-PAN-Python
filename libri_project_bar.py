# błędy:
#     - w 773 usunąć $i// - =773  0\$i//$tKalina (Kraków). -$g1868, nr 6$91868
import pandas as pd
import io
import re
import numpy as np
from my_functions import cSplit, gsheet_to_df, df_to_mrc, mrc_to_mrk
import regex as re
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import NoSuchElementException
import datetime
import ast

now = datetime.datetime.now()
year = now.year
month = now.month
day = now.day

# def
def f(row, id_field):
    if row['field'] == id_field and id_field == 'LDR':
        val = row.name
    elif row['field'] == id_field:
        val = row['content'].strip()
    else:
        val = np.nan
    return val

def replacer(s, newstring, index, nofail=False):
    # raise an error if index is outside of the string
    if not nofail and index not in range(len(s)):
        raise ValueError("index outside given string")

    # if not erroring, but the index is still not in the correct range..
    if index < 0:  # add it to the beginning
        return newstring + s
    if index > len(s):  # add it to the end
        return s + newstring

    # insert the new string between "slices" of the original
    return s[:index] + newstring + s[index + 1:]

def replace_with_backslash(x):
    if pd.isnull(x):
        x = np.nan
    elif x[0] == ' ' and x[1] == ' ':
        x = replacer(x, '\\', 0)
        x = replacer(x, '\\', 1)
    elif x[0] == ' ':
        x = replacer(x, '\\', 0)    
    elif x[1] == ' ':
        x = replacer(x, '\\', 1)
    return x

# main

# =============================================================================
# path_in = "C:/Users/Cezary/Downloads/bar-zrzut-20200310.TXT"
# encoding = 'cp852'
# reader = io.open(path_in, 'rt', encoding = encoding).read().splitlines()
# 
# mrk_list = []
# for row in reader:
#     if row[:5] == '     ':
#         mrk_list[-1] += row[5:]
#     else:
#         mrk_list.append(row)
#                
# bar_list = []
# for i, row in enumerate(mrk_list):
#     if 'LDR' in row:
#         new_field = '001  bar' + '{:09d}'.format(i+1)
#         bar_list.append(row)
#         bar_list.append(new_field)
#     else:
#         bar_list.append(row)
#         
# df = pd.DataFrame(bar_list, columns = ['test'])
# df = df[df['test'] != '']
# df['field'] = df['test'].replace(r'(^...)(.+?$)', r'\1', regex = True)
# df['content'] = df['test'].replace(r'(^...)(.+?$)', r'\2', regex = True)
# df['help'] = df.apply(lambda x: f(x, 'LDR'), axis=1)
# df['help'] = df['help'].ffill()
# df['id'] = df.apply(lambda x: f(x, '001'), axis = 1)
# df['id'] = df.groupby('help')['id'].ffill().bfill()
# df = df[['id', 'field', 'content']]
# df['content'] = df.groupby(['id', 'field'])['content'].transform(lambda x: '❦'.join(x.drop_duplicates().astype(str)))
# df = df.drop_duplicates().reset_index(drop=True)
# bar_catalog = df.pivot(index = 'id', columns = 'field', values = 'content')
# =============================================================================

# =============================================================================
# bar_catalog.to_excel('bar_catalog_1st_stage.xlsx', index=False)
# bar_catalog = pd.read_excel('F:/Cezary/Documents/IBL/Pliki python/bar_catalog_1st_stage.xlsx')
# =============================================================================


# =============================================================================
# problemy = []
# for i, row in bar_catalog.iterrows():
#     for ind, elem in row.items():
#         if pd.notnull(elem) and '~' in elem:
#             try:
#                 val = re.findall("\~", elem)[0]
#                 problemy.append([val, ind, elem])
#             except IndexError:
#                 pass
# 
# problemy_unique = list(set([e[0] for e in problemy]))
# test = [e, f, g for e, f, g in problemy if e == '~R']
#             
# tyldy = []
# przyklady = []
# for elem in problemy:
#     if elem[0] not in tyldy and elem[1]:
#         tyldy.append(elem[0])
#         przyklady.append(elem)
# 
# df = pd.DataFrame(problemy, columns=['error', 'marc_field', 'example'])
# df['index'] = df.index+1
# df = cSplit(df, 'index', 'example', '❦')    
# df['has_tilde'] = df.apply(lambda x: True if x['error'] in x['example'] else False, axis=1)        
# df = df[df['has_tilde']==True].reset_index(drop=True)
# df['search'] = df['example'].str.replace('^..\%.', '', regex=True).str.replace('\%.', '', regex=True)
# df['search'] = df.apply(lambda x: re.sub('(.+)( \/ )(.+)', r'\1', x['search']) if x['marc_field'] == '245' else x['search'], axis=1)
# df['search'] = df['search'].apply(lambda x: regex.sub('(^[^\pL0-9]+)([\pL0-9]+)', r'\2', x, 1))
# 
# 
# browser = webdriver.Chrome()
# 
# for i, row in df.iterrows():
#     print(str(i) + '/' + str(len(df)))
#     if row['marc_field'] == '100':
#         try:
#             browser.get("http://bar.ibl.waw.pl/cgi-bin//makwww.exe?BM=01&IZ=Autor")
#             box = browser.find_element_by_css_selector('.text1')
#             box.send_keys(row['search'], Keys.ENTER)
#             first_hit = browser.find_element_by_css_selector('hr+ .submit4')
#             first_hit.click()
#             first_record = browser.find_element_by_css_selector('hr+ .submit5')
#             first_record.click()
#             response = requests.get(browser.current_url + '&MC=MARC')
#             soup = BeautifulSoup(response.text, 'html.parser')
#             content = re.findall('100.+?(?= 245)', soup.text)[0]
#             df.at[i, 'proper form'] = content
#         except:
#             df.at[i, 'proper form'] = 'brak danych'      
#     elif row['marc_field'] == '245':
#         try:
#             browser.get("http://bar.ibl.waw.pl/cgi-bin//makwww.exe?BM=01&IZ=Tytu%B3")
#             box = browser.find_element_by_css_selector('.text1')
#             box.send_keys(row['search'], Keys.ENTER)
#             first_hit = browser.find_element_by_css_selector('hr+ .submit4')
#             first_hit.click()
#             first_record = browser.find_element_by_css_selector('hr+ .submit5')
#             first_record.click()
#             response = requests.get(browser.current_url + '&MC=MARC')
#             soup = BeautifulSoup(response.text, 'html.parser')
#             content = re.findall('245.+?(?= 246|490|600|655)', soup.text)[0]
#             df.at[i, 'proper form'] = content
#         except:
#             df.at[i, 'proper form'] = 'brak danych'   
#     elif row['marc_field'] == '600':
#         try:
#             browser.get("http://bar.ibl.waw.pl/cgi-bin//makwww.exe?BM=01&IZ=Temat")
#             box = browser.find_element_by_css_selector('.text1')
#             box.send_keys(row['search'], Keys.ENTER)
#             first_hit = browser.find_element_by_css_selector('hr+ .submit4')
#             first_hit.click()
#             first_record = browser.find_element_by_css_selector('hr+ .submit5')
#             first_record.click()
#             response = requests.get(browser.current_url + '&MC=MARC')
#             soup = BeautifulSoup(response.text, 'html.parser')
#             content = re.findall('600.+?(?= 773)', soup.text)[0]
#             df.at[i, 'proper form'] = content
#         except:
#             df.at[i, 'proper form'] = 'brak danych'   
#     elif row['marc_field'] == '773':
#         try:
#             browser.get("http://bar.ibl.waw.pl/cgi-bin//makwww.exe?BM=01&IZ=Czasopismo")
#             box = browser.find_element_by_css_selector('.text1')
#             box.send_keys(row['search'], Keys.ENTER)
#             first_hit = browser.find_element_by_css_selector('hr+ .submit4')
#             first_hit.click()
#             first_record = browser.find_element_by_css_selector('hr+ .submit5')
#             first_record.click()
#             response = requests.get(browser.current_url + '&MC=MARC')
#             soup = BeautifulSoup(response.text, 'html.parser')
#             content = re.findall('773.+', soup.text)[0]
#             df.at[i, 'proper form'] = content
#         except:
#             df.at[i, 'proper form'] = 'brak danych'  
#             
# df.to_excel('bar_kodowanie.xlsx', index=False)          
# =============================================================================

bar_catalog = pd.read_excel('F:/Cezary/Documents/IBL/Pliki python/bar_catalog_1st_stage.xlsx')
bar_encoding = gsheet_to_df('1idplMbMhqIG-PhQSIJINcFJxKWMqR38MzlXowSNSh80', 'Sheet1')[['error', 'encoding']].drop_duplicates()
bar_encoding['length'] = bar_encoding['error'].apply(lambda x: len(x))
bar_encoding = bar_encoding.sort_values(['length', 'error'], ascending=(False, False))[['error', 'encoding']].values.tolist()
bar_encoding = [(re.escape(m), n if n is not None else '') for m, n in bar_encoding]
bar_encoding2 = gsheet_to_df('1idplMbMhqIG-PhQSIJINcFJxKWMqR38MzlXowSNSh80', 'Arkusz1')[['error', 'encoding']].drop_duplicates()
bar_encoding2['length'] = bar_encoding2['error'].apply(lambda x: len(x))
bar_encoding2 = bar_encoding2.sort_values(['length', 'error'], ascending=(False, False))[['error', 'encoding']].values.tolist()
bar_encoding2 = [(re.escape(m), n if n is not None else '') for m, n in bar_encoding2]

bar_catalog['001'] = bar_catalog['001'].str.strip()
bar_catalog['008'] = bar_catalog['008'].str.strip().str.replace('(^|.)(\%.)', '', regex=True).str.replace('([\+\!])', '-', regex=True)
bar_catalog['do 100'] = bar_catalog['100'].str.replace(r'(^.+?)(❦)(.+?$)', r'\3', regex=True)
bar_catalog['100'] = bar_catalog['100'].str.replace(r'(^.+?)(❦)(.+?$)', r'\1', regex=True)
bar_catalog['700'] = bar_catalog[['do 100', '700']].apply(lambda x: ''.join(x.dropna().astype(str)), axis=1)
del bar_catalog['do 100']
bar_catalog['100'] = bar_catalog['100'].str.replace(r'(?<=\%d)(\()(.+?)(\)\.{0,1})', r'\2', regex=True).str.replace('(?<=[a-zà-ž][a-zà-ž])\.$', '', regex=True)

bar_catalog = cSplit(bar_catalog, '001', '600', '❦')
bar_catalog['600'] = bar_catalog['600'].str.replace(r'(?<=\%d)(\()(.+?)(\)\.{0,1})', r'\2', regex=True).str.replace('(?<=[a-zà-ž][a-zà-ž])\.$', '', regex=True)
bar_catalog['787'] = bar_catalog['600'].str.replace('(\%d.+?)(?=\%)', '', regex=True).str.replace(r'(?<=^)(..)', r'08', regex=True)
bar_catalog['787'] = bar_catalog['787'].apply(lambda x: x if pd.notnull(x) and '%t' in x else np.nan)
bar_catalog['600'] = bar_catalog['600'].str.replace('(\%t.+?)(?=\%|$)', '', regex=True).str.strip()
bar_catalog['600'] = bar_catalog.groupby('001')['600'].transform(lambda x: '❦'.join(x.dropna()))
bar_catalog['787'] = bar_catalog.groupby('001')['787'].transform(lambda x: '❦'.join(x.dropna()))
bar_catalog = bar_catalog.drop_duplicates()
bar_catalog['600'] = bar_catalog['600'].str.split('❦').apply(set).str.join('❦')

bar_catalog = cSplit(bar_catalog, '001', '700', '❦')
bar_catalog['700'] = bar_catalog['700'].str.replace(r'(?<=\%d)(\()(.+?)(\)\.{0,1})', r'\2', regex=True).str.replace('(?<=[a-zà-ž][a-zà-ž])\.$', '', regex=True)
bar_catalog['700'] = bar_catalog.groupby('001')['700'].transform(lambda x: '❦'.join(x.dropna()))
bar_catalog = bar_catalog.drop_duplicates()
bar_catalog['700'] = bar_catalog['700'].str.split('❦').apply(set).str.join('❦')
bar_catalog['LDR'] = '-----nab-a22-----4u-4500'

bar_catalog = cSplit(bar_catalog, '001', '773', '❦')
bar_catalog['do 773'] = bar_catalog['773'].str.extract(r'(?<=\%g)(.{4})')
bar_catalog['do 773'] = bar_catalog['do 773'].apply(lambda x: '$9' + x if pd.notnull(x) else np.nan)
bar_catalog['773'] = bar_catalog['773'].str.replace(r'(?<=^)(..)', r'0\\', regex=True)
bar_catalog['773'] = bar_catalog[['773', 'do 773']].apply(lambda x: ''.join(x.dropna().astype(str)), axis=1)
del bar_catalog['do 773']
bar_catalog['773'] = bar_catalog.groupby('001')['773'].transform(lambda x: '❦'.join(x.dropna()))
bar_catalog = bar_catalog.drop_duplicates()
bar_catalog['995'] = '\\\\$aBibliografia Bara'

bar_catalog = bar_catalog.replace(r'^\s*$', np.NaN, regex=True)
for column in bar_catalog:       
    bar_catalog[column] = bar_catalog[column].apply(lambda x: replace_with_backslash(x))
bar_catalog = bar_catalog.replace(r'(?<=❦)(  )(?=\%)', r'\\\\', regex=True).replace(r'(?<=❦)( )([^ ])(?=\%)', r'\\\2', regex=True).replace(r'(?<=❦)([^ ])( )(?=\%)', r'\1\\', regex=True)
bar_catalog = bar_catalog.replace(r'(\s{0,1}\%)(.)', r'$\2', regex=True)
fields_order = bar_catalog.columns.tolist()
fields_order.sort(key = lambda x: ([str,int].index(type("a" if re.findall(r'\w+', x)[0].isalpha() else 1)), x))
bar_catalog = bar_catalog.reindex(columns=fields_order)

for i, column in enumerate(bar_catalog):
    print(str(i) + '/' + str(len(bar_catalog.columns)))
    for ind, elem in enumerate(bar_encoding):
        print('    ' + str(ind) + '/' + str(len(bar_encoding)))
        bar_catalog[column] = bar_catalog[column].str.replace(elem[0], elem[1], regex=True)
        
for i, column in enumerate(bar_catalog):
    print(str(i) + '/' + str(len(bar_catalog.columns)))
    for ind, elem in enumerate(bar_encoding2):
        print('    ' + str(ind) + '/' + str(len(bar_encoding2)))
        bar_catalog[column] = bar_catalog[column].str.replace(elem[0], elem[1], regex=True)
        
bar_catalog.to_excel('bar_catalog.xlsx', index=False)

df_to_mrc(bar_catalog, '❦', f'bar_catalog{year}-{month}-{day}.mrc', f'bar_catalog{year}-{month}-{day}.txt')
mrc_to_mrk(f'bar_catalog{year}-{month}-{day}.mrc', f'bar_catalog{year}-{month}-{day}.mrk')

#errors
errors_txt = io.open(f'bar_catalog{year}-{month}-{day}.txt', 'rt', encoding='UTF-8').read().splitlines()
errors_txt = [e for e in errors_txt if e]

new_list = []

for sublist in errors_txt:
    sublist = ast.literal_eval(sublist)
    di = {x:y for x,y in sublist}
    new_list.append(di)
    
for dictionary in new_list:
    for key in dictionary.keys():
        dictionary[key] = '❦'.join([e for e in dictionary[key] if e])

df = pd.DataFrame(new_list)
df['LDR'] = '-----nab---------4u-----'
#investigate the file thoroughly if more errors

df_to_mrc(df, '❦', f'bar_catalog_vol_2{year}-{month}-{day}.mrc', f'bar_catalog_vol_2{year}-{month}-{day}.txt')
mrc_to_mrk(f'bar_catalog_vol_2{year}-{month}-{day}.mrc', f'bar_catalog_vol_2{year}-{month}-{day}.mrk')












