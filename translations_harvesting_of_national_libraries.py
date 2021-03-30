from oaipmh.client import Client
from oaipmh.metadata import MetadataRegistry
from pymarc import marcxml, MARCReader
import io
from lxml.etree import tostring
from datetime import datetime, timedelta
import sys
import os
import glob
import regex as re
import pandas as pd
from my_functions import mrc_to_mrk
from tqdm import tqdm
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
import gspread as gs
from gspread_dataframe import set_with_dataframe, get_as_dataframe
from google_drive_research_folders import cr_projects
import datetime

#%% date

now = datetime.datetime.now()
year = now.year
month = '{:02d}'.format(now.month)
day = '{:02d}'.format(now.day)

#%% google authentication

gc = gs.oauth()
gauth = GoogleAuth()
gauth.LocalWebserverAuth()
drive = GoogleDrive(gauth)

file_list = drive.ListFile({'q': f"'{cr_projects}' in parents and trashed=false"}).GetList() 
#[print(e['title'], e['id']) for e in file_list]
translation_folder = [file['id'] for file in file_list if file['title'] == 'Vimr Project'][0]

#%% Polish National Library - All translations into Polish

path = 'F:/Cezary/Documents/IBL/Migracja z BN/bn_all/2021-02-08/'
files = [f for f in glob.glob(path + '*.mrk', recursive=True)]

fiction_types = ['1', 'd', 'f', 'h', 'j', 'p', 'u', '|', '\\']
years = range(1990,2021)
encoding = 'utf-8' 
new_list = []

for file in tqdm(files):
    
    marc_list = io.open(file, 'rt', encoding = encoding).read().splitlines()
    
    mrk_list = []
    for row in marc_list:
        if row.startswith('=LDR'):
            mrk_list.append([row])
        else:
            if row:
                mrk_list[-1].append(row)
            
    for sublist in mrk_list:
        language = ''.join([ele for ele in sublist if ele.startswith('=008')])[41:44]
        type_of_record_bibliographical_level = ''.join([ele for ele in sublist if ele.startswith('=LDR')])[12:14]
        if [ele for ele in sublist if ele.startswith('=041')]: 
            is_translation = True
        else:
            is_translation = False
        try:
            fiction_type = ''.join([ele for ele in sublist if ele.startswith('=008')])[39]
        except IndexError:
            fiction_type = 'a'
        try:
            bib_year = int(''.join([ele for ele in sublist if ele.startswith('=008')])[13:17])
        except ValueError:
            bib_year = 1000
        if language == 'pol' and type_of_record_bibliographical_level == 'am' and fiction_type in fiction_types and bib_year in years and is_translation:
            new_list.append(sublist)

final_list = []
for lista in new_list:
    slownik = {}
    for el in lista:
        if el[1:4] in slownik:
            slownik[el[1:4]] += f"❦{el[6:]}"
        else:
            slownik[el[1:4]] = el[6:]
    final_list.append(slownik)

marc_df = pd.DataFrame(final_list)
fields = marc_df.columns.tolist()
fields = [i for i in fields if 'LDR' in i or re.compile('\d{3}').findall(i)]
marc_df = marc_df.loc[:, marc_df.columns.isin(fields)]
fields.sort(key = lambda x: ([str,int].index(type("a" if re.findall(r'\w+', x)[0].isalpha() else 1)), x))
marc_df = marc_df.reindex(columns=fields) 

marc_df.to_excel(f'Translation_into_Polish_{year}-{month}-{day}.xlsx', index=False)

# =============================================================================
# harvested_data_sheet = gc.create(f'Translation_into_Polish_{year}-{month}-{day}', translation_folder)
# worksheet = harvested_data_sheet.get_worksheet(0)
# set_with_dataframe(worksheet, marc_df)
# 
# harvested_data_sheet.batch_update({
#     "requests": [
#         {
#             "updateDimensionProperties": {
#                 "range": {
#                     "sheetId": worksheet._properties['sheetId'],
#                     "dimension": "ROWS",
#                     "startIndex": 0,
#                     #"endIndex": 100
#                 },
#                 "properties": {
#                     "pixelSize": 20
#                 },
#                 "fields": "pixelSize"
#             }
#         }
#     ]
# })
# 
# worksheet.freeze(rows=1)
# worksheet.set_basic_filter()
# =============================================================================


#%% All translations into Czech

fiction_types = ['1', 'd', 'f', 'h', 'j', 'p', 'u', '|', '\\']
years = range(1990,2021)
encoding = 'UTF-8' 
new_list = []
   
marc_list = io.open('F:/Cezary/Documents/IBL/Translations/Czech database/2021-03-18/nkp_nkc_2021-03-18.mrk', 'rt', encoding = encoding).read().splitlines()

mrk_list = []
for row in marc_list:
    if row.startswith('=LDR'):
        mrk_list.append([row])
    else:
        if row:
            mrk_list[-1].append(row)
        
for sublist in tqdm(mrk_list):
    language = ''.join([ele for ele in sublist if ele.startswith('=008')])[41:44]
    type_of_record_bibliographical_level = ''.join([ele for ele in sublist if ele.startswith('=LDR')])[12:14]
    if [ele for ele in sublist if ele.startswith('=041')]: 
        is_translation = True
    else:
        is_translation = False
    try:
        fiction_type = ''.join([ele for ele in sublist if ele.startswith('=008')])[39]
    except IndexError:
        fiction_type = 'a'
    try:
        bib_year = int(''.join([ele for ele in sublist if ele.startswith('=008')])[13:17])
    except ValueError:
        bib_year = 1000
    if language == 'cze' and type_of_record_bibliographical_level == 'am' and fiction_type in fiction_types and bib_year in years and is_translation:
        new_list.append(sublist)

final_list = []
for lista in new_list:
    slownik = {}
    for el in lista:
        if el[1:4] in slownik:
            slownik[el[1:4]] += f"❦{el[6:]}"
        else:
            slownik[el[1:4]] = el[6:]
    final_list.append(slownik)

marc_df = pd.DataFrame(final_list)
fields = marc_df.columns.tolist()
fields = [i for i in fields if 'LDR' in i or re.compile('\d{3}').findall(i)]
marc_df = marc_df.loc[:, marc_df.columns.isin(fields)]
fields.sort(key = lambda x: ([str,int].index(type("a" if re.findall(r'\w+', x)[0].isalpha() else 1)), x))
marc_df = marc_df.reindex(columns=fields) 

marc_df.to_excel(f'Translation_into_Czech_{year}-{month}-{day}.xlsx', index=False)

#%% All translations into Swedish
#%% All translations into Norwegian





















 