#original code by Ondrej Vimr
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

#%%harvesting OAI-PMH

#date
now = datetime.datetime.now()
year = now.year
month = '{:02d}'.format(now.month)
day = '{:02d}'.format(now.day)

# vars

# savefile = 'F:/Cezary/Documents/IBL/Translations/nkp_nkc_{year}-{month}-{day}.marc'
# logfile = 'F:/Cezary/Documents/IBL/Translations/log_{year}-{month}-{day}.txt'
savefile = f'C:/Users/User/Desktop/nkp_nkc_{year}-{month}-{day}.marc'
logfile = f'C:/Users/User/Desktop/2021-03-18/log_{year}-{month}-{day}.txt'
URL = 'https://aleph.nkp.cz/OAI'
start_date = '2012-12-20 00:00:00'   # [YYYY-mm-dd HH:MM:SS]
stop_date = datetime.now().replace(minute=0, hour=0, second=0, microsecond=0).strftime("%Y-%m-%d %H:%M:%S")

# defs

def MarcXML(xml):
    handler = marcxml.XmlHandler()
    data = tostring(xml, encoding='UTF-8')
    marcxml.parse_xml(io.BytesIO(data), handler)
    return handler.records[0]

def valid_date(s):
    try:
        return datetime.strptime(s, '%Y-%m-%d %H:%M:%S')
    except:
        print('Invalid date format.')
        sys.exit(1)

def saverecords(recs):
    for record in recs:
        metadata = record[1]
        with open(savefile, 'ab') as out: 
            out.write(metadata.as_marc())
    with open(logfile, 'a') as log:
        log.write(f'{from_date} {until_date}\n')
    print(from_date, until_date)
    return

def get_bool(prompt):
    while True:
        try:
           return {"true":True,"false":False}[input(prompt).lower()]
        except KeyError:
           print("Invalid input please enter True or False!")

# init

registry = MetadataRegistry()
registry.registerReader('marc21', MarcXML)
client = Client(URL, registry)

start = valid_date(start_date)
stop = valid_date(stop_date)

# main

while start < stop:
    from_date = start
    start = start + timedelta(days=1)  # increase days one by one
    until_date = start
    try: 
        records = client.listRecords(metadataPrefix='marc21', set='NKC', from_=from_date, until=until_date)
        saverecords(records)
    except: pass # skipping deleted entries

print('Done.')

#%% processing mrc to df

mrc_to_mrk('F:/Cezary/Documents/IBL/Translations/Czech database/2021-03-18/nkp_nkc_2021-03-18.marc', 'F:/Cezary/Documents/IBL/Translations/Czech database/2021-03-18/nkp_nkc_2021-03-18.mrk')

fiction_types = ['1', 'd', 'f', 'h', 'j', 'p', 'u', '|', '\\']

filter_fiction_type = get_bool('Filter with a fiction type? ')

encoding = 'utf-8' 
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
    translated_from_czech = '$hcze' in ''.join([ele for ele in sublist if ele.startswith('=041')])
    if filter_fiction_type:
        try:
            fiction_type = ''.join([ele for ele in sublist if ele.startswith('=008')])[39]
        except IndexError:
            fiction_type = ''
        if language != 'cze' and type_of_record_bibliographical_level == 'am' and fiction_type in fiction_types and translated_from_czech:
            new_list.append(sublist)
    else:
        if language != 'cze' and type_of_record_bibliographical_level == 'am' and translated_from_czech:
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

#marc_df.to_excel('NKP_Czech_translations.xlsx', index=False)

#%% sending file to google drive

gc = gs.oauth()
gauth = GoogleAuth()
gauth.LocalWebserverAuth()
drive = GoogleDrive(gauth)

file_list = drive.ListFile({'q': f"'{cr_projects}' in parents and trashed=false"}).GetList() 
#[print(e['title'], e['id']) for e in file_list]
translation_folder = [file['id'] for file in file_list if file['title'] == 'Vimr Project'][0]

nkp_sheet = sheet = gc.create('NKP_Czech_translations', translation_folder)
worksheet = nkp_sheet.get_worksheet(0)
worksheet.update_title('NKP_Czech_translations')
try:
    set_with_dataframe(worksheet, marc_df)
except gs.WorksheetNotFound:
    nkp_sheet.add_worksheet(title="NKP_Czech_translations", rows="100", cols="20")
    set_with_dataframe(worksheet, marc_df)
    
# =============================================================================
# try:
#     set_with_dataframe(nkp_sheet.worksheet('NKP_no_fiction_condition'), marc_df)
# except gs.WorksheetNotFound:
#     nkp_sheet.add_worksheet(title="NKP_no_fiction_condition", rows="100", cols="20")
#     set_with_dataframe(nkp_sheet.worksheet('NKP_no_fiction_condition'), marc_df)
# =============================================================================

worksheets = ['NKP_Czech_translations', 'NKP_no_fiction_condition']

for worksheet in worksheets:
    worksheet = nkp_sheet.worksheet(worksheet)

    nkp_sheet.batch_update({
        "requests": [
            {
                "updateDimensionProperties": {
                    "range": {
                        "sheetId": worksheet._properties['sheetId'],
                        "dimension": "ROWS",
                        "startIndex": 0,
                        #"endIndex": 100
                    },
                    "properties": {
                        "pixelSize": 20
                    },
                    "fields": "pixelSize"
                }
            }
        ]
    })
    
    worksheet.freeze(rows=1)
    worksheet.set_basic_filter()

print('Done!')






























