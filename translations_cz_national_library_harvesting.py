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
now = datetime.now()
year = now.year
month = '{:02d}'.format(now.month)
day = '{:02d}'.format(now.day)

# vars
set_oai = 'SKC'
savefile = f'F:/Cezary/Documents/IBL/Translations/nkc_{set_oai}_{year}-{month}-{day}.marc'
logfile = f'F:/Cezary/Documents/IBL/Translations/nkc_{set_oai}_log_{year}-{month}-{day}.txt'
# savefile = f'C:/Users/User/Desktop/nkp_nkc_{year}-{month}-{day}.marc'
# logfile = f'C:/Users/User/Desktop/2021-03-18/log_{year}-{month}-{day}.txt'
URL = 'https://aleph.nkp.cz/OAI'
start_date = '2012-12-20 00:00:00'   # [YYYY-mm-dd HH:MM:SS]
stop_date = datetime.now().replace(minute=0, hour=0, second=0, microsecond=0).strftime("%Y-%m-%d %H:%M:%S")

#testy
start_date = '2014-12-29 00:00:00'   
stop_date = '2014-12-29 00:00:00'

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

# function for getting value of resumptionToken after parsting oai-pmh URL
def oaipmh_resumptionToken(URL):
    file = urllib2.urlopen(URL)
    data = file.read()
    file.close()
    dom = parseString(data)
    return dom.getElementsByTagName('resumptionToken')[0].firstChild.nodeValue

# init

registry = MetadataRegistry()
registry.registerReader('marc21', MarcXML)
client = Client(URL, registry)

start = valid_date(start_date)
stop = valid_date(stop_date)

# main

while start < stop:
    from_date = start
    # start = start + timedelta(days=1)  # increase days one by one
    start = start + timedelta(seconds=3600)  # increase by hour
    until_date = start
    try: 
        records = client.listRecords(metadataPrefix='marc21', set=set_oai, from_=from_date, until=until_date)
        saverecords(records)
    except: pass # skipping deleted entries

print('Done.')

#%% processing mrc to df

mrc_to_mrk('C:/Users/User/Desktop/nkp_nkc_2021-04-07.marc', 'C:/Users/User/Desktop/nkp_nkc_2021-04-07.mrk')

fiction_types = ['1', 'd', 'f', 'h', 'j', 'p', 'u', '|', '\\']

filter_fiction_type = get_bool('Filter with a fiction type? ')

encoding = 'utf-8' 
new_list = []

# marc_list = io.open('C:/Users/User/Desktop/nkp_nkc_2021-04-07.mrk', 'rt', encoding = encoding).read().splitlines()

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

if filter_fiction_type:
    marc_df.to_excel(f'NKP_Czech_translations_SKC_{year}-{month}-{day}.xlsx', index=False)
else:
    marc_df.to_excel(f'NKP_Czech_translations_SKC_without_filter_fiction_type_{year}-{month}-{day}.xlsx', index=False)
#%% sending file to google drive

gc = gs.oauth()
gauth = GoogleAuth()
gauth.LocalWebserverAuth()
drive = GoogleDrive(gauth)

file_list = drive.ListFile({'q': f"'{cr_projects}' in parents and trashed=false"}).GetList() 
#[print(e['title'], e['id']) for e in file_list]
translation_folder = [file['id'] for file in file_list if file['title'] == 'Vimr Project'][0]

nkp_sheet = gc.create('NKP_Czech_translations', translation_folder)
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


#%% nowe podejście
import requests
oai_url, set_oai, date_from, date_until, metadata_prefix = 'https://aleph.nkp.cz/OAI', 'SKC', '2014-12-29', '2014-12-29', 'marc21'
params = {'verb':'ListRecords',
          'metadataPrefix': metadata_prefix,
          'set': set_oai,
          'from': date_from,
          'until': date_until}

response = requests.get(oai_url, params=params)

mrc_response = MARCReader(response.content)

for record in mrc_response:
    print(record)

writer = MARCWriter(open('test.mrc', 'wb'))
writer.write(mrc_response)
writer.close()

writer = MARCWriter(open('test.xml', 'wb'))
records = map_xml(writer.write, mrc_response) 
writer.close()


















# z resumption token
from sickle import Sickle
from sickle.iterator import OAIResponseIterator(sickle, params)
from pymarc import Record, Field, MARCWriter, MARCReader, map_xml, XMLWriter, marcxml
import xml.etree.ElementTree as et
from lxml import etree
from lxml.etree import tostring
import io

namespaces = {
    'oai': 'http://www.openarchives.org/OAI/2.0/',
    'marc21': 'http://www.loc.gov/MARC21/slim'
} 

def xml_to_mrc(path_in, path_out, namespace=False):
    writer = MARCWriter(open(path_out, 'wb'))
    records = map_xml(writer.write, path_in, namespace) 
    writer.close() 

oai_url, set_oai, date_from, date_until, metadata_prefix = 'https://aleph.nkp.cz/OAI', 'SKC', '2014-12-29', '2014-12-29', 'marc21'

sickle = Sickle(oai_url)
records = sickle.ListRecords(
    **{'metadataPrefix': metadata_prefix,
       'set': set_oai,
       'from': date_from,
       'until': date_until
       })

for record in records:
    data = tostring(record.xml, encoding='UTF-8')
    print(type(data))
    with open('test.xml', 'wt', encoding='UTF-8') as file:
        file.write(data)  
    
    
    print(data)
    
    xml_to_mrc(str(record.xml), 'test.mrc')
    print(record.xml)

record_xml = records.sickle

dir(records)
records.mapper = Record

writer = MARCWriter(open('test.mrc', 'wb'))
for record in records:
    print(record)
    writer.write(record)
writer.close() 
    


record_xml = records.next().xml
writer = XMLWriter(open('file.xml','wb'))
records = map_xml(writer.write, record_xml) 

writer.write(record_xml)
writer.close() 


def MarcXML(xml):
    handler = marcxml.XmlHandler()
    data = tostring(xml, encoding='UTF-8')
    marcxml.parse_xml(io.BytesIO(data), handler)
    return handler.records

test = MarcXML(record_xml)
writer = MARCWriter(open('test.mrc', 'wb'))
for el in test:
    writer.write(test)
writer.close() 




handler = marcxml.XmlHandler()
data = tostring(record_xml, encoding='UTF-8')
marcxml.parse_xml(io.BytesIO(data), handler)













with open('response.xml', 'w') as fp:
    fp.write(record_xml)


tree = etree.ElementTree(record_xml)
















with open('response.xml', 'w') as fp:
    fp.write(records)

for record in records:
    with open('test.xml', 'wt', encoding='UTF-8') as file:
        file.write(record.raw)


xml_to_mrc('test.xml', 'test.mrc', namespace=True)


        
tree = et.parse('test.xml')
writer = XMLWriter(open('file.xml','wb'))
writer.write(MARCReader(tree))
writer.close() 


for record in records:
    try:
        writer = XMLWriter(open('file.xml','wb'))
        writer.write(str(record))
        writer.close() 
    except Exception:
        print(record)
    
    
    
    
for record in records:
    with open('test.xml', 'wt', encoding='UTF-8') as file:
        file.write(str(record))
        
with open('test.xml', 'wt', encoding='UTF-8') as file:
        file.write(records)        
        
        
    xml_to_mrc('test.xml', 'test.mrc')
        
    # tree = et.parse('test.xml')
    record = map_xml(writer.write, 'test.xml')
writer.close() 

writer = MARCWriter(open('test.mrc','ab'))
for record in records:
    record = map_xml(writer.write, str(record))
writer.close() 

def xml_to_mrc(path_in, path_out):
    writer = pymarc.MARCWriter(open(path_out, 'wb'))
    records = pymarc.map_xml(writer.write, path_in) 
    writer.close() 
    
    
writer = MARCWriter(open('test.mrc','ab'))
for record in records:
    writer.write(MARCReader(record))
writer.close()


with open('response.xml', 'w') as fp:
    fp.write(records.next().raw.encode('utf8'))

records.next()


for record in records:
    print(record.xml())

for record in records:
    record = MARCReader(record.raw)
    print(record)

#'get_metadata', 'header', 'metadata', 'raw', 'xml']

    if tytul_czasopisma == 'Czytanie Literatury':
        records = sickle.ListRecords(metadataPrefix='oai_dc', set='com_11089_5783')
    else:
        records = sickle.ListRecords(metadataPrefix='oai_dc')




























