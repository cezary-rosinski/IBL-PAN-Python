import requests
from pymarc import MARCReader
from my_functions import xml_to_mrk, xml_to_mrc, mrc_to_mrk
import pymarc
import xml.etree.ElementTree as et
import sys
from datetime import datetime, timedelta
from tqdm import tqdm
import io
import pymarc
import regex as re
import pandas as pd

encoding = 'UTF-8'
start_date = '2018-02-21 00:00:00'   # [YYYY-mm-dd HH:MM:SS]
stop_date = '2021-04-02 00:00:00'

def valid_date(s):
    try:
        return datetime.strptime(s, '%Y-%m-%d %H:%M:%S')
    except:
        print('Invalid date format.')
        sys.exit(1)

start = valid_date(start_date)
stop = valid_date(stop_date)

list_of_records = []
ns = '{http://www.openarchives.org/OAI/2.0/}'

while start < stop:
    from_date = start
    print(from_date.strftime('%Y-%m-%dT%H:%M:%SZ'))
    start = start + timedelta(days=1)
    until_date = start
    
    url = f"https://bibsys.alma.exlibrisgroup.com/view/oai/47BIBSYS_NB/request?verb=ListRecords&metadataPrefix=marc21&set=oai_komplett&from={from_date.strftime('%Y-%m-%dT%H:%M:%SZ')}&until={until_date.strftime('%Y-%m-%dT%H:%M:%SZ')}"
    response = requests.get(url)
    root = et.fromstring(response.content)
    records = root.findall(f'.//{ns}record/{ns}metadata/{{http://www.loc.gov/MARC21/slim}}record')
    
    for r in records:
        r = et.tostring(r)
        with open('test.xml', 'wb') as file:
            file.write(b"<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n")
            file.write(r)
        try:
            xml_to_mrk('test.xml', 'test.mrk')
            marc_list = io.open('test.mrk', 'rt', encoding = encoding).read().splitlines()
            list_of_records.append(marc_list)
        except (ValueError, AttributeError):
            pass
    
    while 'resumptionToken>' in response.content.decode('utf-8'):
        rtoken = response.content.decode('utf-8').split('resumptionToken>')[1][:-2]
        url = f"https://bibsys.alma.exlibrisgroup.com/view/oai/47BIBSYS_NB/request?verb=ListRecords&resumptionToken={rtoken}"
        response = requests.get(url)
        root = et.fromstring(response.content)
        records = root.findall(f'.//{ns}record/{ns}metadata/{{http://www.loc.gov/MARC21/slim}}record')
        
        for r in records:
            r = et.tostring(r)
            with open('test.xml', 'wb') as file:
                file.write(b"<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n")
                file.write(r)
            try:
                xml_to_mrk('test.xml', 'test.mrk')
                marc_list = io.open('test.mrk', 'rt', encoding = encoding).read().splitlines()
                list_of_records.append(marc_list)
            except (ValueError, AttributeError):
                pass
        
#%% date
now = datetime.now()
year = now.year
month = '{:02}'.format(now.month)
day = '{:02}'.format(now.day)

#%% zapisywanie do pliku .mrc

outputfile = open(f'norwegian_library_{year}-{month}-{day}.mrc', 'wb')
for record in tqdm(list_of_records, total=len(list_of_records)):
    try:
        for field in record:
            if field[1:4] == 'LDR':
                leader = field[6:]
                pymarc_record = pymarc.Record(to_unicode=True, force_utf8=True, leader=leader)
            elif field[1:4].isnumeric() and int(field[1:4]) < 10:
                tag = field[1:4]
                data = field[6:]
                marc_field = pymarc.Field(tag=tag, data=data)
                pymarc_record.add_ordered_field(marc_field)
            elif field[1:4].isnumeric() and int(field[1:4]) >= 10:
                tag = field[1:4]
                record_in_list = re.split('\$(.)', ''.join(field[6:]))
                indicators = list(record_in_list[0])
                subfields = record_in_list[1:]
                marc_field = pymarc.Field(tag=tag, indicators=indicators, subfields=subfields)
                pymarc_record.add_ordered_field(marc_field)
            elif field[1:4].isalpha() and field[1:4].isupper() and '$' in field:
                tag = field[1:4]
                record_in_list = re.split('\$(.)', ''.join(field[6:]))
                indicators = list(record_in_list[0])
                subfields = record_in_list[1:]
                marc_field = pymarc.Field(tag=tag, indicators=indicators, subfields=subfields)
                pymarc_record.add_ordered_field(marc_field)
        outputfile.write(pymarc_record.as_marc())
    except ValueError:
        pass
outputfile.close()  
      
#%% processing mrc to df

mrc_to_mrk('C:/Users/User/Desktop/norwegian_library_2021-04-07.mrc', 'C:/Users/User/Desktop/norwegian_library_2021-04-07.mrk')

fiction_types = ['1', 'd', 'f', 'h', 'j', 'p', 'u', '|', '\\']
years = range(1990,2021)
encoding = 'utf-8' 
   
marc_list = io.open('C:/Users/User/Desktop/norwegian_library_2021-04-07.mrk', 'rt', encoding = encoding).read().splitlines()

mrk_list = []
for row in marc_list:
    if row.startswith('=LDR'):
        mrk_list.append([row])
    else:
        if row:
            mrk_list[-1].append(row)

new_list = []        
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
    if language in ['nor', 'nob', 'nno'] and type_of_record_bibliographical_level == 'am' and fiction_type in fiction_types and bib_year in years and is_translation:
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

marc_df.to_excel(f'Translation_into_Norwegian_{year}-{month}-{day}.xlsx', index=False)
    




































