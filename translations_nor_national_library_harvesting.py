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

#%%harvesting OAI-PMH

#date
now = datetime.now()
year = now.year
month = '{:02d}'.format(now.month)
day = '{:02d}'.format(now.day)

# vars

savefile = f'F:/Cezary/Documents/IBL/Translations/nor_{year}-{month}-{day}.marc'
logfile = f'F:/Cezary/Documents/IBL/Translations/nor_log_{year}-{month}-{day}.txt'
# savefile = f'C:/Users/User/Desktop/nor_{year}-{month}-{day}.marc'
# logfile = f'C:/Users/User/Desktop/nor_log_{year}-{month}-{day}.txt'
URL = 'http://bibsys.alma.exlibrisgroup.com/view/oai/47BIBSYS_NB'
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

# init

registry = MetadataRegistry()
registry.registerReader('marc21', MarcXML)
client = Client(URL, registry)

start = valid_date(start_date)
start = valid_date('2020-01-21 00:00:00')
stop = valid_date(stop_date)

# main

while start < stop:
    from_date = start
    start = start + timedelta(days=1)  # increase days one by one
    until_date = start
    try: 
        records = client.listRecords(metadataPrefix='marc21', set='oai_komplett', from_=from_date, until=until_date)
        saverecords(records)
    except: pass # skipping deleted entries

print('Done!')

for record in records:
    print(record)

#https://bibsys.alma.exlibrisgroup.com/view/oai/47BIBSYS_NB/request?verb=ListRecords&metadataPrefix=marc21&set=oai_komplett&from=2020-01-21T00:00:00Z&until=2020-01-22T00:00:00Z



import requests
from pymarc import MARCReader
from my_functions import xml_to_mrk, xml_to_mrc
import pymarc
import xml.etree.ElementTree as et

url = 'https://bibsys.alma.exlibrisgroup.com/view/oai/47BIBSYS_NB/request?verb=ListRecords&metadataPrefix=marc21&set=oai_komplett&from=2020-01-21T00:00:00Z&until=2020-01-22T00:00:00Z'
response = requests.get(url)
with open('test2.xml', 'wb') as file:
    file.write(response.content)
    
tree = et.parse('test.xml')
root = tree.getroot() 
ns = '{http://www.openarchives.org/OAI/2.0/}'
root.tag
records = root.findall(f'.//{ns}record/{ns}metadata/{{http://www.loc.gov/MARC21/slim}}record')

writer = pymarc.TextWriter(io.open('testtest.mrk', 'wt', encoding="utf-8"))
for record in records:
    pymarc.map_xml(writer.write, str(record)) 
    writer.close() 
    


def xml_to_mrk(path_in, path_out):
    writer = pymarc.TextWriter(io.open(path_out, 'wt', encoding="utf-8"))
    pymarc.map_xml(writer.write, path_in) 
    writer.close() 
    
    
lista = []
for record in record

records[0].getchildren()

root.getchildren()












for e in root[-1].getchildren():
    print(e)
    with open('test3.xml', 'wb') as file:
        file.write(et.tostring(e))

root[-1].getchildren()[0].getchildren()[-1].getchildren()[0].getchildren()

lista = []
for e in root[-1].getchildren():
    try:
        val = e.getchildren()[-1]
        lista.append(et.tostring(val))
    except IndexError:
        pass
    dotrzeć do rekordów i je poiterować - może da się z nich zrobić marki
    
    
    
    
    
    
    
    
    
    
    
    
    
    

tree = et.parse('test.xml')
root = tree.getroot()

dir(root)

print(root[-1])

for e in root[-1].getchildren():
    e = MARCReader(et.tostring(e))
    print(e)

test = MARCReader(et.tostring(root[-1]))
next(test)
for e in root:
    print(e)

with open('test2.xml', 'wb') as f:
    for e in root[-1].getchildren():
        f.write(et.tostring(e))

xml_to_mrk('test2.xml', 'test.mrk')   







for e in tree.iter():
    print(e)
    
writer = pymarc.TextWriter(io.open('test.mrk', 'wt', encoding="utf-8"))    
records = pymarc.map_xml(writer.write, 'test.xml')     
    
xml_to_mrk('test.xml', 'test.mrk')    
xml_to_mrc('test.xml', 'test.mrc')  
xml_to   


def xml_to_mrk(path_in, path_out):
    writer = pymarc.TextWriter(io.open(path_out, 'wt', encoding="utf-8"))
    records = pymarc.map_xml(writer.write, path_in) 
    writer.close() 


def xml_to_mrc(path_in, path_out):
    writer = pymarc.MARCWriter(open(path_out, 'wb'))
    records = pymarc.map_xml(writer.write, path_in) 
    writer.close()  

import lxml.etree
# tree_pretty = lxml.etree.parse('test.xml')
# pretty = lxml.etree.tostring(tree_pretty, encoding="unicode", pretty_print=True)








   
    

reader = MARCReader(response.content, force_utf8=True)


(marc_target, to_unicode=True, force_utf8=False,
hide_utf8_warnings=False, utf8_handling=’strict’,
file_encoding=’iso8859-1’, permissive=False)
for record in reader:
    print(1)
with open('file.mrc', 'wb') as out:
   out.write(reader.as_marc())

for record in reader:
    with open('file.mrc', 'wb') as out:
        out.write(record.as_marc())


import urllib.request
from datetime import datetime, timedelta
import sys

start_date = '2018-02-21 00:00:00'   # [YYYY-mm-dd HH:MM:SS]
stop_date = '2021-04-01 00:00:00'

def valid_date(s):
    try:
        return datetime.strptime(s, '%Y-%m-%d %H:%M:%S')
    except:
        print('Invalid date format.')
        sys.exit(1)

start = valid_date(start_date)
stop = valid_date(stop_date)

while start < stop:
    from_date = start
    start = start + timedelta(days=1)  # increase days one by one
    until_date = start
    print(from_date)
    harvesturl = 'https://bibsys.alma.exlibrisgroup.com/view/oai/47BIBSYS_NB/request?verb=ListRecords&metadataPrefix=marc21&set=oai_komplett&from='+from_date.strftime('%Y-%m-%dT%H:%M:%SZ')+'&until='+until_date.strftime('%Y-%m-%dT%H:%M:%SZ')
    try:
        with urllib.request.urlopen(harvesturl) as f:
            urlcontent = f.read().decode('utf-8')
            allcontent = urlcontent
            while 'resumptionToken>' in urlcontent:
                #print('token now') # monitoring tokens                
                rtoken = urlcontent.split('resumptionToken>')[1][:-2]
                harvesturl = 'https://bibsys.alma.exlibrisgroup.com/view/oai/47BIBSYS_NB/request?verb=ListRecords&resumptionToken='+rtoken
                #print(harvesturl)
                try:
                    with urllib.request.urlopen(harvesturl) as t:
                        urlcontent = t.read().decode('utf-8')
                        allcontent+=urlcontent
                except urllib.error.URLError as e:
                    print(e.reason)
            print(allcontent) # harvest of the day here - please save me
    except urllib.error.URLError as e:
        print(e.reason)

print('done')








































