#original code by Ondrej Vimr
from oaipmh.client import Client
from oaipmh.metadata import MetadataRegistry
from pymarc import marcxml, MARCReader
import io
from lxml.etree import tostring
from datetime import datetime, timedelta
import sys
import os

# vars

savefile = 'F:/Cezary/Documents/IBL/Translations/Czech database/2021-03-18/nkp_nkc.marc'
logfile = 'F:/Cezary/Documents/IBL/Translations/Czech database/2021-03-18/log.txt'
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


















