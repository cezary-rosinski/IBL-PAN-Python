import requests
from pymarc import MARCReader
from my_functions import xml_to_mrk, xml_to_mrc
import pymarc
import xml.etree.ElementTree as et
import sys
from datetime import datetime, timedelta
from tqdm import tqdm
import io

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
        









































