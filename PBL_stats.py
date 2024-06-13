from pbl_credentials import pbl_user, pbl_password
import cx_Oracle
import pandas as pd
import numpy as np
from tqdm import tqdm
import json
import ijson
import xml.etree.ElementTree as ET
import xml.dom.minidom

#%%
# stary PBL 912178

dsn_tns = cx_Oracle.makedsn('pbl.ibl.poznan.pl', '1521', service_name='xe')
connection = cx_Oracle.connect(user=pbl_user, password=pbl_password, dsn=dsn_tns, encoding='windows-1250')

pbl_query = """select z.za_zapis_id, z.za_ro_rok, z.za_status_imp
from pbl_zapisy z
where z.za_status_imp like 'IOK' or z.za_status_imp is null"""
pbl_records = pd.read_sql(pbl_query, con=connection).fillna(value = np.nan)

#nowy PBL 1214588

json_path = r"C:\Users\Cezary\Documents\PBL-converter\elb_input\biblio.json"
# json_path = r"D:\IBL\Libri\dane z libri do pbl\2023-09-08 Mickiewicz\biblio.json"

data = ijson.parse(open(json_path, 'r'))
new_pbl_test = []
for prefix, event, value in data:
    if prefix == 'item.id':
        new_pbl_test.append([value])
    elif prefix == 'item.publishDate.item':
        new_pbl_test[-1].append(value)
        
#nowy PBL retro 435178
xml_files_retro = [
              r"C:\Users\Cezary\Documents\PBL-converter\xml_output\retro\1981_t1\import_retro_books_1981_t1_0.xml",
              r"C:\Users\Cezary\Documents\PBL-converter\xml_output\retro\1981_t1\import_retro_journal_items_1981_t1_0.xml",
              r"C:\Users\Cezary\Documents\PBL-converter\xml_output\retro\1981_t2\import_retro_books_1981_t2_0.xml",
              r"C:\Users\Cezary\Documents\PBL-converter\xml_output\retro\1981_t2\import_retro_journal_items_1981_t2_0.xml",
              r"C:\Users\Cezary\Documents\PBL-converter\xml_output\retro\1982_t1\import_retro_books_1982_t1_0.xml",
              r"C:\Users\Cezary\Documents\PBL-converter\xml_output\retro\1982_t1\import_retro_journal_items_1982_t1_0.xml",
              r"C:\Users\Cezary\Documents\PBL-converter\xml_output\retro\1982_t2\import_retro_books_1982_t2_0.xml",
              r"C:\Users\Cezary\Documents\PBL-converter\xml_output\retro\1982_t2\import_retro_journal_items_1982_t2_0.xml",
              r"C:\Users\Cezary\Documents\PBL-converter\xml_output\retro\1983_t1\import_retro_books_1983_t1_0.xml",
              r"C:\Users\Cezary\Documents\PBL-converter\xml_output\retro\1983_t1\import_retro_journal_items_1983_t1_0.xml",
              r"C:\Users\Cezary\Documents\PBL-converter\xml_output\retro\1983_t2\import_retro_books_1983_t2_0.xml",
              r"C:\Users\Cezary\Documents\PBL-converter\xml_output\retro\1983_t2\import_retro_journal_items_1983_t2_0.xml",
              r"C:\Users\Cezary\Documents\PBL-converter\xml_output\retro\1984_t1\import_retro_books_1984_t1_0.xml",
              r"C:\Users\Cezary\Documents\PBL-converter\xml_output\retro\1984_t1\import_retro_journal_items_1984_t1_0.xml",
              r"C:\Users\Cezary\Documents\PBL-converter\xml_output\retro\1984_t2\import_retro_books_1984_t2_0.xml",
              r"C:\Users\Cezary\Documents\PBL-converter\xml_output\retro\1984_t2\import_retro_journal_items_1984_t2_0.xml",
              r"C:\Users\Cezary\Documents\PBL-converter\xml_output\retro\1985_t1\import_retro_books_1985_t1_0.xml",
              r"C:\Users\Cezary\Documents\PBL-converter\xml_output\retro\1985_t1\import_retro_journal_items_1985_t1_0.xml",
              r"C:\Users\Cezary\Documents\PBL-converter\xml_output\retro\1985_t2\import_retro_books_1985_t2_0.xml",
              r"C:\Users\Cezary\Documents\PBL-converter\xml_output\retro\1985_t2\import_retro_journal_items_1985_t2_0.xml",
              r"C:\Users\Cezary\Documents\PBL-converter\xml_output\retro\1986_t1\import_retro_books_1986_t1_0.xml",
              r"C:\Users\Cezary\Documents\PBL-converter\xml_output\retro\1986_t1\import_retro_journal_items_1986_t1_0.xml",
              r"C:\Users\Cezary\Documents\PBL-converter\xml_output\retro\1986_t2\import_retro_books_1986_t2_0.xml",
              r"C:\Users\Cezary\Documents\PBL-converter\xml_output\retro\1986_t2\import_retro_journal_items_1986_t2_0.xml",
              r"C:\Users\Cezary\Documents\PBL-converter\xml_output\retro\1987_t1\import_retro_books_1987_t1_0.xml",
              r"C:\Users\Cezary\Documents\PBL-converter\xml_output\retro\1987_t1\import_retro_journal_items_1987_t1_0.xml",
              r"C:\Users\Cezary\Documents\PBL-converter\xml_output\retro\1987_t2\import_retro_books_1987_t2_0.xml",
              r"C:\Users\Cezary\Documents\PBL-converter\xml_output\retro\1987_t2\import_retro_journal_items_1987_t2_0.xml",
              r"C:\Users\Cezary\Documents\PBL-converter\xml_output\retro\1988_t1\import_retro_books_1988_t1_0.xml",
              r"C:\Users\Cezary\Documents\PBL-converter\xml_output\retro\1988_t1\import_retro_journal_items_1988_t1_0.xml",
              r"C:\Users\Cezary\Documents\PBL-converter\xml_output\retro\1988_t2\import_retro_books_1988_t2_0.xml",
              r"C:\Users\Cezary\Documents\PBL-converter\xml_output\retro\1988_t2\import_retro_journal_items_1988_t2_0.xml"
              ]
new_pbl_retro_records = 0
for xml_file in tqdm(xml_files_retro):
    tree = ET.parse(xml_file)
    root = tree.getroot()
    if '_books_' in xml_file:
        new_pbl_retro_records += len(root.findall('.//book'))
    else: new_pbl_retro_records += len(root.findall('.//journal-item'))

    # dom = xml.dom.minidom.parse(xml_file)
    # pretty_xml_as_string = dom.toprettyxml()  

#ile BN w ELB i nowym PBL-u 437251
old_pbl_in_elb = set([e[0] for e in new_pbl_test if e[0].startswith('pl')])
bn_in_elb_test = set([e[0] for e in new_pbl_test])
bn_in_elb = bn_in_elb_test - old_pbl_in_elb

#tylko stary PBL 135169
old_pbl_in_elb = set([int(e[2:]) for e in old_pbl_in_elb])
old_pbl_full = set([int(e) for e in pbl_records['ZA_ZAPIS_ID'].to_list()])
only_old_pbl = old_pbl_full - old_pbl_in_elb






mrk_list = []
for row in marc_list:
    if row.startswith('=LDR'):
        mrk_list.append([row])
    else:
        if row:
            mrk_list[-1].append(row)


with open(json_path, encoding='utf8') as json_data:
    d = json.load(json_data)
    new_pbl_records = [{k:v if isinstance(v, str) else v[0] for k,v in e.items() if k in ['id', 'publishDate']} for e in d]
    
    [e for e in d]
    


    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
