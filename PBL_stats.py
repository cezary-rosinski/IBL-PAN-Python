from pbl_credentials import pbl_user, pbl_password
import cx_Oracle
import pandas as pd
import numpy as np
from tqdm import tqdm
import json
import ijson
import xml.etree.ElementTree as ET
import xml.dom.minidom
import regex as re

#%%
# stary PBL 912178

dsn_tns = cx_Oracle.makedsn('pbl.ibl.poznan.pl', '1521', service_name='xe')
connection = cx_Oracle.connect(user=pbl_user, password=pbl_password, dsn=dsn_tns, encoding='windows-1250')

pbl_query = """select z.za_zapis_id, z.za_ro_rok, z.za_status_imp, rz.rz_nazwa
from pbl_zapisy z
full join pbl_rodzaje_zapisow rz on rz.rz_rodzaj_id = z.za_rz_rodzaj1_id
where z.za_status_imp like 'IOK' or z.za_status_imp is null"""
pbl_records = pd.read_sql(pbl_query, con=connection).fillna(value = np.nan)

pbl_records.to_excel('data/zapisy_stary_pbl.xlsx', index=False)

#nowy PBL 1214588

json_path = r"C:\Users\Cezary\Documents\PBL-converter\elb_input\biblio.json"
# json_path = r"D:\IBL\Libri\dane z libri do pbl\2023-09-08 Mickiewicz\biblio.json"

data = ijson.parse(open(json_path, 'r'))
new_pbl_test = []
for prefix, event, value in data:
    if prefix == 'item.id':
        new_pbl_test.append([value])
    elif prefix in ['item.publishDate.item', 'item.format_major.item']:
        new_pbl_test[-1].append(value)
        
new_pbl_records = pd.DataFrame(new_pbl_test, columns=['id', 'format', 'year'])
new_pbl_records.to_csv('data/zapisy_nowy_pbl.csv', index=False)
        
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
# new_pbl_retro_records = 0
new_pbl_retro = []
for xml_file in tqdm(xml_files_retro):
    # xml_file = xml_files_retro[0]
    year = re.search('\d+', xml_file).group(0)
    tree = ET.parse(xml_file)
    root = tree.getroot()
    if '_books_' in xml_file:
        new_pbl_retro.append((year, 'book', len(root.findall('.//book'))))
        # new_pbl_retro_records += len(root.findall('.//book'))
    else:
        new_pbl_retro.append((year, 'journal-item', len(root.findall('.//journal-item'))))
        #new_pbl_retro_records += len(root.findall('.//journal-item'))

    # dom = xml.dom.minidom.parse(xml_file)
    # pretty_xml_as_string = dom.toprettyxml() 
    
new_pbl_retro_records = pd.DataFrame(new_pbl_retro, columns=['year', 'format', 'number of records'])
new_pbl_retro_records.to_excel('data/zapisy_retro_nowy_pbl.xlsx', index=False)

#ile BN w ELB i nowym PBL-u 437251
old_pbl_in_elb = set([e[0] for e in new_pbl_test if e[0].startswith('pl')])
bn_in_elb_test = set([e[0] for e in new_pbl_test])
bn_in_elb = bn_in_elb_test - old_pbl_in_elb

#tylko stary PBL 135169
old_pbl_in_elb = set([int(e[2:]) for e in old_pbl_in_elb])
old_pbl_full = set([int(e) for e in pbl_records['ZA_ZAPIS_ID'].to_list()])
only_old_pbl = old_pbl_full - old_pbl_in_elb


#%%

old_pbl = pd.read_excel(r"C:\Users\Cezary\Downloads\zapisy_stary_pbl.xlsx")
new_pbl = pd.read_csv(r"C:\Users\Cezary\Downloads\zapisy_nowy_pbl.csv")
retro_pbl = pd.read_excel(r"C:\Users\Cezary\Downloads\zapisy_retro_nowy_pbl.xlsx")
    
old_pbl.columns.values
old_pbl['ZA_RO_ROK'].dtypes 
new_pbl.columns.values
new_pbl['year'].dtypes    
retro_pbl.columns.values    
retro_pbl['year'].dtypes    

old_pbl_plot = old_pbl[['ZA_RO_ROK', 'RZ_NAZWA']]
old_pbl_plot = old_pbl_plot.groupby(['ZA_RO_ROK']).count()
old_pbl_plot.plot()

old_pbl_plot = old_pbl[['ZA_RO_ROK', 'RZ_NAZWA', 'ZA_ZAPIS_ID']]
old_pbl_plot = old_pbl_plot.groupby(['ZA_RO_ROK', 'RZ_NAZWA']).count()
old_pbl_plot.plot(colormap='jet', rot=90)
old_pbl_plot.to_excel('data/stary_pbl_rodzaje_statystyki.xlsx')

new_pbl_plot = new_pbl[['year', 'format']]
new_pbl_plot = new_pbl_plot[(new_pbl_plot['year'].notnull()) & 
                            (new_pbl_plot['year'] >= 1978) & 
                            (new_pbl_plot['year'] <= 2023)]
new_pbl_plot = new_pbl_plot.groupby(['year']).count()
new_pbl_plot.plot()
    
new_pbl_plot = new_pbl[['year', 'format', 'id']]
new_pbl_plot = new_pbl_plot[(new_pbl_plot['year'].notnull()) & 
                            (new_pbl_plot['year'] >= 1978) & 
                            (new_pbl_plot['year'] <= 2023)]
new_pbl_plot = new_pbl_plot.groupby(['year', 'format']).count()
new_pbl_plot.plot(kind='bar', rot=90)   
new_pbl_plot.to_excel('data/nowy_pbl_rodzaje_statystyki.xlsx')
    
retro_pbl_plot = retro_pbl[['year', 'number of records']]
retro_pbl_plot = retro_pbl_plot.groupby(['year']).sum()
retro_pbl_plot.plot(colormap='jet', rot=90)

retro_pbl_plot = retro_pbl.copy()
retro_pbl_plot = retro_pbl_plot.groupby(['year', 'format']).sum()
retro_pbl_plot.plot(kind='bar', rot=90)
    
    
    
    
    
    
    
    
    
    
    
    
