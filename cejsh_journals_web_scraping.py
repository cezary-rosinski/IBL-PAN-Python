import requests
from bs4 import BeautifulSoup
import re
import itertools
import numpy as np
import pandas as pd
import cx_Oracle
from my_functions import get_cosine_result

url = "http://cejsh.icm.edu.pl/cejsh/browse/journals"
response = requests.get(url)
response.encoding = 'UTF-8'
soup = BeautifulSoup(response.text, 'html.parser')

selector_elements = list(itertools.repeat(['', '.grey_row'], 10))
selector_elements = [item for sublist in selector_elements for item in sublist]
range_elements = list(range(3,23))

page_iteration = list(zip(selector_elements, range_elements))

journals_info = []
for elem in page_iteration:
    one_journal = ''.join([elem.text for elem in soup.select(f"div.dynamic_content:nth-child(2) div:nth-child(2) div.round-border:nth-child(10) table:nth-child(1) tbody:nth-child(1) > tr{elem[0]}:nth-child({elem[1]})")]).split('\n\n\n\t\t\t')
    one_journal = [elem.strip() for elem in one_journal if elem]
    journals_info.append(one_journal)

no_of_pages = int(re.findall('\d+', [elem.text.strip() for elem in soup.select('.page-number')][0].split('\t')[-1].strip())[0]) + 1
no_of_pages = range(2,no_of_pages)

for index, page in enumerate(no_of_pages):
    print(str(index) + '/' + str(len(no_of_pages)))
    url = f"http://cejsh.icm.edu.pl/cejsh/browse/journals/{page}"
    response = requests.get(url)
    response.encoding = 'UTF-8'
    soup = BeautifulSoup(response.text, 'html.parser')
    for elem in page_iteration:
        one_journal = ''.join([elem.text for elem in soup.select(f"div.dynamic_content:nth-child(2) div:nth-child(2) div.round-border:nth-child(10) table:nth-child(1) tbody:nth-child(1) > tr{elem[0]}:nth-child({elem[1]})")]).split('\n\n\n\t\t\t')
        one_journal = [elem.strip() for elem in one_journal if elem]
        journals_info.append(one_journal)
    
journals_info = [elem for elem in journals_info if elem]
journals_info_table = pd.DataFrame(journals_info, columns=['journal number', 'journal title', 'publisher']).fillna(value=np.nan)
journals_info_table.to_excel('CEJSH_lista_czasopism.xlsx', index=False)

cejsh_list = journals_info_table['journal title'].to_list()   

dsn_tns = cx_Oracle.makedsn('pbl.ibl.poznan.pl', '1521', service_name='xe')
connection = cx_Oracle.connect(user='IBL_SELECT', password='CR333444', dsn=dsn_tns, encoding='windows-1250')

pbl_query = """select zr.zr_zrodlo_id, zr.zr_tytul
            from IBL_OWNER.pbl_zrodla zr"""

pbl_magazines = pd.read_sql(pbl_query, connection)

pbl_list = pbl_magazines['ZR_TYTUL'].to_list()  

combinations = list(itertools.product(pbl_list, cejsh_list))

df = pd.DataFrame(combinations, columns=['pbl_journal', 'cejsh_journal'])
df['similarity'] = df.apply(lambda x: get_cosine_result(x['pbl_journal'], x['cejsh_journal']), axis=1)
df = df[df['similarity'] > 0.35].sort_values(['pbl_journal', 'similarity'], ascending=[True, False])

df.to_excel('mapowanie_czasopism_PBL_CEJSH.xlsx', index=False)

print('Done')












