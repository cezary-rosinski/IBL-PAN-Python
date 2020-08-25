from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from bs4 import BeautifulSoup
import re
from selenium.common.exceptions import NoSuchElementException
import numpy as np
import time 
import pandas as pd
import itertools
import cx_Oracle
from my_functions import get_cosine_result

browser = webdriver.Chrome()
browser.get("http://czasopisma.fbc.net.pl/?s=newspaper_title%2520asc&ipp=90")
items_on_page = len(browser.find_elements_by_class_name('item__header')) + 1
journals_info = []
for index, elem in enumerate(range(1,items_on_page)):
    print('    ' + str(index + 1) + '/' + str(len(range(1,items_on_page))))
    journal_title = browser.find_element_by_css_selector(f"div.app:nth-child(2) div.main-page:nth-child(1) div.search__container div.search__boxes div.search__box.search__box--right:nth-child(2) div.block.block--results div.block__content article:nth-child(1) ul:nth-child(1) li.item:nth-child({elem}) div.item__header > h3.item__header--title").text
    journal_subject = browser.find_element_by_css_selector(f"div.app:nth-child(2) div.main-page:nth-child(1) div.search__container div.search__boxes div.search__box.search__box--right:nth-child(2) div.block.block--results div.block__content article:nth-child(1) ul:nth-child(1) li.item:nth-child({elem}) div.item__content > div.item__infobox").text.split('\n')
    try:
        subject_index = journal_subject.index('Temat:')
        colon_indexes = [i for i, item in enumerate(journal_subject) if re.search('\:', item)]
        next_colon_index = next(x for x in colon_indexes if x > subject_index)
        journal_subject = '❦'.join(journal_subject[subject_index+1:next_colon_index])
    except ValueError:
        journal_subject = np.nan
    journals_info.append([journal_title, journal_subject])

no_of_pages = range(2,int(max([elem.text for elem in browser.find_elements_by_css_selector('.pagination__bottom--list-link')]))+1)

for ind, no in enumerate(no_of_pages):
    print(str(ind) + '/' + str(len(no_of_pages)))
    browser.get(f"http://czasopisma.fbc.net.pl/?p={no}&s=newspaper_title%2520asc&ipp=90")
    time.sleep(1)
    items_on_page = len(browser.find_elements_by_class_name('item__header')) + 1
    for index, elem in enumerate(range(1,items_on_page)):
        print('    ' + str(index + 1) + '/' + str(len(range(1,items_on_page))))
        journal_title = browser.find_element_by_css_selector(f"div.app:nth-child(2) div.main-page:nth-child(1) div.search__container div.search__boxes div.search__box.search__box--right:nth-child(2) div.block.block--results div.block__content article:nth-child(1) ul:nth-child(1) li.item:nth-child({elem}) div.item__header > h3.item__header--title").text
        journal_subject = browser.find_element_by_css_selector(f"div.app:nth-child(2) div.main-page:nth-child(1) div.search__container div.search__boxes div.search__box.search__box--right:nth-child(2) div.block.block--results div.block__content article:nth-child(1) ul:nth-child(1) li.item:nth-child({elem}) div.item__content > div.item__infobox").text.split('\n')
        try:
            subject_index = journal_subject.index('Temat:')
            colon_indexes = [i for i, item in enumerate(journal_subject) if re.search('\:', item)]
            next_colon_index = next(x for x in colon_indexes if x > subject_index)
            journal_subject = '❦'.join(journal_subject[subject_index+1:next_colon_index])
        except ValueError:
            journal_subject = np.nan
        journals_info.append([journal_title, journal_subject])
browser.quit()

journals_info_table = pd.DataFrame(journals_info, columns=['journal title', 'journal subject'])
journals_info_table.to_excel('FBC_lista_czasopism.xlsx', index=False)

fbc_list = journals_info_table['journal title'].to_list()   

dsn_tns = cx_Oracle.makedsn('pbl.ibl.poznan.pl', '1521', service_name='xe')
connection = cx_Oracle.connect(user='IBL_SELECT', password='CR333444', dsn=dsn_tns, encoding='windows-1250')

pbl_query = """select zr.zr_zrodlo_id, zr.zr_tytul
            from IBL_OWNER.pbl_zrodla zr"""

pbl_magazines = pd.read_sql(pbl_query, connection)

pbl_list = pbl_magazines['ZR_TYTUL'].to_list()  

combinations = list(itertools.product(pbl_list, fbc_list))

df = pd.DataFrame(combinations, columns=['pbl_journal', 'fbc_journal'])
df['similarity'] = df.apply(lambda x: get_cosine_result(x['pbl_journal'], x['fbc_journal']), axis=1)
df = df[df['similarity'] > 0.35].sort_values(['pbl_journal', 'similarity'], ascending=[True, False])

df.to_excel('mapowanie_czasopism_PBL_FBC.xlsx', index=False)

print('Done')













