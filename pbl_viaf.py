import cx_Oracle
import pandas as pd
from my_functions import gsheet_to_df, df_to_mrc, cosine_sim_2_elem, mrc_to_mrk, cSplit, get_cosine_result
import pymarc
import numpy as np
import copy
import math
from datetime import datetime
import re
from functools import reduce
import pandasql
from urllib.parse import urlparse
import requests
from bs4 import BeautifulSoup
from Levenshtein import distance as levenshtein_distance


dsn_tns = cx_Oracle.makedsn('pbl.ibl.poznan.pl', '1521', service_name='xe')
connection = cx_Oracle.connect(user='IBL_SELECT', password='CR333444', dsn=dsn_tns, encoding='windows-1250')

# PBL queries

pbl_indeks_osobowy = """select distinct ind.odi_imie, ind.odi_nazwisko
from IBL_OWNER.pbl_osoby_do_indeksu ind"""
                    
pbl_indeks_osobowy = pd.read_sql(pbl_indeks_osobowy, con=connection).fillna(value = np.nan)
pbl_indeks_osobowy['full name'] = pbl_indeks_osobowy['ODI_NAZWISKO'] + ' ' + pbl_indeks_osobowy['ODI_IMIE']

test = pbl_indeks_osobowy.head(100)
# viaf
viaf_errors = []
pbl_indeks_viaf = pd.DataFrame()
for index, row in test.iterrows():
    print(str(index) + '/' + str(len(test)))
    try:
        url = re.sub('\s+', '%20', f"http://viaf.org/viaf/search?query=local.personalNames%20all%20%22{row['full name']}%22&sortKeys=holdingscount&recordSchema=BriefVIAF")
        url = 'http://viaf.org/viaf/search?query=local.personalNames%20all%20%22Uspie%C5%84ski%20Edward%22&sortKeys=holdingscount&recordSchema=BriefVIAF'
        index = 50
        row = test.iloc[50]
        response = requests.get(url)
        response.encoding = 'UTF-8'
        soup = BeautifulSoup(response.text, 'html.parser')
        people_links = soup.findAll('a', attrs={'href': re.compile("viaf/\d+")})
        flags = people_links.find_all('img', attrs={'src': re.compile("viaf/images/flags")})
        viaf_people = []
        for people in people_links:
            print(people.text)
            flags = people.find_all('img', attrs={'src': re.compile("viaf/images/flags")})
            for flag in flags:
                print(flag['title'])
            print('______________________')
            
            person_name = re.split('â\x80\x8e|\u200e ', re.sub('\s+', ' ', people.text).strip())
            person_link = re.sub(r'(.+?)(\#.+$)', r'http://viaf.org\1viaf.xml', people['href'].strip())
            viaf_people.append([person_name, person_link])
        viaf_people = pd.DataFrame(viaf_people, columns=['viaf name', 'viaf'])
        viaf_people = pd.DataFrame(viaf_people['viaf name'].tolist(), viaf_people['viaf']).stack()
        viaf_people = viaf_people.reset_index()[[0, 'viaf']]
        viaf_people.columns = ['viaf name', 'viaf']
        viaf_people['full name'] = f"{row['full name']}"
        for ind, vname in viaf_people.iterrows():
            viaf_people.at[ind, 'cosine'] = get_cosine_result(vname['viaf name'], vname['full name'])
        if viaf_people['cosine'].max() == 0:
            for ind, vname in viaf_people.iterrows():
                viaf_people.at[ind, 'levenshtein'] = levenshtein_distance(vname['viaf name'], vname['full name'])
            
            
        
        viaf_people['viaf len'] = viaf_people['viaf'].apply(lambda x: len(x))
        viaf_people = viaf_people[(viaf_people['cosine'] == viaf_people['cosine'].max()) &
                                  (viaf_people['viaf len'] == viaf_people['viaf len'].min())]

# zamiast head dać polską nazwę, jeśli nie ma to head
        
        viaf_people = viaf_people.head(1).drop(columns=['cosine', 'viaf len'])
        pbl_indeks_viaf = pbl_indeks_viaf.append(viaf_people)
    except IndexError:
        error = [row['full name']]
        viaf_errors.append(error)   
        
test = pd.merge(test, viaf_people, how='left', on='full name')
      
viaf_df = pd.DataFrame(viaf_enrichment, columns=['index', 'cz_name', 'cz_dates', 'viaf_id', 'IDs', 'nationality', 'occupation', 'language', 'name_and_source'])
errorfile = io.open('cz_translation_errors.txt', 'wt', encoding = 'UTF-8')
for element in viaf_errors:
errorfile.write(str(element) + '\n\n')
errorfile.close()

viaf_df.to_excel('cz_viaf.xlsx', index = False)

























