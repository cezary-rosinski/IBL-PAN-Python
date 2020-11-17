import cx_Oracle
import pandas as pd
from my_functions import gsheet_to_df, df_to_mrc, cosine_sim_2_elem, mrc_to_mrk, cSplit, get_cosine_result
import pymarc
import numpy as np
import copy
import math
from datetime import datetime
import regex as re
from functools import reduce
import pandasql
from urllib.parse import urlparse
import requests
from bs4 import BeautifulSoup
#from Levenshtein import distance as levenshtein_distance


dsn_tns = cx_Oracle.makedsn('pbl.ibl.poznan.pl', '1521', service_name='xe')
connection = cx_Oracle.connect(user='IBL_SELECT', password='CR333444', dsn=dsn_tns, encoding='windows-1250')

# PBL queries

pbl_indeks_osobowy = """select distinct ind.odi_imie, ind.odi_nazwisko
from IBL_OWNER.pbl_osoby_do_indeksu ind"""                   
pbl_indeks_osobowy = pd.read_sql(pbl_indeks_osobowy, con=connection).fillna(value = np.nan)
pbl_indeks_osobowy['full name'] = pbl_indeks_osobowy.apply(lambda x: ' '.join(x.dropna().astype(str)), axis=1)

cluster_maryi = pbl_indeks_osobowy[pbl_indeks_osobowy['ODI_NAZWISKO'].str.contains('Jezus')]

test = pbl_indeks_osobowy.head(25)
# viaf
viaf_errors = []
pbl_indeks_viaf = pd.DataFrame()
for index, row in test.iterrows():
    print(str(index+1) + '/' + str(len(test)))
    try:
        url = re.sub('\s+', '%20', f"http://viaf.org/viaf/search?query=local.personalNames%20all%20%22{row['full name']}%22&sortKeys=holdingscount&recordSchema=BriefVIAF")
        response = requests.get(url)
        response.encoding = 'UTF-8'
        soup = BeautifulSoup(response.text, 'html.parser')
        people_links = soup.findAll('a', attrs={'href': re.compile("viaf/\d+")})
        viaf_people = []
        for people in people_links:
            person_name = re.split('â\x80\x8e|\u200e ', re.sub('\s+', ' ', people.text).strip())
            person_link = re.sub(r'(.+?)(\#.+$)', r'http://viaf.org\1viaf.xml', people['href'].strip())
            person_link = [person_link] * len(person_name)
            libraries = str(people).split('<br/>')
            libraries = [re.sub('(.+)(\<span.*+$)', r'\2', s.replace('\n', ' ')) for s in libraries if 'span' in s]
            single_record = list(zip(person_name, person_link, libraries))
            viaf_people += single_record
        viaf_people = pd.DataFrame(viaf_people, columns=['viaf name', 'viaf', 'libraries'])
        viaf_people['full name'] = f"{row['full name']}"
        for ind, vname in viaf_people.iterrows():
            viaf_people.at[ind, 'cosine'] = get_cosine_result(vname['viaf name'], vname['full name'])
        viaf_people['viaf len'] = viaf_people['viaf'].apply(lambda x: len(x))

        viaf_people = viaf_people[viaf_people['cosine'] == viaf_people['cosine'].max()]
        viaf_people = viaf_people[viaf_people['viaf len'] == viaf_people['viaf len'].min()]
        if len(viaf_people[viaf_people['libraries'].str.contains('Biblioteka Narodowa (Polska)|NUKAT (Polska)')]) == 0:
            viaf_people = viaf_people.head(1).drop(columns=['cosine', 'viaf len', 'libraries'])
        else:
            viaf_people = viaf_people[viaf_people['libraries'].str.contains('Biblioteka Narodowa (Polska)|NUKAT (Polska)')]
        pbl_indeks_viaf = pbl_indeks_viaf.append(viaf_people)
    except (IndexError, KeyError):
        error = [row['full name']]
        viaf_errors.append(error)   
        
test = pd.merge(test, pbl_indeks_viaf, how='left', on='full name')
      

































