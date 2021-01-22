import cx_Oracle
import pandas as pd
from my_functions import gsheet_to_df, get_cosine_result
import numpy as np
import regex as re
import requests
from bs4 import BeautifulSoup
import itertools
import time

dsn_tns = cx_Oracle.makedsn('pbl.ibl.poznan.pl', '1521', service_name='xe')
connection = cx_Oracle.connect(user='IBL_SELECT', password='CR333444', dsn=dsn_tns, encoding='windows-1250')

# PHASE 1 - automatic comparing PBL persons index with VIAF

pbl_indeks_osobowy = """select distinct ind.odi_imie, ind.odi_nazwisko
from IBL_OWNER.pbl_osoby_do_indeksu ind"""                   
pbl_indeks_osobowy = pd.read_sql(pbl_indeks_osobowy, con=connection).fillna(value = np.nan)

pbl_indeks_osobowy['full name'] = pbl_indeks_osobowy.apply(lambda x: ' '.join(x.dropna().astype(str)), axis=1).str.replace('\* ', '').str.replace(' \*', '')

# =============================================================================
# pbl_indeks_osobowy.to_excel('pbl_indeks_osobowy.xlsx', index=False)
# pbl_indeks_osobowy = pd.read_excel('pbl_indeks_osobowy.xlsx')
# =============================================================================

# viaf
pbl_indeks_viaf = pd.DataFrame()
for index, row in pbl_indeks_osobowy.iterrows():
    print(str(index+1) + '/' + str(len(pbl_indeks_osobowy)))
    connection_no = 1
    while True:
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
    
            viaf_people = viaf_people[viaf_people['cosine'] == viaf_people['cosine'].max()]
            if len(viaf_people[viaf_people['libraries'].str.contains('Biblioteka Narodowa \(Polska\)|NUKAT \(Polska\)|National Library of Poland|NUKAT Center of Warsaw University Library')]) == 0:
                viaf_people = viaf_people.head(1).drop(columns=['cosine', 'libraries'])
            else:
                viaf_people = viaf_people[viaf_people['libraries'].str.contains('Biblioteka Narodowa \(Polska\)|NUKAT \(Polska\)|National Library of Poland|NUKAT Center of Warsaw University Library')]
                viaf_people = viaf_people.head(1).drop(columns=['cosine', 'libraries'])
            pbl_indeks_viaf = pbl_indeks_viaf.append(viaf_people)
        except (IndexError, KeyError):
            pass
        except requests.exceptions.ConnectionError:
            print(connection_no)
            connection_no += 1
            time.sleep(300)
            continue
        break

# pbl_indeks_viaf = pd.read_excel('pbl_indeks_viaf.xlsx')
        
pbl_indeks_osobowy = pd.merge(pbl_indeks_osobowy, pbl_indeks_viaf, how='left', on='full name')
pbl_indeks_osobowy['viaf name'] = pbl_indeks_osobowy['viaf name'].str.replace('\\u200e', '').str.strip()
pbl_indeks_osobowy['viaf'] = pbl_indeks_osobowy['viaf'].str.replace('viaf.xml', '')

pbl_indeks_osobowy = pbl_indeks_osobowy.sort_values('viaf')
pbl_indeks_osobowy_bez_viaf = pbl_indeks_osobowy[pbl_indeks_osobowy['viaf'].isnull()]
pbl_indeks_osobowy = pbl_indeks_osobowy[pbl_indeks_osobowy['viaf'].notnull()]

count = pbl_indeks_osobowy['viaf'].value_counts().to_frame().rename(columns={'viaf': 'count'})
count['viaf'] = count.index
count.reset_index(drop=True, inplace=True)

pbl_indeks_osobowy = pd.merge(pbl_indeks_osobowy, count, how='left', on='viaf').sort_values(['count', 'viaf'], ascending=[False, True])

pbl_indeks_osobowy.to_csv('pbl_index_viaf.csv', index=False)

pbl_indeks_osobowy_bez_viaf.drop(columns=['viaf name', 'viaf'], inplace=True)
pbl_indeks_osobowy_bez_viaf.reset_index(drop=True, inplace=True)
pbl_indeks_osobowy_bez_viaf.to_csv('pbl_index_without_viaf.csv', index=False)

# PHASE 2 - manual corrections

# PHASE 3 - joining names without viaf with viaf names

# przeanalizować, czy rekord nie odnosi się tylko do jednej biblioteki (najprawdopodobniej BN PL), jeśli tak, to znaleźć inne rekordy dla tej osoby, zestawić z kwerendą w wikidacie i oddać rekord powiązany z wikidatą (czy skoro nie ma BN PL i nazwy polskiej to brać nazewnictwo z wikidaty?)

#wczytać wartości z dysku google dla unique_viaf i plik pbl_indeks_osobowy_bez_viaf

unique_viaf = pbl_indeks_osobowy[['viaf', 'viaf name']].drop_duplicates()

df_similarity = pd.DataFrame()
for i, row in pbl_indeks_osobowy_bez_viaf.iterrows():
    print(f"{i+1}/{len(pbl_indeks_osobowy_bez_viaf)}")
    combinations = list(itertools.product([row['full name']], unique_viaf['viaf name'].to_list()))
    df = pd.DataFrame(combinations, columns=['full name', 'viaf name'])
    df['similarity'] = df.apply(lambda x: get_cosine_result(x['full name'], x['viaf name']), axis=1)
    df = df[df['similarity'] > 0.35].sort_values(['viaf name', 'full name'], ascending=[True, False])
    df['ODI_IMIE'] = row['ODI_IMIE']
    df['ODI_NAZWISKO'] = row['ODI_NAZWISKO']
    df_similarity = df_similarity.append(df)
    
df_similarity = pd.merge(df_similarity, unique_viaf, how='left', on='viaf name')
col_order = pbl_indeks_osobowy.columns.tolist()[:-1]
col_order.append('similarity')

df_similarity = df_similarity.reindex(columns=col_order)  

#4 PHASE 4 - manual corrections of names reconciliation  
























