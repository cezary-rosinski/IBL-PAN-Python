from my_functions import gsheet_to_df
import xml.etree.ElementTree as et
import requests
import pandas as pd
import re
import numpy as np
from bs4 import BeautifulSoup
from my_functions import cosine_sim_2_elem
from langdetect import detect_langs
from textblob import TextBlob 



# Pozyskanie elementów danych osobowych z serwisu VIAF

pbl_names = gsheet_to_df('1FoLmNZ0b-sZ5_IHPdl7JNfoJkdT3Z1N_flg6tbf91k4', 'VIAF')
pbl_names.fillna(value=np.nan, inplace=True)
pbl_names['search'] = pbl_names.apply(lambda x: ' '.join(x.dropna().astype(str)), axis=1)

ns = '{http://viaf.org/viaf/terms#}'
viaf_enrichment = []
viaf_errors = []
for index, row in pbl_names.iterrows():
    try:
        print(str(index) + '/' + str(len(pbl_names)))
        url = re.sub('\s+', '%20', f"http://viaf.org/viaf/search?query=local.personalNames%20all%20%22{row['search']}%22&sortKeys=holdingscount&recordSchema=BriefVIAF")
        response = requests.get(url)
        response.encoding = 'UTF-8'
        soup = BeautifulSoup(response.text, 'html.parser')
        people_links = soup.findAll('a', attrs={'href': re.compile("viaf/\d+")})
        viaf_people = []
        for people in people_links:
            person_name = re.sub('\s+', ' ', people.text).strip().split('\u200e ')
            person_link = re.sub(r'(.+?)(\#.+$)', r'http://viaf.org\1viaf.xml', people['href'].strip())
            viaf_people.append([person_name, person_link])
        viaf_people = pd.DataFrame(viaf_people, columns=['viaf name', 'viaf'])
        viaf_people = pd.DataFrame(viaf_people['viaf name'].tolist(), viaf_people['viaf']).stack()
        viaf_people = viaf_people.reset_index()[[0, 'viaf']]
        viaf_people.columns = ['viaf name', 'viaf']
        viaf_people['original name'] = row['search']
        for ind, vname in viaf_people.iterrows():
            viaf_people.at[ind, 'cosine'] = cosine_sim_2_elem([vname['viaf name'], vname['original name']]).iloc[:, -1].to_string(index=False).strip()
        viaf_people = viaf_people[viaf_people['cosine'] == viaf_people['cosine'].max()]
        for i, line in viaf_people.iterrows():
            url = line['viaf']
            response = requests.get(url)
            with open('viaf.xml', 'wb') as file:
                file.write(response.content)
            tree = et.parse('viaf.xml')
            root = tree.getroot()
            viaf_id = root.findall(f'.//{ns}viafID')[0].text
            IDs = root.findall(f'.//{ns}mainHeadings/{ns}data/{ns}sources/{ns}sid')
            IDs = '❦'.join([t.text for t in IDs])
            nationality = root.findall(f'.//{ns}nationalityOfEntity/{ns}data/{ns}text')
            nationality = '❦'.join([t.text for t in nationality])
            occupation = root.findall(f'.//{ns}occupation/{ns}data/{ns}text')
            occupation = '❦'.join([t.text for t in occupation])
            language = root.findall(f'.//{ns}languageOfEntity/{ns}data/{ns}text')
            language = '❦'.join([t.text for t in language])
            date_of_birth = root.findall(f'.//{ns}birthDate')
            date_of_birth = '❦'.join([t.text for t in date_of_birth])
            date_of_death = root.findall(f'.//{ns}deathDate')
            date_of_death = '❦'.join([t.text for t in date_of_death])
            names = root.findall(f'.//{ns}x400/{ns}datafield')
            sources = root.findall(f'.//{ns}x400/{ns}sources')
            name_source = []
            for (name, source) in zip(names, sources):   
                person_name = ' '.join([child.text for child in name.getchildren() if child.tag == f'{ns}subfield' and child.attrib['code'].isalpha()])
                library = '~'.join([child.text for child in source.getchildren() if child.tag == f'{ns}sid'])
                name_source.append([person_name, library])   
            for i, elem in enumerate(name_source):
                name_source[i] = '‽'.join(name_source[i])
            name_source = '❦'.join(name_source)
            
            person = [row['search'], viaf_id, date_of_birth, date_of_death, IDs, nationality, occupation, language, name_source]
            viaf_enrichment.append(person)
    except IndexError:
        viaf_errors.append(row['search'])
            
viaf_df = pd.DataFrame(viaf_enrichment, columns=['wyszukiwana nazwa', 'viaf id', 'data urodzenia', 'data śmierci', 'IDs', 'narodowość', 'zawód', 'język', 'inne formy nazewnictwa']).drop_duplicates()

# Automatyczne wykrycie języka publikacji na podstawie tytułu

def clear_title(x):
    if x[-1] == ']':
        val = re.sub('(^.*?)([\.!?] {0,1})\[.*?$', r'\1\2', x).strip()
    elif x[0] == '[':
        val = re.sub('(^.*?\])(.*?$)', r'\2', x).strip()
    else:
        val = x
    return val

def title_lang1(x):
    try:
        val = detect_langs(re.sub('[^a-zA-ZÀ-ž\s]', '', x.lower()))
    except:
        val = ''
    return val

def title_lang2(x):
    try:
        val = TextBlob(re.sub('[^a-zA-ZÀ-ž\s]', '', x.lower())).detect_language()
    except:
        val = ''
    return val

titles = gsheet_to_df('1FoLmNZ0b-sZ5_IHPdl7JNfoJkdT3Z1N_flg6tbf91k4', 'wykrywanie języka')
titles['tytuł czysty'] = titles['tytuł'].apply(lambda x: clear_title(x))
titles['język1'] = titles['tytuł czysty'].apply(lambda x: title_lang1(x))
titles['język2'] = titles['tytuł czysty'].apply(lambda x: title_lang2(x))


