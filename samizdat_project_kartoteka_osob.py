from my_functions import gsheet_to_df, marc_parser_1_field
import pandas as pd
import sys
sys.path.insert(1, 'C:/Users/Cezary/Documents/SPUB-project')
from SPUB_query_wikidata import query_wikidata
import requests
import time
from urllib.error import HTTPError
from http.client import RemoteDisconnected
from tqdm import tqdm
import json
import regex as re
import ast
from collections import Counter
from my_functions import marc_parser_dict_for_field
import numpy as np

#%%
#wczytanie zasobów bibliograficznych:
records_dict = {}
worksheets = ['bn_books', 'cz_books', 'cz_articles', 'pbl_articles', 'pbl_books']
for worksheet in tqdm(worksheets):
    temp_df = gsheet_to_df('1y0E4mD1t4ZBN9YNmAwM2e912Q39Bb493Z6y5CSAuflw', worksheet)
    if worksheet in ['bn_books', 'cz_books', 'cz_articles']:
        temp_df.columns = [f"X{'{:03}'.format(e)}" if isinstance(e, int) else e for e in temp_df.columns.values]
    records_dict.update({worksheet: temp_df})
    
#wczytanie kartoteki osób po pracach
kartoteka_osob = pd.DataFrame()

for worksheet in ['pojedyncze ok', 'grupy_ok', 'osoby_z_jednym_wierszem', 'reszta']:
    temp_df = gsheet_to_df('1xOAccj4SK-4olYfkubvittMM8R-jwjUjVhZXi9uGqdQ', worksheet)
    temp_df = temp_df[temp_df['decyzja'].isin(['tak', 'new'])]
    kartoteka_osob = kartoteka_osob.append(temp_df)
    
sex_classification = kartoteka_osob[['sexLabel.value']].dropna().drop_duplicates().rename(columns={'sexLabel.value': 'Typeofsex'})
sex_classification.to_csv('samizdat_sex_classification_to_nodegoat.csv', index=False, encoding='utf-8') 

data_sources_classification = ['PL_BN_books', 'PL_BN_journals', 'PL_BezCenzury_books', 'PL_BezCenzury_articles', 'CZ_books', 'CZ_articles']

kolumny = list(kartoteka_osob.columns.values)

groupby = kartoteka_osob.groupby('Project_ID')
people_file = []
for name, group in tqdm(groupby, total=len(groupby)):
    # 198, 346, 936, 1947, 1883
    group = groupby.get_group('346')
    try:
        sex_label = group['sexLabel.value'].dropna().drop_duplicates().to_list()[0]
    except IndexError: sex_label = np.nan
    project_id = group['Project_ID'].dropna().drop_duplicates().to_list()[0]
    try:
        viaf_id = f"http://viaf.org/viaf/64007537{group['viaf_ID'].dropna().drop_duplicates().to_list()[0]}"
    except IndexError: viaf_id = np.nan
    wikidata_id = f"https://www.wikidata.org/wiki/{group['wikidata_ID'].dropna().drop_duplicates().to_list()[0]}"
    #konsultacja z PW
    biblio_names = group['name_form_id'].to_list()
    biblio_names = [e.split('|') for e in biblio_names]
    
    instruction = {'Index Name': ['%1', '%2', '%3', '%4', '%5', '%6'],
                   'Name': ['%1', '%4', '%6'],
                   'Given Name': ['%2'],
                   'Pseudonyms': ['%p', '%k'],
                   'True Name': ['%w'],
                   'Other Name Forms': ['%o', '%s', '%r', '%m'],
                   'Addition to Name': ['%5'],
                   'Numeration': ['%3'],
                   'Numeration (arabic)': ['%n'],
                   'Birth': ['%d'],
                   'Death': ['%d']}
    person_names_list = []
    for biblio_name_set in biblio_names:
        # biblio_name_set = biblio_names[0]
        for biblio_name in biblio_name_set:
            # biblio_name = biblio_name_set[0]
            source = biblio_name.split('-')[0]
            record_id = biblio_name.split('-')[1]
            df = records_dict[source]
            field = biblio_name.split('-')[-2]
            form_parsed = marc_parser_dict_for_field(df[df['id'] == record_id][field].to_list()[0], '%')
            use_of_instruction = instruction.copy()
            for form_dict in form_parsed:
                # form_dict = form_parsed[0]
                for key, value in form_dict.items():
                    good_keys = {k:value.strip() for k,v in instruction.items() if key in v}
                    use_of_instruction = {k:list(map(lambda x: x.replace(key, good_keys[k]), v)) if k in good_keys else v for k,v in use_of_instruction.items()}
            use_of_instruction = {k:' '.join([e for e in v if '%' not in e]) for k,v in use_of_instruction.items() if any('%' not in s for s in v)}
            if use_of_instruction:
                person_names_list.append(use_of_instruction)
    person_names_dict = {}
    for dictionary in person_names_list:
        for key, value in dictionary.items():
            if key not in person_names_dict:
                person_names_dict[key] = [value]
            else:
                person_names_dict[key].append(value)
    person_names_dict = {k:list(set(v)) for k,v in person_names_dict.items()}
    
    #dodatki z wikidaty
    try:
        wiki_pseudonym = group['pseudonym.value'].dropna().drop_duplicates().to_list()[0]
    except IndexError: wiki_pseudonym = np.nan
    wiki_occupation = [e.split('❦') for e in group['occupationLabel.value'].dropna().drop_duplicates().to_list()]
    wiki_occupation = list(set([e for sub in wiki_occupation for e in sub]))
    
    try:
        wiki_birthplace = group['birthplaceLabel.value'].dropna().drop_duplicates().to_list()[0]
    except IndexError: wiki_birthplace = np.nan
    try:
        wiki_deathplace = group['deathplaceLabel.value'].dropna().drop_duplicates().to_list()[0]
    except IndexError: wiki_deathplace = np.nan
    try:
        wiki_birthdate = group['birthdate.value'].dropna().drop_duplicates().to_list()[0]
    except IndexError: wiki_birthdate = np.nan
    try:
        wiki_deathdate = group['deathdate.value'].dropna().drop_duplicates().to_list()[0]
    except IndexError: wiki_deathdate = np.nan
    
    wiki = {'pseudonym': wiki_pseudonym,
            'occupation': wiki_occupation,
            'birthplace': wiki_birthplace,
            'deathplace': wiki_deathplace,
            'birthdate': wiki_birthdate,
            'deathdate': wiki_deathdate}

#Czemu nie pojawia się człon -Mikke?    
#Czemu Korwin się miesza???
    
    
#dodać pole Wikidata Name – label osoby z wikidaty w językach [pl, en, język ojczysty osoby] (4 kolumny: wikidata name pl, wikidta name en, wikidata mother tongue form, wikidata mother tongue)
#spiąć birthplace, deathplace z kartoteką miejsc
# jeśli w wiki birthdate i birthplace, to brać, jeśli nie, to daty z biblio
#połączyć pseudonimy z biblio i wiki
    
# POMYSŁ – dla osób bez wikidaty pobrać dodatkowe info z VIAF (ile to osób?)
    
    
                
    [dict(s) for s in set(frozenset(d.items()) for d in person_names_list)]  
    
    
    index_name = max(group['Index_Name'].to_list(), key=lambda x: len(x))
    
    temp_dict = {'Sex Label': sex_label,
                 'Project ID': project_id,
                 'VIAF ID': viaf_id,
                 'Wikidata ID': wikidata_id,
                 'Index_Name': index_name,
                 }

test = 'bn_books-83554-bnb3956-X100-%1|bn_books-83573-bnb3958-X100-%1|bn_books-83591-bnb3960-X100-%1|bn_books-83611-bnb3962-X100-%1|bn_books-83630-bnb3964-X100-%1|bn_books-83646-bnb3966-X100-%1|bn_books-83667-bnb3968-X100-%1|bn_books-83688-bnb3970-X100-%1|bn_books-83554-bnb3956-X100-%p|bn_books-83573-bnb3958-X100-%p|bn_books-83591-bnb3960-X100-%p|bn_books-83611-bnb3962-X100-%p|bn_books-83630-bnb3964-X100-%p|bn_books-83646-bnb3966-X100-%p|bn_books-83667-bnb3968-X100-%p|bn_books-83688-bnb3970-X100-%p|bn_books-83554-bnb3956-X100-%k|bn_books-83573-bnb3958-X100-%k|bn_books-83591-bnb3960-X100-%k|bn_books-83646-bnb3966-X100-%k|bn_books-83667-bnb3968-X100-%k'

test = test.split('|')
for form in test:
    form = test[0]
    source = form.split('-')[0]
    record_id = form.split('-')[1]
    df = records_dict[source]
    field = 100
    ttt = marc_parser_dict_for_field(df[df['id'] == record_id][field].to_list()[0], '%')
    
 
people_file = kartoteka_osob.copy()   
#Sex Label
people_file['Sex Label'] = people_file['sexLabel.value']
#Project ID
test = {k:v for k,v in dict(Counter(people_file['Project_ID'])).items() if v > 1}
#VIAF ID

#Wikidata ID

#Index Name

#Name

#Given Name

#Pseudonym, Kryptonym

#True Name

#Other Name Forms

#Addition to name

#Name Numeration

#Name Numeration (Arabic)

#Unknown Name Source

#DataSource - classification

#Birth

#Death
    
    
    
    
    
    
    
    
    
    
    
    
    
# test = [e.split('❦') for e in kartoteka_osob['occupationLabel.value'].dropna().to_list()]
# test = pd.DataFrame(sorted(list(set([e for sub in test for e in sub]))), columns=['occupation'])
# test.to_excel('samizdat_occupation.xlsx', index=False)
    
viafy_osob = kartoteka_osob.loc()[kartoteka_osob['autor.value'].notnull()]['viaf_ID'].drop_duplicates().to_list()

list_of_dicts = ask_wikidata_for_places(viafy_osob)