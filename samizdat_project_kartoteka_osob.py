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
from collections import defaultdict
from difflib import SequenceMatcher

#%% wczytanie zasobów bibliograficznych:
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
    
#%% klasyfikacja płci    
sex_classification = kartoteka_osob[['sexLabel.value']].dropna().drop_duplicates().rename(columns={'sexLabel.value': 'Typeofsex'})
wikidata_sex = {'mężczyzna': 'https://www.wikidata.org/wiki/Q8441',
                'kobieta': 'https://www.wikidata.org/wiki/Q467',
                'interpłciowość': 'https://www.wikidata.org/wiki/Q1097630'}
sex_classification['Wikidata_ID'] = sex_classification['Typeofsex'].apply(lambda x: wikidata_sex[x])
sex_classification.to_csv('samizdat_sex_classification_to_nodegoat.csv', index=False, encoding='utf-8') 

#%%
#klasyfikacja zawodów
# samizdat_occupation = gsheet_to_df('1-oBjrUytvx4LGSkuRJEUYDkmNx3l7sjdA0wIGTFI4Lc', 'Sheet1')
# occupations = samizdat_occupation['occupation'].to_list()

# final = {}
# url = 'https://query.wikidata.org/sparql'
# for occupation in tqdm(occupations):
#     if not re.search('Q\d+', occupation):
#         while True:
#             try:
#                 sparql_query = f"""SELECT distinct ?item ?itemLabel ?itemDescription WHERE{{  
#                   ?item ?label "{occupation}"@pl.
#                     SERVICE wikibase:label {{ bd:serviceParam wikibase:language "pl". }}    
#                 }}"""
#                 results = requests.get(url, params = {'format': 'json', 'query': sparql_query})
#                 results = results.json()
#                 try:
#                     wd_id = re.findall('Q\d+', [e['item']['value'] for e in results['results']['bindings'] if e['itemLabel']['value'] == occupation][0])[0]
#                 except IndexError:
#                     wd_id = re.findall('Q\d+', [e['item']['value'] for e in results['results']['bindings'] if 'wikidata' in e['item']['value'] and 'Q' in e['item']['value']][0])[0]
#                 sparql_query = f"""select distinct ?item ?label (lang(?label) as ?lang) where {{
#                   wd:{wd_id} rdfs:label ?label
#                   filter(lang(?label) = 'pl' || lang(?label) = 'en')
#                 }}"""  
#                 results = requests.get(url, params = {'format': 'json', 'query': sparql_query})
#                 results = results.json()     
#                 temp = [[e['label']['xml:lang'], e['label']['value']] for e in results['results']['bindings']]
#                 for el in temp:
#                     if wd_id not in final:
#                         final[wd_id] = {el[0]:el[-1]}
#                     else:
#                         final[wd_id].update({el[0]:el[-1]})
#             except ValueError:
#                 time.sleep(4)
#                 continue
#             break
#     else:
#         url2 = f'https://www.wikidata.org/wiki/Special:EntityData/{occupation}.json'
#         result = requests.get(url2).json()
#         try:
#             final[occupation] = {k:v['value'] for k,v in result['entities'][occupation]['labels'].items() if k in ['en', 'pl']}
#         except KeyError:
#             final[list(result['entities'].keys())[0]] = {k:v['value'] for k,v in result['entities'][list(result['entities'].keys())[0]]['labels'].items() if k in ['en', 'pl']}
        
# test = pd.DataFrame.from_dict(final, orient='index')     
# test.to_excel('test.xlsx')  
        
# uzupelnienia = ['Q48282', 'Q846430', 'Q7019111', 'Q33999', 'Q33999', 'Q33999', 'Q111017237', 'Q5197818', 'Q1397808', 'Q25393460', 'Q152002', 'Q36180', 'Q17167049', 'Q49757', 'Q1238570', 'Q1650915', 'Q1397808', 'Q9379869']


# final2 = {}
# for occupation in tqdm(uzupelnienia):
#     url2 = f'https://www.wikidata.org/wiki/Special:EntityData/{occupation}.json'
#     result = requests.get(url2).json()
#     try:
#         final2[occupation] = {k:v['value'] for k,v in result['entities'][occupation]['labels'].items() if k in ['en', 'pl']}
#     except KeyError:
#         final2[list(result['entities'].keys())[0]] = {k:v['value'] for k,v in result['entities'][list(result['entities'].keys())[0]]['labels'].items() if k in ['en', 'pl']}

# test2 = pd.DataFrame.from_dict(final2, orient='index')     
# test2.to_excel('test2.xlsx')  
    
occupation_classification = gsheet_to_df('1-oBjrUytvx4LGSkuRJEUYDkmNx3l7sjdA0wIGTFI4Lc', 'occupation').drop(columns=['old_label_pl', 'old_id']).drop_duplicates()
occupation_classification['Wikidata_ID'] = 'https://www.wikidata.org/wiki/' + occupation_classification['Wikidata_ID']
occupation_classification.to_csv('samizdat_occupation_classification_to_nodegoat.csv', index=False, encoding='utf-8') 

#%% tworzenie kartoteki osób
occupation_classification = gsheet_to_df('1-oBjrUytvx4LGSkuRJEUYDkmNx3l7sjdA0wIGTFI4Lc', 'occupation')

data_sources_classification = ['PL_BN_books', 'PL_BN_journals', 'PL_BezCenzury_books', 'PL_BezCenzury_articles', 'CZ_books', 'CZ_articles']

kolumny = list(kartoteka_osob.columns.values)
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
groupby = kartoteka_osob.groupby('Project_ID')

#ogarnąć pseudonimy w tabeli

#Index Name – string – longest nazwa z tabeli + sztorc lub nazwa z tabeli
#Project ID – string
#VIAF ID – external
#Wikidata ID – external
#Sex Label – classification
#Occupation – classification
#Name – string
#Given Name – string
#Pseudonym, Kryptonym – string
#True Name – string
#Other Name Forms – string
#Addition to name – string
#Name Numeration – string
#Name Numeration – string
#Name Numeration (Arabic) – number
#DataSource – classification
#sztorc

#sub-objects - located - powiązać z kartoteką miejsc


people_dict = {}
for name, group in tqdm(groupby, total=len(groupby)):
    # 198, 346, 936, 1947, 1883, 1045, 520
    name = '168'
    group = groupby.get_group(name)
    try:
        sex_label = group['sexLabel.value'].dropna().drop_duplicates().to_list()[0]
    except IndexError: sex_label = np.nan
    project_id = group['Project_ID'].dropna().drop_duplicates().to_list()[0]
    try:
        viaf_id = f"http://viaf.org/viaf/{group['viaf_ID'].dropna().drop_duplicates().to_list()[0]}"
    except IndexError: viaf_id = np.nan
    try:
        wikidata_id = f"https://www.wikidata.org/wiki/{group['wikidata_ID'].dropna().drop_duplicates().to_list()[0]}"
    except IndexError: wikidata_id = np.nan

    biblio_names = group['name_form_id'].to_list()
    try:
        biblio_names = [e.split('|') for e in biblio_names]
    
        person_names_list = []
        for biblio_name_set in biblio_names:
            # biblio_name_set = biblio_names[0]
            # biblio_name_set = biblio_names[-1]
            for biblio_name in biblio_name_set:
                # biblio_name = biblio_name_set[0]
                # biblio_name = biblio_name_set[1]
                source = biblio_name.split('-')[0]
                record_id = biblio_name.split('-')[1]
                df = records_dict[source]
                field = biblio_name.split('-')[-2]
                temp = df[df['id'] == record_id][field].to_list()[0].split('|')
                for pers in temp:
                    # pers = temp[0]
                    form_parsed = marc_parser_dict_for_field(pers, '%')
                    dd = defaultdict(list)
                    for d in form_parsed:
                        for key, value in d.items():
                            dd[key].append(value.strip())
                    form_parsed = [{e:' '.join([el.strip() for el in dd[e]]) if e!='%p' else ';'.join([el.strip() for el in dd[e]])} for e in dd]
                    use_of_instruction = instruction.copy()
                    for form_dict in form_parsed:
                        # form_dict = form_parsed[0]
                        for key, value in form_dict.items():
                            good_keys = {k:value.strip() for k,v in instruction.items() if key in v}
                            use_of_instruction = {k:list(map(lambda x: x.replace(key, good_keys[k]), v)) if k in good_keys else v for k,v in use_of_instruction.items()}
                    use_of_instruction = {k:' '.join([e for e in v if '%' not in e]) for k,v in use_of_instruction.items() if any('%' not in s for s in v)}
                    if use_of_instruction:
                        person_names_list.append(use_of_instruction)
    
            test = Counter([tuple(e.items()) for e in person_names_list])
            temp_dict = {}
            for k,v in test.items():
                k = {key:value for key, value in k}['Index Name']
                if k not in temp_dict:
                    temp_dict[k] = v
                else: temp_dict[k] += v
            if len(set(temp_dict.values())) == 1:
                proper_person = max([(e, SequenceMatcher(a=group['Index_Name'].to_list()[0],b=e).ratio()) for e in temp_dict.keys()], key=lambda x: x[-1])[0]
            else:
                proper_person = max({k: v for k,v in temp_dict.items()}, key=temp_dict.get)
            
            person_names_dict = {}
            for dictionary in person_names_list:
                if dictionary['Index Name'] == proper_person:
                    for key, value in dictionary.items():
                        if key not in person_names_dict:
                            person_names_dict[key] = [value]
                        else:
                            person_names_dict[key].append(value)
        person_names_dict = {k:list(set(v)) for k,v in person_names_dict.items()}
    except ValueError:
        person_names_dict = {}
    person_names_dict['nazwa z tabeli'] = group['Index_Name'].to_list()
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
            'deathdate': wiki_deathdate,
            'sex': sex_label,
            'wikidata_id': wikidata_id,
            'viaf_id': viaf_id}
    person_names_dict.update(wiki)
    people_dict[name] = person_names_dict
    
klucze = []
for dictionary in people_dict:
    for key in people_dict[dictionary]:
        if key not in klucze:
            klucze.append(key)
klucze = tuple(klucze)
            
for dictionary in people_dict:
    keys = tuple(people_dict[dictionary].keys())
    difference = set(klucze) - set(keys)
    for el in difference:
        people_dict[dictionary].update({el:np.nan})

#occupation for matching with classification
occupation_to_delete = gsheet_to_df('1-oBjrUytvx4LGSkuRJEUYDkmNx3l7sjdA0wIGTFI4Lc', 'Sheet1')
occupation_to_delete = occupation_to_delete[occupation_to_delete['PL'] == 'delete']['occupation'].to_list()

people_dict = {k:{ke:[e for e in va if e not in occupation_to_delete] if ke == 'occupation' else va for ke,va in v.items()} for k,v in people_dict.items()}

old_occupation = dict(zip(occupation_classification['old_label_pl'], occupation_classification['Wikidata_ID']))
old_occupation.update(dict(zip(occupation_classification['old_id'], occupation_classification['Wikidata_ID'])))
old_occupation = {k:v for k,v in old_occupation.items() if pd.notnull(k)}

people_dict = {k:{ke:[old_occupation[e] if e in old_occupation else e for e in va ] if ke == 'occupation' else va for ke,va in v.items()} for k,v in people_dict.items()}

wikidata_occupation = dict(zip(occupation_classification['name_pl'], occupation_classification['Wikidata_ID']))

people_dict = {k:{ke:[wikidata_occupation[e] if e in wikidata_occupation else e for e in va ] if ke == 'occupation' else va for ke,va in v.items()} for k,v in people_dict.items()}

people_dict = {k:{ke:[f'https://www.wikidata.org/wiki/{e}' for e in va] if ke == 'occupation' else va for ke,va in v.items()} for k,v in people_dict.items()}

#sex for matching with classification
sex = {'mężczyzna': {'sex_label_pl': 'mężczyzna',
                     'sex_label_en': 'man',
                     'Wikidata_ID': 'https://www.wikidata.org/wiki/Q8441'},
       'kobieta': {'sex_label_pl': 'kobieta',
                   'sex_label_en': 'female',
                   'Wikidata_ID': 'https://www.wikidata.org/wiki/Q467'},
       'interpłciowość': {'sex_label_pl': 'interpłciowość',
                          'sex_label_en': 'intersex',
                          'Wikidata_ID': 'https://www.wikidata.org/wiki/Q1097630'}}

people_dict = {k:{ke:sex[va]['Wikidata_ID'] if ke == 'sex' and pd.notnull(va) else va for ke,va in v.items()} for k,v in people_dict.items()}

df = pd.DataFrame.from_dict(people_dict, orient='index')
df.to_excel('samizdat_osoby_plik_testowy.xlsx')    
    
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


#%% wszystkie osoby, których nie ma na przykładzie Bieleckiego po name_form_id

etap1 = gsheet_to_df('1Y8Y_pfkuKiv5npL6QJAXWbDO6twIJqsK3ArvaokjFkU', 'samizdat')
etap1_adresy = [e.split('|') for e in etap1['name_form_id'].to_list()]
etap1_adresy = set([e for sub in etap1_adresy for e in sub])

etap2 = gsheet_to_df('1xOAccj4SK-4olYfkubvittMM8R-jwjUjVhZXi9uGqdQ', 'dane_biblio')
etap2_adresy = [e.split('|') for e in etap2['name_form_id'].to_list()]
etap2_adresy = set([e for sub in etap2_adresy for e in sub])

roznica = list(etap1_adresy - etap2_adresy)

etap1_dict = dict(zip(etap1['name_form'].to_list(), etap1['name_form_id'].to_list()))
etap1_dict = {k:v.split('|') for k,v in etap1_dict.items()}

roznica_dict = {k:v for k,v in etap1_dict.items() if any(e in roznica for e in v)}
roznica_dict = {k:'|'.join(v) for k,v in roznica_dict.items()}

roznica_df = pd.DataFrame.from_dict(roznica_dict, orient='index')
roznica_df.to_excel('test.xlsx')



roznica_dict_bez_all_pbl = {k:v for k,v in roznica_dict.items() if len(v) != len([e for e in v if 'pbl' in e])}







x = 'bn_books-27072-bnb1329-X100-%1|bn_books-27100-bnb1330-X100-%1|bn_books-27121-bnb1331-X100-%1|bn_books-27148-bnb1332-X100-%1|bn_books-27163-bnb1333-X100-%1|bn_books-27182-bnb1334-X100-%1|bn_books-27203-bnb1335-X100-%1|cz_books-2428302-czb202-X600-$$a|cz_books-2429199-czb618-X700-$$a|cz_articles-2367383-cza244-X100-$$a|cz_articles-2390770-cza248-X100-$$a|cz_articles-2167498-cza428-X600-$$a|cz_articles-2168390-cza454-X600-$$a|cz_articles-2168646-cza458-X600-$$a|cz_articles-2363295-cza629-X600-$$a|cz_articles-2363397-cza634-X600-$$a|cz_articles-2367749-cza670-X600-$$a|cz_articles-2393543-cza778-X600-$$a|cz_articles-2393570-cza783-X600-$$a|cz_articles-2421154-cza1015-X600-$$a|cz_articles-2115367-cza1164-X700-$$a|cz_articles-2389626-cza1314-X700-$$a|cz_articles-2397438-cza1334-X700-$$a|cz_articles-2404320-cza1394-X700-$$a'.split('|')
len(x)

[e for e in roznica_dict_bez_all_pbl["Havel V~'aclav|Havel, Vaclav"] if 'pbl' not in e]

















