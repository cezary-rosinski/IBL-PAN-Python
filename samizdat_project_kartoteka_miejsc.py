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

#%% def
def ask_wikidata_for_places(list_of_viafs):
    list_of_dicts = []
    url = 'https://query.wikidata.org/sparql' 
    for viaf in tqdm(list_of_viafs):
        while True:
            try:
                sparql_query = f"""PREFIX wdt: <http://www.wikidata.org/prop/direct/>
                SELECT distinct ?author ?birthplace ?deathplace WHERE {{
                  ?author wdt:P214 "{viaf}" ;
                  optional {{ ?author wdt:P19 ?birthplace . }}
                  optional {{ ?author wdt:P20 ?deathplace . }}
                SERVICE wikibase:label {{ bd:serviceParam wikibase:language "pl". }}}}"""    
                results = requests.get(url, params = {'format': 'json', 'query': sparql_query})
                results = results.json()
                results_df = pd.json_normalize(results['results']['bindings'])
                columns = [e for e in results_df.columns.tolist() if 'value' in e]
                results_df = results_df[results_df.columns.intersection(columns)]       
                for column in results_df.drop(columns='author.value'):
                    if 'value' in column:
                        results_df[column] = results_df.groupby('author.value')[column].transform(lambda x: '❦'.join(x.drop_duplicates().astype(str)))
                results_df = results_df.drop_duplicates().reset_index(drop=True)   
                result = results_df.to_dict('records')
                list_of_dicts.append(result[0])
                time.sleep(2)
            except (AttributeError, KeyError, ValueError):
                time.sleep(2)
            except (HTTPError, RemoteDisconnected) as error:
                print(error)# time.sleep(61)
                time.sleep(5)
                continue
            break
    return list_of_dicts

#%% miejsca z haseł osobowych
kartoteka_osob = pd.DataFrame()

for worksheet in ['pojedyncze ok', 'grupy_ok', 'osoby_z_jednym_wierszem', 'reszta']:
    temp_df = gsheet_to_df('1xOAccj4SK-4olYfkubvittMM8R-jwjUjVhZXi9uGqdQ', worksheet)
    kartoteka_osob = kartoteka_osob.append(temp_df)
    
viafy_osob = kartoteka_osob.loc()[kartoteka_osob['autor.value'].notnull()]['viaf_ID'].drop_duplicates().to_list()

list_of_dicts = ask_wikidata_for_places(viafy_osob)

with open('samizdat_kartoteka_osob_viaf.json', 'w', encoding='utf-8') as f:
    json.dump(list_of_dicts, f)
    
#%% wikidata query dla miejsc z haseł osobowych
    
with open('samizdat_kartoteka_osob_viaf.json') as json_file:
    places_from_wikidata = json.load(json_file)
    
places_wikidata = [{v for k,v in e.items() if k in ['birthplace.value', 'deathplace.value']} for e in places_from_wikidata]
places_wikidata = [e.split('❦') for sub in places_wikidata for e in sub if e]
places_wikidata = list(set([e for sub in places_wikidata for e in sub if 'Q' in e]))[:10]

languages = ['pl', 'en', 'fr', 'de', 'es', 'cs']

wikidata_places_dict = {}
for element in tqdm(places_wikidata):
    # element = 'http://www.wikidata.org/entity/Q3286190'
    wikidata_id = re.findall('Q.+$', element)[0]
    url = f'https://www.wikidata.org/wiki/Special:EntityData/{wikidata_id}.json'
    result = requests.get(url).json()
    for lang in languages:
        try:
            label = result['entities'][wikidata_id]['labels'][lang]['value']
            break
        except KeyError:
            pass
    try:
        geonames_id = [e['mainsnak']['datavalue']['value'] for e in result['entities'][wikidata_id]['claims']['P1566']]
    except KeyError: geonames_id = None
    try:
        country_id = [e['mainsnak']['datavalue']['value']['id'] for e in result['entities'][wikidata_id]['claims']['P17'] if e['rank'] == 'preferred'][0]
    except IndexError:
        country_id = [e['mainsnak']['datavalue']['value']['id'] for e in result['entities'][wikidata_id]['claims']['P17']][0]
    url = f'https://www.wikidata.org/wiki/Special:EntityData/{country_id}.json'
    result_country = requests.get(url).json()
    for lang2 in languages:
        country_label = result_country['entities'][country_id]['labels'][lang2]['value']
        try:
            country_code = [e['mainsnak']['datavalue']['value'] for e in result_country['entities'][country_id]['claims']['P297']][0]
        except KeyError: country_coe = None
        break
    coordinates = [(e['mainsnak']['datavalue']['value']['latitude'], e['mainsnak']['datavalue']['value']['longitude']) for e in result['entities'][wikidata_id]['claims']['P625']][0]
    temp_dict = {element: {'wikidata_id': wikidata_id,
                           'label': label,
                           'label_lang': lang,
                           'geonames_id': geonames_id,
                           'country_id': country_id,
                           'country_label': country_label,
                           'country_lang': lang2,
                           'country_code': country_code,
                           'coordinates': coordinates}}
    wikidata_places_dict.update(temp_dict)

df = pd.DataFrame.from_dict(wikidata_places_dict, orient='index')    
#nazwa geonamesID, coordinates, country


#%% miejsca z rekordów bibliograficznych
rekordy_bn = gsheet_to_df('1y0E4mD1t4ZBN9YNmAwM2e912Q39Bb493Z6y5CSAuflw', 'Kopia arkusza bn_books')

# Pola 110, 710, 711, 120 -  w każdym z tych pól mogą wystąpić w podpolach 6 i 7.
# Pole 120  - podpole 8.
# Pole 210 - podpola A i E.
# Pole 600 - podpola G i C.

geo_w_biblio = [[110, '%6'], 
                [110, '%7'], 
                [120, '%8'], 
                [210, '%a'], 
                [210, '%e']]

places_from_bibliographical_records = pd.DataFrame()
for field, subfield in tqdm(geo_w_biblio):
    temp_df = marc_parser_1_field(rekordy_bn, 'id', field, '%')[['id', subfield]]
    temp_df = temp_df[temp_df[subfield] != '']
    temp_df['in_records'] = temp_df.apply(lambda x: f"{x['id']}-{field}-{subfield}", axis=1)
    temp_df['in_records'] = temp_df.groupby(subfield)['in_records'].transform(lambda x: '|'.join(x.str.strip()))
    temp_df = temp_df[[subfield, 'in_records']].drop_duplicates().reset_index(drop=True).rename(columns={subfield:'name'})
    places_from_bibliographical_records = places_from_bibliographical_records.append(temp_df)
    
places_from_bibliographical_records['in_records'] = places_from_bibliographical_records.groupby('name')['in_records'].transform(lambda x: '|'.join(x.str.strip()))
places_from_bibliographical_records = places_from_bibliographical_records.drop_duplicates().reset_index(drop=True)
    
places_from_bibliographical_records.to_excel('samizdat_miejsca_z_rekordów.xlsx', index=False)













































