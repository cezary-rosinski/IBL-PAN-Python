import glob
import json
import requests
from xml.etree import ElementTree
from tqdm import tqdm
from my_functions import gsheet_to_df
import regex as re
from concurrent.futures import ThreadPoolExecutor
import numpy as np
import time
import pickle
from my_functions import simplify_string
import pandas as pd
from geonames_accounts import geonames_users
import random

# bazować na danych stąd: https://docs.google.com/spreadsheets/d/1U7vjHwLI7BQ7OHtM7x7NodW0t2vqnNOqkrALuyXSWw8/edit#gid=0

#%% def

def get_bn_and_polona_response(polona_identifier):
    # file = polona_ids[0]
    # polona_identifier = 'pl-92889483'
# for polona_identifier in tqdm(polona_ids[30:40]):
    try:
        polona_id = re.findall('\d+', polona_identifier)[0]
        query = f'https://polona.pl/api/entities/{polona_id}'
        polona_response = requests.get(query).json()
        bn_id = polona_response['semantic_relation'].split('/')[-1]
        query = f'http://khw.data.bn.org.pl/api/polona-lod/{bn_id}'
        polona_lod = requests.get(query).json()
        query = f'https://data.bn.org.pl/api/institutions/bibs.json?id={bn_id}'
        try:
            try:
                bn_response = requests.get(query).json()['bibs'][0]  
            except IndexError:
                query = f'https://data.bn.org.pl/api/institutions/bibs.json?id={bn_id[:-1]}'
                bn_response = requests.get(query).json()['bibs'][0]
            temp_dict = {polona_identifier:{'polona_id':polona_id,
                                            'bn_id':bn_id,
                                            'polona_response':polona_response,
                                            'polona_lod':polona_lod,
                                            'bn_response':bn_response}}
            bn_dict.update(temp_dict)
        except IndexError:
            errors.append(polona_identifier)
    except (ValueError, KeyError, AttributeError):
        errors.append(polona_identifier)
    # except ValueError:
    #     time.sleep(2)
    #     get_bn_and_polona_response(polona_identifier)

#%% load
novels_df = gsheet_to_df('1U7vjHwLI7BQ7OHtM7x7NodW0t2vqnNOqkrALuyXSWw8', 'Arkusz1')

with open('miasto_wieś_bn_dict.pickle', 'rb') as handle:
    bn_dict = pickle.load(handle)

with open('miasto_wieś_bn_errors.txt', 'rt', encoding='utf-8') as f:
    errors = f.read()    
  
country_codes = pd.read_excel('translation_country_codes.xlsx')
country_codes = [list(e[-1]) for e in country_codes.iterrows()]
country_codes = dict(zip([e[0] for e in country_codes], [{'MARC_name': e[1], 'iso_alpha_2': e[2], 'Geonames_name': e[-1]} for e in country_codes]))

#%% main

polona_ids = [e for e in novels_df['id'].to_list() if not(isinstance(e, float))]
# errors = []
errors = [e for e in polona_ids if not e.startswith('pl')]
polona_ids = [e for e in polona_ids if e not in errors]

bn_dict = {}

with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(get_bn_and_polona_response, polona_ids),total=len(polona_ids)))

with open('miasto_wieś_bn_dict.pickle', 'wb') as handle:
    pickle.dump(bn_dict, handle, protocol=pickle.HIGHEST_PROTOCOL)

with open('miasto_wieś_bn_errors.txt', 'w', encoding='utf-8') as f:
    for e in errors:
        f.write(f'{e}\n')
        

#podtytuły
#linki do polony
#wikidata
#geonames
#wikidata płeć


test = bn_dict[list(bn_dict.keys())[200]]

new_dict = {}
for d in tqdm(bn_dict):
    # d = list(bn_dict.keys())[0]
    polona_id = re.findall('\d+', d)[0]
    polona_url = f'polona.pl/item/{polona_id}'
    
    try:
        author_wikidata = bn_dict[d]['polona_lod']['Twórca/współtwórca'][list(bn_dict[d]['polona_lod']['Twórca/współtwórca'].keys())[0]]['Identyfikator Wikidata (URI)']['link']
    except KeyError:
        author_wikidata = np.nan

    try:
        subtitle = [el for el in [e for e in bn_dict[d]['bn_response']['marc']['fields'] if '245' in e][0]['245']['subfields'] if 'b' in el][0]['b']
    except IndexError:
        subtitle = np.nan

    place = [el for el in [e for e in bn_dict[d]['bn_response']['marc']['fields'] if '260' in e][0]['260']['subfields'] if 'a' in el][0]['a']
    place = simplify_string(place, nodiacritics=False).strip()

    country_code = [e for e in bn_dict[d]['bn_response']['marc']['fields'] if '008' in e][0]['008'][15:18].strip()
    
    temp_dict = {d:{'author_wikidata': author_wikidata,
                    'subtitle': subtitle,
                    'place_of_publication': place,
                    'country_code': country_code,
                    'polona_url': polona_url}}
    new_dict.update(temp_dict)

df = pd.DataFrame.from_dict(new_dict, orient='index').reset_index()[['index', 'author_wikidata', 'polona_url', 'subtitle']]
df.to_excel('test.xlsx', index=False)


#tutaj dać tylko set miejscowości

#płeć z wikidaty
#geonames id
#miejsce urodzenia - miasto + wikidata ID + koordynaty






while True:
    url = 'http://api.geonames.org/searchJSON?'
    q_country = country_codes[country_code.lower()]['iso_alpha_2']
    params = {'username': random.choice(geonames_users), 'q': place, 'featureClass': 'P', 'country': q_country}
    # params = {'username': geoname_users[users_index], 'q': place, 'featureClass': 'P', 'fuzzy': 0.4}
    result = requests.get(url, params=params).json()
    if 'status' in result:
        users_index += 1
        continue
    if result['totalResultsCount'] > 0:  
        try:
            country_from_dict = {v['geonames_country'] for k,v in places_dict.items() if k == place}.pop()
            if pd.notnull(country_from_dict):
                best_selection = [e for e in result['geonames'] if e['countryName'] == country_from_dict]
            else: 
                best_selection = [e for e in result['geonames'] if e['countryName'] == {v['country'] for k,v in places_dict.items() if k == place}.pop()]
            places_geonames.update({place: max(best_selection, key=lambda x: x['population'])})
        except ValueError:
            try:
                best_selection = [e for e in result['geonames'] if e['adminName1'] == {v['country'] for k,v in places_dict.items() if k == place}.pop()]
                places_geonames.update({place: max(best_selection, key=lambda x: x['population'])})
            
            except ValueError:
                # places_geonames.update({place: max(result['geonames'], key=lambda x: x['population'])})
                places_geonames.update({place: 'No geonames response'})
        
        #nie mogę brać max population, bo dla amsterdamu to jest Nowy Jork
        #trzeba brać 1. sugestię geonames
        # places_geonames.update({place: max(result['geonames'], key=lambda x: x['population'])})
        #!!!!!tutaj dodać klucz państwa!!!!!!!
        # places_geonames.update({place: result['geonames'][0]})
    else: places_geonames.update({place: 'No geonames response'})
    break
















def get_viaf_name(viaf_url):
    url = viaf_url + '/viaf.json'
    r = requests.get(url)
    try:
        if r.json().get('mainHeadings'):
            if isinstance(r.json()['mainHeadings']['data'], list):
                name = r.json()['mainHeadings']['data'][0]['text']
            else:
                name = r.json()['mainHeadings']['data']['text']
            viaf_names_resp[viaf_url] = name
        elif r.json().get('redirect'):
            new_viaf = r.json()['redirect']['directto']
            new_url = 'https://www.viaf.org/viaf/' + new_viaf
            viaf_names_resp[viaf_url] = new_url
            get_viaf_name(new_url)
    except KeyboardInterrupt as exc:
        raise exc
    except:
        raise print(url)

viaf_names_resp = {}
viaf_url_set = set(df['related_viaf'])

with ThreadPoolExecutor(max_workers=50) as excecutor:
    list(tqdm(excecutor.map(get_viaf_name, viaf_url_set)))












    dict_data.update({'katalog_bn_info': response})
    
    
    # file = files[0]
    with open(file, encoding='utf-8') as d:
        dict_data = json.load(d)
        try:
            pl_id = dict_data['pl_record_no']
        except KeyError:
            query = f'https://polona.pl/api/entities/{dict_data["pl_id"]}'
            pl_id = requests.get(query).json()
            
        query = f'http://khw.data.bn.org.pl/api/polona-lod/{pl_id}'
        # query = f'http://khw.data.bn.org.pl/api/nlp_id/bibs?id=b1935990{pl_id}'
        # response = requests.get(query)
        # tree = ElementTree.fromstring(response.content)
        # for e in tree:
        #     print(e)
        response = requests.get(query).json()
        dict_data.update({'polona_lod_info': response})
        query = f'https://data.bn.org.pl/api/institutions/bibs.json?id={pl_id}'
        response = requests.get(query).json()
        dict_data.update({'katalog_bn_info': response})
        bn_dict[dict_data['pl_id']] = dict_data
        
        
    