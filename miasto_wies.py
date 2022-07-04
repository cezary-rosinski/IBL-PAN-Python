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
import viapy
from ast import literal_eval

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
def get_wikidata_info_for_person(el):
    # el = list(wikidata_ids)[0]
    wikidata_id = re.findall('Q\d+', el)[0]
    url = f"https://www.wikidata.org/wiki/Special:EntityData/{wikidata_id}.json"
    result = requests.get(url).json()

    try:
        birthplace_id = result['entities'][wikidata_id]['claims']['P19'][0]['mainsnak']['datavalue']['value']['id']
        url_temp = f"https://www.wikidata.org/wiki/Special:EntityData/{birthplace_id}.json"
        birthplace_result = requests.get(url_temp).json()
        try:
            birthplace = birthplace_result['entities'][birthplace_id]['labels']['pl']['value']
        except KeyError:
            birthplace = birthplace_result['entities'][birthplace_id]['labels']['en']['value']
        try:
            birthplace_lat = birthplace_result['entities'][birthplace_id]['claims']['P625'][0]['mainsnak']['datavalue']['value']['latitude']
            birthplace_lon = birthplace_result['entities'][birthplace_id]['claims']['P625'][0]['mainsnak']['datavalue']['value']['longitude']
        except KeyError:
            birthplace_lat = np.nan
            birthplace_lon = np.nan
    except KeyError:
        birthplace_id = np.nan
        birthplace = np.nan
        birthplace_lat = np.nan
        birthplace_lon = np.nan
    try:
        sex = result['entities'][wikidata_id]['claims']['P21'][0]['mainsnak']['datavalue']['value']['id']
        url_temp = f"https://www.wikidata.org/wiki/Special:EntityData/{sex}.json"
        sex_result = requests.get(url_temp).json()
        sex = sex_result['entities'][sex]['labels']['pl']['value']
    except KeyError:
        sex = np.nan
    temp_dict = {'birthplace': birthplace,
                 'birthplace_id': birthplace_id,
                 'birthplace_latitude': birthplace_lat,
                 'birthplace_longitude': birthplace_lon,
                 'sex': sex}    
    wikidata_response.update({el:temp_dict})

def get_missing_person(missing_person):
# for missing_person in tqdm(missing_wikidata):
    # missing_person = 'Adamówna, Małgorzata'
    url = re.sub('\s+', '%20', f"http://viaf.org/viaf/search?query=local.personalNames%20all%20%22{missing_person}%22&sortKeys=holdingscount&httpAccept=application/json")
    try:
        viaf_response = requests.get(url).json()
        if isinstance(viaf_response['searchRetrieveResponse']['records'][0]['record']['recordData']['sources']['source'], list):
            wikidata_id = [e for e in viaf_response['searchRetrieveResponse']['records'][0]['record']['recordData']['sources']['source'] if 'WKP' in e['#text']][0]['@nsid']
        else:
            if 'WKP' in viaf_response['searchRetrieveResponse']['records'][0]['record']['recordData']['sources']['source']['#text']:
                viaf_response['searchRetrieveResponse']['records'][0]['record']['recordData']['sources']['source']['@nsid']
            else: wikidata_id = np.nan         
    except (ValueError, IndexError):
        wikidata_id = np.nan
    except (KeyError, TypeError):
        wikidata_id = np.nan
        print(missing_person)
    missing_people_dict.update({missing_person:wikidata_id})
    
def harvest_first_edition(author, title):
    # author, title = novels_pl_list[0]
    # author, title = "Sienkiewicz, Henryk", "Ogniem i mieczem"
    url = 'https://data.bn.org.pl/api/networks/bibs.json?'
    params = {'author': author, 'title': title, 'limit': 100}
    result = requests.get(url, params = params).json()
    #next pages
    bibs = result['bibs']
    while result['nextPage'] != '':
        result = requests.get(result['nextPage']).json()
        if result['bibs']:
            bibs.extend(result['bibs'])
    try:
        bibs = min(bibs, key=lambda item: int(item['publicationYear']))
        publication_year = bibs['publicationYear']
        publication_place = bibs['placeOfPublication'].split(' : ')[0]
        publication_country = bibs['placeOfPublication'].split(' : ')[-1]
    except ValueError:
        publication_year = publication_place = publication_country = np.nan
    novels_pl_dict[(author, title)] = {'year': publication_year,
                                       'place': publication_place,
                                       'country': publication_country}  
    
def harvest_geonames(place):
    fuzzy = 1
    # place = 'Wisła'
    url = 'http://api.geonames.org/searchJSON?'
    params = {'username': random.choice(geonames_users), 'q': place, 'featureClass': 'P'}
    result = requests.get(url, params=params).json()
    try:
        while result['totalResultsCount'] == 0:
            fuzzy -= 0.1
            params = {'username': random.choice(geonames_users), 'q': place, 'featureClass': 'P', 'fuzzy': fuzzy}
            result = requests.get(url, params=params).json()
        temp_resp = {k:v for k,v in max(result['geonames'], key=lambda x: x['population']).items() if k in ['lng', 'lat', 'name', 'geonameId']}
    except KeyError:
        temp_resp = {'lng': np.nan,
                     'geonameId': np.nan,
                     'name': np.nan,
                     'lat': np.nan}
    places_dict[place] = temp_resp  
#%% load
novels_df = gsheet_to_df('1U7vjHwLI7BQ7OHtM7x7NodW0t2vqnNOqkrALuyXSWw8', 'Arkusz1')

with open('miasto_wieś_bn_dict.pickle', 'rb') as handle:
    bn_dict = pickle.load(handle)

with open('miasto_wieś_bn_errors.txt', 'rt', encoding='utf-8') as f:
    errors = f.read().splitlines() 
  
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

# with open('miasto_wieś_bn_dict.pickle', 'wb') as handle:
#     pickle.dump(bn_dict, handle, protocol=pickle.HIGHEST_PROTOCOL)

# with open('miasto_wieś_bn_errors.txt', 'w', encoding='utf-8') as f:
#     for e in errors:
#         f.write(f'{e}\n')
        

#podtytuły
#linki do polony
#wikidata
#geonames
#wikidata płeć

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

wikidata_ids = set([e for e in novels_df['creator_wikidata'].to_list() if not(isinstance(e, float))])

# for el in tqdm(wikidata_ids):


wikidata_response = {}        
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(get_wikidata_info_for_person, wikidata_ids),total=len(wikidata_ids)))        
 
df = pd.DataFrame.from_dict(wikidata_response, orient='index')

#wikidata uzupełnienie

with open('missing_wikidata.txt', 'rt', encoding='utf-8') as f:
    missing_wikidata = f.read().splitlines()
    
# missing_person = missing_wikidata[0]


missing_people_dict = {}
with ThreadPoolExecutor() as executor:
    list(tqdm(executor.map(get_missing_person, missing_wikidata),total=len(missing_wikidata)))

missing_people_dict = {k:v for k,v in missing_people_dict.items() if not(isinstance(v, float))}
df = pd.DataFrame.from_dict(missing_people_dict, orient='index')
missing_people = set(missing_people_dict.values())

wikidata_response = {}        
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(get_wikidata_info_for_person, missing_people),total=len(missing_people)))  
df = pd.DataFrame.from_dict(wikidata_response, orient='index')
 
#geonames

places_dict = {k:{ka:va for ka,va in v.items() if ka in ['country_code', 'place_of_publication']} for k,v in new_dict.items()} 
czy_pl = {k:v for k,v in dict(zip(novels_df['id'], novels_df['PL/nie-PL'])).items() if v == 'PL'}

places_dict = {k:v for k,v in places_dict.items() if k in czy_pl}

places_set = set(zip([places_dict[e]['place_of_publication'] for e in places_dict], [places_dict[e]['country_code'] for e in places_dict]))    
 

test = [e for e in places_set if e[-1] == 'lt'][0]
list(places_dict.keys())[list(places_dict.values()).index({'place_of_publication':test[0], 'country_code':test[1]})]
list(places_dict.keys())[list(places_dict.values()).index({'place_of_publication':'bruxella', 'country_code':'be'})]
country_codes['lt']['iso_alpha_2']


#ta funkcja jest skopana
def get_geonames_response(place, country_code, count=0):
    # place_and_country_code = ('bruxella', 'be')
    # place = 'bruxella'
    # country_code = 'be'
    while True:
        url = 'http://api.geonames.org/searchJSON?'
        try:
            q_country = country_codes[country_code.lower()]['iso_alpha_2']
            params = {'username': random.choice(geonames_users), 'q': place, 'featureClass': 'P', 'country': q_country}
        except (AttributeError, KeyError):
            params = {'username': random.choice(geonames_users), 'q': place, 'featureClass': 'P', 'fuzzy': 0.8}
        # params = {'username': geoname_users[users_index], 'q': place, 'featureClass': 'P', 'fuzzy': 0.4}
        result = requests.get(url, params=params).json()
        if 'status' in result or result['totalResultsCount'] == 0:
            count += 1
            if count == 2:
                geonames_resp.update({(place, country_code): 'No geonames response'})
                count = 0
                break
            country_code = np.nan
            get_geonames_response(place, country_code)
        if result['totalResultsCount'] > 0:  
            geonames_resp.update({(place, country_code): {k:v for k,v in max(result['geonames'], key=lambda x: x['population']).items() if k in ['lng', 'lat', 'name', 'geonameId']}})
        else:
            geonames_resp.update({(place, country_code): 'No geonames response'})
        break    

# get_geonames_response(*('bruxella', 'be'))
geonames_resp = {}        
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(lambda p: get_geonames_response(*p), places_set),total=len(places_set)))  

 
len([e for e in geonames_resp.values() if e == 'No geonames response'])
# geonames do poprawy

#uzupełnienie

#grupowanie i wybór pierwowzoru

novels_pl_list = gsheet_to_df('1U7vjHwLI7BQ7OHtM7x7NodW0t2vqnNOqkrALuyXSWw8', 'uniq_novels_pl')[['creator', 'title']].values.tolist()
        
novels_pl_dict = {}    
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(lambda p: harvest_first_edition(*p), novels_pl_list),total=len(novels_pl_list)))          
        
places = {v['place'] for k,v in novels_pl_dict.items()}
places = [re.split(',|;', e.strip()) for e in places if not(isinstance(e,float))]
places = set([e.replace('[etc.]', '').replace('[','').replace(']','').replace('etc.','').strip() for sub in places for e in sub])
places = set(simplify_string(e, nodiacritics=False) for e in places)

places_dict = {}       
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(harvest_geonames, places),total=len(places))) 
    
for key in tqdm(novels_pl_dict):
    test = [places_dict[e] for e in places_dict if not(isinstance(novels_pl_dict[key]['place'], float)) and e in novels_pl_dict[key]['place'].lower()]
    novels_pl_dict[key].update({'geonames':test})

df = pd.DataFrame.from_dict(novels_pl_dict, orient='index').reset_index()

#%% statystyki pierwodruków

pierwodruki = gsheet_to_df('1U7vjHwLI7BQ7OHtM7x7NodW0t2vqnNOqkrALuyXSWw8', 'pierwodruki BN')
korpus = gsheet_to_df('1U7vjHwLI7BQ7OHtM7x7NodW0t2vqnNOqkrALuyXSWw8', 'korpus roboczy')
korpus = korpus[(korpus['list'].isin(['novels','eltec'])) &
                (korpus['PL/nie-PL'] == 'PL') &
                (korpus['czas akcji: przed/po 1815'].isin(['po', 'na granicy', 'przed i po'])) &
                (korpus['do usunięcia'].isna())]
korpus['simplify'] = korpus['Unnamed: 0'].apply(lambda x: simplify_string(x).strip())
pierwodruki['simplify'] = pierwodruki['Unnamed: 4'].apply(lambda x: simplify_string(x).strip())
#

#liczba tokenów
# id najnowszej i liczba tokenów dla najnowszego id


pierwodruki['num_tokens'] = ''
pierwodruki['num_tokens_id'] = ''
errors = []
for i, row in tqdm(pierwodruki.iterrows(), total=pierwodruki.shape[0]):
    try:
        # clean_name = simplify_string(row['Unnamed: 4']).strip()
        clean_name = row['simplify']
        test_df = korpus[korpus['simplify'] == clean_name]
        num_tokens = max([int(e) for e in test_df['num_tokens'].to_list()])
        num_tokens_id = test_df[test_df['num_tokens'] == str(num_tokens)]['id'].to_list()[0]
        pierwodruki.at[i,'num_tokens'] = num_tokens
        pierwodruki.at[i,'num_tokens_id'] = num_tokens_id
    except:
        errors.append(row)

#zabory

zabory = gsheet_to_df('1U7vjHwLI7BQ7OHtM7x7NodW0t2vqnNOqkrALuyXSWw8', 'zabory')
zabory_dict = dict(zip(zabory['geonameId'], zabory['zabór']))

pierwodruki['zabory'] = ''
for i, row in tqdm(pierwodruki.iterrows(), total=pierwodruki.shape[0]):
    # i = 0
    # row = pierwodruki.iloc[i,:]
    geonames = literal_eval(row['geonames'])
    pierwodruki.at[i,'zabory'] = [zabory_dict[str(e['geonameId'])] for e in geonames]
    
    















    
    
    














           
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




# w tabeli uzupełnić wikidatę dla autora przy wszystkich wystąpieniach osoby











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
        
        
    