from tqdm import tqdm
import requests
import pandas as pd
import time
from urllib.error import HTTPError
from http.client import RemoteDisconnected
import regex as re
from difflib import SequenceMatcher
from my_functions import simplify_string, marc_parser_dict_for_field, cluster_strings
import numpy as np
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
import gspread as gs
from google_drive_research_folders import PBL_folder
from gspread_dataframe import get_as_dataframe, set_with_dataframe
import datetime
import cx_Oracle
import pickle
import sys
sys.path.insert(1, 'C:/Users/Cezary/Documents/SPUB-project')
from geonames_accounts import geoname_users
from collections import Counter
from unidecode import unidecode
from Levenshtein import ratio
from ast import literal_eval
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor 
import random

#%% date
now = datetime.datetime.now().date()
year = now.year
month = '{:02}'.format(now.month)
day = '{:02}'.format(now.day)

#%% def
def query_wikidata_for_iso(dictionary, property_code):
    # 'P297' or 'P300'
    url = 'https://query.wikidata.org/sparql' 
    for k in tqdm(dictionary):
        while True:
            try:
                sparql_query = f"""select ?iso_alpha_2 where {{
                ?country wdt:P4801 "countries/{k}" .
                ?country wdt:{property_code} ?iso_alpha_2 . }}"""    
                results = requests.get(url, params = {'format': 'json', 'query': sparql_query})
                results = results.json()
                iso_code = [e['iso_alpha_2']['value'] for e in results['results']['bindings']]
                dictionary[k]['iso_alpha_2'] = iso_code
                time.sleep(2)
            except (AttributeError, KeyError, ValueError):
                time.sleep(2)
            except (HTTPError, RemoteDisconnected) as error:
                print(error)# time.sleep(61)
                time.sleep(5)
                continue
            break
    return dictionary

def get_most_frequent_country(x):
    try:
        return Counter({key:val for key,val in dict(Counter(x)).items() if key not in ['No place, unknown, or undetermined', 'unknown'] and pd.notnull(key)}).most_common(1)[0][0]
    except IndexError:
        return 'unknown'
    
def flatten(A):
    rt = []
    for i in A:
        if isinstance(i,tuple): rt.extend(flatten(i))
        else: rt.append(i)
    return rt

def simplify_place_name(x):
    return ''.join([unidecode(e).lower() for e in re.findall('[\p{L}- ]',  x)]).strip()

def reharvest_geonames(ov_list):
    old, city, country = ov_list
# places_geonames_extra_2 = {}
# errors = []
# for old, city, country in tqdm(ov_geonames):
    # old, city, country = ov_geonames[264]
    if pd.notnull(city) and city in places_geonames and isinstance(places_geonames[city], dict):
        places_geonames_extra_2[old] = places_geonames[city]
    elif all(pd.notnull(e) for e in [city, country]): 
        try:
            url = 'http://api.geonames.org/searchJSON?'
            q_country = ccodes[country.lower()]
            params = {'username': 'crosinski', 'q': city, 'featureClass': 'P', 'country': q_country}
            result = requests.get(url, params=params).json()
            places_geonames_extra_2.update({old: max(result['geonames'], key=lambda x: x['population'])})
        except (KeyError, ValueError):
            errors.append((old, city, country))
    elif pd.isnull(city):
        try:
            url = 'http://api.geonames.org/searchJSON?'
            q_country = ccodes[country.lower()]
            params = {'username': 'crosinski', 'q': old, 'featureClass': 'P', 'country': q_country}
            result = requests.get(url, params=params).json()
            places_geonames_extra_2.update({old: max(result['geonames'], key=lambda x: x['population'])})
        except (KeyError, ValueError):
            errors.append((old, city, country))
    elif pd.isnull(country):
        try:
            url = 'http://api.geonames.org/searchJSON?'
            params = {'username': 'crosinski', 'q': city, 'featureClass': 'P'}
            result = requests.get(url, params=params).json()
            places_geonames_extra_2.update({old: max(result['geonames'], key=lambda x: x['population'])})
        except (KeyError, ValueError):
            errors.append((old, city, country))
    else: errors.append((old, city, country)) 
#%% geonames harvesting
translations_df = pd.read_excel('translation_deduplicated_2022-03-28.xlsx', sheet_name='phase_4')
# with open('MARC21_country_codes.txt', ) as f:
#     country_codes = f.read().splitlines()

# country_codes = [e.split('\t') for e in country_codes]
# country_codes = dict(zip([e[0] for e in country_codes], [{'MARC_name': e[-1]} for e in country_codes]))
# test = query_wikidata_for_iso(country_codes, 'P300')
# df = pd.DataFrame.from_dict(test, orient='index')
# df['iso_alpha_2'] = df['iso_alpha_2'].apply(lambda x: x[0] if x else np.nan)
# df.to_excel('translation_country_codes.xlsx')

country_codes = pd.read_excel('translation_country_codes.xlsx')

# re_harvesting = country_codes[country_codes['iso_alpha_2'].isnull()][['MARC_code', 'MARC_name']]
# re_harvesting = dict(zip([e for e in re_harvesting['MARC_code'].to_list()], [{'MARC_name': e} for e in re_harvesting['MARC_name'].to_list()]))

# re_harvesting = query_wikidata_for_iso(re_harvesting, 'P300')
# df = pd.DataFrame.from_dict(re_harvesting, orient='index')
# df['iso_alpha_2'] = df['iso_alpha_2'].apply(lambda x: x[0] if x else np.nan)
# df.to_excel('translation_country_codes_2.xlsx')

country_codes = [list(e[-1]) for e in country_codes.iterrows()]
country_codes = dict(zip([e[0] for e in country_codes], [{'MARC_name': e[1], 'iso_alpha_2': e[2], 'Geonames_name': e[-1]} for e in country_codes]))

places = translations_df[['001', 'author_id', '008', '260']]
# places = places[places.index.isin([1, 2, 72, 155, 262, 445, 667, 1437, 2298, 4402, 13258])]

places['country'] = places['008'].apply(lambda x: x[15:18])
places.drop(columns='008', inplace=True)
places['country'] = places['country'].str.replace('\\', '', regex=False)
places['country_name'] = places['country'].apply(lambda x: country_codes[x]['MARC_name'] if x in country_codes else 'unknown')
places['geonames_name'] = places['country'].apply(lambda x: country_codes[x]['Geonames_name'] if x in country_codes else 'unknown')
#155, 13258
# places['places'] = places['260'].str.replace(' - ', '$a')
places['places'] = places['260'].apply(lambda x: [list(e.values())[0] for e in marc_parser_dict_for_field(x, '\$') if '$a' in e] if not(isinstance( x, float)) else x)
places['places'] = places['places'].apply(lambda x: ''.join([f'$a{e}' for e in x]) if not(isinstance(x, float)) else np.nan)

places['places'] = places['places'].apply(lambda x: re.sub('( : )(?!\$)', r'$b', x) if pd.notnull(x) else x)
places['places'] = places['places'].apply(lambda x: re.sub('( ; )(?!\$)', r'$a', x) if pd.notnull(x) else x)
places['places'] = places['places'].apply(lambda x: re.sub('\d', r'$a', x) if pd.notnull(x) else x)
places['places'] = places['places'].apply(lambda x: re.sub(' - ', r'$a', x) if pd.notnull(x) else x)
places['places'] = places['places'].apply(lambda x: re.sub(' \& ', r'$a', x) if pd.notnull(x) else x)
places['places'] = places['places'].apply(lambda x: re.sub(', ', r'$a', x) if pd.notnull(x) else x)
places['places'] = places['places'].apply(lambda x: re.sub('\(', r'$a', x) if pd.notnull(x) else x)
places['places'] = places['places'].apply(lambda x: re.sub('\[', r'$a', x) if pd.notnull(x) else x)
places['places'] = places['places'].apply(lambda x: re.sub('\/', r'$a', x) if pd.notnull(x) else x)

places['places'] = places['places'].apply(lambda x: [list(e.values())[0] for e in marc_parser_dict_for_field(x, '\$') if '$a' in e] if pd.notnull(x) else x)
places['places'] = places['places'].apply(lambda x: x if x else np.nan)

places['simple'] = places['places'].apply(lambda x: [simplify_place_name(e).strip() for e in x] if not(isinstance(x, float)) else x if pd.notnull(x) else x)

places['simple'] = places['simple'].apply(lambda x: [e for e in x if e] if not(isinstance(x, float)) else np.nan)

# wywalić len(1:2) from simple

places_dict = {}
for i, row in tqdm(places.iterrows(), total=places.shape[0]):
    try:
        for el in row['simple']:
            if el and el not in places_dict:
                places_dict[el] = {'records': [row['001']],
                                   'country': [row['country_name']]}
            elif el: 
                places_dict[el]['records'].append(row['001'])
                places_dict[el]['country'].append(row['country_name'])
    except TypeError:
        pass
    
places_dict = {k:v for k,v in places_dict.items() if len(k) > 2}
places_dict_multiple_countries = {k:{ke:set([e for e in va if e not in ['No place, unknown, or undetermined', 'unknown']]) if ke == 'country' else va for ke,va in v.items()} for k,v in places_dict.items()}
{k:v.update({'geonames_country': {va['Geonames_name'] for ke,va in country_codes.items() if va['MARC_name'] in v['country']}}) for k,v in places_dict_multiple_countries.items()}
places_dict_multiple_countries = {k:{ke:{e for e in va if pd.notnull(e)} if ke == 'geonames_country' else va for ke,va in v.items()} for k,v in places_dict_multiple_countries.items()}

places_dict = {k:{ke:get_most_frequent_country(va) if ke == 'country' else va for ke, va in v.items()} for k,v in places_dict.items()}
{k:v.update({'geonames_country': {va['Geonames_name'] for ke,va in country_codes.items() if va['MARC_name'] == v['country']}}) for k,v in places_dict.items()}
places_dict = {k:{ke:va.pop() if ke == 'geonames_country' and len(va) != 0 else va if ke != 'geonames_country' else np.nan for ke,va in v.items()} for k,v in places_dict.items()}

# places_dict['prahaberlin']
# test = places[places['001'] == 263668500]

length_ordered = sorted([e for e in places_dict], key=len, reverse=True)
#co zrobić z długimi nazwami?

frequency = {k:len(v['records']) for k,v in places_dict.items()}

places_ordered = [e for e in sorted([e for e in frequency], key=frequency.get, reverse=True)]

places_clusters = cluster_strings(places_ordered, 0.75)
places_clusters_with_country = {k:[(e, places_dict[e]['country'], places_dict[e]['geonames_country']) for e in v] for k,v in places_clusters.items()}


# {k:v for k,v in places_clusters.items() if 'zdobniki i winiety wykonano wedlug projektow artysty malarza teodora grotta' in v}
# {k:v for k,v in places_dict.items() if k == 'jmenem sboru pro zrizeni narodniho divadla v praze vydava j otto'}
# test_df = places[places['001'].isin([45705074,
#   505913617,
#   1004381943,
#   505913618,
#   505913603,
#   20940220,
#   505913606,
#   505913608])]
# {k:v for k,v in places_dict.items() if k == 'prahaberlin'}

# places_geonames = {}
# users_index = 0
# #search bez fuzzy = 1406/2423

# for place in tqdm(places_ordered):
#     # place = 'amsterdam'
#     # place = 'berlin ost'
#     # place = 'chicago'
#     # place = 'leningrad'
#     # place = 'pl 55 praha'
#     # place = 'izabelin'
#     # place = 'hanau am main'
#     # place = 'new york'
#     # place = 'ste-anne-de-bellevue'
#     while True:
#         url = 'http://api.geonames.org/searchJSON?'
#         params = {'username': geoname_users[users_index], 'q': place, 'featureClass': 'P'}
#         # params = {'username': geoname_users[users_index], 'q': place, 'featureClass': 'P', 'fuzzy': 0.4}
#         result = requests.get(url, params=params).json()
#         if 'status' in result:
#             users_index += 1
#             continue
#         if result['totalResultsCount'] > 0:  
#             try:
#                 country_from_dict = {v['geonames_country'] for k,v in places_dict.items() if k == place}.pop()
#                 if pd.notnull(country_from_dict):
#                     best_selection = [e for e in result['geonames'] if e['countryName'] == country_from_dict]
#                 else: 
#                     best_selection = [e for e in result['geonames'] if e['countryName'] == {v['country'] for k,v in places_dict.items() if k == place}.pop()]
#                 places_geonames.update({place: max(best_selection, key=lambda x: x['population'])})
#             except ValueError:
#                 try:
#                     best_selection = [e for e in result['geonames'] if e['adminName1'] == {v['country'] for k,v in places_dict.items() if k == place}.pop()]
#                     places_geonames.update({place: max(best_selection, key=lambda x: x['population'])})
                
#                 except ValueError:
#                     # places_geonames.update({place: max(result['geonames'], key=lambda x: x['population'])})
#                     places_geonames.update({place: 'No geonames response'})
            
#             #nie mogę brać max population, bo dla amsterdamu to jest Nowy Jork
#             #trzeba brać 1. sugestię geonames
#             # places_geonames.update({place: max(result['geonames'], key=lambda x: x['population'])})
#             #!!!!!tutaj dodać klucz państwa!!!!!!!
#             # places_geonames.update({place: result['geonames'][0]})
#         else: places_geonames.update({place: 'No geonames response'})
#         break

# with open('translations_places.pickle', 'wb') as handle:
#     pickle.dump(places_geonames, handle, protocol=pickle.HIGHEST_PROTOCOL)
with open('translations_places.pickle', 'rb') as handle:
    places_geonames = pickle.load(handle)



# tu był pomysł, żeby reszty szukać po wszystkich krajach, które występują – wyniki słabe
# iso_codes = [[e['Geonames_name'], e['iso_alpha_2']] for e in country_codes.values()]
# iso_codes = dict(zip([e[0] for e in iso_codes], [e[-1] for e in iso_codes]))

# places_geonames2 = {}
# users_index = 0
# for place in tqdm(places_ordered_rest):
#     # place = 'monchaltorf'
#     # place = 'middlesex'
#     while True:
#         countries = '&'.join([f'country={iso_codes[e]}' for e in places_dict_multiple_countries[place]['geonames_country']])
#         url = f'http://api.geonames.org/searchJSON?q={place}&username={geoname_users[users_index]}&{countries}&featureClass=P&continentCode='
#         result = requests.get(url).json()
#         if 'status' in result:
#             users_index += 1
#             continue
#         try:
#             # places_geonames2.update({place: max(result['geonames'], key=lambda x: x['population'])})
#             places_geonames2.update({place: max(result['geonames'], key=lambda x: ratio(place, x['name']))})
#         except ValueError: places_geonames2.update({place: 'No geonames response'})
#         break

# places_geonames_ok = {k:v for k,v in places_geonames.items() if isinstance(v, dict)}

places_geonames_ok = {k:v for k,v in places_geonames.items() if isinstance(v, dict)}

places_ordered_rest = [e for e in places_ordered if e not in places_geonames_ok]
places_clusters_rest = cluster_strings(places_ordered_rest, 0.76)

d = {v:i for i, v in enumerate(places_ordered_rest)}
places_clusters_rest = dict(sorted(places_clusters_rest.items(), key=lambda v: d[v[0]]))

places_ordered_rest_from_dict = list(places_clusters_rest.keys())

places['geonames'] = places['simple'].apply(lambda x: [places_geonames_ok[e]['geonameId'] if e in places_geonames_ok else np.nan for e in x] if not(isinstance(x, float)) else np.nan)

#manual enrichment
places_geonames_extra = {}
places_geonames_extra.update({k:places_geonames['praha'] for k in places_clusters_rest['v praze']})
places_geonames_extra.update({k:places_geonames['reinbek'] for k in places_clusters_rest['reinbek bei hamburg']})
places_geonames_extra.update({k:places_geonames['ljubljana'] for k in places_clusters_rest['v ljubljani']})
places_geonames_extra.update({k:{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2661881&username=crosinski').json().items() if ka in places_geonames['praha']} for k in places_clusters_rest['aarau']})
places_geonames_extra.update({k:places_geonames['brno'] for k in places_clusters_rest['v brne']})
places_geonames_extra.update({k:places_geonames['sofia'] for k in places_clusters_rest['sofiia']})
places_geonames_extra.update({k:{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2661604&username=crosinski').json().items() if ka in places_geonames['praha']} for k in places_clusters_rest['basel']})
places_geonames_extra.update({k:places_geonames['bratislava'] for k in places_clusters_rest['v bratislave']})
places_geonames_extra.update({k:places_geonames['gorizia'] for k in places_clusters_rest['v gorici']})
places_geonames_extra.update({k:{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2618425&username=crosinski').json().items() if ka in places_geonames['praha']} for k in places_clusters_rest['kopenhagen']})
places_geonames_extra.update({k:places_geonames['zagreb'] for k in places_clusters_rest['u zagrebu']})
places_geonames_extra.update({k:places_geonames['budisin'] for k in places_clusters_rest['w budysinje']})
places_geonames_extra.update({k:{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=3074967&username=crosinski').json().items() if ka in places_geonames['praha']} for k in places_clusters_rest['hradec kralove']})
places_geonames_extra.update({k:places_geonames['martin'] for k in places_clusters_rest['turciansky sv martin']})
places_geonames_extra.update({k:places_geonames['st petersburg'] for k in places_clusters_rest['s-peterburg']})
places_geonames_extra.update({k:places_geonames['berlin'] for k in places_clusters_rest['berlin ua']})
places_geonames_extra.update({k:places_geonames['praha'] for k in places_clusters_rest['nakladatelstvi ceskoslovenske akademie ved']})
places_geonames_extra.update({k:{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=723819&username=crosinski').json().items() if ka in places_geonames['praha']} for k in places_clusters_rest['presov']})
places_geonames_extra.update({k:places_geonames['new delhi'] for k in places_clusters_rest['nayi dilli']})
places_geonames_extra.update({k:{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=672546&username=crosinski').json().items() if ka in places_geonames['praha']} for k in places_clusters_rest['nadlac']})
places_geonames_extra.update({k:{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2659631&username=crosinski').json().items() if ka in places_geonames['praha']} for k in places_clusters_rest['monchaltorf']})
places_geonames_extra.update({k:{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=1840898&username=crosinski').json().items() if ka in places_geonames['praha']} for k in places_clusters_rest['kyonggi-do paju-si']})
places_geonames_extra.update({k:places_geonames['london'] for k in places_clusters_rest['paul hamlyn']})
places_geonames_extra.update({k:places_geonames['london'] for k in places_clusters_rest['unwin']})
places_geonames_extra.update({k:places_geonames['seoul'] for k in places_clusters_rest['soul tukpyolsi']})
places_geonames_extra.update({k:{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=1819729&username=crosinski').json().items() if ka in places_geonames['praha']} for k in places_clusters_rest['hong kong']})
places_geonames_extra.update({k:{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=786714&username=crosinski').json().items() if ka in places_geonames['praha']} for k in places_clusters_rest['prishtine']})
places_geonames_extra.update({k:places_geonames['ostrava'] for k in places_clusters_rest['v ostrave']})
places_geonames_extra.update({k:places_geonames['ottensheim'] for k in places_clusters_rest['ottensheim an der donau']})
places_geonames_extra.update({k:{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=5184082&username=crosinski').json().items() if ka in places_geonames['praha']} for k in places_clusters_rest['chester springs']})
places_geonames_extra.update({k:{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=5351549&username=crosinski').json().items() if ka in places_geonames['praha']} for k in places_clusters_rest['gardena']})
places_geonames_extra.update({k:places_geonames['ostrava'] for k in places_clusters_rest['mor ostrava']})
places_geonames_extra.update({k:places_geonames['opava'] for k in places_clusters_rest['troppau']})
places_geonames_extra.update({k:{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=618426&username=crosinski').json().items() if ka in places_geonames['praha']} for k in places_clusters_rest['kisineu']})
places_geonames_extra.update({k:places_geonames['london'] for k in places_clusters_rest['g allen']})
places_geonames_extra.update({k:places_geonames['frankfurt am main'] for k in places_clusters_rest['frankfurt a m']})
places_geonames_extra.update({k:places_geonames['kosice'] for k in places_clusters_rest['v kosiciach']})
places_geonames_extra.update({k:{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2939811&username=crosinski').json().items() if ka in places_geonames['praha']} for k in places_clusters_rest['w chosebuzu']})
places_geonames_extra.update({k:places_geonames['olomouc'] for k in places_clusters_rest['olmutz']})
places_geonames_extra.update({k:places_geonames['helsinki'] for k in places_clusters_rest['helsingissa']})
places_geonames_extra.update({k:places_geonames['ostrava'] for k in places_clusters_rest['mahr-ostrau']})
places_geonames_extra.update({k:places_geonames['zilina'] for k in places_clusters_rest['v ziline']})
places_geonames_extra.update({k:places_geonames['plzen'] for k in places_clusters_rest['v plzni']})
places_geonames_extra.update({k:places_geonames['praha'] for k in places_clusters_rest['artia']})
places_geonames_extra.update({k:places_geonames['praha'] for k in places_clusters_rest['praha in-']})
places_geonames_extra.update({k:{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=1275339&username=crosinski').json().items() if ka in places_geonames['praha']} for k in places_clusters_rest['bombay']})
places_geonames_extra.update({k:{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=1275004&username=crosinski').json().items() if ka in places_geonames['praha']} for k in places_clusters_rest['calcutta']})
places_geonames_extra.update({k:places_geonames['klagenfurt'] for k in places_clusters_rest['klagenfurt am worthersee']})
places_geonames_extra.update({k:places_geonames['presov'] for k in places_clusters_rest['prjasiv']})
places_geonames_extra.update({k:places_geonames['esplugues de llobregat'] for k in places_clusters_rest['esplugas de llobregat']})
places_geonames_extra.update({k:places_geonames['klagenfurt'] for k in places_clusters_rest['v celovcu']})
places_geonames_extra.update({k:places_geonames['london'] for k in places_clusters_rest['london printed in czechoslovakia']})
places_geonames_extra.update({k:{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=3188582&username=crosinski').json().items() if ka in places_geonames['praha']} for k in places_clusters_rest['tuzla']})
places_geonames_extra.update({k:{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=723819&username=crosinski').json().items() if ka in places_geonames['praha']} for k in places_clusters_rest['prjasiv']})
places_geonames_extra.update({k:{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=3074967&username=crosinski').json().items() if ka in places_geonames['praha']} for k in places_clusters_rest['hradec kralove']})
places_geonames_extra.update({k:{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=3074967&username=crosinski').json().items() if ka in places_geonames['praha']} for k in places_clusters_rest['koniggratz']})
places_geonames_extra.update({k:places_geonames['warszawa'] for k in places_clusters_rest['warzsawa']})
places_geonames_extra.update({k:{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2761538&username=crosinski').json().items() if ka in places_geonames['praha']} for k in places_clusters_rest['weitra']})
places_geonames_extra.update({k:{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=360630&username=crosinski').json().items() if ka in places_geonames['praha']} for k in places_clusters_rest['cairo']})
places_geonames_extra.update({k:{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=360630&username=crosinski').json().items() if ka in places_geonames['praha']} for k in places_clusters_rest['al-qahirat']})
places_geonames_extra.update({k:places_geonames['taiwan'] for k in places_clusters_rest['tai bei xian xin dian shi']})
places_geonames_extra.update({k:{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2904789&username=crosinski').json().items() if ka in places_geonames['praha']} for k in places_clusters_rest['hildesheim']})
places_geonames_extra.update({k:places_geonames['mexico'] for k in places_clusters_rest['ciudad de mexico']})
places_geonames_extra.update({k:places_geonames['bratislava'] for k in places_clusters_rest['poszony']})
places_geonames_extra.update({k:places_geonames['bautzen'] for k in places_clusters_rest['budysyn']})
places_geonames_extra.update({k:places_geonames['praha'] for k in places_clusters_rest['spolek ceskych bibliofilu']})
places_geonames_extra.update({k:places_geonames['london'] for k in places_clusters_rest['v londyne']})
places_geonames_extra.update({k:{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=1668341&username=crosinski').json().items() if ka in places_geonames['praha']} for k in places_clusters_rest['taipei']})
places_geonames_extra.update({k:places_geonames['seoul'] for k in places_clusters_rest['korea']})
places_geonames_extra.update({k:{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2820860&username=crosinski').json().items() if ka in places_geonames['praha']} for k in places_clusters_rest['tubingen']})
places_geonames_extra.update({k:places_geonames['budapest'] for k in places_clusters_rest['madarsko']})
places_geonames_extra.update({k:places_geonames['praha'] for k in places_clusters_rest['na smichove']})
places_geonames_extra.update({k:places_geonames['wien'] for k in places_clusters_rest['wien ua']})
places_geonames_extra.update({k:{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=667268&username=crosinski').json().items() if ka in places_geonames['praha']} for k in places_clusters_rest['sibiu']})
places_geonames_extra.update({k:{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2618425&username=crosinski').json().items() if ka in places_geonames['praha']} for k in places_clusters_rest['kbh']})
places_geonames_extra.update({k:places_geonames['helsinki'] for k in places_clusters_rest['hki']})
places_geonames_extra.update({k:places_geonames['praha'] for k in places_clusters_rest['prag ii']})
places_geonames_extra.update({k:places_geonames['praha'] for k in places_clusters_rest['sv praha']})
places_geonames_extra.update({k:places_geonames['kassel'] for k in places_clusters_rest['kassel-wilhelmshoehe']})
places_geonames_extra.update({k:places_geonames['martin'] for k in places_clusters_rest['matica slovenska']})
places_geonames_extra.update({k:places_geonames['oxford'] for k in places_clusters_rest['basil blackwell']})

places_geonames_ok.update(places_geonames_extra)
places['geonames'] = places['simple'].apply(lambda x: [places_geonames_ok[e]['geonameId'] if e in places_geonames_ok else np.nan for e in x] if not(isinstance(x, float)) else np.nan)
#appendix OV
ov_geonames = pd.read_excel("C:\\Users\\Cezary\\Downloads\\ov_geonames.xlsx")
ov_geonames = ov_geonames[(ov_geonames['city'].notnull()) |
                          (ov_geonames['country'].notnull())].values.tolist()

r = requests.get('https://www.geonames.org/countries/').content
soup = BeautifulSoup(r, 'lxml')          

ccodes = {}
for code, country in zip(soup.select('#countries td:nth-child(1)'), soup.select('td:nth-child(5)')):
    ccodes[country.text.lower()] = code.text
                         
places_geonames_extra_2 = {}
errors = []
with ThreadPoolExecutor(max_workers=50) as excecutor:
    list(tqdm(excecutor.map(reharvest_geonames, ov_geonames), total=len(ov_geonames)))  

#miejscowosci z przecinkiem
comas = [e for e in errors if pd.notnull(e[1]) and ',' in e[1]]

for old, city, country in comas:
    # old, city, country = comas[0]
    cities = [e.strip() for e in city.split(',')]
    records = places_dict[old]['records']
    for c in cities:
        places_dict[c]['records'].extend(records)
    del places_dict[old]

#same państwa

capitals = {}
for capital, country in zip(soup.select('td:nth-child(6)'), soup.select('td:nth-child(5)')):
    capitals[country.text.lower()] = capital.text

multiple_countries = [e for e in errors if (pd.isnull(e[1]) or ',' not in e[1]) and ',' in e[-1]]
places_dict['brasilia']['records'].extend(places_dict[multiple_countries[0][0]]['records'])
places_dict['lisboa']['records'].extend(places_dict[multiple_countries[0][0]]['records'])
del places_dict[multiple_countries[0][0]]

only_countries = [e for e in errors if (pd.isnull(e[1]) or ',' not in e[1]) and ',' not in e[-1]]

for old, city, country in only_countries:
    # old, city, country = only_countries[2]
    capital = capitals[country.lower()].lower()
    if capital in places_geonames and isinstance(places_geonames[capital], dict):
        places_geonames_extra_2[old] = places_geonames[capital]
    else: 
        url = 'http://api.geonames.org/searchJSON?'
        q_country = ccodes[country.lower()]
        params = {'username': 'crosinski', 'q': capital, 'featureClass': 'P', 'country': q_country}
        result = requests.get(url, params=params).json()
        places_geonames_extra_2.update({old: max(result['geonames'], key=lambda x: x['population'])})

places_geonames_ok.update(places_geonames_extra_2)
places['geonames'] = places['simple'].apply(lambda x: [places_geonames_ok[e]['geonameId'] if e in places_geonames_ok else np.nan for e in x] if not(isinstance(x, float)) else np.nan)

#nadpisać geonames
# 2936759 -> 3078610


# step 1: list of places to reharvest from OV
# step 2: for [nan] with county == unknown query geonames and pick the most populated place and population > 0

test_df = places[(places['geonames'].apply(lambda x: all(pd.isnull(e) for e in x) if not(isinstance(x, float)) else False)) &
                 (~places['country_name'].isin(['unknown', 'No place, unknown, or undetermined']))]
countries = dict(zip(test_df['001'], test_df['geonames_name']))
countries = {k:capitals[v.lower()] for k,v in countries.items()}

help_dict = {'belgrade': 'belehrad',
             'rome': 'roma',
             'lisbon': 'lisboa',
             'nur-sultan': {ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=1526273&username=crosinski').json().items() if ka in places_geonames['praha']},
             'colombo': {ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=1248991&username=crosinski').json().items() if ka in places_geonames['praha']},
             'panama city': {ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=3703443&username=crosinski').json().items() if ka in places_geonames['praha']},
             'torshavn': {ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2611396&username=crosinski').json().items() if ka in places_geonames['praha']}}

countries = {k:[places_geonames_ok[v.lower()]['geonameId']] if v.lower() in places_geonames_ok else [help_dict[v.lower()]['geonameId']] if isinstance(help_dict[v.lower()], dict) else [places_geonames_ok[help_dict[v.lower()]]['geonameId']] for k,v in countries.items()}

places['geonames'] = places[['001', 'geonames']].apply(lambda x: countries[x['001']] if x['001'] in countries else x['geonames'], axis=1)
#w tabeli nowa kolumna z państwem z geonames

geonames_ids = [e for e in places['geonames'].to_list() if not(isinstance(e, float))]
geonames_ids = set([e for sub in geonames_ids for e in sub if not(isinstance(e, float))])

geonames_resp = {}
# for geoname in geonames_ids:
def get_geonames_country(geoname_id):
    # geoname_id = list(geonames_ids)[0]
    user = random.choice(geoname_users)
    #w funkcję wpisać losowanie randomowego username
    try:
        geonames_resp[geoname_id] = requests.get(f'http://api.geonames.org/getJSON?geonameId={geoname_id}&username={user}').json()['countryName']
    except KeyError:
        get_geonames_country(geoname_id)

with ThreadPoolExecutor(max_workers=50) as executor:
    list(tqdm(executor.map(get_geonames_country, geonames_ids), total=len(geonames_ids)))
    
#dodać kolumnę    
    
places['geonames_country'] = places['geonames'].apply(lambda x: [geonames_resp[e] for e in x if not(isinstance(e, float))] if not(isinstance(x, float)) else x)   
    
    
    
    














test = places[places['geonames'].apply(lambda x: any(pd.isnull(e) for e in x) if not(isinstance(x, float)) else False)]
places_ok = places[~places['001'].isin(test['001'])]
places_ordered_rest = [e for e in places_ordered_rest_from_dict if e not in places_geonames_ok]

with open('places_rest.txt', 'w') as f:
    for el in places_ordered_rest:
        f.write(el+'\n')

test_df = places[places['001'].isin(places_dict['pennsylvania']['records'])]

sample_df = places[(places['simple'].apply(lambda x: any('-' in e for e in x) if not(isinstance(x, float)) else False)) &
                   (places['geonames'].apply(lambda x: any(pd.isnull(e) for e in x) if not(isinstance(x, float)) else False))]

#staty
print(f'total no. of records: {places.shape[0]}')
print(f'no. of records without place in 260: {places[places["geonames"].isna()].shape[0]}')
print(f'no. of records with at least one geonames ID: {places[places["geonames"].apply(lambda x: any(pd.notnull(e) for e in x) if not(isinstance(x, float)) else False)].shape[0]}')
print(f'geonames coverage: {places[places["geonames"].apply(lambda x: any(pd.notnull(e) for e in x) if not(isinstance(x, float)) else False)].shape[0]/places.shape[0]}')
places.to_excel(f'translations_geonames_{now}.xlsx', index=False)


test_df = places[places['260'].str.contains('❦', na=False)]


# drop nan from list
# coordinates for geonames
# deduplicate geonames and countries
# harvest cnl and build clusters for works































