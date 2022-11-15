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
import pickle
import sys
sys.path.insert(1, 'C:/Users/Cezary/Documents/SPUB-project')
from geonames_accounts import geonames_users
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

# places_copy = places.copy()

places['geonames'] = places['geonames'].apply(lambda x: list(set([e for e in x if pd.notnull(e)])) if isinstance(x, list) else x)
places['geonames'] = places['geonames'].apply(lambda x: x if x else np.nan)

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
    
#dodać koordynaty
    
def get_geonames_name(geoname_id):
    # geoname_id = list(geonames_ids)[0]
    user = random.choice(geoname_users)
    #w funkcję wpisać losowanie randomowego username
    try:
        geonames_resp[geoname_id] = requests.get(f'http://api.geonames.org/getJSON?geonameId={geoname_id}&username={user}').json()['name']
    except KeyError:
        get_geonames_name(geoname_id)    
        
def get_geonames_coordinates(geoname_id):
    # geoname_id = list(geonames_ids)[0]
    user = random.choice(geoname_users)
    #w funkcję wpisać losowanie randomowego username
    try:
        response = requests.get(f'http://api.geonames.org/getJSON?geonameId={geoname_id}&username={user}').json()
        lat = response['lat']
        lng = response['lng']
        geonames_resp[geoname_id] = {'lat': lat,
                                     'lng': lng}
    except KeyError:
        get_geonames_coordinates(geoname_id) 
    
geonames_resp = {}
with ThreadPoolExecutor(max_workers=50) as executor:
    list(tqdm(executor.map(get_geonames_name, geonames_ids), total=len(geonames_ids)))
places['geonames_place_name'] = places['geonames'].apply(lambda x: [geonames_resp[e] for e in x if not(isinstance(e, float))] if not(isinstance(x, float)) else x)   

geonames_resp = {}
with ThreadPoolExecutor(max_workers=50) as executor:
    list(tqdm(executor.map(get_geonames_coordinates, geonames_ids), total=len(geonames_ids)))
places['geonames_lat'] = places['geonames'].apply(lambda x: [geonames_resp[e]['lat'] for e in x if not(isinstance(e, float))] if not(isinstance(x, float)) else x)   
places['geonames_lng'] = places['geonames'].apply(lambda x: [geonames_resp[e]['lng'] for e in x if not(isinstance(e, float))] if not(isinstance(x, float)) else x)  

#wybrać właściwe kolumny

places = places[['001', 'geonames', 'geonames_place_name', 'geonames_country', 'geonames_lat', 'geonames_lng']].rename(columns={'geonames':'geonames_id','geonames_place_name':'geonames_name'})

translations_df = pd.merge(translations_df, places, on='001')
with open('translations_ov_more_to_delete.txt', 'rt') as f:
    to_del = f.read().splitlines()
to_del = [int(e) for e in to_del]
    
translations_df = translations_df[~translations_df['001'].isin(to_del)]

translations_df.to_excel(f'translation_deduplicated_with_geonames_{now}.xlsx', index=False)


#%% uzupełnienia

#wgranie kompletnych danych
all_records_df = pd.read_excel(r"C:\Users\Cezary\Downloads\everything_merged_2022-02-24.xlsx")

#wgranie aktualnego pliku
translations_df = pd.read_excel('translation_before_manual_2022-06-27.xlsx')

# selected_records_ids = [973830002,10000020259,573128908,536215106,422146959,561064698,65225959,956310775,10000019022,974702866,10000019999,70297632,918522275,10000019361,744549810,43613233,141423051,1037364959,180939365,10000018169,1036916395,1089434597,162375520,744781853,561023527,634691356,1078754555,634691262,10000016822,804915536,7153460,311165923,914965247,85383044,176652729,176727390,10000018540,315123955,10000015528,10000009148,999650215,10000020192,1028286680,85530707,707608056,10000019589,1156233990,10000019861,10000019908,561629681,10000016748,411825033,75829811,895445766,10000006161,10000018032,10000009021,1074496565,1062258283,85601610,10000018358,263668500,10000020412,1081533313,893786372,10000016742,923199414,85697815,880478140,85569867,59964762,42178477,320578199,444523937,893760338,820387383,85578609,977576007,1047511994,885436796,10000020406,921286870,53262577,76722348,251397791,1186081004,10000018160,10000018166,10000018167,877190815,1190769822,70401067,10000009007,10000009029,918488254,367432746,276867728,276863120,1066769233,10000018041,10000019526,1047888450,35089923,186510939,10000019620,10000019646,10000017737,10000018027,471042020,10000018151,310786855,276868088,276868092,867635843,910922646,320520167,85616037,85568144,901893768,150443531,1113840778,10000017068,370416349,935618061,895088984,1088856590,503184530,166057110,923672472,162641079,1110924362,10000015858,444507671,10000008294,809554614,1034619625,719129465,221635076,317436737,1091867544,10000013788,314841940]

# selected_translations_df = translations_df.loc[translations_df['001'].isin(selected_records_ids)]
translations_without_geonames = translations_df.loc[translations_df['geonames_id'].isnull()]

selected_records = set([int(el) for sub in [e.split('❦') for e in translations_without_geonames['group_ids'].to_list()] for el in sub])

groups_dict = dict(zip(translations_without_geonames['001'], [[int(el) for el in e.split('❦')] for e in translations_without_geonames['group_ids']]))

# selected_records_plus = [int(el) for sub in [e.split('❦') for e in selected_translations_df['group_ids'].to_list()] for el in sub]
# selected_records = set(selected_records_ids + selected_records_plus)

selected_records_df = all_records_df.loc[all_records_df['001'].isin(selected_records)].drop_duplicates()


#re-harvesting geonames

country_codes = pd.read_excel('translation_country_codes.xlsx')
country_codes = [list(e[-1]) for e in country_codes.iterrows()]
country_codes = dict(zip([e[0] for e in country_codes], [{'MARC_name': e[1], 'iso_alpha_2': e[2], 'Geonames_name': e[-1]} for e in country_codes]))

places = selected_records_df[['001', '008', '260']]
# places = places[places.index.isin([1, 2, 72, 155, 262, 445, 667, 1437, 2298, 4402, 13258])]

places['country'] = places['008'].apply(lambda x: x[15:18])
places.drop(columns='008', inplace=True)
places['country'] = places['country'].str.replace('\\', '', regex=False)
places['country_name'] = places['country'].apply(lambda x: country_codes[x]['MARC_name'] if x in country_codes else 'unknown')
places['geonames_name'] = places['country'].apply(lambda x: country_codes[x]['Geonames_name'] if x in country_codes else 'unknown')
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
#manualne korekty
places.at[places.loc[places['001'] == 561629681].index.values[0], 'places'] = ['London', 'Glasgow']
places.at[places.loc[places['001'] == 162375520].index.values[0], 'places'] = ['Dresden', 'Leipzig']
places.at[places.loc[places['001'] == 469594167].index.values[0], 'places'] = ['Düsseldorf', 'Köln']
places.at[places.loc[places['001'] == 504116129].index.values[0], 'places'] = ['London', 'New York']
places.at[places.loc[places['001'] == 809046852].index.values[0], 'places'] = ['London', 'New York']
places.at[places.loc[places['001'] == 310786855].index.values[0], 'places'] = ['Leonberg', 'Warmbronn']
places.at[places.loc[places['001'] == 804915536].index.values[0], 'places'] = ['Polock', 'Brest']
places.at[places.loc[places['001'] == 263668500].index.values[0], 'places'] = ['Praha', 'Berlin']
places.at[places.loc[places['001'] == 504310019].index.values[0], 'places'] = ['Wien', 'Teschen']
places.at[places.loc[places['001'] == 367432746].index.values[0], 'places'] = ['Wien', 'Leipzig']

places['simple'] = places['places'].apply(lambda x: [simplify_place_name(e).strip() for e in x] if not(isinstance(x, float)) else x if pd.notnull(x) else x)
places['simple'] = places['simple'].apply(lambda x: [e for e in x if e] if not(isinstance(x, float)) else np.nan)

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

length_ordered = sorted([e for e in places_dict], key=len, reverse=True)

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

places_geonames = {}
users_index = 0
#search bez fuzzy = 1406/2423

for place in tqdm(places_ordered):
    # place = 'amsterdam'
    # place = 'berlin ost'
    # place = 'chicago'
    # place = 'leningrad'
    # place = 'pl 55 praha'
    # place = 'izabelin'
    # place = 'hanau am main'
    # place = 'new york'
    # place = 'ste-anne-de-bellevue'
    while True:
        url = 'http://api.geonames.org/searchJSON?'
        params = {'username': geoname_users[users_index], 'q': place, 'featureClass': 'P'}
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

with open('translations_places_2.pickle', 'wb') as handle:
    pickle.dump(places_geonames, handle, protocol=pickle.HIGHEST_PROTOCOL)
    
# with open('translations_places_2.pickle', 'rb') as f:
#     places_geonames = pickle.load(f)

places_geonames_ok = {k:v for k,v in places_geonames.items() if isinstance(v, dict)}
places_ordered_rest = [e for e in places_ordered if e not in places_geonames_ok]
places_clusters_rest = cluster_strings(places_ordered_rest, 0.76)
d = {v:i for i, v in enumerate(places_ordered_rest)}
places_clusters_rest = dict(sorted(places_clusters_rest.items(), key=lambda v: d[v[0]]))
places_ordered_rest_from_dict = list(places_clusters_rest.keys())
places['geonames'] = places['simple'].apply(lambda x: [places_geonames_ok[e]['geonameId'] if e in places_geonames_ok else np.nan for e in x] if not(isinstance(x, float)) else np.nan)

places_geonames_extra = {}
places_geonames_extra.update({k:{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=7285013&username=crosinski').json().items() if ka in places_geonames['praha']} for k in places_clusters_rest['aarau']})
places_geonames_extra.update({k:{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=6552815&username=crosinski').json().items() if ka in places_geonames['praha']} for k in places_clusters_rest['altenmedingen']})
places_geonames_extra.update({k:{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=3069011&username=crosinski').json().items() if ka in places_geonames['praha']} for k in places_clusters_rest['am heiligen berge bei olmutz']})
places_geonames_extra.update({k:places_geonames['amsterdam'] for k in places_clusters_rest['amsterodam']})
places_geonames_extra.update({k:{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=6451134&username=crosinski').json().items() if ka in places_geonames['praha']} for k in places_clusters_rest['avon']})
places_geonames_extra.update({k:{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=6558227&username=crosinski').json().items() if ka in places_geonames['praha']} for k in places_clusters_rest['bad aibling']})
places_geonames_extra.update({k:{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=7872008&username=crosinski').json().items() if ka in places_geonames['praha']} for k in places_clusters_rest['bad goisern']})
places_geonames_extra.update({k:{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=6559114&username=crosinski').json().items() if ka in places_geonames['praha']} for k in places_clusters_rest['bad homburg']})
places_geonames_extra.update({k:{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=1253184&username=crosinski').json().items() if ka in places_geonames['praha']} for k in places_clusters_rest['bakingampeta vijayavada']})
places_geonames_extra.update({k:{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2656192&username=crosinski').json().items() if ka in places_geonames['praha']} for k in places_clusters_rest['basingstoke']})
places_geonames_extra.update({k:{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=3034714&username=crosinski').json().items() if ka in places_geonames['praha']} for k in places_clusters_rest['bassac']})
places_geonames_extra.update({k:{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=3203982&username=crosinski').json().items() if ka in places_geonames['praha']} for k in places_clusters_rest['bjelovar']})
places_geonames_extra.update({k:places_geonames['london'] for k in places_clusters_rest['boosey']})
places_geonames_extra.update({k:{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=5906267&username=crosinski').json().items() if ka in places_geonames['praha']} for k in places_clusters_rest['boucherville']})
places_geonames_extra.update({k:{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=3078837&username=crosinski').json().items() if ka in places_geonames['praha']} for k in places_clusters_rest['brandys nad labem']})
places_geonames_extra.update({k:places_geonames['bratislava'] for k in places_clusters_rest['bratislave']})
places_geonames_extra.update({k:{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=3469058&username=crosinski').json().items() if ka in places_geonames['praha']} for k in places_clusters_rest['brazilio']})
places_geonames_extra.update({k:{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=3466296&username=crosinski').json().items() if ka in places_geonames['praha']} for k in places_clusters_rest['chapeco']})
places_geonames_extra.update({k:{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=3178085&username=crosinski').json().items() if ka in places_geonames['praha']} for k in places_clusters_rest['cormons']})
places_geonames_extra.update({k:{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2207821&username=crosinski').json().items() if ka in places_geonames['praha']} for k in places_clusters_rest['crows nest']})
places_geonames_extra.update({k:{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=3053918&username=crosinski').json().items() if ka in places_geonames['praha']} for k in places_clusters_rest['csorna']})
places_geonames_extra.update({k:{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2938714&username=crosinski').json().items() if ka in places_geonames['praha']} for k in places_clusters_rest['daun']})
places_geonames_extra.update({k:{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2936871&username=crosinski').json().items() if ka in places_geonames['praha']} for k in places_clusters_rest['dinslaken']})
places_geonames_extra.update({k:{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=5118226&username=crosinski').json().items() if ka in places_geonames['praha']} for k in places_clusters_rest['doran']})
places_geonames_extra.update({k:{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=5097459&username=crosinski').json().items() if ka in places_geonames['praha']} for k in places_clusters_rest['east rutherford']})
places_geonames_extra.update({k:{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=5097677&username=crosinski').json().items() if ka in places_geonames['praha']} for k in places_clusters_rest['englewood cliffs']})
places_geonames_extra.update({k:places_geonames['london'] for k in places_clusters_rest['evans bros']})
places_geonames_extra.update({k:places_geonames['london'] for k in places_clusters_rest['fore publications']})
places_geonames_extra.update({k:places_geonames['london'] for k in places_clusters_rest['g allen']})
places_geonames_extra.update({k:{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=677697&username=crosinski').json().items() if ka in places_geonames['praha']} for k in places_clusters_rest['galati']})
places_geonames_extra.update({k:places_geonames['oxford'] for k in places_clusters_rest['george sheppard']})
places_geonames_extra.update({k:{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=3014728&username=crosinski').json().items() if ka in places_geonames['praha']} for k in places_clusters_rest['grenoble']})
places_geonames_extra.update({k:{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=890422&username=crosinski').json().items() if ka in places_geonames['praha']} for k in places_clusters_rest['gweru']})
places_geonames_extra.update({k:{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=3069381&username=crosinski').json().items() if ka in places_geonames['praha']} for k in places_clusters_rest['haida']})
places_geonames_extra.update({k:places_geonames['hamburg'] for k in places_clusters_rest['hamburg wegner']})
places_geonames_extra.update({k:{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=6951076&username=crosinski').json().items() if ka in places_geonames['praha']} for k in places_clusters_rest['harmonds-worth']})
places_geonames_extra.update({k:places_geonames['london'] for k in places_clusters_rest['heinemann']})
places_geonames_extra.update({k:{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=5099133&username=crosinski').json().items() if ka in places_geonames['praha']} for k in places_clusters_rest['hoboken']})
places_geonames_extra.update({k:{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2902768&username=crosinski').json().items() if ka in places_geonames['praha']} for k in places_clusters_rest['hof']})
places_geonames_extra.update({k:{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2902768&username=crosinski').json().items() if ka in places_geonames['praha']} for k in places_clusters_rest['hof a d saal']})
places_geonames_extra.update({k:places_geonames['london'] for k in places_clusters_rest['hogarth press']})
places_geonames_extra.update({k:{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2775516&username=crosinski').json().items() if ka in places_geonames['praha']} for k in places_clusters_rest['horn']})
places_geonames_extra.update({k:places_geonames['london'] for k in places_clusters_rest['hutchinson']})
places_geonames_extra.update({k:places_geonames['london'] for k in places_clusters_rest['i nicholson']})
places_geonames_extra.update({k:{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2896736&username=crosinski').json().items() if ka in places_geonames['praha']} for k in places_clusters_rest['idstein']})
places_geonames_extra.update({k:places_geonames['london'] for k in places_clusters_rest['john lane']})
places_geonames_extra.update({k:places_geonames['london'] for k in places_clusters_rest['jonathan cape']})
places_geonames_extra.update({k:places_geonames['katowice'] for k in places_clusters_rest['kattowitz']})
places_geonames_extra.update({k:{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2618425&username=crosinski').json().items() if ka in places_geonames['praha']} for k in places_clusters_rest['kbh']})
places_geonames_extra.update({k:{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2890381&username=crosinski').json().items() if ka in places_geonames['praha']} for k in places_clusters_rest['kirchseeon']})
places_geonames_extra.update({k:{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2953424&username=crosinski').json().items() if ka in places_geonames['praha']} for k in places_clusters_rest['kissingen']})
places_geonames_extra.update({k:{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=3072649&username=crosinski').json().items() if ka in places_geonames['praha']} for k in places_clusters_rest['kremsier']})
places_geonames_extra.update({k:{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=3006213&username=crosinski').json().items() if ka in places_geonames['praha']} for k in places_clusters_rest['la tour-daigues']})
places_geonames_extra.update({k:{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=4635031&username=crosinski').json().items() if ka in places_geonames['praha']} for k in places_clusters_rest['la vergne']})
places_geonames_extra.update({k:{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2879185&username=crosinski').json().items() if ka in places_geonames['praha']} for k in places_clusters_rest['leinfelden bei stuttgart']})
places_geonames_extra.update({k:{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=3071677&username=crosinski').json().items() if ka in places_geonames['praha']} for k in places_clusters_rest['leitmeritz']})
places_geonames_extra.update({k:{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2751792&username=crosinski').json().items() if ka in places_geonames['praha']} for k in places_clusters_rest['ljouvert']})
places_geonames_extra.update({k:{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2890381&username=crosinski').json().items() if ka in places_geonames['praha']} for k in places_clusters_rest['ludewig']})
places_geonames_extra.update({k:{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2158177&username=crosinski').json().items() if ka in places_geonames['praha']} for k in places_clusters_rest['melbourne']})
places_geonames_extra.update({k:{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=3146125&username=crosinski').json().items() if ka in places_geonames['praha']} for k in places_clusters_rest['melhus']})
places_geonames_extra.update({k:{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2266464&username=crosinski').json().items() if ka in places_geonames['praha']} for k in places_clusters_rest['mem martins']})
places_geonames_extra.update({k:{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=625144&username=crosinski').json().items() if ka in places_geonames['praha']} for k in places_clusters_rest['mensk']})
places_geonames_extra.update({k:{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=3058730&username=crosinski').json().items() if ka in places_geonames['praha']} for k in places_clusters_rest['mestecko']})
places_geonames_extra.update({k:places_geonames['london'] for k in places_clusters_rest['methuen']})
places_geonames_extra.update({k:places_geonames['praha'] for k in places_clusters_rest['moderschan']})
places_geonames_extra.update({k:{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2747373&username=crosinski').json().items() if ka in places_geonames['praha']} for k in places_clusters_rest['mouton']})
places_geonames_extra.update({k:{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=3192484&username=crosinski').json().items() if ka in places_geonames['praha']} for k in places_clusters_rest['na prevaljah']})
places_geonames_extra.update({k:{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2864387&username=crosinski').json().items() if ka in places_geonames['praha']} for k in places_clusters_rest['neuotting am inn']})
places_geonames_extra.update({k:{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2864034&username=crosinski').json().items() if ka in places_geonames['praha']} for k in places_clusters_rest['neustadt in holstein']})
places_geonames_extra.update({k:places_geonames['london'] for k in places_clusters_rest['new english library']})
places_geonames_extra.update({k:{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=4945256&username=crosinski').json().items() if ka in places_geonames['praha']} for k in places_clusters_rest['newburyport']})
places_geonames_extra.update({k:{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2990363&username=crosinski').json().items() if ka in places_geonames['praha']} for k in places_clusters_rest['nimes']})
places_geonames_extra.update({k:{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=601294&username=crosinski').json().items() if ka in places_geonames['praha']} for k in places_clusters_rest['nokis']})
places_geonames_extra.update({k:{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=3194351&username=crosinski').json().items() if ka in places_geonames['praha']} for k in places_clusters_rest['novomesto']})
places_geonames_extra.update({k:{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=671768&username=crosinski').json().items() if ka in places_geonames['praha']} for k in places_clusters_rest['oradea']})
places_geonames_extra.update({k:places_geonames['praha'] for k in places_clusters_rest['orbis pub co']})
places_geonames_extra.update({k:{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2748956&username=crosinski').json().items() if ka in places_geonames['praha']} for k in places_clusters_rest['oud-gastel']})
places_geonames_extra.update({k:{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2789549&username=crosinski').json().items() if ka in places_geonames['praha']} for k in places_clusters_rest['oude-god']})
places_geonames_extra.update({k:{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=256197&username=crosinski').json().items() if ka in places_geonames['praha']} for k in places_clusters_rest['paiania']})
places_geonames_extra.update({k:{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=6951076&username=crosinski').json().items() if ka in places_geonames['praha']} for k in places_clusters_rest['penguin books']})
places_geonames_extra.update({k:{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=3058505&username=crosinski').json().items() if ka in places_geonames['praha']} for k in places_clusters_rest['petrovec']})
places_geonames_extra.update({k:{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=3190589&username=crosinski').json().items() if ka in places_geonames['praha']} for k in places_clusters_rest['pozega']})
places_geonames_extra.update({k:places_geonames['london'] for k in places_clusters_rest['pp ix']})
places_geonames_extra.update({k:{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=3067716&username=crosinski').json().items() if ka in places_geonames['praha']} for k in places_clusters_rest['prace']})
places_geonames_extra.update({k:places_geonames['praha'] for k in places_clusters_rest['praha prag']})
places_geonames_extra.update({k:{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2639842&username=crosinski').json().items() if ka in places_geonames['praha']} for k in places_clusters_rest['purley']})
places_geonames_extra.update({k:{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2848845&username=crosinski').json().items() if ka in places_geonames['praha']} for k in places_clusters_rest['reinbek bei hamburg']})
places_geonames_extra.update({k:{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2848845&username=crosinski').json().items() if ka in places_geonames['praha']} for k in places_clusters_rest['reinbek hbg']})
places_geonames_extra.update({k:{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2848273&username=crosinski').json().items() if ka in places_geonames['praha']} for k in places_clusters_rest['remschied']})
places_geonames_extra.update({k:{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=456172&username=crosinski').json().items() if ka in places_geonames['praha']} for k in places_clusters_rest['riga']})
places_geonames_extra.update({k:places_geonames['london'] for k in places_clusters_rest['robert anscombe']})
places_geonames_extra.update({k:{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=7576449&username=crosinski').json().items() if ka in places_geonames['praha']} for k in places_clusters_rest['rotmanka']})
places_geonames_extra.update({k:{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2766824&username=crosinski').json().items() if ka in places_geonames['praha']} for k in places_clusters_rest['salzburg']})
places_geonames_extra.update({k:{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=667268&username=crosinski').json().items() if ka in places_geonames['praha']} for k in places_clusters_rest['sibiu']})
places_geonames_extra.update({k:places_geonames['london'] for k in places_clusters_rest['spck']})
places_geonames_extra.update({k:places_geonames['london'] for k in places_clusters_rest['spring books']})
places_geonames_extra.update({k:{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=758416&username=crosinski').json().items() if ka in places_geonames['praha']} for k in places_clusters_rest['stanislawow']})
places_geonames_extra.update({k:{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2950767&username=crosinski').json().items() if ka in places_geonames['praha']} for k in places_clusters_rest['starnberg am see']})
places_geonames_extra.update({k:places_geonames['praha'] for k in places_clusters_rest['supraphon']})
places_geonames_extra.update({k:{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=3044681&username=crosinski').json().items() if ka in places_geonames['praha']} for k in places_clusters_rest['szentendre']})
places_geonames_extra.update({k:{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2747373&username=crosinski').json().items() if ka in places_geonames['praha']} for k in places_clusters_rest['the haugue']})
places_geonames_extra.update({k:{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2611497&username=crosinski').json().items() if ka in places_geonames['praha']} for k in places_clusters_rest['tonder']})
places_geonames_extra.update({k:{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=3064117&username=crosinski').json().items() if ka in places_geonames['praha']} for k in places_clusters_rest['treben']})
places_geonames_extra.update({k:places_geonames['praha'] for k in places_clusters_rest['u sisku']})
places_geonames_extra.update({k:places_geonames['london'] for k in places_clusters_rest['unwin']})
places_geonames_extra.update({k:{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=3063548&username=crosinski').json().items() if ka in places_geonames['praha']} for k in places_clusters_rest['usti nad labem']})
places_geonames_extra.update({k:places_geonames['praha'] for k in places_clusters_rest['v cheshskoi pragie']})
places_geonames_extra.update({k:places_geonames['praha'] for k in places_clusters_rest['v prahe']})
places_geonames_extra.update({k:{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=688746&username=crosinski').json().items() if ka in places_geonames['praha']} for k in places_clusters_rest['v sevljusi']})
places_geonames_extra.update({k:{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=690548&username=crosinski').json().items() if ka in places_geonames['praha']} for k in places_clusters_rest['v uzgorode']})
places_geonames_extra.update({k:{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2732649&username=crosinski').json().items() if ka in places_geonames['praha']} for k in places_clusters_rest['vila do conde']})
places_geonames_extra.update({k:{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2732547&username=crosinski').json().items() if ka in places_geonames['praha']} for k in places_clusters_rest['vila nova de famalicao']})
places_geonames_extra.update({k:{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=6543862&username=crosinski').json().items() if ka in places_geonames['praha']} for k in places_clusters_rest['villeneuve dascq']})
places_geonames_extra.update({k:{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=784136&username=crosinski').json().items() if ka in places_geonames['praha']} for k in places_clusters_rest['vrsac']})
places_geonames_extra.update({k:{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=3056683&username=crosinski').json().items() if ka in places_geonames['praha']} for k in places_clusters_rest['vrutky']})
places_geonames_extra.update({k:{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2745154&username=crosinski').json().items() if ka in places_geonames['praha']} for k in places_clusters_rest['vught']})
places_geonames_extra.update({k:{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=3078478&username=crosinski').json().items() if ka in places_geonames['praha']} for k in places_clusters_rest['w budine']})
places_geonames_extra.update({k:{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=3054643&username=crosinski').json().items() if ka in places_geonames['praha']} for k in places_clusters_rest['w pesti']})
places_geonames_extra.update({k:places_geonames['praha'] for k in places_clusters_rest['w prazy']})
places_geonames_extra.update({k:places_geonames['london'] for k in places_clusters_rest['watson']})
places_geonames_extra.update({k:{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2813638&username=crosinski').json().items() if ka in places_geonames['praha']} for k in places_clusters_rest['wattenheim']})
places_geonames_extra.update({k:{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2812482&username=crosinski').json().items() if ka in places_geonames['praha']} for k in places_clusters_rest['weimar']})
places_geonames_extra.update({k:places_geonames['london'] for k in places_clusters_rest['william heinemann']})
places_geonames_extra.update({k:places_geonames['paris'] for k in places_clusters_rest['xv paris']})
places_geonames_extra.update({k:{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2657896&username=crosinski').json().items() if ka in places_geonames['praha']} for k in places_clusters_rest['zurich']})
places_geonames_extra.update({k:{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2925177&username=crosinski').json().items() if ka in places_geonames['praha']} for k in places_clusters_rest['freiburg i breisgau']})
places_geonames_extra.update({k:{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=3073152&username=crosinski').json().items() if ka in places_geonames['praha']} for k in places_clusters_rest['schwarz-kostelezt']})
places_geonames_extra.update({k:{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=3272941&username=crosinski').json().items() if ka in places_geonames['praha']} for k in places_clusters_rest['riedstadt']})
places_geonames_extra.update({k:{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2648579&username=crosinski').json().items() if ka in places_geonames['praha']} for k in places_clusters_rest['glasgow']})
places_geonames_extra.update({k:{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=3101321&username=crosinski').json().items() if ka in places_geonames['praha']} for k in places_clusters_rest['teschen']})
places_geonames_extra.update({k:{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=623317&username=crosinski').json().items() if ka in places_geonames['praha']} for k in places_clusters_rest['polock']})
places_geonames_extra.update({k:{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=629634&username=crosinski').json().items() if ka in places_geonames['praha']} for k in places_clusters_rest['brest']})
places_geonames_extra.update({k:{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2878695&username=crosinski').json().items() if ka in places_geonames['praha']} for k in places_clusters_rest['leonberg']})
places_geonames_extra.update({k:{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2814088&username=crosinski').json().items() if ka in places_geonames['praha']} for k in places_clusters_rest['warmbronn']})
places_geonames_extra.update({k:{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2935022&username=crosinski').json().items() if ka in places_geonames['praha']} for k in places_clusters_rest['dresden']})

places_geonames_ok.update(places_geonames_extra)
places['geonames'] = places['simple'].apply(lambda x: [places_geonames_ok[e]['geonameId'] if e in places_geonames_ok else np.nan for e in x] if not(isinstance(x, float)) else np.nan)

places['geonames'] = places['geonames'].apply(lambda x: list(set([e for e in x if pd.notnull(e)])) if isinstance(x, list) else x)
places['geonames'] = places['geonames'].apply(lambda x: x if x else np.nan)

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
    
#dodać koordynaty
    
def get_geonames_name(geoname_id):
    # geoname_id = list(geonames_ids)[0]
    user = random.choice(geoname_users)
    #w funkcję wpisać losowanie randomowego username
    try:
        geonames_resp[geoname_id] = requests.get(f'http://api.geonames.org/getJSON?geonameId={geoname_id}&username={user}').json()['name']
    except KeyError:
        get_geonames_name(geoname_id)    
        
def get_geonames_coordinates(geoname_id):
    # geoname_id = list(geonames_ids)[0]
    user = random.choice(geoname_users)
    #w funkcję wpisać losowanie randomowego username
    try:
        response = requests.get(f'http://api.geonames.org/getJSON?geonameId={geoname_id}&username={user}').json()
        lat = response['lat']
        lng = response['lng']
        geonames_resp[geoname_id] = {'lat': lat,
                                     'lng': lng}
    except KeyError:
        get_geonames_coordinates(geoname_id) 
    
geonames_resp = {}
with ThreadPoolExecutor(max_workers=50) as executor:
    list(tqdm(executor.map(get_geonames_name, geonames_ids), total=len(geonames_ids)))
places['geonames_place_name'] = places['geonames'].apply(lambda x: [geonames_resp[e] for e in x if not(isinstance(e, float))] if not(isinstance(x, float)) else x)   

geonames_resp = {}
with ThreadPoolExecutor(max_workers=50) as executor:
    list(tqdm(executor.map(get_geonames_coordinates, geonames_ids), total=len(geonames_ids)))
places['geonames_lat'] = places['geonames'].apply(lambda x: [geonames_resp[e]['lat'] for e in x if not(isinstance(e, float))] if not(isinstance(x, float)) else x)   
places['geonames_lng'] = places['geonames'].apply(lambda x: [geonames_resp[e]['lng'] for e in x if not(isinstance(e, float))] if not(isinstance(x, float)) else x)  

#wybrać właściwe kolumny

places = places[['001', 'geonames', 'geonames_place_name', 'geonames_country', 'geonames_lat', 'geonames_lng']].rename(columns={'geonames':'geonames_id','geonames_place_name':'geonames_name'})

places = places.loc[places['geonames_id'].notnull()]

places_to_replace = places.groupby('001').head(1).reset_index(drop=True)
places_to_replace.index = places_to_replace['001']
places_to_replace.drop(columns='001',inplace=True)
places_to_replace = places_to_replace.to_dict(orient='index')


groups_dict2 = {k:v for k,v in groups_dict.items() if any(e in v for e in places_to_replace.keys())}
groups_dict2 = {k:[places_to_replace.get(e) for e in v] for k,v in groups_dict2.items()}

test = {k:[dict((a,list(b)) for a,b in e) for e in list(set([tuple(((a,tuple(b)) for a,b in e.items())) for e in v if e]))] for k,v in groups_dict2.items()}

from collections import defaultdict

new_dict = {}
for k,v in test.items():
    dd = defaultdict(list)
    for d in v:
        for key, value in d.items():
            dd[key].append(value)
    dd = {ka:[e for sub in va for e in sub] for ka,va in dd.items()}
    new_dict.update({k:dd})

#teraz muszę wpisać dd to tabeli

#to rozwiązanie!!!
for k,v in new_dict.items():
    for ka, va in v.items():
        # places.at[places.loc[places['001'] == 561629681].index.values[0], 'places'] = ['London', 'Glasgow']
        translations_df.at[translations_df.loc[translations_df['001'] == k].index.values[0], ka] = va
        #tutaj muszę zastosować .at; vide: places.at[places.loc[places['001'] == 561629681].index.values[0], 'places'] = ['London', 'Glasgow']

translations_df.to_excel(f'translation_before_manual_{now}.xlsx', index=False)    






























