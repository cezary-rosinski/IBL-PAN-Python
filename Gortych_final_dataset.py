import requests
from tqdm import tqdm
import numpy as np
from my_functions import gsheet_to_df, simplify_string, cluster_strings, marc_parser_to_dict
from concurrent.futures import ThreadPoolExecutor
import json
import glob
import random
import time
from datetime import datetime
import Levenshtein as lev
import io
import pandas as pd
from bs4 import BeautifulSoup
import pickle
from SPARQLWrapper import SPARQLWrapper, JSON
import sys
import time
from urllib.error import HTTPError, URLError
import geopandas as gpd
import geoplot
import geoplot.crs as gcrs
from shapely.geometry import shape, Point
import sys
sys.path.insert(1, 'C:/Users/Cezary/Documents/IBL-PAN-Python')
from geonames_accounts import geonames_users

#%%
def viaf_autosuggest(query):
    url = "https://viaf.org/viaf/AutoSuggest"
    params = {"query": query}
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)", 
        "Accept-Encoding": "gzip", 
        "Accept": "application/json"
    }

    response = requests.get(url, headers=headers, params=params)

    if response.status_code == 200:
        return response.json()
    else:
        print(f"Błąd {response.status_code}: {response.text}")
        return None

def get_wikidata_qid_from_viaf(viaf_id):
    #viaf_id = '9891285'
    # endpoint_url = "https://query.wikidata.org/sparql"
    query = f"""
    SELECT ?item WHERE {{
      ?item wdt:P214 "{viaf_id}".
    }}
    """
    user_agent = "WDQS-example Python/%s.%s" % (sys.version_info[0], sys.version_info[1])
    sparql = SPARQLWrapper("https://query.wikidata.org/sparql", agent=user_agent)
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)
    while True:
        try:
            data = sparql.query().convert()
            break
        except HTTPError:
            time.sleep(2)
        except URLError:
            time.sleep(5)
    try:
        qid_url = data["results"]["bindings"][0]["item"]["value"]
        qid = qid_url.split("/")[-1]
        return qid
    except (IndexError, KeyError):
        return None

# # Przykład użycia
# viaf_id = "59157273"  # zamień na swój identyfikator
# qid = get_wikidata_qid_from_viaf(viaf_id)
# print(f"QID: {qid}")

#%% persons ver 1
df_novels = gsheet_to_df('1iU-u4xjotqa3ZLijF5bMU7xWv-Hxgq1n8N3i-7UCdfU', 'novels')

authors = set(df_novels['author'].to_list())
authors_unique = [e.split(', ') for e in authors]
authors_unique = set([e.strip() for sub in authors_unique for e in sub])

authors_ids = []
for author in tqdm(authors_unique):
    # author = list(authors)[0]
    r = [e for e in viaf_autosuggest(author).get('result') if e.get('nametype') == 'personal'][0]
    author_record = {k:v for k,v in r.items() if k in ['displayForm', 'viafid']}
    author_viaf = author_record.get('viafid')
    author_wikidata = get_wikidata_qid_from_viaf(author_viaf)
    author_record.update({'searchName': author, 'wikidataID': author_wikidata, 'wikidata_uri': f"https://www.wikidata.org/wiki/{author_wikidata}" if author_wikidata else None, 'viaf_uri': f"https://viaf.org/en/viaf/{author_viaf}"})
    authors_ids.append(author_record)

authors_df = pd.DataFrame(authors_ids)

#%% persons ver 2

df_novels = gsheet_to_df('1iU-u4xjotqa3ZLijF5bMU7xWv-Hxgq1n8N3i-7UCdfU', 'authors')

def get_wikidata_label(wikidata_id):
    languages = ['pl', 'en', 'fr', 'de', 'es', 'cs']
    url = f'https://www.wikidata.org/wiki/Special:EntityData/{wikidata_id}.json'
    try:
        result = requests.get(url).json()
        for lang in languages:
            label = result['entities'][wikidata_id]['labels'][lang]['value']
            break
    except ValueError:
        label = None
    return label 

def harvest_wikidata(wikidata_id):
    wikidata_id = 'Q1097549'
    url = f'https://www.wikidata.org/wiki/Special:EntityData/{wikidata_id}.json'
    result = requests.get(url).json()
    try:
        birthdate_value = result.get('entities').get(wikidata_id).get('claims').get('P569')[0].get('mainsnak').get('datavalue').get('value').get('time')[1:11]
    except TypeError:
        birthdate_value = None
    try:
        deathdate_value = result.get('entities').get(wikidata_id).get('claims').get('P570')[0].get('mainsnak').get('datavalue').get('value').get('time')[1:11]
    except TypeError:
        deathdate_value = None
    try:
        birthplaceLabel_value = get_wikidata_label(result.get('entities').get(wikidata_id).get('claims').get('P19')[0].get('mainsnak').get('datavalue').get('value').get('id'))
    except TypeError:
        birthplaceLabel_value = None
    try:
        birthplace_value = result.get('entities').get(wikidata_id).get('claims').get('P19')[0].get('mainsnak').get('datavalue').get('value').get('id')
    except TypeError:
        birthplace_value = None    
        
        
    try:
        deathplaceLabel_value = get_wikidata_label(result.get('entities').get(wikidata_id).get('claims').get('P20')[0].get('mainsnak').get('datavalue').get('value').get('id'))
    except TypeError:
        deathplaceLabel_value = None
    try:
        sexLabel_value = get_wikidata_label(result.get('entities').get(wikidata_id).get('claims').get('P21')[0].get('mainsnak').get('datavalue').get('value').get('id'))
    except TypeError:
        sexLabel_value = None
    try:
        pseudonym_value = '❦'.join([e.get('mainsnak').get('datavalue').get('value') for e in result.get('entities').get(wikidata_id).get('claims').get('P742')])
    except (TypeError, AttributeError):
        pseudonym_value = None
    try:
        occupationLabel_value = '❦'.join([get_wikidata_label(e.get('mainsnak').get('datavalue').get('value').get('id')) for e in result.get('entities').get(wikidata_id).get('claims').get('P106')])
    except AttributeError:
        occupationLabel_value = None
    temp_dict = {wikidata_id: {'autor.value': f'http://www.wikidata.org/entity/{wikidata_id}',
                               'birthdate.value': birthdate_value,
                               'deathdate.value': deathdate_value,
                               'birthplaceLabel.value': birthplaceLabel_value,
                               'deathplaceLabel.value': deathplaceLabel_value,
                               'sexLabel.value': sexLabel_value,
                               'pseudonym.value': pseudonym_value,
                               'occupationLabel.value': occupationLabel_value}}
    

wikidata_supplement = {}

for wikidata_id in tqdm(new_wikidata_ids):
    # wikidata_id = new_wikidata_ids[0]
    # wikidata_id = 'Q240174'
    url = f'https://www.wikidata.org/wiki/Special:EntityData/{wikidata_id}.json'
    result = requests.get(url).json()
    try:
        birthdate_value = result.get('entities').get(wikidata_id).get('claims').get('P569')[0].get('mainsnak').get('datavalue').get('value').get('time')[1:11]
    except TypeError:
        birthdate_value = None
    try:
        deathdate_value = result.get('entities').get(wikidata_id).get('claims').get('P570')[0].get('mainsnak').get('datavalue').get('value').get('time')[1:11]
    except TypeError:
        deathdate_value = None
    try:
        birthplaceLabel_value = get_wikidata_label(result.get('entities').get(wikidata_id).get('claims').get('P19')[0].get('mainsnak').get('datavalue').get('value').get('id'))
    except TypeError:
        birthplaceLabel_value = None
    try:
        deathplaceLabel_value = get_wikidata_label(result.get('entities').get(wikidata_id).get('claims').get('P20')[0].get('mainsnak').get('datavalue').get('value').get('id'))
    except TypeError:
        deathplaceLabel_value = None
    try:
        sexLabel_value = get_wikidata_label(result.get('entities').get(wikidata_id).get('claims').get('P21')[0].get('mainsnak').get('datavalue').get('value').get('id'))
    except TypeError:
        sexLabel_value = None
    try:
        pseudonym_value = '❦'.join([e.get('mainsnak').get('datavalue').get('value') for e in result.get('entities').get(wikidata_id).get('claims').get('P742')])
    except (TypeError, AttributeError):
        pseudonym_value = None
    try:
        occupationLabel_value = '❦'.join([get_wikidata_label(e.get('mainsnak').get('datavalue').get('value').get('id')) for e in result.get('entities').get(wikidata_id).get('claims').get('P106')])
    except AttributeError:
        occupationLabel_value = None
    temp_dict = {wikidata_id: {'autor.value': f'http://www.wikidata.org/entity/{wikidata_id}',
                               'birthdate.value': birthdate_value,
                               'deathdate.value': deathdate_value,
                               'birthplaceLabel.value': birthplaceLabel_value,
                               'deathplaceLabel.value': deathplaceLabel_value,
                               'sexLabel.value': sexLabel_value,
                               'pseudonym.value': pseudonym_value,
                               'occupationLabel.value': occupationLabel_value}}
    wikidata_supplement.update(temp_dict)

#%% persons ver 3 -- occupation and historical background

df_novels = gsheet_to_df('1iU-u4xjotqa3ZLijF5bMU7xWv-Hxgq1n8N3i-7UCdfU', 'authors')
occupation = set([ele for sub in [[el.strip().lower() for el in e.split ('❦')] for e in df_novels['occupation'] if isinstance(e,str)] for ele in sub])
df_occupation = pd.DataFrame(occupation)


# data = gpd.read_file("https://raw.githubusercontent.com/isellsoap/deutschlandGeoJSON/main/1_deutschland/1_sehr_hoch.geo.json")
# data.crs

# geoplot.polyplot(data, projection=gcrs.AlbersEqualArea(), edgecolor='darkgrey', facecolor='lightgrey', linewidth=.3, figsize=(12, 8))


with open(r"C:\Users\Cezary\Downloads\germany_1_sehr_hoch.geo.json", 'r', encoding='utf-8') as f:
    germany_json = json.load(f)
with open(r"C:\Users\Cezary\Downloads\bundeslands_1_sehr_hoch.geo.json", 'r', encoding='utf-8') as f:
    germany_states_json = json.load(f)

# berlin_point = Point(13.383333, 52.516667)
# warsaw_point = Point(21.011111, 52.23)

# polygon.contains(berlin_point)
# polygon.contains(warsaw_point)


lands_dict = {'Baden-Württemberg': 'West Germany',
              'Bayern': 'West Germany',
              'Berlin': 'Berlin',
              'Brandenburg': 'East Germany',
              'Bremen': 'West Germany',
              'Hamburg': 'West Germany',
              'Hessen': 'West Germany',
              'Mecklenburg-Vorpommern': 'East Germany',
              'Niedersachsen': 'West Germany',
              'Nordrhein-Westfalen': 'West Germany',
              'Rheinland-Pfalz': 'West Germany',
              'Saarland': 'West Germany',
              'Sachsen-Anhalt': 'East Germany',
              'Sachsen': 'East Germany',
              'Schleswig-Holstein': 'West Germany',
              'Thüringen': 'East Germany'}

polygon_germany = shape(germany_json.get('features')[0].get('geometry'))
polygons_lands_dict = dict(zip([e.get('properties').get('name') for e in germany_states_json.get('features')], [e.get('geometry') for e in germany_states_json.get('features')]))
polygons_lands_dict = {k:shape(v) for k,v in polygons_lands_dict.items()}

wikidata_places = set([e for e in df_novels['birthplace_id'].to_list() if isinstance(e, str)])

historical_background = []
for wikidata_id in tqdm(wikidata_places):
    # wikidata_id = list(wikidata_places)[0]
    
    url = f'https://www.wikidata.org/wiki/Special:EntityData/{wikidata_id}.json'
    try:
        result = requests.get(url).json()

        longitude = result.get('entities').get(wikidata_id).get('claims').get('P625')[0].get('mainsnak').get('datavalue').get('value').get('longitude')
        latitude = result.get('entities').get(wikidata_id).get('claims').get('P625')[0].get('mainsnak').get('datavalue').get('value').get('latitude')
        point = Point(longitude, latitude)
    
        for k,v in polygons_lands_dict.items():
            if v.contains(point):
                historical_background.append({'wikidata': wikidata_id, 'historical background': lands_dict.get(k)})
    except TypeError:
        pass
            
historical_background_df = pd.DataFrame(historical_background)

#%% persons ver 4 -- occupation 

df_authors = gsheet_to_df('1iU-u4xjotqa3ZLijF5bMU7xWv-Hxgq1n8N3i-7UCdfU', 'authors')

occupation_dict = gsheet_to_df('1iU-u4xjotqa3ZLijF5bMU7xWv-Hxgq1n8N3i-7UCdfU', 'occupation')

occupation_dict = dict(zip(occupation_dict['jest'].to_list(), occupation_dict['ma być'].to_list()))

occupation_old = df_authors['occupation_old_wikidata'].to_list()

occupation_result = []
for o_field in occupation_old:
    o_cell = []
    for o in o_field.split('❦'):
        o_cell.append(occupation_dict.get(o))
    o_cell = '❦'.join(set([e for e in o_cell if isinstance(e, str)]))
    occupation_result.append(o_cell)
    
df_occupation = pd.DataFrame(occupation_result)

            
#%% institutions 

df_novels = gsheet_to_df('1iU-u4xjotqa3ZLijF5bMU7xWv-Hxgq1n8N3i-7UCdfU', 'novels')

publishers = set(df_novels['publisher'].to_list())

publishers_ids = []
for p in tqdm(publishers):
    # p = list(publishers)[0]
    # test = viaf_autosuggest(p)
    try:
        r = [e for e in viaf_autosuggest(p).get('result') if e.get('nametype') == 'corporate'][0]
        publisher_record = {k:v for k,v in r.items() if k in ['displayForm', 'viafid']}
        publisher_viaf = publisher_record.get('viafid')
        publisher_wikidata = get_wikidata_qid_from_viaf(publisher_viaf)
        publisher_record.update({'searchName': p, 'wikidataID': publisher_wikidata, 'wikidata_uri': f"https://www.wikidata.org/wiki/{publisher_wikidata}" if publisher_wikidata else None, 'viaf_uri': f"https://viaf.org/en/viaf/{publisher_viaf}"})
        publishers_ids.append(publisher_record)
    except TypeError:
        publishers_ids.append({'searchName': p})
        
publishers_df = pd.DataFrame(publishers_ids)

#%% places

df_novels = gsheet_to_df('1iU-u4xjotqa3ZLijF5bMU7xWv-Hxgq1n8N3i-7UCdfU', 'novels')

df_authors = gsheet_to_df('1iU-u4xjotqa3ZLijF5bMU7xWv-Hxgq1n8N3i-7UCdfU', 'authors')

places_novels = set([ele for sub in [[el.strip() for el in e.split(';')] for e in df_novels['place'].to_list()] for ele in sub])

def query_geonames(m):
    # m = 'Dublin'
    url = 'http://api.geonames.org/searchJSON?'
    params = {'username': random.choice(geonames_users), 'q': m, 'featureClass': 'P', 'style': 'FULL'}
    result = requests.get(url, params=params).json()
    if 'status' in result:
        time.sleep(5)
        query_geonames(m)
    else:
        try:
            geonames_resp = {k:v for k,v in max(result['geonames'], key=lambda x:x['score']).items() if k in ['geonameId', 'name', 'lat', 'lng']}
        except ValueError:
            geonames_resp = None
        places_with_geonames[m] = geonames_resp

places_with_geonames = {}
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(query_geonames, places_novels),total=len(places_novels)))

def get_wikidata_qid_from_geonames(geonames_id):
    #geonames_id = 2950159
    query = f"""
PREFIX wdt: <http://www.wikidata.org/prop/direct/>

SELECT ?item WHERE {{
  ?item wdt:P1566 "{geonames_id}" .
}}
"""
    user_agent = "WDQS-example Python/%s.%s" % (sys.version_info[0], sys.version_info[1])
    sparql = SPARQLWrapper("https://query.wikidata.org/sparql", agent=user_agent)
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)
    while True:
        try:
            data = sparql.query().convert()
            break
        except HTTPError:
            time.sleep(2)
        except URLError:
            time.sleep(5)
    try:
        qid_url = data["results"]["bindings"][0]["item"]["value"]
        qid = qid_url.split("/")[-1]
        return qid
    except (IndexError, KeyError):
        return None

for k,v in tqdm(places_with_geonames.items()):
    geonames_id = v.get('geonameId')
    qid = get_wikidata_qid_from_geonames(geonames_id)
    v.update({'wikidataID': qid})

for k,v in tqdm(places_with_geonames.items()):
    v.update({'searchName': k})

df_places = pd.DataFrame(places_with_geonames.values())

#%% places ver. 2

df_places_from_novels = gsheet_to_df('1iU-u4xjotqa3ZLijF5bMU7xWv-Hxgq1n8N3i-7UCdfU', 'places_from_novels')

df_authors = gsheet_to_df('1iU-u4xjotqa3ZLijF5bMU7xWv-Hxgq1n8N3i-7UCdfU', 'authors')

places_1_novels = set(df_places_from_novels['wikidataID'].to_list())
places_2_birthplace = set(df_authors['birthplace_id'].to_list())
places_3_deathplace = set(df_authors['deathplace_id'].to_list())

places_ids = set.union(*[places_1_novels, places_2_birthplace, places_3_deathplace])
places_ids = [e for e in places_ids if isinstance(e, str)]

def harvest_wikidata_for_place(wikidata_id):
    # wikidata_id = 'Q64'
    url = f'https://www.wikidata.org/wiki/Special:EntityData/{wikidata_id}.json'
    result = requests.get(url).json()
    try:
        name = result.get('entities').get(wikidata_id).get('labels').get('de').get('value')
    except AttributeError:
        name = result.get('entities').get(wikidata_id).get('labels').get('en').get('value')
    try:
        latitude = result.get('entities').get(wikidata_id).get('claims').get('P625')[0].get('mainsnak').get('datavalue').get('value').get('latitude')
        longitude = result.get('entities').get(wikidata_id).get('claims').get('P625')[0].get('mainsnak').get('datavalue').get('value').get('longitude')
    except TypeError:
        latitude = None
        longitude = None
        print(f'{wikidata_id}, {name}, no coordinates')
    
    temp_dict = {'wikidata_id': wikidata_id,
                 'wikidata_url': f'https://www.wikidata.org/wiki/{wikidata_id}',
                 'name': name,
                 'latitude': latitude,
                 'longitude': longitude}
    return temp_dict

places_result = []

for p in tqdm(places_ids):
    places_result.append(harvest_wikidata_for_place(p))
    
places_df = pd.DataFrame(places_result)
places_df.loc[places_df['name'] == 'Laas', 'latitude'] = 46.94278
places_df.loc[places_df['name'] == 'Laas', 'longitude'] = 13.07694
#Laas: 46.94278, 13.07694

#%% prizes from authors

df_authors = gsheet_to_df('1iU-u4xjotqa3ZLijF5bMU7xWv-Hxgq1n8N3i-7UCdfU', 'authors')

authors_ids = [e for e in df_authors['wikidataID'].to_list() if isinstance(e, str)]

def get_wikidata_label_for_prize(wikidata_id, pref_langs = ['de', 'en']):
    # wikidata_id = 'Q130690218'
    url = f'https://www.wikidata.org/wiki/Special:EntityData/{wikidata_id}.json'
    try:
        result = requests.get(url).json()
        langs = [e for e in list(result.get('entities').get(wikidata_id).get('labels').keys()) if e in pref_langs]
        if langs:
            for lang in langs:
                label = result['entities'][wikidata_id]['labels'][lang]['value']
                break
        else: label = None
    except ValueError:
        label = None
    return label 

def harvest_wikidata_author_for_prize(wikidata_id):
    # wikidata_id = authors_ids[0]
    # wikidata_id = 'Q254032'
    # wikidata_id = 'Q1477082'
    url = f'https://www.wikidata.org/wiki/Special:EntityData/{wikidata_id}.json'
    result = requests.get(url).json()
    
    awards = result.get('entities').get(wikidata_id).get('claims').get('P166')
    if awards:
        awards_result = []
        for a in awards:
            # a = awards[3]
            prize_id = a.get('mainsnak').get('datavalue').get('value').get('id')
            prize_name = get_wikidata_label_for_prize(prize_id)
            try:
                year = a.get('qualifiers').get('P585')[0].get('datavalue').get('value').get('time')[1:5]
            except TypeError:
                year = a.get('qualifiers').get('P582')[0].get('datavalue').get('value').get('time')[1:5]
            except AttributeError:
                year = None
            temp_dict = {'person_id': wikidata_id,
                         'prize_id': prize_id,
                         'prize_name': prize_name,
                         'prize_year': year}
            awards_result.append(temp_dict)
        return awards_result

prizes_final_results = []
for author in tqdm(authors_ids):
    prize_for_person_result = harvest_wikidata_author_for_prize(author)
    if prize_for_person_result:
        prizes_final_results.extend(prize_for_person_result)
    
prizes_df = pd.DataFrame(prizes_final_results) 





















