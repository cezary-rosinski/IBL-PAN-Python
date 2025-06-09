import sys
import requests
from tqdm import tqdm
from my_functions import gsheet_to_df, simplify_string, cluster_strings, marc_parser_to_dict
from concurrent.futures import ThreadPoolExecutor
import json
import pandas as pd
from SPARQLWrapper import SPARQLWrapper, JSON
from ast import literal_eval
from typing import Optional
import regex as re
import time
from urllib.error import HTTPError, URLError
from geonames_accounts import geonames_users
import random

#%% def
def get_wikidata_label(wikidata_id, pref_langs = ['pl', 'en', 'fr', 'de', 'es', 'cs']):
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

def harvest_wikidata_for_person(wikidata_id):
# for wikidata_id in tqdm(list(wikidata_ids)[565:570]):
    #wikidata_id = list(wikidata_ids)[1]
    #wikidata_id = "Q192236"
    try:
        url = f'https://www.wikidata.org/wiki/Special:EntityData/{wikidata_id}.json'
        result = requests.get(url).json()
        
        try:
            birthdate_value = result.get('entities').get(wikidata_id).get('claims').get('P569')[0].get('mainsnak').get('datavalue').get('value').get('time')[:5]
        except TypeError:
            birthdate_value = None
        except AttributeError:
            try:
                birthdate_value = result.get('entities').get(wikidata_id).get('claims').get('P569')[0].get('qualifiers').get('P1319')[0].get('datavalue').get('value').get('time')[:5]
            except TypeError:
                birthdate_value = result.get('entities').get(wikidata_id).get('claims').get('P569')[0].get('qualifiers').get('P1326')[0].get('datavalue').get('value').get('time')[:5]
            except AttributeError:
                birthdate_value = None
                
        try:
            deathdate_value = result.get('entities').get(wikidata_id).get('claims').get('P570')[0].get('mainsnak').get('datavalue').get('value').get('time')[:5]
        except TypeError:
            deathdate_value = None
        except AttributeError:
            try:
                deathdate_value = result.get('entities').get(wikidata_id).get('claims').get('P570')[0].get('qualifiers').get('P1319')[0].get('datavalue').get('value').get('time')[:5]
            except TypeError:
                deathdate_value = result.get('entities').get(wikidata_id).get('claims').get('P570')[0].get('qualifiers').get('P1326')[0].get('datavalue').get('value').get('time')[:5]
            except AttributeError:
                deathdate_value = None
            
        try:
            birth = result.get('entities').get(wikidata_id).get('claims').get('P19')
            birthplace_value = max(birth, key=len).get('mainsnak').get('datavalue').get('value').get('id')
        except (AttributeError, TypeError):
            birthplace_value = None
        if birthplace_value:
            try:
                birthplaceLabel_value = get_wikidata_label(birthplace_value)
            except TypeError:
                birthplaceLabel_value = None 
        else: birthplaceLabel_value = None
        try:
            death = result.get('entities').get(wikidata_id).get('claims').get('P20')
            deathplace_value = max(death, key=len).get('mainsnak').get('datavalue').get('value').get('id')
        except (AttributeError, TypeError):
            deathplace_value = None
        if deathplace_value:
            try:
                deathplaceLabel_value = get_wikidata_label(deathplace_value)
            except TypeError:
                deathplaceLabel_value = None
        else: deathplaceLabel_value = None
         
        temp_dict = {'person_wikidata': wikidata_id,
                     'birthdate.value': birthdate_value,
                     'deathdate.value': deathdate_value,
                     'birthplace_wikidata': birthplace_value,
                     'birthplaceLabel.value': birthplaceLabel_value,
                     'deathplace_value': deathplace_value,
                     'deathplaceLabel.value': deathplaceLabel_value}
        wikidata_supplement.append(temp_dict)
    except ValueError:
        errors.append(wikidata_id)
 

def most_productive_century(
    birth_year: Optional[int],
    death_year: Optional[int]
) -> Optional[int]:

    # 1) brak obu lat → None
    if birth_year is None and death_year is None:
        return None

    # 2) obliczamy start/end aktywności
    if birth_year is not None and death_year is not None:
        start = birth_year + 25
        end   = death_year
    elif birth_year is None:
        # tylko death_year: punktowy rok aktywności w death_year
        start = death_year
        end   = death_year + 1
    else:
        # tylko birth_year: punktowy rok aktywności w birth_year+25
        start = birth_year + 25
        end   = start + 1

    # jeśli przedział jest pusty lub dziwny → None
    if start >= end:
        return None

    def century_of(y: int) -> int:
        # dla y > 0: (y-1)//100 + 1
        # dla y <= 0: -( ((-y)-1)//100 + 1 )
        if y > 0:
            return (y - 1) // 100 + 1
        else:
            return -(((-y) - 1) // 100 + 1)

    def century_bounds(c: int) -> (int, int):
        # dla c>0: [(c-1)*100+1, c*100+1)
        # dla c<0: astronomicznie odpowiednie BC
        if c > 0:
            return ((c - 1) * 100 + 1, c * 100 + 1)
        else:
            absC = -c
            start_bc = -(absC * 100 - 1)
            end_bc   = -((absC - 1) * 100 - 1)
            return (start_bc, end_bc)

    # obliczamy stulecia dla granic aktywności
    c_start = century_of(start)
    c_end   = century_of(end - 1)

    # --- SPECIAL CASE: cały zakres w jednym stuleciu? ---
    if c_start == c_end:
        return c_start

    # w przeciwnym razie iterujemy po stuleciach od c_start do c_end
    step = 1 if c_end >= c_start else -1
    overlaps = {}

    for c in range(c_start, c_end + step, step):
        s_c, e_c = century_bounds(c)
        ov_start = max(start, s_c)
        ov_end   = min(end,   e_c)
        overlaps[c] = max(0, ov_end - ov_start)

    if not overlaps:
        return None

    # wybieramy max lata i w razie remisu największy numer stulecia
    max_years = max(overlaps.values())
    candidates = [c for c, yrs in overlaps.items() if yrs == max_years]
    return max(candidates)

#%% people

df_people = gsheet_to_df('1M2gc-8cGZ8gh8TTnm4jl430bL4tccdri9nw-frUWYLQ', 'people')
wikidata_ids = set([e for e in df_people['person_wikidata_id'].to_list() if isinstance(e, str)])

errors = []
wikidata_supplement = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(harvest_wikidata_for_person, wikidata_ids),total=len(wikidata_ids)))

df_people_wikidata = pd.DataFrame(wikidata_supplement)

# [e for e in wikidata_supplement if e.get('person_wikidata') == 'Q5493177']

# df_people_wikidata.loc[df_people_wikidata['person_wikidata'] == 'Q5493177']

# harvest_wikidata_for_person('Q192236')

#%% productivity period
df_people = gsheet_to_df('1M2gc-8cGZ8gh8TTnm4jl430bL4tccdri9nw-frUWYLQ', 'people')

# wikidata_supplement = []
# for i, row in df_people.iterrows():
#     if isinstance(row['person_wikidata_id'], str):
#         temp_dict = {'wikidata_id': row['person_wikidata_id'],
#                      'birthdate': row['birthdate'],
#                      'deathdate': row['deathdate']}
#         wikidata_supplement.append(temp_dict)
        
wikidata_supplement = []
for i, row in df_people.iterrows():
    if isinstance(row['person_id'], str):
        temp_dict = {'person_id': row['person_id'],
                     'birthdate': row['birthdate'],
                     'deathdate': row['deathdate']}
        wikidata_supplement.append(temp_dict)

productivity = []
for p in wikidata_supplement:
    # p = wikidata_supplement[2]
    try:
        if p.get('birthdate')[0].isnumeric():
            b = int(p.get('birthdate').split('-')[0])
        else: b = -int(p.get('birthdate').split('-')[1])
    except TypeError:
        b = None
    try:
        if p.get('deathdate')[0].isnumeric():
            d = int(p.get('deathdate').split('-')[0])
        else: d = -int(p.get('deathdate').split('-')[1])
    except TypeError:
        d = None
    productivity.append({'person_id': p.get('person_id'),
                         'century': most_productive_century(b, d)})
    
df_productivity = pd.DataFrame(productivity)






# — Przykłady —

# 1) pełne dane:
print(most_productive_century(-102, 102))  # → 16 (XVI w.)

# 2) brak birth_year, jest death_year=1605:
#    aktywność tylko w 1605 → XVII w. (1601–1700)
print(most_productive_century(None, 1605)) # → 17

# 3) brak death_year, birth_year=1500:
#    aktywność w 1525 → XVI w. (1501–1600)
print(most_productive_century(1500, None)) # → 16

# 4) brak obu lat:
print(most_productive_century(None, None)) # → None


#%% events

df_texts = gsheet_to_df('1M2gc-8cGZ8gh8TTnm4jl430bL4tccdri9nw-frUWYLQ', 'texts')
events = set([ele for sub in [[el.strip() for el in e.split(';')] for e in df_texts['Tumults and Mentioned'].to_list() if pd.notnull(e) and e != "No Data"] for ele in sub])

tumults = []
for e in events:
    search = e
    place = ' '.join(e.split(' ')[:-1])
    year = e.split(' ')[-1]
    temp_dict = {'name': search,
                 'place': place,
                 'year': year,
                 'type': 'tumult'}
    tumults.append(temp_dict)
    
df_tumults = pd.DataFrame(tumults)

df_events = gsheet_to_df('1M2gc-8cGZ8gh8TTnm4jl430bL4tccdri9nw-frUWYLQ', 'events')
tum_ids = dict(zip(df_events['name'].to_list(), df_events['event_id'].to_list()))

tum = [[el.strip() for el in e.split(';')] if isinstance(e, str) else None for e in df_texts['Tumults and Mentioned'].to_list()]

tums = []
for i, t in enumerate(tum):
    if t:
        tums.append('; '.join([tum_ids.get(e, 'no Wikidata ID') for e in t]))
    else: tums.append(None)
tums = [e if e != "no Wikidata ID" else None for e in tums]

tum_df = pd.DataFrame(tums)

#%% places

# places from texts
df_texts = gsheet_to_df('1M2gc-8cGZ8gh8TTnm4jl430bL4tccdri9nw-frUWYLQ', 'texts')
places_texts = set([e for e in df_texts['Place'].to_list() if e != 'No Data'])

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
    list(tqdm(excecutor.map(query_geonames, places_texts),total=len(places_texts)))

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

# places from people
df_people = gsheet_to_df('1M2gc-8cGZ8gh8TTnm4jl430bL4tccdri9nw-frUWYLQ', 'people')
b_places = set([e for e in df_people['birthplace_wikidata'].to_list() if pd.notnull(e)])
b_df = pd.DataFrame(b_places)
d_places = set([e for e in df_people['deathplace_wikidata'].to_list() if pd.notnull(e)])
d_df = pd.DataFrame(d_places)

# places from events
df_events = gsheet_to_df('1M2gc-8cGZ8gh8TTnm4jl430bL4tccdri9nw-frUWYLQ', 'events')
t_places = set(df_events['place'].to_list())

places_with_geonames = {}
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(query_geonames, t_places),total=len(t_places)))
    
for k,v in tqdm(places_with_geonames.items()):
    if v:
        geonames_id = v.get('geonameId')
        qid = get_wikidata_qid_from_geonames(geonames_id)
        v.update({'wikidataID': qid})

for k,v in tqdm(places_with_geonames.items()):
    if v:
        v.update({'searchName': k})

places_with_geonames = {k:v for k,v in places_with_geonames.items() if v}
df_places = pd.DataFrame(places_with_geonames.values())

# places together

df_places = gsheet_to_df('1M2gc-8cGZ8gh8TTnm4jl430bL4tccdri9nw-frUWYLQ', 'places_roboczy')
places_ids = set(df_places['wikidata_id'].to_list())

def get_wikidata_label(wikidata_id, pref_langs = ['en', 'pl', 'fr', 'de']):
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

def harvest_wikidata_for_place(wikidata_id):
    # wikidata_id = 'Q64'
    url = f'https://www.wikidata.org/wiki/Special:EntityData/{wikidata_id}.json'
    result = requests.get(url).json()
    try:
        name = result.get('entities').get(wikidata_id).get('labels').get('en').get('value')
    except AttributeError:
        try:
            name = result.get('entities').get(wikidata_id).get('labels').get('pl').get('value')
        except AttributeError:
            try:
                name = result.get('entities').get(wikidata_id).get('labels').get('fr').get('value')
            except AttributeError:
                try:
                    name = result.get('entities').get(wikidata_id).get('labels').get('de').get('value')
                except AttributeError:
                    name = 'no place name'
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
    places_result.append(temp_dict)

# places_result = []
# for p in tqdm(places_ids):
#     places_result.append(harvest_wikidata_for_place(p))
    
places_result = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(harvest_wikidata_for_place, places_ids),total=len(places_ids)))
    
places_df = pd.DataFrame(places_result)
























