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

#%% date
now = datetime.datetime.now()
year = now.year
month = '{:02}'.format(now.month)
day = '{:02}'.format(now.day)

#%% geonames harvesting
translations_df = pd.read_excel('translation_deduplicated_2022-03-28.xlsx', sheet_name='phase_4')
with open('MARC21_country_codes.txt', ) as f:
    country_codes = f.read().splitlines()

country_codes = [e.split('\t') for e in country_codes]
country_codes = dict(zip([e[0] for e in country_codes], [e[-1] for e in country_codes]))

places = translations_df[['001', '008', '260']]
places['country'] = places['008'].apply(lambda x: x[15:18])
places.drop(columns='008', inplace=True)
places['country'] = places['country'].str.replace('\\', '', regex=False)
places['country_name'] = places['country'].apply(lambda x: country_codes[x] if x in country_codes else 'unknown')

# places['places'] = places['260'].str.replace(' - ', '$a')
places['places'] = places['260'].apply(lambda x: re.sub('( : )(?!\$)', r' $b', x) if pd.notnull(x) else x)
places['places'] = places['places'].apply(lambda x: re.sub('( ; )(?!\$)', r' $a', x) if pd.notnull(x) else x)
places['places'] = places['places'].apply(lambda x: [list(e.values())[0] for e in marc_parser_dict_for_field(x, '\$') if '$a' in e] if pd.notnull(x) else x)
places['places'] = places['places'].apply(lambda x: x if x else np.nan)

test = places[places['places'].apply(lambda x: any([bool(re.search('\d', e)) for e in x]) if not(isinstance(x, float)) else False)]
test['places'] = test['places'].apply(lambda x: [el for sub in [re.split('\d+', e) for e in x] for el in sub if el])
places = places[~places['001'].isin(test['001'])]
places = pd.concat([places, test])

test = places[places['places'].apply(lambda x: any([bool(re.search(' - ', e)) for e in x]) if not(isinstance(x, float)) else False)]
test['places'] = test['places'].apply(lambda x: [el for sub in [e.split(' - ') for e in x] for el in sub if el])
places = places[~places['001'].isin(test['001'])]
places = pd.concat([places, test])

test = places[places['places'].apply(lambda x: any([bool(re.search(', ', e)) for e in x]) if not(isinstance(x, float)) else False)]
test['places'] = test['places'].apply(lambda x: [el for sub in [e.split(', ') for e in x] for el in sub if el])
places = places[~places['001'].isin(test['001'])]
places = pd.concat([places, test])

test = places[places['places'].apply(lambda x: any([bool(re.search('\[', e)) for e in x]) if not(isinstance(x, float)) else False)]
test['places'] = test['places'].apply(lambda x: [el for sub in [e.split('[') for e in x] for el in sub if el])
places = places[~places['001'].isin(test['001'])]
places = pd.concat([places, test])

places['simple'] = places['places'].apply(lambda x: [simplify_string(e).strip() for e in x] if (isinstance(x, list) and pd.notnull(x).all()) else x if pd.notnull(x) else x)

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

places_dict = {k:{ka:va if ka=='records' else Counter(va).most_common(1)[0][0] for ka, va in v.items()} for k,v in places_dict.items()}
               
frequency = {k:len(v['records']) for k,v in places_dict.items()}

places_ordered = [e for e in sorted([e for e in frequency], key=frequency.get, reverse=True)]

places_clusters = cluster_strings(places_ordered, 0.65)

# {k:v for k,v in places_clusters.items() if 'berlin ost' in v}

#sprawdzić zilina czechoslovakia, zu beziehen ostermundigen, zurich  prag, zurich postfach, zurich seefeldstr

places_geonames = {}
users_index = 0

for place in tqdm(places_ordered):
    # place = 'amsterdam'
    # place = 'berlin ost'
    # place = 'chicago'
    # place = 'leningrad'
    # place = 'pl 55 praha'
    # place = 'izabelin'
    while True:
        url = 'http://api.geonames.org/searchJSON?'
        params = {'username': geoname_users[users_index], 'q': place, 'featureClass': 'P', 'fuzzy': '0.5'}
        result = requests.get(url, params=params).json()
        if 'status' in result:
            users_index += 1
            continue
        if result['totalResultsCount'] > 0:  
            try:
                best_selection = [e for e in result['geonames'] if e['countryName'] == {v['country'] for k,v in places_dict.items() if k == place}.pop()]
                places_geonames.update({place: max(best_selection, key=lambda x: x['population'])})
            except ValueError:
                try:
                    best_selection = [e for e in result['geonames'] if e['adminName1'] == {v['country'] for k,v in places_dict.items() if k == place}.pop()]
                    places_geonames.update({place: max(best_selection, key=lambda x: x['population'])})
                
                except ValueError:
                    places_geonames.update({place: max(result['geonames'], key=lambda x: x['population'])})
            
            #nie mogę brać max population, bo dla amsterdamu to jest Nowy Jork
            #trzeba brać 1. sugestię geonames
            # places_geonames.update({place: max(result['geonames'], key=lambda x: x['population'])})
            #!!!!!tutaj dodać klucz państwa!!!!!!!
            # places_geonames.update({place: result['geonames'][0]})
        else: places_geonames.update({place: 'No geonames response'})
        break

with open('translations_places.pickle', 'wb') as handle:
    pickle.dump(places_geonames, handle, protocol=pickle.HIGHEST_PROTOCOL)
with open('translations_places_no_fuzzy.pickle', 'rb') as handle:
    places_geonames = pickle.load(handle)


x = ['beograd', 'v belgrad', 'u beogradu', 'petrograd', 'beograd nolit']
x = ['barcelona', 'barcelona espana', 'barcelona etc', 'barcelona esp']
x = ['prague', 'prag', 'pragae', 'prago', 'prace', 'in prague', 'praque', 'paraguay', 'praague']
x = ['amsterdam', 'amsterdam etc', 'amsterdamw', 'amsterodam', 'amstersdam']

[places_geonames_old[e]['geonameId'] if isinstance(places_geonames_old[e], dict) else 0 for e in x]

test = places_clusters.copy()
test = {k:[places_geonames[e]['geonameId'] if isinstance(places_geonames[e], dict) else 0 for e in v] for k,v in test.items()}
test = {k:[e if e!=0 else v[0] for e in v] for k,v in test.items()}

ttt = {k:(tuple(v), tuple(test[k])) for k,v in places_clusters.items()}

ok = {}
for k,v in ttt.items():
    # k = 'prague'
    # v = ttt[k]
    if len(set(v[-1])) == 1:
        if v[-1][0] not in ok:
            ok.update({v[-1][0]: list(v[0])})
        else: 
            if isinstance(v[0], list):
                ok[v[-1][0]].extend(v[0])
            else: ok[v[-1][0]].append(v[0])
    else:
        good = [el for ind, el in enumerate(v[0]) if ind in [i for i, e in enumerate(v[-1]) if e == v[-1][0]]]
        if v[-1][0] not in ok:
            ok.update({v[-1][0]: good})
        else: 
            if isinstance(good, list):
                ok[v[-1][0]].extend(good)
            else: ok[v[-1][0]].append(good)
        not_good = [(g, e) for (g, e) in zip(v[-1], v[0]) if e not in good]        
        for el in not_good:
            if el[0] not in ok:
                ok.update({el[0]: [el[-1]]})
            else: 
                if isinstance(el[-1], list):
                    ok[el[0]].extend(el[-1])
                else: ok[el[0]].append(el[-1])
        
#dlaczego w ttt tuple dla moskwy się zwiększa?
                
def flatten(A):
    rt = []
    for i in A:
        if isinstance(i,tuple): rt.extend(flatten(i))
        else: rt.append(i)
    return rt
ok = {k:list(set(flatten(v))) for k,v in ok.items()}







  
ok = {([places_geonames_old[e]['geonameId'] for e in places_geonames_old if places_geonames_old[e]['geonameId']==k][0] if k!=0 else k):v for k,v in ok.items()}    

for k,v in ok.items():
    for e in places_geonames_old:
        if places_geonames_old[e]['geonameId'] == k:
            print(places_geonames_old[e]['geonameId'])
        
    
    
test_df = places[places['001'].isin(places_dict['moskauleningrad'])]





ttt = {}
for k,v in test.items():
    for i, e in enumerate(v):
        if e == 0:
            test[k][i] = v[0]






df = pd.DataFrame()
users_index = 0

for miasto in tqdm(miasta):
    while True:
        url = 'http://api.geonames.org/searchJSON?'
        params = {'username': geoname_users[users_index], 'q': miasto, 'country': '', 'featureClass': '', 'continentCode': '', 'fuzzy': '0.6'}
        result = requests.get(url, params=params).json()
        if 'status' in result:
            users_index += 1
            continue
        test = pd.DataFrame.from_dict(result['geonames'])
        if test.shape[0]:
            test['similarity'] = test['toponymName'].apply(lambda x: SequenceMatcher(0,simplify_string(miasto),simplify_string(x)).ratio())
            test['query name'] = miasto
            test = test[test['similarity'] == test['similarity'].max()][['query name', 'geonameId', 'name', 'countryName', 'similarity']]
            df = df.append(test)
        else:
            test = pd.DataFrame({'query name':[miasto], 'geonameId':'', 'name':'', 'countryName':'', 'similarity':''})
            df = df.append(test)
        break

df.to_excel(f'kartoteka_miejsc_PBL_{year}-{month}-{day}.xlsx', index=False)