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
    try:
        url = f'https://www.wikidata.org/wiki/Special:EntityData/{wikidata_id}.json'
        result = requests.get(url).json()
        
        try:
            birthdate_value = result.get('entities').get(wikidata_id).get('claims').get('P569')[0].get('mainsnak').get('datavalue').get('value').get('time')[:11]
        except TypeError:
            birthdate_value = None
        except AttributeError:
            try:
                birthdate_value = result.get('entities').get(wikidata_id).get('claims').get('P569')[0].get('qualifiers').get('P1319')[0].get('datavalue').get('value').get('time')[:11]
            except TypeError:
                birthdate_value = result.get('entities').get(wikidata_id).get('claims').get('P569')[0].get('qualifiers').get('P1326')[0].get('datavalue').get('value').get('time')[:11]
            except AttributeError:
                birthdate_value = None
                
        try:
            deathdate_value = result.get('entities').get(wikidata_id).get('claims').get('P570')[0].get('mainsnak').get('datavalue').get('value').get('time')[:11]
        except TypeError:
            deathdate_value = None
        except AttributeError:
            try:
                deathdate_value = result.get('entities').get(wikidata_id).get('claims').get('P570')[0].get('qualifiers').get('P1319')[0].get('datavalue').get('value').get('time')[:11]
            except TypeError:
                deathdate_value = result.get('entities').get(wikidata_id).get('claims').get('P570')[0].get('qualifiers').get('P1326')[0].get('datavalue').get('value').get('time')[:11]
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

    if birth_year is None and death_year is None:
        return None

    if birth_year is not None and death_year is not None:
        start = birth_year + 25
        end = death_year
    elif birth_year is None:
        start = death_year
        end = death_year + 1
    else:
        start = birth_year + 25
        end = start + 1

    if start >= end:
        return None

    def century_of(y: int) -> int:
        if y > 0:
            return (y - 1) // 100 + 1
        else:
            return -(((-y) - 1) // 100 + 1)

    def century_bounds(c: int) -> (int, int):
        if c > 0:
            return ((c - 1) * 100 + 1, c * 100 + 1)
        else:
            absC = -c
            start_bc = -(absC * 100 - 1)
            end_bc   = -((absC - 1) * 100 - 1)
            return (start_bc, end_bc)

    c_start = century_of(start)
    c_end = century_of(end - 1)
    step = 1 if c_end >= c_start else -1

    overlaps = {}
    for c in range(c_start, c_end + step, step):
        s_c, e_c = century_bounds(c)
        ov_start = max(start, s_c)
        ov_end   = min(end,   e_c)
        overlaps[c] = max(0, ov_end - ov_start)

    if not overlaps:
        return None

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

#%% productivity period
df_people = gsheet_to_df('1M2gc-8cGZ8gh8TTnm4jl430bL4tccdri9nw-frUWYLQ', 'people')

wikidata_supplement = []
for i, row in df_people.iterrows():
    if isinstance(row['person_wikidata_id'], str):
        temp_dict = {'wikidata_id': row['person_wikidata_id'],
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
    productivity.append({'wikidata_id': p.get('wikidata_id'),
                         'century': most_productive_century(b, d)})
    
df_productivity = pd.DataFrame(productivity)

#%% people ids in texts

df_texts = df_people = gsheet_to_df('1M2gc-8cGZ8gh8TTnm4jl430bL4tccdri9nw-frUWYLQ', 'texts')
df_people = df_people = gsheet_to_df('1M2gc-8cGZ8gh8TTnm4jl430bL4tccdri9nw-frUWYLQ', 'people')

people_dict = dict(zip([e.strip() if isinstance(e, str) else None for e in df_people['person_name'].to_list()], df_people['person_id'].to_list()))

#additional authors
interested_columns = ['Additional Authors', 'Dedicated to', 'Key Historical Figures Mentioned', 'Key Authors Cited', 'Response to Author']
test = [[el.strip() for el in e.split(';')] if isinstance (e, str) and 'No Data' not in e else None for e in df_texts[interested_columns[4]].to_list()]

test_fix = []
for i, authors in enumerate(test):
    if authors:
        test_fix.append('; '.join([people_dict.get(e, 'no id') for e in authors]))
    else: test_fix.append(None)

test_df = pd.DataFrame(test_fix)



key_authors = [[el.strip() for el in e.split(';')] if isinstance(e, str) else None for e in df_texts['Key authors cited:'].to_list()]

key_authors_wikidata = []
for i, authors in enumerate(key_authors):
    if authors:
        key_authors_wikidata.append('; '.join([people_dict.get(e, 'no Wikidata ID') for e in authors]))
    else: key_authors_wikidata.append(None)

#%% places

# places from texts

# places from people


























