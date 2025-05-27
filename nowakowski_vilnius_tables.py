import sys
import requests
from tqdm import tqdm
from my_functions import gsheet_to_df, simplify_string, cluster_strings, marc_parser_to_dict
from concurrent.futures import ThreadPoolExecutor
import json
import pandas as pd
from SPARQLWrapper import SPARQLWrapper, JSON
from ast import literal_eval

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
# for wikidata_id in tqdm(wikidata_ids):
    #wikidata_id = list(wikidata_ids)[1]
    url = f'https://www.wikidata.org/wiki/Special:EntityData/{wikidata_id}.json'
    result = requests.get(url).json()
    try:
        birthdate_value = result.get('entities').get(wikidata_id).get('claims').get('P569')[0].get('mainsnak').get('datavalue').get('value').get('time')[:11]
    except TypeError:
        birthdate_value = None
    except AttributeError:
        deathdate_value = result.get('entities').get(wikidata_id).get('claims').get('P569')[0].get('qualifiers').get('P1319')[0].get('datavalue').get('value').get('time')[:11]
    try:
        deathdate_value = result.get('entities').get(wikidata_id).get('claims').get('P570')[0].get('mainsnak').get('datavalue').get('value').get('time')[:11]
    except TypeError:
        deathdate_value = None
    except AttributeError:
        deathdate_value = result.get('entities').get(wikidata_id).get('claims').get('P570')[0].get('qualifiers').get('P1319')[0].get('datavalue').get('value').get('time')[:11]
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
        deathplace_value = result.get('entities').get(wikidata_id).get('claims').get('P20')[0].get('mainsnak').get('datavalue').get('value').get('id')
    except TypeError:
        deathplace_value = None 
    temp_dict = {'person_wikidata': wikidata_id,
                 'birthdate.value': birthdate_value,
                 'deathdate.value': deathdate_value,
                 'birthplace_wikidata': birthplace_value,
                 'birthplaceLabel.value': birthplaceLabel_value,
                 'deathplace_value': deathplace_value,
                 'deathplaceLabel.value': deathplaceLabel_value}
    wikidata_supplement.append(temp_dict)

#%% people

df_people = gsheet_to_df('1M2gc-8cGZ8gh8TTnm4jl430bL4tccdri9nw-frUWYLQ', 'people')
wikidata_ids = set([e for e in df_people['person_wikidata_id'].to_list() if isinstance(e, str)])

wikidata_supplement = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(harvest_wikidata_for_person, wikidata_ids),total=len(wikidata_ids)))

    
#%% places

# places from texts

# places from people


























