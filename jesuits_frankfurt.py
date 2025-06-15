import sys
# sys.path.insert(1, 'D:\IBL\Documents\IBL-PAN-Python')
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
from geonames_accounts import geonames_users
from ast import literal_eval
import math
import regex as re
from collections import ChainMap

#%% members

query = """SELECT DISTINCT ?item ?itemLabel WHERE {
  SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],mul,en". }
  {
    SELECT DISTINCT ?item WHERE {
      ?item p:P611 ?statement0.
      ?statement0 (ps:P611/(wdt:P279*)) wd:Q36380.
      ?item p:P31 ?statement1.
      ?statement1 (ps:P31/(wdt:P279*)) wd:Q5.
    }
  }
}
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

members = [{e.get('item').get('value'): e.get('itemLabel').get('value')} for e in data.get('results').get('bindings')]
members_ids = set([re.findall('Q\d+', list(e.keys())[0])[0] for e in members])

def get_wikidata_claims(wikidata_id):
    # wikidata_id = 'Q100614'
    url = f'https://www.wikidata.org/wiki/Special:EntityData/{wikidata_id}.json'
    result = requests.get(url).json()
    claims = list(result.get('entities').get(wikidata_id).get('claims').keys())
    jesuit_claims.extend(claims)    

jesuit_claims = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(get_wikidata_claims, members_ids),total=len(members_ids)))

jesuit_claims = set(jesuit_claims)

def get_claim_label(property_id):
    # property_id = 'P31'
    url = f'https://www.wikidata.org/wiki/Special:EntityData/{property_id}.json'
    result = requests.get(url).json()
    label = result.get('entities').get(property_id).get('labels').get('en').get('value')
    # test_dict = {'property_id': property_id,
    #              'property_label': label}
    jesuit_claims_labels.append({property_id: label})
    
jesuit_claims_labels = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(get_claim_label, jesuit_claims),total=len(jesuit_claims)))
    
# jesuit_claims_ids = [e.get('property_label') for e in jesuit_claims_labels if e.get('property_label')[0].islower()]                   
# test_df = pd.DataFrame(jesuit_claims_labels)

jesuit_claims_ids = set([list(e.keys())[0] for e in jesuit_claims_labels if list(e.values())[0].islower()])
claims_labels = [e.get(list(e.keys())[0]) for e in jesuit_claims_labels if not 'ID' in e.get(list(e.keys())[0])]

# #%% plot statyczny
# from sklearn.feature_extraction.text import TfidfVectorizer
# from sklearn.decomposition import PCA
# from sklearn.manifold import TSNE
# import matplotlib.pyplot as plt

# # 0. Twoja lista haseł
# words = claims_labels

# # 1. TF–IDF
# vectorizer = TfidfVectorizer(ngram_range=(1,2), min_df=1)
# X_tfidf = vectorizer.fit_transform(words)
# n_samples = X_tfidf.shape[0]

# # 2. PCA ↓k wymiarów, gdzie k = min(20, n_samples-1)
# k = min(20, n_samples - 1)
# pca = PCA(n_components=k, random_state=42)
# X_pca = pca.fit_transform(X_tfidf.toarray())

# # 3. t-SNE ↓2D, ustawiamy perplexity < n_samples/3
# perp = max(5, min(30, n_samples // 3))
# tsne = TSNE(n_components=2, perplexity=perp, random_state=42)
# coords = tsne.fit_transform(X_pca)

# # 4. Większa i czytelna wizualizacja
# plt.figure(figsize=(20, 12))
# plt.scatter(coords[:, 0], coords[:, 1], s=120, alpha=0.8)

# # offset etykiet: 3% zakresu
# dx = (coords[:,0].max() - coords[:,0].min()) * 0.03
# dy = (coords[:,1].max() - coords[:,1].min()) * 0.03
# for (x, y), label in zip(coords, words):
#     plt.text(x + dx, y + dy, label, fontsize=12)

# plt.title("„Semantyczna” chmura słów (TF–IDF + PCA + t-SNE)", fontsize=18)
# plt.xlabel("Wymiar 1", fontsize=16)
# plt.ylabel("Wymiar 2", fontsize=16)
# plt.grid(True, linestyle='--', alpha=0.5)
# plt.tight_layout()
# plt.show()

#%% plot dynamiczny
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.decomposition import PCA
from sklearn.manifold import TSNE
import plotly.express as px

# 0. Twoja lista haseł
words = claims_labels

# 1. TF–IDF
vectorizer = TfidfVectorizer(ngram_range=(1,2), min_df=1)
X_tfidf = vectorizer.fit_transform(words)

# 2. PCA ↓k wymiarów
n_samples = X_tfidf.shape[0]
k = min(20, n_samples - 1)
pca = PCA(n_components=k, random_state=42)
X_pca = pca.fit_transform(X_tfidf.toarray())

# 3. t-SNE ↓2D
perp = max(5, min(30, n_samples // 3))
tsne = TSNE(n_components=2, perplexity=perp, random_state=42)
coords = tsne.fit_transform(X_pca)

# 4. Przygotowanie DataFrame
df = pd.DataFrame({
    'x': coords[:, 0],
    'y': coords[:, 1],
    'label': words
})

# 5. Interaktywny wykres Plotly
fig = px.scatter(df, x='x', y='y', hover_name='label',
                 title='Interactive semantic word cloud for Jesuit properties on Wikidata')
fig.update_traces(marker=dict(size=12, opacity=0.7))
fig.update_layout(width=1000, height=700)

# Zapis do pliku HTML
html_path = 'data/Frankfurt/frankfurt_jesuits_semantic_cloud_properties.html'
fig.write_html(html_path, include_plotlyjs='cdn')

#%% jesuit members -- signals of conflict or peace

df = gsheet_to_df('1Ev3vLuMvnW_CD55xycpXFt1TMNw0vpi_aI3IB1BCp7A', 'Arkusz1')
df = df.loc[df['interesting'] == 'x']

interesting_properties = df['property_id'].to_list()
claims_conflict_labels = dict(zip(df['property_id'].to_list(), df['property_label'].to_list()))

def get_members_wikidata_claims(wikidata_id):
    # wikidata_id = 'Q4403688'
    # wikidata_id = list(members_ids)[5964]
    url = f'https://www.wikidata.org/wiki/Special:EntityData/{wikidata_id}.json'
    result = requests.get(url).json()
    claims = result.get('entities').get(wikidata_id).get('claims')
    claims = {k:v for k,v in claims.items() if k in interesting_properties}
    pce_iteration = []
    for k,v in claims.items():
        for e in v:
            try:
                pce_iteration.append((wikidata_id, k, e.get('mainsnak').get('datavalue').get('value').get('id')))
            except AttributeError:
                try:
                    pce_iteration.append((wikidata_id, k, e.get('qualifiers').get('P1932')[0].get('datavalue').get('value')))
                except TypeError:
                    print(k)
    person_claim_entity.extend(pce_iteration)
 
person_claim_entity = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(get_members_wikidata_claims, members_ids),total=len(members_ids)))

#jak na podstawie tego badań konfliktowość?
entities_for_search = set([e[-1] for e in person_claim_entity if isinstance(e[-1], str) and e[-1].startswith('Q')])

wikidata_redirect = {'Q108140949': 'Q56312763'}

def get_wikidata_label(wikidata_id, pref_langs = ['en', 'es', 'fr', 'de', 'pl']):
    # wikidata_id = 'Q104785800'
    # wikidata_id = list(entities_for_search)[0]
    url = f'https://www.wikidata.org/wiki/Special:EntityData/{wikidata_id}.json'
    try:
        result = requests.get(url).json()
        try:
            langs = [e for e in list(result.get('entities').get(wikidata_id).get('labels').keys()) if e in pref_langs]
        except AttributeError:
            wikidata_id = wikidata_redirect.get(wikidata_id)
            langs = [e for e in list(result.get('entities').get(wikidata_id).get('labels').keys()) if e in pref_langs]
        if langs:
            order = {lang: idx for idx, lang in enumerate(pref_langs)}
            sorted_langs = sorted(langs, key=lambda x: order.get(x, float('inf')))
            for lang in sorted_langs:
                label = result['entities'][wikidata_id]['labels'][lang]['value']
                break
        else: label = None
    except ValueError:
        label = None
    # wiki_labels.append({'wikidata_id': wikidata_id,
    #                     'wikidata_label': label})
    wiki_labels.append({wikidata_id: label})

wiki_labels = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(get_wikidata_label, entities_for_search),total=len(entities_for_search)))

members_dict = {re.findall('Q\d+', k)[0]:v for k,v in dict(ChainMap(*members)).items()}
claims_dict = claims_conflict_labels
entity_dict = dict(ChainMap(*wiki_labels))

final_data = [[members_dict.get(a), claims_dict.get(b), entity_dict.get(c,entity_dict.get(wikidata_redirect.get(c))) if isinstance(c, str) and c[0] == 'Q' else c] for a, b, c in person_claim_entity]

final_df = pd.DataFrame(final_data, columns=['person', 'relation', 'entity'])

final_df.to_csv('data/Frankfurt/jesuits_conflict.csv', index=False)

#%% jesuit generals
query = """SELECT DISTINCT ?item ?itemLabel WHERE {
  SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],mul,en". }
  {
    SELECT DISTINCT ?item WHERE {
      ?item p:P39 ?statement0.
      ?statement0 (ps:P39/(wdt:P279*)) wd:Q1515704.
    }
  }
}
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

generals = [{e.get('item').get('value'): e.get('itemLabel').get('value')} for e in data.get('results').get('bindings')]
generals = [e for e in generals if e != {'http://www.wikidata.org/entity/Q64782473': 'Ignatius of Loyola (fictional character)'}]
generals_ids = set([re.findall('Q\d+', list(e.keys())[0])[0] for e in generals])

df = gsheet_to_df('1Ev3vLuMvnW_CD55xycpXFt1TMNw0vpi_aI3IB1BCp7A', 'Arkusz1')
df = df.loc[df['interesting'] == 'x']

interesting_properties = df['property_id'].to_list()
claims_conflict_labels = dict(zip(df['property_id'].to_list(), df['property_label'].to_list()))

def get_members_wikidata_claims(wikidata_id):
    # wikidata_id = 'Q4403688'
    # wikidata_id = list(members_ids)[5964]
    url = f'https://www.wikidata.org/wiki/Special:EntityData/{wikidata_id}.json'
    result = requests.get(url).json()
    claims = result.get('entities').get(wikidata_id).get('claims')
    claims = {k:v for k,v in claims.items() if k in interesting_properties}
    pce_iteration = []
    for k,v in claims.items():
        for e in v:
            try:
                pce_iteration.append((wikidata_id, k, e.get('mainsnak').get('datavalue').get('value').get('id')))
            except AttributeError:
                try:
                    pce_iteration.append((wikidata_id, k, e.get('qualifiers').get('P1932')[0].get('datavalue').get('value')))
                except TypeError:
                    print(k)
    generals_claim_entity.extend(pce_iteration)
 
generals_claim_entity = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(get_members_wikidata_claims, generals_ids),total=len(generals_ids)))

#jak na podstawie tego badań konfliktowość?
generals_entities_for_search = set([e[-1] for e in generals_claim_entity if isinstance(e[-1], str) and e[-1].startswith('Q')])

wikidata_redirect = {'Q108140949': 'Q56312763',
                     'Q131676059': 'Q3946901'}

def get_wikidata_label(wikidata_id, pref_langs = ['en', 'es', 'fr', 'de', 'pl']):
    # wikidata_id = 'Q104785800'
    # wikidata_id = list(dominican_entities_for_search)[1621]
    url = f'https://www.wikidata.org/wiki/Special:EntityData/{wikidata_id}.json'
    try:
        result = requests.get(url).json()
        try:
            langs = [e for e in list(result.get('entities').get(wikidata_id).get('labels').keys()) if e in pref_langs]
        except AttributeError:
            wikidata_id = wikidata_redirect.get(wikidata_id)
            langs = [e for e in list(result.get('entities').get(wikidata_id).get('labels').keys()) if e in pref_langs]
        if langs:
            order = {lang: idx for idx, lang in enumerate(pref_langs)}
            sorted_langs = sorted(langs, key=lambda x: order.get(x, float('inf')))
            for lang in sorted_langs:
                label = result['entities'][wikidata_id]['labels'][lang]['value']
                break
        else: label = None
    except ValueError:
        label = None
    # wiki_labels.append({'wikidata_id': wikidata_id,
    #                     'wikidata_label': label})
    generals_wiki_labels.append({wikidata_id: label})

generals_wiki_labels = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(get_wikidata_label, generals_entities_for_search),total=len(generals_entities_for_search)))

members_dict = {re.findall('Q\d+', k)[0]:v for k,v in dict(ChainMap(*generals)).items()}
claims_dict = claims_conflict_labels
entity_dict = dict(ChainMap(*generals_wiki_labels))

generals_final_data = [[members_dict.get(a), claims_dict.get(b), entity_dict.get(c,entity_dict.get(wikidata_redirect.get(c))) if isinstance(c, str) and c[0] == 'Q' else c] for a, b, c in generals_claim_entity]

generals_final_df = pd.DataFrame(generals_final_data, columns=['person', 'relation', 'entity'])

generals_final_df.to_csv('data/Frankfurt/generals_jesuits_conflict.csv', index=False)


#%% jesuit missionaries

def select_missionaries(wikidata_id):
    # wikidata_id = 'Q589581'
    # wikidata_id = 'Q106834931'
    url = f'https://www.wikidata.org/wiki/Special:EntityData/{wikidata_id}.json'
    result = requests.get(url).json()
    descriptions = [e.get('value') for e in list(result.get('entities').get(wikidata_id).get('descriptions').values())]
    if any("miss" in f.lower() or "misj" in f.lower() for f in descriptions):
        jesuit_missionaries.append(wikidata_id)
    
jesuit_missionaries = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(select_missionaries, members_ids),total=len(members_ids)))

df = gsheet_to_df('1Ev3vLuMvnW_CD55xycpXFt1TMNw0vpi_aI3IB1BCp7A', 'Arkusz1')
df = df.loc[df['interesting'] == 'x']

interesting_properties = df['property_id'].to_list()
claims_conflict_labels = dict(zip(df['property_id'].to_list(), df['property_label'].to_list()))

def get_missionaries_wikidata_claims(wikidata_id):
    # wikidata_id = 'Q4403688'
    # wikidata_id = list(members_ids)[5964]
    url = f'https://www.wikidata.org/wiki/Special:EntityData/{wikidata_id}.json'
    result = requests.get(url).json()
    claims = result.get('entities').get(wikidata_id).get('claims')
    claims = {k:v for k,v in claims.items() if k in interesting_properties}
    pce_iteration = []
    for k,v in claims.items():
        for e in v:
            try:
                pce_iteration.append((wikidata_id, k, e.get('mainsnak').get('datavalue').get('value').get('id')))
            except AttributeError:
                try:
                    pce_iteration.append((wikidata_id, k, e.get('qualifiers').get('P1932')[0].get('datavalue').get('value')))
                except TypeError:
                    print(k)
    missionaries_claim_entity.extend(pce_iteration)
 
missionaries_claim_entity = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(get_missionaries_wikidata_claims, jesuit_missionaries),total=len(jesuit_missionaries)))

#jak na podstawie tego badań konfliktowość?
missionaries_entities_for_search = set([e[-1] for e in missionaries_claim_entity if isinstance(e[-1], str) and e[-1].startswith('Q')])

wikidata_redirect = {'Q108140949': 'Q56312763',
                     'Q131676059': 'Q3946901'}

def get_wikidata_label(wikidata_id, pref_langs = ['en', 'es', 'fr', 'de', 'pl']):
    # wikidata_id = 'Q104785800'
    # wikidata_id = list(dominican_entities_for_search)[1621]
    url = f'https://www.wikidata.org/wiki/Special:EntityData/{wikidata_id}.json'
    try:
        result = requests.get(url).json()
        try:
            langs = [e for e in list(result.get('entities').get(wikidata_id).get('labels').keys()) if e in pref_langs]
        except AttributeError:
            wikidata_id = wikidata_redirect.get(wikidata_id)
            langs = [e for e in list(result.get('entities').get(wikidata_id).get('labels').keys()) if e in pref_langs]
        if langs:
            order = {lang: idx for idx, lang in enumerate(pref_langs)}
            sorted_langs = sorted(langs, key=lambda x: order.get(x, float('inf')))
            for lang in sorted_langs:
                label = result['entities'][wikidata_id]['labels'][lang]['value']
                break
        else: label = None
    except ValueError:
        label = None
    # wiki_labels.append({'wikidata_id': wikidata_id,
    #                     'wikidata_label': label})
    missionaries_wiki_labels.append({wikidata_id: label})

missionaries_wiki_labels = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(get_wikidata_label, missionaries_entities_for_search),total=len(missionaries_entities_for_search)))

missionaries_dict = {re.findall('Q\d+', k)[0]:v for k,v in dict(ChainMap(*members)).items()}
missionaries_dict = {k:v for k,v in missionaries_dict.items() if k in jesuit_missionaries}
claims_dict = claims_conflict_labels
entity_dict = dict(ChainMap(*missionaries_wiki_labels))

missionaries_final_data = [[missionaries_dict.get(a), claims_dict.get(b), entity_dict.get(c,entity_dict.get(wikidata_redirect.get(c))) if isinstance(c, str) and c[0] == 'Q' else c] for a, b, c in missionaries_claim_entity]

missionaries_final_df = pd.DataFrame(missionaries_final_data, columns=['person', 'relation', 'entity'])

missionaries_final_df.to_csv('data/Frankfurt/jesuits_missionaries.csv', index=False)


#%% dominikanie

query = """SELECT DISTINCT ?item ?itemLabel WHERE {
  SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],mul,en". }
  {
    SELECT DISTINCT ?item WHERE {
      ?item p:P611 ?statement0.
      ?statement0 (ps:P611/(wdt:P279*)) wd:Q131479.
      ?item p:P31 ?statement1.
      ?statement1 (ps:P31/(wdt:P279*)) wd:Q5.
    }
  }
}
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

dominican_members = [{e.get('item').get('value'): e.get('itemLabel').get('value')} for e in data.get('results').get('bindings')]
dominican_members_ids = set([re.findall('Q\d+', list(e.keys())[0])[0] for e in dominican_members])

df = gsheet_to_df('1Ev3vLuMvnW_CD55xycpXFt1TMNw0vpi_aI3IB1BCp7A', 'Arkusz1')
df = df.loc[df['interesting'] == 'x']

interesting_properties = df['property_id'].to_list()
claims_conflict_labels = dict(zip(df['property_id'].to_list(), df['property_label'].to_list()))

def get_members_wikidata_claims(wikidata_id):
    # wikidata_id = 'Q4403688'
    # wikidata_id = list(members_ids)[5964]
    url = f'https://www.wikidata.org/wiki/Special:EntityData/{wikidata_id}.json'
    result = requests.get(url).json()
    claims = result.get('entities').get(wikidata_id).get('claims')
    claims = {k:v for k,v in claims.items() if k in interesting_properties}
    pce_iteration = []
    for k,v in claims.items():
        for e in v:
            try:
                pce_iteration.append((wikidata_id, k, e.get('mainsnak').get('datavalue').get('value').get('id')))
            except AttributeError:
                try:
                    pce_iteration.append((wikidata_id, k, e.get('qualifiers').get('P1932')[0].get('datavalue').get('value')))
                except TypeError:
                    print(k)
    dominican_person_claim_entity.extend(pce_iteration)
 
dominican_person_claim_entity = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(get_members_wikidata_claims, dominican_members_ids),total=len(dominican_members_ids)))

#jak na podstawie tego badań konfliktowość?
dominican_entities_for_search = set([e[-1] for e in dominican_person_claim_entity if isinstance(e[-1], str) and e[-1].startswith('Q')])

wikidata_redirect = {'Q108140949': 'Q56312763',
                     'Q131676059': 'Q3946901'}

def get_wikidata_label(wikidata_id, pref_langs = ['en', 'es', 'fr', 'de', 'pl']):
    # wikidata_id = 'Q104785800'
    # wikidata_id = list(dominican_entities_for_search)[1621]
    url = f'https://www.wikidata.org/wiki/Special:EntityData/{wikidata_id}.json'
    try:
        result = requests.get(url).json()
        try:
            langs = [e for e in list(result.get('entities').get(wikidata_id).get('labels').keys()) if e in pref_langs]
        except AttributeError:
            wikidata_id = wikidata_redirect.get(wikidata_id)
            langs = [e for e in list(result.get('entities').get(wikidata_id).get('labels').keys()) if e in pref_langs]
        if langs:
            order = {lang: idx for idx, lang in enumerate(pref_langs)}
            sorted_langs = sorted(langs, key=lambda x: order.get(x, float('inf')))
            for lang in sorted_langs:
                label = result['entities'][wikidata_id]['labels'][lang]['value']
                break
        else: label = None
    except ValueError:
        label = None
    # wiki_labels.append({'wikidata_id': wikidata_id,
    #                     'wikidata_label': label})
    dominican_wiki_labels.append({wikidata_id: label})

dominican_wiki_labels = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(get_wikidata_label, dominican_entities_for_search),total=len(dominican_entities_for_search)))

members_dict = {re.findall('Q\d+', k)[0]:v for k,v in dict(ChainMap(*dominican_members)).items()}
claims_dict = claims_conflict_labels
entity_dict = dict(ChainMap(*dominican_wiki_labels))

dominican_final_data = [[members_dict.get(a), claims_dict.get(b), entity_dict.get(c,entity_dict.get(wikidata_redirect.get(c))) if isinstance(c, str) and c[0] == 'Q' else c] for a, b, c in dominican_person_claim_entity]

dominican_final_df = pd.DataFrame(dominican_final_data, columns=['person', 'relation', 'entity'])

dominican_final_df.to_csv('data/Frankfurt/dominican_conflict.csv', index=False)

#%% franciszkanie

query = """SELECT DISTINCT ?item ?itemLabel WHERE {
  SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],mul,en". }
  {
    SELECT DISTINCT ?item WHERE {
      ?item p:P611 ?statement0.
      ?statement0 (ps:P611/(wdt:P279*)) wd:Q913972.
      ?item p:P31 ?statement1.
      ?statement1 (ps:P31/(wdt:P279*)) wd:Q5.
    }
  }
}
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

franciscan_members = [{e.get('item').get('value'): e.get('itemLabel').get('value')} for e in data.get('results').get('bindings')]
franciscan_members_ids = set([re.findall('Q\d+', list(e.keys())[0])[0] for e in franciscan_members])

df = gsheet_to_df('1Ev3vLuMvnW_CD55xycpXFt1TMNw0vpi_aI3IB1BCp7A', 'Arkusz1')
df = df.loc[df['interesting'] == 'x']

interesting_properties = df['property_id'].to_list()
claims_conflict_labels = dict(zip(df['property_id'].to_list(), df['property_label'].to_list()))

def get_members_wikidata_claims(wikidata_id):
    # wikidata_id = 'Q4403688'
    # wikidata_id = list(franciscan_members_ids)[1228]
    url = f'https://www.wikidata.org/wiki/Special:EntityData/{wikidata_id}.json'
    result = requests.get(url).json()
    claims = result.get('entities').get(wikidata_id).get('claims')
    claims = {k:v for k,v in claims.items() if k in interesting_properties}
    pce_iteration = []
    for k,v in claims.items():
        for e in v:
            try:
                pce_iteration.append((wikidata_id, k, e.get('mainsnak').get('datavalue').get('value').get('id')))
            except AttributeError:
                try:
                    pce_iteration.append((wikidata_id, k, e.get('qualifiers').get('P1932')[0].get('datavalue').get('value')))
                except (AttributeError, TypeError):
                    print(k)
    franciscan_person_claim_entity.extend(pce_iteration)
 
franciscan_person_claim_entity = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(get_members_wikidata_claims, franciscan_members_ids),total=len(franciscan_members_ids)))

#jak na podstawie tego badań konfliktowość?
franciscan_entities_for_search = set([e[-1] for e in franciscan_person_claim_entity if isinstance(e[-1], str) and e[-1].startswith('Q')])

wikidata_redirect = {'Q108140949': 'Q56312763',
                     'Q131676059': 'Q3946901'}

def get_wikidata_label(wikidata_id, pref_langs = ['en', 'es', 'fr', 'de', 'pl']):
    # wikidata_id = 'Q104785800'
    # wikidata_id = list(franciscan_entities_for_search)[1228]
    url = f'https://www.wikidata.org/wiki/Special:EntityData/{wikidata_id}.json'
    try:
        result = requests.get(url).json()
        try:
            langs = [e for e in list(result.get('entities').get(wikidata_id).get('labels').keys()) if e in pref_langs]
        except AttributeError:
            wikidata_id = wikidata_redirect.get(wikidata_id)
            langs = [e for e in list(result.get('entities').get(wikidata_id).get('labels').keys()) if e in pref_langs]
        if langs:
            order = {lang: idx for idx, lang in enumerate(pref_langs)}
            sorted_langs = sorted(langs, key=lambda x: order.get(x, float('inf')))
            for lang in sorted_langs:
                label = result['entities'][wikidata_id]['labels'][lang]['value']
                break
        else: label = None
    except ValueError:
        label = None
    # wiki_labels.append({'wikidata_id': wikidata_id,
    #                     'wikidata_label': label})
    franciscan_wiki_labels.append({wikidata_id: label})

franciscan_wiki_labels = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(get_wikidata_label, franciscan_entities_for_search),total=len(franciscan_entities_for_search)))

members_dict = {re.findall('Q\d+', k)[0]:v for k,v in dict(ChainMap(*franciscan_members)).items()}
claims_dict = claims_conflict_labels
entity_dict = dict(ChainMap(*franciscan_wiki_labels))

franciscan_final_data = [[members_dict.get(a), claims_dict.get(b), entity_dict.get(c,entity_dict.get(wikidata_redirect.get(c))) if isinstance(c, str) and c[0] == 'Q' else c] for a, b, c in franciscan_person_claim_entity]

franciscan_final_df = pd.DataFrame(franciscan_final_data, columns=['person', 'relation', 'entity'])

franciscan_final_df.to_csv('data/Frankfurt/franciscan_conflict.csv', index=False)




















