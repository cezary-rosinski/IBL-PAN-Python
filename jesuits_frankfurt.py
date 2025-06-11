import sys
sys.path.insert(1, 'D:\IBL\Documents\IBL-PAN-Python')
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

#%% genrals
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

def get_coordinates(qid):
    
    # qid = 
    query = f"""
    SELECT ?birthCoord ?deathCoord WHERE {{
      wd:{qid} wdt:P19 ?birthPlace;
                wdt:P20 ?deathPlace.
      ?birthPlace wdt:P625 ?birthCoord.
      ?deathPlace wdt:P625 ?deathCoord.
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
    binding = data["results"]["bindings"][0]

    # Parse "Point(lon lat)" into (lat, lon)
    def parse_point(p):
        point = p["value"].replace("Point(", "").replace(")", "").split()
        lon, lat = map(float, point)
        return lat, lon

    birth_coords = parse_point(binding["birthCoord"])
    death_coords = parse_point(binding["deathCoord"])
    return birth_coords, death_coords

def haversine(coord1, coord2):
    R = 6371.0  # Earth radius in kilometers
    lat1, lon1 = map(math.radians, coord1)
    lat2, lon2 = map(math.radians, coord2)
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = math.sin(dlat/2)**2 + math.cos(lat1)*math.cos(lat2)*math.sin(dlon/2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c

for e in generals:
    e = generals[0]
    qid = list(e.keys())[0].split('/')[-1]
    birth, death = get_coordinates(qid)
    dist = haversine(birth, death)
    print(f"Distance for {qid}, {list(e.values())[0]}: {dist:.2f} km")


#%% participants

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
    test_dict = {'property_id': property_id,
                 'property_label': label}
    jesuit_claims_labels.append(test_dict)
    
jesuit_claims_labels = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(get_claim_label, jesuit_claims),total=len(jesuit_claims)))
    
jesuit_claims_ids = [e.get('property_label') for e in jesuit_claims_labels if e.get('property_label')[0].islower()]                   
test_df = pd.DataFrame(jesuit_claims_labels)

#%% plot statyczny
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.decomposition import PCA
from sklearn.manifold import TSNE
import matplotlib.pyplot as plt

# 0. Twoja lista haseł
words = jesuit_claims_ids

# 1. TF–IDF
vectorizer = TfidfVectorizer(ngram_range=(1,2), min_df=1)
X_tfidf = vectorizer.fit_transform(words)
n_samples = X_tfidf.shape[0]

# 2. PCA ↓k wymiarów, gdzie k = min(20, n_samples-1)
k = min(20, n_samples - 1)
pca = PCA(n_components=k, random_state=42)
X_pca = pca.fit_transform(X_tfidf.toarray())

# 3. t-SNE ↓2D, ustawiamy perplexity < n_samples/3
perp = max(5, min(30, n_samples // 3))
tsne = TSNE(n_components=2, perplexity=perp, random_state=42)
coords = tsne.fit_transform(X_pca)

# 4. Większa i czytelna wizualizacja
plt.figure(figsize=(20, 12))
plt.scatter(coords[:, 0], coords[:, 1], s=120, alpha=0.8)

# offset etykiet: 3% zakresu
dx = (coords[:,0].max() - coords[:,0].min()) * 0.03
dy = (coords[:,1].max() - coords[:,1].min()) * 0.03
for (x, y), label in zip(coords, words):
    plt.text(x + dx, y + dy, label, fontsize=12)

plt.title("„Semantyczna” chmura słów (TF–IDF + PCA + t-SNE)", fontsize=18)
plt.xlabel("Wymiar 1", fontsize=16)
plt.ylabel("Wymiar 2", fontsize=16)
plt.grid(True, linestyle='--', alpha=0.5)
plt.tight_layout()
plt.show()

#%% plot dynamiczny
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.decomposition import PCA
from sklearn.manifold import TSNE
import plotly.express as px

# 0. Twoja lista haseł
words = jesuit_claims_ids


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
                 title='Interaktywna semantyczna chmura słów (TF–IDF + PCA + t-SNE)')
fig.update_traces(marker=dict(size=12, opacity=0.7))
fig.update_layout(width=1000, height=700)

# Zapis do pliku HTML
html_path = 'semantic_cloud.html'
fig.write_html(html_path, include_plotlyjs='cdn')

html_path

#%% signals of conflict or peace

df = gsheet_to_df('1Ev3vLuMvnW_CD55xycpXFt1TMNw0vpi_aI3IB1BCp7A', 'Arkusz1')
df = df.loc[df['interesting'] == 'x']

interesting_properties = df['property_id'].to_list()

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








#%%






def get_wikidata_claims(entity_id):
    entity_id = 'Q100614'
    """
    Pobiera wszystkie "claims" (właściwości i ich wartości) dla podanego ID wikidata (np. Q100614).
    Zwraca słownik, gdzie kluczem jest property (np. P31), a wartością lista wartości.
    """
    URL = "https://www.wikidata.org/w/api.php"
    params = {
        "action": "wbgetclaims",
        "entity": entity_id,
        "format": "json"
    }
    response = requests.get(URL, params=params)
    response.raise_for_status()
    data = response.json()

    claims = data.get("claims", {})
    props = {}

    for prop, claim_list in claims.items():
        values = []
        for claim in claim_list:
            mainsnak = claim.get("mainsnak", {})
            datavalue = mainsnak.get("datavalue", {})
            if "value" in datavalue:
                values.append(datavalue["value"])
        props[prop] = values

    return props

if __name__ == "__main__":
    entity = "Q100614"
    props = get_wikidata_claims(entity)

    # Wyświetlenie wyników
    for prop, vals in props.items():
        print(f"{prop}:")
        for v in vals:
            print("   ", v)



#%%



def get_wikidata_qid_from_viaf(viaf_id):
    #viaf_id = '9891285'
    # endpoint_url = "https://query.wikidata.org/sparql"
    query = """SELECT DISTINCT ?item ?itemLabel WHERE {
  SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],mul,en". }
  {
    SELECT DISTINCT ?item WHERE {
      ?item p:P611 ?statement0.
      ?statement0 (ps:P611/(wdt:P279*)) wd:Q36380.
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
    try:
        qid_url = data["results"]["bindings"][0]["item"]["value"]
        qid = qid_url.split("/")[-1]
        return qid
    except (IndexError, KeyError):
        return None