from itertools import product
import requests
from urllib.parse import urlencode
from tqdm import tqdm
import pandas as pd
import ast

BASE_API_URL = 'https://api.gotriple.eu/api/'

#%%

keywords_1 = ['government', 'state', 'public policy', 'public funding', 'ministry of culture', 'municipality', 'public administration']

keywords_2 = ['art', 'culture', 'heritage', 'museum', 'memorial', 'cinema', 'music', 'literature', 'festival', 'creative']

keywords_3 = ['value', 'identity', 'ideology', 'propaganda', 'soft power', 'social cohesion', 'patriotism', 'social change']

issues = list(product(keywords_1, keywords_2, keywords_3))
issues = [' AND '.join(e) for e in issues]

#%% params

params = {
    'q': issues[0],
    'fq': {
        'type': 'typ_article',
        #'has_pdf': 'true',
        #'in_language': 'en'
    },
    'include_duplicates': 'false',
    'aggs': {

    },
    'sort': 'name:desc', # name, publication_date, most_recent --> name:desc
    'page': 1,
    'size': 100, # max 100
}

#%%

def build_api_url(params):
    cleaned_params = {}
    for key, value in params.items():
        if value:
            if isinstance(value, dict):
                cleaned_params[key] = urlencode(value).replace('&', ';')
            else:
               cleaned_params[key] = value
    return urlencode(cleaned_params)

#%%
objects_list = []
endpoint = 'documents'

for i in tqdm(issues):

    params = {
        'q': i,
        'fq': {
            'type': 'typ_article',
            #'has_pdf': 'true',
            #'in_language': 'en'
        },
        'include_duplicates': 'false',
        'aggs': {
    
        },
        'sort': 'name:desc', # name, publication_date, most_recent --> name:desc
        'page': 1,
        'size': 100, # max 100
    }
    
    url = f'{BASE_API_URL}{endpoint}?{build_api_url(params)}'
    while True:
        response = requests.get(url)
        if not response.ok:
          print('Response error:', response.status_code)
          print(response.text)
          break
        if not response.json()['data']:
          print('Empty "data" field')
          break
        else:
            iteration = response.json()['data']
            [e.update({'query': i}) for e in iteration]
            objects_list.extend(iteration)
            # print(url)
            params['page'] += 1
            url = f'{BASE_API_URL}{endpoint}?{build_api_url(params)}'
        
        
#%%

keys = ['id', 'abstract', 'additional_type', 'contributor', 'datePublished', 'doi', 'identifier', 'keywords', 'producer', 'provider', 'author', 'lang', 'query', 'headline']

data = {k:v for k,v in objects_list[0].items() if k in keys}

final_result = [{k:v for k,v in e.items() if k in keys} for e in objects_list]


def normalize_records(records: list[dict]) -> pd.DataFrame:
    rows = []

    for rec in records:
        row = {
            "id": rec.get("id"),
            "doi": "; ".join(d for d in rec.get("doi", []) if d),
            "provider": "; ".join(rec.get("provider", [])),
            "query": rec.get("query"),

            "abstract_text": " ".join(
                a.get("text", "") for a in rec.get("abstract", [])
            ),

            "keywords": "; ".join(
                k.get("text", "") for k in rec.get("keywords", [])
            ),

            "authors": "; ".join(
                a.get("fullname", "") for a in rec.get("author", [])
            ),
            "title": " ".join(
                a.get("text", "") for a in rec.get("headline", [])
            ),
        }

        rows.append(row)

    return pd.DataFrame(rows)

df = normalize_records(final_result)

def aggregate_by_id(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df
        .groupby("id", as_index=False)
        .agg({
            "doi": "first",
            "provider": "first",
            "abstract_text": "first",
            "keywords": "first",
            "authors": "first",
            "query": lambda x: "; ".join(sorted(set(q for q in x if q)))
        })
    )


# ===== PRZYKŁADOWE UŻYCIE =====
df_aggregated = aggregate_by_id(df)

df_aggregated.to_excel('data/cc_fasca_gotriple_query.xlsx', index=False)


























