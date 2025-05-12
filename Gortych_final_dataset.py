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

# Przykład użycia
viaf_id = "59157273"  # zamień na swój identyfikator
qid = get_wikidata_qid_from_viaf(viaf_id)
print(f"QID: {qid}")

#%%

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










