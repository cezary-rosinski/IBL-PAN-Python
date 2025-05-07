from requests import get
import glob
from tqdm import tqdm
import pandas as pd
import regex as re
import requests
from concurrent.futures import ThreadPoolExecutor
import pickle
from opencitations_token import oc_token
import csv
from collections import Counter
from core_api_key import core_api_key
from functools import partial

#%% OC metadata dump

path = r'D:\IBL\OpenCitations\csv_final/'
files = [file for file in glob.glob(path + '*.csv', recursive=True)]

text_counter = 0

headers = ['venue-name', 'venue-ids', 'type', 'issn', 'issn_count']

with open("data/opencitations_metadata.csv", "w", newline="", encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=headers)
    writer.writeheader()
    
    for file in tqdm(files):
        # file = files[0]
        df = pd.read_csv(file, low_memory=False)
        
        no_of_texts = df.shape[0]
        text_counter += no_of_texts
        
        v_iteration = list(zip(df['venue'].to_list(), df['type'].to_list()))
        #v_p_iteration = list(zip(df['venue'].to_list(), df['publisher'].to_list(), df['type'].to_list()))
        
        temp_list = []
        for v, t in v_iteration:
            # v, p = v_p_iteration[0]
            v_name = v.split('[')[0].strip() if pd.notnull(v) else None
            v_ids = v.replace(v_name, '').strip() if pd.notnull(v) else None
            if pd.notnull(v) and 'issn' in v:
                v_issn = re.findall('(?>issn\:)(.{4}-.{4})', v_ids)
                if v_issn:
                    v_issn = v_issn[0]
                else: v_issn = None
            else: v_issn = None
            if v_issn:
                temp_dict = {'venue-name': v_name,
                             'venue-ids': v_ids,
                             'type': t,
                             'issn': v_issn}
                
                temp_list.append(temp_dict)
        issn_counter = Counter(d.get('issn') for d in temp_list)
        for d in temp_list:
            d['issn_count'] = issn_counter.get(d.get('issn'))
        dict_set = [dict(s) for s in set(frozenset(d.items()) for d in temp_list)]
        for ds in dict_set:
            writer.writerow(ds)

# with open('data/opencitations.p', 'wb') as fp:
#     pickle.dump(result, fp, protocol=pickle.HIGHEST_PROTOCOL)
    
# with open('data/opencitations.p', 'rb') as fp:
#     result_bn_years = pickle.load(fp)

#%% issn api

df = pd.read_csv("data/opencitations_metadata.csv")

df_grouped = df.groupby("issn").agg({
    "venue-name": "first",
    "venue-ids": "first",
    "type": "first",
    "issn_count": "sum"
}).reset_index()

issns = df_grouped['issn'].to_list()

issn_country = {}
# for issn in tqdm(issns):
def get_country_for_issn(issn):
    url = f"https://portal.issn.org/resource/ISSN/{issn}?format=json"
    try:
        r = requests.get(url).json()
        try:
            country = [e.get('label') for e in r.get('@graph') if 'id.loc.gov/vocabulary/countries' in e.get('@id')][0]
        except (TypeError, IndexError):
            country = None
        issn_country.update({issn: country})
    except ValueError:
        issn_country.update({issn: None})

issn_country = {}
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(get_country_for_issn, issns),total=len(issns)))
    
#issn country stats
countries = set(list(issn_country.values())) #228 państw
Counter(v for k,v in issn_country.items() if v == 'Poland') #2063 issny zarejestrowane w Polsce
Counter(v for k,v in issn_country.items() if v == 'France') #2115
Counter(v for k,v in issn_country.items() if v == 'England') #8565
Counter(v for k,v in issn_country.items() if v == 'Germany') #4640
Counter(v for k,v in issn_country.items() if v == 'Czech Republic') #401
Counter(v for k,v in issn_country.items() if v == 'Italy') #883
Counter(v for k,v in issn_country.items() if v == 'Spain') #1791

countries_issn_counter = dict(Counter(v for k,v in issn_country.items()))
df_grouped['country'] = df_grouped['issn'].apply(lambda x: issn_country.get(x))
df_grouped['country-counter'] = df_grouped['country'].apply(lambda x: countries_issn_counter.get(x))

df_grouped.to_excel('data/opencitations_journals.xlsx', index=False)

#%% df journals analysis

df_journals = pd.read_excel('data/opencitations_journals.xlsx')

df_journals.loc[df_journals['country'] == 'England'].shape[0]



#%% core api
#szukać subjects

issn_list = set(df['issn'].to_list())

# def harvest_core(issn, api_key):
core_results = []
for issn in tqdm(issn_list):
    # issn = '1453-1305'
    # api_key = core_api_key
    
    url = f'https://api.core.ac.uk/v3/journals/issn:{issn}?apiKey={core_api_key}'
    r = requests.get(url)
    result = r.json()
    result = {k:v for k,v in result.items() if k in ['language', 'title', 'subjects', 'publisher']}
    # return {issn: result}
    core_results.append({issn: result})

# harvest_with_key = partial(harvest_core, api_key=core_api_key)

# with ThreadPoolExecutor() as executor:
#     core_results = list(tqdm(
#         executor.map(harvest_with_key, issn_list),
#         total=len(issn_list),
#         desc="Pobieranie z CORE"
#     ))

#https://api.core.ac.uk/v3/journals/issn:1453-1305?apiKey=xxx

#%% API literaturoznawstwo

df = pd.read_excel('data/czasopisma_literaturoznawcze_scopus.xlsx')
issns_list = [e for e in df['ISSN'].to_list() + df['e-ISSN'].to_list() if pd.notnull(e)]

# API_CALL = "https://w3id.org/oc/meta/api/v1/metadata/issn:"
API_CALL_journal = "https://opencitations.net/meta/api/v1/metadata/issn:"
API_CALL_citations = "https://opencitations.net/index/api/v2/venue-citation-count/issn:"
HTTP_HEADERS = {"authorization": oc_token}

responses = []
for issn in tqdm(issns_list):
    # issn = issns_list[0]
    url = API_CALL_journal + issn
    response_journal = get(url, headers=HTTP_HEADERS).json()
    
    url2 = API_CALL_citations + issn
    response_citations = get(url2, headers=HTTP_HEADERS).json()
    
    temp_dict = {issn: [True if response_journal else False, response_citations[0].get('count')]}
    responses.append(temp_dict)

unique_issn = []
for i, row in df.iterrows():
    if not pd.isnull(row['ISSN']):
        unique_issn.append(row['ISSN'])
    else: unique_issn.append(row['e-ISSN'])
    
responses_unique = [e for e in responses if list(e.keys())[0] in unique_issn]
len([e for e in responses_unique if e.get(list(e.keys())[0])[-1] == '0'])
# test = "https://w3id.org/oc/meta/api/v1/metadata/issn:2083-2222?require=doi"
# testa = get(test, headers=HTTP_HEADERS)
# testa.text

#%% OC API
doi = '10.14746/fp.2016.3.26703' #0
doi = '10.14746/fp.2020.20.24906'

API_CALL = f"https://opencitations.net/index/api/v2/references/doi:{doi}"
API_CALL = "https://opencitations.net/index/api/v2/citations/doi:{doi}"
API_CALL = "https://opencitations.net/index/api/v2/reference-count/doi:{doi}"
API_CALL = "https://w3id.org/oc/meta/api/v1/metadata/doi:{doi}"
API_CALL = "https://w3id.org/oc/meta/api/v1/metadata/issn:2719-4167"
API_CALL = "https://opencitations.net/index/api/v2/citations/doi:10.31261/PiL.2021.03.05"
HTTP_HEADERS = {"authorization": oc_token}

test = get(API_CALL, headers=HTTP_HEADERS).json()




API_CALL = "https://opencitations.net/index/api/v2/references/doi:10.1186/1756-8722-6-59"
API_CALL = "https://opencitations.net/index/api/v2/citations/doi:10.1186/1756-8722-6-59"
API_CALL = "https://opencitations.net/index/api/v2/reference-count/doi:10.1186/1756-8722-6-59"
API_CALL = "https://opencitations.net/index/api/v2/metadata/doi:10.1186/1756-8722-6-59"

API_CALL = "https://w3id.org/oc/meta/api/v1/metadata/doi:10.1007/978-1-4020-9632-7"
HTTP_HEADERS = {"authorization": oc_token}

get(API_CALL, headers=HTTP_HEADERS)

test = get(API_CALL, headers=HTTP_HEADERS).json()
















