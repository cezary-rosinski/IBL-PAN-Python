from requests import get
import glob
from tqdm import tqdm
import pandas as pd
import regex as re
import requests
from concurrent.futures import ThreadPoolExecutor
import pickle
from opencitations_token import oc_token

#%% OC metadata dump

path = r'D:\IBL\OpenCitations\csv_final/'
files = [file for file in glob.glob(path + '*.csv', recursive=True)]

venue_publisher = []
text_counter = 0
for file in tqdm(files):
    # file = files[0]
    df = pd.read_csv(file)
    v_p_iteration = list(zip(df['venue'].to_list(), df['publisher'].to_list(), df['type'].to_list()))
    no_of_texts = len(v_p_iteration)
    text_counter += no_of_texts
    temp_list = []
    for v, p, t in v_p_iteration:
        # v, p = v_p_iteration[0]
        v_name = v.split('[')[0].strip() if pd.notnull(v) else None
        p_name = p.split('[')[0].strip() if pd.notnull(p) else None
        v_ids = v.replace(v_name, '').strip() if pd.notnull(v) else None
        if pd.notnull(v) and 'issn' in v:
            v_issn = re.findall('(?>issn\:)(.{4}-.{4})', v_ids)
            if v_issn:
                v_issn = v_issn[0]
            else: v_issn = None
        else: v_issn = None
        temp_dict = {'venue-name': v_name,
                     'venue-ids': v_ids,
                     'publisher-name': p_name,
                     'publisher-ids': p.replace(p_name, '').strip() if pd.notnull(p) else None,
                     'type': t,
                     'issn': v_issn}
        temp_list.append(temp_dict)
    venue_publisher.extend(temp_list)

# with open('person_bn_publishing_years.p', 'wb') as fp:
#     pickle.dump(result, fp, protocol=pickle.HIGHEST_PROTOCOL)
    
with open('person_bn_publishing_years.p', 'rb') as fp:
    result_bn_years = pickle.load(fp)

#%% issn api

issns = set([e.get('issn') for e in venue_publisher if pd.notnull(e.get('issn'))])

issn_country = {}
# for issn in tqdm(issns):
def get_country_for_issn(issn):
    url = f"https://portal.issn.org/resource/ISSN/{issn}?format=json"
    try:
        r = requests.get(url).json()
        try:
            country = [e.get('label') for e in r.get('@graph') if 'id.loc.gov/vocabulary/countries' in e.get('@id')][0]
        except IndexError:
            country = None
        issn_country.update({issn: country})
    except ValueError:
        issn_country.update({issn: None})

issn_country = {}
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(get_country_for_issn, issns),total=len(issns)))   

#%% OC API
doi = '10.14746/fp.2016.3.26703' #0
doi = '10.14746/fp.2020.20.24906'
oc_token = 'c18c68a7-9b88-47a6-b189-e8638002b0f2'

API_CALL = f"https://opencitations.net/index/api/v2/references/doi:{doi}"
API_CALL = "https://opencitations.net/index/api/v2/citations/doi:{doi}"
API_CALL = "https://opencitations.net/index/api/v2/reference-count/doi:{doi}"
API_CALL = "https://w3id.org/oc/meta/api/v1/metadata/doi:{doi}"
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
