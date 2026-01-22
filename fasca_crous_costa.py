from itertools import product
import requests
from urllib.parse import urlencode

#%%

keywords_1 = ['government', 'state', 'public policy', 'public funding', 'ministry of culture', 'municipality', 'public administration']

keywords_2 = ['art', 'culture', 'heritage', 'museum', 'memorial', 'cinema', 'music', 'literature', 'festival', 'creative']

keywords_3 = ['value', 'identity', 'ideology', 'propaganda', 'soft power', 'social cohesion', 'patriotism', 'social change']

issues = list(product(keywords_1, keywords_2, keywords_3))
issues = [' AND '.join(e) for e in issues]

#%% params

params = {
    'q': 'Art',
    'fq': {
        'type': 'typ_article',
        'has_pdf': 'true',
        'in_language': 'en'
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
BASE_API_URL = 'https://api.gotriple.eu/api/'

objects_list = []
endpoint = 'documents'
headers = {
    "Accept": "application/json"
}
url = f'{BASE_API_URL}{endpoint}?{build_api_url(params)}'
while True:
    response = requests.get(url, headers=headers)
    if response.ok:
        objects_list.extend(response.json()['hydra:member'])
        print(url)
    if not response.json()['hydra:view'].get('hydra:next'):
        break
    else:
        url = BASE_API_URL + response.json()['hydra:view']['hydra:next'][1:]
print(len(objects_list))


test = response.json()

url

https://api.gotriple.eu/api/documents?q=machine%20learning&include_duplicates=false&page=1&size=25&fq=topic%3Asocio%3Byear%3A2020%2C2021&aggs=topic%2Cinclude%3Dsocio%2Cexclude%3Dpsy%2Csize%3D10%2Csort%3Dcount%2Corder%3Ddesc&sort=most_recent%3Adesc
https://api.gotriple.eu/documents?q=Art&fq=type%3Dtyp_article%3Bhas_pdf%3Dtrue%3Bin_language%3Den&include_duplicates=false&sort=name%3Adesc&page=1&size=100






























