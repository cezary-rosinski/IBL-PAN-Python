import requests
from bs4 import BeautifulSoup
from my_functions import gsheet_to_df
import regex as re
from tqdm import tqdm
import pandas as pd

#%% reading literary awards spreadsheet

literary_awards = gsheet_to_df('1satFAR9-5DFGwk-PFrgcWVOBMahNs7RcQ3dY1BvLVvM', 'Awards')
literary_awards_wikidata = literary_awards.loc[literary_awards['Wikidata ID'].str.contains('wikidata', na=False)]
literary_awards_wikidata_list = literary_awards_wikidata['Wikidata ID'].to_list()
literary_awards_wikidata_list_ids = set([re.findall('Q.+$', e)[0] for e in literary_awards_wikidata_list])

#%% reading wikidata mapping

wikidata_mapping = gsheet_to_df('1satFAR9-5DFGwk-PFrgcWVOBMahNs7RcQ3dY1BvLVvM', 'wikidata mapping')
wikidata_properties = set([e for e in wikidata_mapping['Wikidata property'].to_list() if not isinstance(e, float)])
davids_model = wikidata_mapping["David's model"].to_list()
davids_model_with_properties = {k:v for k,v in dict(zip(wikidata_mapping['Wikidata property'], wikidata_mapping["David's model"])).items() if not isinstance(k, float)}


def get_wikidata_data(x):
    try:
        if x.get('mainsnak').get('datavalue').get('value').get('id'):
            return f"www.wikidata.org/wiki/{x.get('mainsnak').get('datavalue').get('value').get('id')}"
        elif x.get('mainsnak').get('datavalue').get('value').get('time'):
            return x.get('mainsnak').get('datavalue').get('value').get('time')
        elif x.get('mainsnak').get('datavalue').get('value').get('text'):
            return x.get('mainsnak').get('datavalue').get('value').get('text')
        elif x.get('mainsnak').get('datavalue').get('value').get('amount'):
            return list(x.get('mainsnak').get('datavalue').get('value').values())
    except AttributeError:
        return x.get('mainsnak').get('datavalue').get('value')
        

wikidata_response_dict = {}
for wikidata_id in tqdm(literary_awards_wikidata_list_ids):
    # wikidata_id = list(literary_awards_wikidata_list_ids)[0]
    # wikidata_id = 'Q37922'
    # wikidata_id = 'Q159909'
    url = f'https://www.wikidata.org/wiki/Special:EntityData/{wikidata_id}.json'
    result = requests.get(url).json()
    wikipedia_links = result.get('entities').get(wikidata_id).get('sitelinks')
    try:
        english_title = [e.get('value') for e in result.get('entities').get(wikidata_id).get('aliases').get('en')][0]
    except TypeError:
        english_title = None
    claims = {k:v for k,v in result.get('entities').get(wikidata_id).get('claims').items() if k in wikidata_properties}
    doe_model = {davids_model_with_properties.get(k):[get_wikidata_data(e) for e in v] for k,v in claims.items()}
    doe_model.update({'Official title in English': english_title})
    wikidata_response_dict.update({wikidata_id: doe_model})
    
df = pd.DataFrame.from_dict(wikidata_response_dict, orient='index')



#%% wikipedia

url = 'https://pl.wikipedia.org/wiki/Kategoria:Polskie_nagrody_literackie'

result = requests.get(url).content
soup = BeautifulSoup(result, 'lxml')
titles = [e.text for e in soup.select('#mw-pages a')]
links = [e['href'] for e in soup.select('#mw-pages a')]
soup.sel



