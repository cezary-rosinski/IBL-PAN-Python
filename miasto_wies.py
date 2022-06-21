import glob
import json
import requests
from xml.etree import ElementTree
from tqdm import tqdm
from my_functions import gsheet_to_df
import regex as re

# bazować na danych stąd: https://docs.google.com/spreadsheets/d/1U7vjHwLI7BQ7OHtM7x7NodW0t2vqnNOqkrALuyXSWw8/edit#gid=0

#%% wczytanie zasobów json

# path = r'C:\Users\Cezary\Downloads\miasto_wieś_metadata/'
# files = [f for f in glob.glob(path + '*.json', recursive=True)]

novels_df = gsheet_to_df('1U7vjHwLI7BQ7OHtM7x7NodW0t2vqnNOqkrALuyXSWw8', 'Arkusz1')
polona_ids = [e for e in novels_df['id'].to_list() if not(isinstance(e, float))]

bn_dict = {}
for file in tqdm(polona_ids):
    file = polona_ids[0]
    polona_id = re.findall('\d+', file)[0]
    query = f'https://polona.pl/api/entities/{polona_id}'
    polona_response = requests.get(query).json()
    bn_id = polona_response['semantic_relation'].split('/')[-1]
    query = f'http://khw.data.bn.org.pl/api/polona-lod/{bn_id}'
    polona_lod = requests.get(query).json()
    query = f'https://data.bn.org.pl/api/institutions/bibs.json?id={bn_id}'
    bn_response = requests.get(query).json()['bibs'][0]
    temp_dict = {file:{'polona_id':polona_id,
                       'bn_id':bn_id,
                       'polona_response':polona_response,
                       'polona_lod':polona_lod,
                       'bn_response':bn_response}}
    bn_dict.update(temp_dict)























    dict_data.update({'katalog_bn_info': response})
    
    
    # file = files[0]
    with open(file, encoding='utf-8') as d:
        dict_data = json.load(d)
        try:
            pl_id = dict_data['pl_record_no']
        except KeyError:
            query = f'https://polona.pl/api/entities/{dict_data["pl_id"]}'
            pl_id = requests.get(query).json()
            
        query = f'http://khw.data.bn.org.pl/api/polona-lod/{pl_id}'
        # query = f'http://khw.data.bn.org.pl/api/nlp_id/bibs?id=b1935990{pl_id}'
        # response = requests.get(query)
        # tree = ElementTree.fromstring(response.content)
        # for e in tree:
        #     print(e)
        response = requests.get(query).json()
        dict_data.update({'polona_lod_info': response})
        query = f'https://data.bn.org.pl/api/institutions/bibs.json?id={pl_id}'
        response = requests.get(query).json()
        dict_data.update({'katalog_bn_info': response})
        bn_dict[dict_data['pl_id']] = dict_data
        
        
    