import sys
sys.path.insert(1, 'C:/Users/Cezary/Documents/SPUB-project')
from SPARQLWrapper import SPARQLWrapper, JSON
import requests
from SPUB_wikidata_connector import get_wikidata_label
from tqdm import tqdm
import pandas as pd


user_agent = "WDQS-example Python/%s.%s" % (sys.version_info[0], sys.version_info[1])
sparql = SPARQLWrapper("https://query.wikidata.org/sparql", agent=user_agent)
sparql.setQuery("""SELECT DISTINCT ?item ?itemLabel WHERE {
  SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE]". }
  {
    SELECT DISTINCT ?item WHERE {
      ?item p:P106 ?statement0.
      ?statement0 (ps:P106/(wdt:P279*)) wd:Q36180.
      ?item p:P607 ?statement1.
      ?statement1 (ps:P607/(wdt:P279*)) _:anyValueP607.
      ?item p:P136 ?statement2.
      ?statement2 (ps:P136/(wdt:P279*)) wd:Q131539.
    }
  }
}""")
sparql.setReturnFormat(JSON)

results = sparql.query().convert()

result = [e.get('item').get('value').replace('http://www.wikidata.org/entity/','') for e in results.get('results').get('bindings')]

final_dict = dict.fromkeys(result, {})

for k,v in tqdm(final_dict.items()):
    # k = result[0]
    url = f"https://www.wikidata.org/wiki/Special:EntityData/{k}.json"
    test = requests.get(url).json()
    
    person_name = get_wikidata_label(k, ['en'])
    place_of_birth = test.get('entities').get(k).get('claims').get('P19')[0].get('mainsnak').get('datavalue').get('value').get('id')
    url2 = f"https://www.wikidata.org/wiki/Special:EntityData/{place_of_birth}.json"
    test2 = requests.get(url2).json()
    place_name = get_wikidata_label(place_of_birth, ['en'])
    latitude = test2.get('entities').get(place_of_birth).get('claims').get('P625')[0].get('mainsnak').get('datavalue').get('value').get('latitude')
    longitude = test2.get('entities').get(place_of_birth).get('claims').get('P625')[0].get('mainsnak').get('datavalue').get('value').get('longitude')
    
    
    temp_dict = {'person_name': person_name,
                 'place_of_birth': place_name,
                 'latitude': latitude,
                 'longitude': longitude}
    final_dict[k] = temp_dict
    
df = pd.DataFrame().from_dict(final_dict, orient='index')
df.to_excel('triple_bonn.xlsx', index=False)





















