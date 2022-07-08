import requests
from tqdm import tqdm
import numpy as np
import wikipedia



url = 'http://api.geonames.org/searchJSON?'
params = {'username': 'crosinski', 'q': 'Upita', 'featureClass': 'P', 'style': 'FULL'}
result = requests.get(url, params=params).json()

test = [(e['name'], e['lat'], e['lng'], [f['name'] for f in e['alternateNames'] if f['lang'] != 'link']) for e in result['geonames']]




miejscowosci = ['Żmudź', 'Wodokty', 'Lubicz', 'Upita', 'Kiejdany', 'Taurogi']

url = 'http://api.geonames.org/searchJSON?'
miejscowosci_total = {}
for m in tqdm(miejscowosci):
    # m = 'Upita'
    params = {'username': 'crosinski', 'q': m, 'featureClass': 'P', 'style': 'FULL'}
    result = requests.get(url, params=params).json()  
    geonames_resp = [[e['geonameId'], e['name'], e['lat'], e['lng'], [f['name'] for f in e['alternateNames']] if 'alternateNames' in e else []] for e in result['geonames']]
    [e[-1].append(e[1]) for e in geonames_resp]
    if len(geonames_resp) == 0:
        miejscowosci_total[m] = geonames_resp
    if len(geonames_resp) == 1:
        miejscowosci_total[m] = geonames_resp
    elif len(geonames_resp) > 1:    
        for i, resp in enumerate(geonames_resp):
            # i = -1
            # resp = geonames_resp[-1]
            if len(resp[-1]) > 1:
                wikipedia_link = [e for e in resp[-1] if 'wikipedia' in e][0]
                wikipedia_title = wikipedia_link.replace('https://en.wikipedia.org/wiki/','')
                wikipedia_query = f'https://en.wikipedia.org/w/api.php?action=query&format=json&prop=pageprops&ppprop=wikibase_item&redirects=1&titles={wikipedia_title}'
                wikipedia_result = requests.get(wikipedia_query).json()['query']['pages']
                wikidata_id = wikipedia_result[list(wikipedia_result.keys())[0]]['pageprops']['wikibase_item']
                wikidata_query = requests.get(f'https://www.wikidata.org/wiki/Special:EntityData/{wikidata_id}.json').json()
                labels = {v['value'] for k,v in wikidata_query['entities'][wikidata_id]['labels'].items()}
                geonames_resp[i][-1].extend(labels)
                geonames_resp[i][-1].remove(wikipedia_link)
        miejscowosci_total[m] = geonames_resp

test = {k:[e for e in v if any(k == f for f in e[-1])] for k,v in miejscowosci_total.items()}


v = miejscowosci_total['Lubicz']
[e[-1] for e in v]

page = 'Upyt%C4%97'
wikipedia_page = wikipedia.WikipediaPage(page)
page(title=page).page_id

import json

with open(r"C:\Users\Cezary\Downloads\pl-6058.ner.json") as f:
    test = json.load(f)



nazwy = set([e['text'] for e in test])



































'https://en.wikipedia.org/wiki/Upyt%C4%97'

'https://en.wikipedia.org/w/api.php?action=query&prop=pageprops&ppprop=wikibase_item&redirects=1&titles=ARTICLE_NAME'
test = 'https://en.wikipedia.org/w/api.php?action=query&format=json&prop=pageprops&ppprop=wikibase_item&redirects=1&titles=Upyt%C4%97'
result = requests.get(test).json()['query']['pages']
wikidata_id = result[list(result.keys())[0]]['pageprops']['wikibase_item']

wikidata_query = requests.get(f'https://www.wikidata.org/wiki/Special:EntityData/{wikidata_id}.json').json()







q_country = ccodes[country.lower()]
params = {'username': 'crosinski', 'q': city, 'featureClass': 'P', 'country': q_country}
result = requests.get(url, params=params).json()
places_geonames_extra_2.update({old: max(result['geonames'], key=lambda x: x['population'])})
except (KeyError, ValueError):
errors.append((old, city, country))