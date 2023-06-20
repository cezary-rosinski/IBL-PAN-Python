#to do:
#zmienic wagę dla sąsiadów, którzy są z innej rodziny na 1.5    
#zmienić dobór stron na wikidacie i uwzglednić languages spoken, written or signed, odznaczyć related values
    
from tqdm import tqdm
import requests
import pandas as pd
import time
from urllib.error import HTTPError
from http.client import RemoteDisconnected
import regex as re
from SPARQLWrapper import SPARQLWrapper, JSON
from collections import defaultdict
import sys
from urllib.error import HTTPError, URLError
from concurrent.futures import ThreadPoolExecutor
from collections import Counter

#%%

polish_query = """SELECT DISTINCT ?item ?itemLabel WHERE {
  SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE]". }
  {
    SELECT DISTINCT ?item WHERE {
      ?item p:P6886 ?statement0.
      ?statement0 (ps:P6886/(wdt:P279*)) wd:Q809.
      ?item p:P106 ?statement1.
      ?statement1 (ps:P106/(wdt:P279*)) wd:Q36180.
    }
  }
}"""
czech_query = """SELECT DISTINCT ?item ?itemLabel WHERE {
  SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE]". }
  {
    SELECT DISTINCT ?item WHERE {
      ?item p:P6886 ?statement0.
      ?statement0 (ps:P6886/(wdt:P279*)) wd:Q9056.
      ?item p:P106 ?statement1.
      ?statement1 (ps:P106/(wdt:P279*)) wd:Q36180.
    }
  }
}"""

spanish_query = """SELECT DISTINCT ?item ?itemLabel WHERE {
  SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE]". }
  {
    SELECT DISTINCT ?item WHERE {
      ?item p:P27 ?statement0.
      ?statement0 (ps:P27/(wdt:P279*)) wd:Q29.
      ?item p:P106 ?statement1.
      ?statement1 (ps:P106/(wdt:P279*)) wd:Q36180.
      ?item p:P6886 ?statement2.
      ?statement2 (ps:P6886/(wdt:P279*)) wd:Q1321.
    }
  }
}"""

bulgarian_query = """SELECT DISTINCT ?item ?itemLabel WHERE {
  SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE]". }
  {
    SELECT DISTINCT ?item WHERE {
      ?item p:P6886 ?statement0.
      ?statement0 (ps:P6886/(wdt:P279*)) wd:Q7918.
      ?item p:P106 ?statement1.
      ?statement1 (ps:P106/(wdt:P279*)) wd:Q36180.
    }
  }
}"""

user_agent = "WDQS-example Python/%s.%s" % (sys.version_info[0], sys.version_info[1])
sparql = SPARQLWrapper("https://query.wikidata.org/sparql", agent=user_agent)
sparql.setQuery(bulgarian_query)
sparql.setReturnFormat(JSON)
while True:
    try:
        results = sparql.query().convert()
        break
    except HTTPError:
        time.sleep(2)
    except URLError:
        time.sleep(5)

spanish_writers_wikidata = [e.get('item').get('value') for e in results.get('results').get('bindings')]
czech_writers_wikidata = [e.get('item').get('value') for e in results.get('results').get('bindings')]
polish_writers_wikidata = [e.get('item').get('value') for e in results.get('results').get('bindings')]
bulgarian_writers_wikidata = [e.get('item').get('value') for e in results.get('results').get('bindings')]


def get_wikidata_label(wikidata_url):   
# for wikidata_url in wikidata_urls:
    # wikidata_url = polish_writers_wikidata[140]
    # wikidata_id = 'Q254032'
    wikidata_id = re.findall('Q.+$', wikidata_url)[0]
    url = f'https://www.wikidata.org/wiki/Special:EntityData/{wikidata_id}.json'
    result = requests.get(url).json()
    try:
        label = result.get('entities').get(wikidata_id).get('labels').get('pl').get('value')
    except AttributeError:
        try:
            label = result.get('entities').get(wikidata_id).get('labels').get('en').get('value')
        except AttributeError:
            label = None
    languages = [e for e in list(result.get('entities').get(wikidata_id).get('sitelinks').keys()) if len(e) in [6,7]]
    wikidata_dict.update({wikidata_url: {'label': label,
                                         'languages': languages}})
 
wikidata_dict = {}
with ThreadPoolExecutor() as executor:
    list(tqdm(executor.map(get_wikidata_label,bulgarian_writers_wikidata), total=len(bulgarian_writers_wikidata)))

lang_for_writers = {v.get('label'):v.get('languages') for k,v in wikidata_dict.items() if v.get('label')}

# writers_no_languages = {k:len(v) for k,v in lang_for_writers.items()}

languages_for_poland = [e for sub in lang_for_writers.values() for e in sub]
languages_for_spain = [e for sub in lang_for_writers.values() for e in sub]
languages_for_bulgaria = [e for sub in lang_for_writers.values() for e in sub]


rodziny_jezykow = {'słowiańskie': ['Polska', 'Czechy', 'Słowacja', 'Rosja', 'Białoruś', 'Ukraina', 'Serbia', 'Chorwacja', 'Słowenia', 'Bułgaria', 'Macedonia'],
                   'germańskie': ['Szwecja', 'Norwegia', 'Dania', 'Islandia', 'Wyspy Owcze', 'Niemcy', 'Holandia', 'Luksemburg', 'Wielka Brytania', 'Szwajcaria'],
                   'romańskie': ['Hiszpania', 'Portugalia', 'Francja', 'Włochy', 'Rumunia', 'Szwajcaria'],
                   'bałtyckie': ['Litwa', 'Łotwa'],
                   'ugrofińskie': ['Estonia', 'Finlandia', 'Węgry'],
                   'pozostałe': ['Grecja', 'Albania', 'Turcja']}
sasiedzi = {'Polska': ['Niemcy', 'Czechy', 'Słowacja', 'Ukraina', 'Białoruś', 'Litwa', 'Rosja'],
            'Hiszpania': ['Portugalia', 'Francja'],
            'Czechy': ['Polska', 'Słowacja', 'Niemcy', 'Austria', 'Węgry'],
            'Macedonia': ['Grecja', 'Albania', 'Bułgaria', 'Kosowo', 'Serbia'],
            'Bułgaria': ['Rumunia', 'Grecja', 'Serbia', 'Macedonia', 'Turcja']}

jezyki_wikipedii = {'pl': 'Polska',
                    'cs': 'Czechy',
                    'de': 'Niemcy',
                    'sk': 'Słowacja',
                    'uk': 'Ukraina',
                    'ru': 'Rosja',
                    'be': 'Białoruś',
                    'lt': 'Litwa',
                    'pt': 'Portugalia',
                    'fr': 'Francja',
                    'sr': 'Serbia',
                    'hr': 'Chorwacja',
                    'bg': 'Bułgaria',
                    'sl': 'Słowenia',
                    'mk': 'Macedonia',
                    'it': 'Włochy',
                    'ro': 'Rumunia',
                    'es': 'Hiszpania',
                    'el': 'Grecja',
                    'aln': 'Albania',
                    'tk': 'Turcja'}


points = 0
points_dict = {0:0, 1:0, 2:0, 3:0}
for lang in languages_for_poland:
    # lang = languages_for_poland[0]
    # lang = 'plwiki'
    lang = lang.replace('wiki','')
    country = jezyki_wikipedii.get(lang)
    if not country:
        points += 3
        points_dict[3] +=1
    elif country == 'Polska':
        points_dict[0] +=1
    elif country in sasiedzi.get('Polska'):
        points += 1
        points_dict[1] +=1
    elif country in rodziny_jezykow.get('słowiańskie'):
        points += 2
        points_dict[2] +=1
    else:
        points += 3
        points_dict[3] +=1

13512/778 #PL liczba punktów na osobę = 17.37
13512/6469 #PL wartość relacji osoby i języka strony = 2.09
{0: 720, 1: 1748, 2: 239, 3: 3762}
{k:v/6469 for k,v in {0: 720, 1: 1748, 2: 239, 3: 3762}.items()}

points = 0
points_dict = {0:0, 1:0, 2:0, 3:0}
for lang in languages_for_spain:
    # lang = languages_for_poland[0]
    # lang = 'plwiki'
    lang = lang.replace('wiki','')
    country = jezyki_wikipedii.get(lang)
    if not country:
        points += 3
        points_dict[3] +=1
    elif country == 'Hiszpania':
        points_dict[0] +=1
        pass
    elif country in sasiedzi.get('Hiszpania'):
        points += 1
        points_dict[1] +=1
    elif country in rodziny_jezykow.get('romańskie'):
        points += 2
        points_dict[2] +=1
    else:
        points += 3
        points_dict[3] +=1
        
11671/606 #ES liczba punktów na osobę = 19.26
11671/4629 #ES wartość relacji osoby i języka strony = 2.52
{0: 487, 1: 275, 2: 205, 3: 3662}
{k:v/4629 for k,v in {0: 487, 1: 275, 2: 205, 3: 3662}.items()}

points = 0
points_dict = {0:0, 1:0, 2:0, 3:0}
for lang in languages_for_bulgaria:
    # lang = languages_for_poland[0]
    # lang = 'plwiki'
    lang = lang.replace('wiki','')
    country = jezyki_wikipedii.get(lang)
    if not country:
        points += 3
        points_dict[3] +=1
    elif country == 'Bułgaria':
        points_dict[0] +=1
    elif country in sasiedzi.get('Bułgaria'):
        points += 1
        points_dict[1] +=1
    elif country in rodziny_jezykow.get('słowiańskie'):
        points += 2
        points_dict[2] +=1
    else:
        points += 3
        points_dict[3] +=1

1408/74 #BG liczba punktów na osobę = 19.02
1408/629 #BG wartość relacji osoby i języka strony = 2.24
{0: 73, 1: 41, 2: 178, 3: 337}
{k:v/629 for k,v in {0: 73, 1: 41, 2: 178, 3: 337}.items()}

count = dict(Counter(writers_no_languages.values()))

len({k:v for k,v in writers_no_languages.items() if v > 10})


#poklasyfikować kraje
###wybrać europejskie kraje
###dla każdego wyszukiwanego kraju wskazać sąsiadów
###klasyfikacja punktacji: 1 pkt za sąsiada, 2 pkt za rodzinę języków, jeśli nie sąsiad, 3 pkt nie sąsiad i nie z rodziny języków






















