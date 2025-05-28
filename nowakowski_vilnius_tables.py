import sys
import requests
from tqdm import tqdm
from my_functions import gsheet_to_df, simplify_string, cluster_strings, marc_parser_to_dict
from concurrent.futures import ThreadPoolExecutor
import json
import pandas as pd
from SPARQLWrapper import SPARQLWrapper, JSON
from ast import literal_eval

#%% def
def get_wikidata_label(wikidata_id, pref_langs = ['pl', 'en', 'fr', 'de', 'es', 'cs']):
    # wikidata_id = 'Q130690218'
    url = f'https://www.wikidata.org/wiki/Special:EntityData/{wikidata_id}.json'
    try:
        result = requests.get(url).json()
        langs = [e for e in list(result.get('entities').get(wikidata_id).get('labels').keys()) if e in pref_langs]
        if langs:
            for lang in langs:
                label = result['entities'][wikidata_id]['labels'][lang]['value']
                break
        else: label = None
    except ValueError:
        label = None
    return label 

def harvest_wikidata_for_person(wikidata_id):
# for wikidata_id in tqdm(list(wikidata_ids)[565:570]):
    #wikidata_id = list(wikidata_ids)[1]
    try:
        url = f'https://www.wikidata.org/wiki/Special:EntityData/{wikidata_id}.json'
        result = requests.get(url).json()
        
        try:
            birthdate_value = result.get('entities').get(wikidata_id).get('claims').get('P569')[0].get('mainsnak').get('datavalue').get('value').get('time')[:11]
        except TypeError:
            birthdate_value = None
        except AttributeError:
            try:
                birthdate_value = result.get('entities').get(wikidata_id).get('claims').get('P569')[0].get('qualifiers').get('P1319')[0].get('datavalue').get('value').get('time')[:11]
            except TypeError:
                birthdate_value = result.get('entities').get(wikidata_id).get('claims').get('P569')[0].get('qualifiers').get('P1326')[0].get('datavalue').get('value').get('time')[:11]
            except AttributeError:
                birthdate_value = None
                
        try:
            deathdate_value = result.get('entities').get(wikidata_id).get('claims').get('P570')[0].get('mainsnak').get('datavalue').get('value').get('time')[:11]
        except TypeError:
            deathdate_value = None
        except AttributeError:
            try:
                deathdate_value = result.get('entities').get(wikidata_id).get('claims').get('P570')[0].get('qualifiers').get('P1319')[0].get('datavalue').get('value').get('time')[:11]
            except TypeError:
                deathdate_value = result.get('entities').get(wikidata_id).get('claims').get('P570')[0].get('qualifiers').get('P1326')[0].get('datavalue').get('value').get('time')[:11]
            except AttributeError:
                deathdate_value = None
            
        try:
            birth = result.get('entities').get(wikidata_id).get('claims').get('P19')
            birthplace_value = max(birth, key=len).get('mainsnak').get('datavalue').get('value').get('id')
        except (AttributeError, TypeError):
            birthplace_value = None
        if birthplace_value:
            try:
                birthplaceLabel_value = get_wikidata_label(birthplace_value)
            except TypeError:
                birthplaceLabel_value = None 
        else: birthplaceLabel_value = None
        try:
            death = result.get('entities').get(wikidata_id).get('claims').get('P20')
            deathplace_value = max(death, key=len).get('mainsnak').get('datavalue').get('value').get('id')
        except (AttributeError, TypeError):
            deathplace_value = None
        if deathplace_value:
            try:
                deathplaceLabel_value = get_wikidata_label(deathplace_value)
            except TypeError:
                deathplaceLabel_value = None
        else: deathplaceLabel_value = None
         
        temp_dict = {'person_wikidata': wikidata_id,
                     'birthdate.value': birthdate_value,
                     'deathdate.value': deathdate_value,
                     'birthplace_wikidata': birthplace_value,
                     'birthplaceLabel.value': birthplaceLabel_value,
                     'deathplace_value': deathplace_value,
                     'deathplaceLabel.value': deathplaceLabel_value}
        wikidata_supplement.append(temp_dict)
    except ValueError:
        errors.append(wikidata_id)

#%% people

df_people = gsheet_to_df('1M2gc-8cGZ8gh8TTnm4jl430bL4tccdri9nw-frUWYLQ', 'people')
wikidata_ids = set([e for e in df_people['person_wikidata_id'].to_list() if isinstance(e, str)])

errors = []
wikidata_supplement = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(harvest_wikidata_for_person, wikidata_ids),total=len(wikidata_ids)))

df_people_wikidata = pd.DataFrame(wikidata_supplement)

#%% productivity period

for p in wikidata_supplement:
    p = wikidata_supplement[0]
    if p.get('birthdate.value')[0] == '+':
        st = int(p.get('birthdate.value').split('-')[0][1:5])+25
    else: st = -int(p.get('birthdate.value').split('-')[1])+25
    if p.get('deathdate.value')[0] == '+':
        en = int(p.get('deathdate.value').split('-')[0][1:5])
    else: en = -int(p.get('deathdate.value').split('-')[1])
    
    productivity_period

from typing import Optional

def most_productive_century(birth_year: int, death_year: int) -> Optional[int]:
    """
    Zwraca numer stulecia, w którym osoba miała najwięcej lat aktywności.
    Przy remisie wybiera najpóźniejsze stulecie.
    
    > 0  : stulecie n.e. (np. 17 = XVII w. n.e.)
    < 0  : stulecie p.n.e. (np. -1 = I w. p.n.e., -2 = II w. p.n.e.)
    
    Aktywność: [birth_year + 25, death_year)
    Lata w systemie astronomicznym (1=1 n.e., 0=1 p.n.e., -1=2 p.n.e., ...).
    Jeśli zakres jest pusty (start >= end), zwraca None.
    """
    start = birth_year + 25
    end   = death_year
    if start >= end:
        return None

    def century_of(y: int) -> int:
        if y > 0:
            return (y - 1) // 100 + 1
        else:
            return -(((-y) - 1) // 100 + 1)

    def century_bounds(c: int) -> (int, int):
        if c > 0:
            # od (c-1)*100+1 do c*100+1 (exclusive)
            return ((c - 1) * 100 + 1, c * 100 + 1)
        else:
            absC = -c
            # c=-1 (I p.n.e.): od -(100-1)= -99 do -((1-1)*100-1)= 0 (exclusive)
            start_bc = -(absC * 100 - 1)
            end_bc   = -((absC - 1) * 100 - 1)
            return (start_bc, end_bc)

    # zakres stuleci do sprawdzenia
    c_start = century_of(start)
    c_end   = century_of(end - 1)
    step    = 1 if c_end >= c_start else -1

    # obliczamy nakładanie dla każdego stulecia
    overlaps = {}
    for c in range(c_start, c_end + step, step):
        s_c, e_c = century_bounds(c)
        ov_start = max(start, s_c)
        ov_end   = min(end,   e_c)
        overlaps[c] = max(0, ov_end - ov_start)

    if not overlaps:
        return None

    # znajdź maksymalną liczbę lat, a potem wybierz stulecie najpóźniejsze
    max_years = max(overlaps.values())
    # filtrujemy tylko te z wartością == max_years
    best_centuries = [c for c, yrs in overlaps.items() if yrs == max_years]
    return max(best_centuries)


# — Przykłady —

# 1) Kopernik: ur. 1473, zm. 1543 → aktywność 1498–1543
#    XVI w. (1501–1601): 3 lata (1498–1501)
#    XVII w. (1601–1701): 42 lata (1601–1640)
#    → zwróci 17
print(most_productive_century(-55, 31))    # 17

# 2) Remisowy przykład (sztuczny):
#    aktywność od 50 n.e. +25 = 75 do 175
#    I) I w. n.e.   (1–101): lata 75–101 = 26 lat
#    II) II w. n.e. (101–201): lata 101–175 = 74 lat
#    III) III w. n.e. (201–301): 0 lat
#    → oczywiście 102–201 daje jednostkowe zwycięstwo, ale dla prawdziwego remisu:
#    weźmy np. zakres 150–250:
#      II w. (101–201): 51 lat (150–201)
#      III w.(201–301): 49 lat (201–250)
#    → II w. jednak większe; trudno wygenerować prawdziwy 50–50 bez celowej konstrukcji
#    Ale przy remisie np. [100,200):
#      I w. (1–101):      1 rok  (100–101)
#      II w.(101–201):   99 lat



okres produktywności – wyliczany ze wzoru year of birth + 25 – year of death (jeśli okres produktywności przypada na dwa stulecia, wyliczamy udział i wybieramy wiek z większym % produktywności)
#%% places

# places from texts

# places from people


























