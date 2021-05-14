# przygotować testową listę rekordów bibliograficznych
# 300 ksiażek z bn (jakie?)
# wydobyć z nich osoby i związać z kartoteką wzorcową BN
# połączyć dane z viaf i wikidatą oraz wydobyć dodatkowe informacje do schematu PBL
import pandas as pd
import io
from google_drive_research_folders import PBL_folder
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
import gspread as gs
from my_functions import cSplit, df_to_mrc, mrk_to_df, mrc_to_mrk
import datetime
import regex as re
from gspread_dataframe import get_as_dataframe, set_with_dataframe
from tqdm import tqdm
import glob
import time
import json
from collections import Counter
import requests
from urllib.error import HTTPError
from http.client import RemoteDisconnected

#def

def marc_parser_dict_for_field(string, subfield_code):
    subfield_list = re.findall(f'{subfield_code}.', string)
    dictionary_field = {}
    for subfield in subfield_list:
        subfield_escape = re.escape(subfield)
        string = re.sub(f'({subfield_escape})', r'❦\1', string)
    for subfield in subfield_list:
        subfield_escape = re.escape(subfield)
        regex = f'(^)(.*?\❦{subfield_escape}|)(.*?)(\,{{0,1}})((\❦{subfield_code})(.*)|$)'
        value = re.sub(regex, r'\3', string).strip()
        dictionary_field[subfield] = value
    return dictionary_field

# 01 - wydobycie listy osób
start_time = time.time()
# file_path = 'bn_harvested_2021_05_12.mrc'
# path_mrk = file_path.replace('.mrc', '.mrk')
# mrc_to_mrk(file_path, path_mrk)
path_mrk = 'bn_harvested_2021_05_12.mrk'

encoding = 'utf-8'
new_list = []

marc_list = io.open(path_mrk, 'rt', encoding = encoding).read().splitlines()

mrk_list = []
for row in marc_list:
    if row.startswith('=LDR'):
        mrk_list.append([row])
    else:
        if row:
            mrk_list[-1].append(row)
 
lista_osob = []           
for sublist in tqdm(mrk_list, total=len(mrk_list)):
    for el in sublist:
        if el.startswith('=100'):
        # if el.startswith(('=100', '=600', '=700')):
            # el = el[8:].replace('$2DBN', '').strip()
            el = re.sub('(\$e|\$t).*', '', el[8:]).replace('$2DBN', '').strip()
            lista_osob.append(el)

slownik_osob = Counter(lista_osob).most_common(200)
slownik_osob = [e[0] for e in slownik_osob]
            
rekordy = []
for sublist in tqdm(mrk_list, total=len(mrk_list)):
    for el in sublist:
        if el.startswith('=100'):
            el = re.sub('(\$e|\$t).*', '', el[8:]).replace('$2DBN', '').strip()
            for osoba in slownik_osob:
                if osoba in el:
                    rekordy.append(sublist)
                    slownik_osob.remove(osoba)
                    break

osoby_z_rekordow = []
for sublist in tqdm(rekordy, total=len(rekordy)):
    for el in sublist:
        if el.startswith(('=100', '=600', '=700')):
            el = re.sub('(\$x|\$4|\$e|\.\$t).*', '', el[8:]).replace('$2DBN', '').strip()
            osoby_z_rekordow.append(el)

osoby_z_rekordow = list(set(osoby_z_rekordow))

# 02 - rekordy wzorcowe osób z BN
#w wynikach nie może być 'title'!!!
# https://data.bn.org.pl/api/authorities.xml?limit=100&name=To%C5%82stoj,%20Lew%20(1828-1910)
url = 'https://data.bn.org.pl/api/authorities.json?'
osoby_z_rekordow_lista_slownikow = []
for i, osoba in tqdm(enumerate(osoby_z_rekordow), total=len(osoby_z_rekordow)):
    slownik_osoby = {}
    # i = 381
    # osoba = osoby_z_rekordow[i]
    slownik_osoby['MARC name'] = osoba
    osoba = marc_parser_dict_for_field(osoba, '\$')
    osoba = ' '.join([v for k, v in osoba.items()])
    slownik_osoby['simple name'] = osoba
    params = {'name': osoba, 'limit':'100'}
    result = requests.get(url, params = params).json()
    authority = [e['marc']['fields'] for e in result['authorities'] if e['title'] == '']
    authority = [[d for d in e if any(a in ['001', '024', '100'] for a in d)] for e in authority]
    # authority = [e for sub in authority for e in sub]
    if len(authority) == 0:
        osoba2 = osoba.replace('?', '')
        slownik_osoby['simple name (no question mark)'] = osoba2
        params = {'name': osoba2, 'limit':'100'}
        result = requests.get(url, params = params).json()
        authority = [e['marc']['fields'] for e in result['authorities'] if e['title'] == '']
        authority = [[d for d in e if any(a in ['001', '024', '100'] for a in d)] for e in authority]
        # authority = [e for sub in authority for e in sub]
    for e in authority:
        person_in_authority = [a['100']['subfields'] for a in e if '100' in a]
        person_in_authority = [a for sub in person_in_authority for a in sub]
        person_in_authority = [[v for k,v in a.items()] for a in person_in_authority]
        person_in_authority = ' '.join([a for sub in person_in_authority for a in sub])
        if person_in_authority == osoba or person_in_authority == osoba2:
            for d in e:
                if '001' in d:
                    id = d['001']
                    if 'BN ID' in slownik_osoby:
                        slownik_osoby['BN ID'] = '❦'.join([id, slownik_osoby['BN ID']])
                    else:
                        slownik_osoby['BN ID'] = id
                elif '024' in d:
                    viaf = d['024']['subfields']
                    for el in viaf:
                        for di in el:
                            if 'a' in di:
                                viaf = re.findall('\d+', el['a'])[0]
                                if 'VIAF ID' in slownik_osoby:
                                    slownik_osoby['VIAF ID'] = '❦'.join([viaf, slownik_osoby['VIAF ID']])
                                else:
                                    slownik_osoby['VIAF ID'] = viaf
    slownik_osoby = {k:'❦'.join(list(set(v.split('❦')))) for k,v in slownik_osoby.items()}
    osoby_z_rekordow_lista_slownikow.append(slownik_osoby)


# 03 - utożsamienie z VIAF
# 04 - utożsamienie z wikidatą
url = 'https://query.wikidata.org/sparql'       
for i, osoba in enumerate(tqdm(osoby_z_rekordow_lista_slownikow, total=len(osoby_z_rekordow_lista_slownikow))):
    while True:
        try:
            viaf = osoba['VIAF ID']
            sparql_query = f"""PREFIX wdt: <http://www.wikidata.org/prop/direct/>
            SELECT distinct ?autor ?autorLabel ?birthplaceLabel ?deathplaceLabel ?birthdate ?deathdate ?sexLabel ?pseudonym ?occupationLabel WHERE {{ 
              ?autor wdt:P214 "{viaf}" ;
              optional {{ ?autor wdt:P19 ?birthplace . }}
              optional {{ ?autor wdt:P569 ?birthdate . }}
              optional {{ ?autor wdt:P570 ?deathdate . }}
              optional {{ ?autor wdt:P20 ?deathplace . }}
              optional {{ ?autor wdt:P21 ?sex . }}
              optional {{ ?autor wdt:P106 ?occupation . }}
              optional {{ ?autor wdt:P742 ?pseudonym . }}
            SERVICE wikibase:label {{ bd:serviceParam wikibase:language "pl". }}}}"""    
            results = requests.get(url, params = {'format': 'json', 'query': sparql_query})
            results = results.json()
            results_df = pd.json_normalize(results['results']['bindings'])
            columns = [e for e in results_df.columns.tolist() if 'value' in e]
            results_df = results_df[results_df.columns.intersection(columns)]       
            for column in results_df.drop(columns='autor.value'):
                results_df[column] = results_df.groupby('autor.value')[column].transform(lambda x: '❦'.join(x.drop_duplicates().astype(str)))
            results_df = results_df.drop_duplicates().reset_index(drop=True)   
            result = results_df.to_dict('records')
            osoby_z_rekordow_lista_slownikow[i]['wikidata ID'] = result
            time.sleep(1)
        except (AttributeError, KeyError, ValueError):
            time.sleep(1)
        except (HTTPError, RemoteDisconnected) as error:
            print(error)# time.sleep(61)
            time.sleep(5)
            continue
        break
print("--- %s seconds ---" % (time.time() - start_time))


for r in rekordy:
    for e in r:
        if '$aRodziewiczówna, Maria$d(1864-1944).' in e:
            print(e)




                    
# final_list = []
# for lista in rekordy:
#     slownik = {}
#     for el in lista:
#         if el[1:4] in slownik:
#             slownik[el[1:4]] += f"❦{el[6:]}"
#         else:
#             slownik[el[1:4]] = el[6:]
#     final_list.append(slownik)

# df = pd.DataFrame(final_list).drop_duplicates().reset_index(drop=True)
# fields = df.columns.tolist()
# fields = [i for i in fields if 'LDR' in i or re.compile('\d{3}').findall(i)]
# df = df.loc[:, df.columns.isin(fields)]
# fields.sort(key = lambda x: ([str,int].index(type("a" if re.findall(r'\w+', x)[0].isalpha() else 1)), x))
# df = df.reindex(columns=fields)   




























