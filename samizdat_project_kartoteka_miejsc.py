from my_functions import gsheet_to_df, marc_parser_1_field
import pandas as pd
import sys
sys.path.insert(1, 'C:/Users/Cezary/Documents/SPUB-project')
from SPUB_query_wikidata import query_wikidata
import requests
import time
from urllib.error import HTTPError
from http.client import RemoteDisconnected
from tqdm import tqdm
import json
import regex as re
import ast

#%% def
def ask_wikidata_for_places(list_of_viafs):
    list_of_dicts = []
    url = 'https://query.wikidata.org/sparql' 
    for viaf in tqdm(list_of_viafs):
        while True:
            try:
                sparql_query = f"""PREFIX wdt: <http://www.wikidata.org/prop/direct/>
                SELECT distinct ?author ?birthplace ?deathplace WHERE {{
                  ?author wdt:P214 "{viaf}" ;
                  optional {{ ?author wdt:P19 ?birthplace . }}
                  optional {{ ?author wdt:P20 ?deathplace . }}
                SERVICE wikibase:label {{ bd:serviceParam wikibase:language "pl". }}}}"""    
                results = requests.get(url, params = {'format': 'json', 'query': sparql_query})
                results = results.json()
                results_df = pd.json_normalize(results['results']['bindings'])
                columns = [e for e in results_df.columns.tolist() if 'value' in e]
                results_df = results_df[results_df.columns.intersection(columns)]       
                for column in results_df.drop(columns='author.value'):
                    if 'value' in column:
                        results_df[column] = results_df.groupby('author.value')[column].transform(lambda x: '❦'.join(x.drop_duplicates().astype(str)))
                results_df = results_df.drop_duplicates().reset_index(drop=True)   
                result = results_df.to_dict('records')
                list_of_dicts.append(result[0])
                time.sleep(2)
            except (AttributeError, KeyError, ValueError):
                time.sleep(2)
            except (HTTPError, RemoteDisconnected) as error:
                print(error)# time.sleep(61)
                time.sleep(5)
                continue
            break
    return list_of_dicts

#%% miejsca z haseł osobowych
# z każdego arkusza biorę tylko te osoby, które mają tak lub new i z nich wyjmuję informacje nt. miejsc
kartoteka_osob = pd.DataFrame()

for worksheet in ['pojedyncze ok', 'grupy_ok', 'osoby_z_jednym_wierszem', 'reszta', 'zapomniani']:
    temp_df = gsheet_to_df('1xOAccj4SK-4olYfkubvittMM8R-jwjUjVhZXi9uGqdQ', worksheet)
    temp_df = temp_df[temp_df['decyzja'].isin(['tak', 'new'])]
    kartoteka_osob = pd.concat([kartoteka_osob, temp_df])
    
# test = [e.split('❦') for e in kartoteka_osob['occupationLabel.value'].dropna().to_list()]
# test = pd.DataFrame(sorted(list(set([e for sub in test for e in sub]))), columns=['occupation'])
# test.to_excel('samizdat_occupation.xlsx', index=False)
    
viafy_osob = kartoteka_osob.loc()[kartoteka_osob['autor.value'].notnull()]['viaf_ID'].drop_duplicates().to_list()

list_of_dicts = ask_wikidata_for_places(viafy_osob)

with open('samizdat_kartoteka_osob_viaf.json', 'w', encoding='utf-8') as f:
    json.dump(list_of_dicts, f)
    
#%% wikidata query dla miejsc z haseł osobowych
    
with open('samizdat_kartoteka_osob_viaf.json') as json_file:
    places_from_wikidata = json.load(json_file)
    
places_wikidata = [{v for k,v in e.items() if k in ['birthplace.value', 'deathplace.value']} for e in places_from_wikidata]
places_wikidata = [e.split('❦') for sub in places_wikidata for e in sub if e]
places_wikidata = list(set([e for sub in places_wikidata for e in sub if 'Q' in e]))#[:10]

languages = ['pl', 'en', 'fr', 'de', 'es', 'cs']

wikidata_places_dict = {}
for element in tqdm(places_wikidata):
    # element = 'http://www.wikidata.org/entity/Q3286190'
    wikidata_id = re.findall('Q.+$', element)[0]
    url = f'https://www.wikidata.org/wiki/Special:EntityData/{wikidata_id}.json'
    result = requests.get(url).json()
    for lang in languages:
        try:
            label = result['entities'][wikidata_id]['labels'][lang]['value']
            break
        except KeyError:
            pass
    try:
        geonames_id = [e['mainsnak']['datavalue']['value'] for e in result['entities'][wikidata_id]['claims']['P1566']]
    except KeyError: geonames_id = None
    try:
        country_id = [e['mainsnak']['datavalue']['value']['id'] for e in result['entities'][wikidata_id]['claims']['P17'] if e['rank'] == 'preferred'][0]
    except IndexError:
        country_id = [e['mainsnak']['datavalue']['value']['id'] for e in result['entities'][wikidata_id]['claims']['P17']][0]
    except KeyError: country_id = None
    url = f'https://www.wikidata.org/wiki/Special:EntityData/{country_id}.json'
    try:
        result_country = requests.get(url).json()
        for lang2 in languages:
            country_label = result_country['entities'][country_id]['labels'][lang2]['value']
            try:
                country_code = [e['mainsnak']['datavalue']['value'] for e in result_country['entities'][country_id]['claims']['P297']][0]
            except KeyError: country_code = None
            break
    except ValueError:
        country_label = None
        country_code = None
    try:
        coordinates = [(e['mainsnak']['datavalue']['value']['latitude'], e['mainsnak']['datavalue']['value']['longitude']) for e in result['entities'][wikidata_id]['claims']['P625']][0]
    except KeyError: coordinates = None
    temp_dict = {element: {'wikidata_id': wikidata_id,
                           'label': label,
                           'label_lang': lang,
                           'geonames_id': geonames_id,
                           'country_id': country_id,
                           'country_label': country_label,
                           'country_lang': lang2,
                           'country_code': country_code,
                           'coordinates': coordinates}}
    wikidata_places_dict.update(temp_dict)

df = pd.DataFrame.from_dict(wikidata_places_dict, orient='index').reset_index().rename(columns={'index':'wikidata_url'}) 
df.to_excel('samizdat_miejsca_z_osob.xlsx', index=False) 
#nazwa geonamesID, coordinates, country




#%% miejsca z rekordów bibliograficznych
rekordy_bn = gsheet_to_df('1y0E4mD1t4ZBN9YNmAwM2e912Q39Bb493Z6y5CSAuflw', 'Kopia arkusza bn_books')

# Pola 110, 710, 711, 120 -  w każdym z tych pól mogą wystąpić w podpolach 6 i 7.
# Pole 120  - podpole 8.
# Pole 210 - podpola A i E.
# Pole 600 - podpola G i C.

geo_w_biblio = [[110, '%6'], 
                [110, '%7'], 
                [120, '%8'], 
                [210, '%a'], 
                [210, '%e']]

places_from_bibliographical_records = pd.DataFrame()
for field, subfield in tqdm(geo_w_biblio):
    temp_df = marc_parser_1_field(rekordy_bn, 'id', field, '%')[['id', subfield]]
    temp_df = temp_df[temp_df[subfield] != '']
    temp_df['in_records'] = temp_df.apply(lambda x: f"{x['id']}-{field}-{subfield}", axis=1)
    temp_df['in_records'] = temp_df.groupby(subfield)['in_records'].transform(lambda x: '|'.join(x.str.strip()))
    temp_df = temp_df[[subfield, 'in_records']].drop_duplicates().reset_index(drop=True).rename(columns={subfield:'name'})
    places_from_bibliographical_records = places_from_bibliographical_records.append(temp_df)
    
places_from_bibliographical_records['in_records'] = places_from_bibliographical_records.groupby('name')['in_records'].transform(lambda x: '|'.join(x.str.strip()))
places_from_bibliographical_records = places_from_bibliographical_records.drop_duplicates().reset_index(drop=True)
    
places_from_bibliographical_records.to_excel('samizdat_miejsca_z_rekordów.xlsx', index=False)


#%% merging
df = pd.read_excel('samizdat_miejsca_z_osob.xlsx')
places_from_bibliographical_records = ["http://www.wikidata.org/wiki/Q761",
"http://www.wikidata.org/wiki/Q586",
"http://www.wikidata.org/wiki/Q41252",
"http://www.wikidata.org/wiki/Q1799",
"http://www.wikidata.org/wiki/Q102350",
"http://www.wikidata.org/wiki/Q2330083",
"http://www.wikidata.org/wiki/Q103217",
"http://www.wikidata.org/wiki/Q104712",
"http://www.wikidata.org/wiki/Q61480",
"http://www.wikidata.org/wiki/Q142",
"http://www.wikidata.org/wiki/Q1792",
"http://www.wikidata.org/wiki/Q588",
"http://www.wikidata.org/wiki/Q385",
"http://www.wikidata.org/wiki/Q105084",
"http://www.wikidata.org/wiki/Q51432",
"http://www.wikidata.org/wiki/Q104731",
"http://www.wikidata.org/wiki/Q8194",
"http://www.wikidata.org/wiki/Q573920",
"http://www.wikidata.org/wiki/Q147934",
"http://www.wikidata.org/wiki/Q52842",
"http://www.wikidata.org/wiki/Q605483",
"http://www.wikidata.org/wiki/Q773227",
"http://www.wikidata.org/wiki/Q102317",
"http://www.wikidata.org/wiki/Q284604",
"http://www.wikidata.org/wiki/Q31487",
"http://www.wikidata.org/wiki/Q1792",
"http://www.wikidata.org/wiki/Q268",
"http://www.wikidata.org/wiki/Q826068",
"http://www.wikidata.org/wiki/Q326582",
"http://www.wikidata.org/wiki/Q9308795",
"http://www.wikidata.org/wiki/Q84",
"http://www.wikidata.org/wiki/Q237512",
"http://www.wikidata.org/wiki/Q37333",
"http://www.wikidata.org/wiki/Q580",
"http://www.wikidata.org/wiki/Q2290846",
"http://www.wikidata.org/wiki/Q146820",
"http://www.wikidata.org/wiki/Q2396786",
"http://www.wikidata.org/wiki/Q6885133",
"http://www.wikidata.org/wiki/Q9248441",
"http://www.wikidata.org/wiki/Q231593",
"http://www.wikidata.org/wiki/Q82765",
"http://www.wikidata.org/wiki/Q924049",
"http://www.wikidata.org/wiki/Q92212",
"http://www.wikidata.org/wiki/Q7342",
"http://www.wikidata.org/wiki/Q556200",
"http://www.wikidata.org/wiki/Q422177",
"http://www.wikidata.org/wiki/Q246685",
"http://www.wikidata.org/wiki/Q36",
"http://www.wikidata.org/wiki/Q721937",
"http://www.wikidata.org/wiki/Q208473",
"http://www.wikidata.org/wiki/Q320007",
"http://www.wikidata.org/wiki/Q62937",
"http://www.wikidata.org/wiki/Q7031799",
"http://www.wikidata.org/wiki/Q104740",
"http://www.wikidata.org/wiki/Q598",
"http://www.wikidata.org/wiki/Q319813",
"http://www.wikidata.org/wiki/Q644002",
"http://www.wikidata.org/wiki/Q105060",
"http://www.wikidata.org/wiki/Q751140",
"http://www.wikidata.org/wiki/Q236083",
"http://www.wikidata.org/wiki/Q393",
"http://www.wikidata.org/wiki/Q387387",
"http://www.wikidata.org/wiki/Q9396389",
"http://www.wikidata.org/wiki/Q106281",
"http://www.wikidata.org/wiki/Q47554",
"http://www.wikidata.org/wiki/Q732001",
"http://www.wikidata.org/wiki/Q329960",
"http://www.wikidata.org/wiki/Q1851596",
"http://www.wikidata.org/wiki/Q110732",
"http://www.wikidata.org/wiki/Q270",
"http://www.wikidata.org/wiki/Q90",
"http://www.wikidata.org/wiki/Q220",
"http://www.wikidata.org/wiki/Q38",
"http://www.wikidata.org/wiki/Q103892",
"http://www.wikidata.org/wiki/Q167941"]

places_from_bibliographical_records = [re.findall('Q.+', e)[0] for e in places_from_bibliographical_records]
places_from_bibliographical_records = [e for e in places_from_bibliographical_records if e not in df['wikidata_id'].to_list()]

# places_from_bibliographical_records = ["Q36600",
# "Q93323",
# "Q611784",
# "Q28587",
# "Q894440",
# "Q21",
# "Q577867",
# "Q4093",
# "Q1818270",
# "Q104407",
# "Q430864",
# "Q207074",
# "Q40219",
# "Q101616",
# "Q241475",
# "Q996881",
# "Q3564800",
# "Q242000",
# "Q1460",
# "Q464763",
# "Q1632616",
# "Q104302",
# "Q456",
# "Q104074",
# "Q46787",
# "Q40416",
# "Q378120",
# "Q10815719",
# "Q992481",
# "Q203645",
# "Q4423536",
# "Q754773",
# "Q9365",
# "Q942842",
# "Q1000475",
# "Q393251",
# "Q7599331",
# "Q7067131",
# "Q917836",
# "Q184163",
# "Q661996",
# "Q1025109",
# "Q2749",
# "Q741876",
# "Q242105",
# "Q193929",
# "Q271858",
# "Q158280",
# "Q171800",
# "Q807",
# "Q1794"]

languages = ['pl', 'en', 'fr', 'de', 'es', 'cs']

wikidata_places_dict = {}
for element in tqdm(places_from_bibliographical_records):
    # element = 'http://www.wikidata.org/entity/Q3286190'
    wikidata_id = re.findall('Q.+$', element)[0]
    url = f'https://www.wikidata.org/wiki/Special:EntityData/{wikidata_id}.json'
    result = requests.get(url).json()
    for lang in languages:
        try:
            label = result['entities'][wikidata_id]['labels'][lang]['value']
            break
        except KeyError:
            pass
    try:
        geonames_id = [e['mainsnak']['datavalue']['value'] for e in result['entities'][wikidata_id]['claims']['P1566']]
    except KeyError: geonames_id = None
    try:
        country_id = [e['mainsnak']['datavalue']['value']['id'] for e in result['entities'][wikidata_id]['claims']['P17'] if e['rank'] == 'preferred'][0]
    except IndexError:
        country_id = [e['mainsnak']['datavalue']['value']['id'] for e in result['entities'][wikidata_id]['claims']['P17']][0]
    except KeyError: country_id = None
    url = f'https://www.wikidata.org/wiki/Special:EntityData/{country_id}.json'
    try:
        result_country = requests.get(url).json()
        for lang2 in languages:
            country_label = result_country['entities'][country_id]['labels'][lang2]['value']
            try:
                country_code = [e['mainsnak']['datavalue']['value'] for e in result_country['entities'][country_id]['claims']['P297']][0]
            except KeyError: country_code = None
            break
    except ValueError:
        country_label = None
        country_code = None
    try:
        coordinates = [(e['mainsnak']['datavalue']['value']['latitude'], e['mainsnak']['datavalue']['value']['longitude']) for e in result['entities'][wikidata_id]['claims']['P625']][0]
    except KeyError: coordinates = None
    temp_dict = {element: {'wikidata_id': wikidata_id,
                           'label': label,
                           'label_lang': lang,
                           'geonames_id': geonames_id,
                           'country_id': country_id,
                           'country_label': country_label,
                           'country_lang': lang2,
                           'country_code': country_code,
                           'coordinates': coordinates}}
    wikidata_places_dict.update(temp_dict)

df2 = pd.DataFrame.from_dict(wikidata_places_dict, orient='index').reset_index().rename(columns={'index':'wikidata_url'}) 
df = pd.concat([df, df2])



#country classification
#Name=string, Code=string, Wikidata_ID=string
country_classification = df[['country_label', 'country_code', 'country_id']].drop_duplicates().rename(columns={'country_label': 'Name', 'country_code': 'Code', 'country_id': 'Wikidata_ID'})


#places file
#objects:
#Name=string, Wikidata_ID=string, Geonames_ID=String, Country_Code=classifiaction
#sub-objets: 
#Located: Location=Point
places_file = df[['label', 'wikidata_id', 'geonames_id', 'country_code', 'coordinates']].rename(columns={'label': 'Name', 'wikidata_id': 'Wikidata_ID', 'geonames_id': 'Geonames_ID', 'country_code': 'Country_Code', 'coordinates': 'Located'})

country_classification.to_excel('samizdat_country_classification.xlsx', index=False)
places_file.to_excel('samizdat_places_file.xlsx', index=False)

#places file
#dodać kolumnę "zmiany"
#przesłać na dysk nowy plik json z osobami i miejscami

#%% ostateczne scalenie plików i przygotowanie plików wsadowych

country_classification = gsheet_to_df('1VT9vt0sIAcXZfL6BL0EBXj6Up54Q5JJtD_QY6TOb_X8', 'Sheet1')
country_classification['Wikidata_ID'] = 'https://www.wikidata.org/wiki/' + country_classification['Wikidata_ID']
country_classification.to_csv('samizdat_country_classification_to_nodegoat.csv', index=False, encoding='utf-8')


places_file = gsheet_to_df('11MDsd1T9onk3tPz84Vb_8AxyHJxFLkuHozNfEer1Wto', 'Sheet1')
places_file['Wikidata_ID'] = 'https://www.wikidata.org/wiki/' + places_file['Wikidata_ID']
places_file['Geonames_ID'] = places_file['Geonames_ID'].apply(lambda x: ast.literal_eval(x))
places_file['Geonames_ID'] = places_file['Geonames_ID'].apply(lambda x: x[0] if isinstance(x,list) else str(x))
places_file['Geonames_ID'] = 'https://www.geonames.org/' + places_file['Geonames_ID']
places_file['Located'] = places_file['Located'].apply(lambda x: ast.literal_eval(x))
places_file['Latitude'] = places_file['Located'].apply(lambda x: x[0])
places_file['Longitude'] = places_file['Located'].apply(lambda x: x[1])
places_file.drop(columns=['Old', 'Located']).to_csv('samizdat_places_to_nodegoat.csv', index=False, encoding='utf-8')












































