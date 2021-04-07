import pandas as pd
from my_functions import marc_parser_1_field, unique_elem_from_column_split, cSplit, replacenth, gsheet_to_df, df_to_gsheet, get_cosine_result
import regex as re
from functools import reduce
import numpy as np
import copy
import requests
from bs4 import BeautifulSoup
from SPARQLWrapper import SPARQLWrapper, JSON
from urllib.error import HTTPError
from http.client import RemoteDisconnected
import time
import xml.etree.ElementTree as et
from google_drive_research_folders import cr_projects
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
import gspread as gs
from tqdm import tqdm
import datetime
from gspread_dataframe import set_with_dataframe, get_as_dataframe

#%% date
now = datetime.datetime.now()
year = now.year
month = '{:02}'.format(now.month)
day = '{:02}'.format(now.day)

#%% google authentication & google drive
#autoryzacja do tworzenia i edycji plików
gc = gs.oauth()
#autoryzacja do penetrowania dysku
gauth = GoogleAuth()
gauth.LocalWebserverAuth()
drive = GoogleDrive(gauth)

#%% google drive files
file_list = drive.ListFile({'q': f"'{cr_projects}' in parents and trashed=false"}).GetList() 
file_list = drive.ListFile({'q': "'1UdglvjjX4r2Hzh5BIAr8FjPuWV89NVs6' in parents and trashed=false"}).GetList()
#[print(e['title'], e['id']) for e in file_list]
nodegoat_people = [file['id'] for file in file_list if file['title'] == 'nodegoat_people_2020_12_10'][0]
nodegoat_people_sheet = gc.open_by_key(nodegoat_people)
nodegoat_people_sheet.worksheets()

nodegoat_people_df = get_as_dataframe(nodegoat_people_sheet.worksheet('Arkusz1'), evaluate_formulas=True).dropna(how='all').dropna(how='all', axis=1).drop_duplicates()[['Project_ID', 'Index_Name', 'Other_Name_Form']]
nodegoat_people_df['Project_ID'] = nodegoat_people_df['Project_ID'].astype(int)
samizdat_people_other_names = cSplit(nodegoat_people_df[['Project_ID', 'Other_Name_Form']], 'Project_ID', 'Other_Name_Form', '; ')
samizdat_people_other_names = samizdat_people_other_names[samizdat_people_other_names['Other_Name_Form'].notnull()].rename(columns={'Other_Name_Form': 'Index_Name'})
nodegoat_people_df = pd.concat([nodegoat_people_df[['Project_ID', 'Index_Name']], samizdat_people_other_names]).sort_values('Project_ID').drop_duplicates().reset_index(drop=True)
nodegoat_people_tuples = list(nodegoat_people_df.to_records(index=False))

nodegoat_people_dict = {}
for index, name in tqdm(nodegoat_people_tuples):
    index = int(index)
    url = f"http://www.viaf.org/viaf/AutoSuggest?query={name}"
    response = requests.get(url)
    response.encoding = 'UTF-8'
    try:
        samizdat_json = response.json()
        samizdat_json = [e for e in samizdat_json['result'] if e['nametype'] == 'personal']
        if index in nodegoat_people_dict:
            nodegoat_people_dict[index]['samizdat_name'].append(name)
            nodegoat_people_dict[index]['viaf'] += [[e['displayForm'], e['viafid'], e['score']] for e in samizdat_json]
        else:
            nodegoat_people_dict[index] = {'samizdatID':index, 'samizdat_name':[name]}
            nodegoat_people_dict[index]['viaf'] = [[e['displayForm'], e['viafid'], e['score']] for e in samizdat_json]
    except (ValueError, TypeError):
        if index in nodegoat_people_dict:
            nodegoat_people_dict[index]['samizdat_name'].append(name)
        else:
            nodegoat_people_dict[index] = {'samizdatID':index, 'samizdat_name':[name]}
            nodegoat_people_dict[index]['viaf'] = []

for k, v in nodegoat_people_dict.items():
    nodegoat_people_dict[k]['viaf'] = [list(x) for x in set(tuple(x) for x in nodegoat_people_dict[k]['viaf'])]

nodegoat_people_df = [value for value in nodegoat_people_dict.values()]    
nodegoat_people_df = pd.json_normalize(nodegoat_people_df)
nodegoat_people_df = nodegoat_people_df.explode('viaf').reset_index(drop=True)
nodegoat_people_df['viaf id'] = nodegoat_people_df['viaf'].apply(lambda x: x[1] if type(x) != float else np.nan)
nodegoat_people_df['viaf score'] = nodegoat_people_df['viaf'].apply(lambda x: int(x[-1]) if type(x) != float else np.nan)
nodegoat_people_df = nodegoat_people_df.sort_values(['samizdatID', 'viaf score'], ascending=[True, False]).groupby(['samizdatID', 'viaf id'], dropna=False).head(1)

sparql = SPARQLWrapper("https://query.wikidata.org/sparql")    
        
for i, row in tqdm(nodegoat_people_df.iterrows(), total=nodegoat_people_df.shape[0]):
    try:
        viaf = row['viaf'][1]
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
        sparql.setQuery(sparql_query)
        sparql.setReturnFormat(JSON)
        results = sparql.query().convert()
        results_df = pd.json_normalize(results['results']['bindings'])
        columns = [e for e in results_df.columns.tolist() if 'value' in e]
        results_df = results_df[results_df.columns.intersection(columns)]       
        for column in results_df.drop(columns='autor.value'):
            results_df[column] = results_df.groupby('autor.value')[column].transform(lambda x: '❦'.join(x.drop_duplicates().astype(str)))
        results_df = results_df.drop_duplicates().reset_index(drop=True)   
        result = results_df.to_dict('records')
        nodegoat_people_df.at[i, 'wikidata'] = result
    except (KeyError, TypeError):
        nodegoat_people_df.at[i, 'wikidata'] = np.nan
    except (HTTPError, RemoteDisconnected) as error:
        print(error)
        time.sleep(61)
        continue
    
    
nodegoat_viaf_wikidata_df = nodegoat_people_df.groupby('samizdatID').filter(lambda x: len(x) == 1)
nodegoat_viaf_wikidata_df = nodegoat_viaf_wikidata_df[nodegoat_viaf_wikidata_df['wikidata'].notnull()].drop(columns=['viaf id', 'viaf score']) 

nodegoat_people_sheet_new = gc.create(f"nodegoat_people_{year}-{month}-{day}", '1UdglvjjX4r2Hzh5BIAr8FjPuWV89NVs6')
worksheet = nodegoat_people_sheet_new.get_worksheet(0)
worksheet.update_title('wikidata ok')

set_with_dataframe(worksheet, nodegoat_viaf_wikidata_df)
nodegoat_people_sheet_new.batch_update({
    "requests": [
        {
            "updateDimensionProperties": {
                "range": {
                    "sheetId": worksheet._properties['sheetId'],
                    "dimension": "ROWS",
                    "startIndex": 0,
                    #"endIndex": 100
                },
                "properties": {
                    "pixelSize": 20
                },
                "fields": "pixelSize"
            }
        }
    ]
})

worksheet.freeze(rows=1)
worksheet.set_basic_filter()

nodegoat_people_df = nodegoat_people_df[~nodegoat_people_df['samizdatID'].isin(nodegoat_viaf_wikidata_df['samizdatID'])].drop(columns=['viaf id', 'viaf score'])
nodegoat_people_df = nodegoat_people_df[['samizdatID', 'samizdat_name', 'viaf', 'wikidata']]
#%% szukanie po tytułach książek w danych z BN
tytuly_bn = [file['id'] for file in file_list if file['title'] == 'samizdat_kartoteka_osób'][0]
tytuly_bn_sheet = gc.open_by_key(tytuly_bn)
tytuly_bn_sheet.worksheets()

tytuly_bn_df = get_as_dataframe(tytuly_bn_sheet.worksheet('bn_books'), evaluate_formulas=True).dropna(how='all').dropna(how='all', axis=1)

nodegoat_people_df_location = get_as_dataframe(nodegoat_people_sheet.worksheet('Arkusz1'), evaluate_formulas=True).dropna(how='all').dropna(how='all', axis=1).drop_duplicates()[['Project_ID', 'name_form_id']]

nodegoat_people_df = nodegoat_people_df.merge(nodegoat_people_df_location, left_on='samizdatID', right_on='Project_ID', how='left').drop(columns='Project_ID')

def marc_parser_dict_for_field(string, subfield_code):
    subfield_list = re.findall(f'{subfield_code}.', string)
    dictionary_field = {}
    for subfield in subfield_list:
        subfield_escape = re.escape(subfield)
        string = re.sub(f'({subfield_escape})', r'❦\1', string)
    for subfield in subfield_list:
        subfield_escape = re.escape(subfield)
        regex = f'(^)(.*?\❦{subfield_escape}|)(.*?)(\,{{0,1}})((\❦{subfield_code})(.*)|$)'
        value = re.sub(regex, r'\3', string)
        dictionary_field[subfield] = value
    return dictionary_field

test = nodegoat_people_df.copy()

for i, row in test.iterrows():
    row = test.iloc[0,:]
    locations = row['name_form_id'].split('|')
    for location in locations:
        location = locations[0]
        location = location.split('-')
        if location[0] == 'bn_books':
            title = tytuly_bn_df[tytuly_bn_df['id'] == int(location[1])][200].reset_index(drop=True)[0]
        title = marc_parser_dict_for_field(title, '\%')['%a']
        url = f"http://www.viaf.org//viaf/search?query=cql.any+=+{title}&maximumRecords=5&httpAccept=application/json"
        response = requests.get(url)
        response.encoding = 'UTF-8'
        samizdat_json = response.json()  
        samizdat_json = samizdat_json['searchRetrieveResponse']['records']
        samizdat_json = [[e['record']['recordData']['mainHeadings']['data'], e['record']['recordData']['viafID'], e['record']['recordData']['titles']['work'], len(e['record']['recordData']['sources']['source'])] for e in samizdat_json if e['record']['recordData']['nameType'] == 'Personal']
        #samizdat_json = max(samizdat_json, key=lambda x: x[-1])
        samizdat_list_of_dicts = []
        for record in samizdat_json:
            record = samizdat_json[0]
            if type(record[0]) == list:
                nazwa = [e['text'] for e in record[0]]
            elif type(record[0]) == dict:
                nazwa = record[0]['text']
            
            if type(record[2]) == list:
                tytuly = [e['title'] for e in record[2]]
            elif type(record[2]) == dict:
                tytuly = record[2]['title']
        
#TUTAJ!!! Jak zdecydować, który rekord przypisać do wiersza w df? czy używać viaf id? a co, gdy go nie ma? dwie strategie? czy powyżej harvestować wszystkie id, czy od razu selekcja właściwego - chyba tak
        
            tytuly = [e['title'] for e in record[2]]
            samizdat_dict = {'viaf':record[1],'nazwa':nazwa, 'tytuły':tytuly}
            samizdat_list_of_dicts.append(samizdat_dict)
            
        nazwa = [e['text'] for e in samizdat_json[0]]
        tytuly = [e['title'] for e in samizdat_json[2]]
        samizdat_dict = {'viaf':samizdat_json[1],'nazwa':nazwa, 'tytuły':tytuly}


        


new_price = {item: value for (item, value) in enumerate(samizdat_json['searchRetrieveResponse']['records'].items())} if samizdat_json['searchRetrieveResponse']['records'][item]['record']['recordData']['nameType'] == 'Personal'}


samizdat_json = [e for e in samizdat_json['result'] if e['nametype'] == 'personal']


yyy = samizdat_json['searchRetrieveResponse']['records']






import copy     
test = copy.deepcopy(nodegoat_people_dict)

        
viaf_used = []        
for k, v in tqdm(test.items(), total=len(test)):
    for name, viaf, score in test[k]['viaf']:
        if viaf not in viaf_used:
            sparql.setQuery(sparql_query)
            sparql.setReturnFormat(JSON)
            results = sparql.query().convert()
            results_df = pd.json_normalize(results['results']['bindings'])
            columns = [e for e in results_df.columns.tolist() if 'value' in e]
            results_df = results_df[results_df.columns.intersection(columns)]       
            for column in results_df.drop(columns='autor.value'):
                results_df[column] = results_df.groupby('autor.value')[column].transform(lambda x: '❦'.join(x.drop_duplicates().astype(str)))
            results_df = results_df.drop_duplicates().reset_index(drop=True)   
            result = results_df.to_dict('records')
            for el in result:
                if 'wikidata' in test[k]:
                    test[k]['wikidata'].append(el)
                else:
                    test[k]['wikidata'] = [el]
    

    
    
    
# wikidata enrichment

samizdat_viaf = gsheet_to_df('1HE-bKfnISmtFqSGci7OvG2wzcpZE9A5SYoYBnPPHQJ0', 'match with viaf')
sparql = SPARQLWrapper("https://query.wikidata.org/sparql")


samizdat_wikidata = pd.DataFrame()
for i, row in samizdat_viaf.iterrows():
    print(f"{i+1}/{len(samizdat_viaf)}")
    try:
        viaf = re.findall('\d+', row['viaf'])[0]
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
        sparql.setQuery(sparql_query)
        sparql.setReturnFormat(JSON)
        results = sparql.query().convert()
        
        
    
        results_df = pd.io.json.json_normalize(results['results']['bindings'])
        results_df['viaf'] = viaf
    
        for column in results_df.drop(columns='viaf'):
            results_df[column] = results_df.groupby('viaf')[column].transform(lambda x: '❦'.join(x.drop_duplicates().astype(str)))
        results_df = results_df.drop_duplicates().reset_index(drop=True)   
        columns = [e for e in results_df.columns.tolist() if 'value' in e]
        results_df = results_df[results_df.columns.intersection(columns)]
        result = results_df.to_dict('records')
        
        
        samizdat_wikidata = samizdat_wikidata.append(results_df)
    except (HTTPError, RemoteDisconnected):
        time.sleep(61)
        continue
        

samizdat_wikidata = samizdat_wikidata[['viaf', 'autor.value', 'birthdate.value', 'deathdate.value', 'birthplaceLabel.value', 'deathplaceLabel.value', 'sexLabel.value', 'pseudonym.value', 'occupationLabel.value']]
samizdat_viaf['viaf'] = samizdat_viaf['viaf'].apply(lambda x: re.findall('\d+', x)[0])
samizdat_wikidata = pd.merge(samizdat_viaf, samizdat_wikidata, how='left', on='viaf').drop_duplicates().reset_index(drop=True)

df_to_gsheet(samizdat_wikidata, '1HE-bKfnISmtFqSGci7OvG2wzcpZE9A5SYoYBnPPHQJ0', 'wikidata enrichment')
# viaf





#stary kod



# viaf
samizdat_viaf = pd.DataFrame()
for index, row in samizdat_people.iterrows():

    search_name = row['Index_Name']
    
    print(str(index+1) + '/' + str(len(samizdat_people)))
    connection_no = 1
    while True:
        try:
            people_links = []
            while len(people_links) == 0 and len(search_name) > 0:
                url = re.sub('\s+', '%20', f"http://viaf.org/viaf/search?query=local.personalNames%20all%20%22{search_name}%22&sortKeys=holdingscount&recordSchema=BriefVIAF")
                response = requests.get(url)
                response.encoding = 'UTF-8'
                soup = BeautifulSoup(response.text, 'html.parser')
                people_links = soup.findAll('a', attrs={'href': re.compile("viaf/\d+")})
                if len(people_links) == 0:
                    search_name = ' '.join(search_name.split(' ')[:-1])
                
            if len(people_links) > 0:
                viaf_people = []
                for people in people_links:
                    person_name = re.split('â\x80\x8e|\u200e ', re.sub('\s+', ' ', people.text).strip())
                    person_link = re.sub(r'(.+?)(\#.+$)', r'http://viaf.org\1viaf.xml', people['href'].strip())
                    person_link = [person_link] * len(person_name)
# =============================================================================
#                     libraries = str(people).split('<br/>')
#                     libraries = [re.sub('(.+)(\<span.*+$)', r'\2', s.replace('\n', ' ')) for s in libraries if 'span' in s]
#                     single_record = list(zip(person_name, person_link, libraries))
#                     viaf_people += single_record
#                 viaf_people = pd.DataFrame(viaf_people, columns=['viaf name', 'viaf', 'libraries'])
# =============================================================================
                    single_record = list(zip(person_name, person_link))
                    viaf_people += single_record
                viaf_people = pd.DataFrame(viaf_people, columns=['viaf name', 'viaf'])
                viaf_people['Project_ID'] = row['Project_ID']
                viaf_people['Index_Name'] = row['Index_Name']
                viaf_people['search name'] = search_name
                for ind, vname in viaf_people.iterrows():
                    viaf_people.at[ind, 'cosine'] = get_cosine_result(vname['viaf name'], vname['search name'])
        
                if viaf_people['cosine'].max() >= 0.5:
                    viaf_people = viaf_people[viaf_people['cosine'] >= 0.5]
                else:
                    viaf_people = viaf_people[viaf_people['cosine'] == viaf_people['cosine'].max()]

# czy informacja o nazwie z polskiej biblioteki jest istotna?                
# =============================================================================
#                 viaf_people['polish library'] = viaf_people['libraries'].apply(lambda x: True if re.findall('Biblioteka Narodowa \(Polska\)|NUKAT \(Polska\)|National Library of Poland|NUKAT Center of Warsaw University Library', x) else False)
#                 viaf_people = viaf_people.drop(columns=['libraries', 'search name', 'cosine'])
#                 viaf_people['viaf name'] = viaf_people.groupby(['viaf', 'Project_ID'])['viaf name'].transform(lambda x: '❦'.join(x.drop_duplicates().astype(str)))
# =============================================================================
                    
                viaf_people = viaf_people.drop(columns='search name').drop_duplicates().reset_index(drop=True) 
            
                samizdat_viaf = samizdat_viaf.append(viaf_people)
            else:
                viaf_people = pd.DataFrame({'viaf name': ['brak'], 'viaf': ['brak'], 'Project_ID': [row['Project_ID']], 'Index_Name': [row['Index_Name']]})
                samizdat_viaf = samizdat_viaf.append(viaf_people)
        except (IndexError, KeyError):
            pass
        except requests.exceptions.ConnectionError:
            print(connection_no)
            connection_no += 1
            time.sleep(300)
            continue
        break

samizdat_viaf = samizdat_viaf[samizdat_viaf['viaf name'] != 'brak']
for column in samizdat_viaf.drop(columns=['viaf', 'Project_ID']):
    samizdat_viaf[column] = samizdat_viaf.groupby(['viaf', 'Project_ID'])[column].transform(lambda x: '❦'.join(x.drop_duplicates().astype(str)))
samizdat_viaf = samizdat_viaf.drop_duplicates().reset_index(drop=True) 

df_to_gsheet(samizdat_viaf, '1HE-bKfnISmtFqSGci7OvG2wzcpZE9A5SYoYBnPPHQJ0', 'match with viaf')

# wikidata enrichment

samizdat_viaf = gsheet_to_df('1HE-bKfnISmtFqSGci7OvG2wzcpZE9A5SYoYBnPPHQJ0', 'match with viaf')
sparql = SPARQLWrapper("https://query.wikidata.org/sparql")

samizdat_wikidata = pd.DataFrame()
for i, row in samizdat_viaf.iterrows():
    print(f"{i+1}/{len(samizdat_viaf)}")
    try:
        viaf = re.findall('\d+', row['viaf'])[0]
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
        sparql.setQuery(sparql_query)
        sparql.setReturnFormat(JSON)
        results = sparql.query().convert()
    
        results_df = pd.io.json.json_normalize(results['results']['bindings'])
        results_df['viaf'] = viaf
    
        for column in results_df.drop(columns='viaf'):
            results_df[column] = results_df.groupby('viaf')[column].transform(lambda x: '❦'.join(x.drop_duplicates().astype(str)))
        results_df = results_df.drop_duplicates().reset_index(drop=True)   
        
        samizdat_wikidata = samizdat_wikidata.append(results_df)
    except (HTTPError, RemoteDisconnected):
        time.sleep(61)
        continue
        

samizdat_wikidata = samizdat_wikidata[['viaf', 'autor.value', 'birthdate.value', 'deathdate.value', 'birthplaceLabel.value', 'deathplaceLabel.value', 'sexLabel.value', 'pseudonym.value', 'occupationLabel.value']]
samizdat_viaf['viaf'] = samizdat_viaf['viaf'].apply(lambda x: re.findall('\d+', x)[0])
samizdat_wikidata = pd.merge(samizdat_viaf, samizdat_wikidata, how='left', on='viaf').drop_duplicates().reset_index(drop=True)

df_to_gsheet(samizdat_wikidata, '1HE-bKfnISmtFqSGci7OvG2wzcpZE9A5SYoYBnPPHQJ0', 'wikidata enrichment')

# viaf enrichment

samizdat_viaf = gsheet_to_df('1HE-bKfnISmtFqSGci7OvG2wzcpZE9A5SYoYBnPPHQJ0', 'match with viaf')
ns = '{http://viaf.org/viaf/terms#}'

samizdat_viaf_enrichment = pd.DataFrame()
for i, row in samizdat_viaf.iterrows():
    try:
        print(f"{i+1}/{len(samizdat_viaf)}")
        connection_no = 1
        viaf = row['viaf']
        
        response = requests.get(viaf)
        with open('viaf.xml', 'wb') as file:
            file.write(response.content)
        tree = et.parse('viaf.xml')
        root = tree.getroot()
        birth_date = root.findall(f'.//{ns}birthDate')
        birth_date = '❦'.join([t.text for t in birth_date])
        death_date = root.findall(f'.//{ns}deathDate')
        death_date = '❦'.join([t.text for t in death_date])
        occupation = root.findall(f'.//{ns}occupation/{ns}data/{ns}text')
        occupation = '❦'.join([t.text for t in occupation])
        gender = root.findall(f'.//{ns}gender')
        gender = '❦'.join([t.text for t in gender])
        viaf_record = [viaf, birth_date, death_date, occupation, gender]
        viaf_record = pd.DataFrame([viaf_record], columns=['viaf', 'birth_date', 'death_date', 'occupation', 'gender'])
        samizdat_viaf_enrichment = samizdat_viaf_enrichment.append(viaf_record)
    except requests.exceptions.ConnectionError:
        print(connection_no)
        connection_no += 1
        time.sleep(61)
        continue
        break

samizdat_viaf_enrichment = pd.merge(samizdat_viaf, samizdat_viaf_enrichment, how='left', on='viaf')

df_to_gsheet(samizdat_viaf_enrichment, '1HE-bKfnISmtFqSGci7OvG2wzcpZE9A5SYoYBnPPHQJ0', 'viaf enrichment')

# 12.01.2021

samizdat_people = gsheet_to_df('1HE-bKfnISmtFqSGci7OvG2wzcpZE9A5SYoYBnPPHQJ0', 'Arkusz1')
match_with_viaf = gsheet_to_df('1HE-bKfnISmtFqSGci7OvG2wzcpZE9A5SYoYBnPPHQJ0', 'match with viaf')
wikidata_enrichment = gsheet_to_df('1HE-bKfnISmtFqSGci7OvG2wzcpZE9A5SYoYBnPPHQJ0', 'wikidata enrichment')
viaf_enrichment = gsheet_to_df('1HE-bKfnISmtFqSGci7OvG2wzcpZE9A5SYoYBnPPHQJ0', 'viaf enrichment')
viaf_enrichment['Project_ID'] = viaf_enrichment['Project_ID'].astype(int)
viaf_enrichment['viaf'] = viaf_enrichment['viaf'].apply(lambda x: re.findall('\d+', x)[0])

wikidata_grouped = wikidata_enrichment.groupby('Project_ID')
wikidata_df = pd.DataFrame()
for i, (identyfikator, group) in enumerate(wikidata_grouped):
    print(f"{i+1}/{len(wikidata_grouped)}")
    if group['autor.value'].notna().sum() > 0:
        df = group[group['autor.value'].notnull()]
    else:
        df = group
    wikidata_df = wikidata_df.append(df)
wikidata_df['Project_ID'] = wikidata_df['Project_ID'].astype(int)

no_wikidata = wikidata_df[wikidata_df['autor.value'].isnull()]
wikidata_df = wikidata_df[wikidata_df['autor.value'].notnull()]

wikidata_grouped = wikidata_df.groupby('Project_ID')
wikidata_to_check = pd.DataFrame()
wikidata_ok = pd.DataFrame()
for id, group in wikidata_grouped:
    if len(group) == 1:
        wikidata_ok = wikidata_ok.append(group)
    else:
        wikidata_to_check = wikidata_to_check.append(group)

df_to_gsheet(wikidata_ok, '1STLQEAowxJOL_WpwWs-6gnexRCyf-9qFAqovVxO8Mcw', 'wikidata_ok')

wikidata_to_check.reset_index(drop=True, inplace=True)

ns = '{http://viaf.org/viaf/terms#}'
for i, row in wikidata_to_check.iterrows():
    try:
        print(f"{i+1}/{len(wikidata_to_check)}")
        connection_no = 1
        viaf = row['viaf']
        url = f"http://viaf.org/viaf/{viaf}/viaf.xml"
        response = requests.get(url)
        with open('viaf.xml', 'wb') as file:
            file.write(response.content)
        tree = et.parse('viaf.xml')
        root = tree.getroot()
        works = root.findall(f'.//{ns}title')
        works = '❦'.join([t.text for t in works])
        publishers = root.findall(f'.//{ns}publishers/{ns}data/{ns}text')
        publishers = '❦'.join([t.text for t in publishers])
        wikidata_to_check.at[i, 'works'] = works
        wikidata_to_check.at[i, 'publishers'] = publishers
    except requests.exceptions.ConnectionError:
        print(connection_no)
        connection_no += 1
        time.sleep(61)
        continue
        break

for column in wikidata_to_check:
    if wikidata_to_check[column].dtype == object:
        wikidata_to_check[column] = wikidata_to_check[column].str.slice(0,50000)

df_to_gsheet(wikidata_to_check, '1STLQEAowxJOL_WpwWs-6gnexRCyf-9qFAqovVxO8Mcw', 'wikidata_to_check')

no_wikidata = no_wikidata.iloc[:,1:5]
no_wikidata = pd.merge(no_wikidata, viaf_enrichment[['Project_ID', 'viaf', 'viaf name', 'birth_date', 'death_date', 'occupation', 'gender']], how='left', on=['Project_ID', 'viaf']).sort_values(['Project_ID', 'viaf']).reset_index(drop=True)

ns = '{http://viaf.org/viaf/terms#}'
for i, row in no_wikidata.iterrows():
    try:
        print(f"{i+1}/{len(no_wikidata)}")
        connection_no = 1
        viaf = row['viaf']
        url = f"http://viaf.org/viaf/{viaf}/viaf.xml"
        response = requests.get(url)
        with open('viaf.xml', 'wb') as file:
            file.write(response.content)
        tree = et.parse('viaf.xml')
        root = tree.getroot()
        works = root.findall(f'.//{ns}title')
        works = '❦'.join([t.text for t in works])
        publishers = root.findall(f'.//{ns}publishers/{ns}data/{ns}text')
        publishers = '❦'.join([t.text for t in publishers])
        no_wikidata.at[i, 'works'] = works
        no_wikidata.at[i, 'publishers'] = publishers
    except requests.exceptions.ConnectionError:
        print(connection_no)
        connection_no += 1
        time.sleep(61)
        continue
        break

df_to_gsheet(no_wikidata, '1STLQEAowxJOL_WpwWs-6gnexRCyf-9qFAqovVxO8Mcw', 'viaf_to_decide')











