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
import copy
import json

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
        value = re.sub(regex, r'\3', string).strip()
        dictionary_field[subfield] = value
    return dictionary_field

test = copy.deepcopy(nodegoat_people_df)

for i, row in tqdm(test.iloc[989:,:].iterrows(), total=test.iloc[989:,:].shape[0]):
    # i = 59
    # row = test.iloc[i,:]
    locations = row['name_form_id'].split('|')
    try:
        viaf = row['viaf'][1]
        list_for_titles = []
        for location in locations:
            #location = locations[0]
            location = location.split('-')
            if location[0] == 'bn_books':
                title = tytuly_bn_df[tytuly_bn_df['id'] == int(location[1])][200].reset_index(drop=True)[0]
                if '%e' in title:
                    title = marc_parser_dict_for_field(title, '\%')['%a'] + ' ' + marc_parser_dict_for_field(title, '\%')['%e']
                else:
                    title = marc_parser_dict_for_field(title, '\%')['%a']
                url = f"http://www.viaf.org//viaf/search?query=cql.any+=+{title}&maximumRecords=1000&httpAccept=application/json"
                response = requests.get(url)
                response.encoding = 'UTF-8'
                try:
                    samizdat_json = response.json()  
                    samizdat_json = samizdat_json['searchRetrieveResponse']['records']
                    samizdat_json = [[e['record']['recordData']['mainHeadings']['data'], e['record']['recordData']['viafID'], e['record']['recordData']['titles']['work'], len(e['record']['recordData']['sources']['source'])] for e in samizdat_json if e['record']['recordData']['nameType'] == 'Personal']
                    try:
                        samizdat_json_selected = [e for e in samizdat_json if e[1] == viaf][0]
                        if type(samizdat_json_selected[0]) == list:
                            nazwa = [e['text'] for e in samizdat_json_selected[0]]
                        elif type(samizdat_json_selected[0]) == dict:
                            nazwa = samizdat_json_selected[0]['text']
                        
                        if type(samizdat_json_selected[2]) == list:
                            tytuly = [e['title'] for e in samizdat_json_selected[2]]
                        elif type(samizdat_json_selected[2]) == dict:
                            tytuly = samizdat_json_selected[2]['title']
                            
                        samizdat_dict = str({'viaf':viaf,'nazwa':nazwa, 'tytuły':tytuly})
                        list_for_titles.append(samizdat_dict)
                        #test.at[i, 'odpytanie po tytułach'] = samizdat_dict
                    except IndexError:
                        test.at[i, 'odpytanie po tytułach'] = 'wiersz do sprawdzenia'
                        pass
                except (KeyError, ValueError):
                    pass
            else:
                test.at[i, 'odpytanie po tytułach'] = 'wiersz do sprawdzenia'
            list_for_titles = list(set(list_for_titles))
            if list_for_titles:
                test.at[i, 'odpytanie po tytułach'] = list_for_titles
            else:
                test.at[i, 'odpytanie po tytułach'] = 'wiersz do usunięcia'
    except TypeError:
        list_for_titles = []
        for location in locations:
            #location = locations[0]
            location = location.split('-')
            if location[0] == 'bn_books':
                title = tytuly_bn_df[tytuly_bn_df['id'] == int(location[1])][200].reset_index(drop=True)[0]
                if '%e' in title:
                    title = marc_parser_dict_for_field(title, '\%')['%a'] + ' ' + marc_parser_dict_for_field(title, '\%')['%e']
                else:
                    title = marc_parser_dict_for_field(title, '\%')['%a']
                url = f"http://www.viaf.org//viaf/search?query=cql.any+=+{title}&maximumRecords=1000&httpAccept=application/json"
                response = requests.get(url)
                response.encoding = 'UTF-8'
                try:
                    samizdat_json = response.json()  
                    samizdat_json = samizdat_json['searchRetrieveResponse']['records']
                    samizdat_json = [[e['record']['recordData']['mainHeadings']['data'], e['record']['recordData']['viafID'], e['record']['recordData']['titles']['work'], len(e['record']['recordData']['sources']['source'])] for e in samizdat_json if e['record']['recordData']['nameType'] == 'Personal']
                    if len(samizdat_json) == 1:
                        samizdat_json_selected = samizdat_json[0]
                        if type(samizdat_json_selected[0]) == list:
                            nazwa = [e['text'] for e in samizdat_json_selected[0]]
                        elif type(samizdat_json_selected[0]) == dict:
                            nazwa = samizdat_json_selected[0]['text']
                        
                        if type(samizdat_json_selected[2]) == list:
                            tytuly = [e['title'] for e in samizdat_json_selected[2]]
                        elif type(samizdat_json_selected[2]) == dict:
                            tytuly = samizdat_json_selected[2]['title']
                            
                        samizdat_dict = str({'viaf':viaf,'nazwa':nazwa, 'tytuły':tytuly})
                        list_for_titles.append(samizdat_dict)
                    else:
                        test.at[i, 'odpytanie po tytułach'] = 'wiersz do sprawdzenia'
                        pass
                except (KeyError, ValueError, TypeError):
                    pass
            else:
                test.at[i, 'odpytanie po tytułach'] = 'wiersz do sprawdzenia'
            list_for_titles = list(set(list_for_titles))
            if list_for_titles:
                test.at[i, 'odpytanie po tytułach'] = list_for_titles
            else:
                test.at[i, 'odpytanie po tytułach'] = 'wiersz do usunięcia'

try:
    set_with_dataframe(nodegoat_people_sheet_new.worksheet('po tytułach'), test)
except gs.WorksheetNotFound:
    nodegoat_people_sheet_new.add_worksheet(title="po tytułach", rows="100", cols="20")
    set_with_dataframe(nodegoat_people_sheet_new.worksheet('po tytułach'), test)
    
worksheet = nodegoat_people_sheet_new.worksheet('po tytułach')

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
 #%%               
                
            
   























