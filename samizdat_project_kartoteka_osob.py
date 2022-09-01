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
from collections import Counter
from my_functions import marc_parser_dict_for_field
import numpy as np
from collections import defaultdict
from difflib import SequenceMatcher
from fuzzywuzzy import fuzz, process
import Levenshtein
from collections import Counter

#%% def
def query_wikidata_person_with_viaf(viaf):
    # viaf_id = 49338782
    viaf_id = re.findall('\d+', viaf)[0]
    user_agent = "WDQS-example Python/%s.%s" % (sys.version_info[0], sys.version_info[1])
    sparql = SPARQLWrapper("https://query.wikidata.org/sparql", agent=user_agent)
    sparql.setQuery(f"""PREFIX wdt: <http://www.wikidata.org/prop/direct/>
                SELECT distinct ?author WHERE {{ 
                  ?author wdt:P214 "{viaf_id}" ;
                SERVICE wikibase:label {{ bd:serviceParam wikibase:language "pl". }}}}""")
    sparql.setReturnFormat(JSON)
    while True:
        try:
            results = sparql.query().convert()
            break
        except HTTPError:
            time.sleep(2)
        except URLError:
            time.sleep(5)
    results = wikidata_simple_dict_resp(results)  
    viafy_wiki[viaf] = results
    return results

def get_viaf_with_wikidata(wikidata_url):
    try:
        wikidata_id = re.findall('Q.+', wikidata_url)[0]
        result = requests.get(f'https://www.wikidata.org/wiki/Special:EntityData/{wikidata_id}.json').json()
        result = f"http://viaf.org/viaf/{result['entities'][f'{wikidata_id}']['claims']['P214'][0]['mainsnak']['datavalue']['value']}"
        wiki_viafy[wikidata_url] = result
    except KeyError:
        print(wikidata_url)
    
    
#%% uzupełnienia zapomnianych
# temp_df = gsheet_to_df('1xOAccj4SK-4olYfkubvittMM8R-jwjUjVhZXi9uGqdQ', 'zapomniani')
# viafy = temp_df['viaf_ID'].dropna().drop_duplicates().to_list()
# wiki = temp_df['wikidata_ID'].dropna().drop_duplicates().to_list()

# viafy_wiki = {}
# with ThreadPoolExecutor() as executor:
#     list(tqdm(executor.map(query_wikidata_person_with_viaf,viafy), total=len(viafy)))
# viafy_wiki = {k:v['author'][0]['value'] for k,v in viafy_wiki.items() if v}
    
# wiki_viafy = {}
# with ThreadPoolExecutor() as executor:
#     list(tqdm(executor.map(get_viaf_with_wikidata,wiki), total=len(wiki)))

#%% wczytanie zasobów bibliograficznych:
records_dict = {}
worksheets = ['bn_books', 'cz_books', 'cz_articles', 'pbl_articles', 'pbl_books']
for worksheet in tqdm(worksheets):
    temp_df = gsheet_to_df('1y0E4mD1t4ZBN9YNmAwM2e912Q39Bb493Z6y5CSAuflw', worksheet)
    if worksheet in ['bn_books', 'cz_books', 'cz_articles']:
        temp_df.columns = [f"X{'{:03}'.format(e)}" if isinstance(e, int) else e for e in temp_df.columns.values]
    records_dict.update({worksheet: temp_df})
    
#wczytanie kartoteki osób po pracach
# kartoteka_osob = pd.DataFrame()

# for worksheet in tqdm(['pojedyncze ok', 'grupy_ok', 'osoby_z_jednym_wierszem', 'reszta', 'zapomniani']):
#     temp_df = gsheet_to_df('1xOAccj4SK-4olYfkubvittMM8R-jwjUjVhZXi9uGqdQ', worksheet)
#     temp_df = temp_df[temp_df['decyzja'].isin(['tak', 'new'])]
#     kartoteka_osob = pd.concat([kartoteka_osob, temp_df])  
    
#%% klasyfikacja płci    
sex_classification = kartoteka_osob[['sexLabel.value']].dropna().drop_duplicates().rename(columns={'sexLabel.value': 'Typeofsex'})
wikidata_sex = {'mężczyzna': 'https://www.wikidata.org/wiki/Q8441',
                'kobieta': 'https://www.wikidata.org/wiki/Q467',
                'interpłciowość': 'https://www.wikidata.org/wiki/Q1097630'}
sex_classification['Wikidata_ID'] = sex_classification['Typeofsex'].apply(lambda x: wikidata_sex[x])
# sex_classification.to_csv('samizdat_sex_classification_to_nodegoat.csv', index=False, encoding='utf-8') 

#%% klasyfikacja zawodów
# samizdat_occupation = gsheet_to_df('1-oBjrUytvx4LGSkuRJEUYDkmNx3l7sjdA0wIGTFI4Lc', 'Sheet1')
# occupations = samizdat_occupation['occupation'].to_list()

# final = {}
# url = 'https://query.wikidata.org/sparql'
# for occupation in tqdm(occupations):
#     if not re.search('Q\d+', occupation):
#         while True:
#             try:
#                 sparql_query = f"""SELECT distinct ?item ?itemLabel ?itemDescription WHERE{{  
#                   ?item ?label "{occupation}"@pl.
#                     SERVICE wikibase:label {{ bd:serviceParam wikibase:language "pl". }}    
#                 }}"""
#                 results = requests.get(url, params = {'format': 'json', 'query': sparql_query})
#                 results = results.json()
#                 try:
#                     wd_id = re.findall('Q\d+', [e['item']['value'] for e in results['results']['bindings'] if e['itemLabel']['value'] == occupation][0])[0]
#                 except IndexError:
#                     wd_id = re.findall('Q\d+', [e['item']['value'] for e in results['results']['bindings'] if 'wikidata' in e['item']['value'] and 'Q' in e['item']['value']][0])[0]
#                 sparql_query = f"""select distinct ?item ?label (lang(?label) as ?lang) where {{
#                   wd:{wd_id} rdfs:label ?label
#                   filter(lang(?label) = 'pl' || lang(?label) = 'en')
#                 }}"""  
#                 results = requests.get(url, params = {'format': 'json', 'query': sparql_query})
#                 results = results.json()     
#                 temp = [[e['label']['xml:lang'], e['label']['value']] for e in results['results']['bindings']]
#                 for el in temp:
#                     if wd_id not in final:
#                         final[wd_id] = {el[0]:el[-1]}
#                     else:
#                         final[wd_id].update({el[0]:el[-1]})
#             except ValueError:
#                 time.sleep(4)
#                 continue
#             break
#     else:
#         url2 = f'https://www.wikidata.org/wiki/Special:EntityData/{occupation}.json'
#         result = requests.get(url2).json()
#         try:
#             final[occupation] = {k:v['value'] for k,v in result['entities'][occupation]['labels'].items() if k in ['en', 'pl']}
#         except KeyError:
#             final[list(result['entities'].keys())[0]] = {k:v['value'] for k,v in result['entities'][list(result['entities'].keys())[0]]['labels'].items() if k in ['en', 'pl']}
        
# test = pd.DataFrame.from_dict(final, orient='index')     
# test.to_excel('test.xlsx')  
        
# uzupelnienia = ['Q48282', 'Q846430', 'Q7019111', 'Q33999', 'Q33999', 'Q33999', 'Q111017237', 'Q5197818', 'Q1397808', 'Q25393460', 'Q152002', 'Q36180', 'Q17167049', 'Q49757', 'Q1238570', 'Q1650915', 'Q1397808', 'Q9379869']


# final2 = {}
# for occupation in tqdm(uzupelnienia):
#     url2 = f'https://www.wikidata.org/wiki/Special:EntityData/{occupation}.json'
#     result = requests.get(url2).json()
#     try:
#         final2[occupation] = {k:v['value'] for k,v in result['entities'][occupation]['labels'].items() if k in ['en', 'pl']}
#     except KeyError:
#         final2[list(result['entities'].keys())[0]] = {k:v['value'] for k,v in result['entities'][list(result['entities'].keys())[0]]['labels'].items() if k in ['en', 'pl']}

# test2 = pd.DataFrame.from_dict(final2, orient='index')     
# test2.to_excel('test2.xlsx')  
    
occupation_classification = gsheet_to_df('1-oBjrUytvx4LGSkuRJEUYDkmNx3l7sjdA0wIGTFI4Lc', 'occupation').drop(columns=['old_label_pl', 'old_id']).drop_duplicates()
occupation_classification['Wikidata_ID'] = 'https://www.wikidata.org/wiki/' + occupation_classification['Wikidata_ID']
occupation_classification.to_csv('samizdat_occupation_classification_to_nodegoat.csv', index=False, encoding='utf-8') 

#%% korekta kodowania nazw osób:
# with open('samizdat_bn_mak.txt', 'rt', encoding='utf-8') as f:
#     records = f.read().splitlines()

# list_of_items_unique = []
# for row in records:
#     if row.startswith('001'):
#         list_of_items_unique.append([row])
#     else:
#         if row:
#             list_of_items_unique[-1].append(row)
    
# people_names = [[re.findall('\%.+$', el)[0] for el in e if el.startswith(('100', '700', '701')) and '%' in el] for e in list_of_items_unique]
# people_names = list(set([e for sub in people_names for e in sub]))

# people_names_tilde = []
# for column in ['X100', 'X700', 'X701']:
#     temp_list = records_dict['bn_books'][column].dropna().drop_duplicates().to_list()
#     people_names_tilde.extend(temp_list)

# people_names_tilde = [e.split('|') for e in people_names_tilde]
# people_names_tilde = list(set([e for sub in people_names_tilde for e in sub if e]))
# people_names_tilde = [re.split('\d{3}\p{L}{2}', e) for e in people_names_tilde]
# people_names_tilde = list(set([e for sub in people_names_tilde for e in sub if e]))

# people_names_tilde = [e for e in people_names_tilde if '~' in e]

# effect_dict = {}
# for tilde in tqdm(people_names_tilde):
#     effect_dict[tilde] = process.extractOne(tilde, people_names)[0]
    
# effect_dict.update({'%1Kup~5cin~7skij %2Roman %kR.K.': "%1Kupčins'kij %2Roman %kR.K.",
#  "%1Masaryk %2Tom~'a~5s %2Garrigue": "%1Masaryk %2Tomáš %2Garrigue",
#  "%1Josemari~'a Escriv~'a de Balaguer %5bł. %wEscriv~'a de Balaguer      Josemari~'a": "%1Josemariá Escrivá de Balaguer %5bł. %wEscrivá de Balaguer Josemariá",
#  "%1T~5re~5s~5n~'ak %2Vlastimil": "%1Třešňák %2Vlastimil",
#  "%1Fejt~:o %2Fran~9cois": "%1Fejtö %2François",
#  "%1Gru~5sa %2Ji~5r~'i": "%1Gruša %2Jiří",
#  "%1Sad~3unait~'e %2Nijol~'e": "%1Sadūnaité %2Nijolé",
#  "%1Mlyn~'a~5r %2Zden~5ek %d1930-": "%1Mlynář %2Zdeněk %d1930-",
#  "%1~5Cern~'y %2V~0aclav": "%1Černý %2Vàclav",
#  "%1Harn~'i~5cek %2Martin": "%1Harníček %2Martin",
#  "%1Ko~5sel~0ivec %2~0Ivan %sKoszeliwec Iwan": "%1Košelìvec %2Ìvan %sKoszeliwec Iwan",
#  "%1~5Sime~5cka %2Milan": "%1Šimečka %2Milan",
#  "%1Gruntor~'ad %2Ji~5r~'i": "%1Gruntorád %2Jiří",
#  "%1Carr~0ere d' %1Encausse %2H~'el~0ene": "%1Carrère d' %1Encausse %2Hélène",
#  "%1Hv~'i~5zd~'ala %2Karel %vop %mHv~'i~5zd'ala Karel": "%1Hvíždála %2Karel %vop %mHvíždála Karel"})

# bn_books = records_dict['bn_books']
# for column in bn_books:
#     if column in ['X100', 'X700', 'X701']:
#         bn_books[column] = bn_books[column].apply(lambda x: effect_dict[x] if x in effect_dict else x)
        
# bn_books.to_excel('bn_books.xlsx', index=False)
#%% uzupełnienia
# with open('text1.txt', 'wt', encoding='utf8') as f:
#     for pseudo, correct in pseudonimy_match:
#         f.write(pseudo + '\t')
#         f.write(correct + '\n')
#         f.write(str(pseudonyms_dict[pseudo]) + '\t')

# correct_list = []    
# with open('text2.txt', 'wt', encoding='utf8') as f:
#     for pseudo, correct in pseudonimy_match:
#         f.write(pseudo + '\t')
#         f.write(correct + '\n')
#         f.write(str(people_dict[correct]) + '\n')
#         if pd.notnull(people_dict[correct]['wikidata_id']):
#             correct_list.append(people_dict[correct])

# test_df = kartoteka_osob[(kartoteka_osob['wikidata_ID'].notnull()) &
#                          (kartoteka_osob['birthdate.value'].isnull())]['wikidata_ID'].to_list()



# lista = []      
# for ind, el in tqdm(enumerate(test_df), total=len(test_df)):
#     # wikidata_id = re.findall('Q\d+', el['wikidata_id'])[0]
#     wikidata_id = re.findall('Q\d+', el)[0]
#     # wikidata_id = 'Q74718593'
#     url = f"https://www.wikidata.org/wiki/Special:EntityData/{wikidata_id}.json"
#     result = requests.get(url).json()
#     try:
#         birthplace_id = result['entities'][wikidata_id]['claims']['P19'][0]['mainsnak']['datavalue']['value']['id']
#         url_temp = f"https://www.wikidata.org/wiki/Special:EntityData/{birthplace_id}.json"
#         birthplace_result = requests.get(url_temp).json()
#         birthplace = birthplace_result['entities'][birthplace_id]['labels']['pl']['value']
#     except KeyError:
#         birthplace = np.nan
#         birthplace_id = np.nan
#     # correct_list[ind]['birthplace'] = birthplace
#     try:
#         deathplace_id = result['entities'][wikidata_id]['claims']['P20'][0]['mainsnak']['datavalue']['value']['id']
#         url_temp = f"https://www.wikidata.org/wiki/Special:EntityData/{deathplace_id}.json"
#         deathplace_result = requests.get(url_temp).json()
#         deathplace = deathplace_result['entities'][deathplace_id]['labels']['pl']['value']
#     except KeyError:
#         deathplace = np.nan
#         deathplace_id = np.nan
#     # correct_list[ind]['deathplace'] = deathplace
#     try:
#         birthdate = result['entities'][wikidata_id]['claims']['P569'][0]['mainsnak']['datavalue']['value']['time']
#     except KeyError:
#         birthdate = np.nan
#     # correct_list[ind]['birthdate'] = birthdate
#     try:
#         deathdate = result['entities'][wikidata_id]['claims']['P570'][0]['mainsnak']['datavalue']['value']['time']
#     except KeyError:
#         deathdate = np.nan
#     # correct_list[ind]['deathdate'] = deathdate
#     try:
#         sex = result['entities'][wikidata_id]['claims']['P21'][0]['mainsnak']['datavalue']['value']['id']
#         url_temp = f"https://www.wikidata.org/wiki/Special:EntityData/{sex}.json"
#         sex_result = requests.get(url_temp).json()
#         sex = sex_result['entities'][sex]['labels']['pl']['value']
#     except KeyError:
#         sex = np.nan
#     # correct_list[ind]['sex'] = sex   
#     try:
#         occupations = [e['mainsnak']['datavalue']['value']['id'] for e in result['entities'][wikidata_id]['claims']['P106']]
#         occupation = []
#         for o in occupations:
#             url_temp = f"https://www.wikidata.org/wiki/Special:EntityData/{o}.json"
#             o_result = requests.get(url_temp).json()
#             occ = o_result['entities'][o]['labels']['pl']['value']
#             occupation.append(occ)
#     except KeyError:
#         occupation = []
#     # correct_list[ind]['occupation'] = occupation  
#     try:
#         pseudonym = [e['mainsnak']['datavalue']['value'] for e in result['entities'][wikidata_id]['claims']['P742']]
#     except KeyError:
#         pseudonym = np.nan
#     temp_dict = {'wikidata_id': wikidata_id,
#                  'birthplace': birthplace,
#                  'birthplace_id': birthplace_id,
#                  'deathplace': deathplace,
#                  'deathplace_id': deathplace_id,
#                  'birthdate': birthdate,
#                  'deathdate': deathdate,
#                  'sex': sex,
#                  'occupation': occupation,
#                  'pseudonym': pseudonym}
#     lista.append(temp_dict)
#     # correct_list[ind]['pseudonym'] = pseudonym 

# df = pd.DataFrame(lista)   
# df['birthdate'] =  df['birthdate'].apply(lambda x: x[1:] if not(isinstance(x,float)) else x)
# df['deathdate'] =  df['deathdate'].apply(lambda x: x[1:] if not(isinstance(x,float)) else x)    
# df['occupation'] = df['occupation'].apply(lambda x: '❦'.join(x) if isinstance(x, list) and x else np.nan)
# df['pseudonym'] = df['pseudonym'].apply(lambda x: '❦'.join(x) if isinstance(x, list) and x else np.nan)#%% tworzenie kartoteki osób

#%%main
# Counter(kartoteka_osob['Project_ID']).most_common(10)
kartoteka_miejsc = gsheet_to_df('11MDsd1T9onk3tPz84Vb_8AxyHJxFLkuHozNfEer1Wto', 'final')
# kartoteka_osob = pd.DataFrame()

# for worksheet in tqdm(['pojedyncze ok', 'grupy_ok', 'osoby_z_jednym_wierszem', 'reszta', 'zapomniani']):
#     temp_df = gsheet_to_df('1xOAccj4SK-4olYfkubvittMM8R-jwjUjVhZXi9uGqdQ', worksheet)
#     temp_df = temp_df[temp_df['decyzja'].isin(['tak', 'new'])]
#     kartoteka_osob = pd.concat([kartoteka_osob, temp_df])  

# kartoteka_osob.to_excel('test.xlsx', index=False)
kartoteka_osob = gsheet_to_df('1xOAccj4SK-4olYfkubvittMM8R-jwjUjVhZXi9uGqdQ', 'final')
    
occupation_classification = gsheet_to_df('1-oBjrUytvx4LGSkuRJEUYDkmNx3l7sjdA0wIGTFI4Lc', 'occupation')

data_sources_classification = ['PL_BN_books', 'PL_BN_journals', 'PL_BezCenzury_books', 'PL_BezCenzury_articles', 'CZ_books', 'CZ_articles']

kolumny = list(kartoteka_osob.columns.values)
instruction_bn = {'Index Name': ['%1', '%2', '%3', '%4', '%5', '%6'],
                'Name': ['%1', '%4', '%6'],
                'Given Name': ['%2'],
                'Pseudonym, Kryptonym': ['%p', '%k'],
                'True Name': ['%w'],
                'Other Name Forms': ['%o', '%s', '%r', '%m'],
                'Addition to Name': ['%5'],
                'Numeration': ['%3'],
                'Numeration (arabic)': ['%n'],
                'Birth': ['%d'],
                'Death': ['%d']}

instruction_cz = {'Index Name': ['$$a'],
                  'Name': ['$$a'],
                  'Given Name': ['$$a'],
                  'Other Name Forms': ['$$n', '$$r', '$$c'],
                  'Addition to Name': ['$$c'],
                  'Numeration': ['$$b'],
                  'Birth': ['$$d'],
                  'Death': ['$$d']}

#Uwaga do PW – są rekordy, które mają czy pseudonim == 'tak', ale nie mają wypełnionego 'pseudonym of'
pseudonimy = kartoteka_osob[(kartoteka_osob['czy pseudonim'] == 'tak') &
                            (kartoteka_osob['pseudonym of'].notnull())]

# !!! dać im znacznik do nodegoat
pseudonimy_do_oznaczenia = kartoteka_osob[(kartoteka_osob['czy pseudonim'] == 'tak') &
                                          (kartoteka_osob['pseudonym of'].isnull())]

pseudonimy_id = pseudonimy['Project_ID'].to_list()
#pseudonimami zająć się na końcu jako uzupełnienia haseł

kartoteka_osob = kartoteka_osob[~kartoteka_osob['Project_ID'].isin(pseudonimy_id)]

# na podstawie zmiennej test będzie trzeba pozmieniać Project_ID pseudonimów, a przypisanie do właściwych haseł zmienić z identyfikatorów viaf na Project_ID – manualna robota

data_sources_dict = {'bn_books': 'PL_BN_books', 
                     'cz_books': 'CZ_books', 
                     'cz_articles': 'CZ_articles', 
                     'pbl_articles': 'PL_BezCenzury_articles', 
                     'pbl_books': 'PL_BezCenzury_books'}

#ogarnąć pseudonimy w tabeli

#Index Name – string – longest nazwa z tabeli + sztorc lub nazwa z tabeli
#Project ID – string
#VIAF ID – external
#Wikidata ID – external
#Sex Label – classification
#Occupation – classification
#Name – string
#Given Name – string
#Pseudonym, Kryptonym – string
#True Name – string
#Other Name Forms – string
#Addition to name – string
#Name Numeration – string
#Name Numeration – string
#Name Numeration (Arabic) – number
#DataSource – classification
#sztorc

#sub-objects - located - powiązać z kartoteką miejsc
groupby = kartoteka_osob.groupby('Project_ID')
groupby2 = pseudonimy.groupby('Project_ID')

people_groupbys = [groupby, groupby2]
people_list_of_dicts = []
for groupby in people_groupbys:
    people_dict = {}
    for name, group in tqdm(groupby, total=len(groupby)):
        # 198, 346, 936, 1947, 1883, 1045, 520
        # name = '501390'
        # group = groupby.get_group(name)
        project_id = name
        try:
            sex_label = group['sexLabel.value'].dropna().drop_duplicates().to_list()[0]
        except IndexError: sex_label = np.nan
        project_id = group['Project_ID'].dropna().drop_duplicates().to_list()[0]
        try:
            viaf_id = "http://viaf.org/viaf/" + re.findall('\d+', group['viaf_ID'].dropna().drop_duplicates().to_list()[0])[0]
        except IndexError: viaf_id = np.nan
        try:
            wikidata_id = "https://www.wikidata.org/wiki/" + re.findall('Q\d+', group['wikidata_ID'].dropna().drop_duplicates().to_list()[0])[0]
        except IndexError: wikidata_id = np.nan
    
        biblio_names = group['name_form_id'].to_list()
        try:
            biblio_names = [e.split('|') for e in biblio_names]
        
            data_sources = []
            person_names_list = []
            for biblio_name_set in biblio_names:
                # biblio_name_set = biblio_names[0]
                # biblio_name_set = biblio_names[-1]
                for biblio_name in biblio_name_set:
                    # biblio_name = biblio_name_set[0]
                    # biblio_name = biblio_name_set[1]
                    # biblio_name = 'bn_books-86777-bnb4116-X100-%1'
                    # biblio_name = 'cz_articles-2323920-cza518-X600-$$a'
                    # biblio_name = 'pbl_books-1397940-pblt6590-tworcy-NA'
                    source = biblio_name.split('-')[0]
                    data_sources.append(source)
                    if source == 'bn_books':
                        record_id = biblio_name.split('-')[1]
                        df = records_dict[source]
                        field = biblio_name.split('-')[-2]
                        temp = df[df['id'] == record_id][field].to_list()[0].split('|')
                        for pers in temp:
                            # pers = temp[0]
                            form_parsed = marc_parser_dict_for_field(pers, '%')
                            dd = defaultdict(list)
                            for d in form_parsed:
                                for key, value in d.items():
                                    dd[key].append(value.strip())
                            form_parsed = [{e:' '.join([el.strip() for el in dd[e]]) if e!='%p' else ';'.join([el.strip() for el in dd[e]])} for e in dd]
                            use_of_instruction = instruction_bn.copy()
                            for form_dict in form_parsed:
                                # form_dict = form_parsed[0]
                                for key, value in form_dict.items():
                                    good_keys = {k:value.strip() for k,v in instruction_bn.items() if key in v}
                                    use_of_instruction = {k:list(map(lambda x: x.replace(key, good_keys[k]), v)) if k in good_keys else v for k,v in use_of_instruction.items()}
                            use_of_instruction = {k:' '.join([e for e in v if '%' not in e]) for k,v in use_of_instruction.items() if any('%' not in s for s in v)}
                            if use_of_instruction:
                                person_names_list.append(use_of_instruction)
                    elif 'cz' in source:
                        record_id = biblio_name.split('-')[1]
                        df = records_dict[source]
                        field = biblio_name.split('-')[-2]
                        temp = df[df['id'] == record_id][field].to_list()[0].split('|')
                        
                        for pers in temp:
                            # pers = temp[0]
                            form_parsed = marc_parser_dict_for_field(pers, '\$\$')
                            dd = defaultdict(list)
                            for d in form_parsed:
                                for key, value in d.items():
                                    dd[key].append(value.strip())
                            form_parsed = [{e:' '.join([el.strip() for el in dd[e]]) if e!='%p' else ';'.join([el.strip() for el in dd[e]])} for e in dd]
                            use_of_instruction = instruction_cz.copy()
                            for form_dict in form_parsed:
                                # form_dict = form_parsed[0]
                                for key, value in form_dict.items():
                                    good_keys = {k:value.strip() for k,v in instruction_cz.items() if key in v}
                                    use_of_instruction = {k:list(map(lambda x: x.replace(key, good_keys[k]), v)) if k in good_keys else v for k,v in use_of_instruction.items()}
                            use_of_instruction = {k:' '.join([e for e in v if '$$' not in e]) for k,v in use_of_instruction.items() if any('$$' not in s for s in v)}
                            if use_of_instruction:
                                use_of_instruction['Index Name'] = use_of_instruction['Index Name'][:-1] if use_of_instruction['Index Name'][-1] == ',' else use_of_instruction['Index Name']
                                use_of_instruction['Name'] = [e.strip() for e in use_of_instruction['Name'].split(',') if e.strip()][0]
                                use_of_instruction['Given Name'] = [e.strip() for e in use_of_instruction['Given Name'].split(',') if e.strip()][-1]
                                person_names_list.append(use_of_instruction)
                        
        
                test = Counter([tuple(e.items()) for e in person_names_list])
                temp_dict = {}
                for k,v in test.items():
                    k = {key:value for key, value in k}['Index Name']
                    if k not in temp_dict:
                        temp_dict[k] = v
                    else: temp_dict[k] += v
                proper_person = max([(e, SequenceMatcher(a=group['Index_Name'].to_list()[0],b=e).ratio()) for e in temp_dict.keys()], key=lambda x: x[-1])[0]
                #dlaczego to było tak?    
                # if len(set(temp_dict.values())) == 1:
                #     proper_person = max([(e, SequenceMatcher(a=group['Index_Name'].to_list()[0],b=e).ratio()) for e in temp_dict.keys()], key=lambda x: x[-1])[0]
                # else:
                #     proper_person = max({k: v for k,v in temp_dict.items()}, key=temp_dict.get)
                
                person_names_dict = {}
                for dictionary in person_names_list:
                    if dictionary['Index Name'] == proper_person:
                        for key, value in dictionary.items():
                            if key not in person_names_dict:
                                person_names_dict[key] = [value]
                            else:
                                person_names_dict[key].append(value)
                                
                    #else: Na razie PBL jest poza
            person_names_dict = {k:list(set(v)) for k,v in person_names_dict.items()}
        except ValueError:
            person_names_dict = {}
        except AttributeError:
            person_names_dict = {'Index Name': group['Index_Name'].dropna().drop_duplicates().to_list()[0]}
        person_names_dict['nazwa z tabeli'] = group['Index_Name'].to_list()
        #dodatki z wikidaty
        try:
            wiki_pseudonym = group['pseudonym.value'].dropna().drop_duplicates().to_list()[0]
        except IndexError: wiki_pseudonym = np.nan
        wiki_occupation = [e.split('❦') for e in group['occupationLabel.value'].dropna().drop_duplicates().to_list()]
        wiki_occupation = list(set([e for sub in wiki_occupation for e in sub]))
        
        try:
            wiki_birthplace = group['birthplaceLabel.value'].dropna().drop_duplicates().to_list()[0]
        except IndexError: wiki_birthplace = np.nan
        try:
            wiki_deathplace = group['deathplaceLabel.value'].dropna().drop_duplicates().to_list()[0]
        except IndexError: wiki_deathplace = np.nan
        try:
            wiki_birthdate = group['birthdate.value'].dropna().drop_duplicates().to_list()[0][:10]
        except IndexError: wiki_birthdate = np.nan
        try:
            wiki_deathdate = group['deathdate.value'].dropna().drop_duplicates().to_list()[0][:10]
        except IndexError: wiki_deathdate = np.nan
        
        wiki = {'pseudonym': wiki_pseudonym,
                'Occupation': wiki_occupation,
                'birthplace': wiki_birthplace,
                'deathplace': wiki_deathplace,
                'birthdate': wiki_birthdate,
                'deathdate': wiki_deathdate,
                'Sex Label': sex_label,
                'Wikidata ID': wikidata_id,
                'VIAF ID': viaf_id,
                'Project ID': project_id,
                'DataSource': ';'.join([data_sources_dict[e] for e in list(set(data_sources))])}
        person_names_dict.update(wiki)
        people_dict[name] = person_names_dict
        
    klucze = []
    for dictionary in people_dict:
        for key in people_dict[dictionary]:
            if key not in klucze:
                klucze.append(key)
    klucze = tuple(klucze)
                
    for dictionary in people_dict:
        keys = tuple(people_dict[dictionary].keys())
        difference = set(klucze) - set(keys)
        for el in difference:
            people_dict[dictionary].update({el:np.nan})
    
    people_list_of_dicts.append(people_dict)
        
#tutaj dodać pseudonimy!!!
pseudonimy_list = list(zip(pseudonimy['Project_ID'], [e.split('|') for e in pseudonimy['pseudonym of'].to_list()]))
pseudonimy_match = []
for d, list_i in pseudonimy_list:
    for i in list_i:
        pseudonimy_match.append((d, i))

people_dict = people_list_of_dicts[0]
pseudonyms_dict = people_list_of_dicts[1]

#tutaj dodać nazwę correct i zostawić tylko te pseudonimy, które != nazwa podstawowa

test = [[[pseudonyms_dict[p]['Index Name'],pseudonyms_dict[p]['nazwa z tabeli'],pseudonyms_dict[p]['Other Name Forms']],c, people_dict[c]['Index Name'] if not isinstance(people_dict[c]['Index Name'], float) else people_dict[c]['nazwa z tabeli']] for p,c in pseudonimy_match]

test = [[[e for e in a if not isinstance(e,float)], b, c if isinstance(c, str) else c[0]] for a,b,c in test]
test = [[list(set([e for sub in a for e in sub])),b,c] for a,b,c in test]
test = [[[e for e in a if e != c],b,c] for a,b,c in test]

pseudonimy = {}
for e in test:
    if e[1] not in pseudonimy:
        pseudonimy[e[1]] = e[0]
    else:
        pseudonimy[e[1]].extend(e[0])

pseudonimy = {k:list(set([f for sub in [e.split('|') for e in v] for f in sub])) for k,v in pseudonimy.items()}

for k,v in tqdm(people_dict.items()):
    # k = '1357'
    # v = people_dict.get(k)
    if k in pseudonimy:
        if isinstance(v['Pseudonym, Kryptonym'], list):
            people_dict[k]['Pseudonym, Kryptonym'].extend(pseudonimy[k])
        else:
            people_dict[k]['Pseudonym, Kryptonym'] = pseudonimy[k]

# {k:v['Pseudonym, Kryptonym'].extend(pseudonimy[k] if k in pseudonimy and isinstance(v['Pseudonym, Kryptonym'], list) else v['Pseudonym, Kryptonym'] == pseudonimy[k]) for k,v in people_dict.items()}





# for pseudo, correct in pseudonimy_match:
#     if isinstance(people_dict[correct]['Pseudonym, Kryptonym'], list) and isinstance(pseudonyms_dict[pseudo]['Index Name'], list):
#         new = list(set(people_dict[correct]['Pseudonym, Kryptonym'] + pseudonyms_dict[pseudo]['Index Name']))
#         people_dict[correct]['Pseudonym, Kryptonym'] = new
        
#     elif isinstance(people_dict[correct]['Pseudonym, Kryptonym'], float) and isinstance(pseudonyms_dict[pseudo]['Index Name'], list):
#         people_dict[correct]['Pseudonym, Kryptonym'] = list(set(pseudonyms_dict[pseudo]['Index Name']))

#     pseudonyms_dict[pseudonimy_match[2][0]]
#     people_dict[pseudonimy_match[2][1]]

# pseudonimy_do_oznaczenia = pseudonimy_do_oznaczenia['Project_ID'].to_list()

{k:v.update({'sztorc':True}) if v['Project ID'] in pseudonimy_do_oznaczenia else v.update({'sztorc':False}) for k,v in people_dict.items()}
        
#occupation for matching with classification
occupation_to_delete = gsheet_to_df('1-oBjrUytvx4LGSkuRJEUYDkmNx3l7sjdA0wIGTFI4Lc', 'Sheet1')
occupation_to_delete = occupation_to_delete[occupation_to_delete['PL'] == 'delete']['occupation'].to_list()

people_dict = {k:{ke:[e for e in va if e not in occupation_to_delete] if ke == 'Occupation' else va for ke,va in v.items()} for k,v in people_dict.items()}

old_occupation = dict(zip(occupation_classification['old_label_pl'], occupation_classification['Wikidata_ID']))
old_occupation.update(dict(zip(occupation_classification['old_id'], occupation_classification['Wikidata_ID'])))
old_occupation = {k:v for k,v in old_occupation.items() if pd.notnull(k)}

people_dict = {k:{ke:[old_occupation[e] if e in old_occupation else e for e in va ] if ke == 'Occupation' else va for ke,va in v.items()} for k,v in people_dict.items()}

wikidata_occupation = dict(zip(occupation_classification['name_pl'], occupation_classification['Wikidata_ID']))

people_dict = {k:{ke:[wikidata_occupation[e] if e in wikidata_occupation else e for e in va ] if ke == 'Occupation' else va for ke,va in v.items()} for k,v in people_dict.items()}

people_dict = {k:{ke:[f'https://www.wikidata.org/wiki/{e}' for e in va] if ke == 'Occupation' else va for ke,va in v.items()} for k,v in people_dict.items()}

#sex for matching with classification
sex = {'mężczyzna': {'sex_label_pl': 'mężczyzna',
                     'sex_label_en': 'man',
                     'Wikidata_ID': 'https://www.wikidata.org/wiki/Q8441'},
       'kobieta': {'sex_label_pl': 'kobieta',
                   'sex_label_en': 'female',
                   'Wikidata_ID': 'https://www.wikidata.org/wiki/Q467'},
       'interpłciowość': {'sex_label_pl': 'interpłciowość',
                          'sex_label_en': 'intersex',
                          'Wikidata_ID': 'https://www.wikidata.org/wiki/Q1097630'}}

people_dict = {k:{ke:sex[va]['Wikidata_ID'] if ke == 'Sex Label' and pd.notnull(va) else va for ke,va in v.items()} for k,v in people_dict.items()}

errors = []

for k,v in people_dict.items():
    # k = '200090'
    # v = people_dict[k]
    for ka, va in v.items():
        # ka = 'Index Name'
        # va = people_dict[k][ka]
        if ka == 'Pseudonym, Kryptonym':
            a = va if not(isinstance(va, float)) else []
            b = v['pseudonym'] if isinstance(v['pseudonym'], list) else v['pseudonym'].split('❦') if isinstance(v['pseudonym'], str) else []
            new = '|'.join(list(set(a + b)))
            people_dict[k][ka] = new
        elif ka == 'Birth':
            if isinstance(v['birthdate'], str):
                people_dict[k][ka] = v['birthdate']
            elif isinstance(va, list):
                try:
                    new = re.findall('\d{4}(?=-)', va[0])[0]
                    people_dict[k][ka] = new
                except IndexError:
                    people_dict[k][ka] = np.nan
        elif ka == 'Death':
            if isinstance(v['deathdate'], str):
                people_dict[k][ka] = v['deathdate']
            elif isinstance(va, list):
                try:
                    new = re.findall('(?<=-)\d{4}', va[0])[0]
                    people_dict[k][ka] = new
                except IndexError:
                    people_dict[k][ka] = np.nan  
        elif ka in ['birthplace', 'deathplace'] and isinstance(va, str):
             try:
                 people_dict[k][ka] = kartoteka_miejsc[kartoteka_miejsc['Name'] == va.split('❦')[0]]['Wikidata_ID'].to_list()[0]
             except IndexError:
                 if not(isinstance(va, float)):
                     people_dict[k][ka] = f'https://www.wikidata.org/wiki/{va}'
                 else:
                     errors.append({k:v})
        elif ka in ['Given Name', 'Name'] and isinstance(va, list):
            people_dict[k][ka] = ' '.join(va)
        elif ka == 'Index Name':
            if isinstance(va, list):
                people_dict[k][ka] = va[0]
            elif isinstance(va, float):
                people_dict[k][ka] = v['nazwa z tabeli'][0]

        #         #to jest do przemodelowania
        #     if not isinstance(ka,str):
        #         people_dict[k][ka] = va[0]
        #     elif isinstance(ka,list):
        #         people_dict[k][ka] = va[0]
        #     elif not isinstance(ka,float) : 
        #         people_dict[k][ka] = va
        #     else:
        #         people_dict[k][ka] = v['nazwa z tabeli'][0]        
        elif ka in ['Occupation', 'Other Name Forms'] and isinstance(va, list):
            people_dict[k][ka] = '|'.join(va)


# ttt = [v['Index Name'] for k,v in people_dict.items()]
# tttt = {}
# for k,v in people_dict.items():
#     if str(type(v.get('Index Name'))) not in tttt:
#         tttt[str(type(v.get('Index Name')))] = [v.get('Index Name'), v.get('nazwa z tabeli')]

# ttt = set([str(type(e)) for e in ttt])

# test = list(set([{v for k,v in list(e.values())[0].items() if k in ['birthplace', 'deathplace']}.pop() for e in errors]))
# test = [e for e in test if e not in kartoteka_miejsc['Name'].to_list() and e not in kartoteka_miejsc['Wikidata_ID'].to_list()]


# # kartoteka_osob.to_excel('test.xlsx', index=False)

# test = [{k:v for k,v in people_dict[e].items() if k in ['birthplace', 'deathplace'] and isinstance(v, str) and v[0] == 'Q'} for e in people_dict]
# test = [e for e in test if e]

people_dict = {k:{ka:va for ka,va in v.items() if ka not in ['nazwa z tabeli', 'pseudonym', 'birthdate', 'deathdate']} for k,v in people_dict.items()}

#tutaj trzeba zdeduplikować pseudonimy

# poszukać jeszcze tyld!!!
# ttt = {k:v for k,v in people_dict.items() if isinstance(v.get('Index Name'),float)}
# ttt = [(k,v) for k,v in people_dict.items() if isinstance(v.get('Index Name'),str) and '~' in v.get('Index Name')]
# ttt = [e[-1].get('Index Name') for e in ttt]

people_df = pd.DataFrame.from_dict(people_dict, orient='index')
people_df = people_df.replace(r'^\s*$', np.NaN, regex=True)

people_df.to_csv('samizdat_people_to_nodegoat.csv', index=False, encoding='utf-8')

#przygotować właściwe typy danych
#ujednolicić daty
#zlepić pseudonimy
#zostawić tylko potrzebne pola

selection = ['1271', '1383', '1945']
selection = ['1390', '501390', '200090']
selection = ['252', '253']
selection = ['1744', '501744', '931']
selection = ['844', '843']
selection = ['1506']

selection = ['1594', '501594']
selection = ['2110', '101142']
selection = ['1744', '501744']
selection = ['1125', '301125', '301438']
selection = ['2039', '502039', '302039', '302041']

test = people_df[people_df.index.isin(selection)]

test = {k:v for k,v in people_dict.items() if k in selection}


#Index Name – string – longest nazwa z tabeli + sztorc lub nazwa z tabeli
#Project ID – string
#VIAF ID – external
#Wikidata ID – external
#Sex Label – classification
#Occupation – classification |
#Name – string " "
#Given Name – string " "
#Pseudonym, Kryptonym – string |
#True Name – string
#Other Name Forms – string |
#Addition to name – string
#Name Numeration – string
#Name Numeration – string
#Name Numeration (Arabic) – number
#DataSource – classification ;
#sztorc 



#%%






















df = pd.DataFrame.from_dict(people_dict, orient='index')

#dodać pseudonimy!!!
df.to_excel('samizdat_osoby_plik_testowy.xlsx')    
    
#dodać pole Wikidata Name – label osoby z wikidaty w językach [pl, en, język ojczysty osoby] (4 kolumny: wikidata name pl, wikidta name en, wikidata mother tongue form, wikidata mother tongue)
#spiąć birthplace, deathplace z kartoteką miejsc
# jeśli w wiki birthdate i birthplace, to brać, jeśli nie, to daty z biblio
#połączyć pseudonimy z biblio i wiki
    
# POMYSŁ – dla osób bez wikidaty pobrać dodatkowe info z VIAF (ile to osób?)
    
    
                
    [dict(s) for s in set(frozenset(d.items()) for d in person_names_list)]  
    
    
    index_name = max(group['Index_Name'].to_list(), key=lambda x: len(x))
    
    temp_dict = {'Sex Label': sex_label,
                 'Project ID': project_id,
                 'VIAF ID': viaf_id,
                 'Wikidata ID': wikidata_id,
                 'Index_Name': index_name,
                 }









test = 'bn_books-83554-bnb3956-X100-%1|bn_books-83573-bnb3958-X100-%1|bn_books-83591-bnb3960-X100-%1|bn_books-83611-bnb3962-X100-%1|bn_books-83630-bnb3964-X100-%1|bn_books-83646-bnb3966-X100-%1|bn_books-83667-bnb3968-X100-%1|bn_books-83688-bnb3970-X100-%1|bn_books-83554-bnb3956-X100-%p|bn_books-83573-bnb3958-X100-%p|bn_books-83591-bnb3960-X100-%p|bn_books-83611-bnb3962-X100-%p|bn_books-83630-bnb3964-X100-%p|bn_books-83646-bnb3966-X100-%p|bn_books-83667-bnb3968-X100-%p|bn_books-83688-bnb3970-X100-%p|bn_books-83554-bnb3956-X100-%k|bn_books-83573-bnb3958-X100-%k|bn_books-83591-bnb3960-X100-%k|bn_books-83646-bnb3966-X100-%k|bn_books-83667-bnb3968-X100-%k'

test = test.split('|')
for form in test:
    form = test[0]
    source = form.split('-')[0]
    record_id = form.split('-')[1]
    df = records_dict[source]
    field = 100
    ttt = marc_parser_dict_for_field(df[df['id'] == record_id][field].to_list()[0], '%')
    
 
people_file = kartoteka_osob.copy()   
#Sex Label
people_file['Sex Label'] = people_file['sexLabel.value']
#Project ID
test = {k:v for k,v in dict(Counter(people_file['Project_ID'])).items() if v > 1}
#VIAF ID

#Wikidata ID

#Index Name

#Name

#Given Name

#Pseudonym, Kryptonym

#True Name

#Other Name Forms

#Addition to name

#Name Numeration

#Name Numeration (Arabic)

#Unknown Name Source

#DataSource - classification

#Birth

#Death
    
    
    
    
    
    
    
    
    
    
    
    
    
# test = [e.split('❦') for e in kartoteka_osob['occupationLabel.value'].dropna().to_list()]
# test = pd.DataFrame(sorted(list(set([e for sub in test for e in sub]))), columns=['occupation'])
# test.to_excel('samizdat_occupation.xlsx', index=False)
    
viafy_osob = kartoteka_osob.loc()[kartoteka_osob['autor.value'].notnull()]['viaf_ID'].drop_duplicates().to_list()

list_of_dicts = ask_wikidata_for_places(viafy_osob)


#%% wszystkie osoby, których nie ma na przykładzie Bieleckiego po name_form_id

etap1 = gsheet_to_df('1Y8Y_pfkuKiv5npL6QJAXWbDO6twIJqsK3ArvaokjFkU', 'samizdat')
etap1_adresy = [e.split('|') for e in etap1['name_form_id'].to_list()]
etap1_adresy = set([e for sub in etap1_adresy for e in sub])

etap2 = gsheet_to_df('1xOAccj4SK-4olYfkubvittMM8R-jwjUjVhZXi9uGqdQ', 'dane_biblio')
etap2_adresy = [e.split('|') for e in etap2['name_form_id'].to_list()]
etap2_adresy = set([e for sub in etap2_adresy for e in sub])

roznica = list(etap1_adresy - etap2_adresy)

etap1_dict = dict(zip(etap1['name_form'].to_list(), etap1['name_form_id'].to_list()))
etap1_dict = {k:v.split('|') for k,v in etap1_dict.items()}

roznica_dict = {k:v for k,v in etap1_dict.items() if any(e in roznica for e in v)}
roznica_dict = {k:'|'.join(v) for k,v in roznica_dict.items()}

roznica_df = pd.DataFrame.from_dict(roznica_dict, orient='index')
roznica_df.to_excel('test.xlsx')



roznica_dict_bez_all_pbl = {k:v for k,v in roznica_dict.items() if len(v) != len([e for e in v if 'pbl' in e])}







x = 'bn_books-27072-bnb1329-X100-%1|bn_books-27100-bnb1330-X100-%1|bn_books-27121-bnb1331-X100-%1|bn_books-27148-bnb1332-X100-%1|bn_books-27163-bnb1333-X100-%1|bn_books-27182-bnb1334-X100-%1|bn_books-27203-bnb1335-X100-%1|cz_books-2428302-czb202-X600-$$a|cz_books-2429199-czb618-X700-$$a|cz_articles-2367383-cza244-X100-$$a|cz_articles-2390770-cza248-X100-$$a|cz_articles-2167498-cza428-X600-$$a|cz_articles-2168390-cza454-X600-$$a|cz_articles-2168646-cza458-X600-$$a|cz_articles-2363295-cza629-X600-$$a|cz_articles-2363397-cza634-X600-$$a|cz_articles-2367749-cza670-X600-$$a|cz_articles-2393543-cza778-X600-$$a|cz_articles-2393570-cza783-X600-$$a|cz_articles-2421154-cza1015-X600-$$a|cz_articles-2115367-cza1164-X700-$$a|cz_articles-2389626-cza1314-X700-$$a|cz_articles-2397438-cza1334-X700-$$a|cz_articles-2404320-cza1394-X700-$$a'.split('|')
len(x)

[e for e in roznica_dict_bez_all_pbl["Havel V~'aclav|Havel, Vaclav"] if 'pbl' not in e]

















