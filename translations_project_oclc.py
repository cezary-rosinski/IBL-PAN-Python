import pandas as pd
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
import gspread as gs
from datetime import datetime
import regex as re
from collections import OrderedDict
from gspread_dataframe import set_with_dataframe, get_as_dataframe
import numpy as np
from my_functions import cosine_sim_2_elem, marc_parser_1_field, gsheet_to_df, xml_to_mrk, cSplit, f, df_to_mrc, mrc_to_mrk, mrk_to_df, xml_to_mrk, mrk_to_mrc, get_cosine_result, df_to_gsheet, cluster_strings, cluster_records, simplify_string
import unidecode
import pandasql
import time
# from google_drive_research_folders import cr_projects
from functools import reduce
import sys
import csv
from tqdm import tqdm
import json
import xml.etree.ElementTree as et
import requests
from collections import Counter
import warnings
import io
import glob

warnings.simplefilter(action='ignore', category=FutureWarning)

#%%
now = datetime.now()
year = now.year
month = '{:02}'.format(now.month)
day = '{:02}'.format(now.day)
#%%
#autoryzacja do tworzenia i edycji plików
gc = gs.oauth()
#autoryzacja do penetrowania dysku
gauth = GoogleAuth()
gauth.LocalWebserverAuth()
drive = GoogleDrive(gauth)

#%% def
def replace_viaf_group(df):
    viaf_groups = {'256578118':'118529174', '83955898':'25095273', '2299152636076120051534':'11196637'}
    df['viaf_id'] = df['viaf_id'].replace(viaf_groups)
    return df

def index_of_correctness(x):
    full_index = 7
    record_index = 0
    if x['008'][35:38] != 'und':
        record_index += 1
    if pd.notnull(x['240']) and '$a' in x['240'] and any(e for e in ['$l', '$i'] if e in x['240']) and x['240'].count('$a') == 1 and '$k' not in x['240']:
        record_index += 3
    # elif pd.notnull(x['240']) and '$a' in x['240'] and x['240'].count('$a') == 1:
    #     record_index += 1.5
    if pd.notnull(x['245']) and all(e for e in ['$a', '$c'] if e in x['245']):
        record_index += 1
    elif pd.notnull(x['245']) and any(e for e in ['$a', '$c'] if e in x['245']): 
        record_index += 0.5
    if pd.notnull(x['260']) and all(e for e in ['$a', '$b', '$c'] if e in x['260']):
        record_index += 1
    elif pd.notnull(x['260']) and any(e for e in ['$a', '$b', '$c'] if e in x['260']):
        record_index += 0.5
    if pd.notnull(x['700']) and pd.notnull(x['700']):
        record_index += 1
    full_index = record_index/full_index
    return full_index

def genre(df):
    genres_dict = {'0':'nonfiction','e':'nonfiction', 'f':'fiction' ,'1':'fiction' ,'h':'fiction' ,'j':'fiction', 'd':'drama','p':'poetry'}
    df['cluster_genre'] = df['fiction_type'].replace(genres_dict)
    df.loc[~df["cluster_genre"].isin(list(genres_dict.values())), "cluster_genre"] = "other"
    return df

def genre_algorithm(df):
    length = len(df['cluster_genre'])
    x = Counter(df['cluster_genre'])
    if x['nonfiction']/length > 0.8:
        return 'nonfiction'
    elif x['drama']/length > 0.1:
        return 'drama'
    elif x['poetry']/length > 0.1:
        return 'poetry'
    else:
        return 'fiction'
    
def longest_string(s):
    return max(s, key=len)

def harvested_list_to_df(bn_harvesting_list):
    final_list = []
    for lista in bn_harvesting_list:
        slownik = {}
        for el in lista:
            if el[1:4] in slownik:
                slownik[el[1:4]] += f"❦{el[6:]}"
            else:
                slownik[el[1:4]] = el[6:]
        final_list.append(slownik)
    
    df = pd.DataFrame(final_list).drop_duplicates().reset_index(drop=True)
    fields = df.columns.tolist()
    fields = [i for i in fields if 'LDR' in i or re.compile('\d{3}').findall(i)]
    df = df.loc[:, df.columns.isin(fields)]
    fields.sort(key = lambda x: ([str,int].index(type("a" if re.findall(r'\w+', x)[0].isalpha() else 1)), x))
    df = df.reindex(columns=fields)   
    return df

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

#%% google drive
# file_list = drive.ListFile({'q': f"'{cr_projects}' in parents and trashed=false"}).GetList() 
# #[print(e['title'], e['id']) for e in file_list]
# translation_folder = [file['id'] for file in file_list if file['title'] == 'Vimr Project'][0]
# file_list = drive.ListFile({'q': f"'{translation_folder}' in parents and trashed=false"}).GetList() 
# #[print(e['title'], e['id']) for e in file_list]
# cz_authority_spreadsheet = [file['id'] for file in file_list if file['title'] == 'cz_authority'][0]
# cz_authority_spreadsheet = gc.open_by_key(cz_authority_spreadsheet)

# cz_authority_spreadsheet.worksheets()

# cz_authority_spreadsheet = pd.read_excel("C:\\Users\\Cezary\\Desktop\\new_cz_authority_df_2022-02-02.xlsx")
cz_authority_spreadsheet = gsheet_to_df('1yKcilB7SEVUkcSmqiTPauPYtALciDKatMvgqiRoEBso', 'Sheet1')

#%% Czech National Library - SKC set harvesting

# paths = ['F:/Cezary/Documents/IBL/Translations/Czech database/2020-05-27/',
#          'F:/Cezary/Documents/IBL/Translations/Czech database/2021-03-18/'
#          'F:/Cezary/Documents/IBL/Translations/Czech database/nkc_SKC_2021-04-07/',
#          'F:/Cezary/Documents/IBL/Translations/Czech database/nkc_SKC_2021-07-27/',
#          'F:/Cezary/Documents/IBL/Translations/Czech database/nkc_SKC_2021-08-05/',
#          'F:/Cezary/Documents/IBL/Translations/Czech database/nkc_SKC_2021-08-10/']

# cz_harvested = []

# for path in tqdm(paths):
#     files = [e for e in glob.glob(path + '*.mrk', recursive=True)]
#     # ids = []    
#     for file in tqdm(files):
#         marc_list = io.open(file, 'rt', encoding = 'utf8').read().splitlines()
#         mrk_list = []
#         for row in marc_list:
#             if row.startswith('=LDR'):
#                 mrk_list.append([row])
#             else:
#                 if row:
#                     mrk_list[-1].append(row)
    
#         final_list = []
#         for lista in mrk_list:
#             slownik = {}
#             for el in lista:
#                 if el[1:4] in slownik:
#                     slownik[el[1:4]] += f"❦{el[6:]}"
#                 else:
#                     slownik[el[1:4]] = el[6:]
#             final_list.append(slownik)
    
#         for el in final_list:
#             try:
#                 is_not_czech = el['008'][35:38] != 'cze'
#                 is_book = el['LDR'][6:8] == 'am'
#                 is_translated_from_czech = '$hcze' in el['041']
#                 if all([is_not_czech, is_book, is_translated_from_czech]):
#                     cz_harvested.append(el)
#             except KeyError:
#                 pass
            
# df = pd.DataFrame(cz_harvested).drop_duplicates().reset_index(drop=True)
# fields = df.columns.tolist()
# fields = [i for i in fields if 'LDR' in i or re.compile('\d{3}').findall(i) or 'cz_id' in i]
# df = df.loc[:, df.columns.isin(fields)]
# fields.sort(key = lambda x: ([str,int].index(type("a" if re.findall(r'\w+', x)[0].isalpha() else 1)), x))
# df = df.reindex(columns=fields)           
# df = df.sort_values('005', ascending=False).groupby('001').head(1)   
# df = df[df['998'].str.contains('local_base=SKC', regex=False)]

# sheet = gc.create(f'Czech translations_{year}-{month}-{day}', translation_folder)

# try:
#     set_with_dataframe(sheet.worksheet('Czech translations'), df_total)
# except gs.WorksheetNotFound:
#     sheet.add_worksheet(title="Czech translations", rows="100", cols="20")
#     set_with_dataframe(sheet.worksheet('Czech translations'), df_total)

# for worksheet in sheet.worksheets():
    
#     sheet.batch_update({
#         "requests": [
#             {
#                 "updateDimensionProperties": {
#                     "range": {
#                         "sheetId": worksheet._properties['sheetId'],
#                         "dimension": "ROWS",
#                         "startIndex": 0,
#                         #"endIndex": 100
#                     },
#                     "properties": {
#                         "pixelSize": 20
#                     },
#                     "fields": "pixelSize"
#                 }
#             }
#         ]
#     })
    
#     worksheet.freeze(rows=1)
#     worksheet.set_basic_filter()


# test = df_total['001'].value_counts().reset_index()






# #mrc to mrk

# # path = 'F:/Cezary/Documents/IBL/Translations/Czech database/nkc_SKC_2021-08-05/'
# path = 'F:/Cezary/Documents/IBL/Translations/Czech database/nkc_SKC_2021-08-10/'
# files = [e for e in glob.glob(path + '*.mrc', recursive=True)]
# for file_path in tqdm(files):
#     path_mrk = file_path.replace('.mrc', '.mrk')

#     mrc_to_mrk(file_path, path_mrk)


paths = ['F:/Cezary/Documents/IBL/Translations/Czech database/2020-05-27/',
         'F:/Cezary/Documents/IBL/Translations/Czech database/2021-03-18/'
         'F:/Cezary/Documents/IBL/Translations/Czech database/nkc_SKC_2021-04-07/',
         'F:/Cezary/Documents/IBL/Translations/Czech database/nkc_SKC_2021-07-27/',
         'F:/Cezary/Documents/IBL/Translations/Czech database/nkc_SKC_2021-08-05/',
         'F:/Cezary/Documents/IBL/Translations/Czech database/nkc_SKC_2021-08-10/']

#wczytanie danych z tabeli
authority_names_df = gsheet_to_df('1QB5EmMhg7qSWfWaJurafHdmXc5uohQpS-K5GBsiqerA', 'incl_missing')
# authority_names_df = authority_names_df[authority_names_df['used in OCLC'] == 'MISSING']
cz_ids = [[(el.split('|')[-1], viaf, name) for el in e.split('❦') if 'NKC' in el] for e, viaf, name in zip(authority_names_df['IDs'], authority_names_df['viaf_id'], authority_names_df['cz_name'])]
cz_ids = [e for sub in cz_ids for e in sub]
cz_id, cz_viaf, cz_name = zip(*cz_ids)
cz_ids_df = pd.DataFrame(cz_ids, columns=['cz_id', 'viaf_id', 'cz_name'])

cz_harvested = []

for path in tqdm(paths):
    files = [e for e in glob.glob(path + '*.mrk', recursive=True)]
    # ids = []    
    for file in tqdm(files):
        marc_list = io.open(file, 'rt', encoding = 'utf8').read().splitlines()
        mrk_list = []
        for row in marc_list:
            if row.startswith('=LDR'):
                mrk_list.append([row])
            else:
                if row:
                    mrk_list[-1].append(row)
                    
        final_list = []
        for lista in mrk_list:
            slownik = {}
            for el in lista:
                if el[1:4] in slownik:
                    slownik[el[1:4]] += f"❦{el[6:]}"
                else:
                    slownik[el[1:4]] = el[6:]
            final_list.append(slownik)
            
        # for el in final_list:
        #     ids.append(el['001'])
        
        for el in final_list:
            try:
                is_not_czech = el['008'][35:38] != 'cze'
                is_book = el['LDR'][6:8] == 'am'
                if if_not_czech and is_book and marc_parser_dict_for_field(el['100'], '\$')['$7'].strip() in cz_id:
                    # el_id = marc_parser_dict_for_field(el['100'], '\$')['$7'].strip()
                    # el['cz_id'] = el_id
                    cz_harvested.append(el)
            except KeyError:
                pass
    
# final_list[0]    
# test = []
# for el in tqdm(final_list):
#     print(el['001'])
    
#     if el['001'] == 'zpk20183059372':
#         test.append(el)
    
# 'jo2015884136' in cz_id    

# 'zpk20183059372' in ids
            
skc_df = pd.DataFrame(cz_harvested).drop_duplicates().reset_index(drop=True)
fields = skc_df.columns.tolist()
fields = [i for i in fields if 'LDR' in i or re.compile('\d{3}').findall(i) or 'cz_id' in i]
skc_df = skc_df.loc[:, skc_df.columns.isin(fields)]
fields.sort(key = lambda x: ([str,int].index(type("a" if re.findall(r'\w+', x)[0].isalpha() else 1)), x))
skc_df = skc_df.reindex(columns=fields) 
skc_df = skc_df.sort_values('005', ascending=False).groupby('001').head(1)   
skc_df = skc_df[skc_df['998'].str.contains('local_base=SKC', regex=False)]

df_total = pd.concat([df, skc_df]).drop_duplicates().reset_index(drop=True)
          
skc_df = pd.merge(skc_df, cz_ids_df, how='left', on='cz_id')                           

missing_in_skc = cz_ids_df[~cz_ids_df['cz_id'].isin(skc_df['cz_id'])]['cz_id'].unique()
with open('missing_in_skc.txt', 'w') as file:
    for el in missing_in_skc:
        file.write(f'{el}/n')
        
skc_df = skc_df.sort_values('005', ascending=False).groupby('001').head(1)        
skc_df.to_excel(f'skc_cz_authority_{year}-{month}-{day}.xlsx', index=False)

skc_df['language'] = skc_df['008'].apply(lambda x: x[35:38])
skc_df_translations = skc_df[skc_df['language'] != 'cze']
skc_df_translations.to_excel(f'skc_translations_cz_authority_{year}-{month}-{day}.xlsx', index=False)

#!!!!problemy:!!!!
#1028612282 to w OCLC trafiło do selections, a w CLT ciągle jest, bo ci nie mają oryginalnych tytułów
# propozycja – difficult records robić dla całości po deduplikacji po OCLC ID

#%% load data
list_of_records = []
original_number_of_records = 0
with open(r"C:\Users\Cezary\Downloads\oclc_lang.csv", 'r', encoding="utf8", errors="surrogateescape") as csv_file:
#with open('C:/Users/User/Desktop/oclc_lang.csv', 'r', encoding="utf8", errors="surrogateescape") as csv_file:
    reader = csv.reader(csv_file, delimiter=',')
    headers = next(reader)
    position_008 = headers.index('008')
    for row in tqdm(reader):
        if row[1] == '1135277855':
            print(row)
        original_number_of_records += 1
        if row[position_008][35:38] != 'cze':
            list_of_records.append(row)
            
oclc_lang = pd.DataFrame(list_of_records, columns=headers)
oclc_lang['language'] = oclc_lang['008'].apply(lambda x: x[35:38])
oclc_viaf = pd.read_excel('F:/Cezary/Documents/IBL/Translations/OCLC/Czech viaf/oclc_viaf.xlsx')
oclc_viaf['language'] = oclc_viaf['008'].apply(lambda x: x[35:38])
oclc_viaf = oclc_viaf[oclc_viaf['language'] != 'cze']

oclc_other_languages = pd.concat([oclc_lang, oclc_viaf]).drop_duplicates()

oclc_other_languages['nature_of_contents'] = oclc_other_languages['008'].apply(lambda x: x[24:27])
oclc_other_languages = oclc_other_languages[oclc_other_languages['nature_of_contents'].isin(['\\\\\\', '6\\\\', '\\6\\', '\\\\6'])]
oclc_other_languages['type of record + bibliographic level'] = oclc_other_languages['LDR'].apply(lambda x: x[6:8])
oclc_other_languages['fiction_type'] = oclc_other_languages['008'].apply(lambda x: x[33])

# cz_authority_df = get_as_dataframe(cz_authority_spreadsheet.worksheet('Sheet1'), evaluate_formulas=True).dropna(how='all').dropna(how='all', axis=1)
# cz_authority_df = cz_authority_df[cz_authority_df['used in OCLC'] == True]
# nowy arkusz Ondreja!!!
# cz_authority_df = gsheet_to_df('1QB5EmMhg7qSWfWaJurafHdmXc5uohQpS-K5GBsiqerA', 'incl_missing')
cz_authority_df = gsheet_to_df('1yKcilB7SEVUkcSmqiTPauPYtALciDKatMvgqiRoEBso', 'Sheet1')
# cz_authority_df = pd.read_excel("C:/Users/Cezary/Downloads/cz_authority.xlsx", sheet_name='incl_missing')

# tutaj wąsko
# viaf_positives = cz_authority_df['viaf_positive'].drop_duplicates().dropna().to_list()
# viaf_positives = cz_authority_df['viaf_positive'].drop_duplicates().dropna().to_list()
# viaf_positives = [f"http://viaf.org/viaf/{l}" for l in viaf_positives if l]

# positive_viafs_names = cz_authority_df[cz_authority_df['viaf_positive'].notnull()][['viaf_positive', 'all_names']]
# positive_viafs_names = cSplit(positive_viafs_names, 'viaf_positive', 'all_names', '❦')
# positive_viafs_names['all_names'] = positive_viafs_names['all_names'].apply(lambda x: re.sub('(.*?)(\$a.*?)(\$0.*$)', r'\2', x) if pd.notnull(x) else np.nan)
# positive_viafs_names = positive_viafs_names[positive_viafs_names['all_names'].notnull()].drop_duplicates()

# positive_viafs_diacritics = cz_authority_df[cz_authority_df['viaf_positive'].notnull()][['viaf_positive', 'cz_name']]
# positive_viafs_diacritics['cz_name'] = positive_viafs_diacritics['cz_name'].apply(lambda x: unidecode.unidecode(x))

# viaf_positives_dict = {}
# for element in viaf_positives:
#     viaf_positives_dict[re.findall('\d+', element)[0]] = {'viaf id':element}
# for i, row in positive_viafs_names.iterrows():
#     if 'form of name' in viaf_positives_dict[row['viaf_positive']]:
#         viaf_positives_dict[row['viaf_positive']]['form of name'].append(row['all_names'])
#     else:
#         viaf_positives_dict[row['viaf_positive']].update({'form of name':[row['all_names']]})
# for i, row in positive_viafs_diacritics.iterrows():
#     if 'unidecode name' in viaf_positives_dict[row['viaf_positive']]:
#         viaf_positives_dict[row['viaf_positive']]['unidecode name'].append(row['cz_name'])
#     else:
#         viaf_positives_dict[row['viaf_positive']].update({'unidecode name':[row['cz_name']]})
        
# viaf_positives_dict = dict(sorted(viaf_positives_dict.items(), key = lambda item : len(item[1]['unidecode name']), reverse=True))

#tutaj szeroko
viaf_positives = [e.split('|') for e in cz_authority_df['viaf_id'].drop_duplicates().dropna().to_list()]
viaf_positives = [e for sub in viaf_positives for e in sub]

positive_viafs_names = cz_authority_df[cz_authority_df['viaf_id'].notnull()][['viaf_id', 'all_names']]
positive_viafs_names = cSplit(positive_viafs_names, 'viaf_id', 'all_names', '❦')
positive_viafs_names['all_names'] = positive_viafs_names['all_names'].apply(lambda x: re.sub('(.*?)(\$a.*?)(\$0.*$)', r'\2', x) if pd.notnull(x) else np.nan)
positive_viafs_names = positive_viafs_names[positive_viafs_names['all_names'].notnull()].drop_duplicates()
positive_viafs_names['index'] = positive_viafs_names.index+1
positive_viafs_names = cSplit(positive_viafs_names, 'index', 'viaf_id', '|').drop(columns='index')

positive_viafs_diacritics = cz_authority_df[cz_authority_df['viaf_id'].notnull()][['viaf_id', 'cz_name']]
positive_viafs_diacritics['cz_name'] = positive_viafs_diacritics['cz_name'].apply(lambda x: unidecode.unidecode(x))
positive_viafs_diacritics['index'] = positive_viafs_diacritics.index+1
positive_viafs_diacritics = cSplit(positive_viafs_diacritics, 'index', 'viaf_id', '|').drop(columns='index')

viaf_positives_dict = {}
for element in tqdm(viaf_positives, total = len(viaf_positives)):
    viaf_positives_dict[re.findall('\d+', element)[0]] = {'viaf id':element}
for i, row in positive_viafs_names.iterrows():
    if 'form of name' in viaf_positives_dict[row['viaf_id']]:
        viaf_positives_dict[row['viaf_id']]['form of name'].append(row['all_names'])
    else:
        viaf_positives_dict[row['viaf_id']].update({'form of name':[row['all_names']]})
for i, row in positive_viafs_diacritics.iterrows():
    if 'unidecode name' in viaf_positives_dict[row['viaf_id']]:
        viaf_positives_dict[row['viaf_id']]['unidecode name'].append(row['cz_name'])
    else:
        viaf_positives_dict[row['viaf_id']].update({'unidecode name':[row['cz_name']]})
        
viaf_positives_dict = dict(sorted(viaf_positives_dict.items(), key = lambda item : len(item[1]['unidecode name']), reverse=True))

# with open("viaf_positives_dict.json", 'w', encoding='utf-8') as file: 
#     json.dump(viaf_positives_dict, file, ensure_ascii=False, indent=4)

# oclc_other_languages['language'].drop_duplicates().sort_values().to_list()

#%% clusters for original titles        
# fiction_types = ['1', 'd', 'f', 'h', 'j', 'p']

df = oclc_other_languages.copy()
df_language_materials_monographs = df[df['type of record + bibliographic level'] == 'am']
negative = df_language_materials_monographs.copy()
df_other_types = df[~df['001'].isin(df_language_materials_monographs['001'])]
# df_first_positive = df_language_materials_monographs[(df_language_materials_monographs['041'].str.contains('\$hcz')) &
#                                                      (df_language_materials_monographs['fiction_type'].isin(fiction_types))]

df_first_positive = df_language_materials_monographs[df_language_materials_monographs['041'].str.contains('\$hcz', na=False)]
positive_1 = df_first_positive.drop(columns=['type of record + bibliographic level', 'nature_of_contents']).drop_duplicates().reset_index(drop=True)

negative = negative[~negative['001'].isin(df_first_positive['001'])]
df_second_positive = marc_parser_1_field(negative, '001', '100', '\$')[['001', '$1']]
df_second_positive['$1'] = df_second_positive['$1'].apply(lambda x: re.findall('\d+', x)[0] if x != '' else x)
df_second_positive = df_second_positive[df_second_positive['$1'].isin(viaf_positives)]
df_second_positive = negative[negative['001'].isin(df_second_positive['001'])]
positive_2 = df_second_positive.drop(columns=['type of record + bibliographic level', 'nature_of_contents']).drop_duplicates().reset_index(drop=True)

negative = negative[~negative['001'].isin(df_second_positive['001'])].reset_index(drop=True)
df_third_positive = "select * from negative a join positive_viafs_names b on a.'100' like '%'||b.all_names||'%'"
df_third_positive = pandasql.sqldf(df_third_positive)
positive_3 = df_third_positive.drop(columns=['all_names', 'type of record + bibliographic level', 'nature_of_contents', 'viaf_id']).drop_duplicates().reset_index(drop=True)

negative = negative[~negative['001'].isin(df_third_positive['001'])].reset_index(drop=True)
df_fourth_positive = "select * from negative a join positive_viafs_diacritics b on a.'100' like '%'||b.cz_name||'%'"
df_fourth_positive = pandasql.sqldf(df_fourth_positive)
positive_4 = df_fourth_positive.drop(columns=['cz_name', 'type of record + bibliographic level', 'nature_of_contents', 'viaf_id']).drop_duplicates().reset_index(drop=True)

negative = negative[~negative['001'].isin(df_fourth_positive['001'])].reset_index(drop=True)
df_all_positive = pd.concat([df_first_positive, df_second_positive, df_third_positive, df_fourth_positive]).drop(columns=['all_names', 'cz_name', 'type of record + bibliographic level', 'nature_of_contents', 'viaf_id']).drop_duplicates().reset_index(drop=True)

# has_viaf = df_second_positive = marc_parser_1_field(df_all_positive, '001', '100', '\$')[['001', '$1']]
# has_viaf['yes no'] = has_viaf['$1'].apply(lambda x: "yes" if x!='' else 'no')
# Counter(has_viaf['yes no'])

df_all_positive['260'] = df_all_positive[['260', '264']].apply(lambda x: x['260'] if pd.notnull(x['260']) else x['264'], axis=1)
df_all_positive['240'] = df_all_positive[['240', '246']].apply(lambda x: x['240'] if pd.notnull(x['240']) else x['246'], axis=1)
df_all_positive['100_unidecode'] = df_all_positive['100'].apply(lambda x: unidecode.unidecode(x).lower() if pd.notnull(x) else x)

df_all_positive.to_excel('oclc_all_positive.xlsx', index=False)
# df_all_positive = pd.read_excel("C:/Users/Cezary/Downloads/Translations/oclc_all_positive.xlsx")

df_all_positive_origin = pd.read_excel('oclc_all_positive.xlsx').reset_index(drop=True)

df_all_positive = df_all_positive_origin.copy().drop(columns=['all_names', 'cz_name']).drop_duplicates().reset_index(drop=True)
skc_df_translations = pd.read_excel('skc_translations_cz_authority_2021-8-12.xlsx').reset_index(drop=True)
skc_df_translations['260'] = skc_df_translations[['260', '264']].apply(lambda x: x['260'] if pd.notnull(x['260']) else x['264'], axis=1)
skc_df_translations['240'] = skc_df_translations[['240', '246']].apply(lambda x: x['240'] if pd.notnull(x['240']) else x['246'], axis=1)
skc_df_translations['100_unidecode'] = skc_df_translations['100'].apply(lambda x: unidecode.unidecode(x).lower() if pd.notnull(x) else x)

df_all_positive = pd.concat([df_all_positive, skc_df_translations])
df_all_positive = replace_viaf_group(df_all_positive)

#index of correctness
    
df_all_positive['index of correctness'] = df_all_positive.apply(lambda x: index_of_correctness(x), axis=1)  

df_oclc_people = marc_parser_1_field(df_all_positive, '001', '100_unidecode', '\\$')[['001', '$a', '$d', '$1']].replace(r'^\s*$', np.nan, regex=True)  
df_oclc_people['$ad'] = df_oclc_people[['$a', '$d']].apply(lambda x: '$d'.join(x.dropna().astype(str)) if pd.notnull(x['$d']) else x['$a'], axis=1)
df_oclc_people['$ad'] = '$a' + df_oclc_people['$ad']
df_oclc_people['simplify string'] = df_oclc_people['$a'].apply(lambda x: simplify_string(x))
df_oclc_people['simplify_ad'] = df_oclc_people['$ad'].apply(lambda x: simplify_string(x))
# df_oclc_people['001'] = df_oclc_people['001'].astype(int)

additional_viaf_to_remove = ['256578118', '83955898', '2299152636076120051534']
people_clusters = {k:v for k,v in viaf_positives_dict.copy().items() if k not in additional_viaf_to_remove}

# Hrabal, Hasek, Capek, Majerova selection
# selection = ['34458072', '4931097', '34454129', '52272']
# # selection = ['52272']
# people_clusters = {key: people_clusters[key] for key in selection}

for key in tqdm(people_clusters, total=len(people_clusters)):
    # key = '55955650'
    viaf_id = people_clusters[key]['viaf id']
    records = []
    records_1 = df_oclc_people[df_oclc_people['$1'] == viaf_id]['001'].to_list()
    records += records_1
    try:
        unidecode_lower_forms_of_names = [simplify_string(e) for e in people_clusters[key]['form of name']]
        records_2 = df_oclc_people[df_oclc_people['$ad'].isin(unidecode_lower_forms_of_names)]['001'].to_list()
        records += records_2
    except KeyError:
        pass
    try:
        unidecode_name = [simplify_string(e) for e in people_clusters[key]['unidecode name']]
        records_3 = df_oclc_people[df_oclc_people['simplify string'].isin(unidecode_name)]['001'].to_list()
        records += records_3
    except KeyError:
        pass
    try:
        unidecode_lower_forms_of_names = [simplify_string(e) for e in people_clusters[key]['form of name']]
        records_4 = df_oclc_people[df_oclc_people['simplify_ad'].isin(unidecode_lower_forms_of_names)]['001'].to_list()
        records += records_4
    except KeyError:
        pass
    records = list(set(records))
    people_clusters[key].update({'list of records':records})
    
people_clusters_records = {}
for key in people_clusters:
    people_clusters_records[key] = people_clusters[key]['list of records']
    
df_people_clusters = pd.DataFrame.from_dict(people_clusters_records, orient='index').stack().reset_index(level=0).rename(columns={'level_0':'cluster_viaf', 0:'001'})

def type_str(x):
    try:
        return str(int(x))
    except ValueError:
        return str(x)    

df_people_clusters['001'] = df_people_clusters['001'].apply(lambda x: type_str(x))
df_all_positive['001'] = df_all_positive['001'].apply(lambda x: type_str(x))
df_all_positive = df_all_positive.drop_duplicates().reset_index(drop=True)

# df_people_clusters = df_people_clusters.merge(df_all_positive, how='left', on='001').drop(columns=['all_names', 'cz_name', 'viaf_positive']).drop_duplicates().reset_index(drop=True)
df_people_clusters = df_people_clusters.merge(df_all_positive, how='left', on='001').drop(columns='cz_name').drop_duplicates().reset_index(drop=True)

# 'zpk20183020995' in df_oclc_people['001'].to_list()

# multiple_clusters = df_people_clusters['001'].value_counts().reset_index()
# multiple_clusters = multiple_clusters[multiple_clusters['001'] > 1]['index'].to_list()
# df_multiple_clusters = df_people_clusters[df_people_clusters['001'].isin(multiple_clusters)].sort_values('001')
# multiple_clusters_ids = df_multiple_clusters['001'].drop_duplicates().to_list()
# df_people_clusters = df_people_clusters[~df_people_clusters['001'].isin(multiple_clusters_ids)]
# df_multiple_clusters = df_multiple_clusters[df_multiple_clusters['cluster_viaf'] == '34454129']
# df_people_clusters = pd.concat([df_people_clusters, df_multiple_clusters])

df_original_titles = df_people_clusters.replace(r'^\s*$', np.nan, regex=True)    
df_original_titles = df_original_titles[(df_original_titles['240'].notnull()) & (df_original_titles['100'].notnull())][['001', '100', '240', 'cluster_viaf']]
df_original_titles_100 = marc_parser_1_field(df_original_titles, '001', '100', '\\$')[['001', '$a', '$d', '$1']].rename(columns={'$a':'name', '$d':'dates', '$1':'viaf'})
counter_100 = df_original_titles_100['001'].value_counts().reset_index()
counter_100 = counter_100[counter_100['001'] == 1]['index'].to_list()
df_original_titles_100 = df_original_titles_100[df_original_titles_100['001'].isin(counter_100)]
df_original_titles_240 = marc_parser_1_field(df_original_titles, '001', '240', '\\$')
try:
    #było
    #df_original_titles_240['original title'] = df_original_titles_240.apply(lambda x: x['$b'] if x['$b'] != '' else x['$a'], axis=1)
    #jest - zmiana 30.06.2021
    df_original_titles_240['original title'] = df_original_titles_240.apply(lambda x: ''.join([x['$a'], x['$b']]) if x['$b'] != '' else x['$a'], axis=1)
except KeyError:
    df_original_titles_240['original title'] = df_original_titles_240['$a']
# df_original_titles_240 = df_original_titles_240[['001', 'original title']]

df_original_titles_simple = pd.merge(df_original_titles_100, df_original_titles_240, how='left', on='001')
df_original_titles_simple = df_original_titles_simple.merge(df_original_titles[['001', 'cluster_viaf']]).reset_index(drop=True)
# df_original_titles_simple['001'] = df_original_titles_simple['001'].astype('Int64').astype('str')
df_original_titles_simple['index'] = df_original_titles_simple.index+1

df_original_titles_simple_grouped = df_original_titles_simple.groupby('cluster_viaf')

df_original_titles_simple = pd.DataFrame()
for name, group in tqdm(df_original_titles_simple_grouped, total=len(df_original_titles_simple_grouped)):
    # name = '25095273'
    # name = '107600220'
    # name = '27873545'
    group = df_original_titles_simple_grouped.get_group(name)
    # było 0.7 - zmiana 30.06.2021
    df = cluster_records(group, 'index', ['original title'], similarity_lvl=0.81)
    df_original_titles_simple = df_original_titles_simple.append(df)

df_original_titles_simple = df_original_titles_simple.sort_values(['cluster_viaf', 'cluster']).rename(columns={'cluster':'cluster_titles'}) 


df_with_original_titles = pd.merge(df_people_clusters, df_original_titles_simple.drop(columns=['index', 'cluster_viaf']), on='001', how='left').drop_duplicates()

correct = df_with_original_titles[df_with_original_titles['index of correctness'] > 0.7]
not_correct = df_with_original_titles[df_with_original_titles['index of correctness'] <= 0.7]
writer = pd.ExcelWriter('all_data_correctness.xlsx', engine = 'xlsxwriter')
correct.to_excel(writer, index=False, sheet_name='correct')
not_correct.to_excel(writer, index=False, sheet_name='not_correct')
writer.save()
writer.close()

# for 4 authors: 2365 from 7026 -> 34%  --- one record == one row
# for Majerova: 59 from 335 -> 18%

# correct = pd.read_excel('all_data_correctness.xlsx', sheet_name='correct')

correct = genre(correct)    

correct_grouped = correct.groupby('cluster_titles')

correct = pd.DataFrame()
for name, group in tqdm(correct_grouped, total=len(correct_grouped)):
    group['cluster_genre'] = genre_algorithm(group)
    correct = correct.append(group)
    
correct.to_excel('translation_correct_data.xlsx', index=False)    
    
#%% HQ records: de-duplication

#HQ or LQ
records_set = input('Choose HQ or LQ: ')
# records_sets = ['LQ', 'HQ']



if records_set == 'HQ':
    records_df = correct.copy()
elif records_set == 'LQ':
    records_df = not_correct.copy()
else:
    sys.exit('Wrong records set!')

records_grouped = records_df.groupby(['cluster_viaf', 'language', 'cluster_titles'], dropna=False)
# correct = pd.read_excel('translation_correct_data.xlsx') 
# ttt = correct.copy()
# correct = ttt.copy()
# not_correct = pd.read_excel('all_data_correctness.xlsx', sheet_name='not_correct')
# not_correct['cluster_titles'] = not_correct['cluster_titles'].apply(lambda x: type_str(x) if pd.notnull(x) else np.nan)
# correct = correct[(correct['cluster_viaf'] == '4931097') & (correct['language'] == 'ger') & (correct['cluster_titles'] == 2526)]   
# correct = correct[correct['cluster_viaf'] == '4931097']
# correct = correct[(correct['cluster_viaf'] == '10256796') & (correct['language'] == 'ger') & (correct['cluster_titles'] == 10081)]
# correct_grouped = correct.groupby(['cluster_viaf', 'language', 'cluster_titles'])
# grupy = list(correct_grouped.groups.keys())
writer = pd.ExcelWriter(f'{records_set}_data_deduplicated.xlsx', engine = 'xlsxwriter')
records_df.to_excel(writer, index=False, sheet_name='phase_0')

phase_1 = pd.DataFrame()
phase_2 = pd.DataFrame()
phase_3 = pd.DataFrame()
phase_4 = pd.DataFrame()
phase_5 = pd.DataFrame()
for name, group in tqdm(records_grouped, total=len(records_grouped)):
    # gg = group.copy()
    # group = gg.copy()
# slice = 100
# result = [g[1] for g in list(correct_grouped)[:slice]]
# df = pd.concat(result)
# df.to_excel(writer, index=False, sheet_name='phase_0') 
# for group in tqdm(result, total=len(result)):
    # group = correct_grouped.get_group(('4931097', 'pol', 2526))
    # group = correct_grouped.get_group(grupy[1])
# group = correct.copy()  
# group = correct_grouped.get_group(('107600220', 'hun', 8643))
    # group = records_grouped.get_group(name)
#phase_1: de-duplication 1: duplicates
    
    # name = test[6]
    # group = records_grouped.get_group(name)
    try:
        title = marc_parser_1_field(group, '001', '245', '\$')[['001', '$a', '$b', '$n', '$p']].replace(r'^\s*$', np.nan, regex=True)
    except KeyError:
        try:
            title = marc_parser_1_field(group, '001', '245', '\$')[['001', '$a', '$b']].replace(r'^\s*$', np.nan, regex=True)
        except KeyError:
            title = marc_parser_1_field(group, '001', '245', '\$')[['001', '$a']].replace(r'^\s*$', np.nan, regex=True)
    title['title'] = title[title.columns[1:]].apply(lambda x: simplify_string(x, with_spaces=False), axis=1)    
    title = title[['001', 'title']]
    group = pd.merge(group, title, how='left', on='001')
    
    try:
        place = marc_parser_1_field(group, '001', '260', '\$')[['001', '$a']].rename(columns={'$a':'place'})
        place = place[place['place'] != '']
        place['place'] = place['place'].apply(lambda x: simplify_string(x, with_spaces=False))
        place = place.groupby('001').agg({'place': longest_string}).reset_index()
    except KeyError:
        place = pd.DataFrame(columns=['001','place'])
    group = pd.merge(group, place, how='left', on='001')
    
    try:
        publisher = marc_parser_1_field(group, '001', '260', '\$')[['001', '$b']].rename(columns={'$b':'publisher'})
        publisher = publisher.groupby('001').head(1).reset_index(drop=True)
        publisher['publisher'] = publisher['publisher'].apply(lambda x: simplify_string(x, with_spaces=False))
        publisher = publisher.groupby('001').agg({'publisher': longest_string}).reset_index()
    except KeyError:
        publisher = pd.DataFrame(columns=['001','publisher'])
    group = pd.merge(group, publisher, how='left', on='001')
    
    year = group.copy()[['001', '008']].rename(columns={'008':'year'})
    year['year'] = year['year'].apply(lambda x: x[7:11])
    group = pd.merge(group, year, how='left', on='001')
    
    df_oclc_duplicates = pd.DataFrame()
    df_oclc_grouped = group.groupby(['title', 'place', 'year'])
    for sub_name, sub_group in df_oclc_grouped:
        if len(sub_group) > 1:
            sub_group['groupby'] = str(sub_name)
            group_ids = '❦'.join([str(e) for e in sub_group['001'].to_list()])
            sub_group['group_ids'] = group_ids
            df_oclc_duplicates = df_oclc_duplicates.append(sub_group)
    df_oclc_duplicates = df_oclc_duplicates.drop_duplicates()
    
    if df_oclc_duplicates.shape[0] > 1:
    
        oclc_duplicates_list = df_oclc_duplicates['001'].drop_duplicates().tolist()
        df_oclc_duplicates_grouped = df_oclc_duplicates.groupby(['title', 'place', 'year'])
        
        df_oclc_deduplicated = pd.DataFrame()
        for sub_name, sub_group in df_oclc_duplicates_grouped:
            for column in sub_group:
                if column in ['fiction_type', '490', '500', '650', '655']:
                    sub_group[column] = '❦'.join(sub_group[column].dropna().drop_duplicates().astype(str))
                else:
                    try:
                        sub_group[column] = max(sub_group[column].dropna().astype(str), key=len)
                    except ValueError:
                        sub_group[column] = np.nan
            df_oclc_deduplicated = df_oclc_deduplicated.append(sub_group)
        
        df_oclc_deduplicated = df_oclc_deduplicated.drop_duplicates().replace(r'^\s*$', np.nan, regex=True)
        df_oclc_deduplicated['001'] = df_oclc_deduplicated['001'].astype(object).astype('str')
        group = group[~group['001'].isin(oclc_duplicates_list)]
        group = pd.concat([group, df_oclc_deduplicated]).drop(columns='title')
        group['group_ids'] = group['group_ids'].apply(lambda x: '❦'.join(set(x.split('❦'))) if pd.notnull(x) else x)
        group = group.drop_duplicates().reset_index(drop=True)
        phase_1 = phase_1.append(group)
    else:
        group.drop(columns='title', inplace=True)
        group = group.drop_duplicates().reset_index(drop=True)
        phase_1 = phase_1.append(group)
    
#phase_2: de-duplication 2: multiple volumes
        
    title = marc_parser_1_field(group, '001', '245', '\$')[['001', '$a']].replace(r'^\s*$', np.nan, regex=True)
    title['title'] = title[title.columns[1:]].apply(lambda x: simplify_string(x, with_spaces=False), axis=1)    
    title = title[['001', 'title']]
    group = pd.merge(group, title, how='left', on='001')  
    
    df_oclc_grouped = group.groupby(['title', 'place', 'year'])
        
    df_oclc_multiple_volumes = pd.DataFrame()
    for sub_name, sub_group in df_oclc_grouped:
        if len(sub_group[sub_group['245'].str.contains('\$n', regex=True)]):
            sub_group['groupby'] = str(sub_name)
            try:
                group_ids = '❦'.join(set([str(e) for e in sub_group['001'].to_list() + sub_group['group_ids'].to_list() if pd.notnull(e)]))
            except KeyError:
                group_ids = '❦'.join(set([str(e) for e in sub_group['001'].to_list() if pd.notnull(e)]))
            sub_group['group_ids'] = group_ids
            df_oclc_multiple_volumes = df_oclc_multiple_volumes.append(sub_group)
            
    if df_oclc_multiple_volumes.shape[0] > 0:
        oclc_multiple_volumes_list = df_oclc_multiple_volumes['001'].drop_duplicates().tolist()
        df_oclc_multiple_volumes_grouped = df_oclc_multiple_volumes.groupby(['title', 'place', 'year'])
    
        df_oclc_multiple_volumes_deduplicated = pd.DataFrame()
        for sub_name, sub_group in df_oclc_multiple_volumes_grouped:
            if len(sub_group[~sub_group['245'].str.contains('\$n', regex=True)]) == 1:
                for column in sub_group:
                    if column in ['fiction_type', '490', '500', '650', '655']:
                        sub_group[column] = '❦'.join(sub_group[column].dropna().drop_duplicates().astype(str))  
                    elif column in ['001', '245']:
                        pass
                    else:
                        try:
                            sub_group[column] = max(sub_group[column].dropna().astype(str), key=len)
                        except ValueError:
                            sub_group[column] = np.nan
                df = sub_group[~sub_group['245'].str.contains('\$n', regex=True)]
                df_oclc_multiple_volumes_deduplicated = df_oclc_multiple_volumes_deduplicated.append(df)
            else:
                for column in sub_group:
                    if column in ['fiction_type', '490', '500', '650', '655']:
                        sub_group[column] = '❦'.join(sub_group[column].dropna().drop_duplicates().astype(str))
                    elif column == '245':
                        field_245 = marc_parser_1_field(sub_group, '001', '245', '\$').replace(r'^\s*$', np.nan, regex=True)
                        field_245 = field_245.iloc[:, lambda x: x.columns.isin(['001', '$a', '$b', '$c'])]
                        field_245['245'] = field_245[field_245.columns[1:]].apply(lambda x: ''.join(x.dropna().astype(str)), axis=1)
                        field_245 = field_245[['001', '245']]
                        field_245['245'] = '10' + field_245['245']
                        sub_group = pd.merge(sub_group.drop(columns='245'), field_245, how='left', on='001')
                        try:
                            sub_group[column] = max(sub_group[column].dropna().astype(str), key=len)
                        except ValueError:
                            sub_group[column] = np.nan
                    else:
                        try:
                            sub_group[column] = max(sub_group[column].dropna().astype(str), key=len)
                        except ValueError:
                            sub_group[column] = np.nan
                sub_group = sub_group.drop_duplicates().reset_index(drop=True)
                df_oclc_multiple_volumes_deduplicated = df_oclc_multiple_volumes_deduplicated.append(sub_group)
                    
        df_oclc_multiple_volumes_deduplicated = df_oclc_multiple_volumes_deduplicated.drop_duplicates().replace(r'^\s*$', np.nan, regex=True)
        df_oclc_multiple_volumes_deduplicated['001'] = df_oclc_multiple_volumes_deduplicated['001'].astype(object).astype('str')
        group = group[~group['001'].isin(oclc_multiple_volumes_list)]
        group = pd.concat([group, df_oclc_multiple_volumes_deduplicated]).drop_duplicates().reset_index(drop=True)
        group['group_ids'] = group['group_ids'].apply(lambda x: '❦'.join(set(x.split('❦'))) if pd.notnull(x) else x)
        group.drop(columns='title', inplace=True)
        group = group.drop_duplicates().reset_index(drop=True)
        phase_2 = phase_2.append(group)
    else:
        group.drop(columns='title', inplace=True)
        group = group.drop_duplicates().reset_index(drop=True)
        phase_2 = phase_2.append(group)

#phase_3: de-duplication 3: fuzzyness
    field_245 = marc_parser_1_field(group, '001', '245', '\$').replace(r'^\s*$', np.nan, regex=True)
    try:
        field_245['$a'] = field_245.apply(lambda x: x['$a'] if pd.notnull(x['$a']) else x['indicator'][2:].split('.', 1)[0], axis=1)
    except KeyError:
        field_245['$a'] = field_245.apply(lambda x: x['indicator'][2:].split('.', 1)[0], axis=1)
    field_245 = field_245.iloc[:, lambda x: x.columns.isin(['001', '$a', '$b'])]
    field_245['title'] = field_245[field_245.columns[1:]].apply(lambda x: simplify_string(x), axis=1)  
    field_245 = field_245[['001', 'title']]
    group = pd.merge(group, field_245, how='left', on='001')
    
    #similarity level == 0.85 | columns == ['title', 'publisher', 'year'] | same 'year'
    df_oclc_clusters = cluster_records(group, '001', ['title', 'publisher', 'year'], 0.85)    
    df_oclc_clusters = df_oclc_clusters[df_oclc_clusters['publisher'] != '']
    df_oclc_duplicates = df_oclc_clusters.groupby(['cluster', 'year']).filter(lambda x: len(x) > 1)
    
    if df_oclc_duplicates.shape[0] > 0:
 
        # if df_oclc_duplicates['001'].value_counts().max() > 1:
        #     sys.exit('ERROR!!!\nclustering problem!!!')
    
        oclc_duplicates_list = df_oclc_duplicates['001'].drop_duplicates().tolist()
        df_oclc_duplicates = df_oclc_duplicates.groupby(['cluster', 'year'])
        
        df_oclc_deduplicated = pd.DataFrame()
        for sub_name, sub_group in df_oclc_duplicates:
            # sub_group = df_oclc_duplicates.get_group((1108272735, '2016'))
            try:
                group_ids = '❦'.join(set([str(e) for e in sub_group['001'].to_list() + sub_group['group_ids'].to_list() if pd.notnull(e)]))
            except KeyError:
                group_ids = '❦'.join(set([str(e) for e in sub_group['001'].to_list() if pd.notnull(e)]))
            sub_group['group_ids'] = group_ids
            for column in sub_group:
                if column in ['fiction_type', '490', '500', '650', '655']:
                    sub_group[column] = '❦'.join(sub_group[column].dropna().drop_duplicates().astype(str))
                elif column == '245':
                    sub_group[column] = sub_group[column][sub_group[column].str.contains('$', regex=False)]
                    try:
                        sub_group[column] = max(sub_group[column].dropna().astype(str), key=len)
                    except ValueError:
                        sub_group[column] = np.nan
                else:
                    try:
                        sub_group[column] = max(sub_group[column].dropna().astype(str), key=len)
                    except ValueError:
                        sub_group[column] = np.nan
            df_oclc_deduplicated = df_oclc_deduplicated.append(sub_group)
            
        df_oclc_deduplicated = df_oclc_deduplicated.drop_duplicates().replace(r'^\s*$', np.nan, regex=True)
        df_oclc_deduplicated['001'] = df_oclc_deduplicated['001'].astype(object).astype('str')
        
        group = group[~group['001'].isin(oclc_duplicates_list)]
        group = pd.concat([group, df_oclc_deduplicated])
        group['group_ids'] = group['group_ids'].apply(lambda x: '❦'.join(set(x.split('❦'))) if pd.notnull(x) else x)
        group = group.drop_duplicates().reset_index(drop=True)
        phase_3 = phase_3.append(group)
    else:
        group.drop(columns='title', inplace=True)
        group = group.drop_duplicates().reset_index(drop=True)
        phase_3 = phase_3.append(group)
    
#phase_4: editions counter
    # edition_clusters = cluster_strings(group['title'], 0.7)
    # edition_clusters_df = pd.DataFrame()
    # for k, v in edition_clusters.items():
    #     df = group.copy()[group['title'].str.strip().isin(v)]
    #     df['edition_cluster'] = k
    #     edition_clusters_df = edition_clusters_df.append(df)
    # edition_clusters_df['edition_index'] = edition_clusters_df.groupby('edition_cluster').cumcount()+1
    # group = edition_clusters_df.copy()
    # phase_4 = phase_4.append(group)
    
#phase_5: simplify the records
    # group = group[['001', '080', '100', '245', '240', '260', '650', '655', '700', 'language', 'fiction_type', 'place', 'year', 'edition_cluster', 'edition_index']]
    # group['001'] = group['001'].astype(int)
    
    # identifiers = group[['001']]
    # udc = marc_parser_1_field(group, '001', '080', '\$')[['001', '$a']].rename(columns={'$a':'universal decimal classification'})
    # udc['universal decimal classification'] = udc.groupby('001')['universal decimal classification'].transform(lambda x: '❦'.join(x.dropna().drop_duplicates().astype(str)))
    # udc = udc.drop_duplicates().reset_index(drop=True)
    # marc_author = marc_parser_1_field(group, '001', '100', '\$')[['001', '$a', '$d', '$1']].rename(columns={'$a':'author name', '$d':'author birth and death', '$1':'author viaf id'})
    # for column in marc_author.columns[1:]:
    #     marc_author[column] = marc_author.groupby('001')[column].transform(lambda x: '❦'.join(x.dropna().drop_duplicates().astype(str)))
    # marc_author = marc_author.drop_duplicates().reset_index(drop=True)
    # title = marc_parser_1_field(group, '001', '245', '\$').replace(r'^\s*$', np.nan, regex=True)
    # title['$a'] = title.apply(lambda x: x['245'] if pd.isnull(x['$a']) else x['$a'], axis=1)
    # title = title.iloc[:, lambda x: x.columns.isin(['001', '$a', '$b'])]
    # title['title'] = title[title.columns[1:]].apply(lambda x: ''.join(x.dropna().astype(str)), axis=1)
    # title = title[['001', 'title']]
    # original_title = marc_parser_1_field(group, '001', '240', '\$').replace(r'^\s*$', np.nan, regex=True)[['001', '$a']].rename(columns={'$a':'original title'})
    # place_of_publication = marc_parser_1_field(group, '001', '260', '\$').replace(r'^\s*$', np.nan, regex=True)[['001', '$a']].rename(columns={'$a':'place of publication'})
    # #$e as alternative place of publication?
    # try:
    #     contributor = marc_parser_1_field(group, '001', '700', '\$').replace(r'^\s*$', np.nan, regex=True)[['001', '$a', '$d', '$e', '$1']].rename(columns={'$a':'contributor name', '$d':'contributor birth and death', '$1':'contributor viaf id', '$e':'contributor role'})
    #     contributor['contributor role'] = contributor['contributor role'].apply(lambda x: x if pd.notnull(x) else 'unknown')
    # except KeyError:
    #     contributor['contributor role'] = 'unknown'
        
    # dfs = [identifiers, udc, marc_author, title, original_title, contributor, group[['001', '650', '655', 'language', 'fiction_type', 'place', 'year', 'edition_cluster', 'edition_index']]]
    # df_oclc_final = reduce(lambda left,right: pd.merge(left,right,on='001', how='outer'), dfs).drop_duplicates()
    # phase_5 = phase_5.append(group)
         
phase_1.to_excel(writer, index=False, sheet_name='phase_1')    
phase_2.to_excel(writer, index=False, sheet_name='phase_2')  
phase_3.drop(columns='title').drop_duplicates().to_excel(writer, index=False, sheet_name='phase_3') 
phase_4.to_excel(writer, index=False, sheet_name='phase_4') 
phase_5.to_excel(writer, index=False, sheet_name='phase_5') 
writer.save()
writer.close()    
    
#%% HQ records: de-duplication - przystosować - jest w chunku wyżej - NIEAKTUALNE
fiction_types = ['1', 'd', 'f', 'h', 'j', 'p']
languages = ['pol', 'swe', 'ita', 'spa']
#languages = ['ita']

now = datetime.datetime.now()

for language in languages:
    print(language)
    df = oclc_other_languages[(oclc_other_languages['language'] == language)]
    df_language_materials_monographs = df[df['type of record + bibliographic level'] == 'am']
    negative = df_language_materials_monographs.copy()
    df_other_types = df[~df['001'].isin(df_language_materials_monographs['001'])]
    #df_other_types.to_excel(f"oclc_{language}_other_types_(first_negative).xlsx", index=False)
    df_first_positive = df_language_materials_monographs[(df_language_materials_monographs['041'].str.contains('\$hcz')) &
                                                         (df_language_materials_monographs['fiction_type'].isin(fiction_types))]
    negative = negative[~negative['001'].isin(df_first_positive['001'])]
    #df_first_positive.to_excel(f"oclc_{language}_positive.xlsx", index=False)
    df_second_positive = marc_parser_1_field(negative, '001', '100', '\$')[['001', '$1']]
    df_second_positive = df_second_positive[df_second_positive['$1'].isin(viaf_positives)]
    df_second_positive = negative[negative['001'].isin(df_second_positive['001'])]
    #df_second_positive.to_excel(f"oclc_{language}_second_positive.xlsx", index=False)
    negative = negative[~negative['001'].isin(df_second_positive['001'])].reset_index(drop=True)
    
    df_third_positive = "select * from negative a join positive_viafs_names b on a.'100' like '%'||b.all_names||'%'"
    df_third_positive = pandasql.sqldf(df_third_positive)
    
    negative = negative[~negative['001'].isin(df_third_positive['001'])].reset_index(drop=True)

    df_fourth_positive = "select * from negative a join positive_viafs_diacritics b on a.'100' like '%'||b.cz_name||'%'"
    df_fourth_positive = pandasql.sqldf(df_fourth_positive)

    negative = negative[~negative['001'].isin(df_fourth_positive['001'])].reset_index(drop=True)
    #negative.to_excel(f"oclc_{language}_negative.xlsx", index=False)
    
    df_all_positive = pd.concat([df_first_positive, df_second_positive, df_third_positive, df_fourth_positive])
    #df_all_positive.to_excel(f"oclc_{language}_df_all_positive.xlsx", index=False)
    
    df_all_positive['260'] = df_all_positive[['260', '264']].apply(lambda x: x['260'] if pd.notnull(x['260']) else x['264'], axis=1)
    df_all_positive['240'] = df_all_positive[['240', '246']].apply(lambda x: x['240'] if pd.notnull(x['240']) else x['246'], axis=1)
    df_all_positive['100_unidecode'] = df_all_positive['100'].apply(lambda x: unidecode.unidecode(x).lower() if pd.notnull(x) else x)
    
    authors = ['Hasek', 
               'Hrabal', 
               'Capek']
    
    year = now.year
    month = now.month
    day = now.day

    
    for i, author in enumerate(authors):
        sheet = gc.create(f'{author}_{language}_{year}-{month}-{day}', translation_folder)
        s = gp.Spread(sheet.id, creds=credentials)
        authors[i] = [authors[i]]
        authors[i] += [sheet.id, s]
    
    for index, (author, g_id, g_sheet) in enumerate(authors):
        # author = authors[0][0]
        # g_id = authors[0][1]
        # g_sheet = authors[0][2]
        print(f"{index+1}/{len(authors)}")
        
        #all
        
        df_oclc = df_all_positive[(df_all_positive['100_unidecode'].notnull()) &       
                                  (df_all_positive['100_unidecode'].str.contains(author.lower()))].reset_index(drop=True)
        df_oclc['001'] = df_oclc['001'].astype(int)
        sh = gc.open_by_key(g_id)
        wsh = sh.get_worksheet(0)
        wsh.update_title('all')
        g_sheet.df_to_sheet(df_oclc, sheet='all', index=0)
        
        #de-duplication 1: duplicates
        try:
            title = marc_parser_1_field(df_oclc, '001', '245', '\$')[['001', '$a', '$b', '$n', '$p']].replace(r'^\s*$', np.nan, regex=True)
        except KeyError:
            title = marc_parser_1_field(df_oclc, '001', '245', '\$')[['001', '$a', '$b']].replace(r'^\s*$', np.nan, regex=True)
        title['title'] = title[title.columns[1:]].apply(lambda x: simplify_string(x, with_spaces=False), axis=1)    
        title = title[['001', 'title']]
        df_oclc = pd.merge(df_oclc, title, how='left', on='001')
        
        place = marc_parser_1_field(df_oclc, '001', '260', '\$')[['001', '$a']].rename(columns={'$a':'place'})
        place = place[place['place'] != '']
        place['place'] = place['place'].apply(lambda x: simplify_string(x, with_spaces=False))
        df_oclc = pd.merge(df_oclc, place, how='left', on='001')
        
        publisher = marc_parser_1_field(df_oclc, '001', '260', '\$')[['001', '$b']].rename(columns={'$b':'publisher'})
        publisher = publisher.groupby('001').head(1).reset_index(drop=True)
        publisher['publisher'] = publisher['publisher'].apply(lambda x: simplify_string(x, with_spaces=False))
        df_oclc = pd.merge(df_oclc, publisher, how='left', on='001')
        
        
        year = df_oclc.copy()[['001', '008']].rename(columns={'008':'year'})
        year['year'] = year['year'].apply(lambda x: x[7:11])
        df_oclc = pd.merge(df_oclc, year, how='left', on='001')
        
        df_oclc_duplicates = pd.DataFrame()
        df_oclc_grouped = df_oclc.groupby(['title', 'place', 'year'])
        for name, group in df_oclc_grouped:
            if len(group) > 1:
                group['groupby'] = str(name)
                group_ids = '❦'.join([str(e) for e in group['001'].to_list()])
                group['group_ids'] = group_ids
                df_oclc_duplicates = df_oclc_duplicates.append(group)
        df_oclc_duplicates = df_oclc_duplicates.drop_duplicates()
        
        oclc_duplicates_list = df_oclc_duplicates['001'].drop_duplicates().tolist()
        df_oclc_duplicates_grouped = df_oclc_duplicates.groupby(['title', 'place', 'year'])
        
        df_oclc_deduplicated = pd.DataFrame()
        for name, group in df_oclc_duplicates_grouped:
            for column in group:
                if column in ['fiction_type', '490', '500', '650', '655']:
                    group[column] = '❦'.join(group[column].dropna().drop_duplicates().astype(str))
                else:
                    try:
                        group[column] = max(group[column].dropna().astype(str), key=len)
                    except ValueError:
                        group[column] = np.nan
            df_oclc_deduplicated = df_oclc_deduplicated.append(group)
        
        df_oclc_deduplicated = df_oclc_deduplicated.drop_duplicates().replace(r'^\s*$', np.nan, regex=True)
        df_oclc_deduplicated['001'] = df_oclc_deduplicated['001'].astype(int)
        
        df_oclc = df_oclc[~df_oclc['001'].isin(oclc_duplicates_list)]
        df_oclc = pd.concat([df_oclc, df_oclc_deduplicated]).drop(columns='title')
        df_oclc['group_ids'] = df_oclc['group_ids'].apply(lambda x: '❦'.join(set(x.split('❦'))) if pd.notnull(x) else x)
        g_sheet.df_to_sheet(df_oclc, sheet='after_removing_duplicates', index=0)
        
        #de-duplication 2: multiple volumes
        
        title = marc_parser_1_field(df_oclc, '001', '245', '\$')[['001', '$a']].replace(r'^\s*$', np.nan, regex=True)
        title['title'] = title[title.columns[1:]].apply(lambda x: simplify_string(x, with_spaces=False), axis=1)    
        title = title[['001', 'title']]
        df_oclc = pd.merge(df_oclc, title, how='left', on='001')  
        
        df_oclc_grouped = df_oclc.groupby(['title', 'place', 'year'])
            
        df_oclc_multiple_volumes = pd.DataFrame()
        for name, group in df_oclc_grouped:
            if len(group[group['245'].str.contains('\$n', regex=True)]):
                group['groupby'] = str(name)
                group_ids = '❦'.join(set([str(e) for e in group['001'].to_list() + group['group_ids'].to_list() if pd.notnull(e)]))
                group['group_ids'] = group_ids
                df_oclc_multiple_volumes = df_oclc_multiple_volumes.append(group)
                
        if df_oclc_multiple_volumes.shape[0] > 0:
            oclc_multiple_volumes_list = df_oclc_multiple_volumes['001'].drop_duplicates().tolist()
            df_oclc_multiple_volumes_grouped = df_oclc_multiple_volumes.groupby(['title', 'place', 'year'])
        
            df_oclc_multiple_volumes_deduplicated = pd.DataFrame()
            for name, group in df_oclc_multiple_volumes_grouped:
                if len(group[~group['245'].str.contains('\$n', regex=True)]) == 1:
                    for column in group:
                        if column in ['fiction_type', '490', '500', '650', '655']:
                            group[column] = '❦'.join(group[column].dropna().drop_duplicates().astype(str))  
                        elif column in ['001', '245']:
                            pass
                        else:
                            try:
                                group[column] = max(group[column].dropna().astype(str), key=len)
                            except ValueError:
                                group[column] = np.nan
                    df = group[~group['245'].str.contains('\$n', regex=True)]
                    df_oclc_multiple_volumes_deduplicated = df_oclc_multiple_volumes_deduplicated.append(df)
                else:
                    for column in group:
                        if column in ['fiction_type', '490', '500', '650', '655']:
                            group[column] = '❦'.join(group[column].dropna().drop_duplicates().astype(str))
                        elif column == '245':
                            field_245 = marc_parser_1_field(group, '001', '245', '\$').replace(r'^\s*$', np.nan, regex=True)
                            field_245 = field_245.iloc[:, lambda x: x.columns.isin(['001', '$a', '$b', '$c'])]
                            field_245['245'] = field_245[field_245.columns[1:]].apply(lambda x: ''.join(x.dropna().astype(str)), axis=1)
                            field_245 = field_245[['001', '245']]
                            field_245['245'] = '10' + field_245['245']
                            group = pd.merge(group.drop(columns='245'), field_245, how='left', on='001')
                            try:
                                group[column] = max(group[column].dropna().astype(str), key=len)
                            except ValueError:
                                group[column] = np.nan
                        else:
                            try:
                                group[column] = max(group[column].dropna().astype(str), key=len)
                            except ValueError:
                                group[column] = np.nan
                    group = group.drop_duplicates().reset_index(drop=True)
                    df_oclc_multiple_volumes_deduplicated = df_oclc_multiple_volumes_deduplicated.append(group)
                        
            df_oclc_multiple_volumes_deduplicated = df_oclc_multiple_volumes_deduplicated.drop_duplicates().replace(r'^\s*$', np.nan, regex=True)
            df_oclc_multiple_volumes_deduplicated['001'] = df_oclc_multiple_volumes_deduplicated['001'].astype(int)
            
            df_oclc = df_oclc[~df_oclc['001'].isin(oclc_multiple_volumes_list)]
            df_oclc = pd.concat([df_oclc, df_oclc_multiple_volumes_deduplicated]).drop_duplicates().reset_index(drop=True)
            df_oclc['group_ids'] = df_oclc['group_ids'].apply(lambda x: '❦'.join(set(x.split('❦'))) if pd.notnull(x) else x)
            g_sheet.df_to_sheet(df_oclc, sheet='after_removing_multiple_volumes', index=0)
            
        #de-duplication 3: fuzzyness
        df_oclc.drop(columns='title', inplace=True)
        field_245 = marc_parser_1_field(df_oclc, '001', '245', '\$').replace(r'^\s*$', np.nan, regex=True)
        field_245['$a'] = field_245.apply(lambda x: x['$a'] if pd.notnull(x['$a']) else x['indicator'][2:].split('.', 1)[0], axis=1)
        field_245 = field_245.iloc[:, lambda x: x.columns.isin(['001', '$a', '$b'])]
        field_245['title'] = field_245[field_245.columns[1:]].apply(lambda x: simplify_string(x), axis=1)  
        field_245 = field_245[['001', 'title']]
        df_oclc = pd.merge(df_oclc, field_245, how='left', on='001')
        
        #similarity level == 0.85 | columns == ['title', 'publisher', 'year'] | same 'year'
        df_oclc_clusters = cluster_records(df_oclc, '001', ['title', 'publisher', 'year'], 0.85)    
        df_oclc_clusters = df_oclc_clusters[df_oclc_clusters['publisher'] != '']
        df_oclc_duplicates = df_oclc_clusters.groupby(['cluster', 'year']).filter(lambda x: len(x) > 1)
        
        if df_oclc_duplicates.shape[0] > 0:
     
            if df_oclc_duplicates['001'].value_counts().max() > 1:
                sys.exit('ERROR!!!\nclustering problem!!!')
        
            oclc_duplicates_list = df_oclc_duplicates['001'].drop_duplicates().tolist()
            df_oclc_duplicates = df_oclc_duplicates.groupby('cluster')
            
            df_oclc_deduplicated = pd.DataFrame()
            for name, group in df_oclc_duplicates:
                group_ids = '❦'.join(set([str(e) for e in group['001'].to_list() + group['group_ids'].to_list() if pd.notnull(e)]))
                group['group_ids'] = group_ids
                for column in group:
                    if column in ['fiction_type', '490', '500', '650', '655']:
                        group[column] = '❦'.join(group[column].dropna().drop_duplicates().astype(str))
                    elif column == '245':
                        group[column] = group[column][group[column].str.contains('$', regex=False)]
                        try:
                            group[column] = max(group[column].dropna().astype(str), key=len)
                        except ValueError:
                            group[column] = np.nan
                    else:
                        try:
                            group[column] = max(group[column].dropna().astype(str), key=len)
                        except ValueError:
                            group[column] = np.nan
                df_oclc_deduplicated = df_oclc_deduplicated.append(group)
                
            df_oclc_deduplicated = df_oclc_deduplicated.drop_duplicates().replace(r'^\s*$', np.nan, regex=True)
            df_oclc_deduplicated['001'] = df_oclc_deduplicated['001'].astype(int)
            df_oclc = df_oclc[~df_oclc['001'].isin(oclc_duplicates_list)]
            df_oclc = pd.concat([df_oclc, df_oclc_deduplicated])
            df_oclc['group_ids'] = df_oclc['group_ids'].apply(lambda x: '❦'.join(set(x.split('❦'))) if pd.notnull(x) else x)
            g_sheet.df_to_sheet(df_oclc, sheet='after_fuzzy_duplicates_0.85_tit_pub_year', index=0)
            
    # =============================================================================
    #     #similarity level == 0.8 | columns == ['title', 'publisher'] | same 'year'
    #     df_oclc.drop(columns='cluster', inplace=True)
    #     df_oclc_clusters = cluster_records(df_oclc, '001', ['title', 'place', 'publisher', 'year'], 0.85) 
    #     df_oclc_duplicates = df_oclc_clusters.groupby(['cluster', 'year']).filter(lambda x: len(x) > 1)
    #     
    #     df_oclc_duplicates[['001', '245', 'year', '260']].to_excel('test_oclc1.xlsx', index=False)
    #     
    #     if df_oclc_duplicates.shape[0] > 0:
    #     
    #         if df_oclc_duplicates['001'].value_counts().max() > 1:
    #             sys.exit('ERROR!!!\nclustering problem!!!')
    #     
    #         oclc_duplicates_list = df_oclc_duplicates['001'].drop_duplicates().tolist()
    #         df_oclc_duplicates = df_oclc_duplicates.groupby('cluster')
    #         
    #         df_oclc_deduplicated = pd.DataFrame()
    #         for name, group in df_oclc_duplicates:
    #             group_ids = '❦'.join(set([str(e) for e in group['001'].to_list() + group['group_ids'].to_list() if pd.notnull(e)]))
    #             group['group_ids'] = group_ids
    #             for column in group:
    #                 if column in ['fiction_type', '490', '500', '650', '655']:
    #                     group[column] = '❦'.join(group[column].dropna().drop_duplicates().astype(str))
    #                 elif column == '245':
    #                     group[column] = group[column][group[column].str.contains('$', regex=False)]
    #                     try:
    #                           group[column] = max(group[column].dropna().astype(str), key=len)
    #                     except ValueError:
    #                           group[column] = np.nan
    #                 else:
    #                     try:
    #                           group[column] = max(group[column].dropna().astype(str), key=len)
    #                     except ValueError:
    #                           group[column] = np.nan
    #             df_oclc_deduplicated = df_oclc_deduplicated.append(group)
    #             
    #         df_oclc_deduplicated = df_oclc_deduplicated.drop_duplicates().replace(r'^\s*$', np.nan, regex=True)
    #         df_oclc_deduplicated['001'] = df_oclc_deduplicated['001'].astype(int)
    #         df_oclc = df_oclc[~df_oclc['001'].isin(oclc_duplicates_list)]
    #         df_oclc = pd.concat([df_oclc, df_oclc_deduplicated])
    #         df_oclc['group_ids'] = df_oclc['group_ids'].apply(lambda x: '❦'.join(set(x.split('❦'))) if pd.notnull(x) else x)
    #         g_sheet.df_to_sheet(df_oclc, sheet='after_fuzzy_duplicates_0.85_tit_pub_year', index=0)
    # =============================================================================
        
        #editions counter
        edition_clusters = cluster_strings(df_oclc['title'], 0.7)
        edition_clusters_df = pd.DataFrame()
        for k, v in edition_clusters.items():
            df = df_oclc.copy()[df_oclc['title'].str.strip().isin(v)]
            df['edition_cluster'] = k
            edition_clusters_df = edition_clusters_df.append(df)
        edition_clusters_df['edition_index'] = edition_clusters_df.groupby('edition_cluster').cumcount()+1
        df_oclc = edition_clusters_df.copy()
        g_sheet.df_to_sheet(df_oclc, sheet='final_marc21_with_editions_counters', index=0)
           
        #simplify the records
        df_oclc = df_oclc[['001', '080', '100', '245', '240', '260', '650', '655', '700', 'language', 'fiction_type', 'place', 'year', 'edition_cluster', 'edition_index']]
        df_oclc['001'] = df_oclc['001'].astype(int)
        
        identifiers = df_oclc[['001']]
        udc = marc_parser_1_field(df_oclc, '001', '080', '\$')[['001', '$a']].rename(columns={'$a':'universal decimal classification'})
        udc['universal decimal classification'] = udc.groupby('001')['universal decimal classification'].transform(lambda x: '❦'.join(x.dropna().drop_duplicates().astype(str)))
        udc = udc.drop_duplicates().reset_index(drop=True)
        marc_author = marc_parser_1_field(df_oclc, '001', '100', '\$')[['001', '$a', '$d', '$1']].rename(columns={'$a':'author name', '$d':'author birth and death', '$1':'author viaf id'})
        for column in marc_author.columns[1:]:
            marc_author[column] = marc_author.groupby('001')[column].transform(lambda x: '❦'.join(x.dropna().drop_duplicates().astype(str)))
        marc_author = marc_author.drop_duplicates().reset_index(drop=True)
        title = marc_parser_1_field(df_oclc, '001', '245', '\$').replace(r'^\s*$', np.nan, regex=True)
        title['$a'] = title.apply(lambda x: x['245'] if pd.isnull(x['$a']) else x['$a'], axis=1)
        title = title.iloc[:, lambda x: x.columns.isin(['001', '$a', '$b'])]
        title['title'] = title[title.columns[1:]].apply(lambda x: ''.join(x.dropna().astype(str)), axis=1)
        title = title[['001', 'title']]
        original_title = marc_parser_1_field(df_oclc, '001', '240', '\$').replace(r'^\s*$', np.nan, regex=True)[['001', '$a']].rename(columns={'$a':'original title'})
        place_of_publication = marc_parser_1_field(df_oclc, '001', '260', '\$').replace(r'^\s*$', np.nan, regex=True)[['001', '$a']].rename(columns={'$a':'place of publication'})
        #$e as alternative place of publication?
        try:
            contributor = marc_parser_1_field(df_oclc, '001', '700', '\$').replace(r'^\s*$', np.nan, regex=True)[['001', '$a', '$d', '$e', '$1']].rename(columns={'$a':'contributor name', '$d':'contributor birth and death', '$1':'contributor viaf id', '$e':'contributor role'})
            contributor['contributor role'] = contributor['contributor role'].apply(lambda x: x if pd.notnull(x) else 'unknown')
        except KeyError:
            contributor['contributor role'] = 'unknown'
            
        dfs = [identifiers, udc, marc_author, title, original_title, contributor, df_oclc[['001', '650', '655', 'language', 'fiction_type', 'place', 'year', 'edition_cluster', 'edition_index']]]
        df_oclc_final = reduce(lambda left,right: pd.merge(left,right,on='001', how='outer'), dfs).drop_duplicates()
        g_sheet.df_to_sheet(df_oclc_final, sheet='simplified shape', index=0)
        time.sleep(60)
        
        






















# =============================================================================
# df_original_titles_simple.to_excel('cluster_original_titles_0.8_author_clusters.xlsx', index=False)
# 
# 
# df_08 = df_original_titles_simple.copy()
# df_07 = df_original_titles_simple.copy().sort_values(['cluster_viaf', 'cluster', 'original title'])  
# 
# df_07.to_excel('cluster_original_titles_0.7_author_clusters.xlsx', index=False)
# df_08.to_excel('cluster_original_titles_0.8_author_clusters.xlsx', index=False)
# =============================================================================



#%% scenariusz - najpierw grupy autorów i tytułów, a potem praca na danych bibliograficznych

#PROBLEM - to tylko rekordy z tytułami oryginalnymi - trzeba zgarnąć wszystkie



#tylko dla rekordów z oryginalnym tytułem
# writer = pd.ExcelWriter('clusters_deduplication_trial.xlsx', engine = 'xlsxwriter')
# test = pd.merge(df_all_positive.drop(columns=['viaf_positive', 'all_names', 'cz_name']), df_original_titles_simple.drop(columns='index'), how='inner', on='001')

#dla wszystkich rekordów z podziałem na clustry viaf

# df_people_clusters['001'] = df_people_clusters['001'].astype('int64')
# test = pd.merge(df_people_clusters, df_original_titles_simple.drop(columns='index'), how='left', on=['001', 'cluster_viaf'])

test = correct.copy()
#test.columns.values
file_names = []
test_full = test.groupby('cluster_viaf')
for name, test in tqdm(test_full, total=len(test_full)):
    #if name == '52272':
    #test = test_full.get_group('52272')
    if True:
        file_name = f'clusters_deduplication_trial_full_viaf_{name}.xlsx'
        file_names.append(file_name)
        writer = pd.ExcelWriter(file_name, engine = 'xlsxwriter')
        test.to_excel(writer, index=False, sheet_name='clusters_viaf_titles')
            
        #de-duplication 1: duplicates
        try:
            title = marc_parser_1_field(test, '001', '245', '\$')[['001', '$a', '$b', '$n', '$p']].replace(r'^\s*$', np.nan, regex=True)
        except KeyError:
            title = marc_parser_1_field(test, '001', '245', '\$')[['001', '$a', '$b']].replace(r'^\s*$', np.nan, regex=True)
        title['title'] = title[title.columns[1:]].apply(lambda x: simplify_string(x, with_spaces=False), axis=1)    
        title = title[['001', 'title']]
        test = pd.merge(test, title, how='left', on='001')
        
        place = marc_parser_1_field(test, '001', '260', '\$')[['001', '$a']].rename(columns={'$a':'place'})
        place = place[place['place'] != '']
        place['place'] = place['place'].apply(lambda x: simplify_string(x, with_spaces=False))
        test = pd.merge(test, place, how='left', on='001')
        
        publisher = marc_parser_1_field(test, '001', '260', '\$')[['001', '$b']].rename(columns={'$b':'publisher'})
        publisher = publisher.groupby('001').head(1).reset_index(drop=True)
        publisher['publisher'] = publisher['publisher'].apply(lambda x: simplify_string(x, with_spaces=False))
        test = pd.merge(test, publisher, how='left', on='001')
        
        year = test.copy()[['001', '008']].rename(columns={'008':'year'})
        year['year'] = year['year'].apply(lambda x: x[7:11])
        test = pd.merge(test, year, how='left', on='001')
        test = test.drop_duplicates().reset_index(drop=False)
        
        df_oclc_duplicates = pd.DataFrame()
        df_oclc_grouped = test.groupby(['title', 'place', 'year'])
        for name, group in df_oclc_grouped:
            if len(group) > 1:
                group['groupby'] = str(name)
                group_ids = '❦'.join([str(e) for e in group['001'].to_list()])
                group['group_ids'] = group_ids
                df_oclc_duplicates = df_oclc_duplicates.append(group)
        df_oclc_duplicates = df_oclc_duplicates.drop_duplicates()
        
        oclc_duplicates_list = df_oclc_duplicates['001'].drop_duplicates().tolist()
        df_oclc_duplicates_grouped = df_oclc_duplicates.groupby(['title', 'place', 'year'])
        
        df_oclc_deduplicated = pd.DataFrame()
        for name, group in df_oclc_duplicates_grouped:
            # group = df_oclc_duplicates_grouped.get_group(('rudarskabalada', 'zagreb', '1948'))
            for column in group:
                if column in ['fiction_type', '490', '500', '650', '655']:
                    group[column] = '❦'.join(group[column].dropna().drop_duplicates().astype(str))
                else:
                    try:
                        group[column] = max(group[column].dropna().astype(str), key=len)
                    except ValueError:
                        group[column] = np.nan
            df_oclc_deduplicated = df_oclc_deduplicated.append(group)
        
        df_oclc_deduplicated = df_oclc_deduplicated.drop_duplicates().replace(r'^\s*$', np.nan, regex=True)
        df_oclc_deduplicated['001'] = df_oclc_deduplicated['001'].astype(int)
        
        test = test[~test['001'].isin(oclc_duplicates_list)]
        test = pd.concat([test, df_oclc_deduplicated]).drop(columns='title')
        test['group_ids'] = test['group_ids'].apply(lambda x: '❦'.join(set(x.split('❦'))) if pd.notnull(x) else x)
        test.to_excel(writer, index=False, sheet_name='after_removing_duplicates')
        
        #de-duplication 2: multiple volumes
        
        title = marc_parser_1_field(test, '001', '245', '\$')[['001', '$a']].replace(r'^\s*$', np.nan, regex=True)
        title['title'] = title[title.columns[1:]].apply(lambda x: simplify_string(x, with_spaces=False), axis=1)    
        title = title[['001', 'title']]
        test = pd.merge(test, title, how='left', on='001')  
        
        df_oclc_grouped = test.groupby(['title', 'place', 'year'])
            
        df_oclc_multiple_volumes = pd.DataFrame()
        for name, group in df_oclc_grouped:
            if len(group[group['245'].str.contains('\$n', regex=True)]):
                group['groupby'] = str(name)
                group_ids = '❦'.join(set([str(e) for e in group['001'].to_list() + group['group_ids'].to_list() if pd.notnull(e)]))
                group['group_ids'] = group_ids
                df_oclc_multiple_volumes = df_oclc_multiple_volumes.append(group)
                
        if df_oclc_multiple_volumes.shape[0] > 0:
            oclc_multiple_volumes_list = df_oclc_multiple_volumes['001'].drop_duplicates().tolist()
            df_oclc_multiple_volumes_grouped = df_oclc_multiple_volumes.groupby(['title', 'place', 'year'])
        
            df_oclc_multiple_volumes_deduplicated = pd.DataFrame()
            for name, group in df_oclc_multiple_volumes_grouped:
                if len(group[~group['245'].str.contains('\$n', regex=True)]) == 1:
                    for column in group:
                        if column in ['fiction_type', '490', '500', '650', '655']:
                            group[column] = '❦'.join(group[column].dropna().drop_duplicates().astype(str))  
                        elif column in ['001', '245']:
                            pass
                        else:
                            try:
                                group[column] = max(group[column].dropna().astype(str), key=len)
                            except ValueError:
                                group[column] = np.nan
                    df = group[~group['245'].str.contains('\$n', regex=True)]
                    df_oclc_multiple_volumes_deduplicated = df_oclc_multiple_volumes_deduplicated.append(df)
                else:
                    for column in group:
                        if column in ['fiction_type', '490', '500', '650', '655']:
                            group[column] = '❦'.join(group[column].dropna().drop_duplicates().astype(str))
                        elif column == '245':
                            field_245 = marc_parser_1_field(group, '001', '245', '\$').replace(r'^\s*$', np.nan, regex=True)
                            field_245 = field_245.iloc[:, lambda x: x.columns.isin(['001', '$a', '$b', '$c'])]
                            field_245['245'] = field_245[field_245.columns[1:]].apply(lambda x: ''.join(x.dropna().astype(str)), axis=1)
                            field_245 = field_245[['001', '245']]
                            field_245['245'] = '10' + field_245['245']
                            group = pd.merge(group.drop(columns='245'), field_245, how='left', on='001')
                            try:
                                group[column] = max(group[column].dropna().astype(str), key=len)
                            except ValueError:
                                group[column] = np.nan
                        else:
                            try:
                                group[column] = max(group[column].dropna().astype(str), key=len)
                            except ValueError:
                                group[column] = np.nan
                    group = group.drop_duplicates().reset_index(drop=True)
                    df_oclc_multiple_volumes_deduplicated = df_oclc_multiple_volumes_deduplicated.append(group)
                        
            df_oclc_multiple_volumes_deduplicated = df_oclc_multiple_volumes_deduplicated.drop_duplicates().replace(r'^\s*$', np.nan, regex=True)
            df_oclc_multiple_volumes_deduplicated['001'] = df_oclc_multiple_volumes_deduplicated['001'].astype(int)
            
            test = test[~test['001'].isin(oclc_multiple_volumes_list)]
            test = pd.concat([test, df_oclc_multiple_volumes_deduplicated]).drop_duplicates().reset_index(drop=True)
            test['group_ids'] = test['group_ids'].apply(lambda x: '❦'.join(set(x.split('❦'))) if pd.notnull(x) else x)
            test.to_excel(writer, index=False, sheet_name='after_removing_multiple_volumes')
            
        #de-duplication 3: fuzzyness
        test.drop(columns='title', inplace=True)
        field_245 = marc_parser_1_field(test, '001', '245', '\$').replace(r'^\s*$', np.nan, regex=True)
        field_245['$a'] = field_245.apply(lambda x: x['$a'] if pd.notnull(x['$a']) else x['indicator'][2:].split('.', 1)[0], axis=1)
        field_245 = field_245.iloc[:, lambda x: x.columns.isin(['001', '$a', '$b'])]
        field_245['title'] = field_245[field_245.columns[1:]].apply(lambda x: simplify_string(x), axis=1)  
        field_245 = field_245[['001', 'title']]
        test = pd.merge(test, field_245, how='left', on='001')
        
        #similarity level == 0.85 | columns == ['title', 'publisher', 'year'] | same 'year'
        df_oclc_clusters = cluster_records(test, '001', ['title', 'publisher', 'year'], 0.85)    
        df_oclc_clusters = df_oclc_clusters[df_oclc_clusters['publisher'] != '']
        df_oclc_duplicates = df_oclc_clusters.groupby(['cluster', 'year']).filter(lambda x: len(x) > 1)
        
        if df_oclc_duplicates.shape[0] > 0:
         
            # if df_oclc_duplicates['001'].value_counts().max() > 1:
            #     sys.exit('ERROR!!!\nclustering problem!!!')
        
            oclc_duplicates_list = df_oclc_duplicates['001'].drop_duplicates().tolist()
            df_oclc_duplicates = df_oclc_duplicates.groupby('cluster')
            
            df_oclc_deduplicated = pd.DataFrame()
            for name, group in df_oclc_duplicates:
                group_ids = '❦'.join(set([str(e) for e in group['001'].to_list() + group['group_ids'].to_list() if pd.notnull(e)]))
                group['group_ids'] = group_ids
                for column in group:
                    if column in ['fiction_type', '490', '500', '650', '655']:
                        group[column] = '❦'.join(group[column].dropna().drop_duplicates().astype(str))
                    elif column == '245':
                        group[column] = group[column][group[column].str.contains('$', regex=False)]
                        try:
                            group[column] = max(group[column].dropna().astype(str), key=len)
                        except ValueError:
                            group[column] = np.nan
                    else:
                        try:
                            group[column] = max(group[column].dropna().astype(str), key=len)
                        except ValueError:
                            group[column] = np.nan
                df_oclc_deduplicated = df_oclc_deduplicated.append(group)
                
            df_oclc_deduplicated = df_oclc_deduplicated.drop_duplicates().replace(r'^\s*$', np.nan, regex=True)
            df_oclc_deduplicated['001'] = df_oclc_deduplicated['001'].astype(int)
            test = test[~test['001'].isin(oclc_duplicates_list)]
            test = pd.concat([test, df_oclc_deduplicated])
            test['group_ids'] = test['group_ids'].apply(lambda x: '❦'.join(set(x.split('❦'))) if pd.notnull(x) else x)
            
            test.to_excel(writer, index=False, sheet_name='after_fuzzy_duplicates_0.85')
        
        #editions counter
        test.drop(columns='title', inplace=True)
        field_245 = marc_parser_1_field(test, '001', '245', '\$').replace(r'^\s*$', np.nan, regex=True)
        field_245['$a'] = field_245.apply(lambda x: x['$a'] if pd.notnull(x['$a']) else x['indicator'][2:].split('.', 1)[0], axis=1)
        field_245 = field_245.iloc[:, lambda x: x.columns.isin(['001', '$a', '$b'])]
        field_245['title'] = field_245[field_245.columns[1:]].apply(lambda x: simplify_string(x), axis=1)  
        field_245 = field_245[['001', 'title']]
        test = pd.merge(test, field_245, how='left', on='001')
            
        edition_clusters = cluster_strings(test['title'].fillna(''), 0.7)
        edition_clusters_df = pd.DataFrame()
        for k, v in edition_clusters.items():
            df = test.copy()[test['title'].str.strip().isin(v)]
            df['edition_cluster'] = k
            edition_clusters_df = edition_clusters_df.append(df)
        edition_clusters_df['edition_index'] = edition_clusters_df.groupby('edition_cluster').cumcount()+1
        test = edition_clusters_df.copy()
        test.to_excel(writer, index=False, sheet_name='final_marc21_with_edition_count')
           
        #simplify the records
        test = test[['001', '080', '100', '245', '240', '260', '650', '655', '700', 'language', 'fiction_type', 'place', 'year', 'edition_cluster', 'edition_index']]
        test['001'] = test['001'].astype(int)
        
        identifiers = test[['001']]
        udc = marc_parser_1_field(test, '001', '080', '\$')[['001', '$a']].rename(columns={'$a':'universal decimal classification'})
        udc['universal decimal classification'] = udc.groupby('001')['universal decimal classification'].transform(lambda x: '❦'.join(x.dropna().drop_duplicates().astype(str)))
        udc = udc.drop_duplicates().reset_index(drop=True)
        marc_author = marc_parser_1_field(test, '001', '100', '\$')[['001', '$a', '$d', '$1']].rename(columns={'$a':'author name', '$d':'author birth and death', '$1':'author viaf id'})
        for column in marc_author.columns[1:]:
            marc_author[column] = marc_author.groupby('001')[column].transform(lambda x: '❦'.join(x.dropna().drop_duplicates().astype(str)))
        marc_author = marc_author.drop_duplicates().reset_index(drop=True)
        title = marc_parser_1_field(test, '001', '245', '\$').replace(r'^\s*$', np.nan, regex=True)
        title['$a'] = title.apply(lambda x: x['245'] if pd.isnull(x['$a']) else x['$a'], axis=1)
        title = title.iloc[:, lambda x: x.columns.isin(['001', '$a', '$b'])]
        title['title'] = title[title.columns[1:]].apply(lambda x: ''.join(x.dropna().astype(str)), axis=1)
        title = title[['001', 'title']]
        original_title = marc_parser_1_field(test, '001', '240', '\$').replace(r'^\s*$', np.nan, regex=True)[['001', '$a']].rename(columns={'$a':'original title'})
        place_of_publication = marc_parser_1_field(test, '001', '260', '\$').replace(r'^\s*$', np.nan, regex=True)[['001', '$a']].rename(columns={'$a':'place of publication'})
        #$e as alternative place of publication?
        try:
            contributor = marc_parser_1_field(test, '001', '700', '\$').replace(r'^\s*$', np.nan, regex=True)[['001', '$a', '$d', '$e', '$1']].rename(columns={'$a':'contributor name', '$d':'contributor birth and death', '$1':'contributor viaf id', '$e':'contributor role'})
            contributor['contributor role'] = contributor['contributor role'].apply(lambda x: x if pd.notnull(x) else 'unknown')
        except KeyError:
            contributor['contributor role'] = 'unknown'
            
        dfs = [identifiers, udc, marc_author, title, original_title, contributor, test[['001', '650', '655', 'language', 'fiction_type', 'place', 'year', 'edition_cluster', 'edition_index']]]
        df_oclc_final = reduce(lambda left,right: pd.merge(left,right,on='001', how='outer'), dfs).drop_duplicates()
        test.to_excel(writer, index=False, sheet_name='simplified shape')
        
        writer.save()
        writer.close()


#%% test dla Majerovej
from collections import Counter
import operator

for file_name in tqdm(file_names):
    viaf = re.findall('\d+', file_name)[0]

    test = pd.read_excel(file_name, sheet_name='final_marc21_with_edition_count')
    test.columns.values
    
    test_simple = test.copy()
    #[['index', '001', '100', '245', '240', 'language', 'name', 'dates', 'viaf', 'cluster_viaf', 'original title', 'cluster_titles', 'title', 'cluster', 'edition_cluster', 'edition_index', 'group_ids']]    
    
    test = test_simple.groupby('edition_cluster')
    
    test_simple_after = pd.DataFrame()
    for name, group in test:
        ttt = dict(Counter([e for e in group['cluster_titles'] if pd.notnull(e)]))
        try:
            cluster = int(max(ttt.items(), key=operator.itemgetter(1))[0])
        except ValueError:
            cluster = np.nan
        yyy = dict(Counter([e for e in group['original title'] if pd.notnull(e)]))
        try:
            original_title = max(yyy.items(), key=operator.itemgetter(1))[0]
        except ValueError:
            original_title = np.nan    
        group['cluster_titles suggestion'] = cluster
        group['original title suggestion'] = original_title
        test_simple_after = test_simple_after.append(group)
    
    test_simple_after = test_simple_after.sort_values('cluster_titles suggestion').drop(columns=['index', 'cluster'])
    test_simple_after.to_excel(f'{viaf}_cluster.xlsx', index=False)



# alternatywne podejście - budowa kartoteki osobowej na podstawie indeksu poprawności
fiction_types = ['1', 'd', 'f', 'h', 'j', 'p']
list_of_records = []
with open('F:/Cezary/Documents/IBL/Translations/OCLC/Czech origin_trans/oclc_lang.csv', 'r', encoding="utf8", errors="surrogateescape") as csv_file:
#with open('C:/Users/User/Desktop/oclc_lang.csv', 'r', encoding="utf8", errors="surrogateescape") as csv_file:
    reader = csv.reader(csv_file, delimiter=',')
    headers = next(reader)
    position_008 = headers.index('008')
    for row in reader:
        if row[position_008][35:38] != 'cze':
            list_of_records.append(row)
            
oclc_lang = pd.DataFrame(list_of_records, columns=headers)
oclc_lang['language'] = oclc_lang['008'].apply(lambda x: x[35:38])
oclc_viaf = pd.read_excel('F:/Cezary/Documents/IBL/Translations/OCLC/Czech viaf/oclc_viaf.xlsx')
#oclc_viaf = pd.read_excel('C:/Users/User/Desktop/oclc_viaf.xlsx')
oclc_viaf['language'] = oclc_viaf['008'].apply(lambda x: x[35:38])
oclc_viaf = oclc_viaf[oclc_viaf['language'] != 'cze']

oclc_other_languages = pd.concat([oclc_lang, oclc_viaf])

oclc_other_languages['nature_of_contents'] = oclc_other_languages['008'].apply(lambda x: x[24:27])
oclc_other_languages = oclc_other_languages[oclc_other_languages['nature_of_contents'].isin(['\\\\\\', '6\\\\', '\\6\\', '\\\\6'])]
oclc_other_languages['type of record + bibliographic level'] = oclc_other_languages['LDR'].apply(lambda x: x[6:8])
oclc_other_languages['fiction_type'] = oclc_other_languages['008'].apply(lambda x: x[33])

df = oclc_other_languages[(oclc_other_languages['type of record + bibliographic level'] == 'am') &
                          (oclc_other_languages['041'].str.contains('\$hcz')) &
                          (oclc_other_languages['fiction_type'].isin(fiction_types))]

def index_of_correctness(x):
    full_index = 7
    record_index = 0
    if x['008'][35:38] != 'und':
        record_index += 1
    if pd.notnull(x['240']) and '$a' in x['240'] and any(e for e in ['$l', '$i'] if e in x['240']) and x['240'].count('$a') == 1 and '$k' not in x['240']:
        record_index += 3
    # elif pd.notnull(x['240']) and '$a' in x['240'] and x['240'].count('$a') == 1:
    #     record_index += 1.5
    if pd.notnull(x['245']) and all(e for e in ['$a', '$c'] if e in x['245']):
        record_index += 1
    elif pd.notnull(x['245']) and any(e for e in ['$a', '$c'] if e in x['245']): 
        record_index += 0.5
    if pd.notnull(x['260']) and all(e for e in ['$a', '$b', '$c'] if e in x['260']):
        record_index += 1
    elif pd.notnull(x['260']) and any(e for e in ['$a', '$b', '$c'] if e in x['260']):
        record_index += 0.5
    if pd.notnull(x['700']) and pd.notnull(x['700']):
        record_index += 1
    full_index = record_index/full_index
    return full_index

df['index of correctness'] = df.apply(lambda x: index_of_correctness(x), axis=1)

correct = df[df['index of correctness'] > 0.7]
not_correct = df[df['index of correctness'] <= 0.7]

writer = pd.ExcelWriter('high-quality_index_whole_OCLC.xlsx', engine = 'xlsxwriter')
correct.to_excel(writer, index=False, sheet_name='correct')
not_correct.to_excel(writer, index=False, sheet_name='not_correct')
writer.save()
writer.close()





 
# przykladowy_rekord = df_all_positive[df_all_positive['001'] == 561473178].squeeze()
    
# przykladowy_rekord = df_people_clusters[df_people_clusters['001'] == 561473178].squeeze()  
    
    
    
#%%control vocabulary for translations    
from langdetect import detect_langs
from langdetect.lang_detect_exception import LangDetectException

def detect_language(x):
    try:
        return detect_langs(x)
    except LangDetectException:
        return np.nan
    
translations = marc_parser_1_field(df_all_positive, '001', '700', '\$')
translations = pd.merge(translations, df_all_positive[['001', 'language']], how='left', on='001')
translation_roles_e = translations[translations['$e'] != ''][['$e', 'language']].drop_duplicates().reset_index(drop=True)
translation_roles_e['index'] = translation_roles_e.index+1
translation_roles_e = cSplit(translation_roles_e, 'index', '$e', '❦').drop(columns='index').drop_duplicates().rename(columns={'language':'target language'})
translation_roles_e['language detected'] = translation_roles_e['$e'].apply(lambda x: detect_language(x))

translation_roles_4 = translations[translations['$4'] != ''][['$4', 'language']].drop_duplicates().reset_index(drop=True)
translation_roles_4['index'] = translation_roles_4.index+1
translation_roles_4 = cSplit(translation_roles_4, 'index', '$4', '❦').drop(columns='index').drop_duplicates().rename(columns={'language':'target language'})

writer = pd.ExcelWriter('translation_control_vocabulary.xlsx', engine = 'xlsxwriter')
translation_roles_e.to_excel(writer, index=False, sheet_name='$e')
translation_roles_4.to_excel(writer, index=False, sheet_name='$4')
writer.save()
writer.close()


#%% dodanie osób
     
cz_authority_spreadsheet = gc.open_by_key('1QB5EmMhg7qSWfWaJurafHdmXc5uohQpS-K5GBsiqerA')  
cz_authority_df = get_as_dataframe(cz_authority_spreadsheet.worksheet('Sheet1'), evaluate_formulas=True).dropna(how='all').dropna(how='all', axis=1)    
cz_authority_viaf = cz_authority_df['viaf_id'].dropna().drop_duplicates().to_list()
    
df = pd.read_excel('C:/Users/Cezary/Downloads/high-quality_index_whole_OCLC.xlsx', sheet_name = ['correct', 'not_correct'])   
df = pd.concat([df['correct'], df['not_correct']]) 
viaf_100 = marc_parser_1_field(df, '001', '100', '\$')['$1'].drop_duplicates().to_list()
viaf_100 = [re.findall('\d+', e)[0] for e in viaf_100 if e != ''] 
    
missing_people_to_add = [e for e in viaf_100 if e not in cz_authority_viaf] 

ns = '{http://viaf.org/viaf/terms#}'
viaf_list = []
for element in tqdm(missing_people_to_add):
    empty_dict = {}
    url = f"https://viaf.org/viaf/{element}/viaf.xml"
    response = requests.get(url)
    with open('viaf.xml', 'wb') as file:
        file.write(response.content)
    tree = et.parse('viaf.xml')
    root = tree.getroot()
    empty_dict['viaf_id'] = root.findall(f'.//{ns}viafID')[0].text
    IDs = root.findall(f'.//{ns}mainHeadings/{ns}data/{ns}sources/{ns}sid')
    empty_dict['IDs'] = '❦'.join([t.text for t in IDs])
    nationality = root.findall(f'.//{ns}nationalityOfEntity/{ns}data/{ns}text')
    empty_dict['nationality'] = '❦'.join([t.text for t in nationality])
    occupation = root.findall(f'.//{ns}occupation/{ns}data/{ns}text')
    empty_dict['occupation'] = '❦'.join([t.text for t in occupation])
    language = root.findall(f'.//{ns}languageOfEntity/{ns}data/{ns}text')
    empty_dict['language'] = '❦'.join([t.text for t in language])
    names = root.findall(f'.//{ns}mainHeadings/{ns}data/{ns}text')
    empty_dict['names'] = '❦'.join([t.text for t in names])
    viaf_list.append(empty_dict)
    
with open('missing_people_to_add.json', 'w', encoding='utf-8') as file: 
            json.dump(viaf_list, file, ensure_ascii=False, indent=4)    
 

unused_people = [e for e in cz_authority_viaf if e not in viaf_100] 
unused_people_df = pd.DataFrame(unused_people)
unused_people_df.to_excel('unused_people.xlsx', index=False)



























    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    