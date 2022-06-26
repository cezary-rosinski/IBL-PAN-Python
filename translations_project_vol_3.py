import pandas as pd
from datetime import datetime
import regex as re
import numpy as np
from my_functions import gsheet_to_df, marc_parser_1_field, cSplit, cluster_records, simplify_string, mrc_to_mrk#, marc_parser_dict_for_field
import unidecode
from tqdm import tqdm
import requests
from collections import Counter
import warnings
import io
import pickle 
from copy import deepcopy
from difflib import SequenceMatcher
from ast import literal_eval
import gspread as gs
from my_functions import create_google_worksheet
import matplotlib.pyplot as plt
from kneed import KneeLocator
from googleapiclient.errors import HttpError
import time

warnings.simplefilter(action='ignore', category=FutureWarning)
pd.set_option('display.float_format', lambda x: '%.3f' % x)
pd.options.mode.chained_assignment = None

#%% defs

def get_audience(x):
    audience_dict = {'a':'Juvenile', 'b':'Juvenile', 'c':'Juvenile', 'd':'Juvenile', 'j':'Juvenile', 'e':'General', 'f':'General', 'g':'General'}
    try:
        if '❦' not in x:
            return audience_dict[x]
        else:
            audiences = []
            for el in x.split('❦'):
                if el not in ['|', '\\']:
                    audiences.append(audience_dict[el])
            return '❦'.join(audiences)
    except KeyError:
        return 'other'
    
def get_ISBNs(x):
    try:
        x = x.split('❦')
        x = [[el['$a'].replace('-','').strip().split(' ')[0] for el in marc_parser_dict_for_field(e, '\$') if '$a' in el] for e in x]
        x = [e for sub in x for e in sub]
        x = list(set([e if e[:3] != '978' else e[3:] for e in x]))
        return '❦'.join(x)
    except AttributeError:
        return 'no ISBN'

def longest_string(s):
    return max(s, key=len)

def get_oclc_id(x):
    try:
        return re.findall('(?<=\(OCoLC\)|\(OCLC\) |OCLC |\(OCLC\)|OCLC)\d+', x['035'])[0]
    except (IndexError, TypeError):
        return x['fakeid']
    
def convert_float_to_int(x):
    try:
        return str(np.int64(x))
    except (ValueError, TypeError):
        return np.nan
    
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

def replace_viaf_group(df):
    viaf_groups = {'256578118':'118529174', '83955898':'25095273', '2299152636076120051534':'11196637', '78000938':'163185334', '10883385':'88045299'}
    df['viaf_id'] = df['viaf_id'].replace(viaf_groups)
    return df

# def quality_index(x):
#     full_index = 7
#     record_index = 0
#     if x['008'][35:38] != 'und':
#         record_index += 1
#     if x['SRC'] != 'Brno' and pd.notnull(x['240']) and '$a' in x['240'] and any(e for e in ['$l', '$i'] if e in x['240']) and x['240'].count('$a') == 1 and '$k' not in x['240']:
#         record_index += 3
#     elif x['SRC'] == 'Brno' and pd.notnull(x['765']) and '$t' in x['765']:
#         record_index += 3
#     # elif pd.notnull(x['240']) and '$a' in x['240'] and x['240'].count('$a') == 1:
#     #     record_index += 1.5
#     if pd.notnull(x['245']) and all(e for e in ['$a', '$c'] if e in x['245']):
#         record_index += 1
#     elif pd.notnull(x['245']) and any(e for e in ['$a', '$c'] if e in x['245']): 
#         record_index += 0.5
#     if pd.notnull(x['260']) and all(e for e in ['$a', '$b', '$c'] if e in x['260']):
#         record_index += 1
#     elif pd.notnull(x['260']) and any(e for e in ['$a', '$b', '$c'] if e in x['260']):
#         record_index += 0.5
#     if pd.notnull(x['700']) and pd.notnull(x['700']):
#         record_index += 1
#     full_index = record_index/full_index
#     return full_index

def quality_index(x):
    full_index = 6
    record_index = 0
    if x['language'] != 'und':
        record_index += 1
    if x['SRC'] == 'Brno' and pd.notnull(x['765']) and '$t' in x['765'] and simplify_string('Název českého originálu') in simplify_string(x['765']):
        record_index += 3
    elif x['SRC'] != 'Brno' and pd.notnull(x['240']) and '$a' in x['240'] and any(e for e in ['$l', '$i'] if e in x['240']) and x['240'].count('$a') == 1:
        record_index += 3
    elif x['language'] != 'und' and pd.notnull(x['work_title']):
        record_index += 3
    elif x['language'] != 'und' and pd.notnull(x['original title']):
        record_index += 3
    if pd.notnull(x['245']) and all(e in x['245'] for e in ['$a', '$c']):
        record_index += 1
    if pd.notnull(x['260']) and all(e in x['260'] for e in ['$a', '$b', '$c']):
        record_index += 1
    full_index = record_index/full_index
    return full_index

#HQ – language of the work is defined and not Czech; Original title (240, viaf_id from 245, or both), 260 a,b,c
# x = test[test['001'] == 320084695].squeeze()
# For HQ not important – translator in 700

def get_longest_cell(x):
    try:
        x = x.split('❦')
        return max(x, key=len)
    except AttributeError:
        return np.nan
    
def genre(x):
    genres_dict = {'0':'nonfiction','e':'nonfiction', 'f':'fiction' ,'1':'fiction' ,'h':'fiction' ,'j':'fiction', 'd':'drama','p':'poetry'}
    try:
        if '❦' not in x:
            return genres_dict[x]
        else:
            genres = []
            for el in x.split('❦'):
                if el not in ['|', '\\']:
                    genres.append(genres_dict[el])
            return '❦'.join(genres)
    except KeyError:
        return 'other'

def genre_algorithm(df):
    x = [e.split('❦') for e in df['cluster_genre'].to_list()]
    x = [e for sub in x for e in sub]
    length = len(x)
    x = Counter(x)
    if x['nonfiction']/length > 0.8:
        return 'nonfiction'
    elif x['drama']/length > 0.1:
        return 'drama'
    elif x['poetry']/length > 0.1:
        return 'poetry'
    else:
        return 'fiction'

def get_viaf_from_nkc(x):
    try:
        return nkc_viaf_ids[x]
    except KeyError:
        return np.nan
    
def find_similar_target_titles(title_a, title_b, similarity_lvl):
    original_similarity = SequenceMatcher(a=title_a, b=title_b).ratio()
    if title_a in title_b:
        return True
    elif original_similarity >= similarity_lvl:
        return True
    elif original_similarity <= 0.3:
        return False
    else:
        words = sorted([title_a.strip(), title_b.strip()], key=lambda x: len(x.strip().split(' ')))
        words_splitted = [e.split(' ') for e in words]
        similarity_lvls = []
        iteration = 0
        while iteration + len(words_splitted[0]) <= len(words_splitted[-1]):
            longer_temp = ' '.join(words_splitted[-1][iteration:iteration+len(words_splitted[0])])
            iteration +=1
            similarity_lvls.append(SequenceMatcher(a=words[0], b=longer_temp).ratio())
        if max(similarity_lvls) >= similarity_lvl:
            return True
        else: 
            return False

#%%
now = datetime.now().date()
year = now.year
month = '{:02}'.format(now.month)
day = '{:02}'.format(now.day)
#%%
# #autoryzacja do tworzenia i edycji plików
# gc = gs.oauth()
# #autoryzacja do penetrowania dysku
# gauth = GoogleAuth()
# gauth.LocalWebserverAuth()
# drive = GoogleDrive(gauth)

#%% Authority file
# try:
#     cz_authority_df = pd.read_excel("C:/Users/Cezary/Downloads/cz_authority.xlsx", sheet_name='incl_missing')
# except FileNotFoundError:
#     cz_authority_df = pd.read_excel("C:/Users/Rosinski/Downloads/cz_authority.xlsx", sheet_name='incl_missing')

# cz_authority_df = pd.read_excel("C:\\Users\\Cezary\\Desktop\\new_cz_authority_df_2022-02-02.xlsx", dtype=str)
cz_authority_df = gsheet_to_df('1yKcilB7SEVUkcSmqiTPauPYtALciDKatMvgqiRoEBso', 'Sheet1')

cz_authority_list = [e for e in cz_authority_df['nkc_Id']]

cz_id_viaf_id = cz_authority_df[['viaf_id', 'nkc_Id']]
cz_id_viaf_id = cSplit(cz_id_viaf_id, 'nkc_Id', 'viaf_id', '|')

viaf_ids = cz_id_viaf_id['viaf_id'].drop_duplicates().to_list()
nkc_ids = cz_id_viaf_id['nkc_Id'].drop_duplicates().to_list()
    
  
#%% CLT
file_path = "F:\\Cezary\\Documents\\IBL\\Translations\\CLT\\czech_translations_full_18_01_2022.mrk"
marc_list = io.open(file_path, 'rt', encoding = 'utf-8').read().splitlines()
records = []
for row in tqdm(marc_list):
    if row.startswith('=LDR'):
        records.append([row])
    else:
        if len(row) > 0:
            records[-1].append(row)
            
final_list = []
for lista in records:
    slownik = {}
    for el in lista:
        if el[1:4] in slownik:
            slownik[el[1:4]] += f"❦{el[6:]}"
        else:
            slownik[el[1:4]] = el[6:]
    final_list.append(slownik)
    
clt_df = pd.DataFrame(final_list)
clt_df['SRC'] = 'CLT'
clt_df['fiction_type'] = clt_df['008'].apply(lambda x: x[33])
clt_df['nkc_id'] = clt_df['100'].apply(lambda x: marc_parser_dict_for_field(x, '\$')['$7'].replace('❦', '').strip() if pd.notnull(x) and '$7' in x else np.nan)

fields = clt_df.columns.tolist()
fields.sort(key = lambda x: ([str,int].index(type("a" if re.findall(r'\w+', x)[0].isalpha() else 1)), x))
clt_df = clt_df.reindex(columns=fields) 
# jeśli w 915 nie ma False, a fiction_type == 0 i autor nie jest w nkc_ids
to_remove = clt_df[(~clt_df['915'].str.contains('False')) &
                   (clt_df['fiction_type'] == '0') &
                   (~clt_df['nkc_id'].isin(nkc_ids))]
# difficult_dbs_dict.update({'Non-fiction in CLT': to_remove})
clt_df = clt_df[~clt_df['001'].isin(to_remove['001'])].reset_index(drop=True)
del clt_df['nkc_id']

#BRNO
file_path = "C:/Users/Cezary/Downloads/scrapeBrno.txt"
marc_list = io.open(file_path, 'rt', encoding = 'utf-8').read().replace('|','$').splitlines()

records = []
for row in tqdm(marc_list):
    if row.startswith('LDR'):
        records.append([row])
    else:
        if not row.startswith('FMT') and len(row) > 0:
            records[-1].append(row)
            
records = [[e.split('\t') for e in record] for record in records]
records = [[[field[0] if not field[0][:3].isnumeric() else field[0][:3], field[1]] for field in record] for record in records]

final_list = []
for record in tqdm(records):
    slownik = {}
    for [field, content] in record:
        if field in slownik:
            slownik[field] += f"❦{content}"
        else:
            slownik[field] = content
    final_list.append(slownik)

brno_df = pd.DataFrame(final_list)

brno_df = brno_df[(brno_df['DEL'].isnull()) &
                  (brno_df['BAS'].str.contains('CZT')) &
                  (~brno_df['041'].isna()) &
                  (brno_df['041'].str.contains('$h cze', regex=False))].drop(columns=['BAS', 'DEL', 'GBS', 'LID', 'MKL', 'Obalkyknih.cz:', 'PAK', 'PRU', 'RDALD', 'SHOD', 'STA', 'SYS', 'A653', 'B260', 'GBS1', 'GBS2', 'M0ZK', 'T787', 'V015', 'V020', 'V043', 'V490', 'V505', 'V787', 'V902']).reset_index(drop=True)
brno_df['008'] = brno_df['008'].str.replace('$', '|')
brno_df['SRC'] = 'Brno'

#NKC
nkc_df = pd.read_excel("C:/Users/Cezary/Downloads/skc_translations_cz_authority_2021-8-12.xlsx").drop(columns=['viaf_id', 'cz_id', 'cz_name']).drop_duplicates().reset_index(drop=True)
nkc_df = nkc_df[~nkc_df['language'].isin(['pol', 'fre', 'eng', 'ger'])].drop_duplicates().reset_index(drop=True)
nkc_df['SRC'] = 'NKC'

#połączenie czeskich zbiorów

cz_total = pd.concat([clt_df, brno_df, nkc_df]).reset_index(drop=True)

#usunięcie rekordów w języku czeskim – ile usuwamy????????????????????????????????
cz_total['language'] = cz_total['008'].apply(lambda x: x[35:38])
cz_total = cz_total[cz_total['language'] != 'cze'].reset_index(drop=True)

cz_total['oldid'] = cz_total['001']
start_value = 10000000000
cz_total['fakeid'] = pd.Series(range(start_value,start_value+cz_total.shape[0]+1,1))
cz_total['fakeid'] = cz_total['fakeid'].astype('Int64').astype('str')
        
cz_total['001'] = cz_total.apply(lambda x: get_oclc_id(x), axis=1)
cz_total.drop(columns='fakeid', inplace=True)


#viafowanie
cz_authority_dict = {k:v for k,v in zip(cz_authority_df['nkc_Id'].to_list(), cz_authority_df['viaf_id'].to_list())}
doubled_viaf = {k:v for k,v in cz_authority_dict.items() if '|' in v}
doubled_viaf_to_remove = ['256578118', '83955898', '2299152636076120051534', '78000938', '10883385', '88374485', '49250791', '297343866', '9432935', '66469255']
doubled_viaf = {k:[e for e in v.split('|') if e not in doubled_viaf_to_remove][0] for k,v in doubled_viaf.items()}
for key in doubled_viaf:
    cz_authority_dict.pop(key)
cz_authority_dict.update(doubled_viaf)

field_100 = marc_parser_1_field(cz_total, '001', '100', '\$')

def get_viaf_from_nkc(x):
    try:
        return cz_authority_dict[x]
    except KeyError:
        return np.nan

field_100['$1'] = field_100['$7'].apply(lambda x: get_viaf_from_nkc(x))

cz_right_people = field_100[field_100['$1'].notnull()]
cz_right_people['001'] = cz_right_people['001'].astype(np.int64)
cz_right_people = dict(zip(cz_right_people['001'],cz_right_people['$1']))

for i, row in tqdm(cz_total.iterrows(), total=cz_total.shape[0]): 
    try:
        cz_total.at[i, '100'] = f"{row['100']}$1http://viaf.org/viaf/{cz_right_people[int(row['001'])]}"
    except KeyError:
        pass

cz_total['100_unidecode'] = cz_total['100'].apply(lambda x: unidecode.unidecode(x).lower() if pd.notnull(x) else x)
cz_total.to_excel('cz_total.xlsx', index=False)

#OCLC
oclc_df = pd.read_excel("oclc_all_positive.xlsx").drop_duplicates().reset_index(drop=True)

oclc_df['viaf_id'] = ''
for i, row in tqdm(oclc_df.iterrows(), total=oclc_df.shape[0]):
    try:
        oclc_df.at[i, 'viaf_id'] = re.findall('\d+', marc_parser_dict_for_field(row['100'], '\\$')['$1'])[0]
    except (KeyError, TypeError):
        oclc_df.at[i, 'viaf_id'] = np.nan
        
oclc_df = replace_viaf_group(oclc_df).drop_duplicates().reset_index(drop=True)
oclc_df['SRC'] = 'OCLC'

#połączenie wszystkich zbiorów
total = pd.concat([cz_total, oclc_df]).reset_index(drop=True)

fields = total.columns.tolist()
fields.sort(key = lambda x: ([str,int].index(type("a" if re.findall(r'\w+', x)[0].isalpha() else 1)), x))
total = total.reindex(columns=fields) 

total['005'] = total['005'].apply(lambda x: convert_float_to_int(x))
total['fiction_type'] = total['008'].apply(lambda x: x[33])
total['audience'] = total['008'].apply(lambda x: x[22])
total['language'] = total['008'].apply(lambda x: x[35:38])

total['260'] = total[['260', '264']].apply(lambda x: x['260'] if pd.notnull(x['260']) else x['264'], axis=1)
total['240'] = total[['240', '246']].apply(lambda x: x['240'] if pd.notnull(x['240']) else x['246'], axis=1)
total['001'] = total['001'].astype(np.int64)

total.to_excel(f'everything_merged_{now}.xlsx', index=False)
# total = pd.read_excel('everything_merged_2022-02-22.xlsx')

# deduplikacja po OCLC ID

total_grouped = total.groupby('001')
duplicates_clt_oclc = total_grouped.filter(lambda x: len(x) > 1).sort_values('001')
total = total[~total['001'].isin(duplicates_clt_oclc['001'])]
duplicates_clt_oclc_grouped = duplicates_clt_oclc.groupby('001')
#3630 duplikatów
#14.2.2022 --> 8919 duplikatów
#24.2.2022 --> 

#765 z brno, 240 z oclc
clt_oclc_deduplicated = pd.DataFrame()
for name, group in tqdm(duplicates_clt_oclc_grouped, total=len(duplicates_clt_oclc_grouped)):
    for column in group:
        if column in ['fiction_type', 'audience', '020', '041']:
            group[column] = '❦'.join(group[column].dropna().drop_duplicates().astype(str))
        else:
            try:
                group[column] = max(group[column].dropna().astype(str), key=len)
            except ValueError:
                group[column] = np.nan
    clt_oclc_deduplicated = clt_oclc_deduplicated.append(group)

clt_oclc_deduplicated = clt_oclc_deduplicated.drop_duplicates()
clt_oclc_deduplicated['001'] = clt_oclc_deduplicated['001'].astype(np.int64)

translations_df = pd.concat([total, clt_oclc_deduplicated]).reset_index(drop=True)  
fields = translations_df.columns.tolist()
fields.sort(key = lambda x: ([str,int].index(type(1 if re.findall(r'\w+', x)[0].isnumeric() else "a")), x))
translations_df = translations_df.reindex(columns=fields) 

translations_df.to_excel(f'pre-initial_stage_{now}.xlsx', index=False)
# translations_df = pd.read_excel('pre-initial_stage_2022-02-25.xlsx')

#%%removal of invalid records
invalid_records = {}

#1. early literature
viaf_to_del = ['74283407', '29835535', '32348156']
nkc_to_del = ['jk01061444', 'jk01150938', 'jk01031383']
translations_df['nkc_id'] = translations_df['100'].apply(lambda x: marc_parser_dict_for_field(x, '\$')['$7'].replace('❦', '').strip() if pd.notnull(x) and '$7' in x else np.nan)

to_remove = translations_df.copy()
translations_df = translations_df[(~translations_df['viaf_id'].isin(viaf_to_del)) & 
                                  (~translations_df['nkc_id'].isin(nkc_to_del))]
to_remove = to_remove[~to_remove['001'].isin(translations_df['001'])]
invalid_records.update({'Early literature': to_remove})
#86899

#2. no author in 100
to_remove = translations_df[translations_df['100'].isnull()]
invalid_records.update({'No author in 100': to_remove})
translations_df = translations_df[translations_df['100'].notnull()]
#82389

#3. Wrong VIAF/NKC IDs
to_remove = []
for i, row in tqdm(translations_df.iterrows(), total = translations_df.shape[0]):
    # row = translations_df[translations_df['001'] == 10000001180].squeeze()
    try:
        nkc_id = marc_parser_dict_for_field(row['100'], '\$')['$7'].replace('❦', '').strip()
    except KeyError:
        nkc_id = 0
    if nkc_id != 0 and nkc_id not in nkc_ids:
        to_remove.append(row['001'])
    elif nkc_id == 0 and pd.isnull(row['viaf_id']):
        pass
    elif pd.notnull(row['viaf_id']) and row['viaf_id'] not in viaf_ids:
        to_remove.append(row['001'])

to_remove = translations_df[translations_df['001'].isin(to_remove)]
invalid_records.update({'Wrong author ID': to_remove})
translations_df = translations_df[~translations_df['001'].isin(to_remove['001'])].reset_index(drop=True)
#74355

#4. undefined year

translations_df['year'] = translations_df['008'].apply(lambda x: x[7:11])
years = translations_df[(translations_df['year'].str.contains('u')) | 
                        (translations_df['year'].isin(['0000', '0001', '9999', '19\\\\', '\\\\\\\\', '1086', '1098']))][['001', 'year', '260']]
years['sugestia'] = years['260'].apply(lambda x: [e for e in re.findall('\d+', x) if len(e) in [3,4]] if pd.notnull(x) and re.findall('\d+', x) else np.nan)
years = years[years['sugestia'].notnull()]

years['sugestia'] = years['sugestia'].apply(lambda x: [e for e in x if e.startswith(('1', '2'))])
years = years[years['sugestia'].apply(lambda x: len(x)) > 0]
years['sugestia'] = years['sugestia'].apply(lambda x: f'{x[0]}0' if len(x[0]) == 3 else x[0])
years = years[years['sugestia'].astype(np.int64) <= now.year][['001', 'sugestia']].rename(columns={'sugestia':'year'})
years_dict = dict(zip(years['001'],years['year']))
translations_df['year'] = translations_df[['001', 'year']].apply(lambda x: years_dict[x['001']] if x['001'] in years_dict else x['year'], axis=1)

to_remove = translations_df[(translations_df['year'].str.contains('u')) | 
                            (translations_df['year'].isin(['0000', '0001', '9999', '19\\\\', '\\\\\\\\', '1086', '1098']))]
invalid_records.update({'Undefined year': to_remove})
translations_df = translations_df[~translations_df['001'].isin(to_remove['001'])].reset_index(drop=True)

translations_df['year'] = translations_df['year'].astype(np.int64)
year_correction = {10000007972: 1900,
                   873686897: 1992,
                   943905259: 1988}
translations_df['year'] = translations_df[['001', 'year']].apply(lambda x: year_correction[x['001']] if x['001'] in year_correction else x['year'], axis=1)
#73289

#5. pre-1810 translations

to_remove = translations_df[translations_df['year'] < 1810]
invalid_records.update({'Pre-1810 translations': to_remove})
translations_df = translations_df[translations_df['year'] >= 1810]
#73263

#6. manual deleting

wrong_und_places = ['"Pp. 176. Kruh Přátel Československé Knihy, péčí týdeníku ""Čechoslovák"":"', '"V Praze, nakl. ""Besedy ucitelske"""', '(Berlin', '(Brno) :', '(České Budějovice', '(Havlíčku̇v Brod', '(Havlíčk°uv Brod', '(Kr. Vinohrady', '(Král. Vinohrady', '(Liberec) :', '(München) :', '(Prag :', '(Prag) :', '(Praha', '(Praha :', '(Praha)', '(Praha) :', '(Praha):', '(Praha-Smichov', '(Praha:', "(Praha:) 'Čsl. spisovatel'", '(Prostějov)', '(V Bratislave', '(V Brně', '(V Brně :', '(V Brně)', '(V Brně) :', '(V Hradci Králové) :', '(V Liberci):', '(V Nuslich)', '(V Ostravě) :', '(V Prace', '(V Praze', '(V Praze :', '(V Praze)', '(V Praze) :', '(V Praze):', '(V Praze-Smíchově)', '(V Ústi nad Labem', '(V Ústí nad Labem', '*R', '2 díl. v Praze', '6 díl. Praha, 1936', 'Âºii. Vyškov na Moravě', 'Berne :', 'Brně :', 'Brno', 'Brno :', 'Brno, Fr. Fadouska [N.D.]', 'Brno, nakl. Polygrafie :', 'Brno:', 'Brně', 'Brumovice :', 'Břeclav :', 'Budějovice', 'Budysin', 'Budyšin', 'Budyšin :', 'Budyšin [Bautzen]', 'Budyšin', 'C̆eské Budějovice', 'Česká Třebová', 'České Budějovice', 'České Budějovice :', 'Československý Spisovatel :', 'Česky Tešín :', 'Dantisci', 'Dedictvi Komenskeho', 'Díl 1. sv. 1, 2. v Praze', 'Evangelicka Bratrska Diaspora Jihoceska', 'Evangelickem Nakladatelstvi', 'Havličk°uv Brod', 'Havlíčkuv Brod :', 'Havlíčkův Brod :', 'Hradci Králové', 'Hradec Králové', 'Hradec Králové :', 'Jablonci nad Nisou :', 'Jablonci nad Nisou :❦', 'Jevičko :', 'Jevíčko :', 'Jinočany :', 'Karviná :', 'Kladno', 'Komeniologicke Dokumentacni Stredisko Pri Museu J.A. Komenskeho v Uh. Brode', 'Kostelec na Hané :❦', 'Král. Vinohrady :', 'Královské Vinohrady', 'Kutná Hora :', 'Liberec :', 'Liberec:', 'Ml. Boleslav :', 'Mladá Boleslav :', 'Mnichov :', 'Moravská Ostrava :', 'Na Kladně :', 'Na Královských Vinohradech :', 'Na Královských Vinohradech:', 'Nak. Bratrske Jednoty', 'Nakladom spolku Tranoscius', 'Nučice :', 'Nyniznovu Vydan', 'Olmütz', 'Olomouc', 'Olomouc :', 'Ostrava', 'Ostrava :', 'Ostrava-Zabreh :', 'PRAG', 'PRAGUE', 'Pardubice', 'Pardubice :', 'Pelhřimov :', 'Pilsen :', 'Place of publication not identified :', 'Plzeň', 'Plzeň :', 'Plzni :', 'Pp. 109. Praha', 'Pp. 111. v Praze', 'Pp. 12. v Praze', 'Pp. 120. v Praze', 'Pp. 127. Praha:', 'Pp. 133. Praha', 'Pp. 144. v Praze', 'Pp. 145. Praha', 'Pp. 155. Praha', 'Pp. 185. v Praze', 'Pp. 200. v Praze', 'Pp. 202. Praha', 'Pp. 211. Praha', 'Pp. 216. v Praze', 'Pp. 223. v Praze', 'Pp. 229. Praha', 'Pp. 24. v Praze', 'Pp. 241. v Praze', 'Pp. 246. v Praze', 'Pp. 258. v Praze', 'Pp. 277. v Praze', 'Pp. 29. v Hranicích', 'Pp. 31. v Praze', 'Pp. 335. pl. 17. Praha', 'Pp. 377. v Praze', 'Pp. 39. 1883.', 'Pp. 400. v Praze', 'Pp. 45.', 'Pp. 46. V Praze', 'Pp. 50. 1880.', 'Pp. 50. v Praze', 'Pp. 501. Praha', 'Pp. 55. 1883.', 'Pp. 55. v Praze', 'Pp. 63. v Praze', 'Pp. 650. v Praze', 'Pp. 72.', 'Pp. 76.', 'Pp. 77. 1883.', 'Pp. 79. v Praze', 'Pp. 81. v Praze', 'Pp. 83. Divadelní ústav:', 'Pp. 93.', 'Pp. 98. v Praze', 'Pp. 99. v Praze', 'Pp. xii. 522. v Praze', 'Praag :', 'Prada :', 'Prag', 'Prag :', 'Prag :❦', 'Praga', 'Praga :', 'Pragae', 'Pragae :', 'Pragae:', 'Prague', 'Prague :', 'Prague, J. Laichter', 'Prague, Statni nakl. politicke literatury', 'Prag❦', 'Praha', 'Praha :', 'Praha :❦', 'Praha :❦❦', 'Praha Českoslov. Spisovatel', 'Praha Statni pedagogicke nakl.', 'Praha Stát. nakl. dětské knihy', 'Praha [Prag]', 'Praha [u.a.]', 'Praha, Ceskoslovenski spisovatel', 'Praha, Czechoslovakia :', 'Praha, F. Backovsky', 'Praha, J. Otto [N.D.]', 'Praha, Statni pedagogicke nakl 1956.', 'Praha, Statni pedagogicke nakl.', 'Praha, etc. :', 'Praha,❦', 'Praha-Dejvice', 'Praha-Vinohrady', 'Praha.', 'Praha:', 'Praha: Mladá fronta', 'Praha] :', 'Prahan :', 'Prahá', 'Praza :', 'Praze', 'Praze :', 'Praze:', 'Práce', 'Prosinec', 'Prága', 'Přerov', 'Ružomberok', 'Růže :', 'Rychnov n. Kn.', 'Stara Ríše', 'Stará Riše :❦', 'Stará Řiše na Moravě', 'Třebíč', 'Třebíč :', 'Tubingae', 'Tubingae :❦', 'Turć. Sv. Martin', 'Třebíčí na Moravé', 'U Zagrebu', 'Uherské Hradiště', 'Uherské Hradiště :', 'Ustí nad Labem', 'Ustredniho spolku jednot ucitelskych na Morave', 'Ústi N.L. :', 'Ústi n. Labem', 'Ústí n. L. :', 'Ústí nad Labem', 'Ústí nad Labem :', 'V Bělé u Bezděze', 'V Bratislavě', 'V Brne', 'V Brnë :', 'V Brně', 'V Brně :', 'V Brně:', 'V Brnǒ :', 'V Brně', 'V Brně [u.a.]', 'V Břeclavě :', 'V Čelabinsku :', 'V České Třebové :', 'V Českém Brodě', 'V HRADICI KRALOVE', 'V Hradci Králove :', 'V Hradci Králové', 'V Hradci Králové :', 'V Hradci Králově', 'V Jinošově', 'V Jinošově na Moravě :', 'V Karlových Varech :', 'V Liberci', 'V Liberci :', 'V Litomyšli', 'V Ljubljani :', 'V Londýně :', 'V Mnichově :', 'V Novém Jičíně :', 'V Novém Jičině', 'V Olomouci :', 'V Ostrave :', 'V Ostravě', 'V Ostravě :', 'V Pelhřimově', 'V Plzni', 'V Policce, F. Popelky', 'V Prace :', 'V Prahe', 'V Praza :', 'V Praze', 'V Praze :', 'V Praze, tiskem E. Gregra', 'V Praze-Spořilově', 'V Praze:', 'V Přerově', 'V Přerově :', 'V Rokycanech', 'V Tasově', 'V Tasově na Moravě :', 'V Táboře:', 'V Telči', 'V Turnově', 'V Táboře', 'V Ustí nad Labem', 'V Ústí nad Labem', 'V Ústí nad Labem :', 'V Ústí nad Labem:', 'V. Praze :', 'Ve Valašském Meziříčí:', 'Ve Vidnie :', 'Ve Vídni :', 'Ve Vranově nad Dyjí', 'Vinohradech', 'Vinohrady', 'Vyškov :', 'W Budyšinje:', 'W Praze', 'W Praze :', 'W. Praze', 'Zlín :', "Žd'ár nad Sázavou :", 'Žižkov', '[Brno]', '[Brno] :', '[Brno]:', '[Erscheinungsort nicht ermittelbar]', '[Erscheinungsort nicht ermittelbar] :', '[Havličkův Brod]', '[Horní Bříza] :', '[Kbh.] :', '[Král. Vinohrady]', '[Liberec]', '[Liberec] :', '[Lieu de publication non identifié] :', '[Lieu de publication non identifié] :❦', '[Place of publication not identified]', '[Place of publication not identified] :', '[Plzen]', '[Plzeň]', '[Plzeň] :', '[Plzeň]:', '[Prag]', '[Prag] :', '[Prag]:', '[Prague, Nakl. Spolku pro zrizeni desky a pomniku Viktoru Dykovi]', '[Prague]', '[Prague] :', '[Prague]:', '[Praha, vyd. red. V.K. Skracha ustredni spolek ceskoslovenskych knihkupeckych ucetnich]', '[Praha]', '[Praha] :', '[Praha] J. Otto', '[Praha] Nakl. ustredniho studentskeho knihkupcectvi', '[Praze] :', '[S.l.?]', '[S.l.]', '[S.l.] :', '[Sárospatak]', '[Stará Řiše]', '[Usti nad Labem] :', '[Ústí nad Labem]', '[Ústí nad Labem] :', '[V Brné] :', '[V Praze :', '[V Praze]', '[V Praze] :', '[V Tasově na Moravě]', '[Ve Velkém Meziřičí]', '[Vork pr. Vejle] :', '[s.l.]', '[v Praze]', 'v Brně', 'v Praze', 'v Praze-Karline']

test_und = translations_df.loc()[translations_df['language'] == 'und']

def place_of_pbl(x):
    try:
        return marc_parser_dict_for_field(x, '\$')['$a']
    except (KeyError, TypeError):
        np.nan

test_und['wrong place'] = test_und['260'].apply(lambda x: place_of_pbl(x))

test_und = test_und[test_und['wrong place'].isin(wrong_und_places)]
translations_df = translations_df[~translations_df['001'].isin(test_und['001'])].reset_index(drop=True)

# manual deleting
german_to_remove = io.open("C:\\Users\\Cezary\\Downloads\\ger_ids_del.txt", encoding='utf-8').readlines()
german_to_remove = [int(re.findall('\d+', e)[0]) for e in german_to_remove]
to_remove1 = translations_df[translations_df['001'].isin(german_to_remove)]
translations_df = translations_df[~translations_df['001'].isin(german_to_remove)]

ids_to_del = io.open("C:\\Users\\Cezary\\Downloads\\del_ids.txt", encoding='utf-8').readlines()
ids_to_del = [int(re.findall('\d+', e)[0]) for e in ids_to_del]
to_remove2 = translations_df[translations_df['001'].isin(ids_to_del)]
translations_df = translations_df[~translations_df['001'].isin(ids_to_del)]

delids_und_mul = io.open("C:\\Users\\Cezary\\Downloads\\delids_und_mul.txt", encoding='utf-8').readlines()
delids_und_mul = [int(re.findall('\d+', e)[0]) for e in delids_und_mul]
to_remove3 = translations_df[translations_df['001'].isin(delids_und_mul)]
translations_df = translations_df[~translations_df['001'].isin(delids_und_mul)]

delids_from_diff = io.open("C:\\Users\\Cezary\\Downloads\\delids_from_diff.txt", encoding='utf-8').readlines()
delids_from_diff = [int(re.findall('\d+', e)[0]) for e in delids_from_diff]
to_remove4 = translations_df[translations_df['001'].isin(delids_from_diff)]
translations_df = translations_df[~translations_df['001'].isin(delids_from_diff)]

to_remove = pd.concat([test_und, to_remove1, to_remove2, to_remove3, to_remove4])
invalid_records.update({'Manual deleting': to_remove})


#tutaj dodać 4 kolumny: język, rok, autorID, dziełoID!!!!!!!!!!!!!!
# translations_df = pd.read_excel(f'translation_database_clean_2022-02-24.xlsx')

cz_authority_dict = dict(zip(cz_authority_df['nkc_Id'], cz_authority_df['proper_viaf_id']))

translations_df['author_id'] = translations_df[['viaf_id', 'nkc_id']].apply(lambda x: x['viaf_id'] if pd.notnull(x['viaf_id']) else cz_authority_dict[x['nkc_id']] if x['nkc_id'] in cz_authority_dict else np.nan, axis=1)

works_ids = translations_df[(translations_df['245'].str.contains('viaf')) | 
                            (translations_df['595'].str.contains('\$1'))][['001', '245', '595']]
works_ids['work_id'] = works_ids[['245', '595']].apply(lambda x: re.findall('\d+', marc_parser_dict_for_field(x['245'], '\$')['$1'].replace('❦', '').strip())[0] if '$1' in x['245'] else marc_parser_dict_for_field(x['595'], '\$')['$1'].replace('❦', '').strip(), axis=1)
works_ids = dict(zip(works_ids['001'],works_ids['work_id']))
translations_df['work_id'] = translations_df['001'].apply(lambda x: works_ids[x] if x in works_ids else np.nan)

translations_df.to_excel(f'initial_stage_{now}.xlsx', index=False)


# Counter([e for e in translations_df['work_id'].dropna().to_list() if 'ubc' in e]).most_common(10)
# test = translations_df[translations_df['work_id'] == 'ubcjk011615498']

#%% new authority file

# new_people = list({v['index']:v for v in new_people}.values())
# new_cz_authority_df = pd.DataFrame(new_people)
# new_cz_authority_df = pd.concat([cz_authority_df, new_cz_authority_df]).reset_index(drop=True)
# new_cz_authority_df.to_excel(f'new_cz_authority_df_{now}.xlsx', index=False)

# new_cz_authority_df = pd.read_excel('new_cz_authority_df_2021-11-29.xlsx')
# new_cz_authority_df = pd.read_excel("C:\\Users\\Cezary\\Desktop\\new_cz_authority_df_2022-02-02.xlsx", dtype=str)
new_cz_authority_df = gsheet_to_df('1yKcilB7SEVUkcSmqiTPauPYtALciDKatMvgqiRoEBso', 'Sheet1')
viaf_positives = [e.split('|') for e in new_cz_authority_df['viaf_id'].drop_duplicates().dropna().to_list()]
viaf_positives = [e for sub in viaf_positives for e in sub]

positive_viafs_names = new_cz_authority_df[new_cz_authority_df['viaf_id'].notnull()][['viaf_id', 'all_names']]
positive_viafs_names = cSplit(positive_viafs_names, 'viaf_id', 'all_names', '❦')
positive_viafs_names['all_names'] = positive_viafs_names['all_names'].apply(lambda x: re.sub('(.*?)(\$a.*?)(\$0.*$)', r'\2', x) if pd.notnull(x) else np.nan)
positive_viafs_names = positive_viafs_names[positive_viafs_names['all_names'].notnull()].drop_duplicates()
positive_viafs_names['index'] = positive_viafs_names.index+1
positive_viafs_names = cSplit(positive_viafs_names, 'index', 'viaf_id', '|').drop(columns='index')

positive_viafs_diacritics = new_cz_authority_df[new_cz_authority_df['viaf_id'].notnull()][['viaf_id', 'cz_name']]
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

# new_cz_authority_df = pd.read_excel('new_cz_authority_df_2021-10-23.xlsx')


#%% author clustering
# translations_df = pd.read_excel('initial_stage_2022-03-07.xlsx')
# invalid_records = {}
people_df = translations_df.copy()[['001', '100_unidecode']]
people_df['100_unidecode'] = people_df['100_unidecode'].apply(lambda x: x if x[0] != '$' else f'1\\{x}')
people_df = marc_parser_1_field(people_df, '001', '100_unidecode', '\\$')[['001', '$a', '$d', '$1']].replace(r'^\s*$', np.nan, regex=True)  
people_df['$ad'] = people_df[['$a', '$d']].apply(lambda x: '$d'.join(x.dropna().astype(str)) if pd.notnull(x['$d']) else x['$a'], axis=1)
people_df['$ad'] = '$a' + people_df['$ad']
people_df['simplify string'] = people_df['$a'].apply(lambda x: simplify_string(x))
people_df['simplify_ad'] = people_df['$ad'].apply(lambda x: simplify_string(x))
people_df['001'] = people_df['001'].astype(np.int64)

# author clusters

authors_ids = dict(zip(translations_df['001'], translations_df['author_id']))
people_df['author_id'] = people_df['001'].apply(lambda x: authors_ids[x])

with_cluster_viaf = people_df[people_df['author_id'].notnull()][['001', 'author_id']]
without_cluster_viaf = people_df[people_df['author_id'].isnull()]

additional_viaf_to_remove = ['256578118', '83955898', '2299152636076120051534', '78000938', '10883385', '88374485', '49250791', '297343866', '9432935', '66469255']
people_clusters = {k:v for k,v in viaf_positives_dict.copy().items() if k not in additional_viaf_to_remove}

clusters1 = {}
for key in tqdm(people_clusters, total=len(people_clusters)):
    records = []
    try:
        unidecode_lower_forms_of_names = [simplify_string(e) for e in people_clusters[key]['form of name']]
        records_2 = without_cluster_viaf[without_cluster_viaf['$ad'].isin(unidecode_lower_forms_of_names)]['001'].to_list()
        records += records_2
    except KeyError:
        pass
    try:
        unidecode_name = [simplify_string(e) for e in people_clusters[key]['unidecode name']]
        records_3 = without_cluster_viaf[without_cluster_viaf['simplify string'].isin(unidecode_name)]['001'].to_list()
        records += records_3
    except KeyError:
        pass
    try:
        unidecode_lower_forms_of_names = [simplify_string(e) for e in people_clusters[key]['form of name']]
        records_4 = without_cluster_viaf[without_cluster_viaf['simplify_ad'].isin(unidecode_lower_forms_of_names)]['001'].to_list()
        records += records_4
    except KeyError:
        pass
    records = list(set(records))
    clusters1.update({key:records})
    
clusters1 = pd.DataFrame.from_dict(clusters1, orient='index').stack().reset_index(level=0).rename(columns={'level_0':'author_id', 0:'001'})
clusters1['001'] = clusters1['001'].astype(np.int64)
    
clusters2 = literal_eval(open('cz_translation_manual_author_cluster.txt').read())
clusters2 = pd.DataFrame.from_dict(clusters2, orient='index').stack().reset_index(level=0).rename(columns={'level_0':'001', 0:'author_id'})
clusters2['001'] = clusters2['001'].astype(np.int64)

# the_rest = without_cluster_viaf[(~without_cluster_viaf['001'].isin(clusters1['001'].to_list())) &
#                                 (~without_cluster_viaf['001'].isin(clusters2['001'].to_list()))]

# the_rest_unique = the_rest['$a'].drop_duplicates().to_list()

# matching = []
# not_matched = []        
# for name in tqdm(the_rest_unique):
#     # name = the_rest_unique[0]
#     # name = 'mickiewicz'
#     url = re.sub('\s+', '%20', f"http://viaf.org/viaf/search?query=local.personalNames%20all%20%22{name}%22&sortKeys=holdingscount&httpAccept=application/json")
#     response = requests.get(url)
#     response.encoding = 'UTF-8'
#     viaf_json = response.json()
#     list_of_numbers_of_records = [e for e in range(int(viaf_json['searchRetrieveResponse']['numberOfRecords']))[11::10]]
#     try:
#         viaf_json = viaf_json['searchRetrieveResponse']['records']
#         viaf_json = [[e['record']['recordData']['mainHeadings']['data'][0]['text'] if isinstance(e['record']['recordData']['mainHeadings']['data'], list) else e['record']['recordData']['mainHeadings']['data']['text'], e['record']['recordData']['viafID']] for e in viaf_json if e['record']['recordData']['nameType'] == 'Personal']
#         for number in list_of_numbers_of_records:
#             url = re.sub('\s+', '%20', f"http://viaf.org/viaf/search?query=local.personalNames%20all%20%22{name}%22&sortKeys=holdingscount&startRecord={number}&httpAccept=application/json")
#             response = requests.get(url)
#             response.encoding = 'UTF-8'
#             viaf_json_next = response.json()
#             viaf_json_next = viaf_json_next['searchRetrieveResponse']['records']
#             try:
#                 viaf_json_next = [[e['record']['recordData']['mainHeadings']['data'][0]['text'] if isinstance(e['record']['recordData']['mainHeadings']['data'], list) else e['record']['recordData']['mainHeadings']['data']['text'], e['record']['recordData']['viafID']] for e in viaf_json_next if e['record']['recordData']['nameType'] == 'Personal']
#                 viaf_json += viaf_json_next
#             except TypeError: pass
    
#         result = [e for e in viaf_json if e[-1] in viaf_ids]
#         for e in result:
#             e.insert(0, name)
#         if result:
#             matching.extend(result)
#         else: not_matched.append(name)
#     except KeyError:
#         not_matched.append(name)
        
# test = translations_df[translations_df['001'] == int(the_rest[the_rest['$a'] == matching[62][0]]['001'].to_list()[0])].squeeze()

# ['zeman, kamil', 'Olbracht, Iván, 1882-1952', '7423797']

# ['baldessari, vojteska', 'ganzelka, irzi']

# test = the_rest[the_rest['$a'].isin(['baldessari, vojteska', 'ganzelka, irzi', 'zeman, kamil'])]['001'].to_list()
# test = translations_df[translations_df['001'].isin(test)]


df_people_clusters = pd.concat([with_cluster_viaf, clusters1, clusters2])
people_clustered = dict(zip(df_people_clusters['001'],df_people_clusters['author_id']))

translations_df['author_id'] = translations_df['001'].apply(lambda x: people_clustered[x] if x in people_clustered else np.nan)

to_remove = translations_df[translations_df['author_id'].isnull()]
invalid_records.update({'Author not connected with VIAF': to_remove})
translations_df = translations_df[translations_df['author_id'].notnull()]

miler_correction = literal_eval(open('Miler_krtek.txt').read())
translations_df['author_id'] = translations_df[['001', 'author_id']].apply(lambda x: miler_correction[x['001']] if x['001'] in miler_correction else x['author_id'], axis=1)

#%% year clustering
#birth years vs. publishing years

# translations_df = pd.read_excel('translation_database_author_clusters_2022-03-01.xlsx')

year_correction = literal_eval(open('cz_translation_year_correction.txt').read())
translations_df['year'] = translations_df[['001', 'year']].apply(lambda x: year_correction[x['001']] if x['001'] in year_correction else x['year'], axis=1)

authority_viaf_and_date_dict = dict(zip(new_cz_authority_df['proper_viaf_id'], new_cz_authority_df['cz_dates']))
authority_viaf_and_date_dict = {k:int(v[:4]) if pd.notnull(v) and v[0] != '-' else np.nan for k,v in authority_viaf_and_date_dict.items()}
authority_viaf_and_date_dict = {k:v for k,v in authority_viaf_and_date_dict.items() if pd.notnull(v)}

wrong_year = translations_df.copy()
wrong_year['birth_year'] = wrong_year['author_id'].apply(lambda x: authority_viaf_and_date_dict[x] if x in authority_viaf_and_date_dict else np.nan)
wrong_year = wrong_year[['001', '100', '245', '260', 'birth_year', 'year', '008']].rename(columns={'008':'008_year'})
wrong_year['008_year'] = wrong_year['008_year'].apply(lambda x: x[7:11])
wrong_year = wrong_year[wrong_year['birth_year'].notnull()]
wrong_year['difference'] = wrong_year['year'] - wrong_year['birth_year']
wrong_year = wrong_year[wrong_year['difference'] <= 15]['001'].to_list()

to_remove = translations_df[translations_df['001'].isin(wrong_year)]
invalid_records.update({'Year publ higher than birth': to_remove})
translations_df = translations_df[~translations_df['001'].isin(wrong_year)]
#57245

# zapisanie invalid records
writer = pd.ExcelWriter(f'invalid_records_{now}.xlsx', engine = 'xlsxwriter')
for k, v in invalid_records.items():
    v.to_excel(writer, index=False, sheet_name=k)
writer.save()
writer.close()  

#difficult records
difficult_dbs_dict = {}  

#problemy!
# Jiri, Marek jk01080125
# dwa hasła? dwie osoby?
# https://viaf.org/viaf/45112529/#Marek,_Ji%C5%99%C3%AD,_1914-1994
# https://viaf.org/viaf/96942718/#Marek,_Ji%C5%99%C3%AD,_1914-1994

#1. Multiple original titles

to_remove = translations_df[((translations_df['240'].str.count('\$a') > 1) & translations_df['595'].isnull()) | 
                            (translations_df['765'].str.count('\$t') > 1) |
                            (translations_df['595'].str.count('\$1') > 1) |
                            (translations_df['765'].str.count('\$a') > 1)]

difficult_dbs_dict.update({'Multiple original titles': to_remove})
translations_df = translations_df[~translations_df['001'].isin(to_remove['001'])].reset_index(drop=True)

#2. Selections

to_remove = translations_df[translations_df['240'].str.contains('\$k', na=False)]
difficult_dbs_dict.update({'Selections': to_remove})
translations_df = translations_df[~translations_df['001'].isin(to_remove['001'])].reset_index(drop=True)

#3. No Czech original
to_remove = translations_df[(translations_df['765'].str.contains('nevyšlo', na=False)) |
                            ((translations_df['SRC'] == 'CLT') &
                             (translations_df['915'].str.contains('True')) &
                             (~translations_df['595'].str.contains('\$t', na=True)))]
difficult_dbs_dict.update({'No Czech original': to_remove})
translations_df = translations_df[~translations_df['001'].isin(to_remove['001'])].reset_index(drop=True)

#4. 'antolo' in 595 for CLT
to_remove = translations_df[translations_df['595'].str.contains('ubcantolo', na=False)]
difficult_dbs_dict.update({'CLT antolo': to_remove})
translations_df = translations_df[~translations_df['001'].isin(to_remove['001'])].reset_index(drop=True)

# zapisanie difficult records
writer = pd.ExcelWriter(f'difficult_records_{now}.xlsx', engine = 'xlsxwriter')
for k, v in difficult_dbs_dict.items():
    v.to_excel(writer, index=False, sheet_name=k)
writer.save()
writer.close()  

translations_df.to_excel(f'translation_database_author_clusters_{now}.xlsx', index=False)

#%% language clustering

translations_df = pd.read_excel('translation_database_author_clusters_2022-03-10.xlsx')

#language correction
translations_df.loc[translations_df['language'] == 'scr', 'language'] = 'hrv'
translations_df.loc[translations_df['language'] == 'scc', 'language'] = 'srp'
translations_df.loc[translations_df['language'].isin(['nno', 'nob']), 'language'] = 'nor'
translations_df.loc[translations_df['language'].isin(['dsb', 'hsb']), 'language'] = 'wen'

# und solution

und_language = translations_df[translations_df['language'] == 'und']
und_language_list = und_language[['001', 'author_id', '245', 'language']].values.tolist()

title_and_language = translations_df[translations_df['language'] != 'und'][['author_id', '245', 'language']].values.tolist()

result = []
for ind, aut, tit, lan in tqdm(und_language_list):
    for aut2, tit2, lan2 in title_and_language:
        if aut == aut2:
            if find_similar_target_titles(tit, tit2, 0.9):
                result.append([ind, tit, tit2, lan2])
not_found = [e for e in und_language_list if e[0] not in [el[0] for el in result]]

lang_df = pd.DataFrame(result, columns=['001', 'und_tit', 'lang_tit', 'language'])

lang_df_grouped = lang_df.groupby('001')
und_lang_corrected = {}
for name, group in tqdm(lang_df_grouped, total=len(lang_df_grouped)):
    ide = group['001'].to_list()[0]
    lan = max(set(group['language'].to_list()), key=group['language'].to_list().count)
    und_lang_corrected.update({ide: lan})
    
translations_df['language'] = translations_df[['001', 'language']].apply(lambda x: und_lang_corrected[x['001']] if x['001'] in und_lang_corrected else x['language'], axis=1)

und_language = translations_df[translations_df['language'] == 'und']
und_language_list = und_language[['001', 'author_id', '245', 'language']].values.tolist()

title_and_language = translations_df[translations_df['language'] != 'und'][['author_id', '245', 'language']].values.tolist()
title_and_language = list(set([(a, marc_parser_dict_for_field(t, '\$')['$a'],l) for a,t,l in title_and_language]))

result = []
for ind, aut, tit, lan in tqdm(und_language_list):
    for aut2, tit2, lan2 in title_and_language:
        if aut == aut2:
            if find_similar_target_titles(tit, tit2, 0.9):
                result.append([ind, tit, tit2, lan2])
not_found = [e for e in und_language_list if e[0] not in [el[0] for el in result]]

lang_df = pd.DataFrame(result, columns=['001', 'und_tit', 'lang_tit', 'language'])

lang_df_grouped = lang_df.groupby('001')
und_lang_corrected = {}
for name, group in tqdm(lang_df_grouped, total=len(lang_df_grouped)):
    ide = group['001'].to_list()[0]
    lan = max(set(group['language'].to_list()), key=group['language'].to_list().count)
    und_lang_corrected.update({ide: lan})
    
translations_df['language'] = translations_df[['001', 'language']].apply(lambda x: und_lang_corrected[x['001']] if x['001'] in und_lang_corrected else x['language'], axis=1)

und_language = translations_df[translations_df['language'] == 'und']
translations_df = translations_df[translations_df['language'] != 'und']

wrong_languages = ['zxx', '\\\\\\', 'mis', 'und']
translations_df = translations_df[~translations_df['language'].isin(wrong_languages)]

translations_df.to_excel(f'translation_database_clusters_year_author_language_{now}.xlsx', index=False)

#%% VIAF work harvesting
translations_df = pd.read_excel('translation_database_clusters_year_author_language_2022-03-14.xlsx')

transltions_df_copy = translations_df.copy()
translations_df = transltions_df_copy.copy()

# translations_df = pd.read_excel("translation_database_2021-11-29.xlsx")

#łączenie CLT z VIAF – work id; konwersja identyfikatorów

# grouped = translations_df[translations_df['work_id'].str.contains('ubc', na=False)].groupby('work_id')
# grouped = translations_df[translations_df['work_id'].isin([e[0] for e in cntr])].groupby('work_id')


# work_viaf_nkc = []
# errors = pd.DataFrame()
# for name, group in tqdm(grouped, total=len(grouped)):
#     # group = grouped.get_group('ubcjk010618655')
#     try:
#         query = viaf_positives_dict[group['author_id'].to_list()[0]]['unidecode name'][0] + ' ' + Counter([marc_parser_dict_for_field(e, '\$')['$t'] for e in group['595'].to_list() if pd.notnull(e) and '$t' in e]).most_common(1)[0][0]   
#         url = re.sub('\s+', '%20', f"http://viaf.org/viaf/search?query=local.uniformTitleWorks%20all%20%22{query}%22&sortKeys=holdingscount&httpAccept=application/json")
#         response = requests.get(url)
#         response.encoding = 'UTF-8'
#         viaf_json = response.json()
#         list_of_numbers_of_records = [e for e in range(int(viaf_json['searchRetrieveResponse']['numberOfRecords']))[11::10]]
#         viaf_json = viaf_json['searchRetrieveResponse']['records']
#         viaf_json = [[e['record']['recordData']['mainHeadings']['data'][0]['text'] if isinstance(e['record']['recordData']['mainHeadings']['data'], list) else e['record']['recordData']['mainHeadings']['data']['text'], e['record']['recordData']['viafID']] for e in viaf_json if e['record']['recordData']['nameType'] == 'UniformTitleWork'][0]
#         viaf_json.append(name)
#         work_viaf_nkc.append(viaf_json)
#     except (IndexError, KeyError):
#         errors = errors.append(group)
# #742 dzieł połączonych między CLT i VIAF
# with open('viaf_and_clt_work_id.pickle', 'wb') as handle:
#     pickle.dump(work_viaf_nkc, handle, protocol=pickle.HIGHEST_PROTOCOL)
# errors_more = []  
# errors_grouped = errors.groupby('work_id')
# for name, group in tqdm(errors_grouped, total = len(errors_grouped)):
#     # group = errors_grouped.get_group('ubcjk019438245')
#     try:
#         info = viaf_positives_dict[group['author_id'].to_list()[0]]['unidecode name'][0] + ' ' + Counter([marc_parser_dict_for_field(e, '\$')['$t'] for e in group['595'].to_list() if pd.notnull(e) and '$t' in e]).most_common(1)[0][0]
#         temp_list = [info, name, group.shape[0]]
#     except IndexError: temp_list = [None, name, group.shape[0]]
#     errors_more.append(temp_list)

# errors_to_viaf = pd.DataFrame([e for e in errors_more if e[0]], columns=['query', 'nkc_id', 'length'])
# errors_to_viaf.to_excel('nkc_ids_to_be_viafed.xlsx', index=False)

with open('viaf_and_clt_work_id.pickle', 'rb') as handle:
    work_viaf_clt = pickle.load(handle)
work_viaf_clt2 = literal_eval(open('viaf_clt_work_id_manual.txt').read())

work_viaf_clt = dict(zip([e[-1] for e in work_viaf_clt], [e[1] for e in work_viaf_clt]))
work_viaf_clt.update(work_viaf_clt2)
#775 dzieł połączonych między CLT i VIAF

translations_df['work_id'] = translations_df['work_id'].apply(lambda x: work_viaf_clt[x] if x in work_viaf_clt else x)

# clt_left = [e for e in enumerate(translations_df['work_id'].dropna().drop_duplicates().to_list(),translations_df.shape[0]) if any(el in e[-1] for el in ['ubc', 'ucb', 'abc'])]
clt_left = [e for e in enumerate(translations_df['work_id'].dropna().drop_duplicates().to_list(),translations_df.shape[0]) if any(l.isalpha() for l in e[-1])]
clt_left = dict(zip([e[-1] for e in clt_left], [str(e[0]) for e in clt_left]))

# any(e in list(clt_left.values()) for e in translations_df['work_id'].drop_duplicates().to_list())

translations_df['work_id'] = translations_df['work_id'].apply(lambda x: clt_left[x] if x in clt_left else x)

#w tym momencie mamy same viafy lub pseudo-viafy

viaf_work_list = [f'http://viaf.org/viaf/{e}' for e in translations_df['work_id'].dropna().drop_duplicates().to_list() if e not in list(clt_left.values())]

# viaf_work_dict = {}
# for url in tqdm(viaf_work_list):
#     result = requests.get(f'{url}/viaf.json').json()
#     try:
#         work_viaf = result['viafID']
#     except KeyError:
#         try:
#             result = result['scavenged']['VIAFCluster']
#             work_viaf = result['viafID']
#         except KeyError:
#             continue
#     try:
#         work_title = result['mainHeadings']['data']['text'].split('|')[-1].strip()
#     except TypeError:
#         if [e['text'] for e in result['mainHeadings']['data'] if '|' in e['text']]:
#             work_title = [e['text'] for e in result['mainHeadings']['data'] if '|' in e['text']][0].split('|')[-1].strip()
#         else:
#             work_title = result['mainHeadings']['data'][0]['text'].split('|')[-1].strip()
    
#     try:
#         author_name = result['titles']['author']['text']
#     except KeyError:
#         try:
#             author_name = result['mainHeadings']['data']['text'].split('|')[0].strip()
#         except TypeError:
#             author_name = marc_parser_dict_for_field(translations_df[translations_df['work_id'] == result['viafID']]['100'].to_list()[0], '\$')['$a']
#     try:
#         author_viaf = result['titles']['author']['@id'].split('|')[-1]
#     except KeyError:
#         try:
#             author_viaf = re.findall('\d+', [e for e in result['mainHeadings']['mainHeadingEl']['datafield']['subfield'] if e['@code'] == '0'][0]['#text'])[0]
#         except (TypeError, IndexError):
#             author_viaf = translations_df[translations_df['work_id'] == result['viafID']]['author_id'].to_list()[0]
#     try:
#         expressions = result['titles']['expression']
#         expressions_dict = {}
#         if isinstance(expressions, list):
#             for expression in result['titles']['expression']:
#                 translation_viaf = expression['@id'].split('|')[-1]
#                 try:
#                     translation_title = [e for e in expression['datafield']['subfield'] if e['@code'] == 't'][0]['#text']
#                 except KeyError:
#                     translation_title = np.nan
#                 try:
#                     translation_lang = expression['lang']
#                 except KeyError:
#                     translation_lang = np.nan
#                 try:
#                     translator = expression['translator']
#                 except KeyError:
#                     translator = np.nan
#                     if pd.notnull(translation_title) and pd.notnull(translation_lang):
#                         expressions_dict.update({translation_viaf:{'translation_viaf': translation_viaf,
#                                                                     'translation_title': translation_title,
#                                                                     'translation_language': translation_lang,
#                                                                     'translator': translator}})
#         else:
#             expression = expressions
#             translation_viaf = expression['@id'].split('|')[-1]
#             try:
#                 translation_title = [e for e in expression['datafield']['subfield'] if e['@code'] == 't'][0]['#text']
#             except KeyError:
#                 translation_title = np.nan
#             try:
#                 translation_lang = expression['lang']
#             except KeyError:
#                 translation_lang = np.nan
#             try:
#                 translator = expression['translator']
#             except KeyError:
#                 translator = np.nan
#             if pd.notnull(translation_title) and pd.notnull(translation_lang):
#                 expressions_dict.update({translation_viaf:{'translation_viaf': translation_viaf,
#                                                             'translation_title': translation_title,
#                                                             'translation_language': translation_lang,
#                                                             'translator': translator}})
              
#         viaf_work_dict.update({work_viaf:{'work_viaf': work_viaf,
#                                           'work_title': work_title,
#                                           'author_name': author_name,
#                                           'author_viaf': author_viaf,
#                                           'translations': expressions_dict}})
#     except KeyError:
#         pass

# #1686 work viaf_ids
# #1546 work ids with actual translations
# difference = [e for e in viaf_work_list if re.findall('\d+', e)[0] not in viaf_work_dict]

# with open('viaf_work_dict_pi.pickle', 'wb') as handle:
#     pickle.dump(viaf_work_dict, handle, protocol=pickle.HIGHEST_PROTOCOL)

# errorfile = io.open('work_viaf_wrong.txt', 'wt', encoding='UTF-8')    
# for element in difference:
#     errorfile.write(str(element) + '\n')
# errorfile.close()


with open('viaf_work_dict_pi.pickle', 'rb') as handle:
    viaf_work_dict = pickle.load(handle)
    
viaf_works_df = pd.DataFrame(viaf_work_dict.values())
viaf_works_df['translations'] = viaf_works_df['translations'].apply(lambda x: '|'.join(x.keys()))
viaf_works_df = cSplit(viaf_works_df, 'work_viaf', 'translations', '|')

viaf_translations_df = [[el for el in e['translations'].values()] for e in viaf_work_dict.values()]
viaf_translations_df = pd.DataFrame([e for sub in viaf_translations_df for e in sub])

viaf_works_df = pd.merge(viaf_works_df, viaf_translations_df, left_on='translations', right_on='translation_viaf', how='left').drop(columns='translations').rename(columns={'work_viaf':'work_id'})

work_corrections = {10000014022: str(max({int(v) for k,v in clt_left.items()})+4),
                    10000014527: str(max({int(v) for k,v in clt_left.items()})+4),
                    952429509: str(max({int(v) for k,v in clt_left.items()})+4),
                    10000000750: str(max({int(v) for k,v in clt_left.items()})+1),
                    10000007893: str(max({int(v) for k,v in clt_left.items()})+1),
                    10000000798: str(max({int(v) for k,v in clt_left.items()})+2),
                    10000014426: str(max({int(v) for k,v in clt_left.items()})+3),
                    10000003939: '310301410',
                    10000005830: '310301410',
                    10000006164: '310301410',
                    10000014707: '310301410',
                    10000014710: '310301410'}

translations_df['work_id'] = translations_df[['001', 'work_id']].apply(lambda x: work_corrections[x['001']] if x['001'] in work_corrections else x['work_id'], axis=1)

clt_titles = translations_df[translations_df['work_id'].notnull()][['001', 'SRC', 'work_id', '595']]
clt_titles['clt_title'] = clt_titles['595'].apply(lambda x: marc_parser_dict_for_field(x, '\$')['$t'].replace('❦', '').strip() if pd.notnull(x) and '$t' in x else np.nan)
clt_titles = clt_titles[['001', 'work_id', 'clt_title']]

orig_titles = pd.merge(clt_titles, viaf_works_df[['work_id', 'work_title']].drop_duplicates(), on='work_id', how='left')
orig_titles['work_title'] = orig_titles[['clt_title', 'work_title']].apply(lambda x: re.sub('(?<!\.)(\.)(?=$)', '', x['clt_title']) if pd.notnull(x['clt_title']) and pd.isnull(x['work_title']) else x['work_title'], axis=1)
orig_titles.loc[orig_titles['001'] == 10000008715, 'work_title'] = 'Nový epochální výlet pana Broučka tentokrát do patnáctého století'
orig_titles = orig_titles[['001', 'work_title']]

translations_df = pd.merge(translations_df, orig_titles, on='001', how='left')


#%% original title clusters

df_original_titles = translations_df.replace(r'^\s*$', np.nan, regex=True)  
df_original_titles = df_original_titles[(df_original_titles['work_id'].notnull()) |
                                        (df_original_titles['240'].notnull()) | 
                                        (df_original_titles['765']) |
                                        (df_original_titles['595'])][['001', '100', '240', '765', '595', 'author_id', 'work_id', 'work_title']]

df_original_titles = df_original_titles[df_original_titles['work_title'].isnull()]

df_original_titles['240'] = df_original_titles['240'].apply(lambda x: get_longest_cell(x))
df_original_titles['765'] = df_original_titles['765'].apply(lambda x: get_longest_cell(x))
df_original_titles['765_or_240'] = df_original_titles[['765', '240']].apply(lambda x: x['765'] if pd.notnull(x['765']) else x['240'], axis=1)
df_original_titles = df_original_titles[df_original_titles['765_or_240'].notnull()]
df_original_titles['765_or_240'] = df_original_titles['765_or_240'].apply(lambda x: x if x[0] != '$' else f'10{x}')
df_original_titles = df_original_titles.drop(columns=['765', '240'])

# df_original_titles_100 = marc_parser_1_field(df_original_titles, '001', '100', '\\$')[['001', '$a', '$d', '$1']].rename(columns={'$a':'name', '$d':'dates', '$1':'viaf'}).drop_duplicates().reset_index(drop=True)
# df_original_titles_100['dates'] = df_original_titles_100['dates'].apply(lambda x: x if x != '1\\' else np.nan)

df_original_titles_765_240 = marc_parser_1_field(df_original_titles, '001', '765_or_240', '\\$')
df_original_titles_765_240['$a'] = df_original_titles_765_240[['$a', '$t']].apply(lambda x: x['$t'] if x['$t'] != '' else x['$a'], axis=1)
df_original_titles_765_240['$l'] = df_original_titles_765_240[['$l', '$i']].apply(lambda x: 'Polish' if x['$i'] in ['Tyt. oryg.:', 'Tytuł oryginału:', 'Tyt. oryg. :', 'Tyt oryg.:'] else x['$l'], axis=1)
df_original_titles_765_240['$l'] = df_original_titles_765_240[['$l', '$9']].apply(lambda x: x['$9'] if x['$9'] != '' else x['$l'], axis=1)
df_original_titles_765_240 = df_original_titles_765_240[['001', '$a', '$b', '$l']].drop_duplicates()

try:
    df_original_titles_765_240['original title'] = df_original_titles_765_240.apply(lambda x: ''.join([x['$a'], x['$b']]) if x['$b'] != '' else x['$a'], axis=1)
except KeyError:
    df_original_titles_765_240['original title'] = df_original_titles_765_240['$a']
    
# df_original_titles_simple = pd.merge(df_original_titles_100, df_original_titles_765_240, how='left', on='001')
# df_original_titles_simple = df_original_titles_simple.merge(df_original_titles[['001', 'cluster_viaf']]).reset_index(drop=True)
# df_original_titles_simple['index'] = df_original_titles_simple.index+1

df_original_titles_simple = pd.merge(df_original_titles_765_240[['001', 'original title']], translations_df[['001', 'author_id', 'work_id', 'work_title']], on='001', how='outer')
df_original_titles_simple['original title'] = df_original_titles_simple[['original title', 'work_title']].apply(lambda x: x['original title'] if pd.notnull(x['original title']) else x['work_title'], axis=1)
df_original_titles_simple = df_original_titles_simple[df_original_titles_simple['original title'].notnull()]

df_original_titles_simple_grouped = df_original_titles_simple.groupby('author_id')

df_original_titles_simple = pd.DataFrame(columns=['001', 'original title', 'author_id', 'work_id', 'work_title', 'cluster'], dtype=str)
for name, group in tqdm(df_original_titles_simple_grouped, total=len(df_original_titles_simple_grouped)):
    # name = '56763450'
    # group = df_original_titles_simple_grouped.get_group(name)
    group['simple_original_title'] = group['original title'].apply(lambda x: simplify_string(x))
    # group = df_original_titles_simple_grouped.get_group('25096219')
    df = cluster_records(group, '001', ['simple_original_title'])
    # df['cluster'] = df['cluster'].astype(np.int64).astype(str)
    df_original_titles_simple = df_original_titles_simple.append(df)

df_original_titles_simple = df_original_titles_simple.sort_values(['author_id', 'cluster']).rename(columns={'cluster':'cluster_titles'})
# df_original_titles_simple['cluster_titles'] = df_original_titles_simple['cluster_titles'].astype(np.int64)
df_original_titles_simple['001'] = df_original_titles_simple['001'].astype(np.int64)

df_with_original_titles = pd.merge(translations_df, df_original_titles_simple[['001', 'original title', 'simple_original_title', 'cluster_titles']], on='001', how='left')

# df_with_original_titles.to_excel(f'df_with_title_clusters_{now}.xlsx', index=False)

def marc_parser_dict_for_field(string, subfield_code):
    subfield_list = re.findall(f'{subfield_code}.', string)
    for subfield in subfield_list:
        subfield_escape = re.escape(subfield)
        string = re.sub(f'({subfield_escape})', r'❦\1', string)
    string = [e.split('\n')[0] for e in string.split('❦') if e]
    dictionary_fields = [e for e in string if re.escape(e)[:len(subfield_code)] == subfield_code]
    dictionary_fields = [{subfield_list[i]:e[len(subfield_list[i]):]} for i, e in enumerate(dictionary_fields)]
    return dictionary_fields



#!!!!to pokazać OV!!!!!!


# test = df_with_original_titles[df_with_original_titles['author_id'] == '56763450']
# test = df_with_original_titles[df_with_original_titles['author_id'] == '51691735']

# #ile viaf
# print(Counter(test['work_id'].notnull()))
# #Counter({False: 511, True: 126}) 20%
# #ile clustrowanie
# print(Counter(test['cluster_titles'].notnull()))
# #Counter({False: 346, True: 291}) 46%

# test = test[['001', 'author_id', 'year', 'language', '245', 'work_id', 'work_title', 'original title', 'simple_original_title', 'cluster_titles']]

# test2 = test.groupby(['cluster_titles', 'simple_original_title']).size().reset_index(name="Time")

# test['simple_target_title'] = test['245'].apply(lambda x: ' '.join([simplify_string(list(e.values())[0]).strip() for e in marc_parser_dict_for_field(x, '\$') if any(el == list(e.keys())[0] for el in ['$a', '$b'])]))

# df = cluster_records(test, '001', ['simple_target_title'])

# cluster_freq = dict(Counter(test['cluster_titles']))
# for key, value in cluster_freq.items():
#     proper_name = dict(Counter(df[df['cluster_titles'] == key]['simple_original_title'].to_list()))
#     try:
#         proper_name = max(proper_name, key=proper_name.get)
#         cluster_freq[key] = (value, proper_name)
#     except ValueError: pass
    
# groupby = df.groupby('cluster')
# relacje_clustrow = []
# df_final = pd.DataFrame()
# for name, group in tqdm(groupby, total=len(groupby)):
#     # 27960929, 49947079, 52796098
#     # name = 50696693
#     # name = 27745720
#     # group = groupby.get_group(name)

#     clusters_from_245 = list(set(group['cluster_titles'].dropna().to_list()))
#     temp_dict = dict(Counter(group['cluster_titles'].dropna().to_list()))
#     temp_dict = {k:v/sum(temp_dict.values()) for k,v in temp_dict.items()}
#     if any(e > 0.85 for e in temp_dict.values()):
#         most_frequent_in_group = dict(Counter(group['cluster_titles'].dropna().to_list()))
#         most_frequent_in_group = {k:v/sum(most_frequent_in_group.values()) for k,v in most_frequent_in_group.items()}
#         most_frequent_in_group = max({k: v for k,v in most_frequent_in_group.items()}, key=most_frequent_in_group.get)
        
#         most_frequent_all = max({k: v for k,v in cluster_freq.items() if k in clusters_from_245}, key=cluster_freq.get)
        
#         mfg_title = cluster_freq[most_frequent_in_group][-1]
#         mfa_title = cluster_freq[most_frequent_all][-1]
        
#         if SequenceMatcher(a=mfg_title, b=mfa_title).ratio() > 0.9:
#             group['cluster_titles'] = most_frequent_all
#         else:
#             group['cluster_titles'] = most_frequent_in_group
#     else:
#         try:
#             relacje_clustrow.append(tuple(sorted({k: v for k,v in cluster_freq.items() if k in clusters_from_245}.items(), key=lambda x: x[1], reverse=True)))
#             proper_cluster = max({k: v for k,v in cluster_freq.items() if k in clusters_from_245}, key=cluster_freq.get)
#             #czy relacje tych clustrów są czymś, co mogę wykorzytać?
#             group['cluster_titles'] = proper_cluster
#         except ValueError: 
#             if group.shape[0] > 1:
#                 group['cluster_titles'] = name
#     df_final = df_final.append(group)

# print(Counter(df_final['cluster_titles'].notnull()))
# #Counter({True: 570, False: 67}) 89%

# #jak połączyć clustry, które odnoszą się do tego samego dzieła?
# relacje_clustrow = list(set([e for e in relacje_clustrow if e and len(e) > 1]))

# t = [[el[0] for el in e] for e in relacje_clustrow if e[0][0] == 1496749]
# t = list(set([e for sub in t for e in sub]))

# t = df[df['cluster_titles'].isin(t)]
# t = df[df['cluster_titles'].isin([244816843, 85143022, 76512129])]

# t = df[df['cluster_titles'].isin([320430736, 10000015208])]
# t = df[df['cluster_titles'].isin([51625712, 251769407, 71653533])] #??

# t = df[df['cluster_titles'].isin([76512129, 244816470, 85143022])]

# t = df[df['cluster_titles'].isin([76512129, 244816470, 85143022, 244816843, 85143022, 76512129])]



#całość

#stats
#ile viaf
print(Counter(df_with_original_titles['work_id'].notnull()))
#Counter({False: 42506, True: 12420}) 23%
#ile clustrowanie
print(Counter(df_with_original_titles['cluster_titles'].notnull()))
#Counter({False: 29430, True: 25496}) 46%

df_translations_simple = df_with_original_titles[['001', 'author_id', 'year', 'language', '245', 'work_id', 'work_title', 'original title', 'simple_original_title', 'cluster_titles']]
df_translations_simple['simple_target_title'] = df_translations_simple['245'].apply(lambda x: ' '.join([simplify_string(list(e.values())[0]).strip() for e in marc_parser_dict_for_field(x, '\$') if any(el == list(e.keys())[0] for el in ['$a', '$b'])]))

df_translations_grouped = df_translations_simple.groupby('author_id')
df_translations_simple = pd.DataFrame()
for name, group in tqdm(df_translations_grouped, total=len(df_translations_grouped)):
    group = cluster_records(group, '001', ['simple_target_title'])
    df_translations_simple = df_translations_simple.append(group)

cluster_freq = dict(Counter(df_translations_simple['cluster_titles'].dropna()))
for key, value in cluster_freq.items():
    proper_name = dict(Counter(df_translations_simple[df_translations_simple['cluster_titles'] == key]['simple_original_title'].to_list()))
    try:
        proper_name = max(proper_name, key=proper_name.get)
        cluster_freq[key] = (value, proper_name)
    except ValueError: pass
    
df_translations_grouped = df_translations_simple.groupby(['author_id', 'cluster'])
relacje_clustrow = []
df_final = pd.DataFrame()
for name, group in tqdm(df_translations_grouped, total=len(df_translations_grouped)):
    clusters_from_245 = list(set(group['cluster_titles'].dropna().to_list()))
    relacje_clustrow.append(tuple(sorted({k: v for k,v in cluster_freq.items() if k in clusters_from_245}.items(), key=lambda x: x[1], reverse=True)))
    
    temp_dict = dict(Counter(group['cluster_titles'].dropna().to_list()))
    temp_dict = {k:v/sum(temp_dict.values()) for k,v in temp_dict.items()}
    if any(e > 0.85 for e in temp_dict.values()):
        most_frequent_in_group = dict(Counter(group['cluster_titles'].dropna().to_list()))
        most_frequent_in_group = {k:v/sum(most_frequent_in_group.values()) for k,v in most_frequent_in_group.items()}
        most_frequent_in_group = max({k: v for k,v in most_frequent_in_group.items()}, key=most_frequent_in_group.get)
        
        most_frequent_all = max({k: v for k,v in cluster_freq.items() if k in clusters_from_245}, key=cluster_freq.get)
        
        mfg_title = cluster_freq[most_frequent_in_group][-1]
        mfa_title = cluster_freq[most_frequent_all][-1]
        
        if SequenceMatcher(a=mfg_title, b=mfa_title).ratio() > 0.9:
            group['cluster_titles'] = most_frequent_all
        else:
            group['cluster_titles'] = most_frequent_in_group
    else:
        try:
            # relacje_clustrow.append(tuple(sorted({k: v for k,v in cluster_freq.items() if k in clusters_from_245}.items(), key=lambda x: x[1], reverse=True)))
            proper_cluster = max({k: v for k,v in cluster_freq.items() if k in clusters_from_245}, key=cluster_freq.get)
            #czy relacje tych clustrów są czymś, co mogę wykorzytać?
            group['cluster_titles'] = proper_cluster
        except ValueError: 
            if group.shape[0] > 1:
                group['cluster_titles'] = name[-1]
    df_final = df_final.append(group)

print(Counter(df_final['cluster_titles'].notnull()))
#Counter({True: 47478, False: 7448}) 86%

relacje_clustrow = list(set([e for e in relacje_clustrow if e and len(e) > 1]))
with open('relacje_clustrow.pickle', 'wb') as handle:
    pickle.dump(relacje_clustrow, handle, protocol=pickle.HIGHEST_PROTOCOL)


df_final['001'] = df_final['001'].astype(np.int64)

df_with_original_titles = pd.merge(df_with_original_titles.drop(columns=['cluster_titles']), df_final[['001', 'simple_target_title', 'cluster_titles']], on='001', how='left')

df_with_original_titles.rename(columns={'work_id':'work_viaf_id', 'cluster_titles':'work_id'}, inplace=True)

df_with_original_titles.to_excel(f'translation_database_clusters_year_author_language_work_{now}.xlsx', index=False)


with open('relacje_clustrow.pickle', 'rb') as handle:
    relacje_clustrow = pickle.load(handle)

#%% deduplication

def marc_parser_dict_for_field(string, subfield_code):
    subfield_list = re.findall(f'{subfield_code}.', string)
    for subfield in subfield_list:
        subfield_escape = re.escape(subfield)
        string = re.sub(f'({subfield_escape})', r'❦\1', string)
    string = [e.split('\n')[0] for e in string.split('❦') if e]
    dictionary_fields = [e for e in string if re.escape(e)[:len(subfield_code)] == subfield_code]
    dictionary_fields = [{subfield_list[i]:e[len(subfield_list[i]):]} for i, e in enumerate(dictionary_fields)]
    return dictionary_fields
#56763450

# records_df = translations_df[translations_df['author_id'] == '56763450']
records_df = df_with_original_titles.copy()
records_grouped = records_df.groupby(['author_id', 'language', 'work_id'], dropna=False)

writer = pd.ExcelWriter(f'translation_deduplicated_{now}.xlsx', engine = 'xlsxwriter')
records_df.to_excel(writer, index=False, sheet_name='phase_0') # original data

# test = records_df.groupby(['author_id', 'language', 'work_id']).size().reset_index(name="Time")

phase_1 = pd.DataFrame() # duplicates
# phase_2 = pd.DataFrame() # multiple volumes
phase_2 = pd.DataFrame() # fuzzyness
phase_3 = pd.DataFrame() # ISBN

# grupy = []
for name, group in tqdm(records_grouped, total=len(records_grouped)):
    # name = ('56763450', 'ger', 1496749)
    # group = records_grouped.get_group(name)
    
    group['title'] = group['simple_target_title']
    try:
        group['place'] = group['260'].apply(lambda x: ' '.join([simplify_string(e['$a']).strip() for e in marc_parser_dict_for_field(x, '\$') if '$a' in e]))
    except TypeError:
        group['place'] = ''
    try:
        group['publisher'] = group['260'].apply(lambda x: ' '.join([simplify_string(e['$b']).strip() for e in marc_parser_dict_for_field(x, '\$') if '$b' in e]))
    except TypeError:
        group['publisher'] = ''
    
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
                if column in ['fiction_type', 'audience', '490', '500', '650', '655', '700']:
                    sub_group[column] = '❦'.join(sub_group[column].dropna().drop_duplicates().astype(str))
                else:
                    try:
                        sub_group[column] = max(sub_group[column].dropna().astype(str), key=len)
                    except ValueError:
                        sub_group[column] = np.nan
            df_oclc_deduplicated = df_oclc_deduplicated.append(sub_group)
        
        df_oclc_deduplicated = df_oclc_deduplicated.drop_duplicates().replace(r'^\s*$', np.nan, regex=True)
        df_oclc_deduplicated['001'] = df_oclc_deduplicated['001'].astype(np.int64)
        group = group.loc[~group['001'].isin(oclc_duplicates_list)]
        group = pd.concat([group, df_oclc_deduplicated])
        group['group_ids'] = group['group_ids'].apply(lambda x: '❦'.join(set(x.split('❦'))) if pd.notnull(x) else x)
        group = group.drop_duplicates().reset_index(drop=True)
        phase_1 = phase_1.append(group)
    else:
        group = group.drop_duplicates().reset_index(drop=True)
        phase_1 = phase_1.append(group)
    
#phase_2: de-duplication 2: multiple volumes TO JUŻ NIEPOTRZEBNE
        
#phase_2: de-duplication 3: fuzzyness
    group['year'] = group['year'].astype(str)
    df_oclc_clusters = cluster_records(group, '001', ['title', 'publisher', 'year'], 0.85)
    df_oclc_clusters['cluster'] = df_oclc_clusters['cluster'].fillna(0)
    df_oclc_clusters['cluster'] = df_oclc_clusters['cluster'].astype(np.int64)
        
    df_oclc_duplicates = pd.DataFrame()
    df_oclc_clusters_grouped = df_oclc_clusters.groupby(['cluster', 'year'])
    for subname, subgroup in df_oclc_clusters_grouped:
        # subname = (28788169, '1993')
        # subgroup = df_oclc_clusters_grouped.get_group(subname)
        if subgroup.shape[0] > 1 and [e for e in subgroup['cluster'].to_list() if e != 0]:
            lowest_id = min(subgroup['001'].to_list())
            subgroup['cluster'] = lowest_id
            df_oclc_duplicates = df_oclc_duplicates.append(subgroup)
    
    if df_oclc_duplicates.shape[0] > 0:
    
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
                if column in ['fiction_type', 'audience', '490', '500', '650', '655', '700']:
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
        df_oclc_deduplicated['001'] = df_oclc_deduplicated['001'].astype(np.int64)
        
        group = group.loc[~group['001'].isin(oclc_duplicates_list)]
        group = pd.concat([group, df_oclc_deduplicated])
        group['group_ids'] = group['group_ids'].apply(lambda x: '❦'.join(set(x.split('❦'))) if pd.notnull(x) else x)
        group = group.drop(columns=['cluster', 'title']).drop_duplicates().reset_index(drop=True)
        phase_2 = phase_2.append(group)
    else:
        group = group.drop(columns='title').drop_duplicates().reset_index(drop=True)
        phase_2 = phase_2.append(group)
    
#phase_3: ISBN  
           
    group['ISBN'] = group['020'].apply(lambda x: get_ISBNs(x))
    
    ISBN_dict = {}
    for i, row in group.iterrows():
        if row['ISBN'] != 'no ISBN':
            x = row['ISBN'].split('❦')
            for el in x:
                if (el, row['year']) in ISBN_dict:
                    ISBN_dict[(el, row['year'])].append(row['001'])
                else:
                    ISBN_dict[(el, row['year'])] = [row['001']]
    ISBN_dict = {k:tuple(v) for k,v in ISBN_dict.items() if len(v) > 1}
    ISBN_dict = list(set([v for k,v in ISBN_dict.items()]))
    ISBN_dict = {e[0]:e[1] for e in ISBN_dict}
    
    group['001'] = group['001'].replace(ISBN_dict)
    
    ISBN_grouped = group.groupby('001')
   
    group = pd.DataFrame()
    for sub_name, sub_group in ISBN_grouped:
        try:
            group_ids = '❦'.join(set([str(e) for e in sub_group['001'].to_list() + sub_group['group_ids'].to_list() if pd.notnull(e)]))
        except KeyError:
            group_ids = '❦'.join(set([str(e) for e in sub_group['001'].to_list() if pd.notnull(e)]))
        sub_group['group_ids'] = group_ids
        for column in sub_group:
            if column in ['fiction_type', 'audience', '490', '500', '650', '655', '700']:
                sub_group[column] = '❦'.join(sub_group[column].dropna().drop_duplicates().astype(str))
            else:
                try:
                    sub_group[column] = max(sub_group[column].dropna().astype(str), key=len)
                except ValueError:
                    sub_group[column] = np.nan
        group = group.append(sub_group)
    
    group = group.drop_duplicates().replace(r'^\s*$', np.nan, regex=True)
    group['001'] = group['001'].astype(np.int64)
    
    group['group_ids'] = group['group_ids'].apply(lambda x: '❦'.join(set(x.split('❦'))) if pd.notnull(x) else x)
    group = group.drop(columns='ISBN').drop_duplicates().reset_index(drop=True)
    phase_3 = phase_3.append(group)
         
phase_3_grouped = phase_3.groupby(['author_id', 'language', 'work_id', 'year'], dropna=False).filter(lambda x: len(x) > 1)
oclc_duplicates_list = phase_3_grouped['001'].drop_duplicates().tolist()
phase_3_grouped = phase_3_grouped.groupby(['author_id', 'language', 'work_id', 'year'], dropna=False)

groupby = pd.DataFrame()
for sub_name, sub_group in tqdm(phase_3_grouped, total=len(phase_3_grouped)):
    try:
        group_ids = '❦'.join(set([str(e) for e in sub_group['001'].to_list() + sub_group['group_ids'].to_list() if pd.notnull(e)]))
    except KeyError:
        group_ids = '❦'.join(set([str(e) for e in sub_group['001'].to_list() if pd.notnull(e)]))
    sub_group['group_ids'] = group_ids
    for column in sub_group:
        if column in ['fiction_type', 'audience', '490', '500', '650', '655', '700']:
            sub_group[column] = '❦'.join(sub_group[column].dropna().drop_duplicates().astype(str))
        else:
            try:
                sub_group[column] = max(sub_group[column].dropna().astype(str), key=len)
            except ValueError:
                sub_group[column] = np.nan
    groupby = groupby.append(sub_group)
      
groupby = groupby.drop_duplicates().replace(r'^\s*$', np.nan, regex=True)
groupby['001'] = groupby['001'].astype(np.int64)

phase_4 = phase_3.loc[~phase_3['001'].isin(oclc_duplicates_list)]
phase_4 = pd.concat([phase_4, groupby])
phase_4['group_ids'] = phase_4['group_ids'].apply(lambda x: '❦'.join(set(x.split('❦'))) if pd.notnull(x) else x)

phase_1.to_excel(writer, index=False, sheet_name='phase_1')    
phase_2.to_excel(writer, index=False, sheet_name='phase_2')  
phase_3.to_excel(writer, index=False, sheet_name='phase_3') 
phase_4.to_excel(writer, index=False, sheet_name='phase_4') 
writer.save()
writer.close()  

print(Counter(phase_4['work_id'] != 0))
# Counter({True: 25375, False: 7197}) 78%
#%% wizyta Czechów – przygotowanie do manualnego clustrowania
# translations_df = pd.read_excel('translation_deduplicated_2022-03-28.xlsx', sheet_name='phase_4')
translations_df = pd.read_excel('translation_deduplicated_with_geonames_2022-05-10.xlsx')

with open('translations_ov_more_to_delete.txt', 'rt') as f:
    to_del = f.read().splitlines()
to_del = [int(e) for e in to_del]
    
translations_df = translations_df[~translations_df['001'].isin(to_del)]

work_ids = dict(zip(translations_df['001'], translations_df['work_id']))
work_ids_list = list(work_ids.values())

#sizes of clusters
work_ids_counter = dict(Counter(work_ids_list))
work_ids_nan = {k:v for k,v in work_ids_counter.items() if pd.isnull(k)}
work_ids_numbers = {k:v for k,v in work_ids_counter.items() if pd.notnull(k) and v >= 10}

# max(work_ids_numbers, key=work_ids_numbers.get)
# work_ids_numbers[max(work_ids_numbers, key=work_ids_numbers.get)]

# temp_df = translations_df[translations_df['work_id'] == max(work_ids_numbers, key=work_ids_numbers.get)]

# no_of_authors = Counter(translations_df[translations_df['work_id'].isin(work_ids_numbers)]['author_id'])

# temp_df = translations_df[translations_df['work_id'].isin(work_ids_numbers)][['001', 'author_id', 'work_id']]

# temp_df = translations_df[translations_df['author_id'].isin(no_of_authors)][['001', 'author_id', 'work_id']]

#how many work_ids with more than 1 language

work_id_more_lang = translations_df.groupby(['work_id']).filter(lambda x: len(x) and len(set(x['language'])) >= 2)[['001', 'author_id', 'work_id', 'language']]

no_of_works = work_id_more_lang['work_id'].drop_duplicates().shape[0]
works = set(work_id_more_lang['work_id'])

#combining two approaches: size of a cluster >= 5 and at least 2 languages
work_ids = [e for e in work_ids_numbers if e in works]

work_ids_sizes = {}
for el in work_ids:
    size = translations_df[translations_df['work_id'] == el].shape[0]
    work_ids_sizes[el] = size

work_ids_sizes = sorted(work_ids_sizes.items(), key= lambda x: x[1], reverse=True)

proper_columns = ['001', '240', '245', '245a', 'language', '260', 'author_id', 'work_id', 'work_title', '490', '500', 'simple_original_title', '776', 'sorted']

work_id_author_id_dict = dict(zip(translations_df['work_id'], translations_df['author_id']))
work_id_author_id_dict = {k:v for k,v in work_id_author_id_dict.items() if k in work_ids}

gc = gs.oauth()
authors_present = {}
# na razie uwaględnione jest tylko pierwsze wystąpienie każdego autora
# 
for ind, (work_id, size) in enumerate(tqdm(work_ids_sizes, total=len(work_ids_sizes)),1):
    # ind = 1
    # work_id, size = work_ids_sizes[ind]
    ind = '{:03d}'.format(ind)
    author_id = work_id_author_id_dict[work_id]
    if author_id in authors_present:
        authors_present[author_id] += 1
        continue
    else:
        authors_present[author_id] = 1
    author_df = translations_df[translations_df['author_id'] == author_id]
    author_df['245a'] = author_df['245'].apply(lambda x: marc_parser_dict_for_field(x, '\$')['$a'])
    author_df = author_df[author_df.columns.intersection(proper_columns)]
    
    temp_groupby = author_df.groupby('work_id')
    work_cluster_sizes = dict(author_df.groupby('work_id')['work_id'].count())
    author_df['sorted'] = author_df['work_id'].apply(lambda x: work_cluster_sizes[x] if x in work_cluster_sizes else np.nan)
    author_df = author_df.sort_values(['sorted', 'work_id'], ascending=[False, False]).drop(columns='sorted')
    
    cluster_dictribution = dict(author_df.groupby('work_id')['work_id'].count().div(len(author_df)))
    (clusters, y) = zip(*dict(sorted(cluster_dictribution.items(), key=lambda item: item[1], reverse=True)).items())
    x = tuple([i for i, e in enumerate(clusters,1)])
    try:
        kn = KneeLocator(x, y, curve='convex', direction='decreasing')
        try:
            cluster_index = round(kn.knee/2)-1
        except TypeError:
            cluster_index = 1
        clusters_to_filter = list(clusters)[cluster_index+1:]
        clusters_to_filter.append(work_id)
        cluster_df = author_df[(author_df['work_id'].isin(clusters_to_filter)) |
                               (author_df['work_id'].isna())]
    except ValueError:
        cluster_df = author_df.copy()
    cluster_df['to_retain'] = np.nan
    cluster_df = cluster_df[['001', '240', 'to_retain', '245', '245a', 'language', '260', '490', '500', '776', 'author_id', 'work_title', 'simple_original_title', 'work_id']]
    
    # sheet = gc.create(f'{ind}_{author_id}_{int(work_id)}_{authors_present[author_id]}', '1x1ywDyyV-YwozVuV0B38uG7CH6mOe3OF')
    sheet = gc.create(f'{ind}_{author_id}_{int(work_id)}_{authors_present[author_id]}', '1CJwe0Bl-exd4aRyqCMqv_XHSyLuE2w4m')
    try:
        create_google_worksheet(sheet.id, str(int(work_id)), cluster_df)
    except Exception:
        time.sleep(60)
        create_google_worksheet(sheet.id, str(int(work_id)), cluster_df)
    except KeyboardInterrupt:
        raise

#wprowadzić system aktualizacji na podstawie manualnych prac Ondreja!!!







































#testing Babicka
author_df = translations_df[translations_df['author_id'] == '56763450']
author_df = translations_df[translations_df['author_id'] == '52272']
author_df = translations_df[translations_df['author_id'] == '51691735']
author_df = translations_df[translations_df['author_id'] == '34458072']

temp_groupby = author_df.groupby('work_id')

test = dict(author_df.groupby('work_id')['work_id'].count())
author_df['sorted'] = author_df['work_id'].apply(lambda x: test[x] if x in test else np.nan)
author_df = author_df.sort_values(['sorted', 'work_id'], ascending=[False, False])
author_df = author_df[author_df.columns.intersection(proper_columns)]


# temp_df = sorted(temp_groupby,  # iterates pairs of (key, corresponding subDataFrame)
#                  key=lambda x: len(x[1]),  # sort by number of rows (len of subDataFrame)
#                  reverse=True)  # reverse the sort i.e. largest first

# temp_df = pd.concat([e[-1] for e in temp_df])

# temp_df = temp_df.append(author_df[~author_df['001'].isin(temp_df['001'])])

# temp_df = temp_df[temp_df.columns.intersection(proper_columns)]



temp_dict = dict(author_df.groupby('work_id')['work_id'].count().div(len(author_df)))

(x, y) = zip(*dict(sorted(temp_dict.items(), key=lambda item: item[1], reverse=True)).items())
x = tuple([i for i, e in enumerate(x,1)])
kn = KneeLocator(x, y, curve='convex', direction='decreasing')
plt.plot(x, y, 'bx-')
plt.vlines(round(kn.knee/2)-1, plt.ylim()[0], plt.ylim()[1], linestyles='dashed')


plt.scatter(x, y, alpha=0.5)
plt.show()
plt.savefig('translations_works.jpg')


fig = author_df.groupby('work_id')['work_id'].count().div(len(author_df)).plot(kind='scatter', figsize = (40,10), title='work_id', legend=True, grid=True, lw=4).get_figure()
fig.savefig('translations_works.jpg')


# Generate a rank column that will be used to sort
# the dataframe numerically
df['Tm_Rank'] = df['Tm'].map(sorterIndex)

                                                                            
           


























                                                                 
#%% genre and audience

translations_df = phase_4.copy()

translations_df = pd.read_excel('translation_deduplicated_2022-03-28.xlsx', sheet_name='phase_4')
ttt_df = translations_df.copy()
translations_df = ttt_df.copy()
#28523

translations_df['cluster_genre'] = translations_df['fiction_type'].apply(lambda x: genre(x))
# test = translations_df[['001', 'cluster_genre', 'fiction_type']]
genre_groupby = translations_df.groupby(['author_id', 'work_id'], dropna=False)
# ttt = translations_df.groupby(["author_id", "work_id"]).size()

translations_df = pd.DataFrame()
for name, group in tqdm(genre_groupby, total=len(genre_groupby)):
    # name = ('4931097', 593302)
    # group = genre_groupby.get_group(name)
    if pd.notnull(name[-1]):
        group['genre'] = genre_algorithm(group)
    translations_df = translations_df.append(group)
    
# ttt = translations_df[['001', 'fiction_type', 'cluster_genre', 'genre']] 

translations_df['audience_label'] = translations_df['audience'].apply(lambda x: get_audience(x))

# ttt = translations_df[['001', 'audience', 'audience_label']]

translations_df.to_excel(f'translation_database_clusters_year_author_language_work_genre_audience_{now}.xlsx', index=False)

translations_df = pd.read_excel('translation_database_clusters_year_author_language_work_genre_audience_2022-03-29.xlsx')


test = translations_df[['001', 'author_id', 'work_id', 'simple_original_title', 'simple_target_title', 'fiction_type', 'cluster_genre']]
test.loc[test['cluster_genre'].isnull(), 'cluster_genre'] = 'other'
test = test[test['work_id'].notnull()]

genre_groupby = test.groupby(['author_id', 'work_id'], dropna=False)
test = pd.DataFrame()
for name, group in tqdm(genre_groupby, total=len(genre_groupby)):
    # name = 122727892
    # group = groupby.get_group(name)
    original_title = Counter(group['simple_original_title'].to_list()).most_common(1)[0][0]
    if pd.notnull(original_title):
        group['simple_original_title'] = original_title
    elif name != 0:
        group['simple_original_title'] = Counter(group['simple_target_title'].to_list()).most_common(1)[0][0]
    else:
        group['simple_original_title'] = '[no title]'
    test = test.append(group)

genre_groupby = test.groupby(['author_id', 'work_id'], dropna=False)
ttt = pd.DataFrame()
for name, group in tqdm(genre_groupby, total=len(genre_groupby)):
    # name = ('4931097', 593302)
    # group = genre_groupby.get_group(name)
    if pd.notnull(name[-1]):
        tete = [e.split('❦') for e in group['cluster_genre'].to_list()]
        tete = [e for sub in tete for e in sub]
        length = len(tete)
        tete = dict(Counter(tete))
        tete = {k:v/length for k,v in tete.items()}
        for el in tete:
            group[el] = tete[el]
    ttt = ttt.append(group)

ttt2 = ttt.drop(columns=['001', 'fiction_type', 'cluster_genre', 'simple_target_title']).drop_duplicates().sort_values(['author_id', 'work_id'])

languages = dict(Counter(translations_df['language'].to_list()))
languages = dict(sorted(languages.items(), key=lambda item: item[1], reverse=True))
languages = list(languages.items())







test = translations_df[['001', 'author_id', 'work_id', 'simple_original_title', 'language', 'year', 'fiction_type', 'cluster_genre', 'genre', 'audience', 'audience_label', 'place']]











def audience_algorithm(df):
    x = [e.split('❦') for e in df['audience_label'].to_list()]
    x = [e for sub in x for e in sub]
    length = len(x)
    x = Counter(x)
    if x['Juvenile']/length > 0.1:
        return 'Juvenile'
    elif x['General']/length > 0.1:
        return 'General'
    else:
        return 'other'

audience_groupby = translations_df.groupby(['author_id', 'work_id'], dropna=False)
test = pd.DataFrame()
for name, group in tqdm(audience_groupby, total=len(audience_groupby)):
    # name = ('4931097', 593302)
    # group = genre_groupby.get_group(name)
    if pd.notnull(name[-1]):
        group['final_audience'] = audience_algorithm(group)
    test = test.append(group)

places = test['place'].drop_duplicates().to_list()


ttt = translations_df[translations_df['place'] == 'praha bratislava'][['260', 'place']]













































#%% quality index
# df_with_original_titles = pd.read_excel('df_with_title_clusters_2021-11-29.xlsx')
# df_with_original_titles = pd.read_excel('df_with_title_clusters_2021-12-16.xlsx')
df_with_original_titles.loc()[df_with_original_titles['work_viaf'] == '1598147270543735700005', 'work_title'] = np.nan
df_with_original_titles.loc()[df_with_original_titles['work_viaf'] == '1598147270543735700005', 'work_viaf'] = np.nan
df_with_original_titles.loc()[df_with_original_titles['001'] == 908861268, 'cluster_titles'] = np.nan

df_with_original_titles['quality_index'] = df_with_original_titles.apply(lambda x: quality_index(x), axis=1)  

test = df_with_original_titles[['001', 'language', 'SRC', '240', '245', '260', '765', 'work_title', 'quality_index']]
quality_results = Counter(df_with_original_titles['quality_index'])

correct = df_with_original_titles[df_with_original_titles['quality_index'] > 0.7]
not_correct = df_with_original_titles[df_with_original_titles['quality_index'] <= 0.7]

writer = pd.ExcelWriter(f'translation_database_clusters_with_quality_index_{now}.xlsx', engine = 'xlsxwriter')
correct.to_excel(writer, index=False, sheet_name='HQ')
not_correct.to_excel(writer, index=False, sheet_name='LQ')
writer.save()
writer.close()



#%% sprawdzenie po wzbogaceniu o tytuły oryginałów

#pokazać OV!!!

# df_with_original_titles = pd.read_excel('df_with_title_clusters_2021-11-29.xlsx')

with open('viaf_work_dict_pi.pickle', 'rb') as handle:
    viaf_work_dict = pickle.load(handle)

ttt = df_with_original_titles.copy()[['001', 'cluster_viaf', '100_unidecode', '245', 'language', 'work_viaf', 'work_title', 'original title', 'cluster_titles', 'quality_index']].sort_values('cluster_titles')

#problemy
# problemy_01 = ttt[ttt['work_viaf'] == '1598147270543735700005']
#oclc ma błędny viaf w 245!!!!


# not_in_relation = test_df.loc()[(test_df['work_viaf'].isnull()) & (test_df['cluster_titles'].isnull())]

# teste = ttt[ttt['work_viaf'].isin(['2140157828009554550009', '309414925', '6795151965300000470007'])]

cluster_author_work = ttt[(ttt['work_viaf'].notnull()) &
                          (ttt['cluster_titles'].notnull())][['cluster_viaf', 'work_viaf', 'cluster_titles']].drop_duplicates()

# a_grouped = cluster_author_work.groupby(['cluster_viaf', 'work_viaf'])
# new_1 = pd.DataFrame()
# for name, group in a_grouped:
#     if group.shape[0] > 1:
#         new_1 = new_1.append(group)
# b_grouped = cluster_author_work.groupby(['cluster_viaf', 'cluster_titles'])
# new_2 = pd.DataFrame()
# for name, group in b_grouped:
#     if group.shape[0] > 1:
#         new_2 = new_2.append(group)

cluster_viaf_title_grouped = cluster_author_work.groupby(['cluster_viaf', 'work_viaf'])

work_viaf_and_cluster_titles = []
for name, group in tqdm(cluster_viaf_title_grouped, total=len(cluster_viaf_title_grouped)):
    work_viaf_and_cluster_titles.append({'cluster_viaf': (name[0],), 'viafs':tuple(sorted([name[-1]])), 'clusters':tuple(sorted([int('{:.0f}'.format(e)) for e in list(set(group['cluster_titles'].to_list())) if pd.notnull(e)]))})

def check_dicts_intersec(dict1, dict2):
    if all([bool(set(val).intersection(dict2.get(key))) for key, val in dict1.items() if key in ['cluster_viaf', 'clusters']]):
        return True
    else:
        return False
        
def merge_if_intersect(dict1, dict2):
    if check_dicts_intersec(dict1, dict2):
        output_dict = {key: tuple(sorted(set(value + dict2[key]))) for key, value in dict1.items()}
    else: output_dict = {}
    return output_dict

# dict2 = [e for e in work_viaf_and_cluster_titles if '176421014' in e['viafs']][0]
# dict1 = [e for e in work_viaf_and_cluster_titles if '179146462549027771326' in e['viafs']][0]
# merge_if_intersect(dict1, dict2)

# work_viaf_and_cluster_titles = [e for e in work_viaf_and_cluster_titles if '34458072' in e['cluster_viaf']]
                  
# jak to wpisać w while? że stara liczba != nowa liczba  

number = 0
final_list = []
control_list = work_viaf_and_cluster_titles[:]
while len(control_list) != len(final_list):
    list_of_merged_dicts = []
    popped_dicts = []
    for dict1 in tqdm(work_viaf_and_cluster_titles):
        for dict2 in work_viaf_and_cluster_titles:
            if dict1 != dict2:
                temp_dict = merge_if_intersect(dict1, dict2)
                if temp_dict:
                    list_of_merged_dicts.append(temp_dict)
                    popped_dicts.append({key: tuple(sorted(set(value + dict1[key]))) for key, value in dict1.items()})
                    popped_dicts.append({key: tuple(sorted(set(value + dict2[key]))) for key, value in dict2.items()})
        
    popped_dicts = list(map(dict, set(tuple(sorted(sub.items())) for sub in popped_dicts)))  
    list_of_merged_dicts = list(map(dict, set(tuple(sorted(sub.items())) for sub in list_of_merged_dicts)))  
    
    final_list = [e for e in work_viaf_and_cluster_titles if e not in popped_dicts]
    final_list.extend(list_of_merged_dicts)
    control_list = work_viaf_and_cluster_titles[:]
    work_viaf_and_cluster_titles = final_list[:]
    number += 1
    print(number)

test_df = ttt.copy()  
# ttt = test_df.copy()   
ttt['cluster_titles'] = ttt['cluster_titles'].apply(lambda x: x if pd.notnull(x) else 0)  
for dictionary in tqdm(final_list):
    proper_work_viaf = min(dictionary['viafs'])
    ttt.loc()[(ttt['work_viaf'].isin(dictionary['viafs'])) | 
              (ttt['cluster_titles'].isin(dictionary['clusters'])), 'work_viaf'] = proper_work_viaf
    ttt.loc()[(ttt['work_viaf'].isin(dictionary['viafs'])) | 
              (ttt['cluster_titles'].isin(dictionary['clusters'])), 'cluster_titles'] = proper_work_viaf
    # proper_title = Counter(ttt.loc()[(ttt['work_viaf'].isin(dictionary['viafs'])) | 
    #                                  (ttt['cluster_titles'].isin(dictionary['clusters']))]['work_title'].dropna()).most_common(1)[0][0]
    proper_title = viaf_work_dict[proper_work_viaf]['work_title']
    ttt.loc()[(ttt['work_viaf'].isin(dictionary['viafs'])) | 
              (ttt['cluster_titles'].isin(dictionary['clusters'])), 'work_title'] = proper_title
    
# ttt['cluster_titles'] = ttt['cluster_titles'].map(int)   
ttt['cluster_titles'] = ttt.apply(lambda x: x['work_viaf'] if x['cluster_titles'] == 0 and pd.notnull(x['work_viaf']) else x['cluster_titles'], axis=1)
#statystyki
Counter(ttt['cluster_titles'] != 0)

original_and_translations_dict = {k:{k2:v2 for k2,v2 in v.items() if k2 in ['author_viaf', 'work_title', 'translations']} \
                                  for k,v in viaf_work_dict.items()}
org_and_trans_dict = {}
for k,v in original_and_translations_dict.items():
    # k,v = list(original_and_translations_dict.items())[0]
    temp_dict = {}
    temp_dict.update({k:{}})
    for k2,v2 in v.items():
        if k2 in ['author_viaf', 'work_title']:
            temp_dict[k].update({k2:v2})
        else:
            trans_titles = []
            for k3,v2 in v2.items():
                trans_titles.append(v2['translation_title'])
            trans_titles = list(set([simplify_string(e).strip() for e in trans_titles]))
            temp_dict[k]['translations'] = trans_titles
    org_and_trans_dict.update(temp_dict)
        
# teraz wzbogacić org_and_trans_dict parami z ttt        

org_and_trans_biblio = {}

ttt_grouped = ttt.groupby('work_viaf')

for name, group in tqdm(ttt_grouped, total=len(ttt_grouped)):
    # name = '746145424543786830041'
    # group = ttt_grouped.get_group(name)
    k = name
    temp_dict = {}
    if k not in org_and_trans_biblio:
        temp_dict.update({k:{}})
    work_title = group['work_title'].to_list()[0]
    author_viaf = list(set(group['cluster_viaf'].to_list()))
    temp_dict[k].update({'work_title': work_title, 'author_viaf': author_viaf})
    translations_list = list(set([simplify_string(marc_parser_dict_for_field(e, '\$')['$a']).strip() for e in group['245'].to_list()]))
    translations_list = [e for e in translations_list if e]
    temp_dict[k].update({'translations': translations_list})
    org_and_trans_biblio.update(temp_dict)


# tttt = ttt[ttt['work_viaf'] == '1097153063212219320003']
# tttt = ttt[ttt['work_viaf'] == '12145911097927061907']
#!!!!!!!!!multiple_authors!!!!!!!!
#tutaj trzeba manualnych prac
# multiple_authors = {k:v for k,v in org_and_trans_biblio.items() if len(v['author_viaf']) > 1}
# multiple_authors_df = ttt[ttt['work_viaf'].isin(multiple_authors.keys())].sort_values(['work_viaf', 'cluster_viaf'])

#hardcoding
proper_authors_dict = {'1097153063212219320003': ['2958781'],
                       '1442145424587186830717': ['34458072'],
                       '1719149619404004010000': ['114463286'],
                       '176421014': ['4931097'],
                       '183690272': ['76500434'],
                       '2140157828009554550009': ['2958781'],
                       '2503154801896656310000': ['2958781'],
                       '306998033': ['2958781'],
                       '307468389': ['76500434'],
                       '310934436': ['2958781'],
                       '310961694': ['2958781'],
                       '5245158551010816540000': ['2504978'],
                       '6999155566443813380005': ['79081562'],
                       '9042154801924856310003': ['2958781'],
                       '9728157828069554550001': ['2958781']}
#hardcoding w słowniku biblio i słowniku viaf
org_and_trans_biblio = {k: {k2:(proper_authors_dict[k] if k2 == 'author_viaf' and k in proper_authors_dict else v2) for k2,v2 in v.items()} \
                        for k,v in org_and_trans_biblio.items()}
org_and_trans_dict = {k: {k2:(proper_authors_dict[k][0] if k2 == 'author_viaf' and k in proper_authors_dict else v2) for k2,v2 in v.items()} \
                        for k,v in org_and_trans_dict.items()}
#hardcoding w tabeli
ttt['cluster_viaf'] = ttt[['cluster_viaf', 'work_viaf']].apply(lambda x: proper_authors_dict[x['work_viaf']][0] if pd.notnull(x['work_viaf']) and x['work_viaf'] in proper_authors_dict else x['cluster_viaf'], axis=1)
#czy hardcodować też w tabeli głównej?
#???????????????
#połączyć dicts

org_and_trans_total = deepcopy(org_and_trans_biblio)
for k,v in tqdm(org_and_trans_total.items()):
    org_and_trans_total[k]['translations'].extend(org_and_trans_dict[k]['translations'])
    org_and_trans_total[k]['translations'] = list(set(org_and_trans_total[k]['translations']))
    # tu trzeba dodać warunek, żeby autorzy byli ci sami, a nie appendować
    org_and_trans_total[k]['author_viaf'].append(org_and_trans_dict[k]['author_viaf'])
    org_and_trans_total[k]['author_viaf'] = list(set(org_and_trans_total[k]['author_viaf']))

#czemu i co to oznacza?
# roznica = {k:v for k,v in org_and_trans_dict.items() if k not in org_and_trans_total}
#problemy
#tutaj trzeba manualnych prac
# multiple_authors2 = {k:v for k,v in org_and_trans_total.items() if len(v['author_viaf']) > 1}
# multiple_authors_df2 = ttt[ttt['work_viaf'].isin(multiple_authors2.keys())].sort_values(['work_viaf', 'cluster_viaf'])
# aaa = multiple_authors_df2[multiple_authors_df2['work_viaf'] == '5411153954900905680008']

records_to_delete = ['182253235', '309393708', '310216967']
ttt = ttt[~(ttt['work_viaf'].isin(records_to_delete)) | (ttt['work_viaf'].isnull())]
org_and_trans_total = {k:v for k,v in org_and_trans_total.items() if k not in records_to_delete}

proper_authors_dict = {'1292152503080710800006': ['109312616'],
'1598147270543735700005': ['109312616'],
'1695145424610786831026': ['29835535'],
'1750151474855100490002': ['66469255'],
'1879145424609386831043': ['24614627'],
'1885159764103408170004': ['66469255'],
'2117152502880410800000': ['34499285'],
'2529159764104908170009': ['29835535'],
'2658156497246117740003': ['2958781'],
'305991439': ['7402030'],
'307099584': ['297343866'],
'307145067344566631201': ['4931097'],
'309258849': ['125283'],
'309362028': ['24614627'],
'309414843': ['2958781'],
'310302495': ['2958781'],
'310327107': ['45112529'],
'310331147': ['76500434'],
'310363659': ['12459159'],
'310405572': ['2958781'],
'310446156': ['34454129'],
'310940822': ['2958781'],
'310949849': ['4931097'],
'314607673': ['24614627'],
'315483043': ['96705622'],
'316306628': ['3028585'],
'316754488': ['62360050'],
'3238151474902400490001': ['163185334'],
'3514151656275808400009': ['66469255'],
'3801152502723710800005': ['7423797'],
'4327152502807510800006': ['54147355'],
'4520159764085808170001': ['56651696'],
'4792152502891410800002': ['34464240'],
'5411153954900905680008': ['71466298'],
'6005154076042011860009': ['39632022'],
'7292159764092308170004': ['45112529'],
'746145424543786830041': ['34454129'],
'7905152502822210800000': ['116307890'],
'8117154801963756310000': ['2958781'],
'8394159764095008170007': ['4931097'],
'8774154622417044710008': ['2958781'],
'9165153289923132770009': ['56651696'],
'9362157527304627300005': ['125283'],
'9452151656217608400003': ['45112529'],
'9672147727675064710000': ['29835535'],
'9933151051972733530006': ['45112529']}
#hardcoding w słowniku biblio i słowniku viaf
org_and_trans_total = {k: {k2:(proper_authors_dict[k] if k2 == 'author_viaf' and k in proper_authors_dict else v2) for k2,v2 in v.items()} \
                        for k,v in org_and_trans_total.items()}
#hardcoding w tabeli
ttt['cluster_viaf'] = ttt[['cluster_viaf', 'work_viaf']].apply(lambda x: proper_authors_dict[x['work_viaf']][0] if pd.notnull(x['work_viaf']) and x['work_viaf'] in proper_authors_dict else x['cluster_viaf'], axis=1)
#czy hardcodować też w tabeli głównej?
#???????????????

# szukanie po target titles
test = ttt.loc()[ttt['work_viaf'].isna()][['001', '245', 'cluster_viaf']].to_dict(orient='records')
for d in tqdm(test):
    # d = test[15]
    simple_245 = marc_parser_dict_for_field(d['245'], '\$')
    try:
        simple_245.pop('$c')
    except KeyError:
        pass
    simple_245 = simplify_string(''.join([e.strip() for e in simple_245.values()]))
    d.update({'simple': simple_245})

viaf_works_df = pd.DataFrame.from_dict(org_and_trans_total, orient='index').reset_index().rename(columns={'index': 'work_viaf'})
viaf_works_df = viaf_works_df.explode('translations')
viaf_works_df = viaf_works_df.explode('author_viaf')
test_viaf = viaf_works_df.to_dict(orient='records')

for i, el in tqdm(enumerate(test), total=len(test)):
    for element in test_viaf:
        if element['translations'] in el['simple'] and el['cluster_viaf'] == element['author_viaf']:
            test[i].update(element)


work_with_viaf_id_simple_2 = pd.DataFrame([e for e in test if len(e) > 4])[['001', 'work_viaf', 'work_title']]

ttt_upgrade = deepcopy(ttt).reset_index(drop=True)
for i, row in tqdm(ttt_upgrade.iterrows(), total=ttt_upgrade.shape[0]):
    # i = 112
    # row = ttt_upgrade.iloc[i,:]
    if not work_with_viaf_id_simple_2[work_with_viaf_id_simple_2['001'] == row['001']]['work_viaf'].empty:
        ttt_upgrade.at[i, 'work_viaf'] = work_with_viaf_id_simple_2[work_with_viaf_id_simple_2['001'] == row['001']]['work_viaf'].to_string(index=False)
        ttt_upgrade.at[i, 'work_title'] = work_with_viaf_id_simple_2[work_with_viaf_id_simple_2['001'] == row['001']]['work_title'].to_string(index=False)


#ładny efekt
ttt10 = ttt_upgrade[(ttt_upgrade['work_viaf'] == '309278823') | (ttt_upgrade['cluster_titles'] == 25)]
ttt10 = ttt_upgrade[(ttt_upgrade['work_viaf'] == '7945157416866716710002') | (ttt_upgrade['cluster_titles'] == 62)]
#statystyki
Counter((ttt_upgrade['cluster_titles'] != 0) | (ttt_upgrade['work_viaf'].notnull()))
tttt = ttt_upgrade[ttt_upgrade['work_viaf'] == '12145911097927061907']

#!!!!!!!!TUTAJ!!!!!!!!!!!

cluster_author_work = ttt_upgrade[(ttt_upgrade['work_viaf'].notnull()) &
                                  (ttt_upgrade['cluster_titles'] != 0)][['cluster_viaf', 'work_viaf', 'cluster_titles']].drop_duplicates()
cluster_viaf_title_grouped = cluster_author_work.groupby(['cluster_viaf', 'work_viaf'])

work_viaf_and_cluster_titles = []
for name, group in tqdm(cluster_viaf_title_grouped, total=len(cluster_viaf_title_grouped)):
    work_viaf_and_cluster_titles.append({'cluster_viaf': (name[0],), 'viafs':tuple(sorted([name[-1]])), 'clusters':tuple(sorted([int('{:.0f}'.format(e)) if isinstance(e, float) else int(e) for e in list(set(group['cluster_titles'].to_list())) if pd.notnull(e) and e != 0]))})

number = 0
final_list = []
control_list = work_viaf_and_cluster_titles[:]
while len(control_list) != len(final_list):
    list_of_merged_dicts = []
    popped_dicts = []
    for dict1 in tqdm(work_viaf_and_cluster_titles):
        for dict2 in work_viaf_and_cluster_titles:
            if dict1 != dict2:
                temp_dict = merge_if_intersect(dict1, dict2)
                if temp_dict:
                    list_of_merged_dicts.append(temp_dict)
                    popped_dicts.append({key: tuple(sorted(set(value + dict1[key]))) for key, value in dict1.items()})
                    popped_dicts.append({key: tuple(sorted(set(value + dict2[key]))) for key, value in dict2.items()})
        
    popped_dicts = list(map(dict, set(tuple(sorted(sub.items())) for sub in popped_dicts)))  
    list_of_merged_dicts = list(map(dict, set(tuple(sorted(sub.items())) for sub in list_of_merged_dicts)))  
    
    final_list = [e for e in work_viaf_and_cluster_titles if e not in popped_dicts]
    final_list.extend(list_of_merged_dicts)
    control_list = work_viaf_and_cluster_titles[:]
    work_viaf_and_cluster_titles = final_list[:]
    number += 1
    print(number)

test_df = ttt_upgrade.copy()  

for dictionary in tqdm(final_list):
    proper_work_viaf = min(dictionary['viafs'])
    ttt_upgrade.loc()[(ttt_upgrade['work_viaf'].isin(dictionary['viafs'])) | 
                      (ttt_upgrade['cluster_titles'].isin(dictionary['clusters'])), 'work_viaf'] = proper_work_viaf
    ttt_upgrade.loc()[(ttt_upgrade['work_viaf'].isin(dictionary['viafs'])) | 
                      (ttt_upgrade['cluster_titles'].isin(dictionary['clusters'])), 'cluster_titles'] = proper_work_viaf
    proper_title = org_and_trans_total[proper_work_viaf]['work_title']
    ttt_upgrade.loc()[(ttt_upgrade['work_viaf'].isin(dictionary['viafs'])) | 
                      (ttt_upgrade['cluster_titles'].isin(dictionary['clusters'])), 'work_title'] = proper_title
    
  
ttt_upgrade['cluster_titles'] = ttt_upgrade.apply(lambda x: x['work_viaf'] if x['cluster_titles'] == 0 and pd.notnull(x['work_viaf']) else x['cluster_titles'], axis=1)
#statystyki
Counter(test_df['cluster_titles'] != 0)
Counter((ttt_upgrade['cluster_titles'] != 0))

aaa = ttt_upgrade[ttt_upgrade['work_viaf'] == '176421014']

df_with_original_titles = pd.merge(ttt_upgrade, df_with_original_titles.drop(columns=['cluster_viaf', '100_unidecode', '245', 'language', 'work_viaf', 'work_title', 'original title', 'cluster_titles', 'quality_index']), how='left', on='001')



#17.01.2022
df_with_original_titles = pd.read_excel('C:\\Users\\Cezary\\Downloads\\translation_database_with_viaf_work.xlsx')
print(Counter(df_with_original_titles['cluster_titles'] != 0))
ttt = df_with_original_titles.copy()[['001', 'cluster_viaf', '100_unidecode', '245', 'language', 'work_viaf', 'work_title', 'original title', 'cluster_titles', 'quality_index2']]

#słownik z czeskich danych

cz_translation_database = pd.read_csv("C:\\Users\\Cezary\\Downloads\\enriched.tsv", sep='\t')
cz_authority = pd.read_excel("C:\\Users\\Cezary\\Desktop\\new_cz_authority_df_2021-11-22.xlsx")
cz_authority['nkc_id'] = cz_authority['IDs'].apply(lambda x: re.findall(r'(?<=NKC\|)(.+?)(?=❦|$)', x)[0] if '❦' in x else x)
cz_authority['nkc_id'] = cz_authority['nkc_id'].apply(lambda x: re.sub('^NKC\|', '', x))

nkc_viaf_ids = cz_authority[['nkc_id', 'viaf_id']].to_dict(orient='index')
nkc_viaf_ids = {v['nkc_id']:v['viaf_id'] for k,v in nkc_viaf_ids.items()}

#w tłumaczeniach są ludzie, których nie ma w projekcie

cz_translation_database['viaf_id'] = cz_translation_database['author_id'].apply(lambda x: get_viaf_from_nkc(x))
viaf_groups = {'118529174|256578118':'118529174', '25095273|83955898':'25095273', '11196637|2299152636076120051534':'11196637', '163185334|78000938':'163185334', '88045299|10883385':'88045299'}
cz_translation_database['viaf_id'] = cz_translation_database['viaf_id'].replace(viaf_groups)
cz_translation_database['orig_title_e'] = cz_translation_database[['orig_title_e', 'orig_title']].apply(lambda x: x['orig_title_e'] if pd.notnull(x['orig_title_e']) else x['orig_title'][4:], axis=1)

org_and_trans_cz_database = {}

cz_database_grouped = cz_translation_database.groupby(['viaf_id', 'norm_title'])
for name, group in tqdm(cz_database_grouped, total=len(cz_database_grouped)):
    # name = ('100734216', 'Břeh snů.')
    # group = cz_database_grouped.get_group(name)
    k = name
    temp_dict = {}
    if k not in org_and_trans_cz_database:
        temp_dict.update({k:{}})
    work_title = [e for e in group['norm_title']][0]
    author_viaf = [e for e in group['viaf_id']][0]
    temp_dict[k].update({'work_title': work_title, 'author_viaf': author_viaf})
    translations_list = list(set([simplify_string(e.strip()) for e in group['orig_title_e'].to_list()]))
    translations_list = [e for e in translations_list if e]
    temp_dict[k].update({'translations': translations_list})
    org_and_trans_cz_database.update(temp_dict)

test = ttt.loc()[ttt['work_viaf'].isna()][['001', '245', 'cluster_viaf', 'cluster_titles']].to_dict(orient='records')
for d in tqdm(test):
    # d = test[15]
    simple_245 = marc_parser_dict_for_field(d['245'], '\$')
    try:
        simple_245.pop('$c')
    except KeyError:
        pass
    simple_245 = simplify_string(''.join([e.strip() for e in simple_245.values()]))
    d.update({'simple': simple_245})

cluster_works_df = pd.DataFrame.from_dict(org_and_trans_cz_database, orient='index').reset_index(drop=True)
cluster_works_df = cluster_works_df.explode('translations')
cluster_works_df = cluster_works_df.explode('author_viaf')

test_cluster = cluster_works_df.to_dict(orient='records')
    
for i, el in tqdm(enumerate(test), total=len(test)):
    # i = 0
    # el = test[i]
    for element in test_cluster:
        # element = test_cluster[0]

        # if element['translations'] in el['simple'] and el['cluster_viaf'] == element['author_viaf']:
        #     test[i].update(element)
        if el['cluster_viaf'] == element['author_viaf']:
            if find_similar_target_titles(element['translations'], el['simple'], 0.9):
                test[i].update(element)


# testtest = [e for e in test if e['index'] == 8514 or e['cluster_titles'] == 1460]
test = [e for e in test if len(e) > 5]
original_titles_and_ids = {}
for d in test:
    key = (d['author_viaf'], d['work_title'])
    if key not in original_titles_and_ids:
        original_titles_and_ids.update({key:{'cluster_titles':[d['cluster_titles']], '001':[d['001']]}})
    else:
        original_titles_and_ids[key]['cluster_titles'].append(d['cluster_titles'])
        original_titles_and_ids[key]['001'].append(d['001'])
    
for k,v in original_titles_and_ids.items():
    original_titles_and_ids[k]['cluster_titles'] = min([e for e in original_titles_and_ids[k]['001']])
    # try:
    #     original_titles_and_ids[k]['cluster_titles'] = min([e for e in original_titles_and_ids[k]['cluster_titles'] if e != 0])
    # except ValueError:
    #     original_titles_and_ids[k]['cluster_titles'] = min([e for e in original_titles_and_ids[k]['001']])
    
        
tata = ttt[ttt['001'].isin(original_titles_and_ids[('109312616', 'Largo desolato.')]['001'])]
tata = ttt[ttt['001'].isin(original_titles_and_ids[('116699011', 'Slib.')]['001'])]

ttt_upgrade = deepcopy(ttt).reset_index(drop=True)
for k,v in tqdm(original_titles_and_ids.items(), total=len(original_titles_and_ids.items())):
    ttt_upgrade.loc()[ttt_upgrade['001'].isin(v['001']), ['cluster_titles', 'original title']] = [v['cluster_titles'], k[-1]]

aaaa = ttt_upgrade[ttt_upgrade['cluster_titles'].isin([613, 233])]

ttt_08 = ttt_upgrade.copy()

tata2 = ttt_upgrade[ttt_upgrade['001'].isin(original_titles_and_ids[('109312616', 'Largo desolato.')]['001'])]

#statystyki
print(Counter((ttt_upgrade['cluster_titles'] != 0)))

#uważać na 001:798 i cluster_titles:798

difference = [i for i,(e,f) in enumerate(zip(ttt_09['cluster_titles'], ttt_08['cluster_titles'])) if e!=f]
ttt_08 = ttt_08[ttt_08.index.isin(difference)]
ttt_09 = ttt_09[ttt_09.index.isin(difference)]

aaaa = ttt_upgrade[ttt_upgrade['cluster_titles'].isin([613, 233])]
bbbb = ttt[ttt['001'].isin(aaaa['001'])]
cccc = ttt_upgrade[ttt_upgrade['001'].isin(aaaa['001'])]

#porządkowanie ze względu na bazę czeskich tłumaczeń, dzieli grupy, na dwie, bo wyciąga tylko pary, które są w bazie, a reszty nie rusza, jeśli ma ruszyć resztę, to merguje więcej grup w jedną, bo w danych są będne tytuły oryginałów i rekordy znajdują się w błędnych clustrach
# może zebrać informacje całościowo: czeskie, Brno, VIAF i rozpatrywać to jako jedno? zbudować bazę oryginałów i tłumaczeń?


#dokończyć

df_with_original_titles = pd.merge(ttt_upgrade, df_with_original_titles.drop(columns=['cluster_viaf', '100_unidecode', '245', 'language', 'work_viaf', 'work_title', 'original title', 'cluster_titles', 'quality_index2']), how='left', on='001')

df_with_original_titles['quality_index2'] = df_with_original_titles.apply(lambda x: quality_index(x), axis=1)  

df_with_original_titles.to_excel(f'translation_database_with_viaf_work_{now}.xlsx', index=False)

correct = df_with_original_titles[df_with_original_titles['quality_index2'] > 0.7] #29153
not_correct = df_with_original_titles[df_with_original_titles['quality_index2'] <= 0.7] #19290

writer = pd.ExcelWriter(f'translation_database_clusters_with_quality_index_{now}.xlsx', engine = 'xlsxwriter')
correct.to_excel(writer, index=False, sheet_name='HQ')
not_correct.to_excel(writer, index=False, sheet_name='LQ')
writer.save()
writer.close()

quality_results = Counter(df_with_original_titles['quality_index2'])


aaaa = ttt_upgrade[ttt_upgrade['cluster_titles'] == 0]
bbbb = ttt_upgrade[ttt_upgrade['cluster_viaf'] == '10256796']
from difflib import SequenceMatcher
a = 'carolus quartus romanorum imperator et boemie rex'
words_a = len(a.split(' '))
b = 'carolus quartus romanorum imperator er bohemiae rex pengl dtufranz charles iv roman emperor and king of bohemia'
words_b = len(b.split(' '))

numbers = []
iteration = 0
while iteration + words_a <= words_b:
    b_temp = ' '.join(b.split(' ')[iteration:iteration+words_a])
    iteration +=1
    print(a)
    print(b_temp)
    numbers.append(SequenceMatcher(a=a, b=b_temp).ratio())
    print(SequenceMatcher(a=a, b=b_temp).ratio())
max(numbers) > 0.9

testa = [e for e in test if e['cluster_viaf'] == '10256796']
test_clustera = [e for e in test_cluster if e['author_viaf'] == '10256796']
#!!!!!!!!!usunąć autorkę!!!!!!! 	cluster_viaf:101843365
# śledzić cluster autora: 	cluster_viaf:10256796 dla utworu Carolus Quartus, Romanorum Imperator et Boemie Rex
# Ondrej da właściwy o


# zastosować rozwiązanie z wcześniejszego podejścia, to, co zrobiłem jest zbyt pomieszane

# pogadać z Ondrejem!!!

#słownik z danych projektu
ttt = df_with_original_titles.copy()[['001', 'cluster_viaf', '100_unidecode', '245', 'language', 'work_viaf', 'work_title', 'original title', 'cluster_titles', 'quality_index2']]
ttt2 = ttt[(ttt['cluster_titles'].apply(lambda x: isinstance(x, int))) &
           (ttt['cluster_titles'] != 0)]

org_and_trans_biblio_by_clusters = {}

ttt_grouped = ttt2.groupby('cluster_titles')

for name, group in tqdm(ttt_grouped, total=len(ttt_grouped)):
    # name = 1
    # group = ttt_grouped.get_group(name)
    k = name
    temp_dict = {}
    if k not in org_and_trans_biblio_by_clusters:
        temp_dict.update({k:{}})
    work_title = Counter(group['original title']).most_common(1)[0][0]
    author_viaf = list(set(group['cluster_viaf'].to_list()))
    temp_dict[k].update({'work_title': work_title, 'author_viaf': author_viaf})
    translations_list = list(set([simplify_string(marc_parser_dict_for_field(e, '\$')['$a']).strip() for e in group['245'].to_list()]))
    translations_list = [e for e in translations_list if e]
    temp_dict[k].update({'translations': translations_list})
    org_and_trans_biblio_by_clusters.update(temp_dict)

test = ttt.loc()[ttt['work_viaf'].isna()][['001', '245', 'cluster_viaf', 'cluster_titles']].to_dict(orient='records')
for d in tqdm(test):
    # d = test[15]
    simple_245 = marc_parser_dict_for_field(d['245'], '\$')
    try:
        simple_245.pop('$c')
    except KeyError:
        pass
    simple_245 = simplify_string(''.join([e.strip() for e in simple_245.values()]))
    d.update({'simple': simple_245})

cluster_works_df = pd.DataFrame.from_dict(org_and_trans_biblio_by_clusters, orient='index').reset_index()
cluster_works_df = cluster_works_df.explode('translations')
cluster_works_df = cluster_works_df.explode('author_viaf')

test_cluster = cluster_works_df.to_dict(orient='records')

for i, el in tqdm(enumerate(test), total=len(test)):
    # i = 0
    # el = test[i]
    for element in test_cluster:
        # element = test_cluster[0]
        try:
            if element['translations'] in el['simple'] and el['cluster_viaf'] == element['author_viaf'] and el['cluster_titles'] != element['index']:
                test[i].update(element)
        except TypeError:
            pass

test = [e for e in test if len(e) > 5]

lista = []
for element in test:
    lista.append([element['cluster_titles'], element['index']])
   
lista = [[d for d in e if d != 0] for e in lista]
lista = [x for x in set(tuple(x) for x in lista)]

iset = set([frozenset(s) for s in lista])  # Convert to a set of sets
result = []
while(iset):                  # While there are sets left to process:
    nset = set(iset.pop())      # Pop a new set
    check = len(iset)           # Does iset contain more sets
    while check:                # Until no more sets to check:
        check = False
        for s in iset.copy():       # For each other set:
            if nset.intersection(s):  # if they intersect:
                check = True            # Must recheck previous sets
                iset.remove(s)          # Remove it from remaining sets
                nset.update(s)          # Add it to the current set
    result.append(tuple(nset))
result = [e for e in result if len(e) > 1]

dobre = []
for element in result:
    # element = result[206]
    temp_list = [e['001'] for e in test if any(d in [e['cluster_titles'], e['index']] for d in element)]
    cluster = min(element)
#!!!!!!PROBLEMY!!!!!!!
#tutaj na podstawie par uzyskać listę rekordów i wpisać najniższą wartość klastra, a następnie ogarnąć tytuły oryginalne tak, jak poniżej
element = result[1]
element = result[45]
element = result[0]
tata = ttt[ttt['cluster_titles'].isin(element)]
tata_total = ttt[ttt['cluster_titles'].isin([e for sub in result for e in sub])]
#2227 or 3857 records for manual editing?
















#uporządkować relacje cluster_titles i index

cluster_relations_dict = {}
for e in test:
    if e['cluster_titles'] not in cluster_relations_dict and e['cluster_titles'] != 0:
        cluster_relations_dict.update({e['cluster_titles']:{'clusters': [e['index']]}})
    elif e['cluster_titles'] == 0:
        if e['index'] not in cluster_relations_dict:
            cluster_relations_dict.update({e['index']:{'records': [e['001']]}})
        else:
            try:            
                cluster_relations_dict[e['index']]['records'].append(e['001'])
            except KeyError:
                cluster_relations_dict[e['index']].update({'records': [e['001']]})    
    else:
        if e['index'] not in cluster_relations_dict[e['cluster_titles']]['clusters']:
            cluster_relations_dict[e['cluster_titles']]['clusters'].append(e['index'])
    
# dobry przykład!!!
# abba = ttt[(ttt['cluster_titles'].isin([415, 6978, 3264, 414, 6629])) | (ttt['001'].isin([320144152, 440129371, 1143807, 800586261, 851191656, 320143565, 320144149, 749677000, 320144143, 320144146, 21184792, 622494525, 876951210, 433995702, 1101120145, 872165447, 320144151, 42155155, 42178318, 1171762, 875672563, 85631704, 1183440519, 256985845, 73291869, 760272881, 771365271, 614575078, 179252284, 858202590, 678861789]))]

# Counter(abba['original title'].dropna()).most_common(1)[0][0]

ttt_upgrade = deepcopy(ttt).reset_index(drop=True)
for key, value in tqdm(cluster_relations_dict.items(), total=len(cluster_relations_dict)):
    if all(e in value for e in ['clusters', 'records']):
        ttt_upgrade.loc()[(ttt_upgrade['cluster_titles'].isin(value['clusters'])) | 
                          (ttt_upgrade['001'].isin(value['records'])), 'cluster_titles'] = key
    elif 'clusters' in value:
        ttt_upgrade.loc()[ttt_upgrade['cluster_titles'].isin(value['clusters']), 'cluster_titles'] = key
    elif 'records' in value:
        ttt_upgrade.loc()[ttt_upgrade['001'].isin(value['records']), 'cluster_titles'] = key

ttt_upgrade_grouped = ttt_upgrade.groupby('cluster_titles')

df = pd.DataFrame()
for name, group in tqdm(ttt_upgrade_grouped, total=len(ttt_upgrade_grouped)):
    try:
        proper_title = Counter(group['original title'].dropna()).most_common(1)[0][0]
        group['original title'] = proper_title
    except IndexError:
        pass
    df = df.append(group)
    
aaaa = ttt[ttt['001'].isin([887433338, 1162849329, 880263364, 918402727, 1161796927])]
aaaa = [t for t in test if t['001'] in [887433338, 1162849329, 880263364, 918402727, 1161796927]]
aaaa = [t for t in test if any(z in [t['cluster_titles'], t['index']] for z in [514])]


tttt1 = ttt_upgrade[ttt_upgrade['cluster_titles'].isin([415])]   
tttt1 = ttt_upgrade[ttt_upgrade['cluster_titles'].isin([3264])]    
aaaa = tttt1[~tttt1['001'].isin(abba['001'])]['001']
aaaa = ttt[ttt['001'] == 742847162]

for e in tqdm(test):
    if e['cluster_titles'] != 0:
        ttt_upgrade.loc()[ttt_upgrade['cluster_titles'] == e['index'], 'cluster_titles'] = e['cluster_titles']
    else:
        ttt_upgrade.loc()[ttt_upgrade['001'] == e['001'], 'cluster_titles'] = e['index']
#statystyki
Counter((ttt_upgrade['cluster_titles'] != 0))


tttt1 = ttt[ttt['cluster_titles'].isin([1460])]
tttt2 = ttt_upgrade[ttt_upgrade['cluster_titles'].isin([1460])]
tttt2b = ttt_upgrade[ttt_upgrade['cluster_titles'].isin([8514])]


tttt3 = ttt_upgrade[ttt_upgrade['001'].isin(tttt1['001'])]
tttt4 = ttt_upgrade[ttt_upgrade['001'].isin([21108340])]

df_with_original_titles = pd.merge(ttt_upgrade, df_with_original_titles.drop(columns=['cluster_viaf', '100_unidecode', '245', 'language', 'work_viaf', 'work_title', 'original title', 'cluster_titles', 'quality_index2']), how='left', on='001')



work_title = Counter(group['original title']).most_common(1)[0][0]



tttt = ttt_upgrade[ttt_upgrade['cluster_titles'].isin([1776])]
tttt = ttt[ttt['cluster_titles'].isin([27, 14009])]

ttt_upgrade.loc()[(ttt_upgrade['work_viaf'].isin(dictionary['viafs'])) | 
                      (ttt_upgrade['cluster_titles'].isin(dictionary['clusters'])), 'work_viaf'] = proper_work_viaf




for i, row in tqdm(ttt_upgrade.iterrows(), total=ttt_upgrade.shape[0]):
    # i = 0
    # row = ttt_upgrade.iloc[i,:]
    if row['001']
    
    
    if not work_with_viaf_id_simple_2[work_with_viaf_id_simple_2['001'] == row['001']]['work_viaf'].empty:
        ttt_upgrade.at[i, 'work_viaf'] = work_with_viaf_id_simple_2[work_with_viaf_id_simple_2['001'] == row['001']]['work_viaf'].to_string(index=False)
        ttt_upgrade.at[i, 'work_title'] = work_with_viaf_id_simple_2[work_with_viaf_id_simple_2['001'] == row['001']]['work_title'].to_string(index=False)




work_with_viaf_id_simple_3 = pd.DataFrame(test)[['001', 'cluster_titles', 'work_title']]

#jeśli cluster_titles != 0 to 001 ma mieć cluster_titles, jeśli cluster_titles == 0, to 001 ma mieć index
#!!!ZASTOSOWAĆ!!!!!!!

tttt = ttt[ttt['cluster_titles'].isin([27, 14009])]
tttt = ttt[ttt['cluster_titles'].isin([30, 689])]
tttt = ttt[ttt['cluster_titles'].isin([38, 11857])]
tttt = ttt[ttt['cluster_titles'].isin([65, 17255])]
tttt = ttt[ttt['cluster_titles'].isin([18567, 18568])]

tttt = ttt[(ttt['001'] == 10000000847) | ttt['cluster_titles'].isin([7526])]
tttt = ttt[(ttt['001'] == 47522565) | ttt['cluster_titles'].isin([1776])]
tttt = ttt[(ttt['001'] == 866081865) | ttt['cluster_titles'].isin([16023])]
tttt = ttt[(ttt['001'] == 813591120) | ttt['cluster_titles'].isin([18482])]
tttt = ttt[(ttt['001'] == 715907416) | ttt['cluster_titles'].isin([1460])]

ttttt = ttt[ttt['245'].str.contains('Little Mole and the Eagle')]


tttt = [e for e in test if e['cluster_titles'] == 0]


ttt_upgrade = deepcopy(ttt).reset_index(drop=True)
for i, row in tqdm(ttt_upgrade.iterrows(), total=ttt_upgrade.shape[0]):
    # i = 112
    # row = ttt_upgrade.iloc[i,:]
    if not work_with_viaf_id_simple_2[work_with_viaf_id_simple_2['001'] == row['001']]['work_viaf'].empty:
        ttt_upgrade.at[i, 'work_viaf'] = work_with_viaf_id_simple_2[work_with_viaf_id_simple_2['001'] == row['001']]['work_viaf'].to_string(index=False)
        ttt_upgrade.at[i, 'work_title'] = work_with_viaf_id_simple_2[work_with_viaf_id_simple_2['001'] == row['001']]['work_title'].to_string(index=False)



#%%

ttt_upgrade.at[48423,'cluster_titles'] != 0


aaa = cluster_author_work[cluster_author_work['work_viaf'] == '1598147270543735700005']
aaa = ttt_upgrade[ttt_upgrade['work_viaf'] == '312401949']

#co jeszcze?
#zbudować dict dla original_title z cluster_titles i do tego target titles i przeszukać

ttt_upgrade[ttt_upgrade['001'] == 1014060193].index
ttt_upgrade[ttt_upgrade['001'] == 884653991].index


#ręcznie trzeba usunąć largo desolato ze słownika dla tłumaczeń zahradni slavnost!!!!
# może edytować problematyczne rekordy:
#78118212 – bo ma zły viaf
#908861268 – bo ma zły tytuł oryginału

















    
differences = pd.DataFrame()
for (i1, row1), (i2, row2) in tqdm(zip(test_df.reset_index(drop=True).iterrows(), ttt.reset_index(drop=True).iterrows()), total=ttt.shape[0]):
    # row1 = test_df.iloc[59,:]
    # row2 = ttt.iloc[59,:]
    if pd.notnull(row1['work_viaf']) and row1['work_viaf'] != row2['work_viaf']:
        row1 = pd.DataFrame(row1).transpose()
        row2 = pd.DataFrame(row2).transpose()
        testowy_df = pd.concat([row1, row2], axis=1)
        differences = differences.append(testowy_df)
    
    
ttt5 = test_df[(test_df['work_viaf'] == '808147727632864710008') |
               (test_df['cluster_titles'] == 9) |
               (test_df['cluster_titles'] == 410)]  
ttt6 = test_df[test_df['work_viaf'] == '1324145424596886831014']

ttt7 = ttt[ttt['work_viaf'] == '176421014']
                                                                                                                                                                         
ttt3 = ttt[ttt['work_viaf'] == '184716869']

ttt4 = [e for e in new_list if e['viafs'] == '746145424543786830041']

# tu jest problem, czemu są dwa? zrobić jedno, żeby drugie się nie dopisywało!
res = list(map(dict, set(tuple(sorted(sub.items())) for sub in total_dicts))) 
testo = [e for e in res if 134 in e['clusters']] 


aaa = Counter(test_df['cluster_titles'])
aaa1 = dict(aaa)
aaa2 = {k:v for k,v in aaa1.items() if v == 1 and pd.notnull(k)}

bbb = [e.split('$c')[0] for e in test_df['245']]
bbb = [e for e in bbb if ' ; ' in e or ' ;$b' in e]


total = pd.merge(df_with_original_titles, ttt, on='001', how='left')
total.to_excel('after_grouping_algorithm_vol_1.xlsx', index=False)
           
final_df = pd.DataFrame(total_dicts)
final_df = final_df.explode('viafs').reset_index(drop=True)
final_df = final_df.explode('clusters').reset_index(drop=True)
final_df = final_df.drop_duplicates().reset_index(drop=True)


           
            







    
    
test = [e for e in work_viaf_and_cluster_titles if len(e[-1]) > 1]

# dla każdego elementu w test mam grupy – teraz trzeba je ujednolicić



test[3][-1].most_common()[-1]


cluster_author_work_grouped = cluster_author_work.groupby('work_viaf')





test = []
for name, group in tqdm(cluster_author_work_grouped, total=len(cluster_author_work_grouped)):
    test.append(dict(Counter(group['work_viaf'])))
'Osudy dobrého vojáka Švejka za světové války' == 'Osudy dobrého vojáka Švejka za světové války'
test = [e for e in test if len(e) > 1]


ttt2 = ttt.loc()[ttt['work_title'] == 'Bambini di Praga']
ttt2 = ttt.loc()[ttt['work_title'] == 'Báječná léta pod psa']
ttt2 = ttt.loc()[ttt['work_title'] == 'České pohádky']

ttt2 = ttt.loc()[(ttt['work_viaf'] == '176421014') |
                 (ttt['work_viaf'] == '179146462549027771326') |
                 (ttt['cluster_titles'] == 134) |
                 (ttt['cluster_titles'] == 2725) |
                 (ttt['cluster_titles'] == 2732) |
                 (ttt['cluster_titles'] == 3230) |
                 (ttt['cluster_titles'] == 5669) |
                 (ttt['cluster_titles'] == 8736) |
                 (ttt['cluster_titles'] == 11007) |
                 (ttt['cluster_titles'] == 16796)]
ttt.to_excel('NW-cale_dane.xlsx', index=False)

#cluster Szwejka z 568 do 1016
# czy wszystkie cluster_titles ids? jak to ogarnąć?
ttt2 = ttt.loc()[ttt['work_title'] == 'Porte des langues ouverte']

ttt3 = ttt.loc()[(ttt['cluster_titles'] == 57) |
                 (ttt['work_title'] == 'Havířská balada')]

ttt4 = ttt.loc()[ttt['001'].isin([1082033388, 1014558108])]

# dla każdego work_viaf poszukać wszystkie cluster_titles, a następnie dla tych cluster_titles wpisać wszędzie work_viaf i work_title, co będzie informacją dla oryginalnego tytułu!!!!

#%% de-duplication HQ

# choose one
# records_df = pd.read_excel('translation_database_clusters_with_quality_index_2021-10-25.xlsx', sheet_name='HQ')
records_df = pd.read_excel('translation_database_with_viaf_work.xlsx')
records_df = records_df[records_df['quality_index2'] <= 0.7]
# records_df = pd.read_excel('translation_database_clusters_with_quality_index_2021-10-27.xlsx', sheet_name='LQ')
# records_df = correct.copy()
#dla HQ
# records_grouped = records_df.groupby(['cluster_viaf', 'language', 'cluster_titles'])
#dla LQ
records_grouped = records_df.groupby(['cluster_viaf', 'language'])
# records_grouped = records_df.groupby(['cluster_viaf', 'language', 'cluster_titles'], dropna=False)

# writer = pd.ExcelWriter(f'HQ_data_deduplicated_{now}.xlsx', engine = 'xlsxwriter')
writer = pd.ExcelWriter(f'LQ_data_deduplicated_{now}.xlsx', engine = 'xlsxwriter')
records_df.to_excel(writer, index=False, sheet_name='phase_0') # original data

phase_1 = pd.DataFrame() # duplicates
phase_2 = pd.DataFrame() # multiple volumes
phase_3 = pd.DataFrame() # fuzzyness
phase_4 = pd.DataFrame() # ISBN

# grupy = []
for name, group in tqdm(records_grouped, total=len(records_grouped)):
    # grupy.append((name, group.shape[0]))
    # grupy = sorted(grupy, key=lambda x: x[-1], reverse=True)
    # name = ('4931097', 'pol', 134)
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
        place = place.loc[place['place'] != '']
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
                if column in ['fiction_type', 'audience', '490', '500', '650', '655']:
                    sub_group[column] = '❦'.join(sub_group[column].dropna().drop_duplicates().astype(str))
                else:
                    try:
                        sub_group[column] = max(sub_group[column].dropna().astype(str), key=len)
                    except ValueError:
                        sub_group[column] = np.nan
            df_oclc_deduplicated = df_oclc_deduplicated.append(sub_group)
        
        df_oclc_deduplicated = df_oclc_deduplicated.drop_duplicates().replace(r'^\s*$', np.nan, regex=True)
        df_oclc_deduplicated['001'] = df_oclc_deduplicated['001'].astype(np.int64)
        group = group.loc[~group['001'].isin(oclc_duplicates_list)]
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
                    if column in ['fiction_type', 'audience', '490', '500', '650', '655']:
                        sub_group[column] = '❦'.join(sub_group[column].dropna().drop_duplicates().astype(str))  
                    elif column in ['001', '245']:
                        pass
                    else:
                        try:
                            sub_group[column] = max(sub_group[column].dropna().astype(str), key=len)
                        except ValueError:
                            sub_group[column] = np.nan
                df = sub_group.loc[~sub_group['245'].str.contains('\$n', regex=True)]
                df_oclc_multiple_volumes_deduplicated = df_oclc_multiple_volumes_deduplicated.append(df)
            else:
                for column in sub_group:
                    if column in ['fiction_type', 'audience', '490', '500', '650', '655']:
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
        df_oclc_multiple_volumes_deduplicated['001'] = df_oclc_multiple_volumes_deduplicated['001'].astype(np.int64)
        group = group.loc[~group['001'].isin(oclc_multiple_volumes_list)]
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

    df_oclc_clusters = cluster_records(group, '001', ['title', 'publisher', 'year'], 0.85)    
    df_oclc_clusters = df_oclc_clusters.loc[df_oclc_clusters['publisher'] != '']
    df_oclc_duplicates = df_oclc_clusters.groupby(['cluster', 'year']).filter(lambda x: len(x) > 1)
    
    if df_oclc_duplicates.shape[0] > 0:
    
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
                if column in ['fiction_type', 'audience', '490', '500', '650', '655']:
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
        df_oclc_deduplicated['001'] = df_oclc_deduplicated['001'].astype(np.int64)
        
        group = group.loc[~group['001'].isin(oclc_duplicates_list)]
        group = pd.concat([group, df_oclc_deduplicated])
        group['group_ids'] = group['group_ids'].apply(lambda x: '❦'.join(set(x.split('❦'))) if pd.notnull(x) else x)
        group = group.drop(columns=['cluster', 'title']).drop_duplicates().reset_index(drop=True)
        phase_3 = phase_3.append(group)
    else:
        group = group.drop(columns='title').drop_duplicates().reset_index(drop=True)
        phase_3 = phase_3.append(group)
    
#phase_4: ISBN  
           
    group['ISBN'] = group['020'].apply(lambda x: get_ISBNs(x))

    ISBN_dict = {}
    for i, row in group.iterrows():
        if row['ISBN'] != 'no ISBN':
            x = row['ISBN'].split('❦')
            for el in x:
                if (el, row['year']) in ISBN_dict:
                    ISBN_dict[(el, row['year'])].append(row['001'])
                else:
                    ISBN_dict[(el, row['year'])] = [row['001']]
    ISBN_dict = {k:tuple(v) for k,v in ISBN_dict.items() if len(v) > 1}
    ISBN_dict = list(set([v for k,v in ISBN_dict.items()]))
    ISBN_dict = {e[0]:e[1] for e in ISBN_dict}
    
    group['001'] = group['001'].replace(ISBN_dict)
    
    ISBN_grouped = group.groupby('001')
   
    group = pd.DataFrame()
    for sub_name, sub_group in ISBN_grouped:
        try:
            group_ids = '❦'.join(set([str(e) for e in sub_group['001'].to_list() + sub_group['group_ids'].to_list() if pd.notnull(e)]))
        except KeyError:
            group_ids = '❦'.join(set([str(e) for e in sub_group['001'].to_list() if pd.notnull(e)]))
        sub_group['group_ids'] = group_ids
        for column in sub_group:
            if column in ['fiction_type', 'audience', '240', '245', '260', '650', '655', '700']:
                sub_group[column] = '❦'.join(sub_group[column].dropna().drop_duplicates().astype(str))
            else:
                try:
                    sub_group[column] = max(sub_group[column].dropna().astype(str), key=len)
                except ValueError:
                    sub_group[column] = np.nan
        group = group.append(sub_group)
    
    group = group.drop_duplicates().replace(r'^\s*$', np.nan, regex=True)
    group['001'] = group['001'].astype(np.int64)
    
    group['group_ids'] = group['group_ids'].apply(lambda x: '❦'.join(set(x.split('❦'))) if pd.notnull(x) else x)
    group = group.drop(columns='ISBN').drop_duplicates().reset_index(drop=True)
    phase_4 = phase_4.append(group)
         
phase_1.to_excel(writer, index=False, sheet_name='phase_1')    
phase_2.to_excel(writer, index=False, sheet_name='phase_2')  
phase_3.to_excel(writer, index=False, sheet_name='phase_3') 
phase_4.to_excel(writer, index=False, sheet_name='phase_4') 
writer.save()
writer.close()    

#%% LQ

try:
    hq_df = pd.read_excel("C:\\Users\\Rosinski\\Documents\\IBL-PAN-Python\\HQ_data_deduplicated_2021-10-27.xlsx", sheet_name='phase_4')
except FileNotFoundError:
    hq_df = pd.read_excel("C:\\Users\\Cezary\\Documents\\IBL-PAN-Python\\HQ_data_deduplicated_2021-10-27.xlsx", sheet_name='phase_4')
    
try:
    lq_df = pd.read_excel("C:\\Users\\Rosinski\\Documents\\IBL-PAN-Python\\translation_database_clusters_with_quality_index_2021-10-27.xlsx", sheet_name='LQ')
except FileNotFoundError:
    lq_df = pd.read_excel("C:\\Users\\Cezary\\Documents\\IBL-PAN-Python\\translation_database_clusters_with_quality_index_2021-10-27.xlsx", sheet_name='LQ')
    
# LQ deduplication
    

hq_df['ISBN'] = hq_df['020'].apply(lambda x: get_ISBNs(x))
hq_dict = hq_df[['001', '020', 'year', 'language', 'original title', '245', '100_unidecode', 'cluster_viaf', 'cluster_titles', 'ISBN']].to_dict(orient='records')

lq_df['ISBN'] = lq_df['020'].apply(lambda x: get_ISBNs(x))
lq_df['year'] = lq_df['008'].apply(lambda x: x[7:11])
lq_dict = lq_df[['001', '245', 'year', 'language', 'cluster_viaf', 'ISBN']].to_dict(orient='records')

# ISBN + year
lista = []
for dic in tqdm(hq_dict):
    if dic['ISBN'] not in ['', 'no ISBN']:
        x = dic['ISBN'].split('❦')
        for isbn in x:
            y = [e['001'] for e in lq_dict if isbn in e['ISBN'].split('❦') and e['year'] == dic['year']]     
            if y:
                y.insert(0, dic['001'])
                lista.append(tuple(y))
                    
lista = list(set(lista))
duplicates1 = [e[1:] for e in lista]
duplicates1 = [e for sub in duplicates1 for e in sub]

lq_duplicates1 = lq_df.loc()[lq_df['001'].isin(duplicates1)]

lq_df = lq_df.loc()[~lq_df['001'].isin(duplicates1)]

#2333 references to HQ
#3460 LQ duplicates

test = hq_df.loc()[hq_df['cluster_viaf'] == '34458072']
test = test[['001', '020', 'year', 'language', 'original title', '245', '100_unidecode', 'cluster_viaf', 'cluster_titles', 'ISBN']]
test = test[test['cluster_titles'] == 391]
test_dict = test.to_dict(orient='records')

# viaf + target language + target title

def simplify_245(x):
    try:
        y = re.split('\/|\:|\;|\=', marc_parser_dict_for_field(x, '\$')['$a'])[0].strip()
    except KeyError:
        y = re.split('\:|\.', x)[0][2:].strip()
    except TypeError:
        y = np.nan
    return y
    
lq_df['simplified245'] = lq_df['245'].apply(lambda x: simplify_245(x))
lq_dict = lq_df[['001', '245', 'simplified245', 'year', 'language', 'cluster_viaf', 'ISBN']].to_dict(orient='records')

#test

ttt = []
for dic in tqdm(test_dict):
    y = simplify_245(dic['245'])    
    z = [e['001'] for e in lq_dict if dic['cluster_viaf'] == e['cluster_viaf'] and dic['language'] == e['language'] and y == e['simplified245']]
    if z:
        ttt.append((dic['001'], z))
    
# # problem - multiple target titles    
a = hq_df.loc()[hq_df['001'] == 51788260]
b = lq_df.loc()[lq_df['001'].isin([73225835])]

# ok but different 260    
a = hq_df.loc()[hq_df['001'] == 10000002623]
b = lq_df.loc()[lq_df['001'].isin([587906250])]
        
# 2 out of 3 are okay        
a = hq_df.loc()[hq_df['001'] == 750568054]
b = lq_df.loc()[lq_df['001'].isin([81219067, 718498465, 81807310])]

# same group as above
a = hq_df.loc()[hq_df['001'] == 834091146]
b = lq_df.loc()[lq_df['001'].isin([81219067, 718498465, 81807310])]

# all data
ttt = []
for dic in tqdm(hq_dict):
    y = simplify_245(dic['245'])    
    z = [e['001'] for e in lq_dict if dic['cluster_viaf'] == e['cluster_viaf'] and dic['language'] == e['language'] and y == e['simplified245']]
    if z:
        z.insert(0, dic['001'])
        ttt.append(tuple(z))

ttt = list(set(ttt))
duplicates2 = [e[1:] for e in ttt]
duplicates2 = [e for sub in duplicates2 for e in sub]

lq_duplicates2 = lq_df.loc()[lq_df['001'].isin(duplicates2)]

lq_df = lq_df.loc()[~lq_df['001'].isin(duplicates2)]

#1585 references to HQ
#3114 LQ duplicates
# LQ reduced to 35644

# 3 out of 5 correct
a = hq_df.loc()[hq_df['001'] == 254393601]
b = lq_df.loc()[lq_df['001'].isin([925253766, 123689349, 720869925, 492082896, 632597099])]

a = hq_df.loc()[hq_df['001'] == 246420506]
b = lq_df.loc()[lq_df['001'].isin([174615060, 781010081, 72436021, 174614599, 312205678, 630855539, 258676117, 248855710, 174232966, 174977791, 256436285, 631168511, 632306897, 162906122, 631234370, 174614589, 174614646, 174615031, 312204523, 64788913, 174938402, 631409595, 174614610, 64788919])]



#deduplicate LQ?
# miejsce wydania, wydawca, liczba stron?

test2 = lq_df.loc()[(lq_df['cluster_viaf'] == '34458072') &
                    (lq_df['language'] == 'pol')]

test2.to_excel('Hrabal_pl_LQ.xlsx', index=False)

test_jpn = lq_df.loc()[lq_df['language'] == 'jpn']
test_jpn.to_excel('Japanese_LQ.xlsx', index=False)

test_und = lq_df.loc()[lq_df['language'] == 'und']
test_und.to_excel('und_LQ.xlsx', index=False)


def place_of_pbl(x):
    try:
        return marc_parser_dict_for_field(x, '\$')['$a']
    except (KeyError, TypeError):
        np.nan

aaa = Counter(test_und['260'].apply(lambda x: place_of_pbl(x)))



#%% tabela translacji do mrc
from my_functions import gsheet_to_df, df_to_mrc, mrc_to_mrk
import pymarc
df = gsheet_to_df('1OI0DlbPhWTpC2J1A7UMl_UMom1ywX61g08tqk3B-rNI', 'Sheet1')
# df = gsheet_to_df('1DDQwIUZeDAhK62cnGCicq1Z9a1JeE-AhPyW6R-vC9mE', 'Sheet1')
# df = gsheet_to_df('1sr5ChV8JhcZRsPDKWjSNJXpCLo0gu-B6XQMC0ICcQ3I', 'Sheet1')

df_columns = [str(e) for e in df.columns.values]

df_columns = ['{:03d}'.format(int(e)) if e.isnumeric() else e for e in df_columns]

df.columns = df_columns

df_to_mrc(df, '❦', 'czech_translations_NEW.mrc', 'czech_translations_NEW_errors.txt')
mrc_to_mrk('czech_translations_NEW.mrc', 'czech_translations_NEW.mrk')

# test = df[df['001'] == 'trl0000003']
# df, field_delimiter, path_out, txt_error_file = test, '❦', 'czech_translations_NEW.mrc', 'czech_translations_NEW_errors.txt'

# def df_to_mrc(df, field_delimiter, path_out, txt_error_file):  
#     mrc_errors = []
#     df = df.replace(r'^\s*$', np.nan, regex=True)
#     outputfile = open(path_out, 'wb')
#     errorfile = io.open(txt_error_file, 'wt', encoding='UTF-8')
#     list_of_dicts = df.to_dict('records')
#     for record in tqdm(list_of_dicts, total=len(list_of_dicts)):
#         record = list_of_dicts[0]
#         record = {k: v for k, v in record.items() if pd.notnull(v)}
#         try:
#             pymarc_record = pymarc.Record(to_unicode=True, force_utf8=True, leader=record['LDR'])
#             # record = {k:v for k,v in record.items() if any(a == k for a in ['LDR', 'AVA']) or re.findall('\d{3}', str(k))}
#             for k, v in record.items():
#                 v = str(v).split(field_delimiter)
#                 if k == 'LDR':
#                     pass
#                 elif k.isnumeric() and int(k) < 10:
#                     tag = k
#                     data = ''.join(v)
#                     marc_field = pymarc.Field(tag=tag, data=data)
#                     pymarc_record.add_ordered_field(marc_field)
#                 else:
#                     if len(v) == 1:
#                         tag = k
#                         record_in_list = re.split('\$(.)', ''.join(v))
#                         indicators = list(record_in_list[0])
#                         subfields = record_in_list[1:]
#                         marc_field = pymarc.Field(tag=tag, indicators=indicators, subfields=subfields)
#                         pymarc_record.add_ordered_field(marc_field)
#                     else:
#                         for element in v:
#                             tag = k
#                             record_in_list = re.split('\$(.)', ''.join(element))
#                             indicators = list(record_in_list[0])
#                             subfields = record_in_list[1:]
#                             marc_field = pymarc.Field(tag=tag, indicators=indicators, subfields=subfields)
#                             pymarc_record.add_ordered_field(marc_field)
#             outputfile.write(pymarc_record.as_marc())
#         except ValueError as err:
#             mrc_errors.append((err, record))
#     if len(mrc_errors) > 0:
#         for element in mrc_errors:
#             errorfile.write(str(element) + '\n\n')
#     errorfile.close()
#     outputfile.close()


#%% wcześniejsze podejście Brno i NKC
#%% Brno database
encoding = 'utf-8'

try:
    file_path = "C:/Users/Cezary/Downloads/scrapeBrno.txt"
    marc_list = io.open(file_path, 'rt', encoding = encoding).read().replace('|','$').splitlines()
except FileNotFoundError:
    file_path = "C:/Users/Rosinski/Downloads/scrapeBrno.txt"
    marc_list = io.open(file_path, 'rt', encoding = encoding).read().replace('|','$').splitlines()

records = []
for row in tqdm(marc_list):
    if row.startswith('LDR'):
        records.append([row])
    else:
        if not row.startswith('FMT') and len(row) > 0:
            records[-1].append(row)
            
records = [[e.split('\t') for e in record] for record in records]
records = [[[field[0] if not field[0][:3].isnumeric() else field[0][:3], field[1]] for field in record] for record in records]

final_list = []
for record in tqdm(records):
    slownik = {}
    for [field, content] in record:
        if field in slownik:
            slownik[field] += f"❦{content}"
        else:
            slownik[field] = content
    final_list.append(slownik)

brno_df = pd.DataFrame(final_list)
fields = brno_df.columns.tolist()
fields.sort(key = lambda x: ([str,int].index(type("a" if re.findall(r'\w+', x)[0].isalpha() else 1)), x))
brno_df = brno_df.reindex(columns=fields) 
brno_df = brno_df[(brno_df['DEL'].isnull()) &
                  (brno_df['BAS'].str.contains('CZT')) &
                  (~brno_df['041'].isna()) &
                  (brno_df['041'].str.contains('$h cze', regex=False))].drop(columns=['BAS', 'DEL', 'GBS', 'LID', 'MKL', 'Obalkyknih.cz:', 'PAK', 'PRU', 'RDALD', 'SHOD', 'STA', 'SYS', 'A653', 'B260', 'GBS1', 'GBS2', 'M0ZK', 'T787', 'V015', 'V020', 'V043', 'V490', 'V505', 'V787', 'V902']).reset_index(drop=True)
brno_df['SRC'] = 'Brno'

start_value = 10000000000
brno_df['fakeid'] = pd.Series(range(start_value,start_value+brno_df.shape[0]+1,1))
brno_df['fakeid'] = brno_df['fakeid'].astype('Int64').astype('str')
        
brno_df['001'] = brno_df.apply(lambda x: get_oclc_id(x), axis=1)
brno_df.drop(columns='fakeid', inplace=True)
brno_df['008'] = brno_df['008'].str.replace('$', '|')

brno_df['260'] = brno_df[['260', '264']].apply(lambda x: x['260'] if pd.notnull(x['260']) else x['264'], axis=1)
brno_df['240'] = brno_df[['240', '246']].apply(lambda x: x['240'] if pd.notnull(x['240']) else x['246'], axis=1)

brno_grouped = brno_df.groupby('001')
brno_duplicates = brno_grouped.filter(lambda x: len(x) > 1).sort_values('001')

field_100['$a'] = field_100['$a'].apply(lambda x: x[:-1] if x[-1] == ',' else x)
field_100['$1'] = ''
new_people = []
for i, row in tqdm(field_100.iterrows(), total=field_100.shape[0]):
    try:
        field_100.at[i,'$1'] = f"http://viaf.org/viaf/{cz_id_viaf_id_dict[row['$7']]}"
    except KeyError:
        url = f"http://viaf.org/viaf/sourceID/NKC%7C{row['$7']}/json"
        response = requests.get(url).url
        viaf_id = re.findall('\d+', response)[0]
        field_100.at[i,'$1'] = f"http://viaf.org/viaf/{viaf_id}"
        new_people.append({'index':viaf_id, 'cz_name':row['$a'], 'cz_dates':row['$d'], 'viaf_id':viaf_id, 'IDs':row['$7']})
brno_df['viaf_id'] = ''
for i, row in tqdm(brno_df.iterrows(), total=brno_df.shape[0]):   
    brno_df.at[i, '100'] = f"{row['100']}$1{field_100[field_100['001'] == row['001']]['$1'].to_list()[0]}"
    viaf_id = field_100[field_100['001'] == row['001']]['$1'].to_list()[0]
    viaf_id = re.findall('\d+', viaf_id)[0]
    brno_df.at[i, 'viaf_id'] = viaf_id
   
brno_df['fiction_type'] = brno_df['008'].apply(lambda x: x[33])
brno_df['audience'] = brno_df['008'].apply(lambda x: x[22])
brno_df['language'] = brno_df['008'].apply(lambda x: x[35:38])
brno_df['100_unidecode'] = brno_df['100'].apply(lambda x: unidecode.unidecode(x).lower() if pd.notnull(x) else x)
    
brno_multiple_original_titles = brno_df[(brno_df['765'].str.count('\$t') > 1) |
                                        (brno_df['765'].str.count('\$a') > 1)]
difficult_dbs_dict.update({'Brno multiple original titles': brno_multiple_original_titles})
brno_df = brno_df[~brno_df['001'].isin(brno_multiple_original_titles['001'])].reset_index(drop=True)

brno_df.to_excel('brno_df.xlsx', index=False)
    
#%% NKC database
try:
    nkc_df = pd.read_excel("C:/Users/Cezary/Downloads/skc_translations_cz_authority_2021-8-12.xlsx").drop(columns=['cz_id', 'cz_name']).drop_duplicates().reset_index(drop=True)
except FileNotFoundError:
    nkc_df = pd.read_excel("C:/Users/Rosinski/Downloads/Translations/skc_translations_cz_authority_2021-8-12.xlsx").drop(columns=['cz_id', 'cz_name']).drop_duplicates().reset_index(drop=True)

nkc_df['005'] = nkc_df['005'].apply(lambda x: convert_float_to_int(x))

nkc_df['fiction_type'] = nkc_df['008'].apply(lambda x: x[33])
nkc_df['audience'] = nkc_df['008'].apply(lambda x: x[22])

nkc_df = nkc_df[~nkc_df['language'].isin(['pol', 'fre', 'eng', 'ger'])].drop_duplicates().reset_index(drop=True)

for i, row in tqdm(nkc_df.iterrows(), total=nkc_df.shape[0]):   
    nkc_df.at[i, '100'] = f"{row['100']}$1http://viaf.org/viaf/{row['viaf_id']}"

nkc_df['260'] = nkc_df[['260', '264']].apply(lambda x: x['260'] if pd.notnull(x['260']) else x['264'], axis=1)
nkc_df['240'] = nkc_df[['240', '246']].apply(lambda x: x['240'] if pd.notnull(x['240']) else x['246'], axis=1)
nkc_df['100_unidecode'] = nkc_df['100'].apply(lambda x: unidecode.unidecode(x).lower() if pd.notnull(x) else x)

start_value = 100000000000
nkc_df['fakeid'] = pd.Series(range(start_value,start_value+nkc_df.shape[0]+1,1))
nkc_df['fakeid'] = nkc_df['fakeid'].astype('Int64').astype('str')      
nkc_df['001'] = nkc_df.apply(lambda x: get_oclc_id(x), axis=1)
nkc_df.drop(columns='fakeid', inplace=True)
nkc_df['SRC'] = 'NKC'

nkc_df.to_excel('nkc_df.xlsx', index=False)

brno_df = pd.read_excel('brno_df.xlsx')
nkc_df = pd.read_excel('nkc_df.xlsx')

total = pd.concat([brno_df, nkc_df]).reset_index(drop=True)
total['001'] = total['001'].astype(np.int64)


# try:
#     oclc_df = pd.read_excel("C:/Users/Cezary/Downloads/oclc_all_positive.xlsx").drop(columns=['all_names', 'cz_name', 'type of record + bibliographic level', 'nature_of_contents', 'viaf_id']).drop_duplicates().reset_index(drop=True)
# except FileNotFoundError:
#     oclc_df = pd.read_excel("C:/Users/Rosinski/Downloads/Translations/oclc_all_positive.xlsx").drop(columns=['all_names', 'cz_name', 'nature_of_contents', 'type of record + bibliographic level', 'viaf_id']).drop_duplicates().reset_index(drop=True)
    
oclc_df = pd.read_excel("oclc_all_positive.xlsx").drop_duplicates().reset_index(drop=True)

oclc_df['viaf_id'] = ''
for i, row in tqdm(oclc_df.iterrows(), total=oclc_df.shape[0]):
    try:
        oclc_df.at[i, 'viaf_id'] = re.findall('\d+', marc_parser_dict_for_field(row['100'], '\\$')['$1'])[0]
    except (KeyError, TypeError):
        oclc_df.at[i, 'viaf_id'] = np.nan
        
wrong_oclc_viafs = list(set([re.findall('\d+', e)[0] for e in oclc_df['viaf_id'].to_list() if pd.notnull(e)]))
translation_viafs = [k for k,v in viaf_positives_dict.items()]
wrong_oclc_viafs = [e for e in wrong_oclc_viafs if e not in translation_viafs]
wrong_oclc_viafs = oclc_df[oclc_df['viaf_id'].isin(wrong_oclc_viafs)]

difficult_dbs_dict.update({'OCLC wrong viafs': wrong_oclc_viafs})

oclc_df = oclc_df[~oclc_df['001'].isin(wrong_oclc_viafs['001'])].reset_index(drop=True)

oclc_df['fiction_type'] = oclc_df['008'].apply(lambda x: x[33])
oclc_df['audience'] = oclc_df['008'].apply(lambda x: x[22])

oclc_df = replace_viaf_group(oclc_df).drop_duplicates().reset_index(drop=True)
oclc_df['001'] = oclc_df['001'].astype(np.int64)
oclc_df['SRC'] = 'OCLC'

oclc_multiple_original_titles = oclc_df[(oclc_df['240'].str.count('\$a') > 1) | 
                                        (oclc_df['765'].str.count('\$t') > 1)]
difficult_dbs_dict.update({'OCLC multiple original titles': oclc_multiple_original_titles})
oclc_df = oclc_df[~oclc_df['001'].isin(oclc_multiple_original_titles['001'])].reset_index(drop=True)

oclc_selections = oclc_df[oclc_df['240'].str.contains('\$k', na=False)]
difficult_dbs_dict.update({'OCLC selections': oclc_selections})
oclc_df = oclc_df[~oclc_df['001'].isin(oclc_selections['001'])].reset_index(drop=True)

oclc_without_author = oclc_df[oclc_df['100'].isnull()]
difficult_dbs_dict.update({'OCLC without author in 100': oclc_without_author})
oclc_df = oclc_df[~oclc_df['001'].isin(oclc_without_author['001'])].reset_index(drop=True)

writer = pd.ExcelWriter(f'difficult_records_{now}.xlsx', engine = 'xlsxwriter')
for k, v in difficult_dbs_dict.items():
    v.to_excel(writer, index=False, sheet_name=k)
writer.save()
writer.close()  

total = pd.concat([cz_total, oclc_df])

total.to_excel(f'initial_stage_{now}.xlsx', index=False)

#tutaj zbiorczo wrzucać difficult records!!

# # ograć duplikaty
# total_grouped = total.groupby('001')
# duplicates_clt_oclc = total_grouped.filter(lambda x: len(x) > 1).sort_values('001')
# total = total[~total['001'].isin(duplicates_clt_oclc['001'])]
# duplicates_clt_oclc_grouped = duplicates_clt_oclc.groupby('001')
# #3630 duplikatów

# #765 z brno, 240 z oclc
# brno_oclc_deduplicated = pd.DataFrame()
# for name, group in tqdm(duplicates_brno_oclc_grouped, total=len(duplicates_brno_oclc_grouped)):
#     for column in group:
#         if column == '240':
#             group[column] = group[group['SRC'] == 'OCLC'][column]
#         elif column == '245':
#             group[column] = group[group['SRC'] == 'Brno'][column]
#         elif column in ['fiction_type', 'audience', '020', '041', '260']:
#             group[column] = '❦'.join(group[column].dropna().drop_duplicates().astype(str))
#         else:
#             group[column] = group[group['SRC'] == 'Brno'][column]
#     brno_oclc_deduplicated = brno_oclc_deduplicated.append(group)

# brno_oclc_deduplicated = brno_oclc_deduplicated[brno_oclc_deduplicated['001'].notnull()]
# brno_oclc_deduplicated['001'] = brno_oclc_deduplicated['001'].astype(np.int64)

# translations_df = pd.concat([total, brno_oclc_deduplicated]).reset_index(drop=True)  
# fields = translations_df.columns.tolist()
# fields.sort(key = lambda x: ([str,int].index(type(1 if re.findall(r'\w+', x)[0].isnumeric() else "a")), x))
# translations_df = translations_df.reindex(columns=fields) 






























