import pandas as pd
import regex as re
import numpy as np
from my_functions import gsheet_to_df#, marc_parser_dict_for_field
from tqdm import tqdm
from collections import Counter
import warnings
import gspread as gs
from my_functions import create_google_worksheet
import time
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
from concurrent.futures import ThreadPoolExecutor
import json
from datetime import datetime

warnings.simplefilter(action='ignore', category=FutureWarning)
warnings.simplefilter(action='ignore', category=pd.errors.PerformanceWarning)
pd.set_option('display.float_format', lambda x: '%.3f' % x)
pd.options.mode.chained_assignment = None

#%%
now = datetime.now().date()
year = now.year
month = '{:02}'.format(now.month)
day = '{:02}'.format(now.day)

#%% defs

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
        
#%%wprowadzić system aktualizacji na podstawie manualnych prac Ondreja!!!
#połączenie z dyskiem
gc = gs.oauth()
gauth = GoogleAuth()
gauth.LocalWebserverAuth()
drive = GoogleDrive(gauth)
#%% wgranie danych
files_list = drive.ListFile({'q': "'1CJwe0Bl-exd4aRyqCMqv_XHSyLuE2w4m' in parents and trashed=false"}).GetList() 
translations_df = pd.read_excel('translation_before_manual_2022-09-20.xlsx')
# with open('authors_with_works.json', 'rt', encoding='utf-8') as f:
#     authors_with_works = json.load(f)
#%% statystyki
# next_translations_df = pd.read_excel('translations_after_first_manual_with_germany_2022-08-12.xlsx')
# translations_df_new = next_translations_df .copy()
# translations_df = gsheet_to_df('1wy64th7IjF0ktAqjz3NQcflNYLAkStD3', 'Arkusz1')
work_ids = dict(zip(translations_df['001'], translations_df['work_id']))
work_ids_list = list(work_ids.values())
#sizes of clusters
work_ids_counter = dict(Counter(work_ids_list))
work_ids_nan = {k:v for k,v in work_ids_counter.items() if pd.isnull(k)}
work_ids_numbers = {k:v for k,v in work_ids_counter.items() if pd.notnull(k) and v >= 10}
work_id_more_lang = translations_df.groupby(['work_id']).filter(lambda x: len(x) and len(set(x['language'])) >= 2)[['001', 'author_id', 'work_id', 'language']]
no_of_works = work_id_more_lang['work_id'].drop_duplicates().shape[0]
works = set(work_id_more_lang['work_id'])
#combining two approaches: size of a cluster >= 5 and at least 2 languages
work_ids = [e for e in work_ids_numbers if e in works]
work_ids_sizes = {}
for el in work_ids:
    size = translations_df[translations_df['work_id'] == el].shape[0]
    work_ids_sizes[el] = size

work_ids_sizes = sorted(work_ids_sizes.items(), key = lambda x: (-x[1], x[0]))
proper_columns = ['001', '240', '245', '245a', 'language', '260', 'author_id', 'work_id', 'work_title', '490', '500', 'simple_original_title', '776', 'sorted']
work_id_author_id_dict = dict(zip(translations_df['work_id'], translations_df['author_id']))
work_id_author_id_dict = {k:v for k,v in work_id_author_id_dict.items() if k in work_ids}

#%% na razie zbędne #wprowadzanie zmian

# statystyki ile clustrów mają autorzy
author_clusters_len = dict(Counter(work_id_author_id_dict.values()))
author_clusters_len = {k: v for k, v in sorted(author_clusters_len.items(), key=lambda item: item[1], reverse=True)}

rounds_dict = {}
for round_no in range(1,24):
    rounds_dict[round_no] = len([e for e in author_clusters_len.values() if e >= round_no])
test_df = pd.DataFrame.from_dict(rounds_dict, orient='index')

#statystyka progresu dla autorów -- to jest ważne

authors_ids = set(work_id_author_id_dict.values())
authors_with_works = {}
for author_id in tqdm(authors_ids):
    # author_id = '109312616'
    author_name = marc_parser_dict_for_field(Counter(translations_df[translations_df['author_id'] == author_id]['100'].to_list()).most_common(1)[0][0], '\$').get('$a')
    clusters_for_author = {k for k,v in work_id_author_id_dict.items() if v == author_id}
    clusters_for_author_with_size = [[a,b] for a,b in work_ids_sizes if a in clusters_for_author]
    [e.append(True) if e[0] in [int(el['title'].split('_')[2]) for el in files_list] else e.append(False) for e in clusters_for_author_with_size]
    clusters_for_author_with_size = [dict(zip(['cluster_id', 'original_cluster_size', 'edited'], e)) for e in clusters_for_author_with_size]
    temp_dict = {author_id: {'author_name': author_name,
                             'clusters': clusters_for_author_with_size}}
    authors_with_works.update(temp_dict)
 
authors_with_works.pop('46774385')
 
with open('authors_with_works.json', 'w', encoding='utf-8') as f:
    json.dump(authors_with_works, f, ensure_ascii=False, indent=4)
    
#%% zbieranie informacji z pracy manualnej

#!!!usunięte clustry:
    # 265_34469656_26799077_4 -- był pusty
    # 258_76500434_13205408_6 -- był pusty, bo wszystkie rekordy zostały przypisane do 054_76500434_19029585_1

files_list = drive.ListFile({'q': f"'1CJwe0Bl-exd4aRyqCMqv_XHSyLuE2w4m' in parents and trashed=false"}).GetList() 

used_clusters = [int(e['title'].split('_')[2]) for e in files_list]

modified_sheets = files_list.copy()

translations_df_new = translations_df.copy()  

edited_records = []
edited_clusters = []
for modified_sheet in tqdm(modified_sheets):
    while True:

        cluster_no, author_id, work_id, author_fequency = modified_sheet['title'].split('_')
        edited_clusters.append(work_id)

        try:
            temp_df = gsheet_to_df(modified_sheet['id'], work_id)[[1, 'to_retain']].rename(columns={1:'001'})
            temp_dict = dict(zip(temp_df['001'].to_list(), temp_df['to_retain'].to_list()))
            edited_records.extend([int(e) for e in {k for k,v in temp_dict.items() if v == 'x'}])            
            temp_dict = {int(k):int(work_id) if v == 'x' else v for k,v in temp_dict.items()}
            temp_dict = {k:v for k,v in temp_dict.items() if not isinstance(v, float)}
            temp_ids = translations_df_new.loc[translations_df_new['work_id'] == int(work_id)]['001'].to_list()
            translations_df_new.loc[translations_df_new['001'].isin(temp_ids), 'work_id'] = np.nan
            translations_df_new['work_id'] = translations_df_new[['001', 'work_id']].apply(lambda x: temp_dict[x['001']] if x['001'] in temp_dict else x['work_id'], axis=1)
            break
            # try:
            #     translations_df_new[['001', 'work_id']].apply(lambda x: temp_dict[x['001']] if x['001'] in temp_dict else x['work_id'], axis=1)
            # except IndexError:
            #     print(cluster_no)
        except KeyboardInterrupt as error:
            raise error
        except Exception:
            time.sleep(10)
            print(modified_sheet['title'])
            # collect_modification(modified_sheet)
            continue


translations_df_new['work_id'] = translations_df_new['work_id'].apply(lambda x: np.int64(x) if isinstance(x, int) else x)  
translations_df_new = translations_df_new.loc[translations_df_new['author_id'] != '46774385']
to_be_changed = translations_df_new.loc[translations_df_new['work_id'] == 57775596]['001'].to_list()
translations_df_new.loc[translations_df_new['001'].isin(to_be_changed), ['author_id', 'work_id']] = ['95207407', 2592307]

test = translations_df_new.copy()

ids_to_be_changed = [233979417, 499657879, 609190927, 865861032, 871518282, 948562166, 1017395251, 1037670745, 1120923101, 693046345, 612484641, 10000000648, 70357873, 638788889, 654310823, 756886304, 970939398, 1069895800, 1075256608, 223391512, 85568040, 22259771, 85701806, 693046207, 1031719525, 1185289556, 1126992949, 1185327579, 77710868, 470367451, 631744717, 887579640, 894762749, 1011668322, 1193400156, 651213307, 651373365, 692935236, 1011215706, 1196397163, 254963673, 911815625, 123755002, 977421011, 977917241, 21715009, 1080763089, 959631455, 994329657, 639960874, 644008507, 187126309, 634453477, 279460747, 187126320, 599773453, 34659978, 562925701, 562925708, 752809693, 976900995, 1024710747]

translations_df_new.loc[translations_df_new['001'].isin(ids_to_be_changed), ['author_id']] = '34454129'
translations_df_new.to_excel(f'translations_after_manual_{now}.xlsx', index=False)
   
edited_clusters = list(set(edited_clusters))  
with open('translation_edited_clusters.txt', 'wt', encoding='utf-8') as f:
    for el in edited_clusters:
        f.write(f'{el}/n')

Counter(edited_records).most_common(10)

#%% dodawanie kolejnych tabel do pracy manualnej
# # modified_sheets = [e for e in modified_sheets if not e['title'].startswith(('030', '034'))]
# #tutaj mogę wyśledzić wyjątki -- nieregularnych autorów
# exceptions = {k:v for k,v in authors_with_works.items() if any(e.get('edited') == False for e in v.get('clusters'))}

# used_authors_dict = {}
# for el in modified_sheets:
#     author_id = el['title'].split('_')[1]
#     if author_id not in used_authors_dict:
#         used_authors_dict[author_id] = 1
#     else:
#         used_authors_dict[author_id] += 1

# used_authors_dict = {k:[e['title'] for e in modified_sheets if k in e['title'] and str(v) == e['title'].split('_')[-1]][0] for k,v in used_authors_dict.items()}
# max_books_per_author = max([e[-1] for e in used_authors_dict.values()])
# latest_work_id_for_author = [e.split('_')[2] for e in used_authors_dict.values() if e[-1] == max_books_per_author]

# # for work_id in latest_work_id_for_author:
#     # work_id = latest_work_id_for_author[-2]
# # def upload_next_clusters(work_id):
# for work_id in tqdm(latest_work_id_for_author):
#     # work_id = latest_work_id_for_author[0]
#     while True:

#         author_id = work_id_author_id_dict[int(work_id)]
#         sheet_id = [e for e in files_list if all(el in e['title'] for el in [work_id, author_id])][0]['id']
#         sheet_for_author_no = int([e['title'].split('_')[-1] for e in files_list if e['id'] == sheet_id][0])
#         try:
#             temp_df = gsheet_to_df(sheet_id, work_id)[[1, 'to_retain', 'work_id']].rename(columns={1:'001'})
#             temp_ids = [int(e) for e in temp_df.loc[temp_df['to_retain'] != 'x']['001'].to_list()]
#             work_id_size = [e[1] for e in work_ids_sizes if e[0] == int(work_id)][0]
            
#             try:
#                 new_cluster = [e for e in work_ids_sizes if e[0] in {k for k,v in work_id_author_id_dict.items() if v == author_id} and e[1] < work_id_size][0][0]
#                 new_cluster_df = translations_df_new.loc[translations_df_new['work_id'] == new_cluster]
#                 rest_df = translations_df_new.loc[translations_df_new['001'].isin(temp_ids)].sort_values('work_id')
#                 rest_df = rest_df.loc[~rest_df['001'].isin(edited_records)]
#                 temp_df = pd.concat([new_cluster_df, rest_df]).drop_duplicates()
#                 temp_df['to_retain'] = np.nan
#                 temp_df['245a'] = temp_df['245'].apply(lambda x: marc_parser_dict_for_field(x, '\$')['$a'] if not(isinstance(x, float)) and '$a' in x else np.nan)
#                 temp_df = temp_df[['001', '240', 'to_retain', '245', '245a', 'language', '260', '490', '500', '776', 'author_id', 'work_title', 'simple_original_title', 'work_id']]
#                 temp_df['work_id'] = temp_df['work_id'].apply(lambda x: str(x) if isinstance(x, np.int64) else x) 
                
#                 ind = [i for i,e in enumerate(work_ids_sizes,1) if e[0] == new_cluster][0]
#                 ind = '{:03d}'.format(ind)
                
#                 sheet = gc.create(f'{ind}_{author_id}_{int(new_cluster)}_{sheet_for_author_no+1}', '1CJwe0Bl-exd4aRyqCMqv_XHSyLuE2w4m')
#                 # create_google_worksheet(sheet.id, str(int(new_cluster)), temp_df)
#                 try:
#                     create_google_worksheet(sheet.id, str(int(new_cluster)), temp_df)
#                 except Exception:
#                     time.sleep(60)
#                     create_google_worksheet(sheet.id, str(int(new_cluster)), temp_df)
#                 except KeyboardInterrupt:
#                     raise
#             except IndexError:
#                 pass
#             break
#         except KeyboardInterrupt as error:
#             raise error
#         except Exception:
#             time.sleep(10)
#             continue     
        
#%% uzupełnienia, tego, co się wymknęło

#dodawać po każdej rundzie kolejne
empty_clusters = [42155155, 26799077, 57775596, 1431393, 16772379, 13205408, 63403408, 708331248, 155533, 13205408, 181698433, 23846781, 260010551, 26443215, 60418681, 45714503, 52570625, 10904089, 69507527]
empty_sheets = ['212_29531402_42155155_2', '265_34469656_26799077_4', '030_42003196_57775596_1', '034_46774385_1431393_1', '258_76500434_13205408_7', '140_19683055_63403408_3', '046_46774385_708331248_1', '056_56651696_155533_1', '258_76500434_13205408_7', '246_4931097_181698433_5', '056_56651696_155533_5', '146_51691735_23846781_17', '153_51691735_260010551_17', '174_51691735_26443215_17', '181_51691735_60418681_17', '197_51691735_45714503_17', '198_51691735_52570625_17', '256_34454129_10904089_23', '273_51691735_69507527_17']

exceptions = {k:v for k,v in authors_with_works.items() if any(e.get('edited') == False for e in v.get('clusters'))}

exceptions = {k:{ka:[{kb:True if kb == 'edited' and e['cluster_id'] in empty_clusters else vb for kb,vb in e.items()} for e in va] if isinstance(va,list) else va for ka,va in v.items()} for k,v in exceptions.items()}
exceptions = {k:v for k,v in exceptions.items() if any(e.get('edited') == False for e in v.get('clusters'))}

# test = {k:v for k,v in authors_with_works.items() if any(e.get('edited') == False for e in v.get('clusters')) and any(e.get('edited') not in empty_clusters for e in v.get('clusters'))}

supplements = {k:[e.get('cluster_id') for e in v.get('clusters') if e.get('edited') == False][0] for k,v in exceptions.items()}

new_spreadsheets = []
for author_id, work_id in tqdm(supplements.items()):
    # author_id, work_id = list(supplements.items())[3]
    while True:
        try:
            previous_author_ids = [int(e['title'].split('_')[2]) for e in files_list if author_id in e['title']]
            new_cluster_df = translations_df_new.loc[translations_df_new['work_id'] == work_id]
            rest_df = translations_df_new.loc[translations_df_new['author_id'] == author_id]
            rest_df = rest_df.loc[~rest_df['work_id'].isin(previous_author_ids)].sort_values('work_id')
            rest_df = rest_df.loc[~rest_df['001'].isin(edited_records)]
            temp_df = pd.concat([new_cluster_df, rest_df]).drop_duplicates()
            temp_df['to_retain'] = np.nan
            temp_df['245a'] = temp_df['245'].apply(lambda x: marc_parser_dict_for_field(x, '\$')['$a'] if not(isinstance(x, float)) and '$a' in x else np.nan)
            temp_df = temp_df[['001', '240', 'to_retain', '245', '245a', 'language', '260', '490', '500', '776', 'author_id', 'work_title', 'simple_original_title', 'work_id']]
            temp_df['work_id'] = temp_df['work_id'].apply(lambda x: str(x) if isinstance(x, np.int64) else x) 
            
            ind = [i for i,e in enumerate(work_ids_sizes,1) if e[0] == work_id][0]
            ind = '{:03d}'.format(ind)
            
            new_spreadsheet_name = f'{ind}_{author_id}_{int(work_id)}_{len(previous_author_ids)+1}'
            if new_spreadsheet_name not in empty_sheets and not new_spreadsheet_name.startswith('056'): 
                sheet = gc.create(new_spreadsheet_name, '1CJwe0Bl-exd4aRyqCMqv_XHSyLuE2w4m')
                new_spreadsheets.append(new_spreadsheet_name)
                try:
                    create_google_worksheet(sheet.id, str(int(work_id)), temp_df)
                except Exception:
                    time.sleep(60)
                    create_google_worksheet(sheet.id, str(int(work_id)), temp_df)
                except KeyboardInterrupt:
                    raise
        except IndexError:
            pass
        break

print(new_spreadsheets)

#%% dodatki po każdej iteracji

#sprawdzić ręcznie, które clustry są puste (bo zostały przypisane wcześniej) i zmienić je w supplements na edited
#puste: 212_29531402_42155155_2, 265_34469656_26799077_4, 030_42003196_57775596_1, 034_46774385_1431393_1
# 258_76500434_13205408_7, 140_19683055_63403408_3, 046_46774385_708331248_1
# 056_56651696_155533_1
# 258_76500434_13205408_7
new_spreadsheets = [e for e in new_spreadsheets if e not in empty_sheets]
print(new_spreadsheets)

# na sam koniec powtórzyć z dodanymi clustrami do empty_clusters

authors_with_works = {k:{ka:[{kb:True if kb == 'edited' and e['cluster_id'] in empty_clusters else vb for kb,vb in e.items()} for e in va] if isinstance(va,list) else va for ka,va in v.items()} for k,v in authors_with_works.items()}

with open('authors_with_works.json', 'w', encoding='utf-8') as f:
    json.dump(authors_with_works, f, ensure_ascii=False, indent=4)
























