import pandas as pd
from datetime import datetime
import regex as re
import numpy as np
from my_functions import marc_parser_1_field, cSplit, cluster_records, simplify_string#, marc_parser_dict_for_field
import unidecode
from tqdm import tqdm
import requests
from collections import Counter
import warnings
import io
import pickle 
from copy import deepcopy
from difflib import SequenceMatcher
import ast


warnings.simplefilter(action='ignore', category=FutureWarning)
pd.set_option('display.float_format', lambda x: '%.3f' % x)

#%%
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

def get_viaf_from_nkc(x):
    try:
        return nkc_viaf_ids[x]
    except KeyError:
        return np.nan
    
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
#BRNO
# brno_df = pd.read_excel("C:\\Users\\Cezary\\Desktop\\brno_df.xlsx")
# brno_titles = brno_df[['viaf_id', '245', '240', '246', '765', 'language']]

# #w 765 české: lub (w 240 $l i w 240 nie ma $k)

# brno_titles = brno_titles[(brno_titles['765'].str.contains('české')) | ((brno_titles['240'].str.contains('$l', na=False)) & (~brno_titles['240'].str.contains('$k', na=False)))]

# brno_titles['target_title'] = brno_titles['245'].apply(lambda x: re.sub('\/$|\:$|\;$', '', marc_parser_dict_for_field(x, '\$')['$a'].strip()).strip())
# brno_titles['original_title'] = brno_titles['765'].apply(lambda x: re.sub('\/$|\:$|\;$', '', marc_parser_dict_for_field(x, '\$')['$t'].strip()).strip() if '$t' in x else np.nan)

# brno_titles = brno_titles[brno_titles['original_title'].notnull()][['viaf_id', 'language', 'target_title', 'original_title']].drop_duplicates()

# brno_titles = brno_titles.groupby(['viaf_id', 'original_title'])

# translation_db_dict = {}
# for name, group in tqdm(brno_titles, total=len(brno_titles)):
#     k = name
#     temp_dict = {}
#     if k not in translation_db_dict:
#         temp_dict.update({k:{}})
#     original_title = [e for e in group['original_title']][0]
#     author_viaf = [e for e in group['viaf_id']][0]
#     temp_dict[k].update({'original_title': original_title, 'author_viaf': author_viaf})
#     translations_list = list(set([(simplify_string(tit.strip()), lan) for tit, lan in zip(group['target_title'], group['language'])]))
#     translations_list = [e for e in translations_list if e]
#     temp_dict[k].update({'translations': translations_list})
#     translation_db_dict.update(temp_dict)

# #UCL
# cz_translation_database = pd.read_csv("C:\\Users\\Cezary\\Downloads\\enriched.tsv", sep='\t')
# cz_authority = pd.read_excel("C:\\Users\\Cezary\\Desktop\\new_cz_authority_df_2021-11-22.xlsx")
# cz_authority['nkc_id'] = cz_authority['IDs'].apply(lambda x: re.findall(r'(?<=NKC\|)(.+?)(?=❦|$)', x)[0] if '❦' in x else x)
# cz_authority['nkc_id'] = cz_authority['nkc_id'].apply(lambda x: re.sub('^NKC\|', '', x))

# nkc_viaf_ids = cz_authority[['nkc_id', 'viaf_id']].to_dict(orient='index')
# nkc_viaf_ids = {v['nkc_id']:v['viaf_id'] for k,v in nkc_viaf_ids.items()}

# #w tłumaczeniach są ludzie, których nie ma w projekcie

# cz_translation_database['viaf_id'] = cz_translation_database['author_id'].apply(lambda x: get_viaf_from_nkc(x))
# viaf_groups = {'118529174|256578118':'118529174', '25095273|83955898':'25095273', '11196637|2299152636076120051534':'11196637', '163185334|78000938':'163185334', '88045299|10883385':'88045299'}
# cz_translation_database['viaf_id'] = cz_translation_database['viaf_id'].replace(viaf_groups)
# cz_translation_database['orig_title_e'] = cz_translation_database[['orig_title_e', 'orig_title']].apply(lambda x: x['orig_title_e'] if pd.notnull(x['orig_title_e']) else x['orig_title'][4:], axis=1)

# cz_translation_database = cz_translation_database[cz_translation_database['viaf_id'].notnull()][['viaf_id', 'orig_title_e', 'norm_title', 'target_lang']].rename(columns={'orig_title_e':'target_title', 'norm_title':'original_title','target_lang':'language'})
# cz_translation_database['original_title'] = cz_translation_database['original_title'].replace('\.$', '', regex=True)
# cz_translation_database['target_title'] = cz_translation_database['target_title'].replace('\/$|\:$|\;$', '', regex=True)


# cz_database_grouped = cz_translation_database.groupby(['viaf_id', 'original_title'])

# for name, group in tqdm(cz_database_grouped, total=len(cz_database_grouped)):
#     k = name
#     original_title = [e for e in group['original_title']][0]
#     author_viaf = [e for e in group['viaf_id']][0]
#     translations_list = list(set([(simplify_string(tit.strip()), lan) for tit, lan in zip(group['target_title'], group['language'])]))
#     translations_list = [e for e in translations_list if e]
#     if k not in translation_db_dict:
#         temp_dict = {k:{}}
#         temp_dict[k].update({'original_title': original_title, 'author_viaf': author_viaf})
#         temp_dict[k].update({'translations': translations_list})
#         translation_db_dict.update(temp_dict)
#     else:
#         translation_db_dict[k]['translations'].extend([e for e in translations_list if e not in translation_db_dict[k]['translations']])
        
# #VIAF
# with open('viaf_work_dict_pi.pickle', 'rb') as handle:
#     viaf_work_dict = pickle.load(handle)
    
    
# viaf_transformed = {}
# for k,v in tqdm(viaf_work_dict.items()):
#     # k = '1442145424587186830717'
#     # v = viaf_work_dict[k]
#     new_key = (v['author_viaf'], v['work_title'])
#     author_viaf = v['author_viaf']
#     original_title = v['work_title']
#     work_viaf = k
#     translations_list = list({(simplify_string(value['translation_title']).strip(), value['translation_language']) for key,value in v['translations'].items()})
#     viaf_transformed.update({new_key:{'original_title': original_title, 'author_viaf': author_viaf, 'translations': translations_list, 'work_viaf':work_viaf}})
    
# for k,v in tqdm(viaf_transformed.items()):
#     if k not in translation_db_dict:
#         translation_db_dict.update({k:v})
#     else:
#         translation_db_dict[k]['translations'].extend([e for e in translations_list if e not in translation_db_dict[k]['translations']])
#         translation_db_dict[k].update({'work_viaf':v['work_viaf']})

# for i, (k,v) in enumerate(translation_db_dict.items(),1):
#     translation_db_dict[k].update({'index':i})
        
# df = pd.DataFrame.from_dict(translation_db_dict, orient='index').reset_index()
# df.to_excel('trans_db.xlsx')

df = pd.read_excel('trans_db.xlsx')

df2 = df.groupby('index')
translation_db_dict = {}
for name, group in tqdm(df2):
    viaf_aut = [e for e in group['level_0']][0]
    orig_title = [e for e in group['level_1']][0]
    key = (viaf_aut, orig_title)
    translations = [ast.literal_eval(e) for e in group['translations']]
    translations = list(set([e for sub in translations for e in sub]))
    try:
        work_viaf = [e for e in group['work_viaf'] if pd.notnull(e)][0]
    except IndexError:
        work_viaf = np.nan
    translation_db_dict.update({key:{'index':name, 'original_title': orig_title, 'author_viaf': viaf_aut, 'translations': translations, 'work_viaf': work_viaf}})
translation_db_dict = {k:v for k,v in translation_db_dict.items() if v['translations']}
      
df_with_original_titles = pd.read_excel('df_with_title_clusters_2022-03-15.xlsx', dtype=str)
df_with_original_titles.rename(columns={'original title': 'original_title'}, inplace=True)
df_with_original_titles['001'] = df_with_original_titles['001'].astype(np.int64)


records_simple = df_with_original_titles[['001', 'language', '245', 'author_id', 'work_id', 'work_title', 'original_title', 'cluster_titles']]

test2 = records_simple[(records_simple['cluster_titles'] == '2324532') | 
                       (records_simple['work_id'] == '2601147270548635700000')]

print(Counter(df_with_original_titles['cluster_titles'].notnull()))
#Counter({False: 29430, True: 25496})
print(Counter(df_with_original_titles['cluster_titles'].notnull() | df_with_original_titles['work_id'].notnull()))
#Counter({False: 29212, True: 25714})

records_simple_list = records_simple.to_dict(orient='records')

for d in tqdm(records_simple_list):
    # d = test[15]
    simple_245 = marc_parser_dict_for_field(d['245'], '\$')
    try:
        simple_245.pop('$c')
    except KeyError:
        pass
    simple_245 = simplify_string(''.join([e.strip() for e in simple_245.values()]))
    d.update({'simple': simple_245})

for d in tqdm(records_simple_list):
    for trans_k, trans_v in translation_db_dict.items():
        if d['author_id'] == trans_v['author_viaf']:
            for trans in trans_v['translations']:
                if find_similar_target_titles(trans[0], d['simple'], 0.9):
                    d.update({'new_original': trans_v['original_title'], 'new_cluster': trans_v['index']})

df = pd.DataFrame(records_simple_list)
print(Counter(df['cluster_titles'].notnull()))
print(Counter(df['new_cluster'].notnull()))
#Counter({False: 35467, True: 19459})
print(Counter(df['cluster_titles'].notnull() | df['new_cluster'].notnull() | df['work_id'].notnull()))
# Counter({True: 32090, False: 22836})

new_clusters = sorted(df['new_cluster'].dropna().drop_duplicates().to_list())
clusters_dict = {}
dublety = []
for cluster in tqdm(new_clusters):
    if clusters_dict:
        clusters_list = [[el[0] for el in e] for e in [*clusters_dict.values()]]
        clusters_list = [e for sub in clusters_list for e in sub]
        clusters_list = [e for sub in clusters_list for e in sub]
        if cluster in clusters_list:
            doubled_cluster = {k for k,v in clusters_dict.items() if cluster in [el for sub in [e[0] for e in v] for el in sub]}.pop()
            dublety.append((cluster, doubled_cluster))
    # cluster = 1
    # cluster = 867
    test_df = df[df['new_cluster'] == cluster]
    sets = []
    sets.append((set(test_df['new_cluster'].dropna().unique()), set(test_df['work_id'].dropna().unique()), set(test_df['cluster_titles'].dropna().unique())))
    test_df = df[(df['new_cluster'].isin(list(sets[-1][0]))) |
                 (df['work_id'].isin(sets[-1][1])) |
                 (df['cluster_titles'].isin(sets[-1][-1]))]
    while sets[-1] != (set(test_df['new_cluster'].dropna().unique()), set(test_df['work_id'].dropna().unique()), set(test_df['cluster_titles'].dropna().unique())):
        sets.append((set(test_df['new_cluster'].dropna().unique()), set(test_df['work_id'].dropna().unique()), set(test_df['cluster_titles'].dropna().unique())))
        test_df = df[(df['new_cluster'].isin(list(sets[-1][0]))) |
                     (df['work_id'].isin(sets[-1][1])) |
                     (df['cluster_titles'].isin(sets[-1][-1]))]
    clusters_dict.update({cluster:sets})

ok = []
not_ok = []
for e in clusters_dict:
    if len(clusters_dict[e]) == 1:
        ok.append(clusters_dict[e][0])
    elif len(set([elem for sub in [[ele for ele in el[0]] for el in clusters_dict[e]] for elem in sub])) == 1:
        ok.append(clusters_dict[e][-1])
    else:
        not_ok.append(clusters_dict[e][-1])
        
not_ok_unique = []
for el in not_ok:
    if el not in not_ok_unique:
        not_ok_unique.append(el)


bad_df = df[(df['new_cluster'] == not_ok_unique[3][0]) |
            (df['work_id'].isin(not_ok_unique[3][1])) |
            (df['cluster_titles'].isin(not_ok_unique[3][-1]))]

bad_df['new_cluster'] = bad_df[['001', 'new_cluster']].apply(lambda x: new_cluster_corrections[x['001']] if x['001'] in new_cluster_corrections else x['new_cluster'], axis=1)


#assigning to new_cluster:
new_cluster_corrections = {751359839: 28,
# Mach a Šebestová ve škole
10000000745: 34,
84940599: 34,
1108304260: 34,
# Mach a Šebestová za školou

# Mach, Šebestová a kouzelné sluchátko
10000005886: 1725,
1108272735: 1725,
443259514: 1725,

# Mach a Šebestová
320690017: max(df['new_cluster'])+1,
312686446: max(df['new_cluster'])+1,

 }


#!!!!!!!!!!!tutaj!!!!!!!!!!!!!!












a = df[df['new_cluster'].isin(dublety[0])]

clusters_dict[34]

ttt = df[df['new_cluster'].isin([1000, 1006])]
ttt = df[df['work_id'].isin(['178752278', '316388212'])]





    
    test_df2 = df[(df['new_cluster'] == sets[-1][0]) |
                  (df['work_id'].isin(sets[-1][1])) |
                  (df['cluster_titles'].isin(sets[-1][-1]))]
    while sets[-1] != (cluster, set(test_df['work_id'].dropna().unique()), set(test_df['cluster_titles'].dropna().unique()))
            
            
    work_ids = test_df['work_id'].dropna().drop_duplicates().to_list()
    test_df2 = df[df['cluster_titles'].isin(work_ids)]
    cluster_titles_ids = test_df['cluster_titles'].dropna().drop_duplicates().to_list()
    test_df3 = df[df['cluster_titles'].isin(cluster_titles_ids)]


    
records_simple2 = df[df['new_cluster'].isnull()]

test2 = df[df['new_cluster'] == 1159]

viaf_work_dict['316388212']

test2 = df[(df['work_id'] == '316388212') |
           (df['new_cluster'] == 867) |
           (df['cluster_titles'].isin(['2592307', '58019579', '10000008196', '58149273', '57775596', '961065800', '891301263']))]

print(test2['cluster_titles'].dropna().drop_duplicates().to_list())





test = df[df['new_cluster'].notnull()][['new_cluster', 'cluster_titles']].drop_duplicates()

# #jeśli relacja 1 do 1 to to:
# test2 = test.groupby("new_cluster").filter(lambda x: len(x) == 1 or (len(x) == 2 and 0 in x['cluster_titles'].to_list()))
# test2 = test2[test2['cluster_titles'] != 0]
#jeśli relacja 1 do 2 to to: – więcej jest ryzykowne, teraz skupić się na frekwencji target title
test2 = test.groupby("new_cluster").filter(lambda x: len(x) <= 2)
test2 = test2[test2['cluster_titles'] != 0]
#jeśli relacja 1 do wielu to to:
# test2 = test[test['cluster_titles'] != 0]

test2 = pd.Series(test2['new_cluster'].values, index=test2['cluster_titles']).to_dict()
        
df['new_cluster'] = df[['new_cluster', 'cluster_titles']].apply(lambda x: test2[x['cluster_titles']] if x['cluster_titles'] in test2 else x['new_cluster'], axis=1)
print(Counter(df['new_cluster'].notnull()))
#Counter({False: 23698, True: 23018})

#tam, gdzie jest new cluster, a new original jest pusty można wyciągnąć nowe tłumaczenia i później je też wykorzystać
test = df[(df['new_original'].isnull()) & df['new_cluster'].notnull()]

test_dict = {k:v for k,v in translation_db_dict2.items() if v['index'] in test['new_cluster'].to_list()}
translation_db_dict3 = {}
for k,v in tqdm(test_dict.items()):
    # k = ('100221612', 'Valdštejnova smrt')
    # v = test_dict[k]
    new_translations = test[test['new_cluster'] == v['index']]
    new_translations = list(set([(simplify_string(marc_parser_dict_for_field(tit, '\$')['$a'].strip()).strip(), lang) for tit, lang in zip(new_translations['245'], new_translations['language'])]))
    translation_db_dict3.update({k:{'index': v['index'], 'author_viaf': v['author_viaf'], 'original_title': v['original_title'], 'work_viaf': v['work_viaf'], 'translations': new_translations}})

#błędne klucze – może warto to potem edytować
to_delete = [('dilo etc usporadal miloslav novotny vol 125', 'und'), ('dilo jana nerudy', 'mul')]
translation_db_dict3[('41915819', 'Povídky malostranské')]['translations'] = [e for e in translation_db_dict3[('41915819', 'Povídky malostranské')]['translations'] if e not in to_delete]
translation_db_dict3.pop(('4931097', 'Tak pravil Josef Švejk'))

# mam teraz nowe tłumaczenia
ttt_list = df.to_dict(orient='records')
  
for d in tqdm(ttt_list):
    if pd.isnull(d['new_cluster']):
        for trans_k, trans_v in translation_db_dict3.items():
            if d['cluster_viaf'] == trans_v['author_viaf']:
                for trans in trans_v['translations']:
                    if find_similar_target_titles(trans[0], d['simple'], 0.9):
                        d.update({'new_original': trans_v['original_title'], 'new_cluster': trans_v['index']})
df = pd.DataFrame(ttt_list)    
print(Counter(df['cluster_titles'] != 0))
print(Counter(df['new_cluster'].notnull()))
#Counter({True: 26116, False: 20600})

#Counter({True: 29225, False: 19200}) – dla wcześniejszego podejścia
#Counter({False: 24753, True: 23672}) – dla relacji 1 index i 1 cluster
#Counter({True: 26926, True: 21496}) – dla relacji 1 index i 1 lub 2 clustry – i to jest ciągle okej
#Counter({True: 30297, False: 18128}) – dla relacji 1 index i wiele clustrów (tutaj mogą być powielane błędy, które są w danych)

# df.to_excel('translation_nowe_klastrowanie.xlsx', index=False)


#co można zrobić? dokładać tłumaczenia – najlepiej zacząć na podstawie frekwencji
#chyba lepiej skorzystać z wcześniejszych clustrów
test_list = df[df['new_cluster'].isnull()].to_dict(orient='records')

for d in tqdm(test_list):
    simple_245a = simplify_string(marc_parser_dict_for_field(d['245'], '\$')['$a'].strip()).strip()
    d.update({'simple_245a': simple_245a})

#cluster_viaf + simple_245a
test = [(e['cluster_viaf'], e['simple_245a']) for e in test_list]
test_count = Counter(test).most_common()

test_list2 = list(set([e['work_title'] for e in test_list if e['cluster_viaf'] == test_count[0][0][0] and e['simple_245a'] == test_count[0][0][-1]]))

# tłumaczenia na podstawie wcześniejszych clustrów

test_list = df[(df['new_cluster'].isnull()) & (df['cluster_titles'] != 0)]
test_grouped = test_list.groupby(['cluster_viaf', 'cluster_titles'])

#to_dict(orient='records')

translation_db_dict = {}
for name, group in tqdm(test_grouped, total=len(test_grouped)):
    # name = ('163185334', '3238151474902400490001')
    # name = ('34454129', 38)
    # group = test_grouped.get_group(name)
    k = name
    temp_dict = {}
    if k not in translation_db_dict:
        temp_dict.update({k:{}})
    try:
        original_title = [e for e in group['work_title'] if pd.notnull(e)][0]
    except IndexError:
        original_title = Counter([e for e in group['original title']]).most_common(1)[0][0]
    author_viaf = name[0]
    temp_dict[k].update({'original_title': original_title, 'author_viaf': author_viaf})
    translations_list = list(set([(simplify_string(marc_parser_dict_for_field(tit, '\$')['$a'].strip()).strip(), lang) for tit, lang in zip(group['245'], group['language'])]))
    translations_list = [e for e in translations_list if e]
    temp_dict[k].update({'translations': translations_list})
    translation_db_dict.update(temp_dict)

# translation_sorted = dict(sorted(translation_db_dict.items(), key = lambda item : len(item[-1]['translations']), reverse=True))
    
for i, (k,v) in enumerate(translation_db_dict.items(),4000):
    translation_db_dict[k].update({'index':i})
    
translation_db_dict = {k:v for k,v in translation_db_dict.items() if pd.notnull(v['original_title'])}

ttt_list = df.to_dict(orient='records')
  
for d in tqdm(ttt_list):
    if pd.isnull(d['new_cluster']):
        for trans_k, trans_v in translation_db_dict.items():
            if d['cluster_viaf'] == trans_v['author_viaf']:
                for trans in trans_v['translations']:
                    if find_similar_target_titles(trans[0], d['simple'], 0.9):
                        d.update({'new_original': trans_v['original_title'], 'new_cluster': trans_v['index']}) #ważne, żeby nie pomieszać clustrów, dlatego str
df2 = pd.DataFrame(ttt_list)    
print(Counter(df2['cluster_titles'] != 0))
print(Counter(df2['new_cluster'].notnull()))
# df2.to_excel('translation_nowe_klastrowanie.xlsx', index=False)
#Counter({True: 37611, False: 9105}) – dla nienadzorowanych relacji z wcześniejszych clustrów; nowe i stare clustry się nie mieszają) – najlepszy wynik

original_titles = {}
for name, group in df2.groupby('new_cluster'):
    try:
        original_title = Counter([e for e in group['new_original'].to_list() if pd.notnull(e)]).most_common()[0][0]
        original_titles.update({name: original_title})
    except IndexError:
        try:
            original_title = Counter([e for e in group['original title'].to_list() if pd.notnull(e)]).most_common()[0][0]
            original_titles.update({name: original_title})
        except IndexError:
            pass
    
df2['new_original'], df2['new_cluster'] = zip(*df2['new_cluster'].apply(lambda x: (original_titles[x], x) if x in original_titles else (np.nan, np.nan)))
#Counter({True: 37569, False: 9147})

df2 = df2.drop(columns=['original title', 'cluster_titles']).rename(columns={'new_original':'original title', 'new_cluster':'cluster_titles'})

df_with_original_titles = pd.merge(df2, df_with_original_titles.drop(columns=['cluster_viaf', '100_unidecode', '245', 'language', 'work_viaf', 'work_title', 'original title', 'cluster_titles', 'quality_index2']), how='left', on='001')

df_with_original_titles['quality_index2'] = df_with_original_titles.apply(lambda x: quality_index(x), axis=1)  
df_with_original_titles.to_excel('translation_database_with_viaf_work.xlsx', index=False)
correct = df_with_original_titles[df_with_original_titles['quality_index2'] > 0.7] #29153 --> 35014
not_correct = df_with_original_titles[df_with_original_titles['quality_index2'] <= 0.7] #19290 --> 11702

# teraz frekwencje tego, co zostało

test = df2[df2['cluster_titles'].isnull()]
test['simple'] = test['245'].apply(lambda x: simplify_string(marc_parser_dict_for_field(x, '\$')['$a'].strip()).strip())
test_freq = Counter([(aut, tit, lang) for aut, tit, lang in zip(test['cluster_viaf'], test['simple'], test['language'])]).most_common()
test_freq2 = [e for e in test_freq if e[-1] >= 5]
test_freq = sorted(test_freq, key=lambda x: x[0][0])

test_df = test[(test['cluster_viaf'] == test_freq[0][0][0]) &
               (test['simple'] == test_freq[0][0][1])]

with open('records_without_original_title_grouped.txt', 'w') as f:
    for e in test_freq:
        f.write(f'{e}\n')

#pokazać Ondrejowi

df_with_original_titles = pd.read_excel('translation_database_with_viaf_work.xlsx')
df2 = df_with_original_titles.copy()[['001', 'cluster_viaf', '100_unidecode', '245', 'language', 'work_viaf', 'work_title', 'original title', 'cluster_titles', 'quality_index2']] 
#uporządkować relacje, gdy nowy cluster ma kilka starych clustrów:
#2
test = df[df['new_cluster'].notnull()][['new_cluster', 'cluster_titles']].drop_duplicates()
statystyki = Counter(test.groupby('new_cluster').count().reset_index(drop=True)['cluster_titles'])
test2 = test.groupby("new_cluster").filter(lambda x: len(x) == 2 and 0 not in x['cluster_titles'].to_list())

test_df = df[(df['new_cluster'].isin(test2['new_cluster']))]

test_df.to_excel('test.xlsx', index=False)


test2 = pd.Series(df['new_cluster'].values, index=df['cluster_titles']).to_dict()

#zobaczyć stare clustry, które nie mają żadnych nowych clustrów

    
    
'45112529'
test_dict = {k:v for k,v in translation_db_dict2.items() if k[0] == '19683055'}


















    
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
# viaf_works_df = pd.DataFrame(viaf_work_dict.values())
# viaf_works_df['translations'] = viaf_works_df['translations'].apply(lambda x: '|'.join(x.keys()))
# viaf_works_df = cSplit(viaf_works_df, 'work_viaf', 'translations', '|')

# viaf_translations_df = [[el for el in e['translations'].values()] for e in viaf_work_dict.values()]
# viaf_translations_df = pd.DataFrame([e for sub in viaf_translations_df for e in sub])

# viaf_works_df = pd.merge(viaf_works_df, viaf_translations_df, left_on='translations', right_on='translation_viaf', how='left').drop(columns='translations')

# viaf_works_enrichment_df = pd.merge(viaf_work_df, viaf_works_df, on='work_viaf')[['001', 'work_viaf', 'work_title']].drop_duplicates()

# with_work_viaf_id = pd.merge(viaf_works_enrichment_df, translations_df, on='001')
# # with_work_viaf_id.to_excel('test.xlsx', index=False)
# work_with_viaf_id_simple_1 = with_work_viaf_id.copy()[['001', 'work_viaf', 'work_title']]















