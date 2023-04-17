import glob
from tqdm import tqdm
import sys
from my_functions import mrc_to_mrk, gsheet_to_df, marc_parser_dict_for_field
sys.path.insert(1, 'C:/Users/Cezary/Documents/Global-trajectories-of-Czech-Literature')
from marc_functions import read_mrk, mrk_to_df
import json
import pandas as pd
from datetime import datetime
from collections import Counter
import regex as re
from ast import literal_eval
import requests
import numpy as np
import Levenshtein as lev

#%% def

def read_mrk_nukat(path):
    records = []
    with open(path, 'r', encoding='utf-8') as mrk:
        record_dict = {}
        for line in mrk.readlines():
            line = line.replace('\n', '')
            if line.startswith('LDR'):
                if record_dict:
                    records.append(record_dict)
                    record_dict = {}
                record_dict[line[:3]] = [line[4:]]
            elif re.findall('^\d{3}', line):
                key = line[:3]
                if key in record_dict:
                    record_dict[key] += [line[4:]]
                else:
                    record_dict[key] = [line[4:]]
            elif line and line.startswith(' '):
                keys = [e for e in record_dict if e != 'LDR']
                max_key = max(keys, key = lambda x: int(x))
                record_dict[max_key][-1] += line.strip()
        records.append(record_dict)
    return records

#%% main
#mrc to mrk 
# path = r"F:\Cezary\Documents\IBL\BN\bn_all\2023-01-23/"
# files = [f for f in glob.glob(path + '*.mrc', recursive=True)]
# for file_path in tqdm(files):
#     path_mrk = file_path.replace('.mrc', '.mrk')
#     mrc_to_mrk(file_path, path_mrk)
    
#zrobić wykres z podziałem na stulecia z liczbą rekordów z dedykacją + relacja dedykacja vs. niededykacja

# Wojtek mówi, że dedykacja w polach 700 i 600, w 500 są luźne uwagi, nieustrukturyzowane
# Starsze w 700, w 600 nowe deskryptory

path = r"F:\Cezary\Documents\IBL\BN\bn_all\2023-01-23/"
files = [f for f in glob.glob(path + '*.mrk', recursive=True)]

# dedyk_records = {'500': [], #8568 #całość 14665
#                  '655': [], #10470 #całość 10939
#                  '700': []} #1460 #całość 4889
# for file in tqdm(files):
#     file = read_mrk(file)
#     for dictionary in file:
#         for k,v in dictionary.items():
#             if k in ['500', '655', '700']:
#                 for el in v:
#                     if 'dedyk' in el.lower():
#                         dedyk_records[k].append(dictionary)

dedyk_records = {'655': [], #10928
                 '700': []} #11662     
for file in tqdm(files):
    file = read_mrk(file)
    for dictionary in file:
        for k,v in dictionary.items():
            if k == '700':
                for el in v:
                    if 'adresat dedykacji' in el.lower() or 'adr. ded.' in el.lower():
                        dedyk_records[k].append(dictionary)
            elif k == '655':
                for el in v:
                    if 'Dedykacje' in el:
                        dedyk_records[k].append(dictionary)
                    
ids_set = []
for k,v in dedyk_records.items():
    for dictionary in v:
        ids_set.append(dictionary.get('001')[0])
ids_set = set(ids_set)

# all_dedyk_records = dedyk_records.get('500') + dedyk_records.get('655') + dedyk_records.get('700')
all_dedyk_records = dedyk_records.get('655') + dedyk_records.get('700')
all_dedyk_records = [{ka:list(va) for ka,va in dict(el).items()} for el in set([tuple({k:tuple(v) for k,v in e.items()}.items()) for e in all_dedyk_records])] #całość unique: 20662

#1500-1700:
records_1500_1700 = []
errors = []
for record in tqdm(all_dedyk_records):
    try:
        if record.get('008')[0][7:11] in ['16uu', '160\\'] or 1500 <= int(record.get('008')[0][7:11]) <= 1700:
            records_1500_1700.append(record)
    except ValueError:
        errors.append(record)

# set([e.get('008')[0][7:11] for e in errors])

df_1500_1700 = mrk_to_df(records_1500_1700) #4518 #całość 7287
df_1500_1700['country'] = df_1500_1700['008'].apply(lambda x: x[0][15:17])
countries_frequency = dict(Counter(df_1500_1700['country'].to_list()))

#save
df_1500_1700.to_excel(f'dedykacje_1500-1700_{datetime.now().date()}.xlsx', index=False)


#na końcu policzyć unikatowe identyfikatory, żeby zobaczyć, ile rekordów – 14331
#uwaga na 500 z "brak dedykacji"


#%% NUKAT

nukat_paths = [r"F:\Cezary\Documents\IBL\NUKAT\wyb_wydzpol.print01", r"F:\Cezary\Documents\IBL\NUKAT\wyb_wydzpol.print02"]

dedyk_nukat = {'100': [], #dedyk + adr. ded.: 1678
               '500': [], #dedyk: 7805 | dedyk + ded.: 17115 | dedyk + adr. ded.: 8080
               '655': [], #dedyk: 0 | dedyk + ded.: 0 | dedyk + adr. ded.: 0
               '700': []} #dedyk: 6284 | dedyk + ded.: 17359  | dedyk + adr. ded.: 16805
for file in tqdm(nukat_paths):
    file = read_mrk_nukat(file)
    for dictionary in file:
        for k,v in dictionary.items():
            if k in ['100', '500', '655', '700']:
                for el in v:
                    if 'adresat dedykacji' in el.lower() or 'adr. ded.' in el.lower():
                        dedyk_nukat[k].append(dictionary)

ids_set = []
for k,v in dedyk_nukat.items():
    for dictionary in v:
        ids_set.append(dictionary.get('001')[0])
ids_set = set(ids_set)

# all_dedyk_records = dedyk_nukat.get('500') + dedyk_nukat.get('655') + dedyk_nukat.get('700')
all_dedyk_records = dedyk_nukat.get('700')

all_dedyk_records = [{ka:list(va) for ka,va in dict(el).items()} for el in set([tuple({k:tuple(v) for k,v in e.items()}.items()) for e in all_dedyk_records])] #nukat: całość unique: 7396

#1500-1700:
records_1500_1700_nukat = []
errors = []
for record in tqdm(all_dedyk_records):
    try:
        if record.get('008')[0][7:11] in ['16uu', '160\\'] or 1500 <= int(record.get('008')[0][7:11]) <= 1700:
            records_1500_1700_nukat.append(record)
    except ValueError:
        errors.append(record)

# set([e.get('008')[0][7:11] for e in errors])

df_1500_1700_nukat = mrk_to_df(records_1500_1700_nukat) #5355
df_1500_1700_nukat['country'] = df_1500_1700_nukat['008'].apply(lambda x: x[0][15:17])
countries_frequency = dict(Counter(df_1500_1700_nukat['country'].to_list()))

#save
df_1500_1700_nukat.to_excel(f'dedykacje_1500-1700_nukat_{datetime.now().date()}.xlsx', index=False)


#%% geotagging

dedicated_nukat = gsheet_to_df('1ZKv2sLoTnzGqWuD4jwgcuRSD8oDM3qdnli8Wd3c1quI', 'Sheet1')
dedicated_bn = gsheet_to_df('1K6J4E8z5fdeobT1f25yoC8B2pJPGfr6K8ZREA8rnvPE', 'Sheet1')

places_nukat_df = gsheet_to_df('1ZKv2sLoTnzGqWuD4jwgcuRSD8oDM3qdnli8Wd3c1quI', 'miejsca')
places_bn_df = gsheet_to_df('1K6J4E8z5fdeobT1f25yoC8B2pJPGfr6K8ZREA8rnvPE', 'miejsca')
places_nukat_df.iloc[:,2] = places_nukat_df.iloc[:,2].apply(lambda x: literal_eval(x) if isinstance(x, str) else x)
places_bn_df.iloc[:,1] = places_bn_df.iloc[:,1].apply(lambda x: literal_eval(x) if isinstance(x, str) else x)

places_nukat = [e for sub in places_nukat_df.iloc[:,2].dropna().to_list() for e in sub]
places_bn = [e for sub in places_bn_df.iloc[:,1].dropna().to_list() for e in sub]

places = []
y = [places_nukat, places_bn]
for el in y:
    for e in el:
        x = marc_parser_dict_for_field(e, '\\$')
        x = [{k:v.replace('\\\\', '').strip() for k,v in e.items()} for e in x]
        x = [list(e.values())[0] for e in x]
        if x not in places:
            places.append(x)
        # else: print(x)

for ind, (country, place) in tqdm(enumerate(places), total=len(places)):
    # country, place = miejsca[0]
    url = f'https://www.wikidata.org/w/api.php?action=query&format=json&list=search&srsearch=inlabel:{place}&srlimit=5&formatversion=2'
    r = requests.get(url).json()
    
    hits = [e.get('title') for e in r.get('query').get('search')]
    ids = [requests.get(f'https://www.wikidata.org/wiki/Special:EntityData/{e}.json').json() for e in hits]
    ids = [e for e in ids if 'P625' in e.get('entities').get(list(e.get('entities').keys())[0]).get('claims')]
    ids = [list(e.get('entities').keys())[0] for e in ids]
    places[ind].append(ids)
    
places = [[c, p, [f'https://www.wikidata.org/wiki/{e}' for e in wiki_list]] for c, p, wiki_list in places]
places_df = pd.DataFrame(places, columns = ['country', 'place', 'wikidata IDs'])
places_df.to_excel('dedykacje_miejsca.xlsx', index=False)


#%% de-duplication

dedicated_nukat = gsheet_to_df('1ZKv2sLoTnzGqWuD4jwgcuRSD8oDM3qdnli8Wd3c1quI', 'Sheet1')
dedicated_bn = gsheet_to_df('1K6J4E8z5fdeobT1f25yoC8B2pJPGfr6K8ZREA8rnvPE', 'Sheet1')

places_df = gsheet_to_df('1ukvhbr8xD428C-2S6xXsOdDeqmHABbfpe5Qh8O08I5k', 'Sheet1')
places = places_df['ID'].dropna().to_list()

places_dict = {}
for place in places:
    # place = places[0]
    # place = 'https://www.wikidata.org/wiki/Q216'
    match = places_df[places_df['ID'] == place][['country', 'place']].values.tolist()
    for c, p in match:
        places_dict[p] = place
    
    # if not place in places_dict:
    #     places_dict[place] = places_df[places_df['ID'] == place][['country', 'place']].values.tolist()
    # else:
    #     places_dict[place].extend(places_df[places_df['ID'] == place][['country', 'place']].values.tolist())

#przy matchowaniu miejsc iterować po wszystkich polach w 752, bo teraz biorę tylko pierwsze
        
dedicated_bn_light = dedicated_bn[['001', '008', '245', '752', '600', '655', '700']]
dedicated_bn_light = dedicated_bn_light.loc[dedicated_bn_light['752'].notnull()]

dedicated_bn_light['001'] = dedicated_bn_light['001'].apply(lambda x: literal_eval(x)[0]) 
dedicated_bn_light['year'] = dedicated_bn_light['008'].apply(lambda x: literal_eval(x)[0][7:11])
dedicated_bn_light['title'] = dedicated_bn_light['245'].apply(lambda x: [e.get('$a') for e in marc_parser_dict_for_field(literal_eval(x)[0], '\\$') if '$a' in e][0])

# test = dedicated_bn_light.loc[(dedicated_bn_light['655'].str.contains('Dedykacje')) & (dedicated_bn_light['600'].isnull()) & (dedicated_bn_light['700'].str.contains('Adresat dedykacji') == False) & (dedicated_bn_light['700'].str.contains('Adr. ded') == False)]
# test_ids = test['001'].to_list()
# test_df = dedicated_bn.copy()
# test_df['001'] = test_df['001'].apply(lambda x: literal_eval(x)[0])
# test_df = test_df.loc[test_df['001'].isin(test_ids)]
# test_df.to_excel('błędy_bn.xlsx', index=False)

#czy wyrzucić błędne rekordy BN?

def get_dedicated_bn(x):
    if pd.notnull(x['655']) and pd.notnull(x['600']) and 'Dedykacje' in x['655']:
        return [marc_parser_dict_for_field(e, '\\$') for e in literal_eval(x['600'])]
    elif pd.notnull(x['700']) and any(el in x['700'] for el in ['Adr. ded.', 'Adresat dedykacji']):
        return [el for el in [marc_parser_dict_for_field(e, '\\$') for e in literal_eval(x['700'])] if any(list(ele.values())[0] in ['Adr. ded.', 'Adresat dedykacji'] for ele in el)]
    else: return None
        
dedicated_bn_light['dedicated'] = dedicated_bn_light.apply(lambda x: get_dedicated_bn(x), axis=1)

def match_place_bn(x):
    try:
        result = [elem for elem in [places_dict.get(ele) for ele in [[el.get('$d') for el in marc_parser_dict_for_field(e, '\\$') if '$d' in el][0] for e in literal_eval(x)]] if elem]
        if result:
            return result
        else: return None
    except:
        return None

dedicated_bn_light['place'] = dedicated_bn_light['752'].apply(lambda x: match_place_bn(x))

dedicated_bn_light = dedicated_bn_light.loc[dedicated_bn_light['place'].notnull()][['001', 'title', 'place', 'year', 'dedicated']] #2492 wcześniej: 6374

dedicated_nukat_light = dedicated_nukat[['001', '008', '245', '752', '700']]
dedicated_nukat_light = dedicated_nukat_light.loc[dedicated_nukat_light['752'].notnull()]

dedicated_nukat_light['001'] = dedicated_nukat_light['001'].apply(lambda x: literal_eval(x)[0])
dedicated_nukat_light['year'] = dedicated_nukat_light['008'].apply(lambda x: literal_eval(x)[0][7:11])
dedicated_nukat_light['title'] = dedicated_nukat_light['245'].apply(lambda x: [e.get('\\a').strip() for e in marc_parser_dict_for_field(literal_eval(x)[0], '\\\\') if '\\a' in e][0])
dedicated_nukat_light['dedicated'] = dedicated_nukat_light['700'].apply(lambda x: [ele for ele in [[{k:v.strip() for k,v in el.items()} for el in marc_parser_dict_for_field(e, '\\\\')] for e in literal_eval(x)] if any(list(elem.values())[0] in ['Adr. ded.', 'Adresat dedykacji'] for elem in ele)])

def match_place_nukat(x):
    try:
        result = [elem for elem in [places_dict.get(ele) for ele in [[el.get('\\d').strip() for el in marc_parser_dict_for_field(e, '\\\\') if '\\d' in el][0] for e in literal_eval(x)]] if elem]
        if result:
            return result
        else: return None
    except:
        return None

dedicated_nukat_light['place'] = dedicated_nukat_light['752'].apply(lambda x: match_place_nukat(x))

dedicated_nukat_light = dedicated_nukat_light.loc[dedicated_nukat_light['place'].notnull()][['001', 'title', 'place', 'year', 'dedicated']] #5546

unique_nukat = dedicated_nukat_light.copy()[['title', 'place', 'year']].drop_duplicates().values.tolist()

df_nukat = dedicated_nukat.copy()
df_nukat['001'] = df_nukat['001'].apply(lambda x: literal_eval(x)[0])
df_bn = dedicated_bn.copy()
df_bn['001'] = df_bn['001'].apply(lambda x: literal_eval(x)[0])

final_df = pd.DataFrame()
empty_row = pd.DataFrame([[]])

for n_t, n_p, n_y in tqdm(unique_nukat): #czas trwania: 18 minut
    # n_t, n_p, n_y = unique_nukat[3]
    n_t, n_p, n_y = ('Processus Juris breuior Joa[n]nis Andr[eae] /',  'https://www.wikidata.org/wiki/Q31487', '1531')
    test_nukat = dedicated_nukat_light.loc[(dedicated_nukat_light['title'] == n_t) &
                                        (dedicated_nukat_light['place'] == n_p) &
                                        (dedicated_nukat_light['year'] == n_y)]['001'].to_list()
    
    test_bn = dedicated_bn_light.loc[(dedicated_bn_light['place'] == n_p) &
                                     (dedicated_bn_light['year'] == n_y)]
    test_bn = test_bn[['001', 'title']].values.tolist()
    test_bn = [i for i,v in test_bn if lev.ratio(n_t, v) > 0.8]    
    
    df_nukat_sample = df_nukat.loc[df_nukat['001'].isin(test_nukat)]
    
    df_bn_sample = df_bn.loc[df_bn['001'].isin(test_bn)]
    
    df = pd.concat([df_nukat_sample, df_bn_sample])
    if df.shape[0] > 1:
        final_df = pd.concat([final_df, df])
        final_df = pd.concat([final_df, empty_row])


final_df.to_excel('nukat_bn_duplikaty.xlsx', index=False)
# pokazać przykład, że Levenshtein może nie wystarczyć: ('Processus Juris breuior Joa[n]nis Andr[eae] /',  'https://www.wikidata.org/wiki/Q31487', '1531')







a, b = 	'https://www.wikidata.org/wiki/Q31487', 1675


for row in dedicated_bn_light.iterrows():
    a = row['place']
    b = row['year']
    

nukat = dedicated_nukat_light.loc[(dedicated_nukat_light['place'] == a) &
                                  (dedicated_nukat_light['year'] == b)]





# ujednoznacznione miejsce wydania, rok + tytuł (string similarity
['080206s1644 pl a  |000 0 lat c'][0][7:11]
[e.get('$a') for e in marc_parser_dict_for_field(['10$aRadosny grob jasnie wielmożnego jego mośći pana Alexandra Mosalskiego, woiewody minskiego starosty kowienskiego, ciwona retowskiego, dzierżavvce iasvvońskiego etc. na kazaniu pogrzebowym w Kownie w kośćiele Troyce świętey panien zakonnych Franciszka świętego reguły pokutuiących wystawiony /$cprzez x. Avgvstyna Witvnskiego, zakonu Franćiszka świętego bernardynow nazwanego, generalnego lektora y kaznodźieię [...].'][0], '\\$') if '$a' in e]

[e.get('$d') for e in marc_parser_dict_for_field(['\\\\$aPolska$dKraków.'][0], '\\$') if '$d' in e]

[e.get('\\a').strip() for e in marc_parser_dict_for_field(['10 \\a Serenissimi Iohannis Casimiri Poloniarvm Sveciæqve Principis CarcerGallicvs / \\c ab Euerhardo Wassenbergio conscriptus.'][0], '\\\\') if '\\a' in e]


[e.get('\\d').strip() for e in marc_parser_dict_for_field([' \\a Polska \\d Gdańsk.'][0], '\\\\') if '\\d' in e]















