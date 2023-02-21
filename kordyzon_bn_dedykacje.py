import glob
from tqdm import tqdm
import sys
from my_functions import mrc_to_mrk
sys.path.insert(1, 'C:/Users/Cezary/Documents/Global-trajectories-of-Czech-Literature')
from marc_functions import read_mrk, mrk_to_df
import json
import pandas as pd
from datetime import datetime
from collections import Counter
import regex as re

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
































