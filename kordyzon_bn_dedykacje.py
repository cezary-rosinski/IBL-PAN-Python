import glob
from tqdm import tqdm
import sys
from my_functions import mrc_to_mrk
sys.path.insert(1, 'C:/Users/Cezary/Documents/Global-trajectories-of-Czech-Literature')
from marc_functions import read_mrk, mrk_to_df
import json
import pandas as pd

#%% main
#mrc to mrk 
# path = r"F:\Cezary\Documents\IBL\BN\bn_all\2023-01-19/"
# files = [f for f in glob.glob(path + '*.mrc', recursive=True)]
# for file_path in tqdm(files):
#     path_mrk = file_path.replace('.mrc', '.mrk')
#     mrc_to_mrk(file_path, path_mrk)
    
#zrobić wykres z podziałem na stulecia z liczbą rekordów z dedykacją + relacja dedykacja vs. niededykacja

# Wojtek mówi, że dedykacja w polach 700 i 600, w 500 są luźne uwagi, nieustrukturyzowane
# Starsze w 700, w 600 nowe deskryptory

path = r"F:\Cezary\Documents\IBL\BN\bn_all\2023-01-19/"
files = [f for f in glob.glob(path + '*.mrk', recursive=True)]

dedyk_records = {'500': [], #8568
                 '655': [], #10470
                 '700': []} #1460
for file in tqdm(files):
    file = read_mrk(file)
    for dictionary in file:
        for k,v in dictionary.items():
            if k in ['500', '655', '700']:
                for el in v:
                    if 'dedyk' in el.lower():
                        dedyk_records[k].append(dictionary)
                    
ids_set = []
for k,v in dedyk_records.items():
    for dictionary in v:
        ids_set.append(dictionary.get('001')[0])
ids_set = set(ids_set)

all_dedyk_records = dedyk_records.get('500') + dedyk_records.get('655') + dedyk_records.get('700')
all_dedyk_records = [{ka:list(va) for ka,va in dict(el).items()} for el in set([tuple({k:tuple(v) for k,v in e.items()}.items()) for e in all_dedyk_records])]

#1500-1700:
records_1500_1700 = []
errors = []
for record in tqdm(all_dedyk_records):
    try:
        if record.get('008')[0][7:11] == '16uu' or 1500 <= int(record.get('008')[0][7:11]) <= 1700:
            records_1500_1700.append(record)
    except ValueError:
        errors.append(record)
df_1500_1700 = mrk_to_df(records_1500_1700)
#save
df_1500_1700.to_excel('dedykacje_1500-1700.xlsx', index=False)


#na końcu policzyć unikatowe identyfikatory, żeby zobaczyć, ile rekordów – 14331
#uwaga na 500 z "brak dedykacji"









