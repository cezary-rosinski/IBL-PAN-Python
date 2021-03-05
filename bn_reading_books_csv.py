import csv
import numpy as np
import pandas as pd
from my_functions import marc_parser_1_field, cluster_records, simplify_string, cluster_strings
import unidecode
import json

# Loading csv
list_of_records = []
with open("F:/Cezary/Documents/IBL/tabele z pulpitu/bn_data_ks_table.csv", 'r', errors="surrogateescape") as csv_file:
    reader = csv.reader(csv_file, delimiter=";")
    headers = next(reader, None)
    position_008 = headers.index('008')
    for row in reader:
        if row[position_008][7:11] in ['2004', '2005', '2006', '2007', '2008', '2009', '2010']:
            list_of_records.append(row)

df = pd.DataFrame(list_of_records, columns=headers)
df['rok'] = df['008'].apply(lambda x: x[7:11])

lata = ['2004', '2005', '2006', '2007', '2008', '2009', '2010']
for rok in lata:
    train = df.copy()[df['rok'] == rok].drop(columns='rok')
    for col in train.columns:
        if train[col].dtype==object:
            train[col]=train[col].apply(lambda x: np.nan if x==np.nan else str(x).encode('utf-8', 'replace').decode('utf-8'))
    train.to_csv(f"bn_{rok}.csv", encoding='utf-8', index=False)
    
    
df = pd.read_csv('F:/Cezary/Documents/IBL/CLARIN/Duplikaty/BN/bn_2004.csv', encoding='utf-8')

title = marc_parser_1_field(df, '001', '245', '\$')[['001', '$a', '$b']].replace(r'^\s*$', np.nan, regex=True)
title['title'] = title[title.columns[1:]].apply(lambda x: simplify_string(x), axis=1)    
title = title[['001', 'title']]
df = pd.merge(df, title, how='left', on='001')

place = marc_parser_1_field(df, '001', '260', '\$')[['001', '$a']].rename(columns={'$a':'place'})
place = place[place['place'] != '']
place['place'] = place['place'].apply(lambda x: simplify_string(x))
df = pd.merge(df, place, how='left', on='001')

publisher = marc_parser_1_field(df, '001', '260', '\$')[['001', '$b']].rename(columns={'$b':'publisher'})
publisher = publisher.groupby('001').head(1).reset_index(drop=True)
publisher['publisher'] = publisher['publisher'].apply(lambda x: simplify_string(x, with_spaces=False))
df = pd.merge(df, publisher, how='left', on='001')

year = df.copy()[['001', '008']].rename(columns={'008':'year'})
year['year'] = year['year'].apply(lambda x: x[7:11])
df = pd.merge(df, year, how='left', on='001')
    

df2 = cluster_records(df, '001', ['title', 'place'])


cluster = cluster_strings(df['title'], 0.7)
df_cluster = df.copy()[(df['title'].str.strip().isin(cluster['kod leonarda da vinci']))]

cluster = dict(sorted(cluster.items(), key = lambda item : len(item[1]), reverse=True))

with open("cluster_test.json", 'w', encoding='utf-8') as f: 
    json.dump(cluster, f, ensure_ascii=False, indent=4)


duplikaty = ["b0000000162122", "b0000001621221", "b0000001622501", "b0000001622481", "b0000001729067", "b0000001623549", "b0000001737955", "b0000001737957", "b0000001656547", "b0000001675252", "b0000001692737", "b0000001713994", "b0000001621191", "b0000001622759", "b0000001654359", "b0000001844424", "b0000001621486", "b0000001621489", "b0000001625318"]

duplikaty_df = df[df['001'].isin(duplikaty)]

duplikaty_df.to_excel('propozycje_duplikatow.xlsx', index=False)




























    
    
  