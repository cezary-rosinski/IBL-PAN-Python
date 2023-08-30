import pandas as pd
import io
from my_functions import f, mrc_to_mrk
import re
import numpy as np
import glob
from dask import delayed, dataframe as dd
from tqdm import tqdm
from collections import Counter

def year(row, field):
    if row['field'] == field:
        val = row['content'][7:11]
    else:
        val = np.nan
    return val

#%% new approach
    
#mrc to mrk 
path = 'D:/IBL/BN/bn_all/2023-07-20/'
files = [f for f in glob.glob(path + '*.mrc', recursive=True)]
for file_path in tqdm(files):
    path_mrk = file_path.replace('.mrc', '.mrk')
    mrc_to_mrk(file_path, path_mrk)

path = 'F:/Cezary/Documents/IBL/Migracja z BN/bn_all/2021-02-08/'
path = 'F:/Cezary/Documents/IBL/Translations/Czech database/nkc_SKC_2021-08-05'
path = 'C:/Users/User/Documents/bn_all/2021-07-26/'
files = [file for file in glob.glob(path + '*.mrk', recursive=True)]

conditions = ['$aPogonowska, Anna$d(1922-2005)', '$aOleska, Lucyna', '$aPogonowska, Anna$d1922-2005', '$aPogonowska, Anna$d1922-']

encoding = 'utf-8'
new_list = []
for file_path in tqdm(files):
    marc_list = io.open(file_path, 'rt', encoding = encoding).read().splitlines()

    mrk_list = []
    for row in marc_list:
        if row.startswith('=LDR'):
            mrk_list.append([row])
        else:
            if row:
                mrk_list[-1].append(row)
                
    for sublist in mrk_list:
        for el in sublist:
            if el.startswith('=490'):
                if '$aBiblioteka Młodych' in el:
                    new_list.append(sublist)
    # for sublist in mrk_list:
    #     for el in sublist:
    #         if el.startswith(('=100', '=600', '=700')):
    #             if any(e in el for e in conditions):
    #                 new_list.append(sublist)
    # for sublist in mrk_list:
    #     try:
    #         year = int(''.join([ele for ele in sublist if ele.startswith('=008')])[13:17])
    #         if year in range(2004,2021):
    #             for el in sublist:
    #                 if el.startswith('=773'):
    #                     val = re.search('(\$t)(.+?)(\$|$)', el).group(2)
    #                     if val in bn_magazines:
    #                         new_list.append(sublist)
    #     except (ValueError, AttributeError):
    #         pass

final_list = []
for lista in new_list:
    slownik = {}
    for el in lista:
        if el[1:4] in slownik:
            slownik[el[1:4]] += f"❦{el[6:]}"
        else:
            slownik[el[1:4]] = el[6:]
    final_list.append(slownik)

marc_df = pd.DataFrame(final_list)
fields = marc_df.columns.tolist()
fields = [i for i in fields if 'LDR' in i or re.compile('\d{3}').findall(i)]
marc_df = marc_df.loc[:, marc_df.columns.isin(fields)]
fields.sort(key = lambda x: ([str,int].index(type("a" if re.findall(r'\w+', x)[0].isalpha() else 1)), x))
marc_df = marc_df.reindex(columns=fields)  

marc_df.to_excel('Fala.xlsx', index=False)

#%% liczenie rekordów dla lat
path = 'F:/Cezary/Documents/IBL/BN/bn_books/'
files = [f for f in glob.glob(path + '*.mrk', recursive=True)]
encoding = 'utf-8'
years_list = []
for file_path in tqdm(files):
    marc_list = io.open(file_path, 'rt', encoding = encoding).read().splitlines()
    
    for row in marc_list:
        if row.startswith('=008'):
            element = row[13:17]
            if element.isnumeric():
                years_list.append(int(element))
            
years_dict = dict(Counter(years_list))

#zbieranie informacji z konkretnego podpola dla całego zbioru

from tqdm import tqdm
import io
import sys
sys.path.insert(1, 'C:/Users/Cezary/Documents/IBL-PAN-Python')
from my_functions import marc_parser_to_list
import pandas as pd


paths = [r"C:\Users\Cezary\Documents\LiBRI\Bibliographic data processing\data\libri_marc_bn_books_2023-08-29.mrk", r"C:\Users\Cezary\Documents\LiBRI\Bibliographic data processing\data\libri_marc_bn_articles_2023-08-29.mrk", r"C:\Users\Cezary\Documents\LiBRI\Bibliographic data processing\data\libri_marc_bn_chapters_2023-08-07.mrk"]

co_creation_types = set()
for path in tqdm(paths):
    # path = paths[-1]
    marc_list = io.open(path, 'rt', encoding = 'utf-8').read().splitlines()
    for field in marc_list:
        if field.startswith('=700'):
            test = marc_parser_to_list(field, '\\$')
            [co_creation_types.add(e.get('$e')) for e in test if '$e' in e]
            
old_mapping = pd.read_excel(r"C:\Users\Cezary\Documents\PBL-converter\additional_files\co-creators_mapping.xlsx")
old_mapping_list = old_mapping['to_map'].to_list()

add_to_mapping = [e for e in co_creation_types if e not in old_mapping_list and e]  

#%% old approach

# Fennica
    
# path = 'F:/Cezary/Documents/IBL/Translations/Fennica/'
# files = [f for f in glob.glob(path + '*.mrc', recursive=True)]
# for i, file_path in enumerate(files):
#     print(str(i) + '/' + str(len(files)))
#     path_mrk = file_path.replace('.mrc', '.mrk')
#     mrc_to_mrk(file_path, path_mrk)

# # BN
# #mrc to mrk 
# path = 'F:/Cezary/Documents/IBL/BN/bn_books/'
# files = [f for f in glob.glob(path + '*.mrc', recursive=True)]
# for file_path in tqdm(files):
#     path_mrk = file_path.replace('.mrc', '.mrk')
#     mrc_to_mrk(file_path, path_mrk)
# #mrk to table    

# files = [f for f in glob.glob(path + '*.mrk', recursive=True)]

# encoding = 'utf-8'
# marc_df = pd.DataFrame()
# for i, file_path in enumerate(files):
#     print(str(i) + '/' + str(len(files)))
#     marc_list = io.open(file_path, 'rt', encoding = encoding).read().splitlines()
#     marc_list = list(filter(None, marc_list))  
#     df = pd.DataFrame(marc_list, columns = ['test'])
#     df['field'] = df['test'].replace(r'(^.)(...)(.+?$)', r'\2', regex = True)
#     df['content'] = df['test'].replace(r'(^.)(.....)(.+?$)', r'\3', regex = True)
#     df['help'] = df.apply(lambda x: f(x, 'LDR'), axis=1)
#     df['help'] = df['help'].ffill()
#     df['year'] = df.apply(lambda x: year(x, '008'), axis=1)
#     df['year'] = df.groupby('help')['year'].ffill().bfill()
#     df = df[df['year'].isin([str(i) for i in range(2013, 2020)])]
#     if len(df) > 0:
#         df['id'] = df.apply(lambda x: f(x, '009'), axis = 1)
#         df['id'] = df.groupby('help')['id'].ffill().bfill()
#         df = df[['id', 'field', 'content']]
#         df['content'] = df.groupby(['id', 'field'])['content'].transform(lambda x: '❦'.join(x.drop_duplicates().astype(str)))
#         df = df.drop_duplicates().reset_index(drop=True)
#         df_wide = df.pivot(index = 'id', columns = 'field', values = 'content')
#         marc_df = marc_df.append(df_wide)

# fields = marc_df.columns.tolist()
# fields = [i for i in fields if 'LDR' in i or re.compile('\d{3}').findall(i)]
# marc_df = marc_df.loc[:, marc_df.columns.isin(fields)]

# fields.sort(key = lambda x: ([str,int].index(type("a" if re.findall(r'\w+', x)[0].isalpha() else 1)), x))
# marc_df = marc_df.reindex(columns=fields)
# columns = [f'X{i}' if re.compile('\d{3}').findall(i) else i for i in marc_df.columns.tolist()]
# marc_df.columns = columns

# marc_df.to_csv('BN_books_2013-2019.csv', index=False)

# marc_df['rok'] = marc_df['X008'].apply(lambda x: x[7:11])

# years = marc_df['rok'].unique().tolist()
# for rok in years:
#     part_df = marc_df[marc_df['rok'] == rok]
#     part_df.to_csv('bn_ks_' + rok + '.csv', index=False) 
    
# # LoC

# path = 'F:/Cezary/Documents/IBL/Translations/LoC/'
# files = [f for f in glob.glob(path + '*.utf8', recursive=True)]
# for i, file_path in enumerate(files):
#     print(str(i) + '/' + str(len(files)))
#     path_mrk = file_path.replace('.utf8', '.mrk')
#     mrc_to_mrk(file_path, path_mrk)
      
# path = 'C:/Users/User/Desktop/LoC/'
# files = [f for f in glob.glob(path + '*.mrk', recursive=True)]

# encoding = 'UTF-8'
# for i, file_path in enumerate(files):
#     print(str(i) + '/' + str(len(files)))
#     marc_list = io.open(file_path, 'rt', encoding = encoding).read().splitlines()
#     marc_list = list(filter(None, marc_list))  
#     df = pd.DataFrame(marc_list, columns = ['test'])
#     df['field'] = df['test'].replace(r'(^.)(...)(.+?$)', r'\2', regex = True)
#     df['content'] = df['test'].replace(r'(^.)(.....)(.+?$)', r'\3', regex = True)
#     df['help'] = df.apply(lambda x: f(x, 'LDR'), axis=1)
#     df['help'] = df['help'].ffill()
#     df['id'] = df.apply(lambda x: f(x, '001'), axis = 1)
#     df['id'] = df.groupby('help')['id'].ffill().bfill()
#     df = df[['id', 'field', 'content']]
#     df = dd.from_pandas(df, npartitions=4)
#     df2 = df.groupby(['id', 'field'])['content'].apply(lambda x: '❦'.join(x.drop_duplicates().astype(str))).reset_index().compute()
#     df_wide = df2.pivot(index = 'id', columns = 'field', values = 'content')
#     df_wide.to_csv(f"{path}df_{i}.csv", index = False)

# fields = marc_df.columns.tolist()
# fields = [i for i in fields if 'LDR' in i or re.compile('\d{3}').findall(i)]
# marc_df = marc_df.loc[:, marc_df.columns.isin(fields)]

# fields.sort(key = lambda x: ([str,int].index(type("a" if re.findall(r'\w+', x)[0].isalpha() else 1)), x))
# marc_df = marc_df.reindex(columns=fields)
# columns = [f'X{i}' if re.compile('\d{3}').findall(i) else i for i in marc_df.columns.tolist()]
# marc_df.columns = columns

# marc_df.to_csv('LoC_database.csv', index=False)

# marc_df['rok'] = marc_df['X008'].apply(lambda x: x[7:11])

# years = marc_df['rok'].unique().tolist()
# for rok in years:
#     part_df = marc_df[marc_df['rok'] == rok]
#     part_df.to_csv('bn_ks_' + rok + '.csv', index=False)
    
    
  
    
    
    
    
    
    
    
    
    
