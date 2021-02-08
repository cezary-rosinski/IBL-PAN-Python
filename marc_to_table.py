import pandas as pd
import io
from my_functions import f, mrc_to_mrk
import re
import numpy as np
import glob
from dask import delayed, dataframe as dd

def year(row, field):
    if row['field'] == field:
        val = row['content'][7:11]
    else:
        val = np.nan
    return val

# Fennica
    
path = 'F:/Cezary/Documents/IBL/Translations/Fennica/'
files = [f for f in glob.glob(path + '*.mrc', recursive=True)]
for i, file_path in enumerate(files):
    print(str(i) + '/' + str(len(files)))
    path_mrk = file_path.replace('.mrc', '.mrk')
    mrc_to_mrk(file_path, path_mrk)

# BN
#mrc to mrk 
path = 'F:/Cezary/Documents/IBL/Migracja z BN/bn_all/2021-02-08/'
files = [f for f in glob.glob(path + '*.mrc', recursive=True)]
for i, file_path in enumerate(files):
    print(str(i) + '/' + str(len(files)))
    path_mrk = file_path.replace('.mrc', '.mrk')
    mrc_to_mrk(file_path, path_mrk)
#mrk to table    

files = [f for f in glob.glob(path + '*.mrk8', recursive=True)]

encoding = 'utf-8'
marc_df = pd.DataFrame()
for i, file_path in enumerate(files):
    print(str(i) + '/' + str(len(files)))
    marc_list = io.open(file_path, 'rt', encoding = encoding).read().splitlines()
    marc_list = list(filter(None, marc_list))  
    df = pd.DataFrame(marc_list, columns = ['test'])
    df['field'] = df['test'].replace(r'(^.)(...)(.+?$)', r'\2', regex = True)
    df['content'] = df['test'].replace(r'(^.)(.....)(.+?$)', r'\3', regex = True)
    df['help'] = df.apply(lambda x: f(x, 'LDR'), axis=1)
    df['help'] = df['help'].ffill()
    df['year'] = df.apply(lambda x: year(x, '008'), axis=1)
    df['year'] = df.groupby('help')['year'].ffill().bfill()
    df = df[df['year'].isin([str(i) for i in range(2013, 2020)])]
    if len(df) > 0:
        df['id'] = df.apply(lambda x: f(x, '009'), axis = 1)
        df['id'] = df.groupby('help')['id'].ffill().bfill()
        df = df[['id', 'field', 'content']]
        df['content'] = df.groupby(['id', 'field'])['content'].transform(lambda x: '❦'.join(x.drop_duplicates().astype(str)))
        df = df.drop_duplicates().reset_index(drop=True)
        df_wide = df.pivot(index = 'id', columns = 'field', values = 'content')
        marc_df = marc_df.append(df_wide)

fields = marc_df.columns.tolist()
fields = [i for i in fields if 'LDR' in i or re.compile('\d{3}').findall(i)]
marc_df = marc_df.loc[:, marc_df.columns.isin(fields)]

fields.sort(key = lambda x: ([str,int].index(type("a" if re.findall(r'\w+', x)[0].isalpha() else 1)), x))
marc_df = marc_df.reindex(columns=fields)
columns = [f'X{i}' if re.compile('\d{3}').findall(i) else i for i in marc_df.columns.tolist()]
marc_df.columns = columns

marc_df.to_csv('BN_books_2013-2019.csv', index=False)

marc_df['rok'] = marc_df['X008'].apply(lambda x: x[7:11])

years = marc_df['rok'].unique().tolist()
for rok in years:
    part_df = marc_df[marc_df['rok'] == rok]
    part_df.to_csv('bn_ks_' + rok + '.csv', index=False) 
    
# LoC

path = 'F:/Cezary/Documents/IBL/Translations/LoC/'
files = [f for f in glob.glob(path + '*.utf8', recursive=True)]
for i, file_path in enumerate(files):
    print(str(i) + '/' + str(len(files)))
    path_mrk = file_path.replace('.utf8', '.mrk')
    mrc_to_mrk(file_path, path_mrk)
      
path = 'C:/Users/User/Desktop/LoC/'
files = [f for f in glob.glob(path + '*.mrk', recursive=True)]

encoding = 'UTF-8'
for i, file_path in enumerate(files):
    print(str(i) + '/' + str(len(files)))
    marc_list = io.open(file_path, 'rt', encoding = encoding).read().splitlines()
    marc_list = list(filter(None, marc_list))  
    df = pd.DataFrame(marc_list, columns = ['test'])
    df['field'] = df['test'].replace(r'(^.)(...)(.+?$)', r'\2', regex = True)
    df['content'] = df['test'].replace(r'(^.)(.....)(.+?$)', r'\3', regex = True)
    df['help'] = df.apply(lambda x: f(x, 'LDR'), axis=1)
    df['help'] = df['help'].ffill()
    df['id'] = df.apply(lambda x: f(x, '001'), axis = 1)
    df['id'] = df.groupby('help')['id'].ffill().bfill()
    df = df[['id', 'field', 'content']]
    df = dd.from_pandas(df, npartitions=4)
    df2 = df.groupby(['id', 'field'])['content'].apply(lambda x: '❦'.join(x.drop_duplicates().astype(str))).reset_index().compute()
    df_wide = df2.pivot(index = 'id', columns = 'field', values = 'content')
    df_wide.to_csv(f"{path}df_{i}.csv", index = False)

fields = marc_df.columns.tolist()
fields = [i for i in fields if 'LDR' in i or re.compile('\d{3}').findall(i)]
marc_df = marc_df.loc[:, marc_df.columns.isin(fields)]

fields.sort(key = lambda x: ([str,int].index(type("a" if re.findall(r'\w+', x)[0].isalpha() else 1)), x))
marc_df = marc_df.reindex(columns=fields)
columns = [f'X{i}' if re.compile('\d{3}').findall(i) else i for i in marc_df.columns.tolist()]
marc_df.columns = columns

marc_df.to_csv('LoC_database.csv', index=False)

marc_df['rok'] = marc_df['X008'].apply(lambda x: x[7:11])

years = marc_df['rok'].unique().tolist()
for rok in years:
    part_df = marc_df[marc_df['rok'] == rok]
    part_df.to_csv('bn_ks_' + rok + '.csv', index=False)
    
    
    
    
    
    
    
    
    
    
    
    
