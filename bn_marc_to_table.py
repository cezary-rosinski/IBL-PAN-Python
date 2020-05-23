import pandas as pd
import io
from my_functions import f
import re
import numpy as np
import glob

def year(row, field):
    if row['field'] == field:
        val = row['content'][7:11]
    else:
        val = np.nan
    return val
    
path = 'C:/Users/User/Documents/bn_all/'
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
        marc_df = marc_df.append(df)

marc_df['content'] = marc_df.groupby(['id', 'field'])['content'].transform(lambda x: '❦'.join(x.drop_duplicates().astype(str)))
marc_df = marc_df.drop_duplicates().reset_index(drop=True)
marc_df_wide = marc_df.pivot(index = 'id', columns = 'field', values = 'content')
marc_df_wide.to_excel('BN_books_2013-2019.xlsx', index=False)

