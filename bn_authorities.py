import pandas as pd
import io
from my_functions import f
import re
import numpy as np
import glob
from my_functions import mrc_to_mrk
  
path = 'C:/Users/User/Documents/bn_authorities/'
# =============================================================================
# files = [f for f in glob.glob(path + '*.mrc', recursive=True)]
# for i, file in enumerate(files):
#     print(str(i) + '/' + str(len(files)))
#     path_out = re.sub('(.+)(\.mrc$)', r'\1.mrk', file)
#     mrc_to_mrk(file, path_out)
# =============================================================================
    
files = [f for f in glob.glob(path + '*.mrk', recursive=True)]

encoding = 'utf-8'
ile_wzorcowych = 0
ile_osobowych = 0
ile_osobowych_z_viaf = 0
for i, file_path in enumerate(files):
    print(str(i) + '/' + str(len(files)))
    marc_list = io.open(file_path, 'rt', encoding = encoding).read().splitlines()
    marc_list = list(filter(None, marc_list))  
    df = pd.DataFrame(marc_list, columns = ['test'])
    df['field'] = df['test'].replace(r'(^.)(...)(.+?$)', r'\2', regex = True)
    df['content'] = df['test'].replace(r'(^.)(.....)(.+?$)', r'\3', regex = True)
    df['help'] = df.apply(lambda x: f(x, 'LDR'), axis=1)
    df['help'] = df['help'].ffill()
    if len(df) > 0:
        df['id'] = df.apply(lambda x: f(x, '001'), axis = 1)
        df['id'] = df.groupby('help')['id'].ffill().bfill()
        df = df[['id', 'field', 'content']]
        df = df[df['field'].isin(['001', '024', '667'])]
        df['content'] = df.groupby(['id', 'field'])['content'].transform(lambda x: '❦'.join(x.drop_duplicates().astype(str)))
        df = df.drop_duplicates().reset_index(drop=True)
        df_wide = df.pivot(index = 'id', columns = 'field', values = 'content')
        len_df = len(df_wide)
        len_osobowe = int(df_wide['667'].str.count('osobowe').sum())
        len_osobowe_viaf = len(df_wide[(df_wide['667'].str.contains('osobowe')) & (df_wide['024'].str.contains('viaf.org'))])
        ile_wzorcowych += len_df
        ile_osobowych += len_osobowe
        ile_osobowych_z_viaf += len_osobowe_viaf
        
print(f'Liczba haseł wzorcowych: {ile_wzorcowych}.')
print(f'Liczba wzorcowych haseł osobowych: {ile_osobowych}.')
print(f'Liczba oviafowanych wzorcowych haseł osobowych: {ile_osobowych_z_viaf}.')
