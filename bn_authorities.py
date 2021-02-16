import pandas as pd
import io
from my_functions import f
import re
import numpy as np
import glob
from my_functions import mrc_to_mrk
  
path = 'F:/Cezary/Documents/IBL/Migracja z BN/bn_authorities/'
files = [f for f in glob.glob(path + '*.mrc', recursive=True)]
for i, file in enumerate(files):
    print(str(i+1) + '/' + str(len(files)))
    path_out = re.sub('(.+)(\.mrc$)', r'\1.mrk', file)
    mrc_to_mrk(file, path_out)
  
files = [f for f in glob.glob(path + '*.mrk', recursive=True)]

encoding = 'utf-8'
mrk_list = []
for i, file_path in enumerate(files):
    print(f"{i+1}/{len(files)}")
    marc_list = io.open(file_path, 'rt', encoding = encoding).read().splitlines()

    for row in marc_list:
        if row.startswith('=LDR'):
            mrk_list.append([row])
        else:
            if row:
                mrk_list[-1].append(row)
                
# =============================================================================
#     for sublist in mrk_list:
#         try:
#             year = int(''.join([ele for ele in sublist if ele.startswith('=008')])[13:17])
#             if year in range(2004,2021):
#                 for el in sublist:
#                     if el.startswith('=773'):
#                         val = re.search('(\$t)(.+?)(\$|$)', el).group(2)
#                         if val in bn_magazines:
#                             new_list.append(sublist)
#         except (ValueError, AttributeError):
#             pass
# =============================================================================

final_list = []
for i, lista in enumerate(mrk_list):
    print(f"{i+1}/{len(mrk_list)}")    
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

#%% analiza

test = marc_df.copy().head(1000)

kanibalizm = marc_df.copy()[(marc_df['150'].notnull()) & (marc_df['150'].str.contains('Ludożerstwo'))].squeeze()
literatura = marc_df.copy()[(marc_df['150'].notnull()) & (marc_df['150'].str.contains('Literatura'))]
emigracja = marc_df.copy()[(marc_df['010'].notnull()) & (marc_df['010'].str.contains('94004251'))]
wietnam = marc_df.copy()[(marc_df['010'].notnull()) & (marc_df['010'].str.contains('2017000023'))].squeeze()





#%% stare podejście
# =============================================================================
# encoding = 'utf-8'
# ile_wzorcowych = 0
# ile_osobowych = 0
# ile_osobowych_z_viaf = 0
# for i, file_path in enumerate(files):
#     print(str(i) + '/' + str(len(files)))
#     marc_list = io.open(file_path, 'rt', encoding = encoding).read().splitlines()
#     marc_list = list(filter(None, marc_list))  
#     df = pd.DataFrame(marc_list, columns = ['test'])
#     df['field'] = df['test'].replace(r'(^.)(...)(.+?$)', r'\2', regex = True)
#     df['content'] = df['test'].replace(r'(^.)(.....)(.+?$)', r'\3', regex = True)
#     df['help'] = df.apply(lambda x: f(x, 'LDR'), axis=1)
#     df['help'] = df['help'].ffill()
#     if len(df) > 0:
#         df['id'] = df.apply(lambda x: f(x, '001'), axis = 1)
#         df['id'] = df.groupby('help')['id'].ffill().bfill()
#         df = df[['id', 'field', 'content']]
#         df = df[df['field'].isin(['001', '024', '667'])]
#         df['content'] = df.groupby(['id', 'field'])['content'].transform(lambda x: '❦'.join(x.drop_duplicates().astype(str)))
#         df = df.drop_duplicates().reset_index(drop=True)
#         df_wide = df.pivot(index = 'id', columns = 'field', values = 'content')
#         len_df = len(df_wide)
#         len_osobowe = int(df_wide['667'].str.count('osobowe').sum())
#         len_osobowe_viaf = len(df_wide[(df_wide['667'].str.contains('osobowe')) & (df_wide['024'].str.contains('viaf.org'))])
#         ile_wzorcowych += len_df
#         ile_osobowych += len_osobowe
#         ile_osobowych_z_viaf += len_osobowe_viaf
#         
# print(f'Liczba haseł wzorcowych: {ile_wzorcowych}.')
# print(f'Liczba wzorcowych haseł osobowych: {ile_osobowych}.')
# print(f'Liczba oviafowanych wzorcowych haseł osobowych: {ile_osobowych_z_viaf}.')
# =============================================================================






















