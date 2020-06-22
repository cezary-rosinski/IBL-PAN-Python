import pandas as pd
import io
import re
import numpy as np
from my_functions import cSplit
from my_functions import df_to_mrc

# def
def f(row, id_field):
    if row['field'] == id_field and id_field == 'LDR':
        val = row.name
    elif row['field'] == id_field:
        val = row['content'].strip()
    else:
        val = np.nan
    return val

def replacer(s, newstring, index, nofail=False):
    # raise an error if index is outside of the string
    if not nofail and index not in range(len(s)):
        raise ValueError("index outside given string")

    # if not erroring, but the index is still not in the correct range..
    if index < 0:  # add it to the beginning
        return newstring + s
    if index > len(s):  # add it to the end
        return s + newstring

    # insert the new string between "slices" of the original
    return s[:index] + newstring + s[index + 1:]

def replace_with_backslash(x):
    if pd.isnull(x):
        x = np.nan
    elif x[0] == ' ' and x[1] == ' ':
        x = replacer(x, '\\', 0)
        x = replacer(x, '\\', 1)
    elif x[0] == ' ':
        x = replacer(x, '\\', 0)    
    elif x[1] == ' ':
        x = replacer(x, '\\', 1)
    return x

# main

path_in = "C:/Users/Cezary/Downloads/bar-zrzut-20200310.TXT"
encoding = 'cp852'
reader = io.open(path_in, 'rt', encoding = encoding).read().splitlines()

mrk_list = []
for row in reader:
    if row[:5] == '     ':
        mrk_list[-1] += row[5:]
    else:
        mrk_list.append(row)
               
bar_list = []
for i, row in enumerate(mrk_list):
    if 'LDR' in row:
        new_field = '001  bar' + '{:09d}'.format(i+1)
        bar_list.append(row)
        bar_list.append(new_field)
    else:
        bar_list.append(row)
        
df = pd.DataFrame(bar_list, columns = ['test'])
df = df[df['test'] != '']
df['field'] = df['test'].replace(r'(^...)(.+?$)', r'\1', regex = True)
df['content'] = df['test'].replace(r'(^...)(.+?$)', r'\2', regex = True)
df['help'] = df.apply(lambda x: f(x, 'LDR'), axis=1)
df['help'] = df['help'].ffill()
df['id'] = df.apply(lambda x: f(x, '001'), axis = 1)
df['id'] = df.groupby('help')['id'].ffill().bfill()
df = df[['id', 'field', 'content']]
df['content'] = df.groupby(['id', 'field'])['content'].transform(lambda x: '❦'.join(x.drop_duplicates().astype(str)))
df = df.drop_duplicates().reset_index(drop=True)
bar_catalog = df.pivot(index = 'id', columns = 'field', values = 'content')

# =============================================================================
# bar_catalog.to_excel('bar_catalog_1st_stage.xlsx', index=False)
# bar_catalog = pd.read_excel('bar_catalog_1st_stage.xlsx')
# =============================================================================

test = bar_catalog.head(1000)

problemy = []
for i, row in bar_catalog.iterrows():
    for elem in row:
        if pd.notnull(elem) and '~' in elem:
            try:
                val = re.findall('\~..', elem)[0]
                problemy.append(val)
            except IndexError:
                pass
problemy = list(set(problemy))

problemy2 = []
for i, row in test.iterrows():
    for elem in row:
        if pd.notnull(elem) and '~' in elem:
            problemy2.append(elem)



bar_catalog['001'] = bar_catalog['001'].str.strip()
bar_catalog['008'] = bar_catalog['008'].str.strip().str.replace('(^|.)(\%.)', '', regex=True).str.replace('([\+\!])', '-', regex=True)
bar_catalog['do 100'] = bar_catalog['100'].str.replace(r'(^.+?)(❦)(.+?$)', r'\3', regex=True)
bar_catalog['100'] = bar_catalog['100'].str.replace(r'(^.+?)(❦)(.+?$)', r'\1', regex=True)
bar_catalog['700'] = bar_catalog[['do 100', '700']].apply(lambda x: ''.join(x.dropna().astype(str)), axis=1)
del bar_catalog['do 100']
bar_catalog['100'] = bar_catalog['100'].str.replace(r'(?<=\%d)(\()(.+?)(\)\.{0,1})', r'\2', regex=True).str.replace('(?<=[a-zà-ž][a-zà-ž])\.$', '', regex=True)

bar_catalog = cSplit(bar_catalog, '001', '600', '❦')
bar_catalog['600'] = bar_catalog['600'].str.replace(r'(?<=\%d)(\()(.+?)(\)\.{0,1})', r'\2', regex=True).str.replace('(?<=[a-zà-ž][a-zà-ž])\.$', '', regex=True)
bar_catalog['787'] = bar_catalog['600'].str.replace('(\%d.+?)(?=\%)', '', regex=True).str.replace(r'(?<=^)(..)', r'08', regex=True)
bar_catalog['787'] = bar_catalog['787'].apply(lambda x: x if pd.notnull(x) and '%t' in x else np.nan)
bar_catalog['600'] = bar_catalog['600'].str.replace('(\%t.+?)(?=\%|$)', '', regex=True).str.strip()
bar_catalog['600'] = bar_catalog.groupby('001')['600'].transform(lambda x: '❦'.join(x.dropna()))
bar_catalog['787'] = bar_catalog.groupby('001')['787'].transform(lambda x: '❦'.join(x.dropna()))
bar_catalog = bar_catalog.drop_duplicates()
bar_catalog['600'] = bar_catalog['600'].str.split('❦').apply(set).str.join('❦')

bar_catalog = cSplit(bar_catalog, '001', '700', '❦')
bar_catalog['700'] = bar_catalog['700'].str.replace(r'(?<=\%d)(\()(.+?)(\)\.{0,1})', r'\2', regex=True).str.replace('(?<=[a-zà-ž][a-zà-ž])\.$', '', regex=True)
bar_catalog['700'] = bar_catalog.groupby('001')['700'].transform(lambda x: '❦'.join(x.dropna()))
bar_catalog = bar_catalog.drop_duplicates()
bar_catalog['700'] = bar_catalog['700'].str.split('❦').apply(set).str.join('❦')
bar_catalog['LDR'] = '-----nab-a22-----4u-4500'

bar_catalog = cSplit(bar_catalog, '001', '773', '❦')
bar_catalog['do 773'] = bar_catalog['773'].str.extract(r'(?<=\%g)(.{4})')
bar_catalog['do 773'] = bar_catalog['do 773'].apply(lambda x: '$9' + x if pd.notnull(x) else np.nan)
bar_catalog['773'] = bar_catalog['773'].str.replace(r'(?<=^)(..)', r'0\\', regex=True)
bar_catalog['773'] = bar_catalog[['773', 'do 773']].apply(lambda x: ''.join(x.dropna().astype(str)), axis=1)
del bar_catalog['do 773']
bar_catalog['773'] = bar_catalog.groupby('001')['773'].transform(lambda x: '❦'.join(x.dropna()))
bar_catalog = bar_catalog.drop_duplicates()
bar_catalog['995'] = '\\\\$aBibliografia Bara'

bar_catalog = bar_catalog.replace(r'^\s*$', np.NaN, regex=True)
for column in bar_catalog:       
    bar_catalog[column] = bar_catalog[column].apply(lambda x: replace_with_backslash(x))
bar_catalog = bar_catalog.replace(r'(?<=❦)(  )(?=\%)', r'\\\\', regex=True).replace(r'(?<=❦)( )([^ ])(?=\%)', r'\\\2', regex=True).replace(r'(?<=❦)([^ ])( )(?=\%)', r'\1\\', regex=True)
bar_catalog = bar_catalog.replace(r'(\s{0,1}\%)(.)', r'$\2', regex=True)
fields_order = bar_catalog.columns.tolist()
fields_order.sort(key = lambda x: ([str,int].index(type("a" if re.findall(r'\w+', x)[0].isalpha() else 1)), x))
bar_catalog = bar_catalog.reindex(columns=fields_order)

bar_catalog.to_excel('bar_catalog.xlsx', index=False)

df_to_mrc(bar_catalog, '❦', 'bar_catalog.mrc')

# =============================================================================
# kod zwrócił 100 błędów
# w kolejnej iteracji przyjrzeć się tym rekordom
# 
# reader = io.open('marc_errors_bar.txt', 'rt').read().splitlines()[0]
# 
# for i, elem in enumerate(reader):
#     reader[i] = re.split('(?<=\)), (?=\()', reader[i])
#     for j, field in enumerate(reader[i]):
#         reader[i][j] = re.sub('(?<=\))\]+$', '', re.sub('^\[+', '', reader[i][j])).replace("', '", '|') 
#         reader[i][j] = list(reader[i][j])
#         reader[i][j][1] = '❦'.join(reader[i][j][1]) 
# =============================================================================














