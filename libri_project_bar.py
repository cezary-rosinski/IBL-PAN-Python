import pandas as pd
import io
import re
import numpy as np

# def
def f(row, id_field):
    if row['field'] == id_field and id_field == 'LDR':
        val = row.name
    elif row['field'] == id_field:
        val = row['content']
    else:
        val = np.nan
    return val

# main

path_in = "C:/Users/Cezary/Downloads/bar-zrzut-20200310.TXT"
encoding = 'cp852'
reader = io.open(path_in, 'rt', encoding = encoding).read().splitlines()

mrk_list = []
for row in reader:
    if row[:5] == '     ':
        mrk_list[-1] + row[5:]
    else:
        mrk_list.append(row)
        
mrk_list2 = []
for i, row in enumerate(mrk_list):
    if 'LDR' in row:
        new_field = '001  bar' + '{:09d}'.format(i+1)
        mrk_list2.append(row)
        mrk_list2.append(new_field)
    else:
        mrk_list2.append(row)
           
bar_list = []
for row in mrk_list2:
    if 'LDR' not in row:
        bar_list[-1] += '\n' + row
    else:
        bar_list.append(row)

for i, record in enumerate(bar_list):
    bar_list[i] = re.split('\n', bar_list[i])
    
bar_df = pd.DataFrame()
for index, record in enumerate(bar_list):
    print(str(index) + '/' + str(len(bar_list)))
    record = list(filter(None, record))
    for i, field in enumerate(record):
        record[i] = re.split('(?<=^...)', record[i].rstrip())
    df = pd.DataFrame(record, columns = ['field', 'content'])
    df['id'] = df.apply(lambda x: f(x, '001'), axis = 1)
    df['id'] = df['id'].ffill().bfill()
    df['content'] = df.groupby(['id', 'field'])['content'].transform(lambda x: '~'.join(x.drop_duplicates().astype(str)))
    df = df.drop_duplicates().reset_index(drop=True)
    df_wide = df.pivot(index = 'id', columns = 'field', values = 'content')
    bar_df = bar_df.append(df_wide)
    
fields_order = bar_df.columns.tolist()
fields_order.sort(key = lambda x: ([str,int].index(type("a" if re.findall(r'\w+', x)[0].isalpha() else 1)), x))
bar_df = bar_df.reindex(columns=fields_order)
  

# 5,5 h to execute the code  
# bar_df.to_excel('bar_trial.xlsx')