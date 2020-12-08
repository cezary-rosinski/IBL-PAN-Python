import pandas as pd
import numpy as np
from my_functions import marc_parser_1_field, gsheet_to_df
import re
import pandasql
from my_functions import cSplit
import json
import requests
from my_functions import df_to_mrc
import io
from my_functions import mrc_to_mrk
import itertools

# def
def unique_subfields(x):
    new_x = []
    for val in x:
        val = val.split('❦')
        new_x += val
    new_x = list(set(new_x))
    try:
        new_x.sort(key = lambda x: ([str,int].index(type("a" if x[1].isalpha() else 1)), x))
    except IndexError:
        pass
    new_x = '|'.join(new_x)
    return new_x

#books

reader = io.open('F:/Cezary/Documents/IBL/Libri/Iteracja 10.2020/libri_marc_bn_books.mrk', 'rt', encoding = 'UTF-8').read().splitlines()     

bn_field_subfield = []        
for i, l in enumerate(reader):
    print(f"{i+1}/{len(reader)}")
    if l:
        field = re.sub('(^.)(...)(.*)', r'\2', l)
        subfields = '❦'.join(re.findall('\$.', l))
        bn_field_subfield.append([field, subfields])
        
df = pd.DataFrame(bn_field_subfield, columns = ['field', 'subfield'])
          
df['subfield'] = df.groupby('field')['subfield'].transform(lambda x: unique_subfields(x))   
df = df.drop_duplicates().sort_values('field')
df = cSplit(df, 'field', 'subfield', '|')













test = [[r[1:4]] for r in reader if r]
test2 = [re.findall('\$.', r) for r in reader if r]
unique_data = []
for t1, t2 in zip(test, test2):
    lista = t1 + t2
    unique_data.append(lista)
    
unique_data = [list(x) for x in set(tuple(x) for x in unique_data)]

u_fields = []
final_data = []
for l in unique_data:
    if l[0] not in u_fields:
        u_fields.append(l[0])
        final_data.append(l)
    else:
        final_data[-1] += l[1:]
        
final_data = [list(set(l)) for l in final_data]

final_data = [sorted(f, key=len, reverse=True) for f in final_data]
















