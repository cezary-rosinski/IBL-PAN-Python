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

#books

reader = io.open('F:/Cezary/Documents/IBL/Libri/Iteracja 10.2020/libri_marc_bn_books.mrk', 'rt', encoding = 'UTF-8').read().splitlines()

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
















