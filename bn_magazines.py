import pandas as pd
import numpy as np
from my_functions import marc_parser_1_field
import re
import pandasql
from my_functions import cSplit
import json
import requests
from my_functions import df_to_mrc
import io
from my_functions import gsheet_to_df
import cx_Oracle
import regex
from functools import reduce
import glob
from my_functions import f

def chunks(l, n):
    for i in range(0, len(l), n):
        yield l[i:i+n]

# path = 'F:/Cezary/Documents/IBL/Migracja z BN/bn_all/'
path = 'C:/Users/User/Documents/bn_all/'
files = [file for file in glob.glob(path + '*.mrk8', recursive=True)]

encoding = 'utf-8'
magazine_list = []
for i, file_path in enumerate(files):
    print(str(i) + '/' + str(len(files)))
    marc_list = io.open(file_path, 'rt', encoding = encoding).read().splitlines()
    marc_list = [elem for elem in marc_list if elem.startswith('=773')]
    magazine_list += marc_list

list_of_magazine_list = list(chunks(magazine_list, 10000))
bn_magazines = pd.DataFrame()
for i, elem in enumerate(list_of_magazine_list):
    print(str(i) + '/' + str(len(list_of_magazine_list)))
    df = pd.DataFrame(elem, columns=['bn_magazine'])
    df['bn_magazine'] = df['bn_magazine'].apply(lambda x: x[6:])
    df['index'] = df.index + 1
    df = marc_parser_1_field(df, 'index', 'bn_magazine', '\$')[['index', '$t']].rename(columns={'$t':'bn_magazine'})
    bn_magazines = bn_magazines.append(df)
    
bn_magazines['bn_magazine'] = bn_magazines['bn_magazine'].str.replace('\.$', '')
bn_magazines = bn_magazines.groupby(['bn_magazine']).count().reset_index(level=['bn_magazine']).rename(columns={'index':'count'}).sort_values('count', ascending=False)
bn_magazines.to_excel('bn_magazines.xlsx', index=False)


