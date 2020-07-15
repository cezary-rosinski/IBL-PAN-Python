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
from my_functions import cosine_sim_2_elem
from my_functions import get_cosine_result

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
    
bn_magazines = bn_magazines.groupby(['bn_magazine']).count().reset_index(level=['bn_magazine']).rename(columns={'index':'count'}).sort_values('count', ascending=False)
bn_magazines.to_excel('bn_magazines.xlsx', index=False)

bn_magazines = pd.read_excel('bn_magazines.xlsx')
bn_magazines = bn_magazines[bn_magazines['bn_magazine'].notnull()]

dsn_tns = cx_Oracle.makedsn('pbl.ibl.poznan.pl', '1521', service_name='xe')
connection = cx_Oracle.connect(user='IBL_SELECT', password='CR333444', dsn=dsn_tns, encoding='windows-1250')

pbl_query = """select zr.zr_zrodlo_id, zr.zr_tytul
            from IBL_OWNER.pbl_zrodla zr"""

pbl_magazines = pd.read_sql(pbl_query, connection)
  
    
pbl_lista = pbl_magazines['ZR_TYTUL'].to_list()    
bn_lista = bn_magazines['bn_magazine'].to_list()   
import itertools
combinations = list(itertools.product(pbl_lista, bn_lista))

df = pd.DataFrame(combinations, columns=['pbl_magazine', 'bn_magazine'])
df['similarity'] = df.apply(lambda x: get_cosine_result(x['pbl_magazine'], x['bn_magazine']), axis=1)


test = combinations[:100000]
import time
start_time = time.time()
for i, elem in enumerate(test):
    print(str(i+1) + '/' + str(len(test)))
    test[i] += (get_cosine_result(elem[0], elem[1]),)
print("--- %s seconds ---" % (time.time() - start_time))

test = df.copy().head(100000)
start_time = time.time()
test['similarity'] = test.apply(lambda x: get_cosine_result(x['pbl_magazine'], x['bn_magazine']), axis=1)
print("--- %s seconds ---" % (time.time() - start_time))



test['pbl_magazine'].values

df['similarity'] = get_cosine_result(df['pbl_magazine'].values, df['bn_magazine'].values)




def soc_iter(TEAM,home,away,ftr):
    df['Draws'] = 'No_Game'
    df.loc[((home == TEAM) & (ftr == 'D')) | ((away == TEAM) & (ftr == 'D')), 'Draws'] = 'Draw'
    df.loc[((home == TEAM) & (ftr != 'D')) | ((away == TEAM) & (ftr != 'D')), 'Draws'] = 'No_Draw'

for i, elem in enumerate(combinations):
    print(str(i+1) + '/' + str(len(combinations)))
    combinations[i] += (get_cosine_result(elem[0], elem[1]),)
    
    
    combinations[0] + ('ja',)




        
def cosine_sim_2_elem(lista):
    kombinacje = combinations(lista, 2)
    list_of_lists = [list(elem) for elem in kombinacje]
    for kombinacja in list_of_lists:
        kombinacja.append(get_cosine_result(kombinacja[0], kombinacja[1]))
    df = pd.DataFrame(data = list_of_lists, columns = ['string1', 'string2', 'cosine_similarity'])
    return df    
        
        
        
        print(row_bn['bn_magazine'])
    test = cosine_sim_2_elem([pbl_magazines.iloc[1,1], bn_magazines.iloc[1,0]])
    print(row['ZR_TYTUL'])
    
    viaf_people.at[ind, 'cosine'] = cosine_sim_2_elem([vname['viaf name'], vname['cz name']]).iloc[:, -1].to_string(index=False).strip()
    viaf_people = viaf_people[viaf_people['cosine'] == viaf_people['cosine'].max()]

cosine = get_cosine_result(pbl_magazines.iloc[1,1], bn_magazines['bn_magazine'])




































