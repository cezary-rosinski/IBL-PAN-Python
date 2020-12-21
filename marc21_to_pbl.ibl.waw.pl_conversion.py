import pandas as pd
import numpy as np
from my_functions import marc_parser_1_field, gsheet_to_df, mrk_to_df, df_to_gsheet, cSplit, df_to_mrc, mrc_to_mrk
import re
import pandasql
import json
import requests
import io
import itertools
import openpyxl

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

#konwerter

df = pd.read_excel('książki_BN.xlsx')

book_id = df['001']

Book_publishingHouses_places_country_name = df.copy()[['001', '008']]

Book_publishingHouses_places_country_name['008'] = Book_publishingHouses_places_country_name['008'].apply(lambda x: x[15:18])
Book_publishingHouses_places_country_name['008'] = Book_publishingHouses_places_country_name['008'].str.replace('\\\\', '')
kraje_BN = gsheet_to_df('1QGhWx2vqJWHVTaQWLNqQA3NwcJgFWH0C1P4ZICZduNA', 'Arkusz1')
Book_publishingHouses_places_country_name = pd.merge(Book_publishingHouses_places_country_name, kraje_BN, how='left', left_on='008', right_on='skrót')[['001', 'kraj']].rename(columns={'kraj':'Book_publishingHouses_places_country_name'})

Book_recordTypes_01 = df.copy()[['001', '008']]
Book_recordTypes_01['008'] = Book_recordTypes_01['008'].apply(lambda x: x[33])
typy_008 = gsheet_to_df('1bvL_mNBTVunueAT3KBFWOV4GYoPsSSWYxdNciS37j_c', '008')
Book_recordTypes_01 = pd.merge(Book_recordTypes_01, typy_008, how='left', left_on='008', right_on='skrót')[['001', 'forma literacka PBL']].rename(columns={'forma literacka PBL': 'Book_recordTypes'})

Book_recordTypes_02 = df.copy()[['001', '380']]
Book_recordTypes_02 = marc_parser_1_field(Book_recordTypes_02, '001', '380', '\$')[['001', '$a']]
typy_380 = gsheet_to_df('1bvL_mNBTVunueAT3KBFWOV4GYoPsSSWYxdNciS37j_c', '380')
typy_380 = typy_380[typy_380['PBL'].notnull()]
Book_recordTypes_02 = pd.merge(Book_recordTypes_02, typy_380, how='left', left_on='$a', right_on='BN')[['001', 'PBL']]
Book_recordTypes_02 = Book_recordTypes_02[Book_recordTypes_02['PBL'].notnull()].rename(columns={'PBL': 'Book_recordTypes'})

# tutaj trzeba dla tej listy: antologia|wiersz|poemat|proza|powieść|opowiadanie|miniatura prozą|proza poetycka|aforyzm|esej|szkic|felieton|reportaż|dziennik|wspomnienie|autobiografia|kazanie|rozmyślanie religijne|scenariusz|teksty dramatyczne|list|wypowiedź|album|rysunek, obraz|tekst biblijny|monografia|biografia|słownik|encyklopedia|bibliografia|katalog|kalendarium|podręcznik|artykuł|recenzja|wywiad|nawiązanie|polemika|sprostowanie|wstęp|posłowie|zgon|kult|ikonografia|nota|inne|monografia autorska|monografia zbiorowa zrobić wyszukanie w 655 (ale najpierw przygotować właściwe formy zwrotów, czyli np. wspomnieni(a), a nie wspomnienie




# to bardziej jest mapowanie deskryptorów na działy

mapowanie_655 = gsheet_to_df('1JQy98r4K7yTZixACxujH2kWY3D39rG1kFlIppNlZnzQ', 'deskryptory_655')
mapowanie_655 = mapowanie_655[mapowanie_655['decyzja'] == 'zmapowane'][['X655', 'dzial_PBL_1', 'dzial_PBL_2', 'dzial_PBL_3', 'dzial_PBL_4', 'dzial_PBL_5', 'haslo_przedmiotowe_PBL_1', 'haslo_przedmiotowe_PBL_2', 'haslo_przedmiotowe_PBL_3', 'haslo_przedmiotowe_PBL_4', 'haslo_przedmiotowe_PBL_5']].reset_index(drop=True)

mapowanie_655_BN_PBL = pd.DataFrame()
for i, row in mapowanie_655.iterrows():
    print(f"{i+1}/{len(mapowanie_655)}")
    mapowanie_655_BN_PBL.at[i, '655'] = row['X655']
    mapowanie_655_BN_PBL.at[i, 'PBL'] = '❦'.join([f for f in row.iloc[1:].to_list() if f])

mapowanie_655_BN_PBL = cSplit(mapowanie_655_BN_PBL, '655', 'PBL', '❦')

Book_recordTypes_03 = df.copy()[['001', '655']]
Book_recordTypes_03 = cSplit(Book_recordTypes_03, '001', '655', '❦')
query = "select * from Book_recordTypes_03 a join mapowanie_655_BN_PBL b on a.'655' like '%'||b.'655'||'%'"
Book_recordTypes_03 = pandasql.sqldf(query)
Book_recordTypes_03 = Book_recordTypes_03[['001', 'PBL']]
Book_recordTypes_03.to_excel('Book_recordTypes_03.xlsx', index=False)






























