# wygenerować raz jeszcze i sprawdzić wszystkie nan
# połączyć wszystkie wersje tytułów z typami tytułów
# dać jeden typ tytułu dla tytułu i języka tytułu
# szczegółowo przyjrzeć się przypisywanym automatycznie działom

import pandas as pd
import numpy as np
from my_functions import marc_parser_1_field, gsheet_to_df, mrk_to_df, df_to_gsheet, cSplit, df_to_mrc, mrc_to_mrk, get_cosine_result
import regex as re
import pandasql
import json
import requests
import io
import itertools
import openpyxl
import roman
import xml.etree.ElementTree as et
import requests
from functools import reduce

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

reader = io.open('F:/Cezary/Documents/IBL/Libri/Iteracja 2021-02/libri_marc_bn_books_2021-2-9.mrk', 'rt', encoding = 'UTF-8').read().splitlines()     

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

df = pd.read_excel('książki_BN.xlsx', engine='openpyxl')

book_id = df[['001']].drop_duplicates().reset_index(drop=True)

Book_publishingHouses_places_country_name = df.copy()[['001', '008']].drop_duplicates().reset_index(drop=True)

Book_publishingHouses_places_country_name['008'] = Book_publishingHouses_places_country_name['008'].apply(lambda x: x[15:18])
Book_publishingHouses_places_country_name['008'] = Book_publishingHouses_places_country_name['008'].str.replace('\\\\', '')
kraje_BN = gsheet_to_df('1QGhWx2vqJWHVTaQWLNqQA3NwcJgFWH0C1P4ZICZduNA', 'Arkusz1')
Book_publishingHouses_places_country_name = pd.merge(Book_publishingHouses_places_country_name, kraje_BN, how='left', left_on='008', right_on='skrót')[['001', 'kraj']].rename(columns={'kraj':'Book.publishingHouses.places.country.name'})

Book_recordTypes_01 = df.copy()[['001', '008']].drop_duplicates().reset_index(drop=True)
Book_recordTypes_01['008'] = Book_recordTypes_01['008'].apply(lambda x: x[33])
typy_008 = gsheet_to_df('1bvL_mNBTVunueAT3KBFWOV4GYoPsSSWYxdNciS37j_c', '008')
Book_recordTypes_01 = pd.merge(Book_recordTypes_01, typy_008, how='left', left_on='008', right_on='skrót')[['001', 'rodzaj i typ PBL']].rename(columns={'rodzaj i typ PBL': 'Book.recordTypes'})

Book_recordTypes_02 = marc_parser_1_field(df, '001', '380', '\$')[['001', '$a']].drop_duplicates().reset_index(drop=True)
typy_380 = gsheet_to_df('1bvL_mNBTVunueAT3KBFWOV4GYoPsSSWYxdNciS37j_c', '380')
typy_380 = typy_380[typy_380['PBL'].notnull()]
Book_recordTypes_02 = pd.merge(Book_recordTypes_02, typy_380, how='left', left_on='$a', right_on='BN')[['001', 'PBL']]
Book_recordTypes_02 = Book_recordTypes_02[Book_recordTypes_02['PBL'].notnull()].rename(columns={'PBL': 'Book.recordTypes'})

Book_recordTypes_03 = df.copy()[['001', '655']].drop_duplicates().reset_index(drop=True)
Book_recordTypes_03 = cSplit(Book_recordTypes_03, '001', '655', '❦')

typy_655 = gsheet_to_df('1KDECeDgTFOvM72zYqBL8fpKJELT_pJOQfF7J7o1oUG8', '655')
typy_655 = typy_655[typy_655['fraza do szukania w BN'].notnull()]
typy_655 = cSplit(typy_655, 'gatunki PBL', 'fraza do szukania w BN', '|')

query = "select * from Book_recordTypes_03 a join typy_655 b on a.'655' like '%'||b.'fraza do szukania w BN'||'%'"
Book_recordTypes_03 = pandasql.sqldf(query)

Book_recordTypes_03['test'] = Book_recordTypes_03.apply(lambda x: x['fraza do szukania w BN'] in x['655'], axis=1)
Book_recordTypes_03 = Book_recordTypes_03[Book_recordTypes_03['test'] == True][['001', 'gatunki PBL']].rename(columns={'gatunki PBL': 'Book.recordTypes'})

Book_recordTypes = pd.concat([Book_recordTypes_01, Book_recordTypes_02, Book_recordTypes_03])
Book_recordTypes['Book.recordTypes'] = Book_recordTypes.groupby('001')['Book.recordTypes'].transform(lambda x: '❦'.join(x.drop_duplicates().astype(str)))
Book_recordTypes = Book_recordTypes.drop_duplicates().reset_index(drop=True)
Book_recordTypes['Book.recordTypes'] = Book_recordTypes['Book.recordTypes'].str.replace('inne❦', '')

Book_languages_01 = df.copy()[['001', '008']].drop_duplicates().reset_index(drop=True)
Book_languages_01['008'] = Book_languages_01['008'].apply(lambda x: x[35:38])
skroty_jezykow_marc21 = gsheet_to_df('138lu4jhz-VhaQsn1vyjiIjwCsQHaAaUEgecqusTII54', 'Arkusz1')
Book_languages_01 = pd.merge(Book_languages_01, skroty_jezykow_marc21, how='left', left_on='008', right_on='Skrót')[['001', 'Język']].rename(columns={'Język':'Book.languages'})

Book_languages_02 = marc_parser_1_field(df, '001', '041', '\$')[['001', '$a']].drop_duplicates().reset_index(drop=True)
Book_languages_02 = cSplit(Book_languages_02, '001', '$a', '❦')
Book_languages_02 = pd.merge(Book_languages_02, skroty_jezykow_marc21, how='left', left_on='$a', right_on='Skrót')[['001', 'Język']].rename(columns={'Język':'Book.languages'})

Book_languages = pd.concat([Book_languages_01, Book_languages_02])
Book_languages['Book.languages'] = Book_languages.groupby('001')['Book.languages'].transform(lambda x: '❦'.join(x.drop_duplicates().astype(str)))
Book_languages = Book_languages.drop_duplicates().reset_index(drop=True)

Book_titles_language_basic = marc_parser_1_field(df, '001', '041', '\$')[['001', '$a']].drop_duplicates().reset_index(drop=True)
Book_titles_language_basic = cSplit(Book_titles_language_basic, '001', '$a', '❦')
Book_titles_language_basic = pd.merge(Book_titles_language_basic, skroty_jezykow_marc21, how='left', left_on='$a', right_on='Skrót')[['001', 'Język']].rename(columns={'Język':'Book.titles.language'})
Book_titles_language_basic['Book.titles.type'] = 'tytuł podstawowy'
Book_titles_language_basic = Book_titles_language_basic[Book_titles_language_basic['Book.titles.language'].notnull()]

Book_titles_language_original = marc_parser_1_field(df, '001', '041', '\$')[['001', '$h']].drop_duplicates().reset_index(drop=True)
Book_titles_language_original = cSplit(Book_titles_language_original, '001', '$h', '❦')
Book_titles_language_original = pd.merge(Book_titles_language_original, skroty_jezykow_marc21, how='left', left_on='$h', right_on='Skrót')[['001', 'Język']].rename(columns={'Język':'Book.titles.language'})
Book_titles_language_original['Book.titles.type'] = 'tytuł oryginału'
Book_titles_language_original = Book_titles_language_original[Book_titles_language_original['Book.titles.language'].notnull()]

Book_titles_language = pd.concat([Book_titles_language_basic, Book_titles_language_original])
Book_titles_language['Book.titles.language'] = Book_titles_language.groupby('001')['Book.titles.language'].transform(lambda x: '❦'.join(x.drop_duplicates().astype(str)))
Book_titles_language['Book.titles.type'] = Book_titles_language.groupby('001')['Book.titles.type'].transform(lambda x: '❦'.join(x.drop_duplicates().astype(str)))
Book_titles_language = Book_titles_language.drop_duplicates().reset_index(drop=True)

Book_remarks = marc_parser_1_field(df, '001', '080', '\$')[['001', '$a']].drop_duplicates().reset_index(drop=True).rename(columns={'$a': 'Book.remarks'})
Book_remarks['Book.remarks'] = Book_remarks.groupby('001')['Book.remarks'].transform(lambda x: '❦'.join(x.drop_duplicates().astype(str)))
Book_remarks = Book_remarks.drop_duplicates().reset_index(drop=True)

Book_authors_person_names_name_01 = marc_parser_1_field(df, '001', '100', '\$')[['001', '$a', '$b']].drop_duplicates().reset_index(drop=True)
Book_authors_person_names_name_01 = Book_authors_person_names_name_01.replace(r'^\s*$', np.nan, regex=True)
Book_authors_person_names_name_01['Book.authors.person.names.name'] = Book_authors_person_names_name_01[Book_authors_person_names_name_01.columns[1:]].apply(lambda x: ' '.join(x.dropna().astype(str)), axis=1)
Book_authors_person_names_name_01 = Book_authors_person_names_name_01[['001', 'Book.authors.person.names.name']]

Book_authors_person_names_name_02 = marc_parser_1_field(df, '001', '700', '\$').drop_duplicates().reset_index(drop=True)
Book_authors_person_names_name_02 = Book_authors_person_names_name_02[(Book_authors_person_names_name_02['$e'] == '') |
                                                                      (Book_authors_person_names_name_02['$e'] == 'Autor')][['001', '$a', '$b']]

def roman_numerals(x):
    try:
        if roman.fromRoman(x.split(' ')[0]):
            val = True
    except (SyntaxError, roman.InvalidRomanNumeralError):
        val = False
    return val

Book_authors_person_names_name_02['czy numeracja'] = Book_authors_person_names_name_02['$b'].apply(lambda x: roman_numerals(x))
Book_authors_person_names_name_02['$b'] = Book_authors_person_names_name_02.apply(lambda x: '' if x['czy numeracja'] == False else x['$b'], axis=1)
Book_authors_person_names_name_02 = Book_authors_person_names_name_02.drop(columns='czy numeracja').replace(r'^\s*$', np.nan, regex=True)
Book_authors_person_names_name_02['Book.authors.person.names.name'] = Book_authors_person_names_name_02[Book_authors_person_names_name_02.columns[1:]].apply(lambda x: ' '.join(x.dropna().astype(str)), axis=1)
Book_authors_person_names_name_02 = Book_authors_person_names_name_02[['001', 'Book.authors.person.names.name']]

Book_authors_person_names_name = pd.concat([Book_authors_person_names_name_01, Book_authors_person_names_name_02])
Book_authors_person_names_name['Book.authors.person.names.name'] = Book_authors_person_names_name.groupby('001')['Book.authors.person.names.name'].transform(lambda x: '❦'.join(x.drop_duplicates().astype(str)))
Book_authors_person_names_name = Book_authors_person_names_name.drop_duplicates().reset_index(drop=True)

Book_triggerInstitutions_history_names_name_01 = marc_parser_1_field(df, '001', '110', '\$')[['001', '$a', '$b']].drop_duplicates().reset_index(drop=True).replace(r'^\s*$', np.nan, regex=True)
Book_triggerInstitutions_history_names_name_01['Book.triggerInstitutions.history.names.name'] = Book_triggerInstitutions_history_names_name_01[Book_triggerInstitutions_history_names_name_01.columns[1:]].apply(lambda x: ', '.join(x.dropna().astype(str)), axis=1)
Book_triggerInstitutions_history_names_name_01 = Book_triggerInstitutions_history_names_name_01[['001', 'Book.triggerInstitutions.history.names.name']]

Book_triggerInstitutions_history_names_name_02 = marc_parser_1_field(df, '001', '710', '\$')[['001', '$a', '$b', '$4']].drop_duplicates().reset_index(drop=True)
Book_triggerInstitutions_history_names_name_02 = Book_triggerInstitutions_history_names_name_02[Book_triggerInstitutions_history_names_name_02['$4'].isin(['', 'oth'])].drop(columns=['$4']).replace(r'^\s*$', np.nan, regex=True)
Book_triggerInstitutions_history_names_name_02['Book.triggerInstitutions.history.names.name'] = Book_triggerInstitutions_history_names_name_02[Book_triggerInstitutions_history_names_name_02.columns[1:]].apply(lambda x: ', '.join(x.dropna().astype(str)), axis=1)
Book_triggerInstitutions_history_names_name_02 = Book_triggerInstitutions_history_names_name_02[['001', 'Book.triggerInstitutions.history.names.name']]

Book_triggerInstitutions_history_names_name = pd.concat([Book_triggerInstitutions_history_names_name_01, Book_triggerInstitutions_history_names_name_02])
Book_triggerInstitutions_history_names_name['Book.triggerInstitutions.history.names.name'] = Book_triggerInstitutions_history_names_name.groupby('001')['Book.triggerInstitutions.history.names.name'].transform(lambda x: '❦'.join(x.drop_duplicates().astype(str)))
Book_triggerInstitutions_history_names_name = Book_triggerInstitutions_history_names_name.drop_duplicates().reset_index(drop=True)

#na razie nie robimy serii wydarzeń
Book_linkedEvents_names_name_01 = marc_parser_1_field(df, '001', '111', '\$')[['001', '$a']].drop_duplicates().reset_index(drop=True)
Book_linkedEvents_names_name_02 = marc_parser_1_field(df, '001', '611', '\$')[['001', '$a']].drop_duplicates().reset_index(drop=True)
Book_linkedEvents_names_name_03 = marc_parser_1_field(df, '001', '711', '\$')[['001', '$a']].drop_duplicates().reset_index(drop=True)

Book_linkedEvents_names_name = pd.concat([Book_linkedEvents_names_name_01, Book_linkedEvents_names_name_02, Book_linkedEvents_names_name_03]).rename(columns={'$a': 'Book.linkedEvents.names.name'})

Book_linkedEvents_names_name['Book.linkedEvents.names.name'] = Book_linkedEvents_names_name.groupby('001')['Book.linkedEvents.names.name'].transform(lambda x: '❦'.join(x.drop_duplicates().astype(str)))

Book_linkedEvents_names_name = Book_linkedEvents_names_name.drop_duplicates().reset_index(drop=True)

# dać Biblę. Apokryfy do działów z apokryfami w PBL

Book_headings_heading_name_01 = marc_parser_1_field(df, '001', '130', '\$').drop_duplicates().reset_index(drop=True)

dzialy_biblijne_PBL = gsheet_to_df('1FxAxWONxY81LSEOo_TItNxKA0efjV0Hp7SPxuI5dikg', 'Arkusz1')

query = "select * from Book_headings_heading_name_01 a join dzialy_biblijne_PBL b on a.'$p' like '%'||b.'dział PBL'||'%'"
Book_headings_heading_name_01 = pandasql.sqldf(query)
Book_headings_heading_name_01 = Book_headings_heading_name_01[['001', 'dział PBL']].rename(columns={'dział PBL': 'Book.headings.heading.name'})

Book_headings_heading_name_02 = marc_parser_1_field(df, '001', '130', '\$').drop_duplicates().reset_index(drop=True)
Book_headings_heading_name_02 = Book_headings_heading_name_02[~Book_headings_heading_name_02['001'].isin(Book_headings_heading_name_01['001'])]

def dzial_biblijny(x):
    if 'ST' in x:
        val = 'Teksty biblijne (Biblia, hebrajska)'
    elif 'NT' in x:
        val = 'Teksty biblijne (Biblia grecka starożytna)'
    else:
        val = np.nan
    return val

Book_headings_heading_name_02['Book.headings.heading.name'] = Book_headings_heading_name_02['$p'].apply(lambda x: dzial_biblijny(x))
Book_headings_heading_name_02 = Book_headings_heading_name_02[Book_headings_heading_name_02['Book.headings.heading.name'].notnull()][['001', 'Book.headings.heading.name']]
# tutaj przygotować mapowanie na działy PBL, bo na razie są wartości z BN
Book_headings_heading_name_03 = marc_parser_1_field(df, '001', '386', '\$').drop_duplicates().reset_index(drop=True)[['001', '$a']].rename(columns={'$a': 'Book.headings.heading.name'})

Book_headings_heading_name_04 = marc_parser_1_field(df, '001', '630', '\$').drop_duplicates().reset_index(drop=True)
query = "select * from Book_headings_heading_name_04 a join dzialy_biblijne_PBL b on a.'$p' like '%'||b.'dział PBL'||'%'"
Book_headings_heading_name_04 = pandasql.sqldf(query)
Book_headings_heading_name_04 = Book_headings_heading_name_04[['001', 'dział PBL']].rename(columns={'dział PBL': 'Book.headings.heading.name'})

Book_headings_heading_name_05 = marc_parser_1_field(df, '001', '630', '\$').drop_duplicates().reset_index(drop=True)
Book_headings_heading_name_05 = Book_headings_heading_name_05[~Book_headings_heading_name_05['001'].isin(Book_headings_heading_name_04['001'])]
Book_headings_heading_name_05['Book.headings.heading.name'] = Book_headings_heading_name_05['$p'].apply(lambda x: dzial_biblijny(x))
Book_headings_heading_name_05 = Book_headings_heading_name_05[Book_headings_heading_name_05['Book.headings.heading.name'].notnull()][['001', 'Book.headings.heading.name']]

mapowania_650 = ['1YBA3yK2DAIm4GUaIKcEaRaE-qBwwGUkNkdktGXF81w4', '1j6fHvpSkesgG9qlFfrBzGasp1Tr8dxJNQTH6etw9sfQ', '1PlRvoZBg_Q3YOxO0b6DZJIm1oGSSWrzckBYQaXWA_uo']
mapowanie_650 = pd.DataFrame()
j = 0
for link in mapowania_650:
    df_650 = gsheet_to_df(link, 'deskryptory_650')
    df_650 = df_650[df_650['decyzja'] == 'zmapowane'][['X650', 'dzial_PBL_1', 'dzial_PBL_2', 'dzial_PBL_3', 'haslo_przedmiotowe_PBL_1', 'haslo_przedmiotowe_PBL_2', 'haslo_przedmiotowe_PBL_3']].reset_index(drop=True)
    for i, row in df_650.iterrows():
        print(f"{i+1}/{len(df_650)}")
        mapowanie_650.at[j, '650'] = row['X650']
        mapowanie_650.at[j, 'PBL'] = '❦'.join([f for f in row.iloc[1:].to_list() if f])
        j += 1

Book_headings_heading_name_06 = df.copy()[['001', '650']].drop_duplicates().reset_index(drop=True)
Book_headings_heading_name_06 = cSplit(Book_headings_heading_name_06, '001', '650', '❦')
query = "select * from Book_headings_heading_name_06 a join mapowanie_650 b on a.'650' like '%'||b.'650'||'%'"
Book_headings_heading_name_06 = pandasql.sqldf(query)
Book_headings_heading_name_06 = Book_headings_heading_name_06[['001', 'PBL']].rename(columns={'PBL': 'Book.headings.heading.name'})
Book_headings_heading_name_06 = cSplit(Book_headings_heading_name_06, '001', 'Book.headings.heading.name', '❦').drop_duplicates().reset_index(drop=True)

mapowanie_655 = gsheet_to_df('1JQy98r4K7yTZixACxujH2kWY3D39rG1kFlIppNlZnzQ', 'deskryptory_655')
mapowanie_655 = mapowanie_655[mapowanie_655['decyzja'] == 'zmapowane'][['X655', 'dzial_PBL_1', 'dzial_PBL_2', 'dzial_PBL_3', 'dzial_PBL_4', 'dzial_PBL_5', 'haslo_przedmiotowe_PBL_1', 'haslo_przedmiotowe_PBL_2', 'haslo_przedmiotowe_PBL_3', 'haslo_przedmiotowe_PBL_4', 'haslo_przedmiotowe_PBL_5']].reset_index(drop=True)

mapowanie_655_BN_PBL = pd.DataFrame()
for i, row in mapowanie_655.iterrows():
    print(f"{i+1}/{len(mapowanie_655)}")
    mapowanie_655_BN_PBL.at[i, '655'] = row['X655']
    mapowanie_655_BN_PBL.at[i, 'PBL'] = '❦'.join([f for f in row.iloc[1:].to_list() if f])

Book_headings_heading_name_07 = df.copy()[['001', '655']].drop_duplicates().reset_index(drop=True)
Book_headings_heading_name_07 = cSplit(Book_headings_heading_name_07, '001', '655', '❦')
query = "select * from Book_headings_heading_name_07 a join mapowanie_655_BN_PBL b on a.'655' like '%'||b.'655'||'%'"
Book_headings_heading_name_07 = pandasql.sqldf(query)
Book_headings_heading_name_07 = Book_headings_heading_name_07[['001', 'PBL']].rename(columns={'PBL': 'Book.headings.heading.name'})
Book_headings_heading_name_07 = cSplit(Book_headings_heading_name_07, '001', 'Book.headings.heading.name', '❦').drop_duplicates().reset_index(drop=True)

Book_headings_heading_name_08 = marc_parser_1_field(df, '001', '730', '\$').drop_duplicates().reset_index(drop=True)
query = "select * from Book_headings_heading_name_08 a join dzialy_biblijne_PBL b on a.'$p' like '%'||b.'dział PBL'||'%'"
Book_headings_heading_name_08 = pandasql.sqldf(query)
Book_headings_heading_name_08 = Book_headings_heading_name_08[['001', 'dział PBL']].rename(columns={'dział PBL': 'Book.headings.heading.name'})

Book_headings_heading_name_09 = marc_parser_1_field(df, '001', '730', '\$').drop_duplicates().reset_index(drop=True)
Book_headings_heading_name_09 = Book_headings_heading_name_09[~Book_headings_heading_name_09['001'].isin(Book_headings_heading_name_08['001'])]
Book_headings_heading_name_09['Book.headings.heading.name'] = Book_headings_heading_name_09['$p'].apply(lambda x: dzial_biblijny(x))
Book_headings_heading_name_09 = Book_headings_heading_name_09[Book_headings_heading_name_09['Book.headings.heading.name'].notnull()][['001', 'Book.headings.heading.name']]

# teraz z 600 wydobywamy wszystkie osoby, docelowo - dział dostaną tylko ludzie PBL
Book_headings_heading_name_10 = marc_parser_1_field(df, '001', '600', '\$').drop_duplicates().reset_index(drop=True)[['001', '$a', '$b']]
Book_headings_heading_name_10['czy numeracja'] = Book_headings_heading_name_10['$b'].apply(lambda x: roman_numerals(x))
Book_headings_heading_name_10['$b'] = Book_headings_heading_name_10.apply(lambda x: '' if x['czy numeracja'] == False else x['$b'], axis=1)
Book_headings_heading_name_10 = Book_headings_heading_name_10.drop(columns='czy numeracja').replace(r'^\s*$', np.nan, regex=True)
Book_headings_heading_name_10['Book.headings.heading.name'] = Book_headings_heading_name_10[Book_headings_heading_name_10.columns[1:]].apply(lambda x: ' '.join(x.dropna().astype(str)), axis=1)
Book_headings_heading_name_10 = Book_headings_heading_name_10[['001', 'Book.headings.heading.name']]

# teraz z 610 wydobywamy wszystkie instytucje, docelowo - dział dostaną tylko instytucje PBL

Book_headings_heading_name_11 = marc_parser_1_field(df, '001', '610', '\$').drop_duplicates().reset_index(drop=True)[['001', '$a', '$b']].replace(r'^\s*$', np.nan, regex=True)
Book_headings_heading_name_11['Book.headings.heading.name'] = Book_headings_heading_name_11[Book_headings_heading_name_11.columns[1:]].apply(lambda x: ' '.join(x.dropna().astype(str)), axis=1)
Book_headings_heading_name_11 = Book_headings_heading_name_11[['001', 'Book.headings.heading.name']]

Book_headings_heading_name = pd.concat([Book_headings_heading_name_01, Book_headings_heading_name_02, Book_headings_heading_name_03, Book_headings_heading_name_04, Book_headings_heading_name_05, Book_headings_heading_name_06, Book_headings_heading_name_07, Book_headings_heading_name_08, Book_headings_heading_name_09, Book_headings_heading_name_10, Book_headings_heading_name_11]).drop_duplicates().reset_index(drop=True)

Book_headings_heading_name['Book.headings.heading.name'] = Book_headings_heading_name.groupby('001')['Book.headings.heading.name'].transform(lambda x: '❦'.join(x.drop_duplicates().astype(str)))
Book_headings_heading_name = Book_headings_heading_name.drop_duplicates().reset_index(drop=True)

book_headings_bible = pd.concat([Book_headings_heading_name_01, Book_headings_heading_name_02, Book_headings_heading_name_04, Book_headings_heading_name_05, Book_headings_heading_name_08, Book_headings_heading_name_09]).drop_duplicates().reset_index(drop=True)['001'].to_list()

#trzeba będzie będzie powiązać z konkretnymi zapisami PBL
Book_linkedCreativeWorks_titles_title_01 = marc_parser_1_field(df, '001', '130', '\$').drop_duplicates().reset_index(drop=True)
Book_linkedCreativeWorks_titles_title_01 = Book_linkedCreativeWorks_titles_title_01[~Book_linkedCreativeWorks_titles_title_01['001'].isin(book_headings_bible)][['001', '$a']].rename(columns={'$a': 'Book.linkedCreativeWorks.titles.title'})

Book_linkedCreativeWorks_titles_title_02 = marc_parser_1_field(df, '001', '630', '\$').drop_duplicates().reset_index(drop=True)
Book_linkedCreativeWorks_titles_title_02 = Book_linkedCreativeWorks_titles_title_02[~Book_linkedCreativeWorks_titles_title_02['001'].isin(book_headings_bible)][['001', '$a', '$n', '$p']].replace(r'^\s*$', np.nan, regex=True)
Book_linkedCreativeWorks_titles_title_02['Book.linkedCreativeWorks.titles.title'] = Book_linkedCreativeWorks_titles_title_02[Book_linkedCreativeWorks_titles_title_02.columns[1:]].apply(lambda x: ' '.join(x.dropna().astype(str)), axis=1)
Book_linkedCreativeWorks_titles_title_02 = Book_linkedCreativeWorks_titles_title_02[['001', 'Book.linkedCreativeWorks.titles.title']]

Book_linkedCreativeWorks_titles_title_03 = marc_parser_1_field(df, '001', '730', '\$').drop_duplicates().reset_index(drop=True)
Book_linkedCreativeWorks_titles_title_03 = Book_linkedCreativeWorks_titles_title_03[(~Book_linkedCreativeWorks_titles_title_03['001'].isin(book_headings_bible)) &
                                         (~Book_linkedCreativeWorks_titles_title_03['$a'].str.contains('Katalog wystawy|Program teatralny'))][['001', '$a', '$p']].replace(r'^\s*$', np.nan, regex=True)
Book_linkedCreativeWorks_titles_title_03['Book.linkedCreativeWorks.titles.title'] = Book_linkedCreativeWorks_titles_title_03[Book_linkedCreativeWorks_titles_title_03.columns[1:]].apply(lambda x: ' '.join(x.dropna().astype(str)), axis=1)
Book_linkedCreativeWorks_titles_title_03 = Book_linkedCreativeWorks_titles_title_03[['001', 'Book.linkedCreativeWorks.titles.title']]

Book_linkedCreativeWorks_titles_title = pd.concat([Book_linkedCreativeWorks_titles_title_01, Book_linkedCreativeWorks_titles_title_02, Book_linkedCreativeWorks_titles_title_03]).drop_duplicates().reset_index(drop=True)

Book_linkedCreativeWorks_titles_title['Book.linkedCreativeWorks.titles.title'] = Book_linkedCreativeWorks_titles_title.groupby('001')['Book.linkedCreativeWorks.titles.title'].transform(lambda x: '❦'.join(x.drop_duplicates().astype(str)))
Book_linkedCreativeWorks_titles_title = Book_linkedCreativeWorks_titles_title.drop_duplicates().reset_index(drop=True)

#są błędy, bo BN nie dał $a
Book_titles_title_original = marc_parser_1_field(df, '001', '240', '\$').drop_duplicates().reset_index(drop=True)
Book_titles_title_original = Book_titles_title_original[Book_titles_title_original['$i'].str.contains('oryg')][['001', '$a', '$b', '$p', '$n']].replace(r'^\s*$', np.nan, regex=True)  
Book_titles_title_original['$a'] = Book_titles_title_original[Book_titles_title_original.columns[1:3]].apply(lambda x: ' '.join(x.dropna().astype(str)), axis=1)
Book_titles_title_original['$p'] = Book_titles_title_original[Book_titles_title_original.columns[3:5]].apply(lambda x: ', '.join(x.dropna().astype(str)), axis=1)
Book_titles_title_original = Book_titles_title_original.replace(r'^\s*$', np.nan, regex=True)[['001', '$a', '$p']]
Book_titles_title_original = Book_titles_title_original[Book_titles_title_original['$a'].notnull()]
Book_titles_title_original['Book.titles.title'] = Book_titles_title_original[Book_titles_title_original.columns[1:]].apply(lambda x: ' '.join(x.dropna().astype(str)) if x['$a'][-1] in ['.', '!', '?'] else '. '.join(x.dropna().astype(str)), axis=1)
Book_titles_title_original = Book_titles_title_original[['001', 'Book.titles.title']].drop_duplicates().reset_index(drop=True)
Book_titles_title_original['Book.titles.type'] = 'tytuł oryginału'

#do ogarnięcia 245$c

Book_titles_title_basic = marc_parser_1_field(df, '001', '245', '\$').drop_duplicates().reset_index(drop=True).replace(' {0,}\/$', '', regex=True)[['001', '$a', '$b', '$p', '$n']].replace(r'^\s*$', np.nan, regex=True)  
Book_titles_title_basic['$a'] = Book_titles_title_basic[Book_titles_title_basic.columns[1:3]].apply(lambda x: ' '.join(x.dropna().astype(str)), axis=1)
Book_titles_title_basic['$p'] = Book_titles_title_basic[Book_titles_title_basic.columns[3:5]].apply(lambda x: ', '.join(x.dropna().astype(str)), axis=1)
Book_titles_title_basic = Book_titles_title_basic.replace(r'^\s*$', np.nan, regex=True)[['001', '$a', '$p']]
Book_titles_title_basic = Book_titles_title_basic[Book_titles_title_basic['$a'].notnull()]
Book_titles_title_basic['Book.titles.title'] = Book_titles_title_basic[Book_titles_title_basic.columns[1:]].apply(lambda x: ' '.join(x.dropna().astype(str)) if x['$a'][-1] in ['.', '!', '?'] else '. '.join(x.dropna().astype(str)), axis=1)
Book_titles_title_basic = Book_titles_title_basic[['001', 'Book.titles.title']].drop_duplicates().reset_index(drop=True)
Book_titles_title_basic['Book.titles.type'] = 'tytuł podstawowy'

Book_titles_title = pd.concat([Book_titles_title_basic, Book_titles_title_original])
Book_titles_title['Book.titles.type'] = Book_titles_title.groupby('001')['Book.titles.type'].transform(lambda x: '❦'.join(x.drop_duplicates().astype(str)))
Book_titles_title['Book.titles.type'] = Book_titles_title.groupby('001')['Book.titles.type'].transform(lambda x: '❦'.join(x.drop_duplicates().astype(str)))
Book_titles_title = Book_titles_title.drop_duplicates().reset_index(drop=True)

Book_edition = marc_parser_1_field(df, '001', '250', '\$').drop_duplicates().reset_index(drop=True).replace(' {0,}\/$', '', regex=True)[['001', '$a']].rename(columns={'$a': 'Book.edition'})

publication = df.copy()[['001', '264']]
publication = publication[publication['264'].notnull()].drop_duplicates().reset_index(drop=True)
publication['podpola'] = publication['264'].apply(lambda x: ''.join(re.findall('(?<=\$)[a,b]', x)))
publication['podpola'] = publication['podpola'].apply(lambda x: re.sub('([^a]*a[^a]+)', r'\1❦', x)).str.replace('^❦', '').str.replace('❦$', '')

def podpola_264(x):
    podpola = list(x['podpola'])
    podpole_indeks = 0
    val = []
    for podpole in podpola:
        try:
            podpole_indeks = x['264'].index(f"${podpole}", podpole_indeks)
            val.append(str(podpole_indeks))
        except ValueError:
            val.append('')
    val = [int(val[i+1]) if v == '' else v for i, v in enumerate(val)]
    ile_heder = 0
    for i, p in zip(val, podpola):
        if p == '❦':
            i += ile_heder
            x['264'] = x['264'][:i] + '❦' + x['264'][i:]
            ile_heder += 1
    return x['264']

publication['264'] = publication.apply(lambda x: podpola_264(x), axis=1)

publication = cSplit(publication, '001', '264', '❦')
publication['podpola'] = publication['264'].apply(lambda x: ''.join(re.findall('(?<=\$)[a,b]', x)))

def adres_wydawniczy(x):
    if x['podpola'].count('a') == 1 and x['podpola'].count('b') == 1:
        final_value = x['264']
    elif x['podpola'].count('a') == 1 and x['podpola'].count('b') > 1:
        grupa_a = re.findall('(\$a.*?)(?=\$b)', x['264'])[0]
        podpola_b = re.sub('([^b]*b)', r'❦\1', x['podpola'])
        podpola_b = list(re.sub('^❦', '', podpola_b))
        podpole_indeks = 0
        val = []
        for podpole in podpola_b:
            try:
                podpole_indeks = x['264'].index(f"${podpole}", podpole_indeks)
                val.append(str(podpole_indeks))
            except ValueError:
                val.append('')  
                podpole_indeks += 1
        val = [int(val[i+1]) if v == '' else int(v) for i, v in enumerate(val)]
        ile_heder = 0
        for i, p in zip(val, podpola_b):
            if p == '❦':
                i += ile_heder
                x['264'] = x['264'][:i] + '❦' + x['264'][i:]
                ile_heder += 1    
        final_value = x['264'].replace('❦', grupa_a)
    elif x['podpola'].count('a') > 1 and x['podpola'].count('b') == 1:
        final_value = re.sub('(\s+\W\$a)', ', ', x['264'])
    elif x['podpola'].count('a') > 1 and x['podpola'].count('b') > 1:
        x['264'] = re.sub('(\s+\W\$a)', ', ', x['264'])
        grupa_a = re.findall('(\$a.*?)(?=\$b)', x['264'])[0]
        podpola_b = re.sub('([^b]*b)', r'❦\1', x['podpola'])
        podpola_b = list(re.sub('^❦', '', podpola_b))
        podpole_indeks = 0
        val = []
        for podpole in podpola_b:
            try:
                podpole_indeks = x['264'].index(f"${podpole}", podpole_indeks)
                val.append(str(podpole_indeks))
            except ValueError:
                val.append('')  
                podpole_indeks += 1
        val = [int(val[i+1]) if v == '' else int(v) for i, v in enumerate(val)]
        ile_heder = 0
        for i, p in zip(val, podpola_b):
            if p == '❦':
                i += ile_heder
                x['264'] = x['264'][:i] + '❦' + x['264'][i:]
                ile_heder += 1    
        final_value = x['264'].replace('❦', grupa_a)
    else:
        final_value = x['264']
    return final_value
                
publication['264'] = publication.apply(lambda x: adres_wydawniczy(x), axis=1)
publication['264'] = publication['264'].str.replace('(\s+\W)(\$a)', r'\1❦\2', regex=True)
publication['index'] = publication.index+1
publication = cSplit(publication, 'index', '264', '❦')

publication_parsed = marc_parser_1_field(publication, 'index', '264', '\$').replace(' {0,}[:;]$', '', regex=True)
publication_parsed = pd.merge(publication_parsed, publication[['index', '001']], how='left', on='index').drop_duplicates().reset_index(drop=True)

Book_publishingHouses_places_name = publication_parsed[['001', '$a']].rename(columns={'$a': 'Book.publishingHouses.places.name'})
Book_publishingHouses_places_name['Book.publishingHouses.places.name'] = Book_publishingHouses_places_name.groupby('001')['Book.publishingHouses.places.name'].transform(lambda x: '❦'.join(x.astype(str)))
Book_publishingHouses_places_name = Book_publishingHouses_places_name.drop_duplicates().reset_index(drop=True)

Book_publishingHouses_institution_history_names_name = publication_parsed[['001', '$b']].rename(columns={'$b': 'Book.publishingHouses.institution.history.names.name'})
Book_publishingHouses_institution_history_names_name['Book.publishingHouses.institution.history.names.name'] = Book_publishingHouses_institution_history_names_name.groupby('001')['Book.publishingHouses.institution.history.names.name'].transform(lambda x: '❦'.join(x.astype(str)))
Book_publishingHouses_institution_history_names_name = Book_publishingHouses_institution_history_names_name.drop_duplicates().reset_index(drop=True)

Book_publicationYear_year = publication_parsed[['001', '$c']].rename(columns={'$c': 'Book.publicationYear.year'})
Book_publicationYear_year = Book_publicationYear_year[Book_publicationYear_year['Book.publicationYear.year'] != ''].drop_duplicates().reset_index(drop=True).replace('\.$', '', regex=True)
Book_publicationYear_year['Book.publicationYear.yearType.name'] = Book_publicationYear_year['Book.publicationYear.year'].apply(lambda x: 'rok copyright' if x[:4] in ['cop.', '[cop'] else 'rok wydania')

# except, bo w pewnym miejscu są "Katowice" - jak reagować na błędy BN?
def publication_year(x):
    try:
        val = re.findall('\d+(?!.*\d+)', x)[0]
    except IndexError:
        val = np.nan
    return val

Book_publicationYear_year['Book.publicationYear.year'] = Book_publicationYear_year['Book.publicationYear.year'].apply(lambda x: publication_year(x))
Book_publicationYear_year = Book_publicationYear_year[Book_publicationYear_year['Book.publicationYear.year'].notnull()] 

# teraz Book.physicalDescription.description

Book_physicalDescription_description_01 = marc_parser_1_field(df, '001', '300', '\$').drop_duplicates().reset_index(drop=True)[['001', '$a', '$b', '$e']].replace(r'^\s*$', np.nan, regex=True).replace('\s+[;:]$', '', regex=True)
Book_physicalDescription_description_01['Book.physicalDescription.description'] = Book_physicalDescription_description_01[Book_physicalDescription_description_01.columns[1:]].apply(lambda x: ', '.join(x.dropna().astype(str)), axis=1)
Book_physicalDescription_description_01 = Book_physicalDescription_description_01[['001', 'Book.physicalDescription.description']]

#wybrać tylko te frazy, które mają pseudonim, resztę wyrzucić

Book_physicalDescription_description_02 = marc_parser_1_field(df, '001', '500', '\$').drop_duplicates().reset_index(drop=True)[['001', '$a']].replace(r'^\s*$', np.nan, regex=True).replace('\s+[;:]$', '', regex=True)
Book_physicalDescription_description_02 = Book_physicalDescription_description_02[(Book_physicalDescription_description_02['$a'].str.contains('pseud')) &
                                        (Book_physicalDescription_description_02['$a'].notnull())].rename(columns={'$a': 'Book.physicalDescription.description'})

Book_physicalDescription_description = pd.concat([Book_physicalDescription_description_01, Book_physicalDescription_description_02])
Book_physicalDescription_description['Book.physicalDescription.description'] = Book_physicalDescription_description.groupby('001')['Book.physicalDescription.description'].transform(lambda x: ', '.join(x.drop_duplicates().astype(str)))
Book_physicalDescription_description = Book_physicalDescription_description.drop_duplicates().reset_index(drop=True)

# ogarnąć później 045, 388 i 648

Book_belongingToSeries_series_principalTitle_01 = marc_parser_1_field(df, '001', '490', '\$').drop_duplicates().reset_index(drop=True).replace(' {0,}\/$', '', regex=True)[['001', '$a', '$b', '$p', '$v']].replace(r'^\s*$', np.nan, regex=True).replace('\s+[\/].*?$', '', regex=True).replace('\s+[;:]$', '', regex=True)
Book_belongingToSeries_series_principalTitle_01['Book.belongingToSeries.series.principalTitle'] = Book_belongingToSeries_series_principalTitle_01[['$a', '$b']].apply(lambda x: ', '.join(x.dropna().astype(str)), axis=1)
    
Book_belongingToSeries_series_principalTitle_02 = marc_parser_1_field(df, '001', '800', '\$').drop_duplicates().reset_index(drop=True).replace(' {0,}\/$', '', regex=True)[['001', '$t', '$v']].replace(r'^\s*$', np.nan, regex=True).replace('\s+[\/].*?$', '', regex=True).replace('\s+[;:]$', '', regex=True)
Book_belongingToSeries_series_principalTitle_02 = Book_belongingToSeries_series_principalTitle_02[~Book_belongingToSeries_series_principalTitle_02['001'].isin(Book_belongingToSeries_series_principalTitle_01['001'])]
Book_belongingToSeries_series_principalTitle_02['Book.belongingToSeries.series.principalTitle'] = Book_belongingToSeries_series_principalTitle_02['$t']

Book_belongingToSeries_series_principalTitle_03 = marc_parser_1_field(df, '001', '830', '\$').drop_duplicates().reset_index(drop=True).replace(' {0,}\/$', '', regex=True)[['001', '$a', '$p', '$v']].replace(r'^\s*$', np.nan, regex=True).replace('\s+[\/].*?$', '', regex=True).replace('\s+[;:]$', '', regex=True)
Book_belongingToSeries_series_principalTitle_03 = Book_belongingToSeries_series_principalTitle_03[(~Book_belongingToSeries_series_principalTitle_03['001'].isin(Book_belongingToSeries_series_principalTitle_01['001'])) &
                                                (~Book_belongingToSeries_series_principalTitle_03['001'].isin(Book_belongingToSeries_series_principalTitle_02['001']))]
Book_belongingToSeries_series_principalTitle_03['Book.belongingToSeries.series.principalTitle'] = Book_belongingToSeries_series_principalTitle_03['$a']

Book_belongingToSeries_series_principalTitle = pd.concat([Book_belongingToSeries_series_principalTitle_01[['001', 'Book.belongingToSeries.series.principalTitle']], Book_belongingToSeries_series_principalTitle_02[['001', 'Book.belongingToSeries.series.principalTitle']], Book_belongingToSeries_series_principalTitle_03[['001', 'Book.belongingToSeries.series.principalTitle']]])

Book_belongingToSeries_seriesSubtitle = pd.concat([Book_belongingToSeries_series_principalTitle_01[['001', '$p']], Book_belongingToSeries_series_principalTitle_03[['001', '$p']]]).rename(columns={'$p': 'Book_belongingToSeries_seriesSubtitle'})
Book_belongingToSeries_seriesSubtitle = Book_belongingToSeries_seriesSubtitle[Book_belongingToSeries_seriesSubtitle['Book_belongingToSeries_seriesSubtitle'].notnull()]

Book_belongingToSeries_seriesNumber = pd.concat([Book_belongingToSeries_series_principalTitle_01[['001', '$v']], Book_belongingToSeries_series_principalTitle_02[['001', '$v']], Book_belongingToSeries_series_principalTitle_03[['001', '$v']]]).rename(columns={'$v': 'Book.belongingToSeries.seriesNumber'})
Book_belongingToSeries_seriesNumber = Book_belongingToSeries_seriesNumber[Book_belongingToSeries_seriesNumber['Book.belongingToSeries.seriesNumber'].notnull()]

#czy można to inaczej zrobić (parsować)?

Book_annotations_01 = df[['001', '505']]
Book_annotations_01 = Book_annotations_01[Book_annotations_01['505'].notnull()].rename(columns={'505': 'Book.annotations'})
Book_annotations_01['Book.annotations'] = Book_annotations_01['Book.annotations'].str.replace('^..', '', regex=True).str.replace('\$.', '', regex=True)

Book_annotations_02 = marc_parser_1_field(df, '001', '700', '\$')[['001', '$a', '$t']].replace(r'^\s*$', np.nan, regex=True)
Book_annotations_02 = Book_annotations_02[Book_annotations_02['$t'].notnull()].drop_duplicates().reset_index(drop=True)
Book_annotations_02['$a'] = Book_annotations_02['$a'].apply(lambda x: re.sub('(?<=\p{Ll})\.$', '', x))
Book_annotations_02['Book.annotations'] = Book_annotations_02[['$a', '$t']].apply(lambda x: ': '.join(x.dropna().astype(str)), axis=1)
Book_annotations_02 = Book_annotations_02[['001', 'Book.annotations']]

# co zrobić ze zdublowanymi danymi w polach 505 i 700?

Book_annotations = pd.concat([Book_annotations_01, Book_annotations_02])
Book_annotations['Book.annotations'] = Book_annotations.groupby('001')['Book.annotations'].transform(lambda x: '. '.join(x.drop_duplicates().astype(str))).str.replace('\.{2}', '.', regex=True)
Book_annotations = Book_annotations.drop_duplicates().reset_index(drop=True)

# nie idzie w BN znaleźć kategorii dla 651a, więc nie wiadomo, kiedy miasto, kiedy państwo - do podziału i przypisania na później

bn_700 = marc_parser_1_field(df, '001', '700', '\$').drop_duplicates().reset_index(drop=True)
bn_700 = bn_700[(~bn_700['001'].isin(Book_authors_person_names_name_02['001'])) &
                (~bn_700['001'].isin(Book_annotations_02['001']))]
bn_700['index'] = bn_700.index+1
bn_700 = cSplit(bn_700, 'index', '$e', '❦')

MARC_wspoltworcy = gsheet_to_df('1FlDXlbPmVfRXu382QxqJ63FnHWlXx91hvv5ztcl0dpA', 'Arkusz1')
MARC_wspoltworcy['index'] = MARC_wspoltworcy.index+1
MARC_wspoltworcy['BN'] = MARC_wspoltworcy[['rodzaje współpracy', 'określenie współpracy', 'BN skrót']].apply(lambda x: '❦'.join(x.drop_duplicates().dropna().astype(str)), axis=1)
MARC_wspoltworcy = cSplit(MARC_wspoltworcy[['index', 'PBL java', 'PBL funkcja', 'BN']], 'index', 'BN', '❦').drop(columns='index')

query = "select * from bn_700 a join MARC_wspoltworcy b on a.'$e' like '%'||b.'BN'||'%'"
bn_700 = pandasql.sqldf(query).drop(columns='BN').drop_duplicates().reset_index(drop=True).replace(r'^\s*$', np.nan, regex=True)

bn_700['PBL funkcja'] = bn_700['PBL funkcja'].apply(lambda x: 'inni' if pd.isna(x) else x)
bn_700['$a'] = bn_700[['$a', '$b']].apply(lambda x: ' '.join(x.dropna().astype(str)), axis=1)
bn_700 = bn_700[['001', '$a', 'PBL java', 'PBL funkcja']].rename(columns={'PBL funkcja': 'Book.coCreations.coCreationTypes'})

for i, row in bn_700.iterrows():
    bn_700.at[i, row['PBL java']] = row['$a']
    
Book_mailingParts_addressee_authors_person_names_name = bn_700[bn_700['Book.mailingParts.addressee.authors.person.names.name'].notnull()][['001', 'Book.mailingParts.addressee.authors.person.names.name']]

# ogarnąć podział między pytającym i odpowiadającym w wywiadzie (vide: b0000005481278 i Olga Tokarczuk z dwoma funkcjami)

Book_interviewParts_interviewer_authors_person_names_name = bn_700[bn_700['Book.interviewParts.interviewer.authors.person.names.name'].notnull()][['001', 'Book.interviewParts.interviewer.authors.person.names.name']]

Book_interviewParts_interlocutor_authors_person_names_name = bn_700[bn_700['Book.interviewParts.interlocutor.authors.person.names.name'].notnull()][['001', 'Book.interviewParts.interlocutor.authors.person.names.name']]

Book_coCreations_coCreators = bn_700[bn_700['Book.coCreations.coCreators'].notnull()][['001', 'Book.coCreations.coCreators', 'Book.coCreations.coCreationTypes']]

#na później
# Book_linkedRecords = marc_parser_1_field(df, '001', '787', '\$').drop_duplicates().reset_index(drop=True)

# na później - dla książek nie mamy odnośników do pełnych tekstów
# Book_externalLinks_url = marc_parser_1_field(df, '001', '856', '\$').drop_duplicates().reset_index(drop=True)

dfs = [book_id,
       Book_publishingHouses_places_country_name,
       Book_recordTypes,
       Book_languages,
       Book_titles_language,
       Book_remarks,
       Book_authors_person_names_name,
       Book_triggerInstitutions_history_names_name,
       Book_linkedEvents_names_name,
       Book_headings_heading_name,
       Book_linkedCreativeWorks_titles_title,
       Book_titles_title,
       Book_edition,
       Book_publishingHouses_places_name,
       Book_publishingHouses_institution_history_names_name,
       Book_publicationYear_year,
       Book_physicalDescription_description,
       Book_belongingToSeries_series_principalTitle,
       Book_belongingToSeries_seriesSubtitle,
       Book_belongingToSeries_seriesNumber,
       Book_annotations,
       Book_mailingParts_addressee_authors_person_names_name,
       Book_interviewParts_interviewer_authors_person_names_name,
       Book_interviewParts_interlocutor_authors_person_names_name,
       Book_coCreations_coCreators]

pbl_ibl_waw_pl = reduce(lambda left,right: pd.merge(left,right,on='001', how='outer'), dfs)
  
pbl_ibl_waw_pl.to_excel('pbl_ibl_waw_pl.xlsx', index=False)





pbl_marc_books['LDR'] = LDR
pbl_marc_books['040'] = X040
del pbl_marc_books['rekord_id']
columns_list = pbl_marc_books.columns.tolist()
columns_list.sort(key = lambda x: ([str,int].index(type("a" if re.findall(r'\w+', x)[0].isalpha() else 1)), x))
pbl_marc_books = pbl_marc_books.reindex(columns=columns_list)

pbl_marc_books.to_excel('pbl_marc_books.xlsx', index=False)








# =============================================================================
# places_651 = marc_parser_1_field(df, '001', '651', '\$')['$a'].drop_duplicates().to_list()
# 
# ns = '{http://www.loc.gov/MARC21/slim}'
# total_df = pd.DataFrame()
# for i, nazwa in enumerate(places_651):
#     print(f"{i+1}/{len(places_651)}")
#     url = f"https://data.bn.org.pl/api/authorities.xml?jurisdiction=Wsie%7CMiasta%7CPa%C5%84stwa&limit=100&boolean=true&marc=151a+{nazwa}"
#     response = requests.get(url)
#     with open('test.xml', 'wb') as file:
#         file.write(response.content)
#     tree = et.parse('test.xml')
#     root = tree.getroot()
#     place_id = [t.text for t in root.findall('.//id')]
#     subject = [t.text for t in root.findall('.//subject')]
#     jurisdiction = [t.text for t in root.findall('.//jurisdiction')]
#     pole_151 = [t for t in root.findall(f'.//{ns}datafield') if t.attrib['tag'] == '151']
#     place = []
#     for x in pole_151:
#         x = x.getchildren()
#         for y in x:
#             place.append(y.text)
#     total_data = list(zip(place_id, subject, jurisdiction, place))
#     total_data = [list(t) for t in total_data]
#     for l in total_data:
#         l.insert(0, nazwa)
#     df_iter = pd.DataFrame(total_data, columns=['search name', 'id', 'BN name', 'category', '151'])
#     total_df = total_df.append(df_iter)    
# =============================================================================


























