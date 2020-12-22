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
import roman

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
Book_recordTypes['Book.recordTypes'] = Book_recordTypes.groupby('001')['Book_recordTypes'].transform(lambda x: '❦'.join(x.drop_duplicates().astype(str)))
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

Book_authors_person_names_name_02 = Book_authors_person_names_name_02[(Book_authors_person_names_name_02['czy numeracja'] == True) | (Book_authors_person_names_name_02['$b'] == '')].drop(columns='czy numeracja')
Book_authors_person_names_name_02 = Book_authors_person_names_name_02.replace(r'^\s*$', np.nan, regex=True)
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

# do końca Book.headings.heading.name brakuje jeszcze 730


































