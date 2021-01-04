# przygotowanie książek do importu
import pandas as pd
from my_functions import marc_parser_1_field, unique_elem_from_column_split, cSplit, replacenth, gsheet_to_df, df_to_gsheet, get_cosine_result
import regex as re
from functools import reduce
import numpy as np
import copy
import requests
from bs4 import BeautifulSoup
from SPARQLWrapper import SPARQLWrapper, JSON
from urllib.error import HTTPError
from http.client import RemoteDisconnected
import time
import xml.etree.ElementTree as et

bn_books = pd.read_csv("F:/Cezary/Documents/IBL/Samizdat/bn_books.csv", sep=';')
bn_books.iloc[142, 18] = '%fWspomnienia 1939-1945 : (fragmenty) %bPużak Kazimierz %ss. 58-130 %z1|%fPolitycy i żołnierze %bGarliński Józef %ss. 131-147 %z2|%aZawiera : "Proces szesnastu" w Moskwie : (wspomnienia osobiste) / K.      Bagiński. Wspomnienia 1939-1945 : (fragmenty) / K. Pużak. Politycy i      żołnierze / J. Garliński %bBagiński Kazimierz %f"Proces szesnastu" w      Moskwie %ss. 3-57 %z0'
cz_books = pd.read_csv("F:/Cezary/Documents/IBL/Samizdat/cz_books.csv", sep=';')
cz_articles = pd.read_csv("F:/Cezary/Documents/IBL/Samizdat/cz_articles.csv", sep=';')
pbl_books = pd.read_csv("F:/Cezary/Documents/IBL/Samizdat/pbl_books.csv", sep=';')
pbl_articles = pd.read_csv("F:/Cezary/Documents/IBL/Samizdat/pbl_articles.csv", sep=';')


# kartoteka intytucji
# bn books
#110
institutions_110 = marc_parser_1_field(bn_books, 'id', 'X110', '%')[['id', '%1', '%2', '%6']]
institutions_110.columns = [['id', 'Entity_Name', 'Related_Entity_Sub_Entity', 'Located_Location']]
institutions_110['MRC'] = '110'
institutions_110['subfield'] = '%1'
institutions_110['Related_Entity_Main_Entity'] = np.nan
sub_institutions_110 = marc_parser_1_field(bn_books, 'id', 'X110', '%')[['id', '%2', '%1', '%7']]
sub_institutions_110 = sub_institutions_110.loc[sub_institutions_110['%2'] != ""]
sub_institutions_110.columns = [['id', 'Entity_Name', 'Related_Entity_Main_Entity', 'Located_Location']]
sub_institutions_110['MRC'] = '110'
sub_institutions_110['subfield'] = '%2'
sub_institutions_110['Related_Entity_Sub_Entity'] = np.nan
bn_institutions110 = pd.concat([institutions_110, sub_institutions_110], axis = 0)
#120
institutions_120 = marc_parser_1_field(bn_books, 'id', 'X120', '%')[['id', '%1', '%2']]
institutions_120.columns = [['id', 'Entity_Name', 'Related_Entity_Sub_Entity']]
institutions_120['MRC'] = '120'
institutions_120['subfield'] = '%1'
institutions_120['Related_Entity_Main_Entity'] = np.nan
institutions_120['Located_Location'] = np.nan
sub_institutions_120 = marc_parser_1_field(bn_books, 'id', 'X120', '%')[['id', '%2', '%1']]
sub_institutions_120 = sub_institutions_120.loc[sub_institutions_120['%2'] != ""]
sub_institutions_120.columns = [['id', 'Entity_Name', 'Related_Entity_Main_Entity']]
sub_institutions_120['MRC'] = '120'
sub_institutions_120['subfield'] = '%2'
sub_institutions_120['Related_Entity_Sub_Entity'] = np.nan
sub_institutions_120['Located_Location'] = np.nan
bn_institutions120 = pd.concat([institutions_120, sub_institutions_120], axis = 0)
#210
X210 = bn_books[['id', 'X210']].dropna()
X210['rok_wydania'] = X210['X210'].str.extract(r'(?<=\%d)(.*?)(?=\%e|$)')
X210['bez_roku'] = X210['X210'].str.replace(r'.\%d.*', "")
X210['ile_wydawnictw'] = X210['bez_roku'].str.count(r'\%c')
X210['ile_miejsc'] = X210['bez_roku'].str.count(r'\%a')
X210['kolejnosc'] = X210['bez_roku'].str.findall(r'(?<=\%)(.)').str.join("")
X210['lista'] = X210['bez_roku'].str.split(r' (?=\%)')

def kolejnosc(row, kolumna_dane, kolumna_kolejnosc):
    if row[kolumna_kolejnosc] == "acc":
        order = [0,1,0,2]
        row[kolumna_dane] = [row[kolumna_dane][i] for i in order]
        return row[kolumna_dane]
    elif row[kolumna_kolejnosc] == "aac":
        row[kolumna_dane][0:2] = [', '.join(row[kolumna_dane][0:2])]
        return row[kolumna_dane]
    elif row[kolumna_kolejnosc] == "acacc":
        order = [0, 1, 2, 3, 2, 4]
        row[kolumna_dane] = [row[kolumna_dane][i] for i in order]
        return row[kolumna_dane]
    elif row[kolumna_kolejnosc] == "aacc":
        row[kolumna_dane][0:2] = [', '.join(row[kolumna_dane][0:2])]
        order = [0, 1, 0, 2]
        row[kolumna_dane] = [row[kolumna_dane][i] for i in order]
        return row[kolumna_dane]
    else:
        return row[kolumna_dane]
    
X210['dobre'] = X210.apply(lambda x: kolejnosc(x, 'lista', 'kolejnosc'), axis = 1)
X210['dobre'] = X210['dobre'].apply(lambda x: [replacenth(i, '%a', '', 2) if i.count('%a') > 1 else i for i in x]).str.join(" ").str.replace(r'(?<!^)(\%a)', r'|\1', regex=True)
X210 = cSplit(X210[['id', 'dobre']], 'id', 'dobre', '|')
X210['dobre'] = X210['dobre'].str.strip().str.replace(' +', ' ')

institutions_210 = marc_parser_1_field(X210, 'id', 'dobre', '%')[['id', '%c', '%a']]
institutions_210.columns = [['id', 'Entity_Name', 'Located_Location']]
institutions_210['MRC'] = '210'
institutions_210['subfield'] = '%c'
institutions_210['Related_Entity_Main_Entity'] = np.nan
institutions_210['Related_Entity_Sub_Entity'] = np.nan
sub_institutions_210 = marc_parser_1_field(bn_books, 'id', 'X210', '%')[['id', '%g', '%e']]
sub_institutions_210 = sub_institutions_210.loc[sub_institutions_210['%g'] != ""]
sub_institutions_210.columns = [['id', 'Entity_Name', 'Located_Location']]
sub_institutions_210['MRC'] = '210'
sub_institutions_210['subfield'] = '%g'
sub_institutions_210['Related_Entity_Sub_Entity'] = np.nan
sub_institutions_210['Related_Entity_Main_Entity'] = np.nan
bn_institutions210 = pd.concat([institutions_210, sub_institutions_210], axis = 0)   
#225
institutions_225 = marc_parser_1_field(bn_books, 'id', 'X225', '%')[['id', '%f']]
institutions_225 = institutions_225.loc[institutions_225['%f'] != ""]
institutions_225.columns = [['id', 'Entity_Name']]
institutions_225['MRC'] = '225'
institutions_225['subfield'] = '%f'
institutions_225['Related_Entity_Main_Entity'] = np.nan
institutions_225['Related_Entity_Sub_Entity'] = np.nan
institutions_225['Located_Location'] = np.nan
#710
institutions_710 = marc_parser_1_field(bn_books, 'id', 'X710', '%')[['id', '%1']]
institutions_710.columns = [['id', 'Entity_Name']]
institutions_710['MRC'] = '710'
institutions_710['subfield'] = '%1'
institutions_710['Related_Entity_Main_Entity'] = np.nan
institutions_710['Related_Entity_Sub_Entity'] = np.nan
institutions_710['Located_Location'] = np.nan
#711
institutions_711 = marc_parser_1_field(bn_books, 'id', 'X711', '%')[['id', '%1']]
institutions_711.columns = [['id', 'Entity_Name']]
institutions_711['MRC'] = '710'
institutions_711['subfield'] = '%1'
institutions_711['Related_Entity_Main_Entity'] = np.nan
institutions_711['Related_Entity_Sub_Entity'] = np.nan
institutions_711['Located_Location'] = np.nan

bn_book_institutions = pd.concat([bn_institutions110, bn_institutions120, bn_institutions210, institutions_225, institutions_710, institutions_711])
bn_book_institutions['source'] = "bn_books"
bn_book_institutions.columns = bn_book_institutions.columns.get_level_values(0)
#cz
cz_books['source'] = "cz_books"
cz_articles['source'] = "cz_articles"
cz_set = pd.concat([cz_books, cz_articles], axis=0, ignore_index = True)[['id', 'X110', 'X264', 'X610', 'X710', 'source']]
#110
institutions_110 = marc_parser_1_field(cz_set, 'id', 'X110', '$\$')[['id', '$$a']]
institutions_110.columns = [['id', 'Entity_Name']]
institutions_110['MRC'] = '110'
institutions_110['subfield'] = '$$a'
institutions_110['Related_Entity_Main_Entity'] = np.nan
institutions_110['Related_Entity_Sub_Entity'] = np.nan
institutions_110['Located_Location'] = np.nan
#264
institutions_264 = marc_parser_1_field(cz_set, 'id', 'X264', '$\$')[['id', '$$b', '$$a']]
institutions_264.columns = [['id', 'Entity_Name', "Located_Location"]]
institutions_264['MRC'] = '264'
institutions_264['subfield'] = '$$b'
institutions_264['Related_Entity_Main_Entity'] = np.nan
institutions_264['Related_Entity_Sub_Entity'] = np.nan
#610
institutions_610 = marc_parser_1_field(cz_set, 'id', 'X610', '$\$')[['id', '$$a']]
institutions_610.columns = [['id', 'Entity_Name']]
institutions_610['MRC'] = '610'
institutions_610['subfield'] = '$$a'
institutions_610['Related_Entity_Main_Entity'] = np.nan
institutions_610['Related_Entity_Sub_Entity'] = np.nan
institutions_610['Located_Location'] = np.nan
#710
institutions_710 = marc_parser_1_field(cz_set, 'id', 'X710', '$\$')[['id', '$$a']]
institutions_710.columns = [['id', 'Entity_Name']]
institutions_710['MRC'] = '710'
institutions_710['subfield'] = '$$a'
institutions_710['Related_Entity_Main_Entity'] = np.nan
institutions_710['Related_Entity_Sub_Entity'] = np.nan
institutions_710['Located_Location'] = np.nan
cz_institutions = pd.concat([institutions_110, institutions_264, institutions_610, institutions_710])
cz_institutions.columns = cz_institutions.columns.get_level_values(0)
cz_set.columns = cz_set.columns.get_level_values(0)
cz_institutions = pd.merge(cz_institutions, cz_set[['id', 'source']],  how='left', left_on = 'id', right_on = 'id')
#pbl
# pbl books
#publishing house
pbl_books_publishing_house = pbl_books[['rekord_id', 'wydawnictwo', 'miejscowosc']].drop_duplicates()
pbl_books_publishing_house.columns = [['id', 'Entity_Name', 'Located_Location']]
pbl_books_publishing_house['MRC'] = np.nan
pbl_books_publishing_house['subfield'] = 'publishing_house'
pbl_books_publishing_house['Related_Entity_Main_Entity'] = np.nan
pbl_books_publishing_house['Related_Entity_Sub_Entity'] = np.nan
pbl_books_publishing_house['source'] = 'pbl_books'
# pbl articles
#subject headings
pbl_articles_institutions = pbl_articles[['rekord_id', 'HP_NAZWA', 'KH_NAZWA']].drop_duplicates()
pbl_articles_institutions = pbl_articles_institutions[pbl_articles_institutions['HP_NAZWA'].isin(["Biblioteki","Filmowe instytucje, kluby, przedsiębiorstwa","Fundacje, fundusze kulturalne, stypendia","Grupy literackie i artystyczne","Instytucje kulturalne, państwowe, społeczne obce","Instytucje kulturalne, państwowe, społeczne polskie","Instytuty pozauczelniane, komitety i towarzystwa naukowe w Polsce","Instytuty pozauczelniane, komitety i towarzystwa naukowe za granicą","Muzea obce","Muzea polskie","Teatry obce","Teatry polskie historia","Teatry polskie współczesne","Uczelniane instytuty (wydziały, zakłady, ośrodki badawcze) obce","Uczelniane instytuty (wydziały, zakłady, ośrodki badawcze) polskie","Uniwersytety, szkoły wyższe i inne uczelnie obce","Uniwersytety, szkoły wyższe i inne uczelnie polskie","Wydawnictwa polskie do 1945 roku","Wydawnictwa polskie po 1945 roku","Związki, kluby, koła, stowarzyszenia twórcze obce","Związki, kluby, koła, stowarzyszenia twórcze polskie"])][['rekord_id', 'KH_NAZWA']]
pbl_articles_institutions.columns = [['id', 'Entity_Name']]
pbl_articles_institutions['MRC'] = np.nan
pbl_articles_institutions['subfield'] = 'subject_heading'
pbl_articles_institutions['Related_Entity_Main_Entity'] = np.nan
pbl_articles_institutions['Related_Entity_Sub_Entity'] = np.nan
pbl_articles_institutions['Located_Location'] = np.nan
pbl_articles_institutions['source'] = 'pbl_articles'
pbl_institutions = pd.concat([pbl_books_publishing_house, pbl_articles_institutions])
pbl_institutions.columns = pbl_institutions.columns.get_level_values(0)
#merge all institutions
samizdat_institutions = pd.concat([bn_book_institutions, cz_institutions, pbl_institutions])
samizdat_institutions['Grouped'] = samizdat_institutions[['source', 'MRC', 'subfield', 'id']].apply(lambda x: '-'.join(x.astype(str)), axis = 1)
samizdat_institutions = samizdat_institutions[['Entity_Name', 'Related_Entity_Sub_Entity', 'Related_Entity_Main_Entity', 'Located_Location', 'Grouped']].sort_values(['Entity_Name', 'Related_Entity_Sub_Entity', 'Related_Entity_Main_Entity'])
samizdat_institutions['simple_name_loc'] = samizdat_institutions[['Entity_Name', 'Located_Location']].apply(lambda x: '|'.join(x.dropna().astype(str).str.lower().str.replace('\W', '')), axis = 1)

for column in samizdat_institutions.iloc[:,:5]:
    samizdat_institutions[column] = samizdat_institutions.groupby('simple_name_loc')[column].transform(lambda x: '|'.join(x.drop_duplicates().astype(str)))

samizdat_institutions = samizdat_institutions.drop_duplicates().sort_values(['simple_name_loc', 'Entity_Name', 'Related_Entity_Sub_Entity', 'Related_Entity_Main_Entity']).reset_index(drop = True)
samizdat_institutions['group_id'] = samizdat_institutions.index + 1
samizdat_institutions = samizdat_institutions.replace('nan', '')
samizdat_institutions.to_excel('C:/Users/Cezary/Desktop/samizdat_instytucje.xlsx', index = False)


# bn books
# object
title = marc_parser_1_field(bn_books, 'id', 'X200', '%')[['id', '%a', '%e']]
variant_title = marc_parser_1_field(bn_books, 'id', 'X200', '%')[['id', '%d', '%r']]
place_of_publication = marc_parser_1_field(bn_books, 'id', 'X210', '%')[['id', '%a']]
year_of_publication = marc_parser_1_field(bn_books, 'id', 'X008', '%')[['id', '%m']]
year_of_publication_de_facto = marc_parser_1_field(bn_books, 'id', 'X008', '%')[['id', '%n']]
responsibility_as_in_source = marc_parser_1_field(bn_books, 'id', 'X201', '%')[['id', '%f', '%g']]
responsibility_as_in_source['responsibility'] = responsibility_as_in_source[responsibility_as_in_source.columns[1:]].apply(
    lambda x: ', '.join(x.dropna().astype(str)),
    axis=1
)
responsibility_as_in_source = responsibility_as_in_source.drop(['%f', '%g'], axis = 1)
physical_description_pages = marc_parser_1_field(bn_books, 'id', 'X215', '%')[['id', '%a']]
physical_description_format = marc_parser_1_field(bn_books, 'id', 'X215', '%')[['id', '%c']]
# potrzeba klasyfikacji
physical_description_elements = marc_parser_1_field(bn_books, 'id', 'X215', '%')[['id', '%b']]

# klasyfikacja
elementy = unique_elem_from_column_split(physical_description_elements, '%b', ', ')

for index, element in enumerate(elementy):
    if element.count('(') == 0:
        elementy[index] = re.sub(r' \d$|\)$', '', element)
    else:
        elementy[index] = re.sub(r' \d$', '', element)
for index, element in enumerate(elementy):
    elementy[index] = re.sub(r'^(\d+ |\[\d+\] )', '', element)
        
elementy = list(set(elementy))
#koniec klasyfikacji

physical_description_elements['elementy'] = physical_description_elements['%b'].apply(lambda x: '' if x == '' else x.split(', '))

#physical_description_elements.loc[physical_description_elements['%b'].str.contains('\)$', regex = True)]

for lista in physical_description_elements['elementy']:
    if len(lista) > 0:
        for i, element in enumerate(lista):
            if element.count('(') == 0:
                lista[i] = re.sub(r' \d$|\)$', '', element)
            else:
                lista[i] = re.sub(r' \d$', '', element)
for lista in physical_description_elements['elementy']:
    if len(lista) > 0:
        for i, element in enumerate(lista):
            lista[i] = re.sub(r'^(\d+ |\[\d+\] )', '', element)
            
physical_description_elements = physical_description_elements[['id', 'elementy']]

physical_description_notes1 = marc_parser_1_field(bn_books, 'id', 'X215', '%')[['id', '%d']]
physical_description_notes2 = marc_parser_1_field(bn_books, 'id', 'X325', '%')[['id', '%a']]   
physical_description_notes = pd.merge(physical_description_notes1, physical_description_notes2,  how='outer', left_on = 'id', right_on = 'id')         

price = marc_parser_1_field(bn_books, 'id', 'X230', '%')[['id', '%c']]
edition_notes = marc_parser_1_field(bn_books, 'id', 'X311', '%')[['id', '%a']]
circulation_notes1 = marc_parser_1_field(bn_books, 'id', 'X320', '%')[['id', '%a']]
circulation_notes1.columns = ['id', 'X320a']
circulation_notes2 = marc_parser_1_field(bn_books, 'id', 'X321', '%')[['id', '%a']]
circulation_notes2.columns = ['id', 'X321a']
circulation_notes3 = marc_parser_1_field(bn_books, 'id', 'X322', '%')[['id', '%a']]
circulation_notes3.columns = ['id', 'X322a']

circulation_notes = [circulation_notes1, circulation_notes2, circulation_notes3]
circulation_notes = reduce(lambda left,right: pd.merge(left,right,on='id', how = 'outer'), circulation_notes)

circulation_notes['total_notes'] = circulation_notes[circulation_notes.columns[1:]].apply(
    lambda x: ','.join(x.dropna().astype(str)),
    axis=1
)

subject_classification = marc_parser_1_field(bn_books, 'id', 'X600', '%')
subject_classification = pd.DataFrame(subject_classification['X600'].str.split('|').tolist(), subject_classification['id']).stack()
subject_classification = subject_classification.reset_index()[[0, 'id']]
subject_classification.columns = ['X600', 'id']
subject_classification = subject_classification.loc[subject_classification['X600'] != ''].reset_index(drop = True)
subject_classification['X600'] = subject_classification['X600'].str.replace(r'^\%.', '')

###
full_record = pd.melt(bn_books,id_vars=['id'],var_name='field', value_name='value').sort_values(['id', 'field'])
full_record = full_record.loc[full_record['value'].notnull()].reset_index(drop = True)
#test['value'] = test['value'].strip()

full_record['total'] = full_record[full_record.columns[1:]].apply(
    lambda x: ': '.join(x.dropna().astype(str)),
    axis=1
)

full_record['full'] = full_record.groupby('id')['total'].transform(lambda x: '; '.join(x.str.strip()))
# drop duplicates
full_record = full_record[['id', 'full']].drop_duplicates()


###
# zrobić
# project_id

#sub_objects
#contained work
cw_date_period = year_of_publication_de_facto
cw_location_reference = place_of_publication

contained_work = copy.deepcopy(bn_books)
#contained_work = pd.concat([contained_work,pd.DataFrame(columns = ['title', 'author', 'corporate_author', 'translation_of_work', 'isbn_of_source', 'flag', 'typ'])])



contained_work = cSplit(contained_work, 'id', 'X200', '|')
contained_work['index'] = contained_work.index + 1

contained_work = pd.concat([contained_work,pd.DataFrame(columns = ['title', 'author', 'corporate_author', 'translation_of_work', 'isbn_of_source', 'flag', 'typ'])])
# anonimowe/zbiorowe bez autora
contained_work.loc[(contained_work['X100'].isnull()) & 
                   (contained_work['X200'].str.contains('%z')==False) & 
                   (contained_work['X300'].isnull()) &
                   (contained_work['X330'].isnull()) &
                   (contained_work['X500'].isnull()), 
                   'typ'] = 'anonimowe/zbiorowe bez autora'

# monografia, jeden autor
contained_work.loc[(contained_work['X100'].notnull()) & 
                   (contained_work['X100'].str.contains('%x')==False) &
                   (contained_work['X200'].str.contains('%z')==False) & 
                   (contained_work['X300'].isnull()) &
                   (contained_work['X330'].isnull()) &
                   (contained_work['X500'].isnull()), 
                   ['author', 'typ']] = [contained_work['X100'].loc[(contained_work['X100'].notnull()) &
                                                                    (contained_work['X100'].str.contains('%x')==False) &
                                                                    (contained_work['X200'].str.contains('%z')==False) &
                                                                    (contained_work['X300'].isnull()) &
                                                                    (contained_work['X330'].isnull()) &
                                                                    (contained_work['X500'].isnull())], 'monografia, jeden autor']
# monografia wieloautorska (bez wykazu dzieł zawartych)                                                                                                       
contained_work.loc[(contained_work['X100'].str.contains('%x')==True) &
                   (contained_work['X200'].str.contains('%z')==False) & 
                   (contained_work['X300'].isnull()) &
                   (contained_work['X330'].isnull()) &
                   (contained_work['X500'].isnull()), 
                   ['author', 'flag', 'typ']] = [contained_work['X100'].loc[(contained_work['X100'].str.contains('%x')==True) &
                                                                            (contained_work['X200'].str.contains('%z')==False) & 
                                                                            (contained_work['X300'].isnull()) &
                                                                            (contained_work['X330'].isnull()) &
                                                                            (contained_work['X500'].isnull())], 'REV', 'monografia wieloautorska (bez wykazu dzieł zawartych)']
# współwydane bez wspólnego tytułu (nie zawiera 200%z)
contained_work.loc[((contained_work['X100'].isnull()) |
                    (contained_work['X100'].str.contains('%x')==False)) &
                    (contained_work['X200'].str.contains('%z')==True) & 
                    (contained_work['X300'].isnull()) &
                    (contained_work['X330'].isnull()) &
                    (contained_work['X500'].isnull()) &
                    (contained_work['X700'].str.contains('%vau')==False), 
                    ['author', 'typ']] = [contained_work['X100'].loc[((contained_work['X100'].isnull()) |
                                                                      (contained_work['X100'].str.contains('%x')==False)) &
                                                                      (contained_work['X200'].str.contains('%z')==True) & 
                                                                      (contained_work['X300'].isnull()) &
                                                                      (contained_work['X330'].isnull()) &
                                                                      (contained_work['X500'].isnull()) &
                                                                      (contained_work['X700'].str.contains('%vau')==False)], 'współwydane bez wspólnego tytułu (nie zawiera 200%z)']
                                                                      
# współwydane bez wspólnego tytułu (zawiera 200%z)
contained_work.loc[((contained_work['X100'].isnull()) |
                    (contained_work['X100'].str.contains('%x')==False)) &
                    (contained_work['X200'].str.contains('%z')==True) & 
                    (contained_work['X300'].isnull()) &
                    (contained_work['X330'].isnull()) &
                    (contained_work['X500'].isnull()) &
                    (contained_work['X700'].str.contains('%vau')==True), 
                    ['author', 'flag', 'typ']] = [contained_work['X100'].loc[((contained_work['X100'].isnull()) |
                                                                              (contained_work['X100'].str.contains('%x')==False)) &
                                                                              (contained_work['X200'].str.contains('%z')==True) & 
                                                                              (contained_work['X300'].isnull()) &
                                                                              (contained_work['X330'].isnull()) &
                                                                              (contained_work['X500'].isnull()) &
                                                                              (contained_work['X700'].str.contains('%vau')==True)], 'REV', 'współwydane bez wspólnego tytułu (zawiera 200%z)']

test = contained_work.loc[contained_work['typ'] == 'współwydane bez wspólnego tytułu (zawiera 200%z)']
  #do poprawki - wchodzi pierwszy if, a reszta to bałagan                                                                            
for i, row in contained_work.loc[contained_work['typ'] == 'współwydane bez wspólnego tytułu (zawiera 200%z)'].iterrows():
    element = re.findall('(\%z\d+)', row['X200'])[0]
    if element == '%z0':
        contained_work['author'][i] = contained_work['X100'][i]
    elif element == '%z1':
        try:
            contained_work['author'][i] = [k for k in row['X700'] if r'%vau' in k][0]
        except:
            contained_work['author'][i] = 'brak autora'
    elif element == '%z2':
        try:
            contained_work['author'][i] = [k for k in row['X700'] if r'%vau' in k][1]
        except IndexError:
            contained_work['author'][i] = 'brak autora'
    elif element == '%z3':
        try:
            contained_work['author'][i] = [k for k in row['X700'] if r'%vau' in k][2]
        except IndexError:
            contained_work['author'][i] = 'brak autora w 700'
    elif element == '%z4':
        try:
            contained_work['author'][i] = [k for k in row['X700'] if r'%vau' in k][3]
        except IndexError:
            contained_work['author'][i] = 'brak autora w 700'
    elif element == '%z5':
        try:
            contained_work['author'][i] = [k for k in row['X700'] if r'%vau' in k][4]
        except IndexError:
            contained_work['author'][i] = 'brak autora'

# współwydane bez wspólnego tytułu (zawiera 200%z) - druga podgrupa
            
title_cw_fixed = contained_work.loc[(contained_work['X200'].str.contains('%z')==True) & 
                   ((contained_work['X300'].str.contains('%b')==True) |
                    (contained_work['X300'].str.contains('%f')==True))][['index', 'X200', 'X300']]
title_cw_fixed1 = marc_parser_1_field(title_cw_fixed, 'index', 'X200', '%')[['index', '%a']]
title_cw_fixed2 = marc_parser_1_field(title_cw_fixed, 'index', 'X300', '%')[['index', '%f', '%b']]
title_cw_fixed = reduce(lambda left, right: pd.merge(left, right, on = 'index', how = 'outer'), [title_cw_fixed1, title_cw_fixed2])
# =============================================================================
# zestawienie z 700 się nie uda, bo zawartości pól się nie pokrywają
# X700 = cSplit(contained_work, 'index', 'X700', '|')[['index', 'X700']]
# title_cw_fixed = reduce(lambda left, right: pd.merge(left, right, on = 'index', how = 'left'), [title_cw_fixed, X700])
# =============================================================================
X100 = cSplit(contained_work, 'index', 'X100', '|')[['index', 'X100']]
X700 = cSplit(contained_work, 'index', 'X700', '|')[['index', 'X700']]
title_cw_fixed = reduce(lambda left, right: pd.merge(left, right, on = 'index', how = 'left'), [title_cw_fixed, X100, X700])

def check_person_title(row, tyt1, tyt2, pers1, pers2, pers3):
    if row[tyt1] in row[tyt2] and row[pers1].split(' ')[0] in str(row[pers2]):
        return row[pers2]
    elif row[tyt1] in row[tyt2] and row[pers1].split(' ')[0] in str(row[pers3]):
        return row[pers3]
    else:
        return np.nan

title_cw_fixed['author'] = title_cw_fixed.apply(lambda x: check_person_title(x, '%a', '%f', '%b', 'X100', 'X700'), axis = 1)

title_cw_fixed = title_cw_fixed.loc[title_cw_fixed['author'].notnull()][['index', '%f', 'author']]
title_cw_fixed.columns = ['index', 'title', 'author']
title_cw_fixed['typ'] = 'współwydane bez wspólnego tytułu (zawiera 200%z)2'
title_cw_fixed = title_cw_fixed.drop_duplicates().set_index('index')
contained_work = title_cw_fixed.combine_first(contained_work.set_index('index')).reset_index().reindex(columns=contained_work.columns)

# współwydane ze wspólnym tytułem 1
title_cw_fixed = contained_work.loc[(contained_work['X200'].str.contains('%z')==False) & 
                                    ((contained_work['X300'].str.contains('%b')==True) |
                                     (contained_work['X300'].str.contains('%f')==True))][['index', 'X100', 'X200', 'X300', 'X700']]
#poprawa kolejności X300
fix_order = pd.read_excel("C:/Users/Cezary/Downloads/do_samizdatu1.xlsx").set_index('index')
title_cw_fixed = fix_order.combine_first(title_cw_fixed.set_index('index')).reset_index().reindex(columns=title_cw_fixed.columns)

#title_cw_fixed['X300'] = title_cw_fixed['X300'].replace(r'(?!^|\|)(\%b)', r'|\1', regex = True)
#title_cw_fixed['X300'] = title_cw_fixed['X300'].replace(r'(\|)(\%[^f])', r'\2', regex = True)


title_cw_fixed = cSplit(title_cw_fixed, 'index', 'X300', '|')
title_cw_fixed = cSplit(title_cw_fixed, 'index', 'X700', '|')
title_cw_fixed = title_cw_fixed.loc[title_cw_fixed['X300'].notnull()]

def check_person(row, pers1, pers2):
    if '%b' in str(row[pers2]) and str(row[pers1]).split(' ')[0][2:] in str(row[pers2]):
        return row[pers1]
    elif '%b' in str(row[pers2]) and pd.isnull(row[pers1]) and row['X100'] != '':
        return row['X100']
    elif '%b' in str(row[pers2]):
        m = re.search('(?<=%b)(.*?)(?= %f|$)', str(row[pers2])).group(1)
        return m
    else:
        return np.nan

title_cw_fixed['author'] = title_cw_fixed.apply(lambda x: check_person(x, 'X700', 'X300'), axis = 1)
title_cw_fixed = title_cw_fixed.loc[(title_cw_fixed['author'].notnull()) |
                          (title_cw_fixed['X700'].isnull())]
title_cw_fixed = title_cw_fixed.drop_duplicates()


test = re.search('(?<=%b)(.*?)(?= %f)', '%bWierzbicki Piotr (1935- ) %fSpór z niańkami').group(1)


#12.11.2020
#unmerge
bn_books = pd.read_csv("F:/Cezary/Documents/IBL/Samizdat/bn_books.csv", sep=';')

df = pd.DataFrame()

un1_full = "bn_books-120-%1-66753|bn_books-120-%1-66992|bn_books-120-%1-67008|bn_books-120-%1-67023|bn_books-120-%1-67038|bn_books-120-%1-67054|bn_books-120-%1-67069|bn_books-120-%1-67090|bn_books-120-%1-67103|bn_books-120-%1-67247|bn_books-120-%1-67263|bn_books-120-%1-67332|bn_books-120-%1-66810|bn_books-120-%1-66831|bn_books-120-%1-66849|bn_books-120-%1-66871|bn_books-120-%1-66890|bn_books-120-%1-66925|bn_books-120-%1-66948|bn_books-120-%1-66969|bn_books-120-%1-67138|bn_books-120-%1-67156|bn_books-120-%1-67125|bn_books-120-%1-67174|bn_books-120-%1-67189|bn_books-120-%1-67280|bn_books-120-%1-67297"
un1 = re.findall('(\d+)(?=\||$)', un1_full)
un1 = bn_books[bn_books['id'].isin(un1)]

institutions_120 = marc_parser_1_field(un1, 'id', 'X120', '%')[['id', '%1', '%2']]
institutions_120.columns = [['id', 'Entity_Name', 'Related_Entity_Sub_Entity']]
institutions_120['MRC'] = '120'
institutions_120['subfield'] = '%1'
institutions_120['Related_Entity_Main_Entity'] = np.nan
institutions_120['Located_Location'] = np.nan
sub_institutions_120 = marc_parser_1_field(un1, 'id', 'X120', '%')[['id', '%2', '%1']]
sub_institutions_120 = sub_institutions_120.loc[sub_institutions_120['%2'] != ""]
sub_institutions_120.columns = [['id', 'Entity_Name', 'Related_Entity_Main_Entity']]
sub_institutions_120['MRC'] = '120'
sub_institutions_120['subfield'] = '%2'
sub_institutions_120['Related_Entity_Sub_Entity'] = np.nan
sub_institutions_120['Located_Location'] = np.nan
bn_institutions120 = pd.concat([institutions_120, sub_institutions_120], axis = 0)
bn_institutions120['source'] = "bn_books"
bn_institutions120.columns = bn_institutions120.columns.get_level_values(0)

bn_institutions120['Grouped'] = bn_institutions120[['source', 'MRC', 'subfield', 'id']].apply(lambda x: '-'.join(x.astype(str)), axis = 1)
bn_institutions120 = bn_institutions120[['Entity_Name', 'Related_Entity_Sub_Entity', 'Related_Entity_Main_Entity', 'Located_Location', 'Grouped']].sort_values(['Entity_Name', 'Related_Entity_Sub_Entity', 'Related_Entity_Main_Entity'])
bn_institutions120['simple_name_loc'] = bn_institutions120[['Entity_Name', 'Related_Entity_Sub_Entity', 'Related_Entity_Main_Entity', 'Located_Location']].apply(lambda x: '|'.join(x.dropna().astype(str).str.lower().str.replace('\W', '')), axis = 1)

for column in bn_institutions120.iloc[:,:5]:
    bn_institutions120[column] = bn_institutions120.groupby('simple_name_loc')[column].transform(lambda x: '|'.join(x.drop_duplicates().astype(str)))

bn_institutions120 = bn_institutions120.drop_duplicates().sort_values(['simple_name_loc', 'Entity_Name', 'Related_Entity_Sub_Entity', 'Related_Entity_Main_Entity']).reset_index(drop = True)
bn_institutions120['group_id'] = bn_institutions120.index + 1
bn_institutions120 = bn_institutions120.replace('nan', '')

bn_institutions120 = bn_institutions120[bn_institutions120['Grouped'].str.contains(un1_full)]
df = df.append(bn_institutions120)

un2_full = "bn_books-110-%1-66685|bn_books-110-%1-66702|bn_books-110-%1-66727|bn_books-110-%1-66741|bn_books-110-%1-66766|bn_books-110-%1-66782|bn_books-110-%1-66797|bn_books-110-%1-66910|bn_books-110-%1-67211|bn_books-110-%1-67227|bn_books-110-%1-67318"
un2 = re.findall('(\d+)(?=\||$)', un2_full)
un2 = bn_books[bn_books['id'].isin(un2)]

institutions_110 = marc_parser_1_field(un2, 'id', 'X110', '%')[['id', '%1', '%2']]
institutions_110.columns = [['id', 'Entity_Name', 'Related_Entity_Sub_Entity']]
institutions_110['MRC'] = '110'
institutions_110['subfield'] = '%1'
institutions_110['Related_Entity_Main_Entity'] = np.nan
institutions_110['Located_Location'] = np.nan
sub_institutions_110 = marc_parser_1_field(un2, 'id', 'X110', '%')[['id', '%2', '%1']]
sub_institutions_110 = sub_institutions_110.loc[sub_institutions_110['%2'] != ""]
sub_institutions_110.columns = [['id', 'Entity_Name', 'Related_Entity_Main_Entity']]
sub_institutions_110['MRC'] = '110'
sub_institutions_110['subfield'] = '%2'
sub_institutions_110['Related_Entity_Sub_Entity'] = np.nan
sub_institutions_110['Located_Location'] = np.nan
bn_institutions110 = pd.concat([institutions_110, sub_institutions_110], axis = 0)
bn_institutions110['source'] = "bn_books"
bn_institutions110.columns = bn_institutions110.columns.get_level_values(0)

bn_institutions110['Grouped'] = bn_institutions110[['source', 'MRC', 'subfield', 'id']].apply(lambda x: '-'.join(x.astype(str)), axis = 1)
bn_institutions110 = bn_institutions110[['Entity_Name', 'Related_Entity_Sub_Entity', 'Related_Entity_Main_Entity', 'Located_Location', 'Grouped']].sort_values(['Entity_Name', 'Related_Entity_Sub_Entity', 'Related_Entity_Main_Entity'])
bn_institutions110['simple_name_loc'] = bn_institutions110[['Entity_Name', 'Related_Entity_Sub_Entity', 'Related_Entity_Main_Entity', 'Located_Location']].apply(lambda x: '|'.join(x.dropna().astype(str).str.lower().str.replace('\W', '')), axis = 1)

for column in bn_institutions110.iloc[:,:5]:
    bn_institutions110[column] = bn_institutions110.groupby('simple_name_loc')[column].transform(lambda x: '|'.join(x.drop_duplicates().astype(str)))

bn_institutions110 = bn_institutions110.drop_duplicates().sort_values(['simple_name_loc', 'Entity_Name', 'Related_Entity_Sub_Entity', 'Related_Entity_Main_Entity']).reset_index(drop = True)
bn_institutions110['group_id'] = bn_institutions110.index + 1
bn_institutions110 = bn_institutions110.replace('nan', '')

bn_institutions110 = bn_institutions110[bn_institutions110['Grouped'].str.contains(un2_full)]
df = df.append(bn_institutions110)

#relations

samizdat_institutions = pd.read_excel("F:/Cezary/Documents/IBL/Samizdat/samizdat_instytucje_2020_11_12.xlsx", "ver3")

inst_relations_with_main = samizdat_institutions[['group_id', 'Related_Entity_Main_Entity']]
inst_relations_with_main = inst_relations_with_main[inst_relations_with_main['Related_Entity_Main_Entity'].notnull()].drop_duplicates().reset_index(drop=True)
inst_relations_with_main['index'] = inst_relations_with_main.index + 1
inst_relations_with_main['Related_Entity_Main_Entity'] = inst_relations_with_main['Related_Entity_Main_Entity'].astype(str)
inst_relations_with_main = cSplit(inst_relations_with_main, 'index', 'Related_Entity_Main_Entity', '\|').drop(columns='index')
inst_relations_with_main['group_id'] = inst_relations_with_main.groupby('Related_Entity_Main_Entity').transform(lambda x: '|'.join(x.drop_duplicates().astype(str)))
inst_relations_with_main = inst_relations_with_main.drop_duplicates()




#24.11.2020

samizdat_institutions = gsheet_to_df('15pI92bcYWOMpSWaqtmQbOAPZys-SzesWbbMzc2zWqDY', 'ver_3').drop(columns='alt_name_vol_2').drop_duplicates()

#cleaning of 'Alternative_form_of_name'

#quotes

def quotes(x):
    x = x.split('|')
    for i, ver in enumerate(x):
        x[i] = ver.replace('\"', '')
    x = '|'.join(list(set(x)))
    return x

samizdat_institutions['Alternative_form_of_name'] = samizdat_institutions['Alternative_form_of_name'].apply(lambda x: quotes(x))

def brackets(x):
    x = x.split('|')
    for i, ver in enumerate(x):
        if 'i. e.' in ver:
            x[i] = ver.replace('[', '').replace(']', '')
        elif regex.findall('\p{Ll}\[', ver):
            if ver.count('[') != ver.count(']'):
                x[i] = f"{ver}]"            
            x[i] = regex.sub('(?<=\p{Ll})(\[.+?\])', '.', x[i])
            x[i] = re.sub('\[.+?\]', '', x[i]).strip()
        elif len(ver) > 0 and ver[0] == '[' and ver[-1] == ']':
            x[i] = ver.replace('[', '').replace(']', '').strip()
        elif '[' in ver:
            if ver.count('[') != ver.count(']'):
                x[i] = f"{ver}]"
            x[i] = re.sub('\[.+?\]', '', x[i])
            x[i] = re.sub(' +', ' ', x[i]).strip()
        elif ']' in ver:
            x[i] = ver.replace(']', '')            
    x = '|'.join(list(set(x)))
    return x
    
samizdat_institutions['Alternative_form_of_name'] = samizdat_institutions['Alternative_form_of_name'].apply(lambda x: brackets(x)).str.replace('(', '').str.replace(')', '')          
samizdat_institutions['Alternative_form_of_name'] = samizdat_institutions.apply(lambda x: np.nan if x['Alternative_form_of_name'] == x['Entity_Name'] else x['Alternative_form_of_name'], axis=1)

# df_to_gsheet(samizdat_institutions, '15pI92bcYWOMpSWaqtmQbOAPZys-SzesWbbMzc2zWqDY')

# merge and collapse

def unique_and_merge(x):
    x = '|'.join(x.dropna().drop_duplicates().astype(str))
    x = '|'.join(list(set([s.strip() for s in x.split('|')])))                                                                              
    return x
    
for column in samizdat_institutions.iloc[:,2:]:
    samizdat_institutions[column] = samizdat_institutions.groupby('group_id')[column].transform(lambda x: unique_and_merge(x))
    samizdat_institutions[column] = samizdat_institutions[column].apply(lambda x: re.sub('^\|', '', x))
    
samizdat_institutions = samizdat_institutions.drop(columns=['REV']).drop_duplicates().reset_index(drop=True)    

df_to_gsheet(samizdat_institutions, '15pI92bcYWOMpSWaqtmQbOAPZys-SzesWbbMzc2zWqDY', 'ver_4')    
    
#07.12.2020 - samizdat people    
 
# kartoteka osób

samizdat_people = gsheet_to_df('1Y8Y_pfkuKiv5npL6QJAXWbDO6twIJqsK3ArvaokjFkU', 'samizdat').drop_duplicates()
samizdat_people['name_simple'] = samizdat_people.apply(lambda x: re.sub('na$', '', x['name_simple']) if x['name_form'][-4:] == ', NA' else x['name_simple'], axis=1)
samizdat_people['name_form'] = samizdat_people['name_form'].str.replace(', NA', '')

tilde_encoding = gsheet_to_df('1dbBf-skFsksLoKmXelDDeP78yLXV_WyjWQ-LbRa-CRE', 'Sheet1')[['error', 'encoding']]
tilde_encoding['length'] = tilde_encoding['error'].apply(lambda x: len(x))
tilde_encoding = tilde_encoding.sort_values(['length', 'error'], ascending=(False, False))[['error', 'encoding']].values.tolist()
tilde_encoding = [(re.escape(m), n if n is not None else '') for m, n in tilde_encoding]
samizdat_people['name_form_without_encoding'] = samizdat_people['name_form']

for ind, elem in enumerate(tilde_encoding):
    print(str(ind+1) + '/' + str(len(tilde_encoding)))
    samizdat_people['name_form'] = samizdat_people['name_form'].str.replace(elem[0], elem[1], regex=True)
samizdat_people['name_form'] = samizdat_people['name_form'].str.replace('\~', '', regex=True)

def people_brackets(x):
    x = x.split('|')
    for i, ver in enumerate(x):
        x[i] = ver.replace('[', '').replace(']', '')
    x = '|'.join(list(set([m.strip() for m in x])))
    return x

samizdat_people['name_form'] = samizdat_people['name_form'].apply(lambda x: people_brackets(x))
samizdat_people['index'] = samizdat_people.index+1

test = cSplit(samizdat_people, 'index', 'name_form_id', '|')

name_and_id = samizdat_people[['name_form', 'id_osoby']]

#04.01.2021 - samizdat people  

samizdat_people = gsheet_to_df('1HE-bKfnISmtFqSGci7OvG2wzcpZE9A5SYoYBnPPHQJ0', 'Arkusz1').drop_duplicates()[['Project_ID', 'Index_Name', 'Other_Name_Form']]
samizdat_people['Project_ID'] = samizdat_people['Project_ID'].astype(int)
samizdat_people_other_names = cSplit(samizdat_people[['Project_ID', 'Other_Name_Form']], 'Project_ID', 'Other_Name_Form', '; ')
samizdat_people_other_names = samizdat_people_other_names[samizdat_people_other_names['Other_Name_Form'].notnull()].rename(columns={'Other_Name_Form': 'Index_Name'})
samizdat_people = pd.concat([samizdat_people[['Project_ID', 'Index_Name']], samizdat_people_other_names]).sort_values('Project_ID').drop_duplicates().reset_index(drop=True)

# viaf
samizdat_viaf = pd.DataFrame()
for index, row in samizdat_people.iterrows():

    search_name = row['Index_Name']
    
    print(str(index+1) + '/' + str(len(samizdat_people)))
    connection_no = 1
    while True:
        try:
            people_links = []
            while len(people_links) == 0 and len(search_name) > 0:
                url = re.sub('\s+', '%20', f"http://viaf.org/viaf/search?query=local.personalNames%20all%20%22{search_name}%22&sortKeys=holdingscount&recordSchema=BriefVIAF")
                response = requests.get(url)
                response.encoding = 'UTF-8'
                soup = BeautifulSoup(response.text, 'html.parser')
                people_links = soup.findAll('a', attrs={'href': re.compile("viaf/\d+")})
                if len(people_links) == 0:
                    search_name = ' '.join(search_name.split(' ')[:-1])
                
            if len(people_links) > 0:
                viaf_people = []
                for people in people_links:
                    person_name = re.split('â\x80\x8e|\u200e ', re.sub('\s+', ' ', people.text).strip())
                    person_link = re.sub(r'(.+?)(\#.+$)', r'http://viaf.org\1viaf.xml', people['href'].strip())
                    person_link = [person_link] * len(person_name)
# =============================================================================
#                     libraries = str(people).split('<br/>')
#                     libraries = [re.sub('(.+)(\<span.*+$)', r'\2', s.replace('\n', ' ')) for s in libraries if 'span' in s]
#                     single_record = list(zip(person_name, person_link, libraries))
#                     viaf_people += single_record
#                 viaf_people = pd.DataFrame(viaf_people, columns=['viaf name', 'viaf', 'libraries'])
# =============================================================================
                    single_record = list(zip(person_name, person_link))
                    viaf_people += single_record
                viaf_people = pd.DataFrame(viaf_people, columns=['viaf name', 'viaf'])
                viaf_people['Project_ID'] = row['Project_ID']
                viaf_people['Index_Name'] = row['Index_Name']
                viaf_people['search name'] = search_name
                for ind, vname in viaf_people.iterrows():
                    viaf_people.at[ind, 'cosine'] = get_cosine_result(vname['viaf name'], vname['search name'])
        
                if viaf_people['cosine'].max() >= 0.5:
                    viaf_people = viaf_people[viaf_people['cosine'] >= 0.5]
                else:
                    viaf_people = viaf_people[viaf_people['cosine'] == viaf_people['cosine'].max()]

# czy informacja o nazwie z polskiej biblioteki jest istotna?                
# =============================================================================
#                 viaf_people['polish library'] = viaf_people['libraries'].apply(lambda x: True if re.findall('Biblioteka Narodowa \(Polska\)|NUKAT \(Polska\)|National Library of Poland|NUKAT Center of Warsaw University Library', x) else False)
#                 viaf_people = viaf_people.drop(columns=['libraries', 'search name', 'cosine'])
#                 viaf_people['viaf name'] = viaf_people.groupby(['viaf', 'Project_ID'])['viaf name'].transform(lambda x: '❦'.join(x.drop_duplicates().astype(str)))
# =============================================================================
                    
                viaf_people = viaf_people.drop(columns='search name').drop_duplicates().reset_index(drop=True) 
            
                samizdat_viaf = samizdat_viaf.append(viaf_people)
            else:
                viaf_people = pd.DataFrame({'viaf name': ['brak'], 'viaf': ['brak'], 'Project_ID': [row['Project_ID']], 'Index_Name': [row['Index_Name']]})
                samizdat_viaf = samizdat_viaf.append(viaf_people)
        except (IndexError, KeyError):
            pass
        except requests.exceptions.ConnectionError:
            print(connection_no)
            connection_no += 1
            time.sleep(300)
            continue
        break

samizdat_viaf = samizdat_viaf[samizdat_viaf['viaf name'] != 'brak']
for column in samizdat_viaf.drop(columns=['viaf', 'Project_ID']):
    samizdat_viaf[column] = samizdat_viaf.groupby(['viaf', 'Project_ID'])[column].transform(lambda x: '❦'.join(x.drop_duplicates().astype(str)))
samizdat_viaf = samizdat_viaf.drop_duplicates().reset_index(drop=True) 

df_to_gsheet(samizdat_viaf, '1HE-bKfnISmtFqSGci7OvG2wzcpZE9A5SYoYBnPPHQJ0', 'match with viaf')

# wikidata enrichment

samizdat_viaf = gsheet_to_df('1HE-bKfnISmtFqSGci7OvG2wzcpZE9A5SYoYBnPPHQJ0', 'match with viaf')
sparql = SPARQLWrapper("https://query.wikidata.org/sparql")

samizdat_wikidata = pd.DataFrame()
for i, row in samizdat_viaf.iterrows():
    print(f"{i+1}/{len(samizdat_viaf)}")
    try:
        viaf = re.findall('\d+', row['viaf'])[0]
        sparql_query = f"""PREFIX wdt: <http://www.wikidata.org/prop/direct/>
        SELECT distinct ?autor ?autorLabel ?birthplaceLabel ?deathplaceLabel ?birthdate ?deathdate ?sexLabel ?pseudonym ?occupationLabel WHERE {{ 
          ?autor wdt:P214 "{viaf}" ;
          wdt:P19 ?birthplace ;
          wdt:P569 ?birthdate ;
          wdt:P570  ?deathdate ; 
          optional {{ ?autor wdt:P20 ?deathplace . }}
          optional {{ ?autor wdt:P21 ?sex . }}
          optional {{ ?autor wdt:P106 ?occupation . }}
          optional {{ ?autor wdt:P742 ?pseudonym . }}
        SERVICE wikibase:label {{ bd:serviceParam wikibase:language "pl". }}}}"""    
        sparql.setQuery(sparql_query)
        sparql.setReturnFormat(JSON)
        results = sparql.query().convert()
    
        results_df = pd.io.json.json_normalize(results['results']['bindings'])
        results_df['viaf'] = viaf
    
        for column in results_df.drop(columns='viaf'):
            results_df[column] = results_df.groupby('viaf')[column].transform(lambda x: '❦'.join(x.drop_duplicates().astype(str)))
        results_df = results_df.drop_duplicates().reset_index(drop=True)   
        
        samizdat_wikidata = samizdat_wikidata.append(results_df)
    except (HTTPError, RemoteDisconnected):
        time.sleep(61)
        continue
        

samizdat_wikidata = samizdat_wikidata[['viaf', 'autor.value', 'birthdate.value', 'deathdate.value', 'birthplaceLabel.value', 'deathplaceLabel.value', 'sexLabel.value', 'pseudonym.value', 'occupationLabel.value']]
samizdat_viaf['viaf'] = samizdat_viaf['viaf'].apply(lambda x: re.findall('\d+', x)[0])
samizdat_wikidata = pd.merge(samizdat_viaf, samizdat_wikidata, how='left', on='viaf').drop_duplicates().reset_index(drop=True)

df_to_gsheet(samizdat_wikidata, '1HE-bKfnISmtFqSGci7OvG2wzcpZE9A5SYoYBnPPHQJ0', 'wikidata enrichment')

# viaf enrichment

# wpisać try catch dla ConnectionError

samizdat_viaf = gsheet_to_df('1HE-bKfnISmtFqSGci7OvG2wzcpZE9A5SYoYBnPPHQJ0', 'match with viaf')
ns = '{http://viaf.org/viaf/terms#}'

samizdat_viaf_enrichment = pd.DataFrame()
for i, row in samizdat_viaf.iterrows():
    print(f"{i+1}/{len(samizdat_viaf)}")
    url = row['viaf']
    
    response = requests.get(url)
    with open('viaf.xml', 'wb') as file:
        file.write(response.content)
    tree = et.parse('viaf.xml')
    root = tree.getroot()
    birth_date = root.findall(f'.//{ns}birthDate')
    birth_date = '❦'.join([t.text for t in birth_date])
    death_date = root.findall(f'.//{ns}deathDate')
    death_date = '❦'.join([t.text for t in death_date])
    occupation = root.findall(f'.//{ns}occupation/{ns}data/{ns}text')
    occupation = '❦'.join([t.text for t in occupation])
    gender = root.findall(f'.//{ns}gender')
    gender = '❦'.join([t.text for t in gender])
    viaf_record = [url, birth_date, death_date, occupation, gender]
    samizdat_viaf_enrichment = samizdat_viaf_enrichment.append(viaf_record)

samizdat_viaf_enrichment = pd.merge(samizdat_viaf, samizdat_viaf_enrichment, how='left', on='viaf')

df_to_gsheet(samizdat_viaf_enrichment, '1HE-bKfnISmtFqSGci7OvG2wzcpZE9A5SYoYBnPPHQJ0', 'viaf enrichment')




for index, row in X100.iterrows():
    print(str(index) + '/' + str(len(X100)))
    try:
       

                for i, line in viaf_people.iterrows():
                    url = line['viaf']
                    response = requests.get(url)
                    with open('viaf.xml', 'wb') as file:
                        file.write(response.content)
                    tree = et.parse('viaf.xml')
                    root = tree.getroot()
                    viaf_id = root.findall(f'.//{ns}viafID')[0].text
                    IDs = root.findall(f'.//{ns}mainHeadings/{ns}data/{ns}sources/{ns}sid')
                    IDs = '❦'.join([t.text for t in IDs])
                    nationality = root.findall(f'.//{ns}nationalityOfEntity/{ns}data/{ns}text')
                    nationality = '❦'.join([t.text for t in nationality])
                    occupation = root.findall(f'.//{ns}occupation/{ns}data/{ns}text')
                    occupation = '❦'.join([t.text for t in occupation])
                    language = root.findall(f'.//{ns}languageOfEntity/{ns}data/{ns}text')
                    language = '❦'.join([t.text for t in language])
                    names = root.findall(f'.//{ns}x400/{ns}datafield')
                    sources = root.findall(f'.//{ns}x400/{ns}sources')
                    name_source = []
                    for (name, source) in zip(names, sources):   
                        person_name = ' '.join([child.text for child in name.getchildren() if child.tag == f'{ns}subfield' and child.attrib['code'].isalpha()])
                        library = '~'.join([child.text for child in source.getchildren() if child.tag == f'{ns}sid'])
                        name_source.append([person_name, library])   
                    for i, elem in enumerate(name_source):
                        name_source[i] = '‽'.join(name_source[i])
                    name_source = '❦'.join(name_source)
                    
                    person = [row['index'], row['$$a'], row['$$d'], viaf_id, IDs, nationality, occupation, language, name_source]
                    viaf_enrichment.append(person)
            except (KeyError, IndexError):
                person = [row['index'], row['$$a'], row['$$d'], '', '', '', '', '', '']
                viaf_enrichment.append(person)
    except IndexError:
        error = [row['index'], row['$$a'], row['$$d']]
        viaf_errors.append(error)       











for i, row in test.iterrows():
    print(f"{i+1}/{len(test)}")
    source = row['name_form_id'].split('-')[0]
    record_id = int(row['name_form_id'].split('-')[1])
    field = row['name_form_id'].split('-')[-2]
    subfield = row['name_form_id'].split('-')[-1]
    if source == 'bn_books':
        original_field = bn_books[bn_books['id'] == record_id][field].to_list()[0].split('|')
        for element in original_field:
            if f"{subfield}{row['name_form_without_encoding'].split(' ')[0]}" in element:
                original_field = element
        test.at[i, 'source'] = source
        test.at[i, 'field'] = field
        test.at[i, 'subfield'] = subfield
        test.at[i, 'original field'] = original_field
    elif source == 'cz_articles':
        original_field = cz_articles[cz_articles['id'] == record_id][field].to_list()[0].split('|')
        for element in original_field:
            if f"{subfield}{row['name_form_without_encoding'].split(' ')[0]}" in element:
                original_field = element
        test.at[i, 'source'] = source
        test.at[i, 'field'] = field
        test.at[i, 'subfield'] = subfield
        if type(original_field) == list:
            original_field = original_field[0]    
        test.at[i, 'original field'] = original_field
    elif source == 'cz_books':
        original_field = cz_books[cz_books['id'] == record_id][field].to_list()[0].split('|')
        for element in original_field:
            if f"{subfield}{row['name_form_without_encoding'].split(' ')[0]}" in element:
                original_field = element
        test.at[i, 'source'] = source
        test.at[i, 'field'] = field
        test.at[i, 'subfield'] = subfield
        test.at[i, 'original field'] = original_field
    elif source == 'pbl_articles':
        field = row['name_form_id'].split('-')[2][:4]
        original_field = row['name_form']
        test.at[i, 'source'] = source
        test.at[i, 'field'] = field
        test.at[i, 'subfield'] = subfield
        test.at[i, 'original field'] = original_field
    elif source == 'pbl_books':
        field = row['name_form_id'].split('-')[2][:4]
        original_field = row['name_form']
        test.at[i, 'source'] = source
        test.at[i, 'field'] = field
        test.at[i, 'subfield'] = subfield
        test.at[i, 'original field'] = original_field




for i, row in test.iterrows():
    print(f"{i+1}/{len(test)}")
    location = '-'.join(row['name_form_id'].split('-')[-2:])
    test.at[i, location] = row['name_form']
    




df_final = pd.DataFrame()
for i, row in test.iterrows():
    df = test.iloc[[i]]

# wziąć każdy wiersz testu i dla niego zrobić merge bo źródle, polu i id
    # może być kilka wierszy, ale to potem będę jeszcze filtrował

people_bn_100 = marc_parser_1_field(bn_books, 'id', 'X100', '%', '|')
for column in people_bn_100.iloc[:,2:]:
    people_bn_100[column] = people_bn_100[column].str.strip()

# bn books 100, 700, 701
people_bn_100['location'] = 'bn_X100'
people_bn_700 = marc_parser_1_field(bn_books, 'id', 'X700', '%', '|')
people_bn_700['location'] = 'bn_X700'
people_bn_701 = marc_parser_1_field(bn_books, 'id', 'X701', '%', '|')
people_bn_701['location'] = 'bn_X701'
# cz books & articles 100, 600, 700
people_cz_100 = marc_parser_1_field(pd.concat([cz_books, cz_articles]), 'id', 'X100', '\$\$')
people_cz_100['location'] = 'cz_X100'
people_cz_600 = marc_parser_1_field(pd.concat([cz_books, cz_articles]), 'id', 'X600', '\$\$')
people_cz_600['location'] = 'cz_X600'
people_cz_700 = marc_parser_1_field(pd.concat([cz_books, cz_articles]), 'id', 'X700', '\$\$')
people_cz_700['location'] = 'cz_X700'

all_people = pd.concat([people_bn_100, people_bn_700, people_bn_701, people_cz_100, people_cz_600, people_cz_700])







count = all_people.groupby(["id", "location"]).size().reset_index(name="frequency")


X040 = marc_parser_1_field(oclc_lang, '001', '040', '\$')
count_040 = X040.groupby(["$a", "$b"]).size().reset_index(name="frequency")
count_040.to_excel('oclc_cataloguing_agency.xlsx', index=False)

'[A.A.], NA'[-4:]

name_form
project_ID
LOD_ID
Index_Name
Name
Given_Name
Pseudonym_Kryptonym
True_Name
Other_Name_Form
Addition_to_Name
Numeration
Numeration_Arabic
Birth
Death




# kartoteka intytucji
# bn books
#110
institutions_110 = marc_parser_1_field(bn_books, 'id', 'X110', '%')[['id', '%1', '%2', '%6']]
institutions_110.columns = [['id', 'Entity_Name', 'Related_Entity_Sub_Entity', 'Located_Location']]
institutions_110['MRC'] = '110'
institutions_110['subfield'] = '%1'
institutions_110['Related_Entity_Main_Entity'] = np.nan
sub_institutions_110 = marc_parser_1_field(bn_books, 'id', 'X110', '%')[['id', '%2', '%1', '%7']]
sub_institutions_110 = sub_institutions_110.loc[sub_institutions_110['%2'] != ""]
sub_institutions_110.columns = [['id', 'Entity_Name', 'Related_Entity_Main_Entity', 'Located_Location']]
sub_institutions_110['MRC'] = '110'
sub_institutions_110['subfield'] = '%2'
sub_institutions_110['Related_Entity_Sub_Entity'] = np.nan
bn_institutions110 = pd.concat([institutions_110, sub_institutions_110], axis = 0)
#120
institutions_120 = marc_parser_1_field(bn_books, 'id', 'X120', '%')[['id', '%1', '%2']]
institutions_120.columns = [['id', 'Entity_Name', 'Related_Entity_Sub_Entity']]
institutions_120['MRC'] = '120'
institutions_120['subfield'] = '%1'
institutions_120['Related_Entity_Main_Entity'] = np.nan
institutions_120['Located_Location'] = np.nan
sub_institutions_120 = marc_parser_1_field(bn_books, 'id', 'X120', '%')[['id', '%2', '%1']]
sub_institutions_120 = sub_institutions_120.loc[sub_institutions_120['%2'] != ""]
sub_institutions_120.columns = [['id', 'Entity_Name', 'Related_Entity_Main_Entity']]
sub_institutions_120['MRC'] = '120'
sub_institutions_120['subfield'] = '%2'
sub_institutions_120['Related_Entity_Sub_Entity'] = np.nan
sub_institutions_120['Located_Location'] = np.nan
bn_institutions120 = pd.concat([institutions_120, sub_institutions_120], axis = 0)
#210
X210 = bn_books[['id', 'X210']].dropna()
X210['rok_wydania'] = X210['X210'].str.extract(r'(?<=\%d)(.*?)(?=\%e|$)')
X210['bez_roku'] = X210['X210'].str.replace(r'.\%d.*', "")
X210['ile_wydawnictw'] = X210['bez_roku'].str.count(r'\%c')
X210['ile_miejsc'] = X210['bez_roku'].str.count(r'\%a')
X210['kolejnosc'] = X210['bez_roku'].str.findall(r'(?<=\%)(.)').str.join("")
X210['lista'] = X210['bez_roku'].str.split(r' (?=\%)')




marc_field_bnb_100$field <- "X100"
bn_record_people_X100 <- marc_field_bnb_100 %>%
  select(record_id = id, `%1`, `%2`, `%d`, `%4`, `%3`, `%s`,`%w`,`%p`,`%k`) %>%
  mutate(`%1` = paste(`%1`,`%3`,sep = " ")) %>%
  mutate(`%2` = paste(`%2`,`%4`,sep = " ")) %>%
  select(-`%3`,-`%4`) %>%
  mutate(data_set = "bn_books", field = "X100") %>%
  select(data_set, field, 1:8)
bn_record_people_X100[] <- lapply(bn_record_people_X100, gsub, pattern=' NA', replacement='')
bn_record_people_X100[] <- lapply(bn_record_people_X100, gsub, pattern='NANA', replacement='')
bn_record_people_X100[] <- lapply(bn_record_people_X100, gsub, pattern='NA$', replacement='')
bn_record_people_X100$name <- paste(bn_record_people_X100$`%1`,bn_record_people_X100$`%2`,bn_record_people_X100$`%d`,sep = "|")
bn_record_people_X100[] <- lapply(bn_record_people_X100, gsub, pattern='\\|NA', replacement='')
#field X700
marc_field_bnb_700 <- bn_books %>%
  select(id,X700)%>%
  filter(X700!="") %>%
  cSplit(.,"X700",sep = "|",direction = "long") %>%
  filter(X700!="")
subfield_list<- str_extract_all(bn_books$X700,"\\%.")
subfield_list<- unique(unlist(subfield_list))
empty_table<- data.frame(matrix(ncol = length(subfield_list),nrow = lengths(marc_field_bnb_700)[1]))
colnames(empty_table) <-subfield_list
marc_field_bnb_700<-cbind(marc_field_bnb_700,empty_table)
subfield_list_char <- paste("(",subfield_list,")",sep = "")
x <- 1:length(subfield_list)
for (i in x) {
  progress(match(i,x), max.value = length(x)) 
  marc_field_bnb_700$X700 <- str_replace(marc_field_bnb_700$X700,subfield_list_char[i],"|\\1")
}
subfield_list_char2 <- str_replace_all(subfield_list,"\\%","\\\\%")
for (i in x) {
  progress(match(i,x), max.value = length(x))
  string_a <- "(^)(.*?\\|"
  string_b <- subfield_list_char2[i]
  string_c <- ")(.*?)(\\,{0,1})((\\|\\%)(.*)|$)"
  string <- paste(string_a,string_b,string_c,sep = "")
  marc_field_bnb_700[,i+2] <- ifelse(grepl(subfield_list_char2[i],marc_field_bnb_700$X700),str_replace_all(gsub(string,"\\3",marc_field_bnb_700$X700),"\\%.", "~"),NA)
}
marc_field_bnb_700$`%1` <- trim(marc_field_bnb_700$`%1`)
marc_field_bnb_700$`%2` <- trim(marc_field_bnb_700$`%2`)
marc_field_bnb_700$`%d` <- trim(marc_field_bnb_700$`%d`)
marc_field_bnb_700$`%4` <- trim(marc_field_bnb_700$`%4`)
marc_field_bnb_700$`%3` <- trim(marc_field_bnb_700$`%3`)
marc_field_bnb_700$`%s` <- trim(marc_field_bnb_700$`%s`)
marc_field_bnb_700$`%w` <- trim(marc_field_bnb_700$`%w`)
marc_field_bnb_700$`%r` <- trim(marc_field_bnb_700$`%r`)
marc_field_bnb_700$`%m` <- trim(marc_field_bnb_700$`%m`)
marc_field_bnb_700$`%p` <- trim(marc_field_bnb_700$`%p`)
marc_field_bnb_700$`%k` <- trim(marc_field_bnb_700$`%k`)
marc_field_bnb_700$field <- "X700"
bn_record_people_X700 <- marc_field_bnb_700 %>%
  select(record_id = id, `%1`, `%2`, `%d`, `%4`, `%3`, `%s`,`%w`,`%p`,`%k`) %>%
  mutate(`%1` = paste(`%1`,`%3`,sep = " ")) %>%
  mutate(`%2` = paste(`%2`,`%4`,sep = " ")) %>%
  select(-`%3`,-`%4`) %>%
  mutate(data_set = "bn_books", field = "X700") %>%
  select(data_set, field, 1:8)
bn_record_people_X700[] <- lapply(bn_record_people_X700, gsub, pattern=' NA', replacement='')
bn_record_people_X700[] <- lapply(bn_record_people_X700, gsub, pattern='NANA', replacement='')
bn_record_people_X700[] <- lapply(bn_record_people_X700, gsub, pattern='NA$', replacement='')
bn_record_people_X700$name <- paste(bn_record_people_X700$`%1`,bn_record_people_X700$`%2`,bn_record_people_X700$`%d`,sep = "|")
bn_record_people_X700[] <- lapply(bn_record_people_X700, gsub, pattern='\\|NA', replacement='')
#field X701
marc_field_bnb_701 <- bn_books %>%
  select(id,X701)%>%
  filter(X701!="") %>%
  cSplit(.,"X701",sep = "|",direction = "long") %>%
  filter(X701!="")
subfield_list<- str_extract_all(bn_books$X701,"\\%.")
subfield_list<- unique(unlist(subfield_list))
empty_table<- data.frame(matrix(ncol = length(subfield_list),nrow = lengths(marc_field_bnb_701)[1]))
colnames(empty_table) <-subfield_list
marc_field_bnb_701<-cbind(marc_field_bnb_701,empty_table)
subfield_list_char <- paste("(",subfield_list,")",sep = "")
x <- 1:length(subfield_list)
for (i in x) {
  progress(match(i,x), max.value = length(x)) 
  marc_field_bnb_701$X701 <- str_replace(marc_field_bnb_701$X701,subfield_list_char[i],"|\\1")
}
subfield_list_char2 <- str_replace_all(subfield_list,"\\%","\\\\%")
for (i in x) {
  progress(match(i,x), max.value = length(x))
  string_a <- "(^)(.*?\\|"
  string_b <- subfield_list_char2[i]
  string_c <- ")(.*?)(\\,{0,1})((\\|\\%)(.*)|$)"
  string <- paste(string_a,string_b,string_c,sep = "")
  marc_field_bnb_701[,i+2] <- ifelse(grepl(subfield_list_char2[i],marc_field_bnb_701$X701),str_replace_all(gsub(string,"\\3",marc_field_bnb_701$X701),"\\%.", "~"),NA)
}
marc_field_bnb_701$`%1` <- trim(marc_field_bnb_701$`%1`)
marc_field_bnb_701$`%2` <- trim(marc_field_bnb_701$`%2`)
marc_field_bnb_701$`%d` <- trim(marc_field_bnb_701$`%d`)
marc_field_bnb_701$`%3` <- trim(marc_field_bnb_701$`%3`)
marc_field_bnb_701$`%s` <- trim(marc_field_bnb_701$`%s`)
marc_field_bnb_701$`%r` <- trim(marc_field_bnb_701$`%r`)
marc_field_bnb_701$`%p` <- trim(marc_field_bnb_701$`%p`)
marc_field_bnb_701$field <- "X701"
bn_record_people_X701 <- marc_field_bnb_701 %>%
  select(record_id = id, `%1`, `%2`, `%d`, `%3`, `%s`, `%p`) %>%
  mutate(`%4` = NA, `%w` = NA, `%k` = NA) %>%
  mutate(`%1` = paste(`%1`,`%3`,sep = " ")) %>%
  mutate(`%2` = paste(`%2`,`%4`,sep = " ")) %>%
  select(-`%3`,-`%4`) %>%
  mutate(data_set = "bn_books", field = "X701") %>%
  select(colnames(bn_record_people_X100)[1:10])
bn_record_people_X701[] <- lapply(bn_record_people_X701, gsub, pattern=' NA', replacement='')
bn_record_people_X701[] <- lapply(bn_record_people_X701, gsub, pattern='NANA', replacement='')
bn_record_people_X701[] <- lapply(bn_record_people_X701, gsub, pattern='NA$', replacement='')
bn_record_people_X701$name <- paste(bn_record_people_X701$`%1`,bn_record_people_X701$`%2`,bn_record_people_X701$`%d`,sep = "|")
bn_record_people_X701[] <- lapply(bn_record_people_X701, gsub, pattern='\\|NA', replacement='')
#concatenate bn
bn_record_people <- rbind(bn_record_people_X100,bn_record_people_X700,bn_record_people_X701) 
bn_record_people <- bn_record_people %>%
  mutate(id_osoby = paste("bnb",seq.int(nrow(bn_record_people)),sep = ""))
names_of_columns <- colnames(bn_record_people)
set1 <- bn_record_people %>%
  select(1:6,11:12) %>%
  mutate(`%1` = paste(`%1`,`%2`,`%d`, sep = " ")) %>% 
  select(data_set,field,record_id,`%1`,name,id_osoby)
set1 <- set1 %>%
  mutate(subfield = colnames(set1)[4])
colnames(set1)[4] <- "name_form"
set1[] <- lapply(set1, gsub, pattern=' NA', replacement='')
set1$name_form <- trim(set1$name_form)
set2 <- bn_record_people[c(1:3,7,11:12)] %>%
  filter(!is.na(`%s`))
set2 <- set2 %>%
  mutate(subfield = colnames(set2)[4])
colnames(set2)[4] <- "name_form"
set3 <- bn_record_people[c(1:3,8,11:12)] %>%
  filter(!is.na(`%w`))
set3 <- set3 %>%
  mutate(subfield = colnames(set3)[4])
colnames(set3)[4] <- "name_form"
set4 <- bn_record_people[c(1:3,9,11:12)] %>%
  filter(!is.na(`%p`))
set4 <- set4 %>%
  mutate(subfield = colnames(set4)[4])
colnames(set4)[4] <- "name_form"
set5 <- bn_record_people[c(1:3,10,11:12)] %>%
  filter(!is.na(`%k`))
set5 <- set5 %>%
  mutate(subfield = colnames(set5)[4])
colnames(set5)[4] <- "name_form"
bn_record_people <- rbind(set1,set2,set3,set4,set5)
bn_record_people <- bn_record_people %>%
  select(data_set,record_id,field,subfield,id_osoby,name_form,id_osoby) %>%
  mutate(name_form_id = paste(data_set,record_id,id_osoby,field,subfield,sep = "-")) %>%
  mutate(name_simple = str_replace_all(str_to_lower(name_form), "\\W", ""))
#cz_books
#field X100
marc_field_czb_100 <- cz_books %>%
  select(id,X100)%>%
  filter(X100!="") %>%
  cSplit(.,"X100",sep = "|",direction = "long") %>%
  filter(X100!="")
marc_field_czb_100 <- mutate(marc_field_czb_100,
               indicator = str_replace_all(marc_field_czb_100$X100,"(^.*?)(\\$.*)","\\1"))
subfield_list<- str_extract_all(cz_books$X100,"\\${2}.")
subfield_list<- unique(unlist(subfield_list))
empty_table<- data.frame(matrix(ncol = length(subfield_list),nrow = lengths(marc_field_czb_100)[1]))
colnames(empty_table) <-subfield_list
marc_field_czb_100<-cbind(marc_field_czb_100,empty_table)
subfield_list_char <- paste("(",subfield_list,")",sep = "")
subfield_list_char <- str_replace_all(subfield_list_char,"\\$","\\\\$")
x <- 1:length(subfield_list)
for (i in x) {
  marc_field_czb_100$X100 <- str_replace(marc_field_czb_100$X100,subfield_list_char[i],"|\\1")
  progress(match(i,x), max.value = length(x)) 
}
for (i in x) {
  progress(match(i,x), max.value = length(x))
  subfield_list_char2 <- str_replace_all(subfield_list,"\\$","\\\\$")
  string_a <- "(^)(.*?\\|"
  string_b <- subfield_list_char2[i]
  string_c <- ")(.*?)(\\,{0,1})((\\|\\${2})(.*)|$)"
  string <- paste(string_a,string_b,string_c,sep = "")
  marc_field_czb_100[,i+3] <- ifelse(grepl(subfield_list_char2[i],marc_field_czb_100$X100),str_replace_all(gsub(string,"\\3",marc_field_czb_100$X100),"\\${2}.", "~"),NA)
}
marc_field_czb_100$`$$a` <- trim(marc_field_czb_100$`$$a`)
marc_field_czb_100$`$$d` <- trim(marc_field_czb_100$`$$d`)
marc_field_czb_100$`$$b` <- str_remove(trim(marc_field_czb_100$`$$b`),"\\.$")
marc_field_czb_100$field <- "X100"
cz_record_people_X100 <- marc_field_czb_100 %>%
  select(record_id = id, `$$a`, `$$b`, `$$d`) %>%
  mutate(`$$a` = paste(`$$a`,`$$b`,sep = " ")) %>%
  mutate(name = paste(`$$a`,`$$d`,sep = "|")) %>%
  select(-3) %>%
  mutate(data_set = "cz_books", field = "X100") %>%
  select(data_set, field,1:4)
cz_record_people_X100[] <- lapply(cz_record_people_X100, gsub, pattern=' NA', replacement='')
cz_record_people_X100[] <- lapply(cz_record_people_X100, gsub, pattern='\\|NA', replacement='')
#field X600
marc_field_czb_600 <- cz_books %>%
  select(id,X600)%>%
  filter(X600!="") %>%
  cSplit(.,"X600",sep = "|",direction = "long") %>%
  filter(X600!="")
marc_field_czb_600 <- mutate(marc_field_czb_600,
               indicator = str_replace_all(marc_field_czb_600$X600,"(^.*?)(\\$.*)","\\1"))
subfield_list<- str_extract_all(cz_books$X600,"\\${2}.")
subfield_list<- unique(unlist(subfield_list))
empty_table<- data.frame(matrix(ncol = length(subfield_list),nrow = lengths(marc_field_czb_600)[1]))
colnames(empty_table) <-subfield_list
marc_field_czb_600<-cbind(marc_field_czb_600,empty_table)
subfield_list_char <- paste("(",subfield_list,")",sep = "")
subfield_list_char <- str_replace_all(subfield_list_char,"\\$","\\\\$")
x <- 1:length(subfield_list)
for (i in x) {
  marc_field_czb_600$X600 <- str_replace(marc_field_czb_600$X600,subfield_list_char[i],"|\\1")
  progress(match(i,x), max.value = length(x)) 
}
for (i in x) {
  progress(match(i,x), max.value = length(x))
  subfield_list_char2 <- str_replace_all(subfield_list,"\\$","\\\\$")
  string_a <- "(^)(.*?\\|"
  string_b <- subfield_list_char2[i]
  string_c <- ")(.*?)(\\,{0,1})((\\|\\${2})(.*)|$)"
  string <- paste(string_a,string_b,string_c,sep = "")
  marc_field_czb_600[,i+3] <- ifelse(grepl(subfield_list_char2[i],marc_field_czb_600$X600),str_replace_all(gsub(string,"\\3",marc_field_czb_600$X600),"\\${2}.", "~"),NA)
}
marc_field_czb_600$`$$a` <- trim(marc_field_czb_600$`$$a`)
marc_field_czb_600$`$$d` <- trim(marc_field_czb_600$`$$d`)
marc_field_czb_600$`$$b` <- str_remove(trim(marc_field_czb_600$`$$b`),"\\.$")
marc_field_czb_600$field <- "X600"
cz_record_people_X600 <- marc_field_czb_600 %>%
  select(record_id = id, `$$a`, `$$b`, `$$d`) %>%
  mutate(`$$a` = paste(`$$a`,`$$b`,sep = " ")) %>%
  mutate(name = paste(`$$a`,`$$d`,sep = "|")) %>%
  select(-3) %>%
  mutate(data_set = "cz_books", field = "X600") %>%
  select(data_set, field,1:4)
cz_record_people_X600[] <- lapply(cz_record_people_X600, gsub, pattern=' NA', replacement='')
cz_record_people_X600[] <- lapply(cz_record_people_X600, gsub, pattern='\\|NA', replacement='')
#field X700
marc_field_czb_700 <- cz_books %>%
  select(id,X700)%>%
  filter(X700!="") %>%
  cSplit(.,"X700",sep = "|",direction = "long") %>%
  filter(X700!="")
marc_field_czb_700 <- mutate(marc_field_czb_700,
               indicator = str_replace_all(marc_field_czb_700$X700,"(^.*?)(\\$.*)","\\1"))
subfield_list<- str_extract_all(cz_books$X700,"\\${2}.")
subfield_list<- unique(unlist(subfield_list))
empty_table<- data.frame(matrix(ncol = length(subfield_list),nrow = lengths(marc_field_czb_700)[1]))
colnames(empty_table) <-subfield_list
marc_field_czb_700<-cbind(marc_field_czb_700,empty_table)
subfield_list_char <- paste("(",subfield_list,")",sep = "")
subfield_list_char <- str_replace_all(subfield_list_char,"\\$","\\\\$")
x <- 1:length(subfield_list)
for (i in x) {
  marc_field_czb_700$X700 <- str_replace(marc_field_czb_700$X700,subfield_list_char[i],"|\\1")
  progress(match(i,x), max.value = length(x)) 
}
for (i in x) {
  progress(match(i,x), max.value = length(x))
  subfield_list_char2 <- str_replace_all(subfield_list,"\\$","\\\\$")
  string_a <- "(^)(.*?\\|"
  string_b <- subfield_list_char2[i]
  string_c <- ")(.*?)(\\,{0,1})((\\|\\${2})(.*)|$)"
  string <- paste(string_a,string_b,string_c,sep = "")
  marc_field_czb_700[,i+3] <- ifelse(grepl(subfield_list_char2[i],marc_field_czb_700$X700),str_replace_all(gsub(string,"\\3",marc_field_czb_700$X700),"\\${2}.", "~"),NA)
}
marc_field_czb_700$`$$a` <- trim(marc_field_czb_700$`$$a`)
marc_field_czb_700$`$$d` <- trim(marc_field_czb_700$`$$d`)
marc_field_czb_700$`$$b` <- str_remove(trim(marc_field_czb_700$`$$b`),"\\.$")
marc_field_czb_700$field <- "X700"
cz_record_people_X700 <- marc_field_czb_700 %>%
  select(record_id = id, `$$a`, `$$b`, `$$d`) %>%
  mutate(`$$a` = paste(`$$a`,`$$b`,sep = " ")) %>%
  mutate(name = paste(`$$a`,`$$d`,sep = "|")) %>%
  select(-3) %>%
  mutate(data_set = "cz_books", field = "X700") %>%
  select(data_set, field,1:4)
cz_record_people_X700[] <- lapply(cz_record_people_X700, gsub, pattern=' NA', replacement='')
cz_record_people_X700[] <- lapply(cz_record_people_X700, gsub, pattern='\\|NA', replacement='')
#concatenate all cz books
cz_record_people <- rbind(cz_record_people_X100,cz_record_people_X600,cz_record_people_X700)
cz_record_people <- cz_record_people %>%
  mutate(id_osoby = paste("czb",seq.int(nrow(cz_record_people)),sep = ""))
names_of_columns <- colnames(cz_record_people)
cz_record_people <- cz_record_people %>%
  select(names_of_columns,id_osoby) %>%
  arrange(field,as.integer(record_id))
cz_record_people <- cz_record_people %>%
  mutate(name_form = paste(`$$a`, `$$d`, sep = " ")) %>% 
  select(data_set,field,record_id,name_form,name,id_osoby)
cz_record_people <- cz_record_people %>%
  mutate(subfield = "$$a")
cz_record_people[] <- lapply(cz_record_people, gsub, pattern=' NA', replacement='')
cz_record_people$name_form <- trim(cz_record_people$name_form)
cz_record_people <- cz_record_people %>%
  select(data_set,record_id,field,subfield,id_osoby,name_form) %>%
  mutate(name_form_id = paste(data_set,record_id,id_osoby,field,subfield,sep = "-")) %>%
  mutate(name_simple = str_replace_all(str_to_lower(name_form), "\\W", ""))
#cz_articles
#field X100
marc_field_cza_100 <- cz_articles %>%
  select(id,X100)%>%
  filter(X100!="") %>%
  cSplit(.,"X100",sep = "|",direction = "long") %>%
  filter(X100!="")
marc_field_cza_100 <- mutate(marc_field_cza_100,
               indicator = str_replace_all(marc_field_cza_100$X100,"(^.*?)(\\$.*)","\\1"))
subfield_list<- str_extract_all(cz_articles$X100,"\\${2}.")
subfield_list<- unique(unlist(subfield_list))
empty_table<- data.frame(matrix(ncol = length(subfield_list),nrow = lengths(marc_field_cza_100)[1]))
colnames(empty_table) <-subfield_list
marc_field_cza_100<-cbind(marc_field_cza_100,empty_table)
subfield_list_char <- paste("(",subfield_list,")",sep = "")
subfield_list_char <- str_replace_all(subfield_list_char,"\\$","\\\\$")
x <- 1:length(subfield_list)
for (i in x) {
  marc_field_cza_100$X100 <- str_replace(marc_field_cza_100$X100,subfield_list_char[i],"|\\1")
  progress(match(i,x), max.value = length(x)) 
}
for (i in x) {
  progress(match(i,x), max.value = length(x))
  subfield_list_char2 <- str_replace_all(subfield_list,"\\$","\\\\$")
  string_a <- "(^)(.*?\\|"
  string_b <- subfield_list_char2[i]
  string_c <- ")(.*?)(\\,{0,1})((\\|\\${2})(.*)|$)"
  string <- paste(string_a,string_b,string_c,sep = "")
  marc_field_cza_100[,i+3] <- ifelse(grepl(subfield_list_char2[i],marc_field_cza_100$X100),str_replace_all(gsub(string,"\\3",marc_field_cza_100$X100),"\\${2}.", "~"),NA)
}
marc_field_cza_100$`$$a` <- trim(marc_field_cza_100$`$$a`)
marc_field_cza_100$`$$d` <- trim(marc_field_cza_100$`$$d`)
marc_field_cza_100$`$$b` <- str_remove(trim(marc_field_cza_100$`$$b`),"\\.$")
marc_field_cza_100$field <- "X100"
cz_record_people_art_X100 <- marc_field_cza_100 %>%
  select(record_id = id, `$$a`, `$$b`, `$$d`) %>%
  mutate(`$$a` = paste(`$$a`,`$$b`,sep = " ")) %>%
  mutate(name = paste(`$$a`,`$$d`,sep = "|")) %>%
  select(-3) %>%
  mutate(data_set = "cz_articles", field = "X100") %>%
  select(data_set, field,1:4)
cz_record_people_art_X100[] <- lapply(cz_record_people_art_X100, gsub, pattern=' NA', replacement='')
cz_record_people_art_X100[] <- lapply(cz_record_people_art_X100, gsub, pattern='\\|NA', replacement='')
#field X600
marc_field_cza_600 <- cz_articles %>%
  select(id,X600)%>%
  filter(X600!="") %>%
  cSplit(.,"X600",sep = "|",direction = "long") %>%
  filter(X600!="")
marc_field_cza_600 <- mutate(marc_field_cza_600,
               indicator = str_replace_all(marc_field_cza_600$X600,"(^.*?)(\\$.*)","\\1"))
subfield_list<- str_extract_all(cz_articles$X600,"\\${2}.")
subfield_list<- unique(unlist(subfield_list))
empty_table<- data.frame(matrix(ncol = length(subfield_list),nrow = lengths(marc_field_cza_600)[1]))
colnames(empty_table) <-subfield_list
marc_field_cza_600<-cbind(marc_field_cza_600,empty_table)
subfield_list_char <- paste("(",subfield_list,")",sep = "")
subfield_list_char <- str_replace_all(subfield_list_char,"\\$","\\\\$")
x <- 1:length(subfield_list)
for (i in x) {
  marc_field_cza_600$X600 <- str_replace(marc_field_cza_600$X600,subfield_list_char[i],"|\\1")
  progress(match(i,x), max.value = length(x)) 
}
for (i in x) {
  progress(match(i,x), max.value = length(x))
  subfield_list_char2 <- str_replace_all(subfield_list,"\\$","\\\\$")
  string_a <- "(^)(.*?\\|"
  string_b <- subfield_list_char2[i]
  string_c <- ")(.*?)(\\,{0,1})((\\|\\${2})(.*)|$)"
  string <- paste(string_a,string_b,string_c,sep = "")
  marc_field_cza_600[,i+3] <- ifelse(grepl(subfield_list_char2[i],marc_field_cza_600$X600),str_replace_all(gsub(string,"\\3",marc_field_cza_600$X600),"\\${2}.", "~"),NA)
}
marc_field_cza_600$`$$a` <- trim(marc_field_cza_600$`$$a`)
marc_field_cza_600$`$$d` <- trim(marc_field_cza_600$`$$d`)
marc_field_cza_600$`$$b` <- str_remove(trim(marc_field_cza_600$`$$b`),"\\.$")
marc_field_cza_600$field <- "X600"
cz_record_people_art_X600 <- marc_field_cza_600 %>%
  select(record_id = id, `$$a`, `$$b`, `$$d`) %>%
  mutate(`$$a` = paste(`$$a`,`$$b`,sep = " ")) %>%
  mutate(name = paste(`$$a`,`$$d`,sep = "|")) %>%
  select(-3) %>%
  mutate(data_set = "cz_articles", field = "X600") %>%
  select(data_set, field,1:4)
cz_record_people_art_X600[] <- lapply(cz_record_people_art_X600, gsub, pattern=' NA', replacement='')
cz_record_people_art_X600[] <- lapply(cz_record_people_art_X600, gsub, pattern='\\|NA', replacement='')
#field X700
marc_field_cza_700 <- cz_articles %>%
  select(id,X700)%>%
  filter(X700!="") %>%
  cSplit(.,"X700",sep = "|",direction = "long") %>%
  filter(X700!="")
marc_field_cza_700 <- mutate(marc_field_cza_700,
               indicator = str_replace_all(marc_field_cza_700$X700,"(^.*?)(\\$.*)","\\1"))
subfield_list<- str_extract_all(cz_articles$X700,"\\${2}.")
subfield_list<- unique(unlist(subfield_list))
empty_table<- data.frame(matrix(ncol = length(subfield_list),nrow = lengths(marc_field_cza_700)[1]))
colnames(empty_table) <-subfield_list
marc_field_cza_700<-cbind(marc_field_cza_700,empty_table)
subfield_list_char <- paste("(",subfield_list,")",sep = "")
subfield_list_char <- str_replace_all(subfield_list_char,"\\$","\\\\$")
x <- 1:length(subfield_list)
for (i in x) {
  marc_field_cza_700$X700 <- str_replace(marc_field_cza_700$X700,subfield_list_char[i],"|\\1")
  progress(match(i,x), max.value = length(x)) 
}
for (i in x) {
  progress(match(i,x), max.value = length(x))
  subfield_list_char2 <- str_replace_all(subfield_list,"\\$","\\\\$")
  string_a <- "(^)(.*?\\|"
  string_b <- subfield_list_char2[i]
  string_c <- ")(.*?)(\\,{0,1})((\\|\\${2})(.*)|$)"
  string <- paste(string_a,string_b,string_c,sep = "")
  marc_field_cza_700[,i+3] <- ifelse(grepl(subfield_list_char2[i],marc_field_cza_700$X700),str_replace_all(gsub(string,"\\3",marc_field_cza_700$X700),"\\${2}.", "~"),NA)
}
marc_field_cza_700$`$$a` <- trim(marc_field_cza_700$`$$a`)
marc_field_cza_700$`$$d` <- trim(marc_field_cza_700$`$$d`)
marc_field_cza_700$field <- "X700"
cz_record_people_art_X700 <- marc_field_cza_700 %>%
  select(record_id = id, `$$a`,`$$d`) %>%
  mutate(name = paste(`$$a`,`$$d`,sep = "|")) %>%
  mutate(data_set = "cz_articles", field = "X700") %>%
  select(data_set, field,1:4)
cz_record_people_art_X700[] <- lapply(cz_record_people_art_X700, gsub, pattern=' NA', replacement='')
cz_record_people_art_X700[] <- lapply(cz_record_people_art_X700, gsub, pattern='\\|NA', replacement='')
#concatenate all cz art
cz_record_people_art <- rbind(cz_record_people_art_X100,cz_record_people_art_X600,cz_record_people_art_X700)
cz_record_people_art <- cz_record_people_art %>%
  mutate(id_osoby = paste("cza",seq.int(nrow(cz_record_people_art)),sep = ""))
names_of_columns <- colnames(cz_record_people_art)
cz_record_people_art <- cz_record_people_art %>%
  select(names_of_columns,id_osoby) %>%
  arrange(field,as.integer(record_id))
cz_record_people_art <- cz_record_people_art %>%
  mutate(name_form = paste(`$$a`, `$$d`, sep = " ")) %>% 
  select(data_set,field,record_id,name_form,name,id_osoby)
cz_record_people_art <- cz_record_people_art %>%
  mutate(subfield = "$$a")
cz_record_people_art[] <- lapply(cz_record_people_art, gsub, pattern=' NA', replacement='')
cz_record_people_art$name_form <- trim(cz_record_people_art$name_form)
cz_record_people_art <- cz_record_people_art %>%
  select(data_set,record_id,field,subfield,id_osoby,name_form) %>%
  mutate(name_form_id = paste(data_set,record_id,id_osoby,field,subfield,sep = "-")) %>%
  mutate(name_simple = str_replace_all(str_to_lower(name_form), "\\W", ""))
#pbl books
pbl_record_people_books_creators <- pbl_books %>%
  select(rekord_id,tworca_id,tworca_nazwisko,tworca_imie) %>%
  filter(!is.na(tworca_id))
pbl_record_people_books_creators <- pbl_record_people_books_creators %>%
  mutate(data_set = "pbl_books") %>%
  mutate(field = "tworcy") %>%
  mutate(name = paste(tworca_nazwisko,tworca_imie,sep = "|")) %>%
  mutate(name_form = paste(tworca_nazwisko,tworca_imie,sep = ", ")) %>%
  mutate(subfield = NA) %>%
  mutate(id_osoby = paste("pblt",tworca_id, sep = "")) %>%
  mutate(name_form_id = paste(data_set,rekord_id,id_osoby,field,subfield,sep = "-")) %>%
  mutate(name_simple = str_replace_all(str_to_lower(name_form), "\\W", "")) %>%
  select(data_set, record_id = rekord_id, field, subfield, id_osoby, name_form, name_form_id, name_simple)
pbl_record_people_books_authors <- pbl_books %>%
  select(rekord_id,autor_id,autor_nazwisko,autor_imie) %>%
  filter(!is.na(autor_id))
pbl_record_people_books_authors <- pbl_record_people_books_authors %>%
  mutate(data_set = "pbl_books") %>%
  mutate(field = "autorzy") %>%
  mutate(name = paste(autor_nazwisko,autor_imie,sep = "|")) %>%
  mutate(name_form = paste(autor_nazwisko,autor_imie,sep = ", ")) %>%
  mutate(subfield = NA) %>%
  mutate(id_osoby = paste("pbla",autor_id, sep = "")) %>%
  mutate(name_form_id = paste(data_set,rekord_id,id_osoby,field,subfield,sep = "-")) %>%
  mutate(name_simple = str_replace_all(str_to_lower(name_form), "\\W", "")) %>%
  select(data_set, record_id = rekord_id, field, subfield, id_osoby, name_form, name_form_id, name_simple)
pbl_record_people_books_coworkers <- pbl_books %>%
  select(rekord_id,wspoltworca_id,wspoltworca_nazwisko,wspoltworca_imie) %>%
  filter(!is.na(wspoltworca_id))
pbl_record_people_books_coworkers <- pbl_record_people_books_coworkers %>%
  mutate(data_set = "pbl_books") %>%
  mutate(field = "wspoltworcy") %>%
  mutate(name = paste(wspoltworca_nazwisko,wspoltworca_imie,sep = "|")) %>%
  mutate(name_form = paste(wspoltworca_nazwisko,wspoltworca_imie,sep = ", ")) %>%
  mutate(subfield = NA) %>%
  mutate(id_osoby = paste("pblw",wspoltworca_id, sep = "")) %>%
  mutate(name_form_id = paste(data_set,rekord_id,id_osoby,field,subfield,sep = "-")) %>%
  mutate(name_simple = str_replace_all(str_to_lower(name_form), "\\W", "")) %>%
  select(data_set, record_id = rekord_id, field, subfield, id_osoby, name_form, name_form_id, name_simple)
pbl_record_people_books <- rbind(pbl_record_people_books_creators,pbl_record_people_books_authors,pbl_record_people_books_coworkers)
#pbl_articles
pbl_record_people_art_creators <- pbl_articles %>%
  select(rekord_id,tworca_id,tworca_nazwisko,tworca_imie) %>%
  filter(!is.na(tworca_id))
pbl_record_people_art_creators <- pbl_record_people_art_creators %>%
  mutate(data_set = "pbl_articles") %>%
  mutate(field = "tworcy") %>%
  mutate(name = paste(tworca_nazwisko,tworca_imie,sep = "|")) %>%
  mutate(name_form = paste(tworca_nazwisko,tworca_imie,sep = ", ")) %>%
  mutate(subfield = NA) %>%
  mutate(id_osoby = paste("pblt",tworca_id, sep = "")) %>%
  mutate(name_form_id = paste(data_set,rekord_id,id_osoby,field,subfield,sep = "-")) %>%
  mutate(name_simple = str_replace_all(str_to_lower(name_form), "\\W", "")) %>%
  select(data_set, record_id = rekord_id, field, subfield, id_osoby, name_form, name_form_id, name_simple)
pbl_record_people_art_authors <- pbl_articles %>%
  select(rekord_id,autor_id,autor_nazwisko,autor_imie) %>%
  filter(!is.na(autor_id))
pbl_record_people_art_authors <- pbl_record_people_art_authors %>%
  mutate(data_set = "pbl_articles") %>%
  mutate(field = "autorzy") %>%
  mutate(name = paste(autor_nazwisko,autor_imie,sep = "|")) %>%
  mutate(name_form = paste(autor_nazwisko,autor_imie,sep = ", ")) %>%
  mutate(subfield = NA) %>%
  mutate(id_osoby = paste("pbla",autor_id, sep = "")) %>%
  mutate(name_form_id = paste(data_set,rekord_id,id_osoby,field,subfield,sep = "-")) %>%
  mutate(name_simple = str_replace_all(str_to_lower(name_form), "\\W", "")) %>%
  select(data_set, record_id = rekord_id, field, subfield, id_osoby, name_form, name_form_id, name_simple)
pbl_record_people_art_coworkers <- pbl_articles %>%
  select(rekord_id,wspoltworca_id,wspoltworca_nazwisko,wspoltworca_imie) %>%
  filter(!is.na(wspoltworca_id))
pbl_record_people_art_coworkers <- pbl_record_people_art_coworkers %>%
  mutate(data_set = "pbl_articles") %>%
  mutate(field = "wspoltworcy") %>%
  mutate(name = paste(wspoltworca_nazwisko,wspoltworca_imie,sep = "|")) %>%
  mutate(name_form = paste(wspoltworca_nazwisko,wspoltworca_imie,sep = ", ")) %>%
  mutate(subfield = NA) %>%
  mutate(id_osoby = paste("pblw",wspoltworca_id, sep = "")) %>%
  mutate(name_form_id = paste(data_set,rekord_id,id_osoby,field,subfield,sep = "-")) %>%
  mutate(name_simple = str_replace_all(str_to_lower(name_form), "\\W", "")) %>%
  select(data_set, record_id = rekord_id, field, subfield, id_osoby, name_form, name_form_id, name_simple)
pbl_record_people_art <- rbind(pbl_record_people_art_creators,pbl_record_people_art_authors,pbl_record_people_art_coworkers)
```

```{r merge personal data}
#putting all together
record_people <- rbind(bn_record_people,cz_record_people,cz_record_people_art,pbl_record_people_books,pbl_record_people_art)
#test <- record_people %>%
#  filter(grepl("Amsterdamski|Żerski",name_form))
#id tymczas, nazwa, id nazwy, id osoby
tab_2 <- record_people %>%
  select(name_form,name_form_id,name_simple,id_osoby) %>%
  group_by(name_simple) %>%
  mutate(name_form_id = paste(unique(name_form_id),collapse = "|")) %>%
  mutate(name_form = paste(unique(name_form), collapse = "|")) %>%
  mutate(id_osoby = paste(unique(id_osoby), collapse = "|")) %>%
  ungroup() %>%
  unique()
tab_2$temp_id <- seq.int(nrow(tab_2))
tab_2 <- tab_2 %>%
  select(temp_id,name_form, name_simple, name_form_id, id_osoby)
# łączenie po id osoby
tab_3 <- tab_2 %>%
  mutate(name_simple_short = str_extract(name_simple,"(^[a-zaáàâãäăāåąæeéèêëěēėęiíìîïīįioóòôõöőøœuúùûüűūůyýcćčçdďđđgģğkķlłļnńñňņŋrřsśšşsßtťŧþţzżźž]+)")) %>%
  group_by(name_simple_short) %>%
  mutate(temp_id = paste(unique(temp_id),collapse = "|")) %>%
  mutate(name_form_id = paste(unique(name_form_id),collapse = "|")) %>%
  mutate(name_form = paste(unique(name_form), collapse = "|")) %>%
  mutate(id_osoby = paste(unique(id_osoby), collapse = "|")) %>%
  mutate(name_simple = paste(unique(name_simple), collapse = "|")) %>%
  ungroup() %>%
  unique() %>%
  select(-name_simple_short)
id_osoby <- tab_3 %>%
  select(id_osoby,temp_id2 = temp_id)
id_osoby <- cSplit(id_osoby,"id_osoby",sep = "|",direction = "long") %>%
  group_by(id_osoby) %>%
  mutate(temp_id2 = paste(temp_id2,collapse = "|")) %>%
  ungroup() %>%
  unique() %>%
  mutate(id_osoby = paste(id_osoby,"|",sep = ""))
tab_3$id_osoby <- paste(tab_3$id_osoby,"|",sep = "")
tab_4 <- sqldf::sqldf("select *
                     from tab_3 a
                     join id_osoby b on a.id_osoby like ('%'||b.id_osoby||'%')") %>%
  select(-temp_id,-id_osoby..6) %>%
  unique() %>%
  mutate(temp_id2 = str_extract(temp_id2,"(^\\d+)(?=\\||$)")) %>%
  group_by(temp_id2) %>%
  mutate(name_form = paste(unique(name_form),collapse = "|")) %>%
  mutate(name_simple = paste(unique(name_simple),collapse = "|")) %>%
  mutate(name_form_id = paste(unique(name_form_id),collapse = "|")) %>%
  mutate(id_osoby = paste(unique(id_osoby),collapse = "")) %>%
  ungroup() %>%
  unique()
x <- 1:length(tab_4$temp_id2)
for (i in x) {
  progress(match(i,x), max.value = length(x)) 
  tab_4$id_osoby[i] <- paste(paste(unique(unlist(strsplit(tab_4$id_osoby[i],"\\|"))),collapse = "|"),"|",sep = "")
}
tab_4 <- tab_4 %>%
  select(id = temp_id2,name_form,name_simple,name_form_id,id_osoby)
write.csv2(tab_4, "C:/Users/Cezary/Desktop/samizdat_kartoteka_osób.csv", row.names = F, na = '', fileEncoding = 'UTF-8')
```


























