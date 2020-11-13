# przygotowanie książek do importu
import pandas as pd
from my_functions import marc_parser_1_field, unique_elem_from_column_split, cSplit, replacenth, gsheet_to_df
import re
from functools import reduce
import numpy as np
import copy

bn_books = pd.read_csv("C:/Users/Cezary/Desktop/bn_books.csv", sep=';')
bn_books.iloc[142, 18] = '%fWspomnienia 1939-1945 : (fragmenty) %bPużak Kazimierz %ss. 58-130 %z1|%fPolitycy i żołnierze %bGarliński Józef %ss. 131-147 %z2|%aZawiera : "Proces szesnastu" w Moskwie : (wspomnienia osobiste) / K.      Bagiński. Wspomnienia 1939-1945 : (fragmenty) / K. Pużak. Politycy i      żołnierze / J. Garliński %bBagiński Kazimierz %f"Proces szesnastu" w      Moskwie %ss. 3-57 %z0'
cz_books = pd.read_csv("C:/Users/Cezary/Desktop/cz_books.csv", sep=';')
cz_articles = pd.read_csv("C:/Users/Cezary/Desktop/cz_articles.csv", sep=';')
pbl_books = pd.read_csv("C:/Users/Cezary/Desktop/pbl_books.csv", sep=';')
pbl_articles = pd.read_csv("C:/Users/Cezary/Desktop/pbl_articles.csv", sep=';')


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

# merge and collapse

samizdat_institutions = gsheet_to_df('15pI92bcYWOMpSWaqtmQbOAPZys-SzesWbbMzc2zWqDY', 'ver_3').drop_duplicates()








































