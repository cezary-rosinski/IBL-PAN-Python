# przygotowanie książek do importu
import pandas as pd
from my_functions import marc_parser_1_field
from my_functions import unique_elem_from_column_split
from my_functions import unique_elem_from_column_regex
from my_functions import cSplit
from my_functions import marc_parser_1_field_simple
import re
from functools import reduce
import numpy as np
import copy


bn_books = pd.read_csv("C:/Users/Cezary/Desktop/bn_books.csv", sep=';')
cz_books = pd.read_csv("C:/Users/Cezary/Desktop/cz_books.csv", sep=';')
cz_articles = pd.read_csv("C:/Users/Cezary/Desktop/cz_articles.csv", sep=';')
pbl_books = pd.read_csv("C:/Users/Cezary/Desktop/pbl_books.csv", sep=';')
pbl_articles = pd.read_csv("C:/Users/Cezary/Desktop/pbl_articles.csv", sep=';')

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
title_cw_fixed['author'] = np.where(title_cw_fixed.apply(lambda x: x['%a'] in x['%f'], axis = 1), title_cw_fixed['%b'], '')
title_cw_fixed = title_cw_fixed.loc[title_cw_fixed['author'] != ''][['index', 'author']]


df2 = title_cw_fixed.set_index('index')
df1 = df2.combine_first(contained_work.set_index('index')).reset_index().reindex(columns=contained_work.columns)


contained_work['author'] = np.where(title_cw_fixed['index'] == contained_work['index'], title_cw_fixed['author'], contained_work['author'])


contained_work.loc[(contained_work['X200'].str.contains('%z')==True) & 
                   ((contained_work['X300'].str.contains('%b')==True) |
                    (contained_work['X300'].str.contains('%f')==True)), 
                   ['title', 'flag', 'typ']] = [contained_work.loc[(contained_work['X200'].str.contains('%z')==True) & 
                                                                   ((contained_work['X300'].str.contains('%b')==True) |
                                                                    (contained_work['X300'].str.contains('%f')==True))], 'REV', 'współwydane bez wspólnego tytułu (zawiera 200%z)2']

#wpisać zmiany do obiektów i wykorzystać te obiekty do nadpisania - kod będzie znacznie krótszy

                                                                   
test = reduce(lambda left,right: pd.merge(left,right,on='id', how = 'outer'), [marc_parser_1_field(contained_work.loc[(contained_work['X200'].str.contains('%z')==True) & 
                                                                   ((contained_work['X300'].str.contains('%b')==True) |
                                                                    (contained_work['X300'].str.contains('%f')==True))], 'id', 'X200', '%')[['id', '%a']],
                                                                                  marc_parser_1_field(contained_work.loc[(contained_work['X300'].str.contains('%z')==True) & 
                                                                   ((contained_work['X300'].str.contains('%b')==True) |
                                                                    (contained_work['X300'].str.contains('%f')==True))], 'id', 'X300', '%')[['id', '%f']]])['id'].drop_duplicates()


test3 = contained_work.loc[contained_work['X300'].str.contains('\|')==True]


test = reduce(lambda left,right: pd.merge(left,right,on='index', how = 'outer'), [marc_parser_1_field(contained_work.loc[(contained_work['X200'].str.contains('%z')==True) & 
                                                                   ((contained_work['X300'].str.contains('%b')==True) |
                                                                    (contained_work['X300'].str.contains('%f')==True))], 'index', 'X200', '%')[['index', '%a']],
                                                                                  marc_parser_1_field(contained_work.loc[(contained_work['X300'].str.contains('%z')==True) & 
                                                                   ((contained_work['X300'].str.contains('%b')==True) |
                                                                    (contained_work['X300'].str.contains('%f')==True))], 'index', 'X300', '%')[['index', '%f']]]).groupby('index')[0].transform(lambda x: ','.join(x))
                  
                  
                  
test = marc_parser_1_field_simple(contained_work.loc[(contained_work['X300'].str.contains('%z')==True) & 
                                                                   ((contained_work['X300'].str.contains('%b')==True) |
                                                                    (contained_work['X300'].str.contains('%f')==True))], 'index', 'X300', '%')[['index', '%f']]                 
                  
                  
                                                                   
                                                                   
                                                                   
                                                                   
df.groupby('Order ID')['Product'].transform(lambda x: ', '.join(x))                                                                   
reduce(lambda left,right: pd.merge(left,right,on='index', how = 'outer'), [marc_parser_1_field(contained_work.loc[(contained_work['X200'].str.contains('%z')==True) & 
                                                                   ((contained_work['X300'].str.contains('%b')==True) |
                                                                    (contained_work['X300'].str.contains('%f')==True))], 'index', 'X200', '%')[['index', '%a']],
                                                                                  marc_parser_1_field(contained_work.loc[(contained_work['X300'].str.contains('%z')==True) & 
                                                                   ((contained_work['X300'].str.contains('%b')==True) |
                                                                    (contained_work['X300'].str.contains('%f')==True))], 'index', 'X300', '%')[['index', '%f']]]).columns[1:]].apply(
                                                                        lambda x: ','.join(x.dropna().astype(str)),
                                                                        axis=1))                                                                  
                                                                   
                                                                   
                                                                   
                                                                   

test = pd.concat([reduce(lambda left,right: pd.merge(left,right,on='index', how = 'outer'), [marc_parser_1_field(contained_work.loc[(contained_work['X200'].str.contains('%z')==True) & 
                                                                   ((contained_work['X300'].str.contains('%b')==True) |
                                                                    (contained_work['X300'].str.contains('%f')==True))], 'index', 'X200', '%')[['index', '%a']],
                                                                                  marc_parser_1_field(contained_work.loc[(contained_work['X300'].str.contains('%z')==True) & 
                                                                   ((contained_work['X300'].str.contains('%b')==True) |
                                                                    (contained_work['X300'].str.contains('%f')==True))], 'index', 'X300', '%')[['index', '%f']]]), reduce(lambda left,right: pd.merge(left,right,on='index', how = 'outer'), [marc_parser_1_field(contained_work.loc[(contained_work['X200'].str.contains('%z')==True) & 
                                                                   ((contained_work['X300'].str.contains('%b')==True) |
                                                                    (contained_work['X300'].str.contains('%f')==True))], 'index', 'X200', '%')[['index', '%a']],
                                                                                  marc_parser_1_field(contained_work.loc[(contained_work['X300'].str.contains('%z')==True) & 
                                                                   ((contained_work['X300'].str.contains('%b')==True) |
                                                                    (contained_work['X300'].str.contains('%f')==True))], 'index', 'X300', '%')[['index', '%f']]])[
                                                                        reduce(lambda left,right: pd.merge(left,right,on='index', how = 'outer'), [marc_parser_1_field(contained_work.loc[(contained_work['X200'].str.contains('%z')==True) & 
                                                                   ((contained_work['X300'].str.contains('%b')==True) |
                                                                    (contained_work['X300'].str.contains('%f')==True))], 'index', 'X200', '%')[['index', '%a']],
                                                                                  marc_parser_1_field(contained_work.loc[(contained_work['X300'].str.contains('%z')==True) & 
                                                                   ((contained_work['X300'].str.contains('%b')==True) |
                                                                    (contained_work['X300'].str.contains('%f')==True))], 'index', 'X300', '%')[['index', '%f']]]).columns[1:]].apply(
                                                                        lambda x: ','.join(x.dropna().astype(str)),
                                                                        axis=1)], axis=1)[['index', 0]].groupby('index')[0].transform(lambda x: ','.join(x))['index', 0].drop_duplicates()


ttt = reduce(lambda left,right: pd.merge(left,right,on='index', how = 'outer'), [marc_parser_1_field(contained_work.loc[(contained_work['X200'].str.contains('%z')==True) & 
                                                                   ((contained_work['X300'].str.contains('%b')==True) |
                                                                    (contained_work['X300'].str.contains('%f')==True))], 'index', 'X200', '%')[['index', '%a']],
                                                                                  marc_parser_1_field(contained_work.loc[(contained_work['X300'].str.contains('%z')==True) & 
                                                                   ((contained_work['X300'].str.contains('%b')==True) |
                                                                    (contained_work['X300'].str.contains('%f')==True))], 'index', 'X300', '%')[['index', '%f']]])
test['total_notes'] = test[test.columns[1:]].apply(
    lambda x: ','.join(x.dropna().astype(str)),
    axis=1
)




