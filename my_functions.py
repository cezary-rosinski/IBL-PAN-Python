import pandas as pd
from itertools import chain
import re
import math
from collections import Counter
from itertools import combinations
import numpy as np

# parser kolumny marc
def marc_parser_1_field(file, field_id, field_data, delimiter):
    marc_field = file.loc[file[field_data].notnull(),[field_id, field_data]]
    marc_field = pd.DataFrame(marc_field[field_data].str.split('|').tolist(), marc_field[field_id]).stack()
    marc_field = marc_field.reset_index()[[0, field_id]]
    marc_field.columns = [field_data, field_id]
    subfield_list = file[field_data].str.findall(f'\{delimiter}.').dropna().tolist()
    subfield_list = sorted(set(list(chain.from_iterable(subfield_list))))
    empty_table = pd.DataFrame(index = range(0, len(marc_field)), columns = subfield_list)
    marc_field = pd.concat([marc_field.reset_index(drop=True), empty_table], axis=1)
    for marker in subfield_list:
        marc_field[field_data] = marc_field[field_data].str.replace(f'({marker})', r'|\1', 1)
    for marker in subfield_list:
        string = f'(^)(.*?\|\{marker}|)(.*?)(\,{{0,1}})((\|\%)(.*)|$)'
        marc_field[marker] = marc_field[field_data].str.replace(string, r'\3')
        marc_field[marker] = marc_field[marker].str.replace(marker, '').str.strip().str.replace(' +', ' ')
    return marc_field

def marc_parser_1_field_simple(file, field_id, field_data, delimiter):
    marc_field = file.loc[file[field_data].notnull(),[field_id, field_data]]
    subfield_list = file[field_data].str.findall(f'\{delimiter}.').dropna().tolist()
    subfield_list = sorted(set(list(chain.from_iterable(subfield_list))))
    empty_table = pd.DataFrame(index = range(0, len(marc_field)), columns = subfield_list)
    marc_field = pd.concat([marc_field.reset_index(drop=True), empty_table], axis=1)
    for marker in subfield_list:
        marc_field[field_data] = marc_field[field_data].str.replace(f'({marker})', r'|\1', 1)
    for marker in subfield_list:
        string = f'(^)(.*?\|\{marker}|)(.*?)(\,{{0,1}})((\|\%)(.*)|$)'
        marc_field[marker] = marc_field[field_data].str.replace(string, r'\3')
        marc_field[marker] = marc_field[marker].str.replace(marker, '').str.strip().str.replace(' +', ' ')
    return marc_field
# ciąg funkcji dla cosine similarity
def get_cosine(vec1, vec2):
    intersection = set(vec1.keys()) & set(vec2.keys())
    numerator = sum([vec1[x] * vec2[x] for x in intersection])

    sum1 = sum([vec1[x]**2 for x in vec1.keys()])
    sum2 = sum([vec2[x]**2 for x in vec2.keys()])
    denominator = math.sqrt(sum1) * math.sqrt(sum2)

    if not denominator:
        return 0.0
    else:
        return float(numerator) / denominator

def text_to_vector(text):
    word = re.compile(r'\w+')
    words = word.findall(text)
    return Counter(words)

def get_cosine_result(content_a, content_b):
    text1 = content_a
    text2 = content_b

    vector1 = text_to_vector(text1)
    vector2 = text_to_vector(text2)

    cosine_result = get_cosine(vector1, vector2)
    return cosine_result

def cosine_sim_2_elem(lista):
    kombinacje = combinations(lista, 2)
    list_of_lists = [list(elem) for elem in kombinacje]
    for kombinacja in list_of_lists:
        kombinacja.append(get_cosine_result(kombinacja[0], kombinacja[1]))
    df = pd.DataFrame(data = list_of_lists, columns = ['string1', 'string2', 'cosine_similarity'])
    return df

# lista unikatowych wartości w kolumnie - split
    
def unique_elem_from_column_split(file, column, delimiter):
    elements = file[column].apply(lambda x: x.split(delimiter)).tolist()
    elements = sorted(set(list(chain.from_iterable(elements))))
    elements = list(filter(None, elements))
    elements = [x.strip() for x in elements]
    elements = [s for s in elements if len(s) > 1]
    return elements

#lista unikatowych wartości w kolumnie - regex
def unique_elem_from_column_regex(file, column, regex):
    lista_elementow = file[column].str.extract(f'({regex})').drop_duplicates().dropna().values.tolist()
    lista_elementow = list(chain.from_iterable(lista_elementow))
    return lista_elementow

#cSplit
def cSplit(file, id_column, split_column, delimiter):
    file.loc[file[split_column].isnull(), split_column] = ''
    new_df = pd.DataFrame(file[split_column].str.split(delimiter).tolist(), index=file[id_column]).stack()
    new_df = new_df.reset_index([0, id_column])
    new_df.columns = [id_column, split_column]
    new_df = pd.merge(new_df, file.loc[:, file.columns != split_column],  how='left', left_on = id_column, right_on = id_column)
    new_df = new_df[file.columns]
    new_df.loc[new_df[split_column] == '', split_column] = np.nan
    return new_df