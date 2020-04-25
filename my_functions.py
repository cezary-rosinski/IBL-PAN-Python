from itertools import chain
import re
import math
from collections import Counter
from itertools import combinations
from Google import Create_Service
import pandas as pd
import numpy as np

# parser kolumny marc
def marc_parser_1_field(df, field_id, field_data, delimiter):
    marc_field = df.loc[df[field_data].notnull(),[field_id, field_data]]
    marc_field = pd.DataFrame(marc_field[field_data].str.split('|').tolist(), marc_field[field_id]).stack()
    marc_field = marc_field.reset_index()[[0, field_id]]
    marc_field.columns = [field_data, field_id]
    subfield_list = df[field_data].str.findall(f'\{delimiter}.').dropna().tolist()
    if marc_field[field_data][0][0] == delimiter[0]: 
        subfield_list = sorted(set(list(chain.from_iterable(subfield_list))))
        empty_table = pd.DataFrame(index = range(0, len(marc_field)), columns = subfield_list)
        marc_field = pd.concat([marc_field.reset_index(drop=True), empty_table], axis=1)
        for marker in subfield_list:
            marker = "".join([i if i.isalnum() else f'\\{i}' for i in marker])            
            marc_field[field_data] = marc_field[field_data].str.replace(f'({marker})', r'|\1', 1)
        for marker in subfield_list:
            string = f'(^)(.*?\|\{marker}|)(.*?)(\,{{0,1}})((\|\{delimiter})(.*)|$)'
            marc_field[marker] = marc_field[field_data].str.replace(string, r'\3')
            marc_field[marker] = marc_field[marker].str.replace(marker, '').str.strip().str.replace(' +', ' ')
    else:
        subfield_list = list(set(list(chain.from_iterable(subfield_list))))
        subfield_list.sort(key = lambda x: ([str,int].index(type("a" if re.findall(r'\w+', x)[0].isalpha() else 1)), x))
        empty_table = pd.DataFrame(index = range(0, len(marc_field)), columns = subfield_list)
        marc_field['indicator'] = marc_field[field_data].str.replace(r'(^.*?)(\$.*)', r'\1')
        marc_field = pd.concat([marc_field.reset_index(drop=True), empty_table], axis=1)
        for marker in subfield_list:
            marker = "".join([i if i.isalnum() else f'\\{i}' for i in marker])            
            marc_field[field_data] = marc_field[field_data].str.replace(f'({marker})', r'|\1', 1)
        for marker in subfield_list:
            marker2 = "".join([i if i.isalnum() else f'\\{i}' for i in marker]) 
            string = f'(^)(.*?\|{marker2}|)(.*?)(\,{{0,1}})((\|\{delimiter})(.*)|$)'
            marc_field[marker] = marc_field[field_data].apply(lambda x: re.sub(string, r'\3', x) if marker in x else '')
            marc_field[marker] = marc_field[marker].str.replace(marker, '').str.strip().str.replace(' +', ' ')
    return marc_field

def marc_parser_1_field_simple(df, field_id, field_data, delimiter):
    marc_field = df.loc[df[field_data].notnull(),[field_id, field_data]]
    subfield_list = df[field_data].str.findall(f'\{delimiter}.').dropna().tolist()
    subfield_list = sorted(set(list(chain.from_iterable(subfield_list))))
    empty_table = pd.DataFrame(index = range(0, len(marc_field)), columns = subfield_list)
    marc_field = pd.concat([marc_field.reset_index(drop=True), empty_table], axis=1)
    for marker in subfield_list:
        marc_field[field_data] = marc_field[field_data].str.replace(f'({marker})', r'|\1', 1)
    for marker in subfield_list:
        string = f'(^)(.*?\|\{marker}|)(.*?)(\,{{0,1}})((\|\{delimiter})(.*)|$)'
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
    
def unique_elem_from_column_split(df, column, delimiter):
    elements = df[column].apply(lambda x: x.split(delimiter)).tolist()
    elements = sorted(set(list(chain.from_iterable(elements))))
    elements = list(filter(None, elements))
    elements = [x.strip() for x in elements]
    elements = [s for s in elements if len(s) > 1]
    return elements

#lista unikatowych wartości w kolumnie - regex
def unique_elem_from_column_regex(df, column, regex):
    lista_elementow = df[column].str.extract(f'({regex})').drop_duplicates().dropna().values.tolist()
    lista_elementow = list(chain.from_iterable(lista_elementow))
    return lista_elementow

#cSplit
def cSplit(df, id_column, split_column, delimiter):
    df.loc[df[split_column].isnull(), split_column] = ''
    new_df = pd.DataFrame(df[split_column].str.split(delimiter).tolist(), index=df[id_column]).stack()
    new_df = new_df.reset_index([0, id_column])
    new_df.columns = [id_column, split_column]
    new_df = pd.merge(new_df, df.loc[:, df.columns != split_column],  how='left', left_on = id_column, right_on = id_column)
    new_df = new_df[df.columns]
    new_df.loc[new_df[split_column] == '', split_column] = np.nan
    return new_df

#explode data frame for equal length
def explode_df(df, lst_cols, fill_value=''):
    # make sure `lst_cols` is a list
    if lst_cols and not isinstance(lst_cols, list):
        lst_cols = [lst_cols]
    # all columns except `lst_cols`
    idx_cols = df.columns.difference(lst_cols)

    # calculate lengths of lists
    lens = df[lst_cols[0]].str.len()

    if (lens > 0).all():
        # ALL lists in cells aren't empty
        return pd.DataFrame({
            col:np.repeat(df[col].values, df[lst_cols[0]].str.len())
            for col in idx_cols
        }).assign(**{col:np.concatenate(df[col].values) for col in lst_cols}) \
          .loc[:, df.columns]
    else:
        # at least one list in cells is empty
        return pd.DataFrame({
            col:np.repeat(df[col].values, df[lst_cols[0]].str.len())
            for col in idx_cols
        }).assign(**{col:np.concatenate(df[col].values) for col in lst_cols}) \
          .append(df.loc[lens==0, idx_cols]).fillna(fill_value) \
          .loc[:, df.columns]

# replace nth occurence
def replacenth(string, sub, wanted, n):
    where = [m.start() for m in re.finditer(sub, string)][n-1]
    before = string[:where]
    after = string[where:]
    after = after.replace(sub, wanted, 1)
    newString = before + after
    return newString 

#read google sheet
def gsheet_to_df(gsheetId, scope):
    CLIENT_SECRET_FILE = 'client_secret.json'
    API_SERVICE_NAME = 'sheets'
    API_VERSION = 'v4'
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']
    s = Create_Service(CLIENT_SECRET_FILE, API_SERVICE_NAME, API_VERSION, SCOPES)
    gs = s.spreadsheets()
    rows = gs.values().get(spreadsheetId=gsheetId,range=scope).execute()
    header = rows.get('values', [])[0]   # Assumes first line is header!
    values = rows.get('values', [])[1:]  # Everything else is data.
    df = pd.DataFrame(values, columns = header)
    return df