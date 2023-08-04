from itertools import chain
import regex as re
import math
from collections import Counter
from itertools import combinations
from Google import Create_Service
import pandas as pd
import numpy as np
import pymarc
import os
import io
import difflib
import statistics
import unidecode
from tqdm import tqdm
import gspread as gs
from gspread_dataframe import set_with_dataframe, get_as_dataframe

#%%

# parser kolumny marc
def marc_parser_1_field(df, field_id, field_data, subfield_code, delimiter='❦'):
    marc_field = df.loc[df[field_data].notnull(),[field_id, field_data]]
    marc_field = pd.DataFrame(marc_field[field_data].str.split(delimiter).tolist(), marc_field[field_id]).stack()
    marc_field = marc_field.reset_index()[[0, field_id]]
    marc_field.columns = [field_data, field_id]
    subfield_list = df[field_data].str.findall(f'{subfield_code}.').dropna().tolist()
    if marc_field[field_data][0].find(subfield_code[-1]) == 0: 
        subfield_list = sorted(set(list(chain.from_iterable(subfield_list))))
        subfield_list = [x for x in subfield_list if re.findall(f'{subfield_code}\w+', x)]
        empty_table = pd.DataFrame(index = range(0, len(marc_field)), columns = subfield_list)
        marc_field = pd.concat([marc_field.reset_index(drop=True), empty_table], axis=1)
        for marker in subfield_list:
            marker = "".join([i if i.isalnum() else f'\\{i}' for i in marker])            
            marc_field[field_data] = marc_field[field_data].str.replace(f'({marker})', r'❦\1', 1)
        for marker in subfield_list:
            marker2 = "".join([i if i.isalnum() else f'\\{i}' for i in marker])
            string = f'(^)(.*?\❦{marker2}|)(.*?)(\,{{0,1}})((\❦{subfield_code})(.*)|$)'
            marc_field[marker] = marc_field[field_data].str.replace(string, r'\3')
            marc_field[marker] = marc_field[marker].str.replace(marker, '').str.strip().str.replace(' +', ' ')
    else:
        subfield_list = list(set(list(chain.from_iterable(subfield_list))))
        subfield_list = [x for x in subfield_list if re.findall(f'{subfield_code}\w+', x)]
        subfield_list.sort(key = lambda x: ([str,int].index(type("a" if re.findall(r'\w+', x)[0].isalpha() else 1)), x))
        empty_table = pd.DataFrame(index = range(0, len(marc_field)), columns = subfield_list)
        marc_field['indicator'] = marc_field[field_data].str.replace(f'(^.*?)({subfield_code}.*)', r'\1')
        marc_field = pd.concat([marc_field.reset_index(drop=True), empty_table], axis=1)
        for marker in subfield_list:
            marker = "".join([i if i.isalnum() else f'\\{i}' for i in marker])            
            marc_field[field_data] = marc_field[field_data].str.replace(f'({marker})', r'❦\1', 1)
        for marker in subfield_list:
            marker2 = "".join([i if i.isalnum() else f'\\{i}' for i in marker]) 
            string = f'(^)(.*?\❦{marker2}|)(.*?)(\,{{0,1}})((\❦{subfield_code})(.*)|$)'
            marc_field[marker] = marc_field[field_data].apply(lambda x: re.sub(string, r'\3', x) if marker in x else '')
            marc_field[marker] = marc_field[marker].str.replace(marker, '').str.strip().str.replace(' +', ' ')
    for (column_name, column_data) in marc_field.iteritems():
        if re.findall(f'{subfield_code}', str(column_name)):
            marc_field[column_name] = marc_field[column_name].str.replace(re.escape(column_name), '❦')
    return marc_field

def marc_parser_1_field_simple(df, field_id, field_data, subfield_code):
    marc_field = df.loc[df[field_data].notnull(),[field_id, field_data]]
    subfield_list = df[field_data].str.findall(f'\{subfield_code}.').dropna().tolist()
    subfield_list = sorted(set(list(chain.from_iterable(subfield_list))))
    empty_table = pd.DataFrame(index = range(0, len(marc_field)), columns = subfield_list)
    marc_field = pd.concat([marc_field.reset_index(drop=True), empty_table], axis=1)
    for marker in subfield_list:
        marc_field[field_data] = marc_field[field_data].str.replace(f'({marker})', r'|\1', 1)
    for marker in subfield_list:
        string = f'(^)(.*?\|\{marker}|)(.*?)(\,{{0,1}})((\|\{subfield_code})(.*)|$)'
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
def cSplit(df, id_column, split_column, delimiter, how = 'long', maxsplit = -1):
    if how == 'long':
        df.loc[df[split_column].isnull(), split_column] = ''
        new_df = pd.DataFrame(df[split_column].str.split(delimiter).tolist(), index=df[id_column]).stack()
        new_df = new_df.reset_index([0, id_column])
        new_df.columns = [id_column, split_column]
        new_df = pd.merge(new_df, df.loc[:, df.columns != split_column],  how='left', left_on = id_column, right_on = id_column)
        new_df = new_df[df.columns]
        new_df.loc[new_df[split_column] == '', split_column] = np.nan
        return new_df
    elif how == 'wide':
        df.loc[df[split_column].isnull(), split_column] = ''
        new_df = pd.DataFrame(df[split_column].str.split(delimiter, maxsplit).tolist(), index=df[id_column])
        new_df = new_df.reset_index(drop=True).fillna(value=np.nan).replace(r'^\s*$', np.nan, regex=True)
        new_df.columns = [f"{split_column}_{str(column_name)}" for column_name in new_df.columns.values]
        new_df = pd.concat([df.loc[:, df.columns != split_column], new_df], axis=1)
        return new_df
    else:
        print("Error: Unhandled method")
    

#explode data frame for equal length
def df_explode(df, lst_cols, sep):
    df1 = pd.DataFrame()
    for column in lst_cols:
        column = df[column].str.split(sep, expand=True).stack().reset_index(level=1, drop=True)
        df1 = pd.concat([df1, column], axis = 1)
    df1.columns = lst_cols
    df.drop(df[lst_cols], axis = 1, inplace = True)
    df_final = df.join(df1).reset_index(drop=True)
    return df_final

# replace nth occurence
def replacenth(string, sub, wanted, n):
    where = [m.start() for m in re.finditer(sub, string)][n-1]
    before = string[:where]
    after = string[where:]
    after = after.replace(sub, wanted, 1)
    newString = before + after
    return newString 

#read google sheet
def gsheet_to_df(gsheetId, worksheet):
    gc = gs.oauth()
    sheet = gc.open_by_key(gsheetId)
    df = get_as_dataframe(sheet.worksheet(worksheet), evaluate_formulas=True, dtype=str).dropna(how='all').dropna(how='all', axis=1)
    # CLIENT_SECRET_FILE = 'client_secret.json'
    # API_SERVICE_NAME = 'sheets'
    # API_VERSION = 'v4'
    # SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
    # s = Create_Service(CLIENT_SECRET_FILE, API_SERVICE_NAME, API_VERSION, SCOPES)
    # gs = s.spreadsheets()
    # rows = gs.values().get(spreadsheetId=gsheetId,range=scope).execute()
    # header = rows.get('values', [])[0]   # Assumes first line is header!
    # values = rows.get('values', [])[1:]  # Everything else is data.
    # df = pd.DataFrame(values, columns = header)
    return df



#write google sheet
def df_to_gsheet(df, gsheetId,scope='Arkusz1'):
    CLIENT_SECRET_FILE = 'client_secret.json'
    API_SERVICE_NAME = 'sheets'
    API_VERSION = 'v4'
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
    service = Create_Service(CLIENT_SECRET_FILE, API_SERVICE_NAME, API_VERSION, SCOPES)
    df.replace(np.nan, '', inplace=True)
    response_date = service.spreadsheets().values().append(
        spreadsheetId=gsheetId,
        valueInputOption='RAW',
        range=scope + '!A1',
        body=dict(
            majorDimension='ROWS',
            values=df.T.reset_index().T.values.tolist())
    ).execute()

# marc conversions
def xml_to_mrc(path_in, path_out):
    writer = pymarc.MARCWriter(open(path_out, 'wb'))
    records = pymarc.map_xml(writer.write, path_in) 
    writer.close()   

def xml_to_mrk(path_in, path_out):
    writer = pymarc.TextWriter(io.open(path_out, 'wt', encoding="utf-8"))
    records = pymarc.map_xml(writer.write, path_in) 
    writer.close() 
    
def mrc_to_mrk(path_in, path_out):
    reader = pymarc.MARCReader(open(path_in, 'rb'), to_unicode=True, force_utf8=True)
    writer = pymarc.TextWriter(io.open(path_out, 'wt', encoding="UTF-8"))
    for record in reader:
        writer.write(record)
    writer.close()
    
def f(row, id_field):
    if row['field'] == id_field and id_field == 'LDR':
        val = row.name
    elif row['field'] == id_field:
        val = row['content']
    else:
        val = np.nan
    return val

def mrk_to_mrc(path_in, path_out, field_with_id):
    outputfile = open(path_out, 'wb')
    reader = io.open(path_in, 'rt', encoding = 'utf-8').read().splitlines()
    mrk_list = []
    for row in reader:
        if 'LDR' not in row:
            mrk_list[-1] += '\n' + row
        else:
            mrk_list.append(row)
    
    full_data = pd.DataFrame()      
    for record in mrk_list:
        record = record.split('=')
        record = list(filter(None, record))
        for i, row in enumerate(record):
            record[i] = record[i].rstrip().split('  ', 1)
        df = pd.DataFrame(record, columns = ['field', 'content'])
        df['id'] = df.apply(lambda x: f(x, field_with_id), axis = 1)
        df['id'] = df['id'].ffill().bfill()
        df['content'] = df.groupby(['id', 'field'])['content'].transform(lambda x: '❦'.join(x.drop_duplicates().astype(str)))
        df = df.drop_duplicates().reset_index(drop=True)
        df_wide = df.pivot(index = 'id', columns = 'field', values = 'content')
        full_data = full_data.append(df_wide)
        
    for index, row in enumerate(full_data.iterrows()):
        table_row = full_data.iloc[[index]].dropna(axis=1)
        for column in table_row:
            table_row[column] = table_row[column].str.split('❦')
        marc_fields = table_row.columns.tolist()
        marc_fields.sort(key = lambda x: ([str,int].index(type("a" if re.findall(r'\w+', x)[0].isalpha() else 1)), x))
        record_id = table_row.index[0]
        table_row = table_row.reindex(columns=marc_fields)
        table_row = table_row.T.to_dict()[record_id]
        leader = ''.join(table_row['LDR'])
        del table_row['LDR']
        table_row = list(table_row.items())
        pymarc_record = pymarc.Record(to_unicode=True, force_utf8=True, leader=leader)
        for i, field in enumerate(table_row):
            if int(table_row[i][0]) < 10:
                tag = table_row[i][0]
                data = ''.join(table_row[i][1])
                marc_field = pymarc.Field(tag=tag, data=data)
                pymarc_record.add_ordered_field(marc_field)
            else:
                if len(table_row[i][1]) == 1:
                    tag = table_row[i][0]
                    record_in_list = re.split('\$(.)', ''.join(table_row[i][1]))
                    indicators = list(record_in_list[0])
                    subfields = record_in_list[1:]
                    marc_field = pymarc.Field(tag=tag, indicators=indicators, subfields=subfields)
                    pymarc_record.add_ordered_field(marc_field)
                else:
                    for element in table_row[i][1]:
                        tag = table_row[i][0]
                        record_in_list = re.split('\$(.)', ''.join(element))
                        indicators = list(record_in_list[0])
                        subfields = record_in_list[1:]
                        marc_field = pymarc.Field(tag=tag, indicators=indicators, subfields=subfields)
                        pymarc_record.add_ordered_field(marc_field)
        outputfile.write(pymarc_record.as_marc())     
    outputfile.close()
    
def df_to_mrc(df, field_delimiter, path_out, txt_error_file):
    # df, field_delimiter, path_out, txt_error_file = bn_harvested_df, '❦', 'test.mrc', 'test.txt'
    # df, field_delimiter, path_out, txt_error_file = bn_articles_marc, '❦', f'data/libri_marc_bn_articles_{year}-{month}-{day}.mrc', f'data/libri_bn_articles_errors_{year}-{month}-{day}.txt'
    mrc_errors = []
    df = df.replace(r'^\s*$', np.nan, regex=True)
    outputfile = open(path_out, 'wb')
    errorfile = io.open(txt_error_file, 'wt', encoding='UTF-8')
    list_of_dicts = df.to_dict('records')
    for record in tqdm(list_of_dicts, total=len(list_of_dicts)):
        record = {k: v for k, v in record.items() if pd.notnull(v)}
        try:
            pymarc_record = pymarc.Record(to_unicode=True, force_utf8=True, leader=record['LDR'])
            # record = {k:v for k,v in record.items() if any(a == k for a in ['LDR', 'AVA']) or re.findall('\d{3}', str(k))}
            for k, v in record.items():
                v = str(v).split(field_delimiter)
                k = str(k) if isinstance(k,int) else k
                if k == 'LDR':
                    pass
                elif k.isnumeric() and int(k) < 10:
                    tag = k
                    data = ''.join(v)
                    marc_field = pymarc.Field(tag=tag, data=data)
                    pymarc_record.add_ordered_field(marc_field)
                else:
                    if len(v) == 1:
                        tag = k
                        record_in_list = re.split('\$(.)', ''.join(v))
                        indicators = list(record_in_list[0])
                        subfields = record_in_list[1:]
                        marc_field = pymarc.Field(tag=tag, indicators=indicators, subfields=[pymarc.Subfield(code=e,value=f) for e,f in zip(subfields[::2], subfields[1::2])])
                        if indicators:
                            pymarc_record.add_ordered_field(marc_field)
                    else:
                        for element in v:
                            tag = k
                            record_in_list = re.split('\$(.)', ''.join(element))
                            indicators = list(record_in_list[0])
                            subfields = record_in_list[1:]
                            marc_field = pymarc.Field(tag=tag, indicators=indicators, subfields=[pymarc.Subfield(code=e,value=f) for e,f in zip(subfields[::2], subfields[1::2])])
                            if indicators:
                                pymarc_record.add_ordered_field(marc_field)
            outputfile.write(pymarc_record.as_marc())
        except ValueError as err:
            mrc_errors.append((err, record))
    if len(mrc_errors) > 0:
        for element in mrc_errors:
            errorfile.write(str(element) + '\n\n')
    errorfile.close()
    outputfile.close()
    # mrc_to_mrk('test.mrc', 'test.mrk')
    
def mrk_to_df(path_in, encoding='UTF-8'):
    reader = io.open(path_in, 'rt', encoding = encoding).read().splitlines()
    mrk_list = []
    for row in reader:
        if '=LDR' not in row:
            mrk_list[-1] += '\n' + row
        else:
            mrk_list.append(row)
            
    final_list = []
    for lista in tqdm(mrk_list):
        lista = [e for e in lista.split('\n') if e]
        slownik = {}
        for el in lista:
            if el[1:4] in slownik:
                slownik[el[1:4]] += f"❦{el[6:]}"
            else:
                slownik[el[1:4]] = el[6:]
        final_list.append(slownik)
        
    df = pd.DataFrame(final_list).drop_duplicates().reset_index(drop=True)
    fields = df.columns.tolist()
    fields = [i for i in fields if 'LDR' in i or re.compile('\d{3}').findall(i)]
    df = df.loc[:, df.columns.isin(fields)]
    fields.sort(key = lambda x: ([str,int].index(type("a" if re.findall(r'\w+', x)[0].isalpha() else 1)), x))
    df = df.reindex(columns=fields)
    return df

def type_str(x):
    try:
        return str(int(x))
    except ValueError:
        return str(x)    


def cluster_records(df, column_with_ids, list_of_columns, similarity_lvl=0.85, how_to_organise='cluster_first', show_time=False):
    try:
        df.drop(columns='cluster',inplace=True)
    except KeyError:
        pass
    list_of_matrixes = []
    if len(list_of_columns) == 1:
        for column in list_of_columns:
            series = {}
            for identifier, title in zip(df[column_with_ids].to_list(), df[column].to_list()):
                if title not in series:
                    series[title] = [identifier]
                else: series[title].append(identifier)
            unique_series = list(series.keys())
            try:
                list_of_matrixes.append(np.array([[difflib.SequenceMatcher(a=w1,b=w2).ratio() for w1 in unique_series] for w2 in unique_series]))
            except TypeError:
                print(f'Column: {column} dtype cannot be integer!')
    else:
        for column in list_of_columns:
            if df[column].notnull().all():
                series = df[column].to_list()
                list_of_matrixes.append(np.array([[difflib.SequenceMatcher(a=w1,b=w2).ratio() for w1 in series] for w2 in series]))
    
    matrix = np.mean(list_of_matrixes, axis=0)             
    if len(list_of_columns) == 1:
        matrix = pd.DataFrame(np.tril(matrix, 0), index=pd.Index(unique_series), columns=unique_series)
    else:
        ids = df[column_with_ids].to_list()
        matrix = pd.DataFrame(np.tril(matrix, 0), index=pd.Index(ids), columns=ids)
    
    stacked_matrix = matrix.stack().reset_index()
    stacked_matrix = stacked_matrix[stacked_matrix[0] >= similarity_lvl].rename(columns={'level_0':column_with_ids, 'level_1':'cluster'})
    stacked_matrix = stacked_matrix.groupby('cluster').filter(lambda x: len(x) > 1)
    if how_to_organise == 'cluster_first':
        stacked_matrix = stacked_matrix[stacked_matrix[column_with_ids] != stacked_matrix['cluster']].sort_values(['cluster', 0], ascending=[True, False]).drop(columns=0)
    elif how_to_organise == 'similarity_first':
        stacked_matrix = stacked_matrix[stacked_matrix[column_with_ids] != stacked_matrix['cluster']].sort_values([0, 'cluster'], ascending=[False, True]).drop(columns=0)
    else:
        stacked_matrix = stacked_matrix[stacked_matrix[column_with_ids] != stacked_matrix['cluster']].sort_values(0).drop(columns=0)
        print("Wrong 'how_to_organise' value!")

    tuples = [tuple(x) for x in stacked_matrix.to_numpy()]
    
    clusters = {}
    for t_id, t_cluster in tuples:
        if t_cluster in clusters and t_cluster in [e for e in clusters.values() for e in e] and t_id not in [e for e in clusters.values() for e in e]:
            clusters[t_cluster].append(t_id)
        elif t_cluster not in clusters and t_cluster in [e for e in clusters.values() for e in e] and t_id not in [e for e in clusters.values() for e in e]:
           clusters[[k for k, v in clusters.items() if t_cluster in v][0]].append(t_id)
        elif t_id not in [e for e in clusters.values() for e in e]:
            clusters[t_cluster] = [t_id, t_cluster]
            
    if len(list_of_columns) == 1:
        test = [e for e in unique_series if e not in clusters.keys() and e not in [el for sub in clusters.values() for el in sub]]
        clusters.update(dict(zip([e for e in test], [[e] for e in test])))
        clusters = {k:[el for sub in [series[e] for e in v] for el in sub] if len([series[e] for e in v]) > 1 else series[v[0]] for k,v in clusters.items()}
        clusters = {min(v):v for k,v in clusters.items()}

    df[column_with_ids] = df[column_with_ids].astype(np.int64)

    group_df = pd.DataFrame.from_dict(clusters, orient='index').stack().reset_index(level=0).rename(columns={'level_0':'cluster', 0:column_with_ids})
    group_df[column_with_ids] = group_df[column_with_ids].astype(np.int64)

# print(group_df[column_with_ids].dtype)
# print(df[column_with_ids].dtype)
    
    df = df.merge(group_df, on=column_with_ids, how='left')

    # df['cluster'] = df[[column_with_ids, 'cluster']].apply(lambda x: x['cluster'] if pd.notnull(x['cluster']) else x[column_with_ids], axis=1).astype('int64')
    # df['cluster'] = df[[column_with_ids, 'cluster']].apply(lambda x: x['cluster'] if pd.notnull(x['cluster']) else x[column_with_ids], axis=1).astype(np.int64).astype(str)
    
    return df

def cluster_strings(strings, similarity_level):
    clusters = {}
    for string in (x.strip() for x in strings):
        if string in clusters:
            clusters[string].append(string)
        else:
            match = difflib.get_close_matches(string, clusters.keys(), cutoff=similarity_level)
            if match:
                clusters[match[0]].append(string)
            else:
                clusters[string] = [string]
    return clusters

def simplify_string(x, with_spaces=True, nodiacritics=True):
    x = pd.Series([e for e in x if type(e) == str])
    if with_spaces and nodiacritics:
        x = unidecode.unidecode('❦'.join(x.dropna().astype(str)).lower())
    elif nodiacritics:
        x = unidecode.unidecode('❦'.join(x.dropna().astype(str)).lower().replace(' ', ''))
    elif with_spaces:
        x = '❦'.join(x.dropna().astype(str)).lower()
    else:
        x = '❦'.join(x.dropna().astype(str))
    final_string = ''
    for letter in x:
        if letter.isalnum() or letter == ' ':
            final_string += letter
    return final_string

import time


def marc_parser_to_dict(string: str, subfield_code: str) -> dict:
    subfield_list = re.findall(f'{subfield_code}.', string)
    dictionary_field = {}
    for subfield in subfield_list:
        subfield_escape = re.escape(subfield)
        string = re.sub(f'({subfield_escape})', r'❦\1', string)
    string = [e.split('\n')[0].strip() for e in string.split('❦') if e]
    dictionary_fields = [e for e in string if re.escape(e)[:len(subfield_code)] == subfield_code]
    dictionary_fields = {subfield_list[i]:e[len(subfield_list[i]):] for i, e in enumerate(dictionary_fields)}
    return dictionary_fields

def substring_range(s, substring):
    for i in re.finditer(re.escape(substring), s):
        yield (i.start(), i.end())

def marc_parser_to_list(string: str, subfield_code: str) -> list:
    subfield_list = re.findall(f'{subfield_code}.', string)
    for subfield in subfield_list:
        subfield_escape = re.escape(subfield)
        string = re.sub(f'({subfield_escape})', r'❦\1', string)
    string = [e.split('\n')[0].strip() for e in string.split('❦') if e]
    dictionary_fields = [e for e in string if re.escape(e)[:len(subfield_code)] == subfield_code]
    dictionary_fields = [{subfield_list[i]:e[len(subfield_list[i]):]} for i, e in enumerate(dictionary_fields)]
    return dictionary_fields


# szukanie NKC id dla VIAF id
# authority_dict = {}
# addendum_dict = {}
# for el in tqdm(viaf_ids):
#     url = f"http://viaf.org/viaf/{el}/viaf.json"
#     response = requests.get(url).json()
#     try:
#         nkc_id = [e['@nsid'] for e in response['sources']['source'] if 'NKC' in e['#text']][0]
#         authority_dict.update({el:nkc_id})
#     except TypeError:
#         nkc_id = response['sources']['source']['@nsid'] if 'NKC' in response['sources']['source']['#text'] else None
#         authority_dict.update({el:nkc_id})
#     except KeyError:
#         print(f'VIAF_ID {el} is wrong! Replace with {response["redirect"]["directto"]}')
#         new_el = response["redirect"]["directto"]
#         new_url = f"http://viaf.org/viaf/{new_el}/viaf.json"
#         new_response = requests.get(new_url).json()
#         try:
#             new_nkc_id = [e['@nsid'] for e in new_response['sources']['source'] if 'NKC' in e['#text']][0]
#         except TypeError:
#             new_nkc_id = new_response['sources']['source']['@nsid'] if 'NKC' in new_response['sources']['source']['#text'] else None
#         addendum_dict.update({new_el:{'old_viaf':el, 'nkc_id':new_nkc_id}})
#     except IndexError:
#         print(f'VIAF_ID {el} has no NKC ID! Check it in the table!')

def create_google_worksheet(sheet_id, worksheet_name, df, delete_default_worksheet=True):
    gc = gs.oauth()
    sheet = gc.open_by_key(sheet_id)
    try:
        set_with_dataframe(sheet.worksheet(worksheet_name), df)
    except gs.WorksheetNotFound:
        sheet.add_worksheet(title=worksheet_name, rows="100", cols="20")
        set_with_dataframe(sheet.worksheet(worksheet_name), df)  
    worksheet = sheet.worksheet(worksheet_name)
    sheet.batch_update({
        "requests": [
            {
                "updateDimensionProperties": {
                    "range": {
                        "sheetId": worksheet._properties['sheetId'],
                        "dimension": "ROWS",
                        "startIndex": 0,
                        #"endIndex": 100
                    },
                    "properties": {
                        "pixelSize": 20
                    },
                    "fields": "pixelSize"
                }
            }
        ]
    })
    worksheet.freeze(rows=1)
    worksheet.set_basic_filter()
    if delete_default_worksheet:
        try:
            sheet.del_worksheet(sheet.worksheet('Arkusz1'))
        except:
            try:
                sheet.del_worksheet(sheet.worksheet('Sheet1'))
            except: None



 











































