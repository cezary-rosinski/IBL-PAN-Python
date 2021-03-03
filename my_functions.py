from itertools import chain
import re
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
        if re.findall(f'{subfield_code}', column_name):
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
def gsheet_to_df(gsheetId, scope):
    CLIENT_SECRET_FILE = 'client_secret.json'
    API_SERVICE_NAME = 'sheets'
    API_VERSION = 'v4'
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
    s = Create_Service(CLIENT_SECRET_FILE, API_SERVICE_NAME, API_VERSION, SCOPES)
    gs = s.spreadsheets()
    rows = gs.values().get(spreadsheetId=gsheetId,range=scope).execute()
    header = rows.get('values', [])[0]   # Assumes first line is header!
    values = rows.get('values', [])[1:]  # Everything else is data.
    df = pd.DataFrame(values, columns = header)
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
    
def df_to_mrc(df, delimiter, path_out, txt_error_file):
    mrc_errors = []
    df = df.replace(r'^\s*$', np.nan, regex=True)
    outputfile = open(path_out, 'wb')
    errorfile = io.open(txt_error_file, 'wt', encoding='UTF-8')
    for index, row in enumerate(df.iterrows()):
        try: 
            print(str(index+1) + '/' + str(len(df)))
            table_row = df.iloc[[index]].dropna(axis=1)
            for column in table_row:
                table_row[column] = table_row[column].astype(str).str.split(delimiter)
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
        except ValueError:
            mrc_errors.append(table_row)
    if len(mrc_errors) > 0:
        for element in mrc_errors:
            errorfile.write(str(element) + '\n\n')
    errorfile.close()
    outputfile.close()
    
def mrk_to_df(path_in, field_with_id, encoding='UTF-8'):
    reader = io.open(path_in, 'rt', encoding = encoding).read().splitlines()
    mrk_list = []
    errors = []
    for row in reader:
        if '=LDR' not in row:
            mrk_list[-1] += '\n' + row
        else:
            mrk_list.append(row)
    full_data_list = []     
    for index, record in enumerate(mrk_list):
        print(str(index) + '/' + str(len(mrk_list)))
        try:
            record = re.split('^=|\n=', record)
            record = list(filter(None, record))
            for i, row in enumerate(record):
                record[i] = record[i].rstrip().split('  ', 1)
            df = pd.DataFrame(record, columns = ['field', 'content'])
            df['id'] = df.apply(lambda x: f(x, field_with_id), axis = 1)
            df['id'] = df['id'].ffill().bfill()
            df['content'] = df.groupby(['id', 'field'])['content'].transform(lambda x: '❦'.join(x.drop_duplicates().astype(str)))
            df = df.drop_duplicates().reset_index(drop=True)
            record_dict = df.pivot(index = 'id', columns = 'field', values = 'content').to_dict('records')
            full_data_list += record_dict
        except ValueError:
            errors.append(record)
    full_df = pd.DataFrame.from_records(full_data_list)
    fields_order = full_df.columns.tolist()
    fields_order.sort(key = lambda x: ([str,int].index(type("a" if re.findall(r'\w+', x)[0].isalpha() else 1)), x))
    full_df = full_df.reindex(columns=fields_order)
    return full_df, errors


def cluster_records(df, column_with_ids, list_of_columns, similarity_lvl=0.8):
    try:
        df.drop(columns='cluster',inplace=True)
    except KeyError:
        pass
    list_of_matrixes = []
    for i, column in enumerate(list_of_columns):
        series = [str(e) for e in df[column].to_list()]
        matrix = pd.DataFrame(index=pd.Index(series), columns=series)
        
        for index_row in range(matrix.shape[0]):
            for index_column in range(matrix.shape[1]):
                m_row = matrix.index[index_row]
                m_col = matrix.columns[index_column]
                matrix.iloc[index_row,index_column] = difflib.SequenceMatcher(a=m_row,b=m_col).ratio()
        list_of_matrixes.append(matrix)
        
    ids = df[column_with_ids].to_list()            
    matrix = pd.DataFrame(index=pd.Index(ids), columns=ids) 
    
    for index_row in range(matrix.shape[0]):
        for index_column in range(matrix.shape[1]):
            list_of_values = []
            for matrix_df in list_of_matrixes:
                list_of_values.append(matrix_df.iloc[index_row,index_column])
            mean_value = statistics.mean(list_of_values)
            matrix.iloc[index_row,index_column] = mean_value
                
    matrix = pd.DataFrame(np.tril(matrix.to_numpy(), 0), index=matrix.index, columns=matrix.columns)
    
    stacked_matrix = matrix.stack().reset_index()
    stacked_matrix = stacked_matrix[stacked_matrix[0] >= similarity_lvl].rename(columns={'level_0':column_with_ids, 'level_1':'cluster'})
    stacked_matrix = stacked_matrix.groupby('cluster').filter(lambda x: len(x) > 1)
    stacked_matrix = stacked_matrix[stacked_matrix[column_with_ids] != stacked_matrix['cluster']].sort_values(0, ascending=False).drop(columns=0)
    tuples = [tuple(x) for x in stacked_matrix.to_numpy()]
    
    clusters = {}
    for t_id, t_cluster in tuples:
        if t_cluster in clusters and t_cluster in [e for e in clusters.values() for e in e] and t_id not in [e for e in clusters.values() for e in e]:
            clusters[t_cluster].append(t_id)
        elif t_cluster in clusters and t_cluster in [e for e in clusters.values() for e in e] and t_id not in [e for e in clusters.values() for e in e]:
           clusters[[k for k, v in clusters.items() if t_cluster in v][0]].append(t_id)
        elif t_id not in [e for e in clusters.values() for e in e]:
            clusters[t_cluster] = [t_id, t_cluster]

    group_df = pd.DataFrame.from_dict(clusters, orient='index').stack().reset_index(level=0).rename(columns={'level_0':'cluster', 0:column_with_ids})
    
    df = df.merge(group_df, on=column_with_ids, how='left')
    df['cluster'] = df[[column_with_ids, 'cluster']].apply(lambda x: x['cluster'] if pd.notnull(x['cluster']) else x[column_with_ids], axis=1).astype('int64')
    
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
































