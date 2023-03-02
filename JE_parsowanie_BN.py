import pandas as pd
from ast import literal_eval
from itertools import chain
import regex as re

#%% def

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

##% main

df = pd.read_excel(r"C:\Users\Cezary\Downloads\ls_bn.xlsx")

for column in df:
    df[column] = df[column].apply(lambda x: '❦'.join(literal_eval(x)) if pd.notnull(x) else x)

df_700 = marc_parser_1_field(df, '001', '700', '\\$')






















