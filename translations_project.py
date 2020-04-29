from my_functions import gsheet_to_df
import xml.etree.ElementTree as et
import requests
import pandas as pd
import re
import numpy as np
import copy
from my_functions import df_explode
from my_functions import xml_to_mrk

### def
def f(row, id_field):
    if row['field'] == id_field and id_field == 'LDR':
        val = row.name
    elif row['field'] == id_field:
        val = row['content']
    else:
        val = np.nan
    return val  

def add_viaf(x):
    return x + 'viaf.xml'

### code

#google sheets
df_names = gsheet_to_df('1QkZCzglN7w0AnEubuoQqHgQqib9Glild1x_3X8T_HyI', 'Sheet 1')
df_names = df_names.loc[df_names['czy_czech'] != 'nie']
# names_list = [df_names['cz_name'].values.tolist(), df_names['viaf_id'].values.tolist()]

### viaf - appending list of names
viafs = df_names.loc[df_names['viaf_id'].str.contains('viaf')]
viaf_list = viafs['viaf_id'].drop_duplicates().values.tolist()
viaf_list = list(map(add_viaf, viaf_list))

viaf_names = []
for index, viaf in enumerate(viaf_list):
    print(str(index) + '/' + str(len(viaf_list)-1))
    response = requests.get(viaf)
    with open('viaf.xml', 'wb') as file:
        file.write(response.content)
    tree = et.parse('viaf.xml')
    root = tree.getroot()
    v_names = root.findall('.//{http://viaf.org/viaf/terms#}x400/{http://viaf.org/viaf/terms#}datafield')
    v_sources = root.findall('.//{http://viaf.org/viaf/terms#}x400/{http://viaf.org/viaf/terms#}sources/{http://viaf.org/viaf/terms#}sid')
    for (name, library) in zip(v_names, v_sources):
        name = ' '.join([child.text for child in name.getchildren() if child.tag == '{http://viaf.org/viaf/terms#}subfield'])
        viaf_names.append([viaf, name, library.text])
        
cz_names = pd.DataFrame(viaf_names, columns =['viaf_id', 'names', 'sources']).drop_duplicates()
cz_names['viaf_id'] = cz_names['viaf_id'].str.replace('viaf.xml', '')
cz_names.to_csv('cz_names_viaf.csv', sep = ';', index = False)     
# cz_names = pd.read_csv("cz_names_viaf.csv", sep=';')

for column in cz_names.iloc[:,1:]:
    cz_names[column] = cz_names.groupby('viaf_id')[column].transform(lambda x: '~'.join(x.astype(str)))
cz_names = cz_names.drop_duplicates()
cz_names.to_excel('cz_names_viaf2.xlsx', index = False)


#swedish database
swe_names = copy.copy(df_names).replace(r'^\s*$', np.nan, regex=True)
swe_names = swe_names.loc[swe_names['IDs'].str.contains("SELIBR") == True].reset_index(drop = True)
swe_names = swe_names[['cz_name', 'viaf_id', 'IDs', 'name_variants', 'IDs_for_names']]
swe_names = df_explode(swe_names, ['name_variants', 'IDs_for_names'], '~')
swe_names = swe_names.loc[swe_names['IDs_for_names'].str.contains("SELIBR") == True].reset_index(drop = True)
swe1 = swe_names[['cz_name', 'viaf_id']].drop_duplicates()
swe2 = swe_names[['name_variants', 'viaf_id']]
swe2.columns = swe1.columns
swe_names = pd.concat([swe1, swe2]).sort_values(['viaf_id', 'cz_name']).values.tolist()

# swedish database harvesting
full_data = pd.DataFrame()
errors = []
for name_index, (name, viaf) in enumerate(swe_names):
    try:
         print(str(name_index) + '/' + str(len(swe_names)-1))
         url = f'http://libris.kb.se/xsearch?query=forf:({name.replace(" ", "%20")})&start=1&n=200&format=marcxml&format_level=full'
         response = requests.get(url)
         with open('test.xml', 'wb') as file:
             file.write(response.content)
         tree = et.parse('test.xml')
         root = tree.getroot()
         try:
             number_of_records = int(root.attrib['records'])
             if 0 < number_of_records <= 200:
                 xml_to_mrk('test.xml')
                 df = pd.read_table('test.mrk',skip_blank_lines=True, header = None)
                 df.columns = ['original']
                 df['field'] = df['original'].str.extract(r'(?<=\=)(...)')
                 df['content'] = df['original'].str.extract(r'(?<=  )(.*$)')
                 df['ldr'] = df.apply(lambda x: f(x, 'LDR'), axis = 1).ffill()
                 df['id'] = df.apply(lambda x: f(x, '001'), axis = 1)
                 df['id'] = df.groupby('ldr')['id'].apply(lambda x: x.ffill().bfill())
                 df = df[['id', 'field', 'content']]
                 df['content'] = df.groupby(['id', 'field'])['content'].transform(lambda x: '|'.join(x.drop_duplicates().astype(str)))
                 df = df.drop_duplicates().reset_index(drop=True)
                 df['name'] = name
                 df['viaf'] = viaf
                 df_people = df[['id', 'name', 'viaf']].drop_duplicates()
                 df_wide = df.pivot(index = 'id', columns = 'field', values = 'content')
                 df_wide = pd.merge(df_wide, df_people,  how='left', left_on = 'id', right_on = 'id')
                 full_data = pd.concat(full_data, df_wide)
             elif number_of_records > 200:
                x = range(1, number_of_records + 1, 200)
                for page_index, i in enumerate(x):
                    if i == 1:
                        xml_to_mrk('test.xml')
                        df = pd.read_table('test.mrk',skip_blank_lines=True, header = None)
                        df.columns = ['original']
                        df['field'] = df['original'].str.extract(r'(?<=\=)(...)')
                        df['content'] = df['original'].str.extract(r'(?<=  )(.*$)')
                        df['ldr'] = df.apply(lambda x: f(x, 'LDR'), axis = 1).ffill()
                        df['id'] = df.apply(lambda x: f(x, '001'), axis = 1)
                        df['id'] = df.groupby('ldr')['id'].apply(lambda x: x.ffill().bfill())
                        df = df[['id', 'field', 'content']]
                        df['content'] = df.groupby(['id', 'field'])['content'].transform(lambda x: '|'.join(x.drop_duplicates().astype(str)))
                        df = df.drop_duplicates().reset_index(drop=True)
                        df['name'] = name
                        df['viaf'] = viaf
                        df_people = df[['id', 'name', 'viaf']].drop_duplicates()
                        df_wide = df.pivot(index = 'id', columns = 'field', values = 'content')
                        df_wide = pd.merge(df_wide, df_people,  how='left', left_on = 'id', right_on = 'id')
                        full_data = pd.concat(full_data, df_wide)
                    else:
                        link = f'http://libris.kb.se/xsearch?query=forf:({name.replace(" ", "%20")})&start={i}&n=200&format=marcxml&format_level=full'
                        response = requests.get(link)
                        with open('test.xml', 'wb') as file:
                            file.write(response.content)
                        xml_to_mrk('test.xml')
                        df = pd.read_table('test.mrk',skip_blank_lines=True, header = None)
                        df.columns = ['original']
                        df['field'] = df['original'].str.extract(r'(?<=\=)(...)')
                        df['content'] = df['original'].str.extract(r'(?<=  )(.*$)')
                        df['ldr'] = df.apply(lambda x: f(x, 'LDR'), axis = 1).ffill()
                        df['id'] = df.apply(lambda x: f(x, '001'), axis = 1)
                        df['id'] = df.groupby('ldr')['id'].apply(lambda x: x.ffill().bfill())
                        df = df[['id', 'field', 'content']]
                        df['content'] = df.groupby(['id', 'field'])['content'].transform(lambda x: '|'.join(x.drop_duplicates().astype(str)))
                        df = df.drop_duplicates().reset_index(drop=True)
                        df['name'] = name
                        df['viaf'] = viaf
                        df_people = df[['id', 'name', 'viaf']].drop_duplicates()
                        df_wide = df.pivot(index = 'id', columns = 'field', values = 'content')
                        df_wide = pd.merge(df_wide, df_people,  how='left', left_on = 'id', right_on = 'id')
                        full_data = pd.concat(full_data, df_wide)
             else:
                 errors.append([name_index, name, viaf]) 
         except:
             errors.append([name_index, name, viaf])
    except:
        errors.append([name_index, name, viaf])
                        
                 
                 
                 
                 
                







# DIY xml parser - abandoned
# =============================================================================
# data_full = []
# errors = []
# for name_index, (name, viaf) in enumerate(swe_names):
#     try:
#         print(str(name_index) + '/' + str(len(swe_names)-1))
#         url = f'http://libris.kb.se/xsearch?query=forf:({name.replace(" ", "%20")})&start=1&n=200&format=marcxml&format_level=full'
#         response = requests.get(url)
#         with open('test.xml', 'wb') as file:
#             file.write(response.content)
#         tree = et.parse('test.xml')
#         root = tree.getroot()
#         try:
#             number_of_records = int(root.attrib['records'])
#             if 0 < number_of_records <= 200:
#                 for record_index, record in enumerate(root.findall('.//{http://www.loc.gov/MARC21/slim}record')):
#                     for field in record:
#                         field_type = re.sub(r'\{.*?\}', '', field.tag)
#                         if len(list(field.attrib.items())) > 0:
#                             field_name = field.attrib['tag']
#                         else:
#                             field_name = '000'
#                         if field.text is None:
#                             field_content = '|'.join([f'${list(child.attrib.items())[-1][-1]}{child.text}' for child in field.getchildren()])
#                         else:
#                             field_content = field.text
#                         data_full.append([name_index, 0, record_index, field_type, field_name, field_content, name, viaf])
#             elif number_of_records > 200:
#                 x = range(1, number_of_records + 1, 200)
#                 for page_index, i in enumerate(x):
#                     if i == 1:
#                         for record_index, record in enumerate(root.findall('.//{http://www.loc.gov/MARC21/slim}record')):
#                             for field in record:
#                                 field_type = re.sub(r'\{.*?\}', '', field.tag)
#                                 if len(list(field.attrib.items())) > 0:
#                                     field_name = field.attrib['tag']
#                                 else:
#                                     field_name = '000'
#                                 if field.text is None:
#                                     field_content = '|'.join([f'${list(child.attrib.items())[-1][-1]}{child.text}' for child in field.getchildren()])
#                                 else:
#                                     field_content = field.text
#                                 data_full.append([name_index, page_index, record_index, field_type, field_name, field_content, name, viaf])
#                     else:
#                         link = f'http://libris.kb.se/xsearch?query=forf:({name.replace(" ", "%20")})&start={i}&n=200&format=marcxml&format_level=full'
#                         response = requests.get(link)
#                         with open('test.xml', 'wb') as file:
#                             file.write(response.content)
#                         ntree = et.parse('test.xml')
#                         nroot = ntree.getroot()
#                         for record_index, record in enumerate(nroot.findall('.//{http://www.loc.gov/MARC21/slim}record')):
#                             for field in record:
#                                 field_type = re.sub(r'\{.*?\}', '', field.tag)
#                                 if len(list(field.attrib.items())) > 0:
#                                     field_name = field.attrib['tag']
#                                 else:
#                                     field_name = '000'
#                                 if field.text is None:
#                                     field_content = '|'.join([f'${list(child.attrib.items())[-1][-1]}{child.text}' for child in field.getchildren()])
#                                 else:
#                                     field_content = field.text
#                                 data_full.append([name_index, page_index, record_index, field_type, field_name, field_content, name, viaf])
#             else:
#                 data_full.append([name_index, np.nan, np.nan, np.nan, np.nan, np.nan, name, viaf])
#         except KeyError:
#             errors.append([name_index, name, viaf]) 
#     except et.ParseError:
#         errors.append([name_index, name, viaf]) 
#         
# swe_errors = pd.DataFrame(errors, columns = ['index', 'name', 'viaf_id'])
# swe_errors.to_excel('swe_errors1.xlsx', index = False)
# 
# swedish_df = pd.DataFrame(data_full, columns =['name_index', 'page_index', 'record', 'field_type', 'field_name', 'field_content', 'person_name', 'viaf_id']).sort_values(
#     ['name_index', 'page_index', 'record', 'field_name', 'field_content'])
# groups = swedish_df['name_index'].drop_duplicates().values.tolist()
# 
# new_data_frame = pd.DataFrame(columns = swedish_df.columns)
# for index, group in enumerate(groups):
#     print(str(index) + '/' + str(len(groups)))
#     test_df = swedish_df.loc[swedish_df['name_index'] == group]
#     if len(test_df) > 1:    
#         test_df['field_content'] = test_df.groupby(['name_index', 'page_index', 'record', 'field_name'])['field_content'].transform(
#             lambda x: '~'.join(x.drop_duplicates().astype(str)))
#         test_df = test_df.drop_duplicates()    
#         test_df['id'] = test_df.apply(lambda x: f(x, '001'), axis = 1)
#         test_df['id'] = test_df.groupby(['name_index', 'page_index', 'record'])['id'].apply(lambda x: x.ffill().bfill())
#         test_df['field_name'].loc[test_df['field_name'] == '000'] = 'LDR'
#         new_data_frame = pd.concat([new_data_frame, test_df])
#     else:
#         new_data_frame = pd.concat([new_data_frame, test_df])
#         
# 
# #after the loop
# swedish_data = new_data_frame[['id', 'field_name', 'field_content']].drop_duplicates()
# swedish_people = new_data_frame[['id', 'person_name', 'viaf_id']].drop_duplicates()
# swedish_df_wide = swedish_data.pivot(index='id', columns='field_name', values='field_content')
# swedish_df_wide = pd.merge(swedish_df_wide, swedish_people,  how='left', left_on = 'id', right_on = 'id')
# 
# swedish_df_wide.to_excel('swe_data.xlsx', index = False)
# =============================================================================
