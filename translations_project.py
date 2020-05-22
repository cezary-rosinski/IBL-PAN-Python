from my_functions import gsheet_to_df
import xml.etree.ElementTree as et
import requests
import pandas as pd
import re
import numpy as np
import copy
from my_functions import df_explode
from my_functions import xml_to_mrk
from my_functions import cSplit
from my_functions import f
from my_functions import marc_parser_1_field
import pymarc
import io
from bs4 import BeautifulSoup

### def

def add_viaf(x):
    return x + 'viaf.xml'

### code
    
# table for personal authorities
path_in = "C:/Users/Cezary/Downloads/CLOselection.txt"
encoding = 'utf-8'
reader = io.open(path_in, 'rt', encoding = encoding).read().splitlines()

reader = list(filter(None, reader))  
df = pd.DataFrame(reader, columns = ['test'])
df['id'] = df['test'].replace(r'^(.{9})(.+?$)', r'\1', regex=True)
df['field'] = df['test'].replace(r'^(.{10})(.{3})(.+?$)', r'\2', regex=True)
df['content'] = df['test'].replace(r'^(.{18})(.+?$)', r'\2', regex=True)
df.loc[df['field'] == '   ', 'field'] = 'ZZZ'
df = df[['id', 'field', 'content']]
df['content'] = df.groupby(['id', 'field'])['content'].transform(lambda x: '❦'.join(x.drop_duplicates().astype(str)))
df = df.drop_duplicates().reset_index(drop=True)
df_wide = df.pivot(index = 'id', columns = 'field', values = 'content')

fields_order = df_wide.columns.tolist()
fields_order.remove('LDR')
fields_order.sort()
fields_order = ['LDR'] + fields_order
df_wide = df_wide.reindex(columns=fields_order)

X100 = marc_parser_1_field(df_wide, '001', '100', '\$\$')
X100 = X100[(X100['$$7'] != '') |
            (X100['$$d'] != '')]
X100['index'] = X100.index + 1

#testowy set
X100 = X100.head(100)

ns = '{http://viaf.org/viaf/terms#}'
viaf_enrichment = []
for index, row in X100.iterrows():
    print(str(index) + '/' + str(len(X100)))
    if row['$$7'] != '':
        #url = "https://viaf.org/viaf/110389400/viaf.xml"
        #url = "http://viaf.org/viaf/sourceID/NKC%7Cjk01010002/viaf.xml"
        url = f"http://viaf.org/viaf/sourceID/NKC%7C{row['$$7']}/viaf.xml"
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
        
        person = [row['index'], row['$$a'], viaf_id, IDs, nationality, occupation, language, name_source]
        viaf_enrichment.append(person)
    else:
     
# dodać else dla wyszukiwania po frazie (jak w R) i zrobić mechanizm cosine similarity, żeby oddało najbardziej odpowiednią wartość (jak w R)
    
test = pd.DataFrame(viaf_enrichment, columns=['index', 'cz_name', 'viaf_id', 'IDs', 'nationality', 'occupation', 'language', 'name_and_source'])



test = df[df['id'] == '000003200']
test = df.head(1000)


  
    










#google sheets
df_names = gsheet_to_df('1QkZCzglN7w0AnEubuoQqHgQqib9Glild1x_3X8T_HyI', 'Sheet 1')
df_names = df_names.loc[df_names['czy_czech'] != 'nie']
# names_list = [df_names['cz_name'].values.tolist(), df_names['viaf_id'].values.tolist()]

### viaf - appending list of names
# =============================================================================
# viafs = df_names.loc[df_names['viaf_id'].str.contains('viaf')]
# viaf_list = viafs['viaf_id'].drop_duplicates().values.tolist()
# viaf_list = list(map(add_viaf, viaf_list))
# 
# viaf_names = []
# for index, viaf in enumerate(viaf_list):
#     print(str(index) + '/' + str(len(viaf_list)-1))
#     response = requests.get(viaf)
#     with open('viaf.xml', 'wb') as file:
#         file.write(response.content)
#     tree = et.parse('viaf.xml')
#     root = tree.getroot()
#     v_names = root.findall('.//{http://viaf.org/viaf/terms#}x400/{http://viaf.org/viaf/terms#}datafield')
#     v_sources = root.findall('.//{http://viaf.org/viaf/terms#}x400/{http://viaf.org/viaf/terms#}sources')
#     for (name, library) in zip(v_names, v_sources):
#         # with dates: name = ' '.join([child.text for child in name.getchildren() if child.tag == '{http://viaf.org/viaf/terms#}subfield' and child.attrib['code'].isalpha()])
#         person_name = ' '.join([child.text for child in name.getchildren() if child.tag == '{http://viaf.org/viaf/terms#}subfield' and child.attrib['code'].isalpha() and child.attrib['code'] not in ['d', 'f']])
#         date = ' '.join([child.text for child in name.getchildren() if child.tag == '{http://viaf.org/viaf/terms#}subfield' and child.attrib['code'] in ['d', 'f']])
#         library = '~'.join([child.text for child in library.getchildren() if child.tag == '{http://viaf.org/viaf/terms#}sid'])
#         viaf_names.append([viaf, person_name, date, library])
#         
# cz_names = pd.DataFrame(viaf_names, columns =['viaf_id', 'names', 'dates', 'sources']).drop_duplicates()
# cz_names['Index'] = cz_names.index + 1
# cz_names = cSplit(cz_names, 'Index', 'sources', '~')[['viaf_id', 'names', 'dates', 'sources']]
# cz_names['viaf_id'] = cz_names['viaf_id'].str.replace('viaf.xml', '')
# cz_names.to_csv('cz_names_viaf_sep_dates.csv', sep = ';', index = False)
# =============================================================================

# =============================================================================
# for column in cz_names.iloc[:,1:]:
#     cz_names[column] = cz_names.groupby('viaf_id')[column].transform(lambda x: '~'.join(x.astype(str)))
# cz_names = cz_names.drop_duplicates()
# cz_names.to_excel('cz_names_viaf2.xlsx', index = False)
# =============================================================================


#swedish database
swe_names = copy.copy(df_names).replace(r'^\s*$', np.nan, regex=True)
swe_names = swe_names.loc[swe_names['IDs'].str.contains("SELIBR") == True].reset_index(drop = True)
swe_names = swe_names[['cz_name', 'viaf_id', 'IDs']]

cz_names_viaf = pd.read_csv("cz_names_viaf.csv", sep=';')

swe_names = pd.merge(swe_names, cz_names_viaf,  how='left', left_on = 'viaf_id', right_on = 'viaf_id')
swe_names = swe_names.loc[swe_names['sources'].str.contains("SELIBR") == True].reset_index(drop = True)
swe1 = swe_names[['cz_name', 'viaf_id']].drop_duplicates()
swe2 = swe_names[['names', 'viaf_id']]
swe2.columns = swe1.columns
swe_names = pd.concat([swe1, swe2]).sort_values(['viaf_id', 'cz_name']).values.tolist()

# swedish database harvesting
full_data = pd.DataFrame()
errors = []
swe_marc = pymarc.TextWriter(io.open('swe.mrk', 'wt', encoding = 'utf-8'))
for name_index, (name, viaf) in enumerate(swe_names):
    print(str(name_index) + '/' + str(len(swe_names)-1))
    url = f'http://libris.kb.se/xsearch?query=forf:({name.replace(" ", "%20")})&start=1&n=200&format=marcxml&format_level=full'
    response = requests.get(url)
    with open('test.xml', 'wb') as file:
        file.write(response.content)
    tree = et.parse('test.xml')
    root = tree.getroot()
    number_of_records = int(root.attrib['records'])
    if 0 < number_of_records <= 200:
        xml_to_mrk('test.xml')
        records = pymarc.map_xml(swe_marc.write, 'test.xml')         
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
        full_data = full_data.append(df_wide)
    elif number_of_records > 200:
        x = range(1, number_of_records + 1, 200)
        for page_index, i in enumerate(x):
            if i == 1:
                xml_to_mrk('test.xml')
                records = pymarc.map_xml(swe_marc.write, 'test.xml')
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
                full_data = full_data.append(df_wide)
            else:
                link = f'http://libris.kb.se/xsearch?query=forf:({name.replace(" ", "%20")})&start={i}&n=200&format=marcxml&format_level=full'
                response = requests.get(link)
                with open('test.xml', 'wb') as file:
                    file.write(response.content)
                xml_to_mrk('test.xml')
                records = pymarc.map_xml(swe_marc.write, 'test.xml')
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
                full_data = full_data.append(df_wide)
    else:
        errors.append([name_index, name, viaf])
swe_marc.close()

full_data.to_excel('swe_data.xlsx', index = False)
swe_errors = pd.DataFrame(errors, columns = ['index', 'name', 'viaf_id'])
swe_errors.to_excel('swe_errors.xlsx', index = False)
swe_names = pd.DataFrame(swe_names, columns = ['name', 'viaf_id'])
swe_names.to_excel('swe_names.xlsx', index = False)



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
