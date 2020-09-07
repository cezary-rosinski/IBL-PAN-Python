from my_functions import gsheet_to_df
import xml.etree.ElementTree as et
import requests
import pandas as pd
import re
import numpy as np
from my_functions import xml_to_mrk
from my_functions import cSplit
from my_functions import f
from my_functions import marc_parser_1_field
import pymarc
import io
from bs4 import BeautifulSoup
from my_functions import cosine_sim_2_elem
import glob
import regex
import unidecode
import pandasql
import regex

### def

def add_viaf(x):
    return x + 'viaf.xml'

def author(row, field):
    if row['field'] == field:
        val = row['field']
    else:
        val = np.nan
    return val

### code
    
# =============================================================================
# # table for personal authorities
# path_in = "C:/Users/Cezary/Downloads/CLOselection.txt"
# encoding = 'utf-8'
# reader = io.open(path_in, 'rt', encoding = encoding).read().splitlines()
# 
# reader = list(filter(None, reader))  
# df = pd.DataFrame(reader, columns = ['test'])
# df['id'] = df['test'].replace(r'^(.{9})(.+?$)', r'\1', regex=True)
# df['field'] = df['test'].replace(r'^(.{10})(.{3})(.+?$)', r'\2', regex=True)
# df['content'] = df['test'].replace(r'^(.{18})(.+?$)', r'\2', regex=True)
# df.loc[df['field'] == '   ', 'field'] = 'ZZZ'
# df = df[['id', 'field', 'content']]
# df['content'] = df.groupby(['id', 'field'])['content'].transform(lambda x: '❦'.join(x.drop_duplicates().astype(str)))
# df = df.drop_duplicates().reset_index(drop=True)
# df_wide = df.pivot(index = 'id', columns = 'field', values = 'content')
# 
# fields_order = df_wide.columns.tolist()
# fields_order.remove('LDR')
# fields_order.sort()
# fields_order = ['LDR'] + fields_order
# df_wide = df_wide.reindex(columns=fields_order)
# 
# df_wide.to_excel('cz_authorities.xlsx')
# 
# X100 = marc_parser_1_field(df_wide, '001', '100', '\$\$')
# X100 = X100[(X100['$$7'] != '') |
#             (X100['$$d'] != '')]
# X100['index'] = X100.index + 1
# 
# ns = '{http://viaf.org/viaf/terms#}'
# viaf_enrichment = []
# viaf_errors = []
# for index, row in X100.iterrows():
#     print(str(index) + '/' + str(len(X100)))
#     try:
#         if row['$$7'] != '':
#             url = f"http://viaf.org/viaf/sourceID/NKC%7C{row['$$7']}/viaf.xml"
#             response = requests.get(url)
#             with open('viaf.xml', 'wb') as file:
#                 file.write(response.content)
#             tree = et.parse('viaf.xml')
#             root = tree.getroot()
#             viaf_id = root.findall(f'.//{ns}viafID')[0].text
#             IDs = root.findall(f'.//{ns}mainHeadings/{ns}data/{ns}sources/{ns}sid')
#             IDs = '❦'.join([t.text for t in IDs])
#             nationality = root.findall(f'.//{ns}nationalityOfEntity/{ns}data/{ns}text')
#             nationality = '❦'.join([t.text for t in nationality])
#             occupation = root.findall(f'.//{ns}occupation/{ns}data/{ns}text')
#             occupation = '❦'.join([t.text for t in occupation])
#             language = root.findall(f'.//{ns}languageOfEntity/{ns}data/{ns}text')
#             language = '❦'.join([t.text for t in language])
#             names = root.findall(f'.//{ns}x400/{ns}datafield')
#             sources = root.findall(f'.//{ns}x400/{ns}sources')
#             name_source = []
#             for (name, source) in zip(names, sources):   
#                 person_name = ' '.join([child.text for child in name.getchildren() if child.tag == f'{ns}subfield' and child.attrib['code'].isalpha()])
#                 library = '~'.join([child.text for child in source.getchildren() if child.tag == f'{ns}sid'])
#                 name_source.append([person_name, library])   
#             for i, elem in enumerate(name_source):
#                 name_source[i] = '‽'.join(name_source[i])
#             name_source = '❦'.join(name_source)
#             
#             person = [row['index'], row['$$a'], row['$$d'], viaf_id, IDs, nationality, occupation, language, name_source]
#             viaf_enrichment.append(person)
#         else:
#             try:
#                 url = re.sub('\s+', '%20', f"http://viaf.org/viaf/search?query=local.personalNames%20all%20%22{row['$$a']} {row['$$d']}%22%20and%20local.sources%20any%20%22nkc%22&sortKeys=holdingscount&recordSchema=BriefVIAF")
#                 response = requests.get(url)
#                 response.encoding = 'UTF-8'
#                 soup = BeautifulSoup(response.text, 'html.parser')
#                 people_links = soup.findAll('a', attrs={'href': re.compile("viaf/\d+")})
#                 viaf_people = []
#                 for people in people_links:
#                     person_name = re.sub('\s+', ' ', people.text).strip().split('\u200e ')
#                     person_link = re.sub(r'(.+?)(\#.+$)', r'http://viaf.org\1viaf.xml', people['href'].strip())
#                     viaf_people.append([person_name, person_link])
#                 viaf_people = pd.DataFrame(viaf_people, columns=['viaf name', 'viaf'])
#                 viaf_people = pd.DataFrame(viaf_people['viaf name'].tolist(), viaf_people['viaf']).stack()
#                 viaf_people = viaf_people.reset_index()[[0, 'viaf']]
#                 viaf_people.columns = ['viaf name', 'viaf']
#                 viaf_people['cz name'] = f"{row['$$a']} {row['$$d']}"
#                 for ind, vname in viaf_people.iterrows():
#                     viaf_people.at[ind, 'cosine'] = cosine_sim_2_elem([vname['viaf name'], vname['cz name']]).iloc[:, -1].to_string(index=False).strip()
#                 viaf_people = viaf_people[viaf_people['cosine'] == viaf_people['cosine'].max()]
#                 for i, line in viaf_people.iterrows():
#                     url = line['viaf']
#                     response = requests.get(url)
#                     with open('viaf.xml', 'wb') as file:
#                         file.write(response.content)
#                     tree = et.parse('viaf.xml')
#                     root = tree.getroot()
#                     viaf_id = root.findall(f'.//{ns}viafID')[0].text
#                     IDs = root.findall(f'.//{ns}mainHeadings/{ns}data/{ns}sources/{ns}sid')
#                     IDs = '❦'.join([t.text for t in IDs])
#                     nationality = root.findall(f'.//{ns}nationalityOfEntity/{ns}data/{ns}text')
#                     nationality = '❦'.join([t.text for t in nationality])
#                     occupation = root.findall(f'.//{ns}occupation/{ns}data/{ns}text')
#                     occupation = '❦'.join([t.text for t in occupation])
#                     language = root.findall(f'.//{ns}languageOfEntity/{ns}data/{ns}text')
#                     language = '❦'.join([t.text for t in language])
#                     names = root.findall(f'.//{ns}x400/{ns}datafield')
#                     sources = root.findall(f'.//{ns}x400/{ns}sources')
#                     name_source = []
#                     for (name, source) in zip(names, sources):   
#                         person_name = ' '.join([child.text for child in name.getchildren() if child.tag == f'{ns}subfield' and child.attrib['code'].isalpha()])
#                         library = '~'.join([child.text for child in source.getchildren() if child.tag == f'{ns}sid'])
#                         name_source.append([person_name, library])   
#                     for i, elem in enumerate(name_source):
#                         name_source[i] = '‽'.join(name_source[i])
#                     name_source = '❦'.join(name_source)
#                     
#                     person = [row['index'], row['$$a'], row['$$d'], viaf_id, IDs, nationality, occupation, language, name_source]
#                     viaf_enrichment.append(person)
#             except (KeyError, IndexError):
#                 person = [row['index'], row['$$a'], row['$$d'], '', '', '', '', '', '']
#                 viaf_enrichment.append(person)
#     except IndexError:
#         error = [row['index'], row['$$a'], row['$$d']]
#         viaf_errors.append(error)       
#              
# viaf_df = pd.DataFrame(viaf_enrichment, columns=['index', 'cz_name', 'cz_dates', 'viaf_id', 'IDs', 'nationality', 'occupation', 'language', 'name_and_source'])
# errorfile = io.open('cz_translation_errors.txt', 'wt', encoding = 'UTF-8')
# for element in viaf_errors:
#     errorfile.write(str(element) + '\n\n')
# errorfile.close()
# 
# viaf_df.to_excel('cz_viaf.xlsx', index = False)
# =============================================================================

#google sheets
df_names = gsheet_to_df('1QB5EmMhg7qSWfWaJurafHdmXc5uohQpS-K5GBsiqerA', 'Sheet1').drop_duplicates()
df_names = df_names.loc[(~df_names['czy_czech'].isin(['nie', 'boardercase'])) &
                        (df_names['viaf_id'] != '')]
df_names.fillna(value=pd.np.nan, inplace=True)
df_names['index'] = df_names['index'].astype(int)

blacklist = gsheet_to_df('1QB5EmMhg7qSWfWaJurafHdmXc5uohQpS-K5GBsiqerA', 'blacklist')
# test filtering
test_names = gsheet_to_df('1QB5EmMhg7qSWfWaJurafHdmXc5uohQpS-K5GBsiqerA', 'test_list').values.tolist()
test_names = [item for sublist in test_names for item in sublist]
df_names = df_names[df_names['viaf_id'].isin(test_names)]

# Swedish database
# with dates and without dates - OV decides
swe_names = df_names.copy()
swe_names_cz = swe_names[['index', 'cz_name', 'viaf_id']]
swe_names = swe_names.loc[swe_names['IDs'].str.contains("SELIBR") == True].reset_index(drop = True)
# swe_names['cz_name'] = swe_names.apply(lambda x: f"{x['cz_name']} {x['cz_dates']}", axis=1)
swe_names = swe_names[['index', 'cz_name', 'viaf_id', 'IDs', 'name_and_source']]
swe_names = cSplit(swe_names, 'index', 'name_and_source', '❦')
swe1 = swe_names[['index', 'cz_name', 'viaf_id']].drop_duplicates()
swe_names = swe_names.loc[swe_names['name_and_source'].str.contains("SELIBR") == True].reset_index(drop = True)   
swe_names = cSplit(swe_names, 'index', 'name_and_source', '‽', 'wide', 1).drop_duplicates()
swe2 = swe_names[['index', 'name_and_source_0', 'viaf_id']]
swe2.columns = swe1.columns
swe_names = pd.concat([swe_names_cz, swe1, swe2]).sort_values(['viaf_id', 'cz_name']).drop_duplicates().values.tolist()     
        
# Swedish database harvesting
full_data = pd.DataFrame()
errors = []
swe_marc = pymarc.TextWriter(io.open('swe.mrk', 'wt', encoding = 'utf-8'))
for name_index, (index, name, viaf) in enumerate(swe_names):
    print(str(name_index) + '/' + str(len(swe_names)-1))
    url = f'http://libris.kb.se/xsearch?query=forf:({name.replace(" ", "%20")})&start=1&n=200&format=marcxml&format_level=full'
    response = requests.get(url)
    with open('test.xml', 'wb') as file:
        file.write(response.content)
    tree = et.parse('test.xml')
    root = tree.getroot()
    number_of_records = int(root.attrib['records'])
    if 0 < number_of_records <= 200:
        xml_to_mrk('test.xml', 'test.mrk')
        records = pymarc.map_xml(swe_marc.write, 'test.xml')         
        df = pd.read_table('test.mrk',skip_blank_lines=True, header = None)
        df.columns = ['original']
        df['field'] = df['original'].str.extract(r'(?<=\=)(...)')
        df['content'] = df['original'].str.extract(r'(?<=  )(.*$)')
        df['ldr'] = df.apply(lambda x: f(x, 'LDR'), axis = 1).ffill()
        df['id'] = df.apply(lambda x: f(x, '001'), axis = 1)
        df['id'] = df.groupby('ldr')['id'].apply(lambda x: x.ffill().bfill())
        df = df[['id', 'field', 'content']]
        df['content'] = df.groupby(['id', 'field'])['content'].transform(lambda x: '❦'.join(x.drop_duplicates().astype(str)))
        df = df.drop_duplicates().reset_index(drop=True)
        df['name'] = name
        df['viaf'] = viaf
        df['index'] = index
        df_people = df[['id', 'index', 'name', 'viaf']].drop_duplicates()
        df_wide = df.pivot(index = 'id', columns = 'field', values = 'content')
        df_wide = df_wide[df_wide['LDR'].str.contains('^.{6}am', regex=True)]
        df_wide = pd.merge(df_wide, df_people,  how='left', left_on = 'id', right_on = 'id')
        full_data = full_data.append(df_wide)
    elif number_of_records > 200:
        x = range(1, number_of_records + 1, 200)
        for page_index, i in enumerate(x):
            if i == 1:
                xml_to_mrk('test.xml', 'test.mrk')
                records = pymarc.map_xml(swe_marc.write, 'test.xml')
                df = pd.read_table('test.mrk',skip_blank_lines=True, header = None)
                df.columns = ['original']
                df['field'] = df['original'].str.extract(r'(?<=\=)(...)')
                df['content'] = df['original'].str.extract(r'(?<=  )(.*$)')
                df['ldr'] = df.apply(lambda x: f(x, 'LDR'), axis = 1).ffill()
                df['id'] = df.apply(lambda x: f(x, '001'), axis = 1)
                df['id'] = df.groupby('ldr')['id'].apply(lambda x: x.ffill().bfill())
                df = df[['id', 'field', 'content']]
                df['content'] = df.groupby(['id', 'field'])['content'].transform(lambda x: '❦'.join(x.drop_duplicates().astype(str)))
                df = df.drop_duplicates().reset_index(drop=True)
                df['name'] = name
                df['viaf'] = viaf
                df['index'] = index
                df_people = df[['id', 'index', 'name', 'viaf']].drop_duplicates()
                df_wide = df.pivot(index = 'id', columns = 'field', values = 'content')
                df_wide = df_wide[df_wide['LDR'].str.contains('^.{6}am', regex=True)]
                df_wide = pd.merge(df_wide, df_people,  how='left', left_on = 'id', right_on = 'id')
                full_data = full_data.append(df_wide)
            else:
                link = f'http://libris.kb.se/xsearch?query=forf:({name.replace(" ", "%20")})&start={i}&n=200&format=marcxml&format_level=full'
                response = requests.get(link)
                with open('test.xml', 'wb') as file:
                    file.write(response.content)
                xml_to_mrk('test.xml', 'test.mrk')
                records = pymarc.map_xml(swe_marc.write, 'test.xml')
                df = pd.read_table('test.mrk',skip_blank_lines=True, header = None)
                df.columns = ['original']
                df['field'] = df['original'].str.extract(r'(?<=\=)(...)')
                df['content'] = df['original'].str.extract(r'(?<=  )(.*$)')
                df['ldr'] = df.apply(lambda x: f(x, 'LDR'), axis = 1).ffill()
                df['id'] = df.apply(lambda x: f(x, '001'), axis = 1)
                df['id'] = df.groupby('ldr')['id'].apply(lambda x: x.ffill().bfill())
                df = df[['id', 'field', 'content']]
                df['content'] = df.groupby(['id', 'field'])['content'].transform(lambda x: '❦'.join(x.drop_duplicates().astype(str)))
                df = df.drop_duplicates().reset_index(drop=True)
                df['name'] = name
                df['viaf'] = viaf
                df['index'] = index
                df_people = df[['id', 'index', 'name', 'viaf']].drop_duplicates()
                df_wide = df.pivot(index = 'id', columns = 'field', values = 'content')
                df_wide = df_wide[df_wide['LDR'].str.contains('^.{6}am', regex=True)]
                df_wide = pd.merge(df_wide, df_people,  how='left', left_on = 'id', right_on = 'id')
                full_data = full_data.append(df_wide)
    else:
        errors.append([name_index, index, name, viaf])
swe_marc.close()

fields_order = full_data.columns.tolist()
fields_order.sort(key = lambda x: ([str,int].index(type(1 if re.findall(r'\w+', x)[0].isdigit() else 'a')), x))
full_data = full_data.reindex(columns=fields_order)
# full_data.to_excel('swe_data.xlsx', index = False)
full_data.to_excel('swe_data_no_dates.xlsx', index = False)
swe_errors = pd.DataFrame(errors, columns = ['name_index', 'authority_index', 'name', 'viaf_id'])
swe_errors.to_excel('swe_errors.xlsx', index = False)
swe_names = pd.DataFrame(swe_names, columns = ['authority_index', 'name', 'viaf_id'])
swe_names.to_excel('swe_names.xlsx', index = False)

# Swedish harvesting Czech original

full_data = pd.DataFrame()
url = 'http://libris.kb.se/xsearch?query=(ORIG:cze)&format_level=full&format=marcxml&start=1&n=200'
response = requests.get(url)
with open('test.xml', 'wb') as file:
    file.write(response.content)
tree = et.parse('test.xml')
root = tree.getroot()
number_of_records = int(root.attrib['records'])
x = range(1, number_of_records + 1, 200)
for page_index, i in enumerate(x):
    print(str(page_index) + '/' + str(len(x)-1))
    if i == 1:
        xml_to_mrk('test.xml', 'test.mrk')
        df = pd.read_table('test.mrk',skip_blank_lines=True, header = None)
        df.columns = ['original']
        df['field'] = df['original'].str.extract(r'(?<=\=)(...)')
        df['content'] = df['original'].str.extract(r'(?<=  )(.*$)')
        df['ldr'] = df.apply(lambda x: f(x, 'LDR'), axis = 1).ffill()
        df['id'] = df.apply(lambda x: f(x, '001'), axis = 1)
        df['id'] = df.groupby('ldr')['id'].apply(lambda x: x.ffill().bfill())
        df = df[['id', 'field', 'content']]
        df['content'] = df.groupby(['id', 'field'])['content'].transform(lambda x: '❦'.join(x.drop_duplicates().astype(str)))
        df = df.drop_duplicates().reset_index(drop=True)
        df_wide = df.pivot(index = 'id', columns = 'field', values = 'content')
        df_wide = df_wide[df_wide['LDR'].str.contains('^.{6}am', regex=True)]
        full_data = full_data.append(df_wide)
    else:
        link = f'http://libris.kb.se/xsearch?query=(ORIG:cze)&format_level=full&format=marcxml&start={i}&n=200'
        response = requests.get(link)
        with open('test.xml', 'wb') as file:
            file.write(response.content)
        xml_to_mrk('test.xml', 'test.mrk')
        df = pd.read_table('test.mrk',skip_blank_lines=True, header = None)
        df.columns = ['original']
        df['field'] = df['original'].str.extract(r'(?<=\=)(...)')
        df['content'] = df['original'].str.extract(r'(?<=  )(.*$)')
        df['ldr'] = df.apply(lambda x: f(x, 'LDR'), axis = 1).ffill()
        df['id'] = df.apply(lambda x: f(x, '001'), axis = 1)
        df['id'] = df.groupby('ldr')['id'].apply(lambda x: x.ffill().bfill())
        df = df[['id', 'field', 'content']]
        df['content'] = df.groupby(['id', 'field'])['content'].transform(lambda x: '❦'.join(x.drop_duplicates().astype(str)))
        df = df.drop_duplicates().reset_index(drop=True)
        df_wide = df.pivot(index = 'id', columns = 'field', values = 'content')
        df_wide = df_wide[df_wide['LDR'].str.contains('^.{6}am', regex=True)]
        full_data = full_data.append(df_wide)

fields_order = full_data.columns.tolist()
fields_order.sort(key = lambda x: ([str,int].index(type(1 if re.findall(r'\w+', x)[0].isdigit() else 'a')), x))
full_data = full_data.reindex(columns=fields_order)
full_data.to_excel('swe_data_cz_origin.xlsx', index = False)

# Polish database
# with dates and without dates - OV decides

pol_names = df_names.copy()
pol_names_cz = pol_names[['index', 'cz_name', 'viaf_id']]
pol_names = pol_names.loc[pol_names['IDs'].str.contains("PLWABN") == True].reset_index(drop = True)
# pol_names['cz_name'] = pol_names.apply(lambda x: f"{x['cz_name']} {x['cz_dates']}", axis=1)
pol_names = pol_names[['index', 'cz_name', 'viaf_id', 'IDs', 'name_and_source']]
pol_names = cSplit(pol_names, 'index', 'name_and_source', '❦')
pol1 = pol_names[['index', 'cz_name', 'viaf_id']].drop_duplicates()
pol_names = pol_names.loc[pol_names['name_and_source'].str.contains("PLWABN") == True].reset_index(drop = True)   
pol_names = cSplit(pol_names, 'index', 'name_and_source', '‽', 'wide', 1).drop_duplicates()
pol2 = pol_names[['index', 'name_and_source_0', 'viaf_id']]
pol2.columns = pol1.columns
pol2['n_letters'] = pol2['cz_name'].apply(lambda x: len(''.join(re.findall('[a-zA-ZÀ-ž]', x))))
pol2['n_uppers'] = pol2['cz_name'].apply(lambda x: len(''.join(re.findall('[A-ZÀ-Ž]', x))))
pol2['has_digits'] = pol2['cz_name'].apply(lambda x: bool(re.findall('\d+', x)))
pol2 = pol2[(pol2['n_letters'] != pol2['n_uppers']) |
            (pol2['has_digits'] == True)]
pol2['n_initials'] = pol2['cz_name'].apply(lambda x: len(''.join(re.findall('(?<= )([A-ZÀ-Ž])(?=\.)', x))))
pol2['n_words'] = pol2['cz_name'].apply(lambda x: len(re.findall('([a-zA-ZÀ-ž]+)(?=[^a-zA-ZÀ-ž])', x)))
pol2 = pol2[(pol2['n_words'] != pol2['n_initials'] + 1) |
            (pol2['has_digits'] == True)]
pol2 = pol2[['index', 'cz_name', 'viaf_id']]
pol_names = pd.concat([pol_names_cz, pol1, pol2]).sort_values(['viaf_id', 'cz_name']).drop_duplicates().values.tolist()

# Polish database harvesting
ns = '{http://www.loc.gov/MARC21/slim}'
full_data = pd.DataFrame()
errors = []
pol_marc = pymarc.TextWriter(io.open('pol.mrk', 'wt', encoding = 'utf-8'))
for name_index, (index, name, viaf) in enumerate(pol_names):
    print(str(name_index) + '/' + str(len(pol_names)-1))
    try:
        url = f'https://data.bn.org.pl/api/bibs.marcxml?author={name.replace(" ", "%20")}&amp;limit=100'
        response = requests.get(url)
        with open('test.xml', 'wb') as file:
            file.write(response.content)
        tree = et.parse('test.xml')
        root = tree.getroot()
        while len(root.findall(f'.//{ns}record')) > 0:
            xml_to_mrk('test.xml', 'test.mrk')
            records = pymarc.map_xml(pol_marc.write, 'test.xml')         
            df = pd.read_table('test.mrk',skip_blank_lines=True, header = None)
            df.columns = ['original']
            df['field'] = df['original'].str.extract(r'(?<=\=)(...)')
            df['content'] = df['original'].str.extract(r'(?<=  )(.*$)')
            df['ldr'] = df.apply(lambda x: f(x, 'LDR'), axis = 1).ffill()
            df['id'] = df.apply(lambda x: f(x, '001'), axis = 1)
            df['id'] = df.groupby('ldr')['id'].apply(lambda x: x.ffill().bfill())
            df = df[['id', 'field', 'content']]
            df['content'] = df.groupby(['id', 'field'])['content'].transform(lambda x: '❦'.join(x.drop_duplicates().astype(str)))
            df = df.drop_duplicates().reset_index(drop=True)
            df['name'] = name
            df['viaf'] = viaf
            df['index'] = index
            df_people = df[['id', 'index', 'name', 'viaf']].drop_duplicates()
            df_wide = df.pivot(index = 'id', columns = 'field', values = 'content')
            df_wide = df_wide[df_wide['LDR'].str.contains('^.{6}am', regex=True)]
            df_wide = pd.merge(df_wide, df_people,  how='left', left_on = 'id', right_on = 'id')
            full_data = full_data.append(df_wide)
            url = root.find(f'.//nextPage').text
            response = requests.get(url)
            with open('test.xml', 'wb') as file:
                file.write(response.content)
            tree = et.parse('test.xml')
            root = tree.getroot()
    except:
        errors.append([name_index, index, name, viaf])
pol_marc.close()

fields_order = full_data.columns.tolist()
fields_order.sort(key = lambda x: ([str,int].index(type(1 if re.findall(r'\w+', x)[0].isdigit() else 'a')), x))
full_data = full_data.reindex(columns=fields_order)
# full_data.to_excel('pol_data.xlsx', index = False)
full_data.to_excel('pol_data_no_dates.xlsx', index = False)
pol_errors = pd.DataFrame(errors, columns = ['name_index', 'authority_index', 'name', 'viaf_id'])
pol_errors.to_excel('pol_errors.xlsx', index = False)
pol_names = pd.DataFrame(pol_names, columns = ['authority_index', 'name', 'viaf_id'])
pol_names.to_excel('pol_names.xlsx', index = False)

# Polish harvesting Czech original

ns = '{http://www.loc.gov/MARC21/slim}'
full_data = pd.DataFrame()
url = 'https://data.bn.org.pl/api/bibs.marcxml?limit=100&amp;marc=041h+cze'
response = requests.get(url)
with open('test.xml', 'wb') as file:
    file.write(response.content)
tree = et.parse('test.xml')
root = tree.getroot()
i = 1
while len(root.findall(f'.//{ns}record')) > 0:
    print(str(i))
    xml_to_mrk('test.xml', 'test.mrk')      
    df = pd.read_table('test.mrk',skip_blank_lines=True, header = None)
    df.columns = ['original']
    df['field'] = df['original'].str.extract(r'(?<=\=)(...)')
    df['content'] = df['original'].str.extract(r'(?<=  )(.*$)')
    df['ldr'] = df.apply(lambda x: f(x, 'LDR'), axis = 1).ffill()
    df['id'] = df.apply(lambda x: f(x, '001'), axis = 1)
    df['id'] = df.groupby('ldr')['id'].apply(lambda x: x.ffill().bfill())
    df = df[['id', 'field', 'content']]
    df['content'] = df.groupby(['id', 'field'])['content'].transform(lambda x: '❦'.join(x.drop_duplicates().astype(str)))
    df = df.drop_duplicates().reset_index(drop=True)
    df_wide = df.pivot(index = 'id', columns = 'field', values = 'content')
    df_wide = df_wide[df_wide['LDR'].str.contains('^.{6}am', regex=True)]
    full_data = full_data.append(df_wide)
    url = root.find(f'.//nextPage').text
    response = requests.get(url)
    with open('test.xml', 'wb') as file:
        file.write(response.content)
    tree = et.parse('test.xml')
    root = tree.getroot()
    i += 1
    
fields_order = full_data.columns.tolist()
fields_order.sort(key = lambda x: ([str,int].index(type(1 if re.findall(r'\w+', x)[0].isdigit() else 'a')), x))
full_data = full_data.reindex(columns=fields_order)
full_data.to_excel('pol_data_cz_origin.xlsx', index = False)

# Finnish database    
fi_names = df_names.copy()
fi_names_cz = fi_names[['index', 'cz_name', 'viaf_id']]
# fi_names['cz_name'] = fi_names.apply(lambda x: f"{x['cz_name']} {x['cz_dates']}", axis=1)
fi_names = fi_names[['index', 'cz_name', 'viaf_id', 'IDs', 'name_and_source']]
fi_names = cSplit(fi_names, 'index', 'name_and_source', '❦')
fi1 = fi_names[['index', 'cz_name', 'viaf_id']].drop_duplicates()
fi_names = cSplit(fi_names, 'index', 'name_and_source', '‽', 'wide', 1).drop_duplicates()
fi2 = fi_names[['index', 'name_and_source_0', 'viaf_id']]
fi2.columns = fi1.columns
def n_letters(x):
    try:
        val = len(''.join(regex.findall('\p{L}', x)))
    except TypeError:
        val = np.nan
    return val
fi2['n_letters'] = fi2['cz_name'].apply(lambda x: n_letters(x))
def n_uppers(x):
    try:
        val = len(''.join(regex.findall('\p{Lu}', x)))
    except TypeError:
        val = np.nan
    return val
fi2['n_uppers'] = fi2['cz_name'].apply(lambda x: n_uppers(x))
def has_digits(x):
    try:
        val = bool(re.findall('\d+', x))
    except TypeError:
        val = np.nan
    return val
fi2['has_digits'] = fi2['cz_name'].apply(lambda x: has_digits(x))
fi2 = fi2[(fi2['n_letters'] != fi2['n_uppers']) |
            (fi2['has_digits'] == True)]
def n_initials(x):
    try:
        val = len(''.join(regex.findall('(?<= )(\p{Lu})(?=\.)', x)))
    except TypeError:
        val = np.nan
    return val
fi2['n_initials'] = fi2['cz_name'].apply(lambda x: n_initials(x))
def n_words(x):
    try:
        val = len(regex.findall('(\p{L}+)(?=[^\p{L}])', x))
    except TypeError:
        val = np.nan
    return val
fi2['n_words'] = fi2['cz_name'].apply(lambda x: n_words(x))
fi2 = fi2[(fi2['n_words'] != fi2['n_initials'] + 1) |
            (fi2['has_digits'] == True)]
fi2 = fi2[['index', 'cz_name', 'viaf_id']]
fi_names = pd.concat([fi_names_cz, fi1, fi2]).sort_values(['viaf_id', 'cz_name']).drop_duplicates()
fi_names = fi_names[(fi_names['cz_name'].notna()) &
                    (fi_names['cz_name'].str.contains(' '))]

fi_names_table = fi_names.copy()
fi_names_table.columns = ['index', 'name', 'viaf_id']

fi_names = fi_names[['cz_name']].drop_duplicates().values.tolist()
fi_names = [re.escape(item) for sublist in fi_names for item in sublist]
fi_names = '|'.join(fi_names)

# Finnish database harvesting
path = 'F:/Cezary/Documents/IBL/Translations/Fennica/'
files = [f for f in glob.glob(path + '*.mrk', recursive=True)]

encoding = 'utf-8'
marc_df = pd.DataFrame()
for i, file_path in enumerate(files):
    print(str(i) + '/' + str(len(files)))
    marc_list = io.open(file_path, 'rt', encoding = encoding).read().splitlines()
    marc_list = list(filter(None, marc_list))  
    df = pd.DataFrame(marc_list, columns = ['test'])
    df['field'] = df['test'].replace(r'(^.)(...)(.+?$)', r'\2', regex = True)
    df['content'] = df['test'].replace(r'(^.)(.....)(.+?$)', r'\3', regex = True)
    df['help'] = df.apply(lambda x: f(x, 'LDR'), axis=1)
    df['help'] = df['help'].ffill()
    df_help = df.copy()[df['field'].isin(['100', '700'])]
    df_help = df_help[df_help['content'].str.contains(fi_names)]['help'].apply(lambda x: int(x)).values.tolist()
    df = df[df['help'].isin(df_help)]
    if len(df) > 0:
        df['id'] = df.apply(lambda x: f(x, '001'), axis = 1)
        df['id'] = df.groupby('help')['id'].ffill().bfill()
        df = df[['id', 'field', 'content']]
        df['content'] = df.groupby(['id', 'field'])['content'].transform(lambda x: '❦'.join(x.drop_duplicates().astype(str)))
        df = df.drop_duplicates().reset_index(drop=True)
        df_wide = df.pivot(index = 'id', columns = 'field', values = 'content')
        df_wide = df_wide[df_wide['LDR'].str.contains('^.{6}am', regex=True)]
        marc_df = marc_df.append(df_wide)

fields = marc_df.columns.tolist()
fields = [i for i in fields if 'LDR' in i or re.compile('\d{3}').findall(i)]
marc_df = marc_df.loc[:, marc_df.columns.isin(fields)]

fi_names = fi_names_table.copy()

X100 = marc_df[['001', '100']]
query = "select * from X100 a join fi_names b on a.'100' like '%'||b.name||'%'"
X100 = pandasql.sqldf(query)
X100 = X100[['001', 'index', 'name', 'viaf_id']]

X700 = marc_df[['001', '700']]
query = "select * from X700 a join fi_names b on a.'700' like '%'||b.name||'%'"
X700 = pandasql.sqldf(query)
X700 = X700[['001', 'index', 'name', 'viaf_id']]

X100700 = pd.concat([X100, X700]).drop_duplicates()
marc_df = pd.merge(marc_df, X100700, on='001', how='left')

fields_order = marc_df.columns.tolist()

fields_order.sort(key = lambda x: ([str,int].index(type(1 if re.findall(r'\w+', x)[0].isdigit() else 'a')), x))
marc_df = marc_df.reindex(columns=fields_order)
marc_df.to_excel('fi_data.xlsx', index=False)
        
# Czech database    
cz_names = pd.read_excel('cz_authorities.xlsx')
cz_names = marc_parser_1_field(cz_names, '001', '100', '\$\$')
cz_names = cz_names[(cz_names['$$7'] != '') |
                    (cz_names['$$d'] != '')]
cz_names['index'] = cz_names.index + 1
cz_names = pd.merge(cz_names, df_names[['index', 'viaf_id']], on='index', how='left')
cz_viaf = cz_names[['viaf_id']]
# test filtering
cz_names = cz_names[cz_names['viaf_id'].isin(test_names)]
cz_names_table = cz_names[['index', '$$a', 'viaf_id']]
cz_names_table.columns = ['index', 'name', 'viaf_id']
cz_names = cz_names['100'].apply(lambda x: x.replace('|', '')).apply(lambda x: x.replace('$$', '$')).values.tolist()
cz_names = [f'{e}$4aut' for e in cz_names]
cz_names = [re.escape(m) for m in cz_names]
cz_names = '|'.join(cz_names)

# Czech database harvesting
path = 'F:/Cezary/Documents/IBL/Translations/Czech database'
files = [f for f in glob.glob(path + '*.mrk', recursive=True)]

encoding = 'utf-8'
marc_df = pd.DataFrame()
for i, file_path in enumerate(files):
    print(str(i) + '/' + str(len(files)))
    marc_list = io.open(file_path, 'rt', encoding = encoding).read().splitlines()
    marc_list = list(filter(None, marc_list))  
    df = pd.DataFrame(marc_list, columns = ['test'])
    df['field'] = df['test'].replace(r'(^.)(...)(.+?$)', r'\2', regex = True)
    df['content'] = df['test'].replace(r'(^.)(.....)(.+?$)', r'\3', regex = True)
    df['help'] = df.apply(lambda x: f(x, 'LDR'), axis=1)
    df['help'] = df['help'].ffill()
    df_help = df.copy()[df['field'].isin(['100', '700'])]
    df_help = df_help[df_help['content'].str.contains(cz_names)]['help'].apply(lambda x: int(x)).values.tolist()
    df = df[df['help'].isin(df_help)]
    if len(df) > 0:
        df['id'] = df.apply(lambda x: f(x, '001'), axis = 1)
        df['id'] = df.groupby('help')['id'].ffill().bfill()
        df = df[['id', 'field', 'content']]
        df['content'] = df.groupby(['id', 'field'])['content'].transform(lambda x: '❦'.join(x.drop_duplicates().astype(str)))
        df = df.drop_duplicates().reset_index(drop=True)
        df_wide = df.pivot(index = 'id', columns = 'field', values = 'content')
        df_wide = df_wide[df_wide['LDR'].str.contains('^.{6}am', regex=True)]
        marc_df = marc_df.append(df_wide)

fields = marc_df.columns.tolist()
fields = [i for i in fields if 'LDR' in i or re.compile('\d{3}').findall(i)]
marc_df = marc_df.loc[:, marc_df.columns.isin(fields)]

cz_names = cz_names_table.copy()

X100 = marc_df[['001', '100']]
query = "select * from X100 a join cz_names b on a.'100' like '%'||b.name||'%'"
X100 = pandasql.sqldf(query)
X100 = X100[['001', 'index', 'name', 'viaf_id']]

X700 = marc_df[['001', '700']]
query = "select * from X700 a join cz_names b on a.'700' like '%'||b.name||'%'"
X700 = pandasql.sqldf(query)
X700 = X700[['001', 'index', 'name', 'viaf_id']]

X100700 = pd.concat([X100, X700]).drop_duplicates()
marc_df = pd.merge(marc_df, X100700, on='001', how='left')

fields_order = marc_df.columns.tolist()

fields_order.sort(key = lambda x: ([str,int].index(type(1 if re.findall(r'\w+', x)[0].isdigit() else 'a')), x))
marc_df = marc_df.reindex(columns=fields_order)
marc_df.to_excel('cz_data.xlsx', index=False)

# Cleaning data
swe_data = pd.read_excel('swe_data_no_dates.xlsx')
pol_data = pd.read_excel('pol_data_no_dates.xlsx')

# | zamienić po kolejnym harvestowaniu na ❦
def right_author(x):
    u_name = unidecode.unidecode(x['name']).lower()
    if pd.notnull(x['100']) and pd.isnull(x['700']):
        try:
            test_100 = re.findall(u_name, unidecode.unidecode(x['100'].lower()))[0]
            if '$' not in test_100 and test_100.count(',') <= 1:
                val = True
            else:
                val = False
        except IndexError:
            val = False
    elif pd.isnull(x['100']) and pd.notnull(x['700']):
        if '❦' not in x['700'] and '$4' in x['700']:
            pattern = f"(?>{u_name}\$4)(.{{3}})(?=\$|$)"
            try:
                test_700 = regex.findall(pattern, unidecode.unidecode(x['700'].lower()))[0]
                if test_700 in ['aut', 'oth']:
                    val = True
                else:
                    val = False
            except IndexError:
                val = False
        elif '❦' not in x['700'] and '$4' not in x['700']:
            try:
                re.findall(u_name, unidecode.unidecode(x['700'].lower()))[0]
                if '$e' not in x['700']:
                    val = True
                else:
                    val = False
            except IndexError:
                val = False
        elif '❦' in x['700']:
            x700 = x['700'].split('❦')
            result = []
            for elem in x700:
                if '$4' in elem:
                    u_name = unidecode.unidecode(swe_data.iloc[50,3].lower())
                    elem = swe_data.iloc[50, swe_data.columns.get_loc('700')].split('❦')[0]
                    
                    pattern = f"(?>{u_name}\$4)(.{{3}})(?=\$|$)"
                    try:
                        test_700 = regex.findall(pattern, unidecode.unidecode(elem.lower()))[0]
                        if test_700 in ['aut', 'oth']:
                            val = True
                        else:
                            val = False
                    except IndexError:
                        val = False
                    result.append(val)
                else:
                    try:
                        re.findall(u_name, unidecode.unidecode(elem.lower()))[0]
                        if '$e' not in elem:
                            val = True
                        else:
                            val = False
                    except IndexError:
                        val = False
                    result.append(val)
            if True in result:
                val = True
            else:
                val = False
    elif pd.notnull(x['100']) and pd.notnull(x['700']):
        try:
            test_100 = re.findall(u_name, unidecode.unidecode(x['100'].lower()))[0]
            if '$' not in test_100 and test_100.count(',') <= 1:
                val1 = True
            else:
                val1 = False
        except IndexError:
            val1 = False
        if '❦' not in x['700'] and '$4' in x['700']:
            pattern = f"(?>{u_name}\$4)(.{{3}})(?=\$|$)"
            try:
                test_700 = regex.findall(pattern, unidecode.unidecode(x['700'].lower()))[0]
                if test_700 in ['aut', 'oth']:
                    val2 = True
                else:
                    val2 = False
            except IndexError:
                val2 = False
        elif '❦' not in x['700'] and '$4' not in x['700']:
            try:
                re.findall(u_name, unidecode.unidecode(x['700'].lower()))[0]
                if '$e' not in x['700']:
                    val2 = True
                else:
                    val2 = False
            except IndexError:
                val2 = False
        elif '❦' in x['700']:
            x700 = x['700'].split('❦')
            result = []
            for elem in x700:
                if '$4' in elem:
                    pattern = f"(?>{u_name}\$4)(.{{3}})(?=\$|$)"
                    try:
                        test_700 = regex.findall(pattern, unidecode.unidecode(elem.lower()))[0]
                        if test_700 in ['aut', 'oth']:
                            val2 = True
                        else:
                            val2 = False
                    except IndexError:
                        val2 = False
                    result.append(val2)
                else:
                    try:
                        re.findall(u_name, unidecode.unidecode(elem.lower()))[0]
                        if '$e' not in elem:
                            val2 = True
                        else:
                            val2 = False
                    except IndexError:
                        val2 = False
                    result.append(val2)
            if True in result:
                val2 = True
            else:
                val2 = False
        if val1 == True or val2 == True:
            val = True
        else:
            val = False
    else:
        val = False
    return val

swe_data['right_author'] = swe_data.apply(lambda x: right_author(x), axis=1)
# swe_data = swe_data[swe_data['right_author'] == True]
# del swe_data['right_author']
swe_data.to_excel('swe_data_clean.xlsx', index=False)

pol_data['right_author'] = pol_data.apply(lambda x: right_author(x), axis=1)
# pol_data = pol_data[pol_data['right_author'] == True]
# del pol_data['right_author']
pol_data.to_excel('pol_data_clean.xlsx', index=False)




# Research section
# Connect appearances for several authors from all datasets










    
    





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
