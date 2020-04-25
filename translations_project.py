from my_functions import gsheet_to_df
import xml.etree.ElementTree as et
import requests
import pandas as pd
import re
import numpy as np

### def
def f(row, id_field):
    if row['field_name'] == id_field:
        val = row['field_content']
    else:
        val = np.nan
    return val  

def add_viaf(x):
    return x + 'viaf.xml'

### code

#google sheets
df_names = gsheet_to_df('1QkZCzglN7w0AnEubuoQqHgQqib9Glild1x_3X8T_HyI', 'Sheet 1')
df_names = df_names.loc[df_names['czy_czech'] != 'nie']
names_list = [df_names['cz_name'].values.tolist(), df_names['viaf_id'].values.tolist()]

### viaf list of names
viafs = df_names.loc[df_names['viaf_id'].str.contains('viaf')]
viaf_list = viafs['viaf_id'].drop_duplicates().values.tolist()
viaf_list = list(map(add_viaf, viaf_list))[:100]

viaf_names = []
for index, viaf in enumerate(viaf_list):
    print(str(index) + '/' + str(len(viaf_list)-1))
    response = requests.get(viaf)
    with open('viaf.xml', 'wb') as file:
        file.write(response.content)
    tree = et.parse('viaf.xml')
    root = tree.getroot()
    v_names = root.findall('.//{http://viaf.org/viaf/terms#}x400/{http://viaf.org/viaf/terms#}datafield/{http://viaf.org/viaf/terms#}subfield')
    v_sources = root.findall('.//{http://viaf.org/viaf/terms#}x400/{http://viaf.org/viaf/terms#}sources/{http://viaf.org/viaf/terms#}sid')
    for (name, library) in zip(v_names, v_sources):
        if name.attrib['code'] == 'a':
            viaf_names.append([viaf, name.text, library.text])
        
cz_names = pd.DataFrame(viaf_names, columns =['viaf_id', 'names', 'sources']).drop_duplicates()
cz_names.to_csv('cz_names_viaf.csv', sep = ';', index = False)     




#swedish database
data_full = []
errors = []
for name_index, name in enumerate(names_list[0]):
    try:
        print(str(name_index) + '/' + str(len(names_list[0])-1))
        url = f'http://libris.kb.se/xsearch?query=forf:({name.replace(" ", "%20")})&start=1&n=200&format=marcxml&format_level=full'
        response = requests.get(url)
        with open('test.xml', 'wb') as file:
            file.write(response.content)
        tree = et.parse('test.xml')
        root = tree.getroot()
        number_of_records = int(root.attrib['records'])
        if 0 < number_of_records <= 200:
            for record_index, record in enumerate(root.findall('.//{http://www.loc.gov/MARC21/slim}record')):
                for field in record:
                    field_type = re.sub(r'\{.*?\}', '', field.tag)
                    if len(list(field.attrib.items())) > 0:
                        field_name = field.attrib['tag']
                    else:
                        field_name = '000'
                    if field.text is None:
                        field_content = '|'.join([f'${list(child.attrib.items())[-1][-1]}{child.text}' for child in field.getchildren()])
                    else:
                        field_content = field.text
                    data_full.append([name_index, 0, record_index, field_type, field_name, field_content, name, names_list[1][name_index]])
        elif number_of_records > 200:
            x = range(1, number_of_records + 1, 200)
            for page_index, i in enumerate(x):
                if i == 1:
                    for record_index, record in enumerate(root.findall('.//{http://www.loc.gov/MARC21/slim}record')):
                        for field in record:
                            field_type = re.sub(r'\{.*?\}', '', field.tag)
                            if len(list(field.attrib.items())) > 0:
                                field_name = field.attrib['tag']
                            else:
                                field_name = '000'
                            if field.text is None:
                                field_content = '|'.join([f'${list(child.attrib.items())[-1][-1]}{child.text}' for child in field.getchildren()])
                            else:
                                field_content = field.text
                            data_full.append([name_index, page_index, record_index, field_type, field_name, field_content, name, names_list[1][name_index]])
                else:
                    link = f'http://libris.kb.se/xsearch?query=forf:({name.replace(" ", "%20")})&start={i}&n=200&format=marcxml&format_level=full'
                    response = requests.get(link)
                    with open('test.xml', 'wb') as file:
                        file.write(response.content)
                    ntree = et.parse('test.xml')
                    nroot = ntree.getroot()
                    for record_index, record in enumerate(nroot.findall('.//{http://www.loc.gov/MARC21/slim}record')):
                        for field in record:
                            field_type = re.sub(r'\{.*?\}', '', field.tag)
                            if len(list(field.attrib.items())) > 0:
                                field_name = field.attrib['tag']
                            else:
                                field_name = '000'
                            if field.text is None:
                                field_content = '|'.join([f'${list(child.attrib.items())[-1][-1]}{child.text}' for child in field.getchildren()])
                            else:
                                field_content = field.text
                            data_full.append([name_index, page_index, record_index, field_type, field_name, field_content, name, names_list[1][name_index]])
        else:
            data_full.append([name_index, np.nan, np.nan, np.nan, np.nan, np.nan, name, names_list[1][name_index]])
    except et.ParseError:
        errors.append([name_index, name, names_list[1][name_index]])
        
                    
swedish_df = pd.DataFrame(data_full, columns =['name_index', 'page_index', 'record', 'field_type', 'field_name', 'field_content', 'person_name', 'viaf_id']).sort_values(
    ['name_index', 'page_index', 'record', 'field_name', 'field_content'])
#drop nan
swedish_df = swedish_df.loc[swedish_df['field_type'].notna()]

groups = swedish_df['name_index'].drop_duplicates().values.tolist()

new_data_frame = pd.DataFrame(columns = swedish_df.columns)
for index, group in enumerate(groups):
    print(str(index) + '/' + str(len(groups)))
    test_df = swedish_df.loc[swedish_df['name_index'] == group]
    if len(test_df) > 1:    
        test_df['field_content'] = test_df.groupby(['name_index', 'page_index', 'record', 'field_name'])['field_content'].transform(
            lambda x: '~'.join(x.drop_duplicates().astype(str)))
        test_df = test_df.drop_duplicates()    
        test_df['id'] = test_df.apply(lambda x: f(x, '001'), axis = 1)
        test_df['id'] = test_df.groupby(['name_index', 'page_index', 'record'])['id'].apply(lambda x: x.ffill().bfill())
        test_df['field_name'].loc[test_df['field_name'] == '000'] = 'LDR'
        new_data_frame = pd.concat([new_data_frame, test_df])
    else:
        new_data_frame = pd.concat([new_data_frame, test_df])
        






swedish_df['field_content'] = swedish_df.groupby(['name_index', 'page_index', 'record', 'field_name'])['field_content'].transform(
    lambda x: '~'.join(x.drop_duplicates().astype(str)))
swedish_df = swedish_df.drop_duplicates()    
  
swedish_df['id'] = swedish_df.apply(lambda x: f(x, '001'), axis = 1)
swedish_df['id'] = swedish_df.groupby(['name_index', 'page_index', 'record'])['id'].apply(lambda x: x.ffill().bfill())
swedish_df['field_name'].loc[swedish_df['field_name'] == '000'] = 'LDR'

#after the loop
swedish_data = swedish_df[['id', 'field_name', 'field_content']].drop_duplicates()
swedish_people = swedish_df[['id', 'person_name', 'viaf_id']].drop_duplicates()
swedish_df_wide = swedish_data.pivot(index='id', columns='field_name', values='field_content')
swedish_df_wide = pd.merge(swedish_df_wide, swedish_people,  how='left', left_on = 'id', right_on = 'id')


names_list[0][2659]
names_list[0][1394]
names_list[0][3619]



#XML

URL = "http://data.bn.org.pl/api/bibs.marcxml?limit=100&amp;marc=773t+Czas+Kultury"
URL = 'http://libris.kb.se/xsearch?query=forf:(Czesław%20Miłosz)&start=1&n=200&format=marcxml&format_level=full'
URL = 'http://libris.kb.se/xsearch?query=forf:(%C4%8Cech,%20Franti%C5%A1ek)&start=1&n=200&format=marcxml&format_level=full'

response = requests.get(URL)
with open('test.xml', 'wb') as file:
    file.write(response.content)
    
tree = et.parse('test.xml')
root = tree.getroot()

lista = []
for record_index, record in enumerate(root.findall('.//{http://www.loc.gov/MARC21/slim}record')):
    for field in record:
        field_type = re.sub(r'\{.*?\}', '', field.tag)
        if len(list(field.attrib.items())) > 0:
            field_name = field.attrib['tag']#list(field.attrib.items())[-1][-1]
        else:
            field_name = '000'
        if field.text is None:
            field_content = '|'.join([f'${list(child.attrib.items())[-1][-1]}{child.text}' for child in field.getchildren()])
        else:
            field_content = field.text
        lista.append([record_index, field_type, field_name, field_content])
        
df = pd.DataFrame(lista, columns =['record', 'field_type', 'field_name', 'field_content']).sort_values(['record', 'field_name', 'field_content'])
df['field_content'] = df.groupby(['record', 'field_name'])['field_content'].transform(lambda x: '~'.join(x.drop_duplicates().astype(str)))
df = df.drop_duplicates()  

  
  
df['id'] = df.apply(lambda x: f(x), axis = 1)
df['id'] = df.groupby('record')['id'].apply(lambda x: x.ffill().bfill())
df['field_name'].loc[df['field_name'] == '000'] = 'LDR'


#wide dataframe
df = df.pivot(index='id', columns='field_name', values='field_content')































    

