from my_functions import from_gsheets_to_df
import xml.etree.ElementTree as et
import requests
import pandas as pd
import re
import numpy as np


#google sheets
df = from_gsheets_to_df('1QkZCzglN7w0AnEubuoQqHgQqib9Glild1x_3X8T_HyI', 'Sheet 1')

#XML

URL = "http://data.bn.org.pl/api/bibs.marcxml?limit=100&amp;marc=773t+Czas+Kultury"

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
            field_name = list(field.attrib.items())[-1][-1]
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

def f(row):
    if row['field_name'] == '009':
        val = row['field_content']
    else:
        val = np.nan
    return val    
  
df['id'] = df.apply(lambda x: f(x), axis = 1)
df['id'] = df.groupby('record')['id'].apply(lambda x: x.ffill().bfill())































    

