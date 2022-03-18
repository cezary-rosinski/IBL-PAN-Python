import pandas as pd
from tqdm import tqdm
import requests
import xml.etree.ElementTree as et
import io

def flatten(foo):
    for x in foo:
        if hasattr(x, '__iter__') and not isinstance(x, str):
            for y in flatten(x):
                yield y
        else:
            yield x

data = pd.read_excel("C:\\Users\\Cezary\\Downloads\\missing_names_plus_data.xlsx")
data_list = data[data['viaf_id'].isnull()]['author_id'].drop_duplicates().to_list()


ns = '{http://viaf.org/viaf/terms#}'
viaf_enrichment = []
viaf_errors = []
for element in tqdm(data_list):
    try:
        url = f"http://viaf.org/viaf/sourceID/NKC%7C{element}/viaf.xml"
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
        person = [element, viaf_id, IDs, nationality, occupation, language, name_source]
        viaf_enrichment.append(person)
    except IndexError:
        error = [element]
        viaf_errors.append(error)       
              
viaf_df = pd.DataFrame(viaf_enrichment, columns=['cz_id', 'viaf_id', 'IDs', 'nationality', 'occupation', 'language', 'name_and_source'])

viaf_df = pd.merge(viaf_df, data[['author_id', 'author']], how='left', left_on='cz_id', right_on='author_id').drop(columns=['author_id']).drop_duplicates()
viaf_df['author'] = viaf_df['author'].str.replace(',$', '', regex=True)
viaf_df['all_names'] = ''


for i, row in tqdm(viaf_df.iterrows(), total=viaf_df.shape[0]):
    
    url = f"http://viaf.org/viaf/sourceID/NKC%7C{row['cz_id']}/viaf.json"
    response = requests.get(url).json()
    
    
    try:
        all_names = [(f"{e['datafield']['@ind1']}{e['datafield']['@ind2']}".replace(' ', '\\'), e['datafield']['subfield']) for e in response['mainHeadings']['mainHeadingEl'] if e['datafield']['@dtype'] == 'MARC21']
        all_names = '❦'.join([f"{e[0]}${e[-1]['@code']}{e[-1]['#text']}" if isinstance(e[-1], dict) else e[0]+''.join([f"${el['@code']}{el['#text']}" for el in e[-1]]) for e in all_names])
    except TypeError:
        all_names = {k:v for k,v in response['mainHeadings']['mainHeadingEl']['datafield'].items() if response['mainHeadings']['mainHeadingEl']['datafield']['@dtype'] == 'MARC21'}
        if isinstance(all_names['subfield'], list):
            all_names = f"{all_names['@ind1']}{all_names['@ind2']}".replace(' ', '\\') + ''.join([f"${el['@code']}{el['#text']}" for el in all_names['subfield']])
        else: all_names = f"{all_names['@ind1']}{all_names['@ind2']}".replace(' ', '\\') + f"{all_names['subfield']['@code']}{all_names['subfield']['#text']}"
    viaf_df.at[i,'all_names'] = all_names


viaf_df.to_excel('cz_viaf.xlsx', index = False)







































