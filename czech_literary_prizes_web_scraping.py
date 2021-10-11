import time
start_time = time.time()
import requests
from bs4 import BeautifulSoup
import re
import pandas as pd
import numpy as np
import regex
from my_functions import cSplit
from tqdm import tqdm

#%% people
FormData = {'q' : '*',
            'so' : 've jménech osob'}
r = requests.post("https://ceny.ucl.cas.cz/index.php", data=FormData)
r.encoding = 'ISO 8859-2'
tekst = r.text
soup = BeautifulSoup(tekst, 'html.parser')
links = soup.findAll('a', attrs={'href': re.compile("^\?o\=")})

urls = []
for link in links:
    if link.text:
        urls.append([link.text.strip(), "http://ceny.ucl.cas.cz/index.php" + link['href']])

# urls = urls[:10]

data= {}
for i, url in tqdm(enumerate(urls, 1)):
    # url = urls[0]
    page = requests.get(url[1])
    page.encoding = 'ISO 8859-2'
    tree = BeautifulSoup(page.text, 'html.parser')
    try:
        data_person = tree.select_one('h2').text.strip()
    except:
        data_person = "brak danych (CR)"
    try:
        role = tree.select_one('h3').text
    except:
        role = "brak danych (CR)"
    try:
        description = tree.select_one('.indent').text.strip()
    except:
        description = "brak danych (CR)"
    data[i] = {'data_person': data_person, 'role': role, 'description': description}
    # data.append([data_person, role, description])
end_time = time.time()
print(end_time - start_time)

# df = pd.DataFrame(data, columns =['data_person', 'role', 'description'])
df = pd.DataFrame.from_dict(data, orient='index')

df['person_year'] = df['data_person'].apply(lambda x: re.sub('(.+)(, )(\d.+)', r'\3', x) if re.findall('\d{4}', x) else np.nan)
df['single_prize'] = df['description'].apply(lambda x: regex.sub('(\n)(\d{4} \p{Lu})', r'❦\2', x))
df['index'] = df.index + 1
df = cSplit(df, 'index', 'single_prize', '❦')
df['single_prize'] = df['single_prize'].str.replace('\n', '❦')
df = df[df['single_prize'].notnull()]
df['prize_year'] = df['single_prize'].str.replace('(^\d{4})(.+)', r'\1')
df['book_title_reason'] = df['single_prize'].apply(lambda x: re.sub('(^\d{4} )(.+)(❦)(.+)', r'\4', x) if re.findall('❦', x) else np.nan)


#%%prizes

FormData = {'q' : '*',
            'sc' : 'v cenách'}
r = requests.post("https://ceny.ucl.cas.cz/index.php", data=FormData)
r.encoding = 'ISO 8859-2'
tekst = r.text
soup = BeautifulSoup(tekst, 'html.parser')
links = soup.findAll('a', attrs={'href': re.compile("^\?c\=")})

urls = []
for link in links:
    if link.text:
        urls.append([link.text.strip(), "http://ceny.ucl.cas.cz/index.php" + link['href']])
        
#%%































