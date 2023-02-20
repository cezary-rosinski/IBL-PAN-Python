from sickle import Sickle
import xml.etree.cElementTree as ET
import requests
from bs4 import BeautifulSoup
from glob import glob
from tqdm import tqdm
import fitz
import regex as re
from requests_html import AsyncHTMLSession
from my_functions import gsheet_to_df
import pandas as pd

#%% def
asession = AsyncHTMLSession()
async def get_sets(url):
    response = await asession.get(url)
    await response.html.arender()
    return response.html.xpath("//body/div[@id='root']/div[@id='main-container']/main[@id='main-content']/div[2]/div[1]/div[2]/div[1]/div[1]/div[1]/div[1]/div[1]/div[1]/a[1]")

#%% main

paths = [r'F:\Cezary\Documents\IBL\IPI PAN\1000 anotowanych artykułów naukowych/', r'F:\Cezary\Documents\IBL\IPI PAN\1000 anotowanych artykułów naukowych\dodane 31.01.2023/']
files = []
for path in paths:
    files.append([f for f in glob(path + '*.pdf', recursive=True)])

files = [e for sub in files for e in sub]

oai_url = 'https://bibliotekanauki.pl/api/oai/articles'
sickle = Sickle(oai_url)

bibliotekanauki_articles = []
errors = []

for file in tqdm(files):
    bn_id = re.findall('\d+', file.split('\\')[-1])[0]
    
    record = sickle.GetRecord(identifier=f'oai:bibliotekanauki.pl:{bn_id}', metadataPrefix='jats')
    try:
        article_dict = {'file': file.split('\\')[-1],
                        'title': record.metadata.get('article-title'),
                        'subject': record.metadata.get('subject'),
                        'journal': record.metadata.get('journal-title'),
                        'keywords': record.metadata.get('kwd')}
        bibliotekanauki_articles.append(article_dict)
    except AttributeError:
        errors.append(file.split('\\')[-1])

errors_df = gsheet_to_df('16j70iW1_K0AClTlJZFmnk6UQGSYtOp3hFKF7FyJ12K8', 'Arkusz1')
errors_ids = {k:v for k,v in dict(zip(errors_df['id z pliku'], errors_df['poprawny identyfikator'])).items() if isinstance(v,str)}

for file in tqdm(errors_ids.values()):
    bn_id = re.findall('\d+', file.split('\\')[-1])[0]
    
    try:
        record = sickle.GetRecord(identifier=f'oai:bibliotekanauki.pl:{bn_id}', metadataPrefix='jats', error='idDoesNotExist')
        try:
            article_dict = {'file': file.split('\\')[-1],
                            'title': record.metadata.get('article-title'),
                            'subject': record.metadata.get('subject'),
                            'journal': record.metadata.get('journal-title'),
                            'keywords': record.metadata.get('kwd')}
            bibliotekanauki_articles.append(article_dict)
        except AttributeError:
            errors.append(file.split('\\')[-1])
    except Exception as e:
        print(f'{e} __ {file}')

df = pd.DataFrame(bibliotekanauki_articles)
df.to_excel('analiza_ipi_pan.xlsx', index=False)
    
#%% notes
doc = fitz.open(file)
page = doc[0]
annot = list(page.annots())[-1]
annot.info
annot.get_text()








url = "https://bibliotekanauki.pl/search?page=1&q=%2522Wpływ udziału społeczeństwa w procedurze oceny oddziaływania na środowisko na rozwój inwestycji infrastrukturalnych%2522"
r = requests.get(url)
r.content

soup = BeautifulSoup(r.content, 'html.parser')






test = await get_sets(url)

test[0].text

test.html.xpath("//body/div[@id='root']/div[@id='main-container']/main[@id='main-content']/div[2]/div[1]/div[2]/div[1]/div[1]/div[1]/div[1]/div[1]/div[1]/a[1]")
    
        
    domain_list = [domain.text for domain in response.html.find('span.scientificFieldHeader')]
    if search_domain in domain_list:
        hum_sets.append(set_spec) 
await get_sets() # await because IPython kernel is running own loop


record.header.identifier
record.metadata
record.xml
record.raw

dir(record)

from xml.dom import minidom
xmlstr = minidom.parseString(ET.tostring(record.xml)).toprettyxml(indent="   ")
print(xmlstr)



sickle = Sickle('https://bibliotekanauki.pl/api/oai/articles')
record = sickle.GetRecords('')

records = sickle.ListRecords(metadataPrefix='oai_dc')
records.next()
<Record oai:eprints.rclis.org:4088>


dir(sickle)


https://bibliotekanauki.pl/api/oai/articles?verb=GetRecord&metadataPrefix=jats&identifier=oai:bibliotekanauki.pl:887601