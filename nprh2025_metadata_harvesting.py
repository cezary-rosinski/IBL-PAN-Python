from my_functions import gsheet_to_df
import requests
from zipfile import ZipFile, BadZipFile
from io import BytesIO
import pandas as pd
from bs4 import BeautifulSoup
import os
from sickle import Sickle
from sickle.oaiexceptions import IdDoesNotExist, BadVerb
from sickle.transport import RequestsTransport
import regex as re
from tqdm import tqdm
import time
import json
from namedentities import unicode_entities
from html import unescape
from requests.exceptions import HTTPError
import time
import pickle
import urllib3
from lxml import etree
import regex as re
from datetime import date
#%% import sources list
df = gsheet_to_df('13jRM5eHBgsAt0BNPDNne-155VFOVFRI2EExvW4S8Tms', 'oai-pmh harvesting')

sources_dict = {}

for index, row in df.iterrows():
    sources_dict[row['source']] = {'url': row['url'], 'oai': row['oai'], 'download_url': row['download_url'], 'type': row['typ']}

#%%
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
original_request = requests.Session.request

def unsafe_request(self, method, url, *args, **kwargs):
    kwargs['verify'] = False  # <- kluczowa linia
    return original_request(self, method, url, *args, **kwargs)

requests.Session.request = unsafe_request
#%% oai download
#ojs & ejournals
articles_counter_ojs = 0
articles_with_metadata_counter_ojs = 0
errors = []
results_ojs = []
for source, value in tqdm(sources_dict.items()):
    # source = 'Fabrica Litterarum Polono-Italica'
    # source = 'Wielogłos'
    # url = sources_dict.get(source).get('oai')
    if value.get('type') == 'ojs':
        url = value.get('oai')
        sick = Sickle(url)
        try:
            records = sick.ListRecords(metadataPrefix='oai_dc')
        except BadVerb:
            try:
                time.sleep(10)
                records = sick.ListRecords(metadataPrefix='oai_dc')
            except BadVerb:
                errors.append(source)
        for record in tqdm(records):
            if record.deleted == False:
                articles_counter_ojs += 1
                record = record.get_metadata()
                record = {k:v for k,v in record.items() if k in ['title', 'identifier', 'subject', 'description', 'date', 'language']}
                if record:
                    record.update({'source': source})
                    articles_with_metadata_counter_ojs += 1
                    results_ojs.append(record)
    elif value.get('type') == 'ejournals':
        url = value.get('oai')
        sick = Sickle(url)
        records = sick.ListRecords(metadataPrefix='jats', set='all')
        for record in tqdm(records):
            if record.deleted == False:
                articles_counter_ojs += 1
                identifier = record.header.identifier
                raw = record.raw
                record = record.get_metadata()
                record = {k:v for k,v in record.items() if k in ['article-title', 'article-id', 'kwd', 'abstract', 'year']}
                if record:
                    record.update({'source': source})
                    record.update({'identifier': identifier})
                    record.update({'raw': raw})
                    articles_with_metadata_counter_ojs += 1
                    results_ojs.append(record)
               
# articles_counter_rcin = 64204
# articles_with_metadata_counter_rcin = 64204
with open(f'data/nprh2025/results_ojs_{date.today()}.pkl', 'wb') as f:
    pickle.dump(results_ojs, f)
            
#rcin  
articles_counter_rcin = 0
articles_with_metadata_counter_rcin = 0
results_rcin = []
for source, value in tqdm(sources_dict.items()):
    if value.get('type') == 'rcin':
        # source = 'Teksty Drugie'
        # url = sources_dict.get(source).get('url')
        url = value.get('url')
        response = requests.get(url)
        soup = BeautifulSoup(response.text, 'html.parser')
        try:
            issues = soup.find('ul', {'class': 'tab-content__tree-sublist'}).find_all("li", {"class": "tab-content__tree-list-item"})
        except AttributeError:
            time.sleep(5)
            soup = BeautifulSoup(response.text, 'html.parser')
            time.sleep(5)
            issues = soup.find('ul', {'class': 'tab-content__tree-sublist'}).find_all("li", {"class": "tab-content__tree-list-item"})
        for issue in tqdm(issues):
            # issue = issues[1]
            issue_structure_url = issue.find('a')['href']
            response_issue_structure = requests.get(issue_structure_url)
            soup_issue_structure = BeautifulSoup(response_issue_structure.text, 'html.parser')
            active_issue = soup_issue_structure.find('div', {'class': 'tab-content__tree-fake-list-item active'})
            try:
                articles_li = active_issue.find_parent('li')
                articles = articles_li.find_all("li", {"tab-content__tree-list-item"})
                for article in articles:
                    #article = articles[10]
                    article_url = article.find('a', {'class': 'tab-content__tree-link'})['href']
                    article_id = article_url.split('/')[-1]
                    oai_id = 'oai:rcin.org.pl:' + article_id
                    sickle = Sickle('https://rcin.org.pl/ibl/dlibra/oai-pmh-repository.xml')
                    try:
                        try:
                            record = sickle.GetRecord(metadataPrefix='oai_dc', identifier=oai_id)
                        except HTTPError:
                            time.sleep(15)
                            record = sickle.GetRecord(metadataPrefix='oai_dc', identifier=oai_id)
                        record = record.get_metadata()
                        articles_counter_rcin += 1
                        if any([e in record.get('language') for e in ['pol', 'pl', 'Pl', 'Pol']]):
                            record = {k:v for k,v in record.items() if k in ['title', 'identifier', 'subject', 'description', 'date']}
                            if len(record) > 1:
                                record.update({'source': source})
                                articles_with_metadata_counter_rcin += 1
                                results_rcin.append(record)
                    except IdDoesNotExist:
                        pass
            except AttributeError:
                pass
# articles_counter_rcin = 8236
# articles_with_metadata_counter_rcin = 6661
with open(f'data/nprh2025/results_rcin_{date.today()}.pkl', 'wb') as f:
    pickle.dump(results_rcin, f)


#%% przetwarzanie wszystkich zwrotek
with open('data/nprh2025/results_rcin_2025-06-27.pkl', 'rb') as f:
    results_rcin = pickle.load(f)
    
with open('data/nprh2025/results_ojs_2025-06-27.pkl', 'rb') as f:
    results_ojs = pickle.load(f)

for e in results_ojs:
    if 'raw' in e:
        soup = BeautifulSoup(e.get('raw'), 'xml')
        art = soup.find('article')
        lang = [art.get("xml:lang")]
        e.update({'language': lang})

# languages = set([tuple(e.get('language')) for e in results_ojs if e.get('language')])
# ['pol', 'pl_PL', 'Polski', 'pl', 'polski', 'Pl', 'PL'] 

results_ojs_pl = []
for e in tqdm(results_ojs):
    if e.get('language'):
        if any([el in e.get('language') for el in ['pol', 'pl_PL', 'Polski', 'pl', 'polski', 'Pl', 'PL']]):
            results_ojs_pl.append(e)

results = results_ojs_pl + results_rcin

# dates = set([tuple(e.get('date')) for e in results if e.get('date')])

def pick_date(x):
    # x = e.get('date')
    # x = ('2013-07-29T11:07:01Z', '2013-07-29T11:07:01Z', '1995')
    dates = [int(e) if len(e) == 4 else int(max(e.split('.'), key=len)) if '.' in e else int(e[:4]) if '-' in e else int(re.findall('\d{4}', e)[0]) for e in x]
    earliest_date = min(dates)
    return earliest_date

for e in tqdm(results):
    if e.get('date'):
        e.update({'earliest_date': pick_date(e.get('date'))})
    elif e.get('year'):
        e.update({'earliest_date': pick_date(e.get('year'))})

results_2000 = [e for e in results if e.get('earliest_date') and e.get('earliest_date') >= 2000]

# check
# languages = set([tuple(e.get('language')) for e in results_2000 if e.get('language')])
# dates = set([e.get('earliest_date') for e in results_2000 if e.get('earliest_date')])

with open(f'data/nprh2025/results_2000_{date.today()}.pkl', 'wb') as f:
    pickle.dump(results_2000, f)

#analiza poezji

#klucze -- test
# dickt_keys = set([el for sub in [list(e.keys()) for e in results_2000] for el in sub])
poetry_keywords = ["poezj", "poet", "wiersz", "liry", "poemat", "sonet", "elegi", "hymn", "tren", "epigram", "frasz", "pieśn", "ballad", "satyr", "epos", "rapsod", "epitalam", "sielank", "limeryk", "dystych", "oktostych", "tercyn", "strof", "wers", "rym", "rytm", "metafor", "trop"]

def contains_poetry_keywords(text):
    if pd.isnull(text):
        return False
    text = str(text).lower()
    return any(re.search(rf'{kw}', text) for kw in poetry_keywords)

# keys_to_check = ['title', 'abstract', 'description', 'subject', 'article-title', 'kwd']  
keys_to_check = ['abstract', 'description', 'subject', 'article-title', 'kwd']  

for e in tqdm(results_2000):
    # e=results_2000[0]
    checking_list = []
    for ktc in keys_to_check:
        if e.get(ktc):
            for el in e.get(ktc):
                # print(ktc)
                checking_list.append(contains_poetry_keywords(el))
    if True in checking_list:
        e.update({'poetry': True})
    else: e.update({'poetry': False})

results_poetry = [e for e in results_2000 if e.get('poetry') == True]

df = pd.DataFrame(results_poetry)
df = df.drop('raw', axis=1)
df.to_excel('data/nprh2025/nprh2025_articles_demo_no_title.xlsx', index=False)





















    
    
