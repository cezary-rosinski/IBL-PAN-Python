#code by Nikodem Wołczuk https://github.com/nwolczuk
from my_functions import gsheet_to_df
import requests
from zipfile import ZipFile, BadZipFile
from io import BytesIO
import pandas as pd
from bs4 import BeautifulSoup
import os
from sickle import Sickle
from sickle.oaiexceptions import IdDoesNotExist
import regex as re
from tqdm import tqdm
import time
import json
from namedentities import unicode_entities
from html import unescape
from requests.exceptions import HTTPError
import time
import pickle

#%% import sources list
df = gsheet_to_df('13jRM5eHBgsAt0BNPDNne-155VFOVFRI2EExvW4S8Tms', 'oai-pmh harvesting')

sources_dict = {}

for index, row in df.iterrows():
    sources_dict[row['source']] = {'url': row['url'], 'oai': row['oai'], 'download_url': row['download_url'], 'type': row['typ']}


#%% oai download

articles_counter_ojs = 0
articles_with_metadata_counter_ojs = 0
results_ojs = []
for source, value in tqdm(sources_dict.items()):
    # source = 'Acta Universitatis Lodziensis. Folia Linguistica Polonica'
    # url = sources_dict.get(source).get('oai')
    if value.get('type') == 'ojs':
        url = value.get('oai')
        sick = Sickle(url)
        records = sick.ListRecords(metadataPrefix='oai_dc')
        for record in tqdm(records):
            if record.deleted == False:
                articles_counter_ojs += 1
                record = record.get_metadata()
                record = {k:v for k,v in record.items() if k in ['identifier', 'subject', 'description', 'date']}
                record.update({'source': source})
                if len(record) > 1:
                    articles_with_metadata_counter_ojs += 1
                    results_ojs.append(record)
            
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
                    #article = articles[0]
                    article_url = article.find('a', {'class': 'tab-content__tree-link'})['href']
                    article_id = article_url.split('/')[-1]
                    oai_id = 'oai:rcin.org.pl:' + article_id
                    sickle = Sickle('https://rcin.org.pl/ibl/dlibra/oai-pmh-repository.xml')
                    try:
                        try:
                            record = sickle.GetRecord(metadataPrefix='oai_dc', identifier=oai_id)
                        except HTTPError:
                            time.sleep(10)
                            record = sickle.GetRecord(metadataPrefix='oai_dc', identifier=oai_id)
                        record = record.get_metadata()
                        articles_counter_rcin += 1
                        if any([e in record.get('language') for e in ['pol', 'pl', 'Pl', 'Pol']]):
                            record = {k:v for k,v in record.items() if k in ['identifier', 'subject', 'description', 'date']}
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
with open('data/nprh2025/results_rcin.pkl', 'wb') as f:
    pickle.dump(results_rcin, f)
#ejournals





#%% przetwarzanie wszystkich zwrotek
with open('data/nprh2025/results_rcin.pkl', 'rb') as f:
    results_rcin = pickle.load(f)

#%%

for source in tqdm(sources_dict):
    if sources_dict[source]['uwagi'] in ('download', 'direct download'):
        articles_list = []
        url = sources_dict[source]['oai'] 
        try:
            get_oai_records(url, oai_set = 'szekspir:ART')
        except KeyboardInterrupt as error: raise error
        except: 
            print('OAI CONNECTION FAIL')
            # continue

        metadata_df = pd.DataFrame()
        source_name = source
        dir_path = f'data/nprh2025/pdfs/{source_name}/'
        os.mkdir(dir_path)
        for record in tqdm(articles_list):
            try:
                if 'language' in record:
                    if any([e in record['language'] for e in ['pol', 'pl', 'Pl', 'Pol', 'PL']]):
                        if 'relation' in record:
                            download_url = record['relation'][0].replace('/view/', '/download/')
                            art_id = download_url.split('/')[-2] + '-' + download_url.split('/')[-1]
                            output_name = source + ' ' + art_id
                            response = requests.get(download_url, verify=False)
                            with open(f'{dir_path}{output_name}.pdf', 'wb') as pdf:
                                pdf.write(response.content)
                            record['pdf_name'] = [output_name + '.pdf']
                            to_append = pd.DataFrame.from_dict({k: [list(set(v))] for k, v in record.items()})
                            metadata_df = pd.concat([metadata_df, to_append], ignore_index=True)
            except KeyboardInterrupt as error: raise error
            except: 
                print('FAIL DWONLOAD')
                continue
        metadata_df.to_excel(f'data/nprh2025/{source}.xlsx', index=False)
        
    elif sources_dict[source]['uwagi'] == 'scrap':
        articles_list = []
        url = sources_dict[source]['oai'] 
        try:
            get_oai_records(url, oai_set = '')
        except KeyboardInterrupt as error: raise error
        except: 
            print('OAI CONNECTION FAIL')
            continue
        
        metadata_df = pd.DataFrame()
        source_name = source
        dir_path = f'data/nprh2025/pdfs/{source_name}/'
        os.mkdir(dir_path)
        for record in tqdm(articles_list):
            try:
                if 'language' in record:
                    if any([e in record['language'] for e in ['pol', 'pl', 'Pl', 'Pol', 'PL']]):
                        if 'relation' in record:
                            url = record['relation'][0]
                            art_id = url.split('/')[-2] + '-' + url.split('/')[-1]
                            output_name = source + ' ' + art_id
                            response = requests.get(url, verify=False)
                            soup = BeautifulSoup(response.text, 'html.parser')
                            download_url = soup.find('a', {'class': 'download'})['href']
                            response = requests.get(download_url, verify=False)
                            with open(f'{dir_path}{output_name}.pdf', 'wb') as pdf:
                                pdf.write(response.content)
                            record['pdf_name'] = [output_name + '.pdf']
                            to_append = pd.DataFrame.from_dict({k: [list(set(v))] for k, v in record.items()})
                            metadata_df = pd.concat([metadata_df, to_append], ignore_index=True)
            except KeyboardInterrupt as error: raise error
            except: 
                print('FAIL DWONLOAD')
                continue
        metadata_df.to_excel(f'data/nprh2025/{source}.xlsx', index=False)
    elif sources_dict[source]['uwagi'] == 'rcin':
        if source in ('Teksty Drugie'): continue # -----------------------------------
        url = url = sources_dict[source]['url']
        metadata_df = pd.DataFrame()
        source_name = source
        output_path = f'data/nprh2025/pdfs/{source_name}/'
        if source_name not in os.listdir('data/nprh2025/pdfs'):
            os.mkdir(output_path)
        response = requests.get(url)
        soup = BeautifulSoup(response.text, 'html.parser')
        issues = soup.find('ul', {'class': 'tab-content__tree-sublist'}).find_all("li", {"class": "tab-content__tree-list-item"})
        for issue in issues:
            try:
                issue_name = issue.find('a').text.strip('\n\t')
                print(issue_name)
                issue_structure_url = issue.find('a')['href']
                response = requests.get(issue_structure_url)
                soup = BeautifulSoup(response.text, 'html.parser')
                active_issue = soup.find('div', {'class': 'tab-content__tree-fake-list-item active'})
                articles_li = active_issue.find_parent('li')
                articles = articles_li.find_all("li", {"tab-content__tree-list-item"})
            except KeyboardInterrupt as error: raise error
            except: continue
            for article in articles:
                try:
                    article_name = article.find('a', {'class': 'tab-content__tree-link'}).text.strip('\n\t').replace('/', '-')
                    article_name = re.sub('[?%:"/]', '', article_name)
                    if 'Spis treści' in article_name or 'Table of contents' in article_name:
                        continue
                    article_url = article.find('a', {'class': 'tab-content__tree-link'})['href']
                    article_id = article_url.split('/')[-1]
                    output_name = source + ' ' + article_url.split('/')[-3] + '-' + article_url.split('/')[-1]
                    if output_name + '.pdf' not in os.listdir(output_path):
                        print(article_name)
                        oai_id = 'oai:rcin.org.pl:' + article_id
                        oai_url = 'https://rcin.org.pl/dlibra/oai-pmh-repository.xml?verb=GetRecord&metadataPrefix=oai_dc&identifier=' + oai_id
                        response = requests.get(oai_url, verify=False)
                        soup = BeautifulSoup(response.text, 'xml')
                        record = soup.find('record')
                        
                        record_dict = {}
                        for elem in record.header.findChildren():
                            if elem.name == 'identifier':
                                record_dict['header_id'] = [elem.text]
                            else:
                                if elem.name in record_dict:
                                    record_dict[elem.name] += [elem.text]
                                else: record_dict[elem.name] = [elem.text]
                        for tag in record.find('oai_dc:dc').findChildren():
                            if tag.attrs:
                                key = tag.name + ':' + list(tag.attrs.values())[0]
                            else: key = tag.name
                            value = [tag.text]
                            if key in record_dict:
                                record_dict[key] += value
                            else: record_dict[key] = value
                        
                        if any([e in record_dict['language:pl'] for e in ['pol', 'pl', 'Pl', 'Pol']]):
                            download_url = f'https://rcin.org.pl/Content/{article_id}/download/'
                            response = requests.get(download_url)
                            zip_file = ZipFile(BytesIO(response.content))
                            zip_content = {name: zip_file.read(name) for name in zip_file.namelist()}
                            for key, value in zip_content.items():
                                if key.endswith('.pdf'):
                                    with open(f'{output_path}{output_name}.pdf', 'wb') as pdf:
                                        pdf.write(value)
                                    record_dict['pdf_name'] = [output_name + '.pdf']
                                    to_append = pd.DataFrame.from_dict({k: [list(set(v))] for k, v in record_dict.items()})
                                    metadata_df = pd.concat([metadata_df, to_append], ignore_index=True)
                except KeyboardInterrupt as error: raise error
                except: continue
        # metadata_df.to_excel(f'metadata/{source}.xlsx', index=False)

#%%

# get rcin metadata
counter = 0
for source in sources_dict:
    if source == 'Teksty Drugie':
        if sources_dict[source]['uwagi'] == 'rcin':
            metadata_df = pd.DataFrame()
            for file in tqdm(os.listdir(f'data/nprh2025/pdfs/{source}/')):
                art_id = file.split('-')[-1].replace('.pdf', '')
                oai_id = 'oai:rcin.org.pl:' + art_id
                oai_url = 'https://rcin.org.pl/dlibra/oai-pmh-repository.xml?verb=GetRecord&metadataPrefix=oai_dc&identifier=' + oai_id
                response = requests.get(oai_url)
                soup = BeautifulSoup(response.text, 'xml')
                record = soup.find('record')
                record_dict = {}
                for elem in record.header.findChildren():
                    if elem.name == 'identifier':
                        record_dict['header_id'] = [elem.text]
                    else:
                        if elem.name in record_dict:
                            record_dict[elem.name] += [elem.text]
                        else: record_dict[elem.name] = [elem.text]
                for tag in record.find('oai_dc:dc').findChildren():
                    if tag.attrs:
                        key = tag.name + ':' + list(tag.attrs.values())[0]
                    else: key = tag.name
                    value = [tag.text]
                    if key in record_dict:
                        record_dict[key] += value
                    else: record_dict[key] = value
                record_dict['pdf_name'] = [file]
                to_append = pd.DataFrame.from_dict({k: [list(set(v))] for k, v in record_dict.items()})
                metadata_df = pd.concat([metadata_df, to_append], ignore_index=True)
                counter += 1
            metadata_df.to_excel(f'data/nprh2025/{source}.xlsx', index=False)

#%% dspace

def get_oai_dsapce_records(url, resumption_token = None):
    if resumption_token:
        request_url = url + '?verb=ListRecords&resumptionToken=' + resumption_token.text
    else:
        request_url = url + '?verb=ListRecords&metadataPrefix=oai_dc&set=com_11089_5783'
    response = requests.get(request_url, verify=False)
    soup = BeautifulSoup(response.text, 'xml')
    list_records = soup.find_all('record')
    resumption_token = soup.find('resumptionToken')
    for record in list_records:
        if record.header.attrs:
            continue
        record_dict = {}
        for elem in record.header.findChildren():
            if elem.name == 'identifier':
                record_dict['header_id'] = [elem.text]
            else:
                record_dict[elem.name] = [elem.text]
        for tag in record.find('oai_dc:dc').findChildren():
            if tag.attrs:
                key = tag.name + ':' + list(tag.attrs.values())[0]
            else: key = tag.name
            value = [tag.text]
            if key in record_dict:
                record_dict[key] += value
            else: record_dict[key] = value
        articles_list.append(record_dict)
    if resumption_token:
        get_oai_dsapce_records(url, resumption_token)
    else: 
        return


for source in sources_dict:
    if sources_dict[source]['uwagi'] == 'dspace':
        articles_list = []
        url = sources_dict[source]['oai']
        try:
            get_oai_dsapce_records(url)
        except KeyboardInterrupt as error: raise error
        except: 
            print('OAI CONNECTION FAIL')
            continue

        metadata_df = pd.DataFrame()
        source_name = source
        if source_name not in os.listdir('data/nprh2025/pdfs'):
            dir_path = f'data/nprh2025/pdfs/{source_name}/'
            os.mkdir(dir_path)
        for record in tqdm(articles_list):
            try:
                if 'language' in record:
                    if any([e in record['language'] for e in ['pol', 'pl', 'Pl', 'Pol']]):
                        art_id = record['header_id'][0].split(':')[-1]
                        output_name = source + ' ' + '-'.join(art_id.split('/'))
                        if output_name + '.pdf' in os.listdir(dir_path): continue
                        base_url = 'https://dspace.uni.lodz.pl/xmlui/handle/'
                        download_url = base_url + art_id
                        response = requests.get(download_url)
                        soup = BeautifulSoup(response.text, 'html.parser')
                        pdf_url = 'https://dspace.uni.lodz.pl' + soup.find('div', {'class': 'item-page-field-wrapper table word-break'}).find('a')['href']
                        response = requests.get(pdf_url)                     
                        with open(f'{dir_path}{output_name}.pdf', 'wb') as pdf:
                            pdf.write(response.content)
                        record['pdf_name'] = [output_name + '.pdf']
                        to_append = pd.DataFrame.from_dict({k: [list(set(v))] for k, v in record.items()})
                        metadata_df = pd.concat([metadata_df, to_append], ignore_index=True)
            except KeyboardInterrupt as error: raise error
            except: 
                print('FAIL DWONLOAD')
                continue
        # metadata_df.to_excel(f'metadata/{source}.xlsx', index=False)


counter = 0
for source in sources_dict:
    if source == 'Czytanie Literatury':
        metadata_df = pd.DataFrame()
        for file in tqdm(os.listdir(f'data/nprh2025/pdfs/{source}/')):
            art_id = file.split(' ')[-1].replace('.pdf', '').replace('-', '/')
            oai_id = 'oai:dspace.uni.lodz.pl:' + art_id
            oai_url = 'http://dspace.uni.lodz.pl:8080/oai/request?verb=GetRecord&metadataPrefix=oai_dc&identifier=' + oai_id
            response = requests.get(oai_url)
            soup = BeautifulSoup(response.text, 'xml')
            record = soup.find('record')
            record_dict = {}
            for elem in record.header.findChildren():
                if elem.name == 'identifier':
                    record_dict['header_id'] = [elem.text]
                else:
                    if elem.name in record_dict:
                        record_dict[elem.name] += [elem.text]
                    else: record_dict[elem.name] = [elem.text]
            for tag in record.find('oai_dc:dc').findChildren():
                if tag.attrs:
                    key = tag.name + ':' + list(tag.attrs.values())[0]
                else: key = tag.name
                value = [tag.text]
                if key in record_dict:
                    record_dict[key] += value
                else: record_dict[key] = value
            record_dict['pdf_name'] = [file]
            to_append = pd.DataFrame.from_dict({k: [list(set(v))] for k, v in record_dict.items()})
            metadata_df = pd.concat([metadata_df, to_append], ignore_index=True)
            counter += 1
        metadata_df.to_excel(f'data/nprh2025/{source}.xlsx', index=False)

#%% ejournals

urls = {'Wielogłos': "https://www.ejournals.eu/Wieloglos/zakladka/112/#tabs",
        'Yearbook of Conrad Studies': 'https://www.ejournals.eu/Yearbook-of-Conrad-Studies/zakladka/46/#tabs',
        'Studia Litteraria': 'https://www.ejournals.eu/Studia-Litteraria/zakladka/86/#tabs',
        'Przekładaniec': 'https://www.ejournals.eu/Przekladaniec/zakladka/87/#tabs',
        'Konteksty Kultury': 'https://www.ejournals.eu/Konteksty_Kultury/zakladka/237/#tabs'
        }

urls = {'Wielogłos': "https://www.ejournals.eu/Wieloglos/zakladka/112/#tabs"}
errors = []
for source, url in tqdm(urls.items()):
    pdf_path = f'data/nprh2025/pdfs/{source}/'
    metadata_path = f'metadata/{source}/'
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'xml')
    years_list = soup.find_all('div', {"class": "numbers-list"})[0].find_all('li', {'class': 'opened'})
    issues_list = []
    for year_item in years_list:
        year = year_item.find('span').text
        issues = [(year, item.text, item['href']) for item in year_item.find_all('a')]
        issues_list.extend(issues)

    for issue in issues_list:
        issue_url = issue[2]
        response = requests.get(issue_url)
        response.encoding = 'utf-8'
        soup = BeautifulSoup(response.text, 'xml')
        articles_items = [e for e in soup.find_all('div', {'class': 'article even'})] + [e for e in soup.find_all('div', {'class': 'article odd'})]
        for elem in articles_items:
            try:
                art_url = elem.find('a', {'class': 'title'})['href']
                output_name = art_url.replace('https://www.ejournals.eu/', '').replace('/', '_')
                if output_name[-1] == '_':
                    output_name = output_name[:-1]
                response = requests.get(art_url)
                soup = BeautifulSoup(response.text, 'xml')
                xml_url = soup.find('a', {'class': 'xml'})['href']
                xml_response = requests.get(xml_url)
                xml_response.encoding = 'utf-8'
                xml = unicode_entities(unescape(xml_response.text))
                if '<LanguageCode>pol</LanguageCode>' in xml or '<LanguageCode>pl</LanguageCode>' in xml:
                    print(output_name)
                    pdf_url = soup.find('div', {'class': 'links oneArticle'}).find('a')['href']
                    response = requests.get(pdf_url)
                    if f'{output_name}.pdf' not in os.listdir(pdf_path):
                        with open(f'{pdf_path}{output_name}.pdf', 'wb') as pdf:
                            pdf.write(response.content)
                    if f'{output_name}.xml' not in os.listdir(metadata_path):
                        with open(f'{metadata_path}{output_name}.xml', 'w', encoding='utf-8') as xml_file:
                            xml_file.write(xml)
            except KeyboardInterrupt as error: raise error
            except: errors.append(('article error', elem))

#%%

def get_full_tag_name(tag):
    if not tag.parent or tag.parent.name == 'Product':
        return tag.name
    else:
        return get_full_tag_name(tag.parent) + '|' + tag.name

sources = ['Wielogłos', 'Studia Litteraria', 'Przekładaniec', 'Konteksty Kultury']

for source in sources:
    metadata = []
    for xml in tqdm(os.listdir(f'metadata/{source}/')):
        xml_metadata_dict = {}
        with open(f'metadata/{source}/{xml}', 'r', encoding='utf-8') as xf:
            xml_str = ' '.join([e.strip() for e in xf.readlines()])
        soup = BeautifulSoup(xml_str, 'xml')
        test = soup.find('Product').find_all()
        for tag in test:
            if not tag.findChildren() and tag.text:
                tag_names_path = get_full_tag_name(tag)
                if not tag_names_path in xml_metadata_dict:
                    xml_metadata_dict[tag_names_path] = [tag.text]
                else:
                    xml_metadata_dict[tag_names_path].append(tag.text)
        metadata.append(xml_metadata_dict)
    break

    
    
