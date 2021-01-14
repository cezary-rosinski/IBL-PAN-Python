from my_functions import gsheet_to_df, df_to_gsheet, get_cosine_result
import pandas as pd
import regex as re
import glob
from unidecode import unidecode
import datetime
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
import fp_credentials
import time
from selenium.common.exceptions import NoSuchElementException, ElementNotInteractableException, NoAlertPresentException, SessionNotCreatedException, TimeoutException
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import numpy as np
import sys
import io
import requests
from bs4 import BeautifulSoup
import pandasql
import Levenshtein as lev
import itertools
import docx

# local files

extentions = ('*.pdf', '*.docx')
local_files = []
for extention in extentions:
    local_files.extend(glob.glob(f'F:/Cezary/Documents/FP/bibliografia starych numerów/***/**/*/{extention}'))

local_df = pd.DataFrame(local_files, columns=['total'])
local_df['numer'] = local_df['total'].apply(lambda x: re.findall('(?<= numerów\\\\)(.*?)(?=\\\\)', x)[0])
local_df['język'] = local_df['total'].apply(lambda x: re.sub('(.* numerów\\\\)(.*?\\\\)(eng|pl)(\\\\.*$)', r'\3', x))
local_df['kiedy'] = local_df['total'].apply(lambda x: re.findall('(?<=eng\\\\|pl\\\\)(.*?)(?=\\\\)', x)[0])
local_df['plik'] = local_df.apply(lambda x: x['total'][x['total'].index(x['kiedy']) + len(x['kiedy']) + 1:], axis=1)
local_df['rozszerzenie'] = local_df['plik'].apply(lambda x: re.sub('(.*?)(\.(?!.*\.))(.*$)', r'\3', x))
local_df['plik'] = local_df['plik'].apply(lambda x: re.sub('(.*?)(\.(?!.*\.))(.*$)', r'\1', x))

local_df = local_df[((local_df['kiedy'] == 'przed korektą') & (local_df['rozszerzenie'] == 'pdf')) |
                    ((local_df['kiedy'] == 'po korekcie') & (local_df['rozszerzenie'] == 'docx'))].reset_index(drop=True)
local_df['index'] = local_df.index+1

# collecting data from the website

browser = webdriver.Firefox()    
browser.get("http://fp.amu.edu.pl/admin")    
browser.implicitly_wait(5)
username_input = browser.find_element_by_id('user_login')
password_input = browser.find_element_by_id('user_pass')

username = fp_credentials.wordpress_username
password = fp_credentials.wordpress_password
time.sleep(1)
username_input.send_keys(username)
password_input.send_keys(password)

login_button = browser.find_element_by_id('wp-submit').click()

# collecting FP issues

urls = ['http://fp.amu.edu.pl/wp-admin/edit.php?post_type=page', 'http://fp.amu.edu.pl/wp-admin/edit.php?post_type=page&paged=2', 'http://fp.amu.edu.pl/wp-admin/edit.php?post_type=page&paged=3']

fp_issues = []

for url in urls:
    browser.get(url)  
    fp_issues_selector = browser.find_elements_by_css_selector('.row-title')
    for issue in fp_issues_selector:
        fp_issues.append([issue.text, issue.get_attribute('href')])
        
fp_issues = [l for l in fp_issues if any(v in l[0] for v in ('Forum Poetyki', 'Forum of Poetics'))]

for index, (issue, url) in enumerate(fp_issues):
    print(f"{index+1}/{len(fp_issues)}")
    browser.get(url)
    whole_link = browser.find_element_by_id('edit-slug-buttons').click()
    direct_link = browser.find_element_by_id('new-post-slug')
    direct_link = direct_link.get_attribute('value')
    content = browser.find_element_by_id('content').get_attribute('value')
    try:
        browser.find_element_by_css_selector('#xili_language_check_pl_pl')
        language = 'pl'
    except NoSuchElementException:
        language = 'eng'
    fp_issues[index] += [f"http://fp.amu.edu.pl/{direct_link}", content, language]
 
# collecting FP articles

url = 'http://fp.amu.edu.pl/wp-admin/edit.php'
browser.get(url)
total_no_of_pages = int(browser.find_element_by_css_selector('.total-pages').text)

fp_articles = []        
no_of_pages = 1 

while no_of_pages <= total_no_of_pages:
    print(f"{no_of_pages}/{total_no_of_pages}")
    titles = browser.find_elements_by_css_selector('.row-title')
    categories = browser.find_elements_by_css_selector('.column-categories a')
    tags = browser.find_elements_by_css_selector('#the-list .column-tags')
    languages = browser.find_elements_by_css_selector('.column-language div')
    for ti, ca, ta, la in zip(titles, categories, tags, languages):
        fp_articles.append([ti.text, ti.get_attribute('href'), ca.text, ta.text, la.text])
    no_of_pages += 1
    try:
        next_page = browser.find_element_by_css_selector('.next-page')
        next_page.click()
    except NoSuchElementException:
        pass

for i, l in enumerate(fp_articles):
    if l[-1][:2] == 'pl':
        fp_articles[i][-1] = 'pl'
    else:
        fp_articles[i][-1] = 'eng'
    
for index, article in enumerate(fp_articles):
    print(f"{index+1}/{len(fp_articles)}")
    browser.get(article[1])
    whole_link = browser.find_element_by_id('edit-slug-buttons').click()
    direct_link = browser.find_element_by_id('new-post-slug')
    direct_link = direct_link.get_attribute('value')
    content = browser.find_element_by_id('content').get_attribute('value')
    fp_articles[index] += [f"http://fp.amu.edu.pl/{direct_link}", content]
    
browser.close()

fp_issues_df = pd.DataFrame(fp_issues, columns=['tytuł numeru', 'www edycji numeru', 'www numeru', 'wstęp', 'język numeru'])
fp_articles_df = pd.DataFrame(fp_articles, columns=['tytuł artykułu', 'www edycji artykułu', 'kategoria', 'tagi', 'język artykułu', 'www numeru', 'abstrakt'])

fp_articles_df['abstrakt'] = fp_articles_df['abstrakt'].apply(lambda x: x.strip())

fp_articles_df = fp_articles_df[~fp_articles_df['tagi'].str.contains('Brak tagów')]

def get_author_and_tag(x):
    tagi = x.split(', ')
    for tag in tagi:
        if re.findall('^\p{Lu}', tag):
            tag_autora = tag
        else:
            tag_numeru = tag
    return (tag_autora, tag_numeru)

fp_articles_df['autor'], fp_articles_df['tag numeru'] = zip(*fp_articles_df['tagi'].apply(lambda x: get_author_and_tag(x)))

nowe_numery = ['lato 2020', 'summer 2020', 'zima 2020', 'winter 2020', 'lato 2019', 'summer 2019', 'wiosna 2020', 'spring 2020', 'jesień 2019', 'fall 2019']

fp_articles_df = fp_articles_df[~fp_articles_df['tag numeru'].isin(nowe_numery)]

query = "select * from fp_issues_df a join fp_articles_df b on a.'tytuł numeru' like '%'||b.'tag numeru'||'%'"
fp_merged = pandasql.sqldf(query)
fp_merged['index'] = fp_merged.index+1

# przywrócić abstrakty, na razie wyrzucone, bo tam są jeszcze pełne teksty - ogarnąć później jak je wyciągać

fp_merged = fp_merged.drop(columns='abstrakt')

# połączyć pliki z bibliografiami z metadanymi przy użyciu cosine similarity

fp_final_df = pd.DataFrame()

for i, row in fp_merged.iterrows():
    print(f"{i+1}/{len(fp_merged)}")
    author = row['autor'].split(' ')[-1]
    title = row['tytuł artykułu'].replace('<i>', '').replace('</i>', '')
    search_phraze = unidecode(f"{author} {title}")
    search_result_pdf = pd.DataFrame()
    search_result_docx = pd.DataFrame()
    for index, row2 in local_df.iterrows():
        if row2['rozszerzenie'] == 'pdf':
            result = lev.distance(search_phraze, row2['plik'])
            result_list = [row['index'], row2['index'], result]
            result_df = pd.DataFrame([result_list], columns=['fp_merged index', 'local_df index', 'lev distance'])
            search_result_pdf = search_result_pdf.append(result_df)
        else:
            result = lev.distance(search_phraze, row2['plik'])
            result_list = [row['index'], row2['index'], result]
            result_df = pd.DataFrame([result_list], columns=['fp_merged index', 'local_df index', 'lev distance'])
            search_result_docx = search_result_docx.append(result_df)
    search_result_pdf = search_result_pdf[(search_result_pdf['lev distance'] == search_result_pdf['lev distance'].min()) &
                                          (search_result_pdf['lev distance'] < 100)]
    search_result_docx = search_result_docx[(search_result_docx['lev distance'] == search_result_docx['lev distance'].min()) &
                                            (search_result_docx['lev distance'] < 100)]
    search_result = pd.concat([search_result_pdf, search_result_docx])
    fp_final_df = fp_final_df.append(search_result)
            

test = pd.merge(local_df, fp_final_df, how='left', left_on='index', right_on='local_df index').drop(columns=['local_df index']).rename(columns={'index': 'local_df index'})
test = pd.merge(test, fp_merged, how='left', left_on='fp_merged index', right_on='index').drop(columns=['index'])

wszystkie = len(test)
zrobione_automatycznie = len(test[test['fp_merged index'].notnull()])
do_zrobienia = len(test[test['fp_merged index'].isnull()])
print(wszystkie, zrobione_automatycznie, do_zrobienia)

df_to_gsheet(test, '1PRoGI6mpOIrWiKs9AF87IMcTsGeBHUx5JNsZJwMnsDI', 'połączone')
df_to_gsheet(fp_merged, '1PRoGI6mpOIrWiKs9AF87IMcTsGeBHUx5JNsZJwMnsDI', 'metadane')
df_to_gsheet(local_df, '1PRoGI6mpOIrWiKs9AF87IMcTsGeBHUx5JNsZJwMnsDI', 'bibliografia załącznikowa')

# 14.01.2021

bibliography = gsheet_to_df('1PRoGI6mpOIrWiKs9AF87IMcTsGeBHUx5JNsZJwMnsDI', 'połączone').iloc[:,0:8]
bibliography_metadata = gsheet_to_df('1PRoGI6mpOIrWiKs9AF87IMcTsGeBHUx5JNsZJwMnsDI', 'metadane')

bibliography = pd.merge(bibliography, bibliography_metadata, how='left', left_on='metadane index', right_on='index').sort_values(['metadane index', 'rozszerzenie']).reset_index(drop=True)

for i, row in bibliography.iterrows():
    if row['rozszerzenie'] == 'docx':
        bibliography.at[i, 'ścieżka do bibliografii'] = row['total']
    elif row['rozszerzenie'] == 'pdf':
        bibliography.at[i-1, 'ścieżka do pdf'] = row['total']
        
bibliography = bibliography[bibliography['ścieżka do bibliografii'].notnull()]

# tytuł numeru	język	wstęp	folder lokalny	numer	rok	spis treści	link do pdf	link do jpg	jpg	odnosnik	url_edycji

# lp	autor	ORCID	tytuł artykułu	kategoria	język	tytuł numeru	tag numeru	abstrakt	folder lokalny	afiliacja	biogram	bibliografia	słowa kluczowe	strony	tłumacz	tag autora	link do pdf	link do jpg	pdf	jpg	odnosnik	url_edycji	spis treści

bibliography_articles = bibliography[['autor', 'tytuł artykułu', 'kategoria', 'język artykułu', 'tytuł numeru', 'tag numeru', 'total', 'www edycji artykułu', 'ścieżka do pdf', 'ścieżka do bibliografii']].rename(columns={'język artykułu':'język', 'total':'folder lokalny', 'www edycji artykułu':'url_edycji'})

bibliography_articles['pdf'] = bibliography_articles['ścieżka do pdf'].apply(lambda x: re.split('(\\\\|\/)(?!.*(\\\\|\/))', x)[-1]).str.replace('\.pdf$', '', regex=True)

numbers_order = ['lato 2015',
 'summer 2015',
 'jesień 2015',
 'fall 2015',
 'zima 2016',
 'winter 2016',
 'wiosna/lato 2016',
 'spring/summer 2016',
 'jesień 2016',
 'fall 2016',
 'zima 2017',
 'winter 2017',
 'wiosna/lato 2017',
 'spring/summer 2017',
 'jesień 2017',
 'fall 2017',
 'zima/wiosna 2018',
 'winter/spring 2018',
 'lato 2018',
 'summer 2018',
 'jesień 2018',
 'fall 2018',  
 'winter/spring 2019',
 'zima/wiosna 2019']

categories_order = ['Teorie',
 'Przekłady',
 'Praktyki',
 'Słownik poetologiczny',
 'Archiwum poetologiczne',
 'Krytyki',
 'Polemiki']

combinations = list(itertools.product(numbers_order, categories_order))

bibliography_articles['sort'] = bibliography_articles[['tag numeru', 'kategoria']].apply(lambda x: (x['tag numeru'], x['kategoria']), axis=1).astype('category')
bibliography_articles['sort'] = bibliography_articles['sort'].cat.set_categories(combinations, ordered=True)
bibliography_articles = bibliography_articles.sort_values('sort').drop(columns='sort').reset_index(drop=True)

# później jeszcze sortować po stronach, gdy Gerard je doda

bibliography_articles['lp'] = None
bibliography_articles['ORCID'] = None
bibliography_articles['abstrakt'] = None
bibliography_articles['afiliacja'] = None
bibliography_articles['biogram'] = None

def getText(filename):
    doc = docx.Document(filename)
    fullText = []
    for para in doc.paragraphs:
        fullText.append(para.text)
    return '\n'.join(fullText)

for i, row in bibliography_articles.iterrows():
    print(f"{i+1}/{len(bibliography_articles)}")
    path = re.sub('\\\\', '/', row['ścieżka do bibliografii'])
    bibliography_articles.at[i, 'bibliografia'] = getText(path).strip()
    
bibliography_articles['słowa kluczowe'] = None
bibliography_articles['strony'] = None
bibliography_articles['tłumacz'] = None
#bibliography_articles['długość bibliografii'] = bibliography_articles['bibliografia'].map(len)

df_to_gsheet(bibliography_articles, '15O0yOBJ-pEWo8iOsyxwtivtgHawQNGnFnB75wx_3pao', 'artykuły')






















